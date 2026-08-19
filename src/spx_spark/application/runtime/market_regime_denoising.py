"""Causal RTH pre-average detector used by the regime publisher."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone

from spx_spark.application.runtime.market_regime_range import (
    causal_spx_session_minutes,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


UTC = timezone.utc
DENOISING_FORWARD_VERSION = "raw_tick_denoising_forward.v1"
DENOISING_FORWARD_CONTRACT_HASH = (
    "sha256:fc276ff1d44bf4a150ff18889c445a6eaa68b12131b93b4c191765617fc1fb27"
)
DENOISING_FORWARD_START = date(2026, 8, 20)
DENOISING_SETUP = "PREAVERAGE15_PULLBACK"
WALL_HAZARD_VERSION = "wall_competing_risk_hazard.v1"
WALL_HAZARD_CONTRACT_HASH = (
    "sha256:ff0e0d1204b97af334ec3d65679bc0dcfdb9e4b3084912e650af6caef05494a2"
)
WALL_HAZARD_FEATURES = (
    "call_wall_distance_scale",
    "put_wall_distance_scale",
    "zero_gamma_distance_scale",
    "expected_move_scale",
)
_WALL_HAZARD_MEDIAN = (
    1.1501412772797974,
    1.1696909191453773,
    0.1449712367677628,
    1.978951565478213,
)
_WALL_HAZARD_MEAN = (
    1.5208851911995038,
    1.650917216480979,
    -0.013691080377019152,
    2.08221867757674,
)
_WALL_HAZARD_SCALE = (
    1.738054184220481,
    1.941876963243608,
    1.84620094727459,
    0.6681519971577666,
)
_WALL_HAZARD_INTERCEPT = (
    -0.8131746831125304,
    1.3923624776740768,
    -0.5791877945615551,
)
_WALL_HAZARD_COEF = (
    (0.1860169991381373, -0.7490547664408425, -0.025360678449045354, -0.06690871056421151),
    (0.31916748635514014, 0.4250397033984637, -0.0675888136350847, -0.054367409907073366),
    (-0.5051844854932778, 0.3240150630423788, 0.09294949208413028, 0.12127612047128496),
)


def advance_denoising_forward(
    market: Mapping[str, object],
    options: Mapping[str, object],
    spx_minutes: Mapping[str, object],
    latest_state: Mapping[str, object],
    previous: Mapping[str, object],
    *,
    now: datetime,
    session_day: date | None,
    spx_minute_max_age_seconds: float,
) -> tuple[dict[str, object], dict[str, object]]:
    """Advance the frozen causal 5-second RTH pre-average detector."""

    prior = _mapping(previous.get("denoising_forward_state"))
    session_id = session_day.isoformat() if session_day else None
    same_session = prior.get("session_id") == session_id
    samples = list(prior.get("samples") or ()) if same_session else []
    cooldowns = dict(_mapping(prior.get("cooldowns"))) if same_session else {}
    last_decision = prior.get("last_decision_epoch") if same_session else None
    latest_signal = _mapping(prior.get("latest_signal")) if same_session else {}
    reason = "rth_session_closed"

    bucket_epoch = int(now.timestamp()) // 5 * 5
    bucket_at = datetime.fromtimestamp(bucket_epoch, tz=UTC)
    cash = _mapping(_mapping(market.get("cross_asset")).get("cash_index"))
    observation = _mapping(_mapping(cash.get("observations")).get("index:SPX"))
    source_at = _parse_at(observation.get("source_at"))
    raw = _number(observation.get("price"))
    usable = (
        session_day is not None
        and DEFAULT_MARKET_CALENDAR.is_rth_open(bucket_at)
        and cash.get("cash_session_open") is True
        and cash.get("status") == "ready"
        and observation.get("status") == "available"
        and observation.get("quality") == "live"
        and observation.get("provider") == "schwab"
        and raw is not None
        and source_at is not None
        and source_at <= bucket_at
        and 0.0 <= (bucket_at - source_at).total_seconds() <= 15.0
    )
    if usable:
        samples = _append_sample(
            samples,
            bucket_epoch=bucket_epoch,
            raw=raw,
            source_at=source_at,
        )
        reason = "observing"

    decision_epoch = bucket_epoch - ((bucket_epoch - 5) % 60)
    if (
        usable
        and session_day >= DENOISING_FORWARD_START
        and last_decision != decision_epoch
    ):
        last_decision = decision_epoch
        signal = _detect(
            samples,
            bucket_epoch=decision_epoch,
            session_id=session_id or "",
        )
        direction = str(signal.get("direction") or "")
        if direction and decision_epoch >= int(cooldowns.get(direction) or 0):
            cooldowns[direction] = decision_epoch + 900
            latest_signal = signal

    signal_valid = (
        _parse_at(latest_signal.get("valid_until"))
        or datetime.min.replace(tzinfo=UTC)
    ) > now
    status = "triggered" if latest_signal and signal_valid else "observing"
    if session_day is None or not DEFAULT_MARKET_CALENDAR.is_rth_open(bucket_at):
        status = "unavailable"
    elif session_day < DENOISING_FORWARD_START:
        status, reason = "forward_not_started", "forward_start_session_2026-08-20"
    elif not usable:
        status, reason = "unavailable", "fresh_live_schwab_spx_unavailable"
    projection = {
        "schema_version": DENOISING_FORWARD_VERSION,
        "contract_hash": DENOISING_FORWARD_CONTRACT_HASH,
        "status": status,
        "action_authority": "none",
        "authorization_policy": "strategy_policy.bootstrap.v40",
        "evidence_status": "forward_unvalidated_user_override",
        "automatic_ordering": False,
        "reason": None if status == "triggered" else reason,
        "wall_hazard": _wall_hazard_projection(
            options,
            spx_minutes,
            latest_state,
            observed_at=now,
            session_day=session_day,
            spx_minute_max_age_seconds=spx_minute_max_age_seconds,
        ),
        **(latest_signal if status == "triggered" else {}),
    }
    return projection, {
        "session_id": session_id,
        "samples": samples,
        "cooldowns": cooldowns,
        "last_decision_epoch": last_decision,
        "latest_signal": latest_signal,
    }


def _wall_hazard_projection(
    options: Mapping[str, object],
    spx_minutes: Mapping[str, object],
    latest_state: Mapping[str, object],
    *,
    observed_at: datetime,
    session_day: date | None,
    spx_minute_max_age_seconds: float,
) -> dict[str, object]:
    structure = _mapping(options.get("structure"))
    volatility = _mapping(options.get("volatility"))
    option_at = _parse_at(options.get("as_of"))
    path = _wall_realized_scale(
        spx_minutes,
        session_day=session_day,
        observed_at=observed_at,
        max_age_seconds=spx_minute_max_age_seconds,
    )
    scale, path_sample_count, path_observed_through = path or (None, 0, None)
    spot_candidates: list[tuple[datetime, datetime, float]] = []
    quote_rows = latest_state.get("best_quotes")
    if isinstance(quote_rows, list):
        for value in quote_rows:
            row = _mapping(value)
            instrument = _mapping(row.get("instrument"))
            canonical_id = str(
                instrument.get("canonical_id") or row.get("instrument_id") or ""
            )
            source_at = _parse_at(row.get("quote_time")) or _parse_at(
                row.get("trade_time")
            )
            transport_at = _parse_at(row.get("last_update_at")) or _parse_at(
                row.get("received_at")
            )
            price = _number(row.get("effective_price"))
            if (
                canonical_id == "index:SPX"
                and row.get("provider") == "schwab"
                and row.get("quality") == "live"
                and source_at is not None
                and transport_at is not None
                and price is not None
                and 0.0 <= (observed_at - source_at).total_seconds() <= 15.0
                and 0.0 <= (observed_at - transport_at).total_seconds() <= 15.0
                and source_at <= transport_at + timedelta(seconds=2)
            ):
                spot_candidates.append((transport_at, source_at, price))
    spot_available_at, spot_source_at, spot = (
        max(spot_candidates) if spot_candidates else (None, None, None)
    )
    call_wall = _number(structure.get("call_wall"))
    put_wall = _number(structure.get("put_wall"))
    zero_gamma = _number(structure.get("zero_gamma"))
    expected_move = _number(volatility.get("expected_move_points_0dte"))
    ready = (
        options.get("quality") == "ready"
        and structure.get("gex_quality") == "open_interest_gex"
        and option_at is not None
        and 0.0 <= (observed_at - option_at).total_seconds() <= 360.0
        and scale is not None
        and spot is not None
        and expected_move is not None
        and (call_wall is not None or put_wall is not None or zero_gamma is not None)
    )
    reason = None
    if spot is None:
        reason = "fresh_live_schwab_spx_unavailable"
    elif scale is None:
        reason = "causal_spx_minute_path_unavailable"
    elif not ready:
        reason = "wall_hazard_option_structure_unavailable"
    base = {
        "schema_version": WALL_HAZARD_VERSION,
        "contract_hash": WALL_HAZARD_CONTRACT_HASH,
        "status": "available" if ready else "unavailable",
        "action_authority": "none",
        "automatic_ordering": False,
        "trained_through": "2026-08-18",
        "evidence_status": "forward_unvalidated_user_override",
        "reason": reason,
        "path_source": "spx_standardized_minutes",
        "path_sample_count": path_sample_count,
        "path_observed_through": (
            path_observed_through.isoformat() if path_observed_through is not None else None
        ),
        "spot_source_at": spot_source_at.isoformat() if spot_source_at else None,
        "spot_available_at": (
            spot_available_at.isoformat() if spot_available_at else None
        ),
    }
    if not ready:
        return base
    assert scale is not None and spot is not None and expected_move is not None
    raw = (
        (call_wall - spot) / scale if call_wall is not None else None,
        (spot - put_wall) / scale if put_wall is not None else None,
        (zero_gamma - spot) / scale if zero_gamma is not None else None,
        expected_move / scale,
    )
    standardized = [
        ((value if value is not None else median) - mean) / feature_scale
        for value, median, mean, feature_scale in zip(
            raw,
            _WALL_HAZARD_MEDIAN,
            _WALL_HAZARD_MEAN,
            _WALL_HAZARD_SCALE,
            strict=True,
        )
    ]
    logits = [
        intercept + math.fsum(weight * value for weight, value in zip(row, standardized, strict=True))
        for intercept, row in zip(_WALL_HAZARD_INTERCEPT, _WALL_HAZARD_COEF, strict=True)
    ]
    anchor = max(logits)
    weights = [math.exp(value - anchor) for value in logits]
    probabilities = [value / math.fsum(weights) for value in weights]
    upper_levels = [value for value in (call_wall, zero_gamma) if value is not None and value > spot]
    lower_levels = [value for value in (put_wall, zero_gamma) if value is not None and value < spot]
    return {
        **base,
        "available_at": max(observed_at, option_at).isoformat() if option_at else observed_at.isoformat(),
        "spot": spot,
        "path_scale_points": scale,
        "features": dict(zip(WALL_HAZARD_FEATURES, raw, strict=True)),
        "probabilities": {
            "down_break": probabilities[0],
            "no_break": probabilities[1],
            "up_break": probabilities[2],
        },
        "upper_barrier": min(upper_levels) if upper_levels else None,
        "lower_barrier": max(lower_levels) if lower_levels else None,
        "oos": {
            "sessions": 17,
            "rows": 1050,
            "delta_multiclass_brier_vs_path": -0.0133362,
            "ci95": [-0.0234477, -0.0027284],
        },
    }


def _wall_realized_scale(
    spx_minutes: Mapping[str, object],
    *,
    session_day: date | None,
    observed_at: datetime,
    max_age_seconds: float,
) -> tuple[float, int, datetime] | None:
    if session_day is None:
        return None
    samples = causal_spx_session_minutes(
        spx_minutes,
        session_day=session_day,
        now=observed_at,
    )
    latest_by_minute = {sample.minute: sample for sample in samples}
    window_start = observed_at.replace(second=0, microsecond=0) - timedelta(minutes=15)
    path = [
        latest_by_minute[minute]
        for minute in sorted(latest_by_minute)
        if window_start <= minute <= observed_at
    ]
    if len(path) < 10:
        return None
    latest = path[-1]
    if not 0.0 <= (observed_at - latest.source_at).total_seconds() <= max_age_seconds:
        return None
    if not 0.0 <= (observed_at - latest.transport_at).total_seconds() <= max_age_seconds:
        return None
    values = [sample.price for sample in path]
    realized = math.sqrt(
        math.fsum((right - left) ** 2 for left, right in zip(values, values[1:]))
    )
    return max(2.5, 1.25 * realized), len(path), latest.source_at


def _append_sample(
    samples: list[object],
    *,
    bucket_epoch: int,
    raw: float,
    source_at: datetime,
) -> list[dict[str, object]]:
    normalized = [
        dict(row)
        for row in samples
        if isinstance(row, Mapping)
        and isinstance(row.get("epoch"), int)
        and int(row["epoch"]) <= bucket_epoch
    ]
    normalized.sort(key=lambda item: int(item["epoch"]))
    if normalized:
        last = normalized[-1]
        last_source = _parse_at(last.get("source_at"))
        for epoch in range(int(last["epoch"]) + 5, bucket_epoch, 5):
            if last_source is None or epoch - int(last_source.timestamp()) > 15:
                break
            normalized.append({**last, "epoch": epoch})
    row = {"epoch": bucket_epoch, "raw": raw, "source_at": source_at.isoformat()}
    if normalized and normalized[-1].get("epoch") == bucket_epoch:
        normalized[-1] = row
    else:
        normalized.append(row)
    return [row for row in normalized if int(row["epoch"]) >= bucket_epoch - 910]


def _detect(
    samples: list[object],
    *,
    bucket_epoch: int,
    session_id: str,
) -> dict[str, object]:
    by_epoch = {
        int(row["epoch"]): _number(row.get("raw"))
        for row in samples
        if isinstance(row, Mapping) and isinstance(row.get("epoch"), int)
    }
    raw_extended = [
        by_epoch.get(epoch) for epoch in range(bucket_epoch - 910, bucket_epoch + 1, 5)
    ]
    weights = (1.0, 2.0, 3.0)
    observed_extended: list[float | None] = []
    for index in range(len(raw_extended)):
        trailing = raw_extended[max(0, index - 2) : index + 1]
        offset = 3 - len(trailing)
        valid = [
            (value, weights[offset + pos])
            for pos, value in enumerate(trailing)
            if value is not None
        ]
        observed_extended.append(
            sum(float(value) * weight for value, weight in valid)
            / sum(weight for _, weight in valid)
            if len(valid) >= 2
            else None
        )
    raw_window, observed = raw_extended[2:], observed_extended[2:]
    finite_raw = [float(value) for value in raw_window if value is not None]
    finite_observed = [float(value) for value in observed if value is not None]
    if (
        len(finite_raw) < math.ceil(0.8 * len(raw_window))
        or len(finite_observed) < math.ceil(0.9 * len(observed))
        or observed[0] is None
        or observed[-13] is None
        or observed[-1] is None
        or raw_window[-1] is None
    ):
        return {}
    realized = math.sqrt(
        math.fsum((right - left) ** 2 for left, right in zip(finite_raw, finite_raw[1:]))
    )
    scale = max(2.5, 1.25 * realized)
    impulse = float(observed[-1]) - float(observed[0])
    resume = float(observed[-1]) - float(observed[-13])
    direction, pullback = "", 0.0
    if impulse >= scale:
        pullback = max(finite_observed) - float(observed[-1])
        if 0.25 * scale <= pullback <= 0.80 * scale and resume > 0:
            direction = "UP"
    elif impulse <= -scale:
        pullback = float(observed[-1]) - min(finite_observed)
        if 0.25 * scale <= pullback <= 0.80 * scale and resume < 0:
            direction = "DOWN"
    if not direction:
        return {}
    signal_at = datetime.fromtimestamp(bucket_epoch, tz=UTC)
    trigger = float(raw_window[-1])
    sign = 1.0 if direction == "UP" else -1.0
    return {
        "status": "triggered",
        "setup_kind": DENOISING_SETUP,
        "setup_variant": "preaverage15_pullback::delta_0.60/vertical_15",
        "direction": direction,
        "session_date": session_id,
        "signal_at": signal_at.isoformat(),
        "valid_until": (signal_at + timedelta(seconds=15)).isoformat(),
        "trigger_level": trigger,
        "target_spx": trigger + sign * scale,
        "invalidation_spx": trigger - sign * scale,
        "local_scale_points": scale,
        "impulse_15m_points": impulse,
        "pullback_points": pullback,
        "resume_1m_points": resume,
    }


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)
