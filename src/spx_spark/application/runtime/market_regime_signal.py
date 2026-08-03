"""Experimental real-time regime and same-day range projection.

The producer is deliberately advisory-only.  It reads existing normalized
latest projections, advances a deterministic three-state online HMM, and
atomically publishes one small JSON document.  It never selects a direction,
contract, order, or notification target.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import signal
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import monotonic

from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object


UTC = timezone.utc
SCHEMA_VERSION = "market_regime_signal.experimental.v1"
FEATURE_SCHEMA_VERSION = "market_regime_features.v1"
MODEL_SCHEMA_VERSION = "online_gaussian_hmm_3state.v1"
RANGE_SCHEMA_VERSION = "same_day_range.experimental.v1"
STATE_NAMES = ("state_00", "state_01", "state_02")
STATE_HINTS = {
    "state_00": "down_pressure",
    "state_01": "balanced",
    "state_02": "up_pressure",
}
TRANSITION = (
    (0.94, 0.055, 0.005),
    (0.04, 0.92, 0.04),
    (0.005, 0.055, 0.94),
)
EMISSION_MEANS = (-0.75, 0.0, 0.75)
EMISSION_SIGMAS = (0.55, 0.45, 0.55)
FEATURE_WEIGHTS = {
    "return_15m_points": 0.30,
    "return_60m_points": 0.40,
    "vwap_distance_points": 0.20,
    "vwap_slope_15m_points": 0.10,
}
P10_Z = 1.2815515655446004
FUTURE_TOLERANCE_SECONDS = 2.0
HMM_CLOSE_SHIFT_FRACTION = 0.25
HMM_ADJUSTED_RANGE_VERSION = "hmm-adjusted-close:k0p25:v1"

_MODEL_SPEC = {
    "schema_version": MODEL_SCHEMA_VERSION,
    "states": STATE_NAMES,
    "transition": TRANSITION,
    "emission_means": EMISSION_MEANS,
    "emission_sigmas": EMISSION_SIGMAS,
    "feature_schema_version": FEATURE_SCHEMA_VERSION,
    "feature_weights": FEATURE_WEIGHTS,
    "parameter_mode": "fixed_bootstrap",
    "inference": "causal_forward_filter",
}
MODEL_VERSION = "sha256:" + hashlib.sha256(
    json.dumps(_MODEL_SPEC, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()


@dataclass(frozen=True, slots=True)
class SignalPaths:
    market: Path
    options: Path
    spx_minutes: Path
    latest_state: Path
    prior_rth_context: Path
    output: Path
    state: Path

    @classmethod
    def from_data_root(cls, data_root: str | Path) -> "SignalPaths":
        latest = Path(data_root).expanduser() / "latest"
        return cls(
            market=latest / "minute_market_frame.json",
            options=latest / "option_structure_frame.json",
            spx_minutes=latest / "spx_standardized_minutes.json",
            latest_state=latest / "state.json",
            prior_rth_context=latest / "prior_rth_context.json",
            output=latest / "experimental_research_signals.json",
            state=latest / "experimental_research_signals.state.json",
        )


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


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _session_date(market: Mapping[str, object], now: datetime) -> date | None:
    raw = market.get("session_id")
    if isinstance(raw, str):
        try:
            candidate = date.fromisoformat(raw)
        except ValueError:
            candidate = None
        if candidate is not None and DEFAULT_MARKET_CALENDAR.session(candidate) is not None:
            return candidate
    return DEFAULT_MARKET_CALENDAR.spx_session_date_for(now, retain_completed=True)


def _expected_move(options: Mapping[str, object]) -> float | None:
    value = _number(_mapping(options.get("volatility")).get("expected_move_points_0dte"))
    return value if value is not None and value > 0.0 else None


def _feature_observation(
    market: Mapping[str, object],
    options: Mapping[str, object],
) -> dict[str, object] | None:
    es = _mapping(market.get("es"))
    expected_move = _expected_move(options)
    raw = {name: _number(es.get(name)) for name in FEATURE_WEIGHTS}
    available = {name: value for name, value in raw.items() if value is not None}
    if not available:
        return None
    fallback_scale = max(
        10.0,
        abs(available.get("return_60m_points", 0.0)),
        2.0 * abs(available.get("return_15m_points", 0.0)),
        2.0 * abs(available.get("vwap_distance_points", 0.0)),
    )
    scale = expected_move or fallback_scale
    total_weight = sum(FEATURE_WEIGHTS[name] for name in available)
    score = sum(
        FEATURE_WEIGHTS[name] * max(-2.0, min(2.0, value / scale))
        for name, value in available.items()
    ) / total_weight
    efficiency = _number(es.get("trend_efficiency_60m"))
    if efficiency is not None:
        score *= 0.75 + 0.5 * max(0.0, min(1.0, efficiency))
    score = max(-2.0, min(2.0, score))
    payload: dict[str, object] = {
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "direction_score": round(score, 10),
        "scale_points": round(scale, 10),
        "scale_source": "expected_move_points_0dte" if expected_move else "bounded_local_fallback",
        "values": {name: raw[name] for name in FEATURE_WEIGHTS},
        "trend_efficiency_60m": efficiency,
    }
    payload["observation_id"] = _canonical_hash(
        {
            "market_frame_id": market.get("frame_id"),
            "market_as_of": market.get("as_of"),
            "observation": payload,
        }
    )
    return payload


def _online_posterior(score: float, prior: Sequence[float]) -> tuple[float, float, float]:
    predicted = tuple(
        sum(float(prior[source]) * TRANSITION[source][target] for source in range(3))
        for target in range(3)
    )
    log_weights = []
    for probability, mean, sigma in zip(
        predicted, EMISSION_MEANS, EMISSION_SIGMAS, strict=True
    ):
        log_likelihood = -math.log(sigma) - 0.5 * ((score - mean) / sigma) ** 2
        log_weights.append(math.log(max(probability, 1e-15)) + log_likelihood)
    maximum = max(log_weights)
    weights = [math.exp(value - maximum) for value in log_weights]
    total = sum(weights)
    return tuple(value / total for value in weights)  # type: ignore[return-value]


def _validated_prior(value: object) -> tuple[float, float, float] | None:
    if not isinstance(value, list) or len(value) != 3:
        return None
    probabilities = [_number(item) for item in value]
    if any(item is None or item < 0.0 for item in probabilities):
        return None
    resolved = [float(item) for item in probabilities if item is not None]
    total = math.fsum(resolved)
    if total <= 0.0:
        return None
    return tuple(item / total for item in resolved)  # type: ignore[return-value]


def _regime_projection(
    *,
    market: Mapping[str, object],
    options: Mapping[str, object],
    previous: Mapping[str, object],
    now: datetime,
    max_input_age_seconds: float,
    session_day: date | None,
) -> tuple[dict[str, object], dict[str, object]]:
    observation = _feature_observation(market, options)
    market_as_of = _parse_at(market.get("as_of"))
    previous_state = _mapping(previous.get("online_state"))
    retained_state: dict[str, object] = {
        "model_version": MODEL_VERSION,
        "session_id": previous_state.get("session_id"),
        "last_observation_id": previous_state.get("last_observation_id"),
        "last_as_of": previous_state.get("last_as_of"),
        "observation_count": int(previous_state.get("observation_count") or 0),
        "posterior": previous_state.get("posterior") or [1 / 3, 1 / 3, 1 / 3],
        "dominant_state": previous_state.get("dominant_state"),
        "dwell_observations": int(previous_state.get("dwell_observations") or 0),
    }
    reasons: list[str] = []
    if market_as_of is None:
        reasons.append("market_as_of_missing")
    elif (now - market_as_of).total_seconds() > max_input_age_seconds:
        reasons.append("market_frame_stale")
    elif (market_as_of - now).total_seconds() > FUTURE_TOLERANCE_SECONDS:
        reasons.append("market_frame_from_future")
    if str(market.get("quality") or "unavailable") == "unavailable":
        reasons.append("market_frame_unavailable")
    if observation is None:
        reasons.append("direction_features_missing")
    if session_day is None:
        reasons.append("session_date_unavailable")
    if reasons:
        return (
            {
                "status": "unavailable",
                "quality": "unavailable",
                "reasons": sorted(set(reasons)),
                "model_schema_version": MODEL_SCHEMA_VERSION,
                "model_version": MODEL_VERSION,
                "posterior": None,
                "dominant_state": None,
                "action_authority": "none",
            },
            retained_state,
        )

    assert market_as_of is not None and observation is not None and session_day is not None
    session_id = session_day.isoformat()
    same_session = (
        previous_state.get("model_version") == MODEL_VERSION
        and previous_state.get("session_id") == session_id
    )
    prior_raw = previous_state.get("posterior") if same_session else None
    prior = _validated_prior(prior_raw) or (1 / 3, 1 / 3, 1 / 3)
    observation_id = str(observation["observation_id"])
    last_as_of = _parse_at(previous_state.get("last_as_of")) if same_session else None
    duplicate = same_session and previous_state.get("last_observation_id") == observation_id
    out_of_order = last_as_of is not None and market_as_of < last_as_of
    if duplicate:
        posterior = prior
        advanced = False
    elif out_of_order:
        return (
            {
                "status": "unavailable",
                "quality": "unavailable",
                "reasons": ["observation_out_of_order"],
                "model_schema_version": MODEL_SCHEMA_VERSION,
                "model_version": MODEL_VERSION,
                "posterior": None,
                "dominant_state": None,
                "action_authority": "none",
            },
            retained_state,
        )
    else:
        posterior = _online_posterior(float(observation["direction_score"]), prior)
        advanced = True

    probabilities = dict(zip(STATE_NAMES, posterior, strict=True))
    dominant = max(STATE_NAMES, key=probabilities.__getitem__)
    entropy = -sum(value * math.log(value) for value in posterior if value > 0.0)
    count = int(previous_state.get("observation_count") or 0) if same_session else 0
    previous_dominant = previous_state.get("dominant_state") if same_session else None
    dwell = int(previous_state.get("dwell_observations") or 0) if same_session else 0
    if advanced:
        count += 1
        dwell = dwell + 1 if previous_dominant == dominant else 1
    state = {
        "model_version": MODEL_VERSION,
        "session_id": session_id,
        "last_observation_id": observation_id,
        "last_as_of": market_as_of.isoformat(),
        "observation_count": count,
        "posterior": list(posterior),
        "dominant_state": dominant,
        "dwell_observations": max(dwell, 1),
    }
    quality = "ready" if market.get("quality") == "ready" and _expected_move(options) else "degraded"
    return (
        {
            "status": "available",
            "quality": quality,
            "reasons": [] if quality == "ready" else ["fallback_feature_scale_or_degraded_frame"],
            "model_schema_version": MODEL_SCHEMA_VERSION,
            "model_version": MODEL_VERSION,
            "parameter_mode": "fixed_bootstrap",
            "inference": "causal_forward_filter",
            "state_hints": STATE_HINTS,
            "posterior": {key: round(value, 10) for key, value in probabilities.items()},
            "dominant_state": dominant,
            "max_state_probability": round(probabilities[dominant], 10),
            "entropy_nats": round(entropy, 10),
            "normalized_entropy": round(entropy / math.log(3.0), 10),
            "observation_count": count,
            "dwell_observations": max(dwell, 1),
            "advanced": advanced,
            "as_of": market_as_of.isoformat(),
            "observation": observation,
            "action_authority": "none",
        },
        state,
    )


def _selected_price(row: Mapping[str, object]) -> float | None:
    selected = _mapping(row.get("selected"))
    value = _number(selected.get("price"))
    return value if value is not None and value > 0.0 else None


def _live_es_price_and_close(
    latest_state: Mapping[str, object],
    *,
    now: datetime,
    max_input_age_seconds: float,
) -> tuple[float | None, float | None]:
    rows = latest_state.get("best_quotes")
    if not isinstance(rows, list):
        return None, None
    candidates: list[tuple[int, datetime, float, float]] = []
    for value in rows:
        row = _mapping(value)
        instrument = _mapping(row.get("instrument"))
        canonical_id = str(instrument.get("canonical_id") or row.get("instrument_id") or "")
        if not canonical_id.startswith("future:ES") or row.get("quality") != "live":
            continue
        price = _number(row.get("mid")) or _number(row.get("effective_price"))
        close = _number(row.get("close"))
        observed_at = (
            _parse_at(row.get("quote_time"))
            or _parse_at(row.get("trade_time"))
            or _parse_at(row.get("last_update_at"))
        )
        age_seconds = (now - observed_at).total_seconds() if observed_at is not None else None
        if (
            price is not None
            and price > 0.0
            and close is not None
            and close > 0.0
            and observed_at is not None
            and age_seconds is not None
            and -FUTURE_TOLERANCE_SECONDS <= age_seconds <= max_input_age_seconds
        ):
            candidates.append((1 if row.get("provider") == "ibkr" else 0, observed_at, price, close))
    if not candidates:
        return None, None
    _, _, price, close = max(candidates)
    return price, close


def _observed_edge(
    spx_minutes: Mapping[str, object],
    *,
    session_day: date,
    edge: str,
) -> tuple[float, datetime] | None:
    session = DEFAULT_MARKET_CALENDAR.session(session_day)
    rows = spx_minutes.get("rows")
    if session is None or not isinstance(rows, list):
        return None
    candidates: list[tuple[datetime, float]] = []
    for value in rows:
        row = _mapping(value)
        if row.get("session_date") != session_day.isoformat():
            continue
        at = _parse_at(row.get("minute"))
        price = _selected_price(row)
        if at is not None and price is not None:
            candidates.append((at, price))
    if not candidates:
        return None
    candidates.sort()
    if edge == "open":
        at, price = candidates[0]
        return (price, at) if abs((at - session.open_at.astimezone(UTC)).total_seconds()) <= 120 else None
    at, price = candidates[-1]
    return (price, at) if abs((at - session.close_at.astimezone(UTC)).total_seconds()) <= 120 else None


def _unavailable_range(reason: str, *, target_at: datetime | None) -> dict[str, object]:
    return {
        "status": "unavailable",
        "quality": "unavailable",
        "reason": reason,
        "p10": None,
        "p50": None,
        "p90": None,
        "target_at": target_at.isoformat() if target_at is not None else None,
    }


def _normal_range(
    *,
    anchor: float,
    sigma_points: float,
    source: str,
    semantics: str,
    target_at: datetime,
) -> dict[str, object]:
    width = P10_Z * sigma_points
    return {
        "status": "available",
        "quality": "degraded",
        "source": source,
        "semantics": semantics,
        "p10": round(anchor - width, 4),
        "p50": round(anchor, 4),
        "p90": round(anchor + width, 4),
        "target_at": target_at.isoformat(),
    }


def _range_projection(
    *,
    market: Mapping[str, object],
    options: Mapping[str, object],
    spx_minutes: Mapping[str, object],
    latest_state: Mapping[str, object],
    prior_rth_context: Mapping[str, object],
    session_day: date | None,
    now: datetime,
    max_input_age_seconds: float,
) -> dict[str, object]:
    if session_day is None:
        missing = _unavailable_range("session_date_unavailable", target_at=None)
        return {"schema_version": RANGE_SCHEMA_VERSION, "session_date": None, "open": missing, "close": missing}
    session = DEFAULT_MARKET_CALENDAR.session(session_day)
    assert session is not None
    observed_open = _observed_edge(spx_minutes, session_day=session_day, edge="open")
    observed_close = _observed_edge(spx_minutes, session_day=session_day, edge="close")
    if observed_open is not None:
        price, at = observed_open
        open_range = {
            "status": "observed",
            "quality": "ready",
            "source": "spx_standardized_minutes",
            "semantics": "observed_rth_open_degenerate",
            "p10": price,
            "p50": price,
            "p90": price,
            "target_at": at.isoformat(),
        }
    elif now >= session.open_at.astimezone(UTC):
        open_range = _unavailable_range(
            "official_rth_open_observation_missing",
            target_at=session.open_at.astimezone(UTC),
        )
    else:
        es_price, es_close = _live_es_price_and_close(
            latest_state,
            now=now,
            max_input_age_seconds=max_input_age_seconds,
        )
        prior_spx_close = _number(prior_rth_context.get("close"))
        prior_for_date = prior_rth_context.get("for_trading_date")
        es_gap_anchor = (
            prior_spx_close + es_price - es_close
            if (
                es_price is not None
                and es_close is not None
                and prior_spx_close is not None
                and prior_for_date == session_day.isoformat()
            )
            else None
        )
        expected_move = _expected_move(options)
        if es_gap_anchor is None or expected_move is None:
            open_range = _unavailable_range(
                "open_proxy_inputs_missing",
                target_at=session.open_at.astimezone(UTC),
            )
        else:
            to_open = max((session.open_at.astimezone(UTC) - now).total_seconds(), 0.0)
            to_close = max((session.close_at.astimezone(UTC) - now).total_seconds(), 1.0)
            sigma = expected_move * math.sqrt(min(to_open / to_close, 1.0))
            open_range = _normal_range(
                anchor=es_gap_anchor,
                sigma_points=sigma,
                source="expected_move_with_prior_spx_close_plus_es_gap",
                semantics="experimental_time_scaled_normal_proxy_not_calibrated",
                target_at=session.open_at.astimezone(UTC),
            )

    if now >= session.close_at.astimezone(UTC) and observed_close is not None:
        price, at = observed_close
        close_range = {
            "status": "observed",
            "quality": "ready",
            "source": "spx_standardized_minutes",
            "semantics": "observed_rth_close_degenerate",
            "p10": price,
            "p50": price,
            "p90": price,
            "target_at": at.isoformat(),
        }
    else:
        option_as_of = _parse_at(options.get("as_of"))
        option_fresh = option_as_of is not None and (
            -FUTURE_TOLERANCE_SECONDS
            <= (now - option_as_of).total_seconds()
            <= max_input_age_seconds
        )
        expiry = str(options.get("front_expiry") or "")
        expiry_ok = bool(expiry) and expiry == session_day.strftime("%Y%m%d")
        density = _mapping(options.get("density"))
        p10 = _number(density.get("p10"))
        p50 = _number(density.get("median")) or _number(density.get("p50"))
        p90 = _number(density.get("p90"))
        density_ok = (
            density.get("quality") in {"ok", "ready", "live"}
            and p10 is not None
            and p50 is not None
            and p90 is not None
            and p10 < p50 < p90
        )
        if option_fresh and expiry_ok and density_ok:
            close_range = {
                "status": "available",
                "quality": "ready",
                "source": "option_structure_frame.density",
                "semantics": "risk_neutral_terminal_not_physical",
                "p10": p10,
                "p50": p50,
                "p90": p90,
                "target_at": session.close_at.astimezone(UTC).isoformat(),
                "expiry": expiry or None,
                "as_of": option_as_of.isoformat() if option_as_of else None,
                "expected_move_points": _expected_move(options),
            }
        else:
            reason = "front_expiry_mismatch" if not expiry_ok else "fresh_risk_neutral_density_unavailable"
            close_range = _unavailable_range(
                reason,
                target_at=session.close_at.astimezone(UTC),
            )
    return {
        "schema_version": RANGE_SCHEMA_VERSION,
        "session_date": session_day.isoformat(),
        "open": open_range,
        "close": close_range,
    }


def build_signal(
    *,
    market: Mapping[str, object],
    options: Mapping[str, object],
    spx_minutes: Mapping[str, object],
    latest_state: Mapping[str, object],
    prior_rth_context: Mapping[str, object],
    previous: Mapping[str, object],
    now: datetime,
    max_input_age_seconds: float,
) -> dict[str, object]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(UTC)
    session_day = _session_date(market, now)
    regime, online_state = _regime_projection(
        market=market,
        options=options,
        previous=previous,
        now=now,
        max_input_age_seconds=max_input_age_seconds,
        session_day=session_day,
    )
    ranges = _range_projection(
        market=market,
        options=options,
        spx_minutes=spx_minutes,
        latest_state=latest_state,
        prior_rth_context=prior_rth_context,
        session_day=session_day,
        now=now,
        max_input_age_seconds=max_input_age_seconds,
    )
    source_times = [
        parsed
        for parsed in (
            _parse_at(market.get("as_of")),
            _parse_at(options.get("as_of")),
            _parse_at(latest_state.get("as_of")),
            _parse_at(prior_rth_context.get("as_of")),
        )
        if parsed is not None
    ]
    fingerprint = _canonical_hash(
        {
            "market": market,
            "options": options,
            "spx_minutes": spx_minutes,
            "latest_state": latest_state,
            "prior_rth_context": prior_rth_context,
        }
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "experimental": True,
        "generated_at": now.isoformat(),
        "as_of": max(source_times).isoformat() if source_times else None,
        "session_date": session_day.isoformat() if session_day else None,
        "input_fingerprint": fingerprint,
        "sources": {
            "market_frame_id": market.get("frame_id"),
            "market_as_of": market.get("as_of"),
            "option_frame_id": options.get("frame_id"),
            "option_as_of": options.get("as_of"),
        },
        "regime": regime,
        "today_range": ranges,
        "online_state": online_state,
        "readiness": "experimental_context_only",
        "action_authority": "none",
        "automatic_ordering": False,
    }


def _wire_range(
    *,
    kind: str,
    values: Mapping[str, object],
    feature_set_version: str,
    model_version: str,
    distribution: str,
    session: str,
    observed_through: datetime,
    available_at: datetime,
    lineage_suffix: str,
) -> dict[str, object] | None:
    lower = _number(values.get("p10"))
    median = _number(values.get("p50"))
    upper = _number(values.get("p90"))
    target_at = _parse_at(values.get("target_at"))
    if (
        lower is None
        or median is None
        or upper is None
        or not lower < median < upper
        or target_at is None
        or target_at <= available_at
        or observed_through > available_at
    ):
        return None
    valid_until = min(available_at + timedelta(seconds=60), target_at)
    return {
        "forecast_id": f"range:{kind.replace('_', '-')}:{lineage_suffix}",
        "forecast_kind": kind,
        "feature_set_version": feature_set_version,
        "model_version": model_version,
        "lineage_id": f"lineage:{kind}:{lineage_suffix}",
        "session": session,
        "observed_through": observed_through.isoformat(),
        "available_at": available_at.isoformat(),
        "valid_until": valid_until.isoformat(),
        "target_at": target_at.isoformat(),
        "distribution": distribution,
        "use_scope": "experimental",
        "lower_probability": 0.1,
        "lower": lower,
        "median": median,
        "upper_probability": 0.9,
        "upper": upper,
    }


def _wire_document(signal: Mapping[str, object], *, available_at: datetime) -> dict[str, object] | None:
    fingerprint = str(signal.get("input_fingerprint") or "")
    suffix = fingerprint.removeprefix("sha256:")[:24]
    if not suffix:
        return None
    session = (
        "rth"
        if DEFAULT_MARKET_CALENDAR.is_rth_open(available_at)
        else "gth"
    )
    regime = _mapping(signal.get("regime"))
    market_regime: dict[str, object] | None = None
    regime_as_of = _parse_at(regime.get("as_of"))
    posterior = _mapping(regime.get("posterior"))
    if regime.get("status") == "available" and regime_as_of is not None and regime_as_of <= available_at:
        ordered = [
            {"state_id": state, "probability": _number(posterior.get(state))}
            for state in STATE_NAMES
        ]
        if all(row["probability"] is not None for row in ordered):
            market_regime = {
                "signal_id": f"regime:{suffix}",
                "feature_set_version": FEATURE_SCHEMA_VERSION,
                "model_version": MODEL_VERSION,
                "lineage_id": f"lineage:regime:{suffix}",
                "session": session,
                "observed_through": regime_as_of.isoformat(),
                "available_at": available_at.isoformat(),
                "valid_until": (available_at + timedelta(seconds=60)).isoformat(),
                "inference_semantics": "causal_filtered",
                "use_scope": "experimental",
                "posterior": ordered,
                "posterior_entropy": _number(regime.get("entropy_nats")),
                "observation_count": int(regime.get("observation_count") or 1),
            }

    ranges = _mapping(signal.get("today_range"))
    observed_through = _parse_at(signal.get("as_of"))
    forecasts: list[dict[str, object]] = []
    if observed_through is not None and observed_through <= available_at:
        open_values = _mapping(ranges.get("open"))
        if open_values.get("status") == "available":
            projected_open = _wire_range(
                kind="projected_open",
                values=open_values,
                feature_set_version="overnight-es-gap-em:v1",
                model_version="projected-open-normal:v1",
                distribution="experimental_heuristic",
                session=session,
                observed_through=observed_through,
                available_at=available_at,
                lineage_suffix=suffix,
            )
            if projected_open is not None:
                forecasts.append(projected_open)
        close_values = _mapping(ranges.get("close"))
        if close_values.get("status") == "available":
            risk_neutral = _wire_range(
                kind="risk_neutral_close",
                values=close_values,
                feature_set_version="option-implied-range:v1",
                model_version="risk-neutral-density:v1",
                distribution="risk_neutral",
                session=session,
                observed_through=observed_through,
                available_at=available_at,
                lineage_suffix=suffix,
            )
            if risk_neutral is not None:
                forecasts.append(risk_neutral)
                if market_regime is not None:
                    probability_by_state = {
                        str(row["state_id"]): float(row["probability"])
                        for row in market_regime["posterior"]  # type: ignore[index]
                    }
                    expected_move = _number(close_values.get("expected_move_points"))
                    if expected_move is None:
                        lower = _number(close_values.get("p10"))
                        upper = _number(close_values.get("p90"))
                        assert lower is not None and upper is not None
                        expected_move = (upper - lower) / (2.0 * P10_Z)
                    shift = HMM_CLOSE_SHIFT_FRACTION * expected_move * (
                        probability_by_state["state_02"] - probability_by_state["state_00"]
                    )
                    adjusted_values = dict(close_values)
                    for key in ("p10", "p50", "p90"):
                        adjusted_values[key] = float(adjusted_values[key]) + shift
                    adjusted = _wire_range(
                        kind="hmm_adjusted_close",
                        values=adjusted_values,
                        feature_set_version="option-range-plus-regime:v1",
                        model_version=HMM_ADJUSTED_RANGE_VERSION,
                        distribution="experimental_heuristic",
                        session=session,
                        observed_through=observed_through,
                        available_at=available_at,
                        lineage_suffix=suffix,
                    )
                    if adjusted is not None:
                        forecasts.append(adjusted)
    if market_regime is None and not forecasts:
        return None
    return {
        "schema_version": "experimental_research_signals.v1",
        "document_id": f"research:{suffix}",
        "generated_at": available_at.isoformat(),
        "market_regime": market_regime,
        "range_forecasts": forecasts,
        "automatic_ordering": False,
    }


def produce_once(
    *,
    paths: SignalPaths,
    now: datetime,
    max_input_age_seconds: float,
) -> dict[str, object]:
    market = read_json_object(paths.market)
    options = read_json_object(paths.options)
    spx_minutes = read_json_object(paths.spx_minutes)
    latest_state = read_json_object(paths.latest_state)
    prior_rth_context = read_json_object(paths.prior_rth_context)
    with exclusive_state_lock(paths.state, timeout_seconds=5.0):
        previous = read_json_object(paths.state)
        fingerprint = _canonical_hash(
            {
                "market": market,
                "options": options,
                "spx_minutes": spx_minutes,
                "latest_state": latest_state,
                "prior_rth_context": prior_rth_context,
            }
        )
        if previous.get("input_fingerprint") == fingerprint:
            cached = _mapping(previous.get("wire_document"))
            if cached:
                if read_json_object(paths.output) != cached:
                    atomic_write_json_secure(paths.output, cached)
                return cached
        payload = build_signal(
            market=market,
            options=options,
            spx_minutes=spx_minutes,
            latest_state=latest_state,
            prior_rth_context=prior_rth_context,
            previous=previous,
            now=now,
            max_input_age_seconds=max_input_age_seconds,
        )
        wire = _wire_document(payload, available_at=now.astimezone(UTC))
        state_payload = {
            "schema_version": "experimental_research_signals.state.v1",
            "input_fingerprint": payload["input_fingerprint"],
            "online_state": payload["online_state"],
            "wire_document": wire,
        }
        atomic_write_json_secure(paths.state, state_payload)
        if wire is not None:
            atomic_write_json_secure(paths.output, wire)
            return wire
        return {
            "schema_version": "experimental_research_signals.unavailable.v1",
            "generated_at": now.astimezone(UTC).isoformat(),
            "reasons": ["no_valid_research_signal"],
        }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish an experimental online regime signal.")
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--market-path", type=Path)
    parser.add_argument("--options-path", type=Path)
    parser.add_argument("--spx-minutes-path", type=Path)
    parser.add_argument("--latest-state-path", type=Path)
    parser.add_argument("--prior-rth-context-path", type=Path)
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--state-path", type=Path)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--max-input-age-seconds", type=float)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--now", help="Aware ISO timestamp for deterministic --once replay.")
    parser.add_argument("--json", action="store_true", help="Print each produced projection.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    storage = StorageSettings.from_env()
    defaults = SignalPaths.from_data_root(args.data_root or storage.data_root)
    paths = SignalPaths(
        market=args.market_path or defaults.market,
        options=args.options_path or defaults.options,
        spx_minutes=args.spx_minutes_path or defaults.spx_minutes,
        latest_state=args.latest_state_path or defaults.latest_state,
        prior_rth_context=args.prior_rth_context_path or defaults.prior_rth_context,
        output=args.output_path or defaults.output,
        state=args.state_path or defaults.state,
    )
    if args.interval_seconds <= 0:
        raise ValueError("--interval-seconds must be positive")
    max_input_age_seconds = (
        float(args.max_input_age_seconds)
        if args.max_input_age_seconds is not None
        else float(storage.latest_stale_after_seconds)
    )
    if max_input_age_seconds <= 0:
        raise ValueError("--max-input-age-seconds must be positive")
    fixed_now = _parse_at(args.now) if args.now else None
    if args.now and fixed_now is None:
        raise ValueError("--now must be an aware ISO-8601 timestamp")
    if fixed_now is not None and not args.once:
        raise ValueError("--now requires --once")
    stop = threading.Event()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_number, lambda *_args: stop.set())
    while not stop.is_set():
        started = monotonic()
        payload = produce_once(
            paths=paths,
            now=fixed_now or datetime.now(tz=UTC),
            max_input_age_seconds=max_input_age_seconds,
        )
        if args.json:
            print(json.dumps(payload, allow_nan=False, sort_keys=True), flush=True)
        if args.once:
            break
        stop.wait(max(args.interval_seconds - (monotonic() - started), 0.0))
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
