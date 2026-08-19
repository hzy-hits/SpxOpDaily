"""Causal RTH pre-average detector used by the regime publisher."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


UTC = timezone.utc
DENOISING_FORWARD_VERSION = "raw_tick_denoising_forward.v1"
DENOISING_FORWARD_CONTRACT_HASH = (
    "sha256:fc276ff1d44bf4a150ff18889c445a6eaa68b12131b93b4c191765617fc1fb27"
)
DENOISING_FORWARD_START = date(2026, 8, 20)
DENOISING_SETUP = "PREAVERAGE15_PULLBACK"


def advance_denoising_forward(
    market: Mapping[str, object],
    previous: Mapping[str, object],
    *,
    now: datetime,
    session_day: date | None,
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
        **(latest_signal if status == "triggered" else {}),
    }
    return projection, {
        "session_id": session_id,
        "samples": samples,
        "cooldowns": cooldowns,
        "last_decision_epoch": last_decision,
        "latest_signal": latest_signal,
    }


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
