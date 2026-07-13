"""Pure multi-horizon ES Globex trend state machine."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

from spx_spark.application.globex_trend.models import GlobexTrendRegime
from spx_spark.settings.globex_trend import GlobexTrendSettings


def initial_state(session_id: str) -> dict[str, Any]:
    return {
        "version": 1,
        "session_id": session_id,
        "regime": GlobexTrendRegime.NEUTRAL.value,
        "candidate_regime": None,
        "candidate_observations": 0,
        "transition_sequence": 0,
        "regime_started_at": None,
        "regime_high": None,
        "regime_low": None,
        "samples": [],
        "metrics": {},
        "last_transition": None,
        "pending_event": None,
        "updated_at": None,
    }


def advance_trend_state(
    state: dict[str, Any],
    *,
    session_id: str,
    at: datetime,
    price: float,
    provider: str,
    source_at: datetime,
    policy: GlobexTrendSettings,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    current = deepcopy(state) if state.get("session_id") == session_id else initial_state(session_id)
    samples = _samples(current)
    if samples:
        if str(samples[-1].get("source_at")) == source_at.isoformat():
            current["updated_at"] = at.isoformat()
            return current, None
        last_at = datetime.fromisoformat(str(samples[-1]["at"]))
        if (at - last_at).total_seconds() < policy.sample_interval_seconds:
            current["updated_at"] = at.isoformat()
            return current, None

    samples.append(
        {
            "at": at.isoformat(),
            "source_at": source_at.isoformat(),
            "price": float(price),
            "provider": provider,
        }
    )
    cutoff = at - timedelta(hours=policy.retention_hours)
    samples = [row for row in samples if datetime.fromisoformat(str(row["at"])) >= cutoff]
    current["samples"] = samples
    metrics = compute_metrics(samples, policy=policy)
    _update_regime_extrema(current, price=float(price))
    metrics["regime_high"] = current["regime_high"]
    metrics["regime_low"] = current["regime_low"]
    metrics["drawdown_from_regime_high_points"] = float(price) - float(
        current["regime_high"]
    )
    metrics["rebound_from_regime_low_points"] = float(price) - float(
        current["regime_low"]
    )
    current["metrics"] = metrics
    current["updated_at"] = at.isoformat()

    regime = GlobexTrendRegime(str(current.get("regime") or "neutral"))
    target, reason = target_regime(regime, metrics, policy=policy)
    if target is None or target is regime:
        current["candidate_regime"] = None
        current["candidate_observations"] = 0
        return current, None

    if current.get("candidate_regime") == target.value:
        observations = int(current.get("candidate_observations") or 0) + 1
    else:
        observations = 1
    current["candidate_regime"] = target.value
    current["candidate_observations"] = observations
    if observations < policy.confirmation_observations:
        return current, None

    sequence = int(current.get("transition_sequence") or 0) + 1
    event = {
        "event_id": f"globex-trend:{session_id}:{sequence}:{target.value}",
        "session_id": session_id,
        "sequence": sequence,
        "from_regime": regime.value,
        "to_regime": target.value,
        "reason": reason,
        "at": at.isoformat(),
        "source_at": source_at.isoformat(),
        "price": float(price),
        "provider": provider,
        "metrics": metrics,
    }
    current["regime"] = target.value
    current["candidate_regime"] = None
    current["candidate_observations"] = 0
    current["transition_sequence"] = sequence
    current["regime_started_at"] = at.isoformat()
    current["regime_high"] = float(price)
    current["regime_low"] = float(price)
    current["last_transition"] = event
    current["pending_event"] = event
    return current, event


def compute_metrics(
    samples: list[dict[str, Any]],
    *,
    policy: GlobexTrendSettings,
) -> dict[str, float | None]:
    if not samples:
        return {}
    latest = samples[-1]
    at = datetime.fromisoformat(str(latest["at"]))
    price = float(latest["price"])
    prices = [float(row["price"]) for row in samples]
    return {
        "price": price,
        "return_15m_points": _horizon_return(
            samples, at=at, price=price, minutes=policy.short_horizon_minutes
        ),
        "return_60m_points": _horizon_return(
            samples, at=at, price=price, minutes=policy.medium_horizon_minutes
        ),
        "return_180m_points": _horizon_return(
            samples, at=at, price=price, minutes=policy.long_horizon_minutes
        ),
        "session_high": max(prices),
        "session_low": min(prices),
        "drawdown_from_high_points": price - max(prices),
        "rebound_from_low_points": price - min(prices),
    }


def target_regime(
    regime: GlobexTrendRegime,
    metrics: dict[str, float | None],
    *,
    policy: GlobexTrendSettings,
) -> tuple[GlobexTrendRegime | None, str | None]:
    short = metrics.get("return_15m_points")
    medium = metrics.get("return_60m_points")
    long = metrics.get("return_180m_points")
    rebound = metrics.get("rebound_from_regime_low_points")
    drawdown = metrics.get("drawdown_from_regime_high_points")

    if (
        regime is GlobexTrendRegime.BEARISH
        and short is not None
        and rebound is not None
        and short >= policy.short_move_points
        and rebound >= policy.reversal_points
    ):
        return GlobexTrendRegime.BULLISH, "confirmed_reversal_from_regime_low"
    if (
        regime is GlobexTrendRegime.BULLISH
        and short is not None
        and drawdown is not None
        and short <= -policy.short_move_points
        and drawdown <= -policy.reversal_points
    ):
        return GlobexTrendRegime.BEARISH, "confirmed_reversal_from_regime_high"

    if regime is not GlobexTrendRegime.NEUTRAL:
        return None, None

    bearish = bool(
        (medium is not None and medium <= -policy.medium_move_points)
        or (long is not None and long <= -policy.long_move_points)
    ) and (short is None or short <= 0)
    bullish = bool(
        (medium is not None and medium >= policy.medium_move_points)
        or (long is not None and long >= policy.long_move_points)
    ) and (short is None or short >= 0)
    if bearish:
        return GlobexTrendRegime.BEARISH, "multi_horizon_downtrend"
    if bullish:
        return GlobexTrendRegime.BULLISH, "multi_horizon_uptrend"
    return None, None


def _samples(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = state.get("samples")
    return [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _update_regime_extrema(state: dict[str, Any], *, price: float) -> None:
    high = state.get("regime_high")
    low = state.get("regime_low")
    state["regime_high"] = max(float(high), price) if isinstance(high, int | float) else price
    state["regime_low"] = min(float(low), price) if isinstance(low, int | float) else price


def _horizon_return(
    samples: list[dict[str, Any]],
    *,
    at: datetime,
    price: float,
    minutes: int,
) -> float | None:
    target = at - timedelta(minutes=minutes)
    candidates = [row for row in samples[:-1] if datetime.fromisoformat(str(row["at"])) <= target]
    if not candidates:
        return None
    reference = max(candidates, key=lambda row: datetime.fromisoformat(str(row["at"])))
    reference_at = datetime.fromisoformat(str(reference["at"]))
    tolerance = max(180.0, minutes * 60.0 * 0.20)
    if (target - reference_at).total_seconds() > tolerance:
        return None
    return price - float(reference["price"])
