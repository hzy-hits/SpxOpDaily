"""Pure multi-horizon ES Globex trend state machine."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

from spx_spark.application.globex_trend.models import GlobexTrendRegime
from spx_spark.settings.globex_trend import GlobexTrendSettings


GTH_DIRECTIONAL_ADVISORY_CONTRACT_VERSION = "gth_directional_advisory.v1"
ADVANCE_HOLD_BUFFER_POINTS = 1.5


def initial_state(session_id: str) -> dict[str, Any]:
    return {
        "version": 2,
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
        "continuation_milestone_index": 0,
        "continuation_candidate_index": None,
        "continuation_candidate_observations": 0,
        "continuation_candidate_provider": None,
        "continuation_events_in_context": 0,
        "last_continuation_at": None,
        "last_continuation": None,
        "last_advance": None,
        "last_advance_at": None,
        "advance_milestone_index": 0,
        "advance_candidate_index": None,
        "advance_candidate_observations": 0,
        "advance_events_in_session": 0,
        "pending_directional_advisory_id": None,
        "active_directional_advisory_id": None,
        "active_directional_advisory_accepted_at": None,
        "continuation_migration_budget_consumed": False,
        "continuation_suppressed_reason": None,
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
    continuation_allowed: bool = True,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    current = (
        deepcopy(state) if state.get("session_id") == session_id else initial_state(session_id)
    )
    _migrate_continuation_state(current, policy=policy)
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
    metrics["drawdown_from_regime_high_points"] = float(price) - float(current["regime_high"])
    metrics["rebound_from_regime_low_points"] = float(price) - float(current["regime_low"])
    current["metrics"] = metrics
    current["updated_at"] = at.isoformat()

    regime = GlobexTrendRegime(str(current.get("regime") or "neutral"))
    target, reason = target_regime(regime, metrics, policy=policy)
    if target is None or target is regime:
        current["candidate_regime"] = None
        current["candidate_observations"] = 0
        if regime is GlobexTrendRegime.NEUTRAL:
            event = _advance_session_trend(
                current,
                at=at,
                price=float(price),
                provider=provider,
                source_at=source_at,
                allowed=continuation_allowed,
                policy=policy,
            )
        else:
            event = _advance_continuation(
                current,
                at=at,
                price=float(price),
                provider=provider,
                source_at=source_at,
                regime=regime,
                allowed=continuation_allowed,
                policy=policy,
            )
        return current, event

    _reset_continuation_candidate(current)
    if current.get("candidate_regime") == target.value:
        observations = int(current.get("candidate_observations") or 0) + 1
    else:
        observations = 1
    current["candidate_regime"] = target.value
    current["candidate_observations"] = observations
    if observations < policy.confirmation_observations:
        return current, None

    sequence = int(current.get("transition_sequence") or 0) + 1
    invalidated_advisory_id = str(current.get("active_directional_advisory_id") or "") or None
    event = {
        "event_type": "transition",
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
        "automatic_ordering": False,
        "operator_action": "observe_only",
        "invalidated_advisory_id": invalidated_advisory_id,
        "advisory_lifecycle_action": (
            "invalidated" if invalidated_advisory_id is not None else None
        ),
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
    current["continuation_milestone_index"] = 0
    _reset_continuation_candidate(current)
    current["last_continuation_at"] = None
    current["pending_directional_advisory_id"] = None
    current["active_directional_advisory_id"] = None
    current["active_directional_advisory_accepted_at"] = None
    if current.get("continuation_migration_budget_consumed") is True:
        current["continuation_events_in_context"] = 0
        current["continuation_migration_budget_consumed"] = False
    current["continuation_suppressed_reason"] = None
    return current, event


def _migrate_continuation_state(
    state: dict[str, Any],
    *,
    policy: GlobexTrendSettings,
) -> None:
    if "continuation_milestone_index" in state:
        state.setdefault("last_continuation", None)
        state.setdefault("last_advance", None)
        state.setdefault("last_advance_at", None)
        state.setdefault("advance_milestone_index", 0)
        state.setdefault("advance_candidate_index", None)
        state.setdefault("advance_candidate_observations", 0)
        state.setdefault("advance_events_in_session", 0)
        state.setdefault("pending_directional_advisory_id", None)
        state.setdefault("active_directional_advisory_id", None)
        state.setdefault("active_directional_advisory_accepted_at", None)
        state.setdefault(
            "continuation_migration_budget_consumed",
            bool(
                state.get("continuation_suppressed_reason") == "legacy_active_context_no_hindsight"
                and int(state.get("continuation_events_in_context") or 0)
                >= policy.continuation_session_budget
            ),
        )
        return
    # Deployment must not emit a hindsight continuation for a trend that was
    # already extended before this policy existed.  Consume the legacy active
    # context; the next formal transition/session starts cleanly.
    active = state.get("regime") in {
        GlobexTrendRegime.BULLISH.value,
        GlobexTrendRegime.BEARISH.value,
    } and isinstance(state.get("last_transition"), dict)
    consumed = policy.continuation_session_budget if active else 0
    state.update(
        {
            "version": 2,
            "continuation_milestone_index": consumed,
            "continuation_candidate_index": None,
            "continuation_candidate_observations": 0,
            "continuation_candidate_provider": None,
            "continuation_events_in_context": consumed,
            "last_continuation_at": None,
            "last_continuation": None,
            "last_advance": None,
            "last_advance_at": None,
            "advance_milestone_index": 0,
            "advance_candidate_index": None,
            "advance_candidate_observations": 0,
            "advance_events_in_session": 0,
            "pending_directional_advisory_id": None,
            "active_directional_advisory_id": None,
            "active_directional_advisory_accepted_at": None,
            "continuation_migration_budget_consumed": active,
            "continuation_suppressed_reason": (
                "legacy_active_context_no_hindsight" if active else None
            ),
        }
    )


def _advance_session_trend(
    state: dict[str, Any],
    *,
    at: datetime,
    price: float,
    provider: str,
    source_at: datetime,
    allowed: bool,
    policy: GlobexTrendSettings,
) -> dict[str, Any] | None:
    """Confirm a no-pullback grind while Globex is still NEUTRAL.

    Established bullish/bearish legs keep using continuation-from-transition.
    A 3-point impulse flip is a transition and must not authorize here.
    """

    metrics = state.get("metrics") if isinstance(state.get("metrics"), dict) else {}
    session_high = metrics.get("session_high")
    session_low = metrics.get("session_low")
    return_15m = metrics.get("return_15m_points")
    return_60m = metrics.get("return_60m_points")
    return_180m = metrics.get("return_180m_points")
    if (
        not isinstance(session_high, int | float)
        or not isinstance(session_low, int | float)
        or not isinstance(return_15m, int | float)
        or int(state.get("advance_events_in_session") or 0)
        >= policy.continuation_session_budget
    ):
        _reset_advance_candidate(state)
        return None

    rebound = float(price) - float(session_low)
    drawdown = float(session_high) - float(price)
    up_ok = (
        return_15m >= 0
        and (return_60m is None or return_60m >= 0)
        and (return_180m is None or return_180m >= 0)
        and rebound >= policy.continuation_step_points
        and drawdown <= ADVANCE_HOLD_BUFFER_POINTS
    )
    down_ok = (
        return_15m <= 0
        and (return_60m is None or return_60m <= 0)
        and (return_180m is None or return_180m <= 0)
        and drawdown >= policy.continuation_step_points
        and rebound <= ADVANCE_HOLD_BUFFER_POINTS
    )
    if up_ok:
        direction = "up"
        extension = rebound
        anchor = float(session_low)
    elif down_ok:
        direction = "down"
        extension = drawdown
        anchor = float(session_high)
    else:
        _reset_advance_candidate(state)
        return None

    last_at_raw = state.get("last_advance_at")
    if isinstance(last_at_raw, str):
        try:
            last_at = datetime.fromisoformat(last_at_raw)
        except ValueError:
            last_at = None
        if (
            last_at is not None
            and (at - last_at).total_seconds() < policy.continuation_cooldown_seconds
        ):
            _reset_advance_candidate(state)
            return None

    milestone_index = int(state.get("advance_milestone_index") or 0) + 1
    if extension < policy.continuation_step_points * milestone_index:
        _reset_advance_candidate(state)
        return None
    if not allowed:
        state["advance_milestone_index"] = milestone_index
        _reset_advance_candidate(state)
        return None

    same_candidate = state.get("advance_candidate_index") == milestone_index
    observations = (
        int(state.get("advance_candidate_observations") or 0) + 1 if same_candidate else 1
    )
    state["advance_candidate_index"] = milestone_index
    state["advance_candidate_observations"] = observations
    if observations < policy.continuation_confirmation_observations:
        return None

    option_right = "C" if direction == "up" else "P"
    event = {
        "event_type": "advance",
        "event_id": (
            f"globex-advance:{state['session_id']}:{direction}:m{milestone_index}"
        ),
        "session_id": state["session_id"],
        "sequence": milestone_index,
        "regime": "bullish" if direction == "up" else "bearish",
        "direction": direction,
        "milestone_index": milestone_index,
        "anchor_price": anchor,
        "extension_points": extension,
        "threshold_points": policy.continuation_step_points * milestone_index,
        "at": at.isoformat(),
        "source_at": source_at.isoformat(),
        "price": float(price),
        "provider": provider,
        "metrics": dict(metrics),
        "advisory_contract_version": GTH_DIRECTIONAL_ADVISORY_CONTRACT_VERSION,
        "advisory_id": f"gth-advance:{state['session_id']}:{direction}:m{milestone_index}",
        "advisory_status": "advisory_ready",
        "signal_stage": "entry_advisory",
        "option_right": option_right,
        "direction_source": "session_extreme_hold",
        "signal_coordinate": {
            "kind": "future",
            "instrument_id": "future:ES",
        },
        "option_coordinate_status": "not_authorized_from_es_direction",
        "quote_attachment_status": "direction_only",
        "parent_advisory_id": None,
        "contract_id": None,
        "entry_limit": None,
        "execution_eligible": False,
        "automatic_ordering": False,
        "operator_action": (
            "evaluate_call_setup" if option_right == "C" else "evaluate_put_setup"
        ),
        "execution_block_reasons": [
            "gth_directional_advisory_only",
            "exact_same_coordinate_option_expression_required",
            "rth_trade_ready_authority_not_reused",
        ],
        "operator_action_note": "gth_trend_advance",
    }
    state["advance_milestone_index"] = milestone_index
    state["advance_events_in_session"] = int(state.get("advance_events_in_session") or 0) + 1
    state["last_advance_at"] = at.isoformat()
    state["last_advance"] = event
    state["pending_event"] = event
    _reset_advance_candidate(state)
    return event


def _advance_continuation(
    state: dict[str, Any],
    *,
    at: datetime,
    price: float,
    provider: str,
    source_at: datetime,
    regime: GlobexTrendRegime,
    allowed: bool,
    policy: GlobexTrendSettings,
) -> dict[str, Any] | None:
    transition = state.get("last_transition")
    if (
        regime is GlobexTrendRegime.NEUTRAL
        or not isinstance(transition, dict)
        or int(state.get("continuation_events_in_context") or 0)
        >= policy.continuation_session_budget
        or int(state.get("continuation_milestone_index") or 0)
        >= policy.continuation_max_milestones_per_transition
    ):
        _reset_continuation_candidate(state)
        return None
    anchor = transition.get("price")
    sequence = transition.get("sequence")
    if not isinstance(anchor, int | float) or not isinstance(sequence, int):
        _reset_continuation_candidate(state)
        return None

    milestone_index = int(state.get("continuation_milestone_index") or 0) + 1
    direction = "up" if regime is GlobexTrendRegime.BULLISH else "down"
    signed_extension = (
        price - float(anchor) if regime is GlobexTrendRegime.BULLISH else float(anchor) - price
    )
    crossed = signed_extension >= policy.continuation_step_points * milestone_index
    if not crossed:
        _reset_continuation_candidate(state)
        return None

    last_at_raw = state.get("last_continuation_at")
    if isinstance(last_at_raw, str):
        try:
            last_at = datetime.fromisoformat(last_at_raw)
        except ValueError:
            last_at = None
        if (
            last_at is not None
            and (at - last_at).total_seconds() < policy.continuation_cooldown_seconds
        ):
            _reset_continuation_candidate(state)
            return None

    if not allowed:
        state["continuation_milestone_index"] = milestone_index
        state["continuation_suppressed_reason"] = "continuation_gate_closed"
        _reset_continuation_candidate(state)
        return None

    same_candidate = state.get("continuation_candidate_index") == milestone_index
    observations = (
        int(state.get("continuation_candidate_observations") or 0) + 1 if same_candidate else 1
    )
    state["continuation_candidate_index"] = milestone_index
    state["continuation_candidate_observations"] = observations
    state["continuation_candidate_provider"] = provider
    if observations < policy.continuation_confirmation_observations:
        return None

    option_right = "C" if direction == "up" else "P"
    advisory_id = f"gth-advisory:{state['session_id']}:{sequence}:{direction}"
    if milestone_index == 1:
        signal_stage = "entry_advisory"
        operator_action = "evaluate_call_setup" if option_right == "C" else "evaluate_put_setup"
        parent_advisory_id = None
    else:
        parent_advisory_id = str(state.get("active_directional_advisory_id") or "") or None
        if parent_advisory_id is None:
            state["continuation_suppressed_reason"] = "management_without_accepted_entry_advisory"
            _reset_continuation_candidate(state)
            return None
        signal_stage = "opportunity_management"
        operator_action = "conditional_take_profit_or_raise_stop"

    event = {
        "event_type": "continuation",
        "event_id": (
            f"globex-cont:{state['session_id']}:{sequence}:{direction}:m{milestone_index}"
        ),
        "session_id": state["session_id"],
        "sequence": sequence,
        "regime": regime.value,
        "direction": direction,
        "milestone_index": milestone_index,
        "anchor_price": float(anchor),
        "extension_points": signed_extension,
        "threshold_points": policy.continuation_step_points * milestone_index,
        "at": at.isoformat(),
        "source_at": source_at.isoformat(),
        "price": price,
        "provider": provider,
        "metrics": dict(state.get("metrics") or {}),
        "advisory_contract_version": GTH_DIRECTIONAL_ADVISORY_CONTRACT_VERSION,
        "advisory_id": advisory_id,
        "advisory_status": "advisory_ready",
        "signal_stage": signal_stage,
        "option_right": option_right,
        "direction_source": "direct_live_es",
        "signal_coordinate": {
            "kind": "future",
            "instrument_id": "future:ES",
        },
        "option_coordinate_status": "not_authorized_from_es_direction",
        "quote_attachment_status": "direction_only",
        "parent_advisory_id": parent_advisory_id,
        "contract_id": None,
        "entry_limit": None,
        "execution_eligible": False,
        "automatic_ordering": False,
        "operator_action": operator_action,
        "execution_block_reasons": [
            "gth_directional_advisory_only",
            "exact_same_coordinate_option_expression_required",
            "rth_trade_ready_authority_not_reused",
        ],
    }
    if milestone_index == 1:
        state["pending_directional_advisory_id"] = advisory_id
    state["continuation_milestone_index"] = milestone_index
    state["continuation_events_in_context"] = (
        int(state.get("continuation_events_in_context") or 0) + 1
    )
    state["last_continuation_at"] = at.isoformat()
    state["last_continuation"] = event
    state["continuation_suppressed_reason"] = None
    state["pending_event"] = event
    _reset_continuation_candidate(state)
    return event


def _reset_advance_candidate(state: dict[str, Any]) -> None:
    state["advance_candidate_index"] = None
    state["advance_candidate_observations"] = 0


def _reset_continuation_candidate(state: dict[str, Any]) -> None:
    state["continuation_candidate_index"] = None
    state["continuation_candidate_observations"] = 0
    state["continuation_candidate_provider"] = None


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

    if medium is None and long is None and short is not None:
        if short <= -policy.short_move_points:
            return GlobexTrendRegime.BEARISH, "initial_short_impulse"
        if short >= policy.short_move_points:
            return GlobexTrendRegime.BULLISH, "initial_short_impulse"

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
