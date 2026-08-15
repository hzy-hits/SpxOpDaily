"""Hard-gate and rank strategy candidates."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import json
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.density import summarize_strike_surface_shape
from spx_spark.analytics.options.strategy_payoff import (
    debit_vertical_reach_reasons,
    vertical_entry_quality,
    vertical_width_path_reasons,
)
from spx_spark.application.market_features.physical_followthrough import (
    estimate_physical_terminal_range,
)
from spx_spark.application.order_map.strategy_regime import (
    StrategyPolicy,
    butterfly_max_entry_minutes,
)
from spx_spark.settings.strategy_distribution import StrategyDistributionSettings

_POLICY_EV_TABLE_PATH = ("research", "policy_ev_table.v1.json")
_POLICY_EV_SCHEMA_VERSION = "policy_ev_table.v1"
_EVENT_SETTLEMENT_SETUP = "EVENT_SETTLEMENT_THRESHOLD"
_GTH_WIDTH_SCAN = "GTH_WIDTH_SCAN"
_GTH_DELTA_SCAN = "GTH_DELTA_SCAN"
_GTH_ATM_PIN = "GTH_ATM_PIN"
_IRON_CONDOR_TYPE = "IRON_CONDOR"
_EVENT_SETTLEMENT_MAX_DEBIT_FRACTION = 0.50


@dataclass(frozen=True, slots=True)
class RankResult:
    passed: list[dict[str, Any]]
    near_misses: list[dict[str, Any]]
    gate_audit: list[dict[str, Any]]


@dataclass(frozen=True, slots=True)
class GthDirectionLock:
    direction: str
    opportunity_id: str
    started_at: datetime


def gth_direction_lock(
    cards: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    stick_seconds: float,
) -> GthDirectionLock | None:
    """Return the active GTH direction lock, or None once hysteresis expires.

    The lock starts at the earliest card in the latest same-direction GTH
    streak. Later reprints of the same winner do not extend it.
    """

    if stick_seconds <= 0:
        return None
    now = _utc(now)
    gth_cards = []
    for row in cards:
        mode = str(row.get("session_mode") or "").strip().lower()
        direction = str(row.get("direction") or "").strip().upper()
        opportunity_id = str(row.get("opportunity_id") or "").strip()
        decision_at = row.get("decision_at")
        if mode != "gth" or not direction or not opportunity_id:
            continue
        if not isinstance(decision_at, datetime):
            continue
        gth_cards.append(
            {
                "direction": direction,
                "opportunity_id": opportunity_id,
                "decision_at": _utc(decision_at),
            }
        )
    if not gth_cards:
        return None
    gth_cards.sort(key=lambda item: item["decision_at"], reverse=True)
    latest = gth_cards[0]
    started_at = latest["decision_at"]
    for row in gth_cards[1:]:
        if row["direction"] != latest["direction"]:
            break
        started_at = row["decision_at"]
    if now >= started_at + timedelta(seconds=stick_seconds):
        return None
    return GthDirectionLock(
        direction=str(latest["direction"]),
        opportunity_id=str(latest["opportunity_id"]),
        started_at=started_at,
    )


def outbox_accepted_strategy_cards(
    rows: Sequence[Mapping[str, Any]],
    *,
    event_exists: Callable[[str], bool],
    exclude_opportunity_id: str = "",
) -> tuple[dict[str, Any], ...]:
    """Keep one row per opportunity that actually reached the outbox.

    Rank stick and delivery flood control share this filter. A decision
    persisted as ``selected`` but never accepted as ``{opportunity}:ready``
    must not lock the desk or consume quota.
    """

    accepted: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_opportunity = str(row.get("opportunity_id") or "")
        if not row_opportunity or row_opportunity == exclude_opportunity_id:
            continue
        known = accepted.get(row_opportunity)
        if known is not None:
            if row["decision_at"] > known["decision_at"]:
                accepted[row_opportunity] = dict(row)
            continue
        if event_exists(f"{row_opportunity}:ready"):
            accepted[row_opportunity] = dict(row)
    return tuple(accepted.values())


def apply_gth_winner_stick(
    passed: list[dict[str, Any]],
    lock: GthDirectionLock | None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Keep the GTH winner/direction during the stick window.

    Prefer the locked opportunity if it still passes. Otherwise keep the best
    remaining candidate in that direction. Opposite directions wait until the
    lock expires rather than replacing the human card.
    """

    if lock is None or not passed:
        return passed, None
    locked_direction = lock.direction.upper()
    matching = [
        row
        for row in passed
        if str(row.get("opportunity_id") or "") == lock.opportunity_id
    ]
    if matching:
        rest = [
            row
            for row in passed
            if str(row.get("opportunity_id") or "") != lock.opportunity_id
        ]
        return [matching[0], *rest], None
    same_direction = [
        row
        for row in passed
        if str(row.get("direction") or "").upper() == locked_direction
    ]
    if same_direction:
        others = [
            row
            for row in passed
            if str(row.get("direction") or "").upper() != locked_direction
        ]
        return [*same_direction, *others], None
    return [], "gth_winner_stick_direction_locked"


def rank_candidates(
    rows: list[dict[str, Any]],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    policy: StrategyPolicy,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
) -> RankResult:
    now = _utc(now)
    surface_shape = summarize_strike_surface_shape(
        _map(_map(facts.get("structure")).get("strike_differential_context"))
    )
    passed: list[dict[str, Any]] = []
    misses: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for row in rows:
        candidate = dict(row)
        deterministic = _hard_gate_candidate(
            candidate, facts, regime, now=now, policy=policy
        )
        if deterministic:
            rejected = _rejected(candidate, deterministic)
            misses.append(rejected)
            audit.append(_audit_row(rejected))
            continue
        candidate = _apply_surface_shape_prior(candidate, surface_shape)
        scored, utility_gates = _score_candidate(
            candidate,
            facts,
            regime,
            data_root=data_root,
            probability_settings=probability_settings,
            now=now,
        )
        # V3-3a: research utility never vetoes. Advisories stay on candidate.edge.
        if utility_gates:
            scored = {
                **scored,
                "edge": {
                    **dict(_map(scored.get("edge"))),
                    "advisories": list(
                        dict.fromkeys(
                            [
                                *(
                                    str(item)
                                    for item in _map(scored.get("edge")).get("advisories")
                                    or ()
                                ),
                                *(
                                    str(gate.get("gate"))
                                    for gate in utility_gates
                                    if gate.get("gate")
                                ),
                            ]
                        )
                    ),
                },
            }
        scored = _attach_policy_ev(
            scored,
            regime,
            data_root=data_root,
        )
        passed.append(scored)
        audit.append(_audit_row(scored))

    # Deterministic structure/friction score picks the winner. The uncalibrated
    # research utility (P/Q bootstrap) is display and tie-break only until the
    # ManagementPolicy EV model passes its promotion gate.
    passed.sort(
        key=lambda item: (
            float(item.get("selection_score") or 0.0),
            float(_map(item.get("utility")).get("utility") or 0.0),
        ),
        reverse=True,
    )
    misses.sort(key=_miss_sort_key, reverse=True)
    return RankResult(passed=passed, near_misses=misses[:3], gate_audit=audit)


def _apply_surface_shape_prior(
    candidate: Mapping[str, Any], summary: Mapping[str, object]
) -> dict[str, Any]:
    base = float(candidate.get("selection_score") or 0.0)
    strategy_type = str(candidate.get("strategy_type") or "")
    prior = 0.0
    if summary.get("snr_quality") == "high":
        if strategy_type.endswith("_BUTTERFLY"):
            prior = 0.025 if summary.get("d4_shape") == "peaked" else 0.0
        elif strategy_type == _IRON_CONDOR_TYPE:
            prior = 0.05 if summary.get("d4_shape") == "peaked" else 0.0
        elif strategy_type.startswith("CALL_") and summary.get("d3_sign") == "up":
            prior = 0.05
        elif strategy_type.startswith("PUT_") and summary.get("d3_sign") == "down":
            prior = 0.05
    prior = min(max(prior, -0.05), 0.05)
    return {
        **dict(candidate),
        "selection_score_base": round(base, 4),
        "surface_shape_prior": round(prior, 4),
        "selection_score": round(base + prior, 4),
    }


def _hard_gate_candidate(
    candidate: dict[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []
    quote = _map(candidate.get("quote"))
    if quote.get("status") != "ready":
        for reason in quote.get("reasons") or ("quote_not_ready",):
            gates.append({"gate": str(reason), "actual": quote.get("status"), "threshold": "ready"})
    for key in ("quote_valid_until", "opportunity_valid_until"):
        valid_until = _time(candidate.get(key))
        if valid_until is None:
            gates.append({"gate": f"{key}_missing", "actual": None, "threshold": "present"})
        elif valid_until <= now:
            gates.append({"gate": f"{key}_expired", "actual": valid_until.isoformat(), "threshold": now.isoformat()})
    gates.extend(_macro_hard_gates(candidate, facts))
    strategy_type = str(candidate.get("strategy_type") or "")
    if strategy_type.endswith("_DEBIT_VERTICAL"):
        gates.extend(_vertical_hard_gates(candidate, facts, regime, policy=policy))
    elif strategy_type.endswith("_BUTTERFLY"):
        gates.extend(_butterfly_hard_gates(candidate, facts, regime, policy=policy))
    elif strategy_type == _IRON_CONDOR_TYPE:
        gates.extend(_iron_condor_hard_gates(candidate, facts))
    else:
        gates.append({"gate": "unsupported_strategy_type", "actual": strategy_type, "threshold": "approved_strategy"})
    if candidate.get("automatic_ordering") is not False or candidate.get("manual_action_only") is not True:
        gates.append({"gate": "manual_action_contract", "actual": candidate.get("automatic_ordering"), "threshold": False})
    if candidate.get("manual_authority_eligible") is False:
        gates.append({
            "gate": "research_alternative_only",
            "actual": candidate.get("source"),
            "threshold": "manual_authority_eligible",
        })
    return gates


def _macro_hard_gates(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
) -> list[dict[str, Any]]:
    event = _map(facts.get("event"))
    if not event or event.get("entry_allowed") is not False:
        return []
    if (
        candidate.get("setup_kind") == _EVENT_SETTLEMENT_SETUP
        and candidate.get("event_spans_release") is True
    ):
        return []
    return [
        {
            "gate": "macro_entry_not_authorized",
            "actual": event.get("state"),
            "threshold": "entry_allowed_or_explicit_event_settlement_view",
        }
    ]


def _vertical_hard_gates(
    candidate: dict[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    if candidate.get("setup_kind") == _EVENT_SETTLEMENT_SETUP:
        return _event_settlement_vertical_hard_gates(candidate)
    if candidate.get("setup_kind") in {_GTH_WIDTH_SCAN, _GTH_DELTA_SCAN}:
        return _gth_scan_vertical_hard_gates(candidate, facts, regime, policy=policy)
    long, short = _map(candidate.get("long")), _map(candidate.get("short"))
    if not long or not short:
        return [{"gate": "vertical_legs_unavailable", "actual": None, "threshold": "long_and_short"}]
    economics, path = _map(candidate.get("economics")), _map(facts.get("path"))
    spot = _number(_map(facts.get("spot")).get("spx"))
    atr = _number(path.get("atr_5m"))
    target = _number(candidate.get("target_spx"))
    stop = _number(candidate.get("invalidation_spx"))
    debit_fraction = _number(economics.get("debit_fraction_of_width"))
    if None in (spot, atr, target, stop, debit_fraction):
        return [{"gate": "entry_quality_atr_or_geometry_unavailable", "actual": None, "threshold": "present"}]
    long_strike = _number(long.get("strike"))
    short_strike = _number(short.get("strike"))
    remaining_move = _number(_map(facts.get("volatility")).get("expected_move_points"))
    if long_strike is None or short_strike is None:
        return [{"gate": "vertical_legs_unavailable", "actual": None, "threshold": "long_and_short"}]
    right = _vertical_right(candidate, long, long_strike=long_strike, short_strike=short_strike)
    path_reasons = vertical_width_path_reasons(
        long_strike=long_strike,
        short_strike=short_strike,
        right=right,
        target=target,
        remaining_expected_move=remaining_move,
    )
    if path_reasons:
        return [
            _width_path_gate(
                reason,
                long_strike=long_strike,
                short_strike=short_strike,
                target=target,
                remaining_expected_move=remaining_move,
            )
            for reason in path_reasons
        ]
    entry_quality, reasons = vertical_entry_quality(
        spot=float(spot),
        atr=float(atr),
        target=float(target),
        stop=float(stop),
        trigger=_number(candidate.get("trigger_level")),
        direction=str(candidate.get("direction")),
        setup_kind=str(candidate.get("setup_kind")),
        distance_to_vwap_points=_number(path.get("distance_to_vwap_points")),
        impulse_15m_points=_number(path.get("impulse_15m_points")),
        debit_fraction=float(debit_fraction),
        thresholds=policy.entry_quality_kwargs(),
    )
    candidate["entry_quality"] = entry_quality
    if "direction_valid_but_entry_too_late" in reasons:
        candidate["setup_state"] = "ENTRY_TOO_LATE"
    return [
        _gate_from_entry_reason(
            reason,
            entry_quality,
            policy,
            setup_kind=str(candidate.get("setup_kind") or ""),
        )
        for reason in reasons
    ]


def _gth_scan_vertical_hard_gates(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    long, short = _map(candidate.get("long")), _map(candidate.get("short"))
    if not long or not short:
        return [{"gate": "vertical_legs_unavailable", "actual": None, "threshold": "long_and_short"}]
    economics = _map(candidate.get("economics"))
    target = _number(candidate.get("target_spx"))
    stop = _number(candidate.get("invalidation_spx"))
    debit_fraction = _number(economics.get("debit_fraction_of_width"))
    long_strike = _number(long.get("strike"))
    short_strike = _number(short.get("strike"))
    if None in (target, stop, debit_fraction, long_strike, short_strike):
        return [{"gate": "gth_scan_geometry_or_payoff_unavailable", "actual": None, "threshold": "present"}]
    gates: list[dict[str, Any]] = []
    path_state = str(regime.get("path_state") or "")
    path_direction = str(regime.get("path_direction") or "").upper()
    direction = str(candidate.get("direction") or "").upper()
    if path_state != "TREND" or path_direction != direction:
        gates.append(
            {
                "gate": "gth_vertical_requires_aligned_trend",
                "actual": f"{path_state}:{path_direction or 'none'}",
                "threshold": f"TREND:{direction or 'directional'}",
            }
        )
    remaining_move = _number(_map(facts.get("volatility")).get("expected_move_points"))
    right = _vertical_right(candidate, long, long_strike=long_strike, short_strike=short_strike)
    if candidate.get("setup_kind") == _GTH_DELTA_SCAN:
        spot = _number(_map(facts.get("spot")).get("spx"))
        if spot is None:
            return [{"gate": "spx_price_unavailable", "actual": None, "threshold": "present"}]
        reach_reasons = debit_vertical_reach_reasons(
            spot=float(spot),
            long_strike=float(long_strike),
            short_strike=float(short_strike),
            right=right,
            remaining_expected_move=remaining_move,
        )
        gates.extend(
            _width_path_gate(
                reason,
                long_strike=float(long_strike),
                short_strike=float(short_strike),
                target=target,
                remaining_expected_move=remaining_move,
            )
            for reason in reach_reasons
        )
        delta_cap = max(policy.gth_delta_targets) if policy.gth_delta_targets else 0.20
        long_delta = _number(long.get("delta"))
        abs_long_delta = None if long_delta is None else abs(long_delta)
        if abs_long_delta is None or abs_long_delta > delta_cap:
            gates.append(
                {
                    "gate": "gth_delta_scan_long_above_cap",
                    "actual": abs_long_delta,
                    "threshold": delta_cap,
                }
            )
    else:
        path_reasons = vertical_width_path_reasons(
            long_strike=float(long_strike),
            short_strike=float(short_strike),
            right=right,
            target=target,
            remaining_expected_move=remaining_move,
        )
        gates.extend(
            _width_path_gate(
                reason,
                long_strike=float(long_strike),
                short_strike=float(short_strike),
                target=target,
                remaining_expected_move=remaining_move,
            )
            for reason in path_reasons
        )
    if float(debit_fraction) > policy.max_debit_fraction:
        gates.append(
            {
                "gate": "max_debit_fraction_exceeded",
                "actual": debit_fraction,
                "threshold": policy.max_debit_fraction,
            }
        )
    return gates


def _event_settlement_vertical_hard_gates(
    candidate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []
    long, short = _map(candidate.get("long")), _map(candidate.get("short"))
    if not long or not short:
        gates.append(
            {
                "gate": "vertical_legs_unavailable",
                "actual": None,
                "threshold": "long_and_short",
            }
        )
    economics = _map(candidate.get("economics"))
    width = _number(economics.get("width_points"))
    debit_fraction = _number(economics.get("debit_fraction_of_width"))
    breakeven = _number(economics.get("breakeven_spx"))
    if width is None or width <= 0.0 or debit_fraction is None or breakeven is None:
        gates.append(
            {
                "gate": "event_settlement_payoff_unavailable",
                "actual": dict(economics),
                "threshold": "valid_vertical_economics",
            }
        )
    elif not 0.0 < debit_fraction < 1.0:
        gates.append(
            {
                "gate": "event_settlement_odds_invalid",
                "actual": debit_fraction,
                "threshold": "0<debit_fraction<1",
            }
        )
    elif debit_fraction > _EVENT_SETTLEMENT_MAX_DEBIT_FRACTION:
        gates.append(
            {
                "gate": "event_settlement_debit_fraction_exceeded",
                "actual": debit_fraction,
                "threshold": f"<={_EVENT_SETTLEMENT_MAX_DEBIT_FRACTION}",
            }
        )
    view = _map(candidate.get("view"))
    event = _map(candidate.get("probability_event"))
    direction = str(candidate.get("direction") or "")
    threshold = _number(view.get("threshold_level"))
    expected_kind = "terminal_above" if direction == "UP" else "terminal_below" if direction == "DOWN" else None
    event_threshold = _number(
        event.get("lower_level") if direction == "UP" else event.get("upper_level")
    )
    if (
        expected_kind is None
        or event.get("kind") != expected_kind
        or threshold is None
        or event_threshold is None
        or abs(threshold - event_threshold) > 1e-9
        or _time(event.get("target_at")) is None
    ):
        gates.append(
            {
                "gate": "event_settlement_proposition_invalid",
                "actual": dict(event),
                "threshold": expected_kind,
            }
        )
    if candidate.get("event_spans_release") is not True:
        gates.append(
            {
                "gate": "event_settlement_release_identity_missing",
                "actual": candidate.get("event_spans_release"),
                "threshold": True,
            }
        )
    return gates


def _butterfly_hard_gates(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    if candidate.get("setup_kind") == _GTH_ATM_PIN:
        return _gth_scan_butterfly_hard_gates(candidate, facts, policy=policy)
    gates: list[dict[str, Any]] = []
    legs = candidate.get("legs")
    economics = _map(candidate.get("economics"))
    if not isinstance(legs, list) or len(legs) != 3 or any(not _map(leg) for leg in legs):
        gates.append({"gate": "butterfly_three_leg_bbo_unavailable", "actual": None, "threshold": "three_legs"})
    if _number(economics.get("max_loss_points")) is None or _number(economics.get("max_gain_points")) is None:
        gates.append({"gate": "butterfly_economics_unavailable", "actual": None, "threshold": "valid_debit"})
    if regime.get("terminal_state") != "PIN_STABLE":
        gates.append({"gate": "butterfly_requires_pin_stable", "actual": regime.get("terminal_state"), "threshold": "PIN_STABLE"})
    shock_state = str(_map(facts.get("shock")).get("state") or "NONE")
    if shock_state not in {"NONE", "RECLAIMED"}:
        gates.append({"gate": "butterfly_shock_veto", "actual": shock_state, "threshold": ["NONE", "RECLAIMED"]})
    path, vc, structure = (
        _map(facts.get("path")),
        _map(facts.get("value_center")),
        _map(facts.get("structure")),
    )
    center = _number(candidate.get("center"))
    spot = _number(_map(facts.get("spot")).get("spx"))
    value_center = _number(vc.get("spx_30m"))
    q_mode = _number(structure.get("q_mode"))
    for gate, reference, threshold in (
        ("butterfly_body_value_center_distance", value_center, policy.pin_body_max_center_distance_points),
        ("butterfly_body_q_mode_distance", q_mode, policy.pin_body_max_center_distance_points),
        ("butterfly_body_spot_distance", spot, policy.pin_body_max_spot_distance_points),
    ):
        distance = abs(center - reference) if center is not None and reference is not None else None
        if distance is None or distance > threshold:
            gates.append({"gate": gate, "actual": distance, "threshold": threshold})
    pin = _map(regime.get("pin")) or _map(candidate.get("pin"))
    depin = _number(pin.get("depin_risk"))
    max_depin = policy.pin_thresholds[5]
    if depin is None or depin >= max_depin:
        gates.append({"gate": "butterfly_depin_risk", "actual": depin, "threshold": f"<{max_depin}"})
    if pin.get("recent_extreme_acceptance") is not False:
        gates.append({"gate": "butterfly_recent_extreme_acceptance", "actual": pin.get("recent_extreme_acceptance"), "threshold": False})
    if _number(path.get("breadth_above_vwap")) is None or _number(_map(facts.get("volatility")).get("vix_return_15m_pct")) is None:
        gates.append({"gate": "butterfly_vix_or_breadth_unavailable", "actual": None, "threshold": "both_present"})
    width = _number(economics.get("width_points")) or _number(candidate.get("width"))
    debit = _number(economics.get("max_loss_points"))
    debit_fraction = debit / width if debit is not None and width is not None and width > 0 else None
    if debit_fraction is None or debit_fraction > policy.butterfly_max_debit_fraction:
        gates.append({"gate": "butterfly_debit_fraction", "actual": debit_fraction, "threshold": policy.butterfly_max_debit_fraction})
    risk_usd = debit * 100.0 if debit is not None else None
    if risk_usd is None or risk_usd > policy.butterfly_max_risk_usd:
        gates.append({"gate": "butterfly_risk_budget", "actual": risk_usd, "threshold": policy.butterfly_max_risk_usd})
    gates.extend(
        _rth_butterfly_pin_location_gates(
            facts, policy=policy, center=center, spot=spot, width=width
        )
    )
    return gates


def _rth_butterfly_pin_location_gates(
    facts: Mapping[str, Any],
    *,
    policy: StrategyPolicy,
    center: float | None,
    spot: float | None,
    width: float | None,
) -> list[dict[str, Any]]:
    """Block pin flies whose tent is already behind spot or still a wall cage.

    GTH width scans do not call this. Missing minutes/EM fail closed.
    """

    gates: list[dict[str, Any]] = []
    if center is None or spot is None or width is None or width <= 0:
        gates.append({"gate": "butterfly_spot_outside_wings", "actual": None, "threshold": "inside_wings"})
    elif not (center - width <= spot <= center + width):
        gates.append({
            "gate": "butterfly_spot_outside_wings",
            "actual": round(spot - center, 4),
            "threshold": width,
        })
    minutes = _number(facts.get("minutes_to_close"))
    max_minutes = butterfly_max_entry_minutes(width, policy)
    if minutes is None or max_minutes is None or minutes > max_minutes:
        gates.append({
            "gate": "butterfly_entry_too_early",
            "actual": minutes,
            "threshold": max_minutes,
        })
    remaining = _number(_map(facts.get("volatility")).get("expected_move_points"))
    structure = _map(facts.get("structure"))
    if remaining is None or remaining <= 0 or center is None or width is None or width <= 0 or spot is None:
        gates.append({
            "gate": "butterfly_unresolved_nearby_wall",
            "actual": None,
            "threshold": "remaining_em_and_wings",
        })
        return gates
    reach = remaining * policy.butterfly_unresolved_wall_em_multiple
    unresolved = [
        wall
        for wall in (_number(structure.get("put_wall")), _number(structure.get("call_wall")))
        if wall is not None and abs(spot - wall) <= reach and abs(center - wall) > width
    ]
    if unresolved:
        gates.append({
            "gate": "butterfly_unresolved_nearby_wall",
            "actual": unresolved,
            "threshold": round(reach, 4),
        })
    return gates


def _gth_scan_butterfly_hard_gates(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    del facts, policy
    return [
        {
            "gate": "gth_butterfly_rth_only",
            "actual": candidate.get("setup_kind"),
            "threshold": "rth_stable_pin",
        }
    ]


def _iron_condor_hard_gates(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
) -> list[dict[str, Any]]:
    from spx_spark.application.order_map.iron_condor import (
        MAX_CREDIT_FRACTION,
        MIN_CREDIT_FRACTION,
        SHORT_DELTA_MAX,
        SHORT_DELTA_MIN,
        WING_WIDTH,
    )

    gates: list[dict[str, Any]] = []
    legs = candidate.get("legs")
    if not isinstance(legs, list) or len(legs) != 4 or any(not _map(leg) for leg in legs):
        gates.append(
            {
                "gate": "iron_condor_four_leg_quote_unavailable",
                "actual": None,
                "threshold": "four_legs",
            }
        )
    economics = _map(candidate.get("economics"))
    credit_fraction = _number(economics.get("credit_fraction_of_width"))
    gain = _number(economics.get("max_gain_points"))
    loss = _number(economics.get("max_loss_points"))
    if gain is None or loss is None or gain <= 0 or loss <= 0 or credit_fraction is None:
        gates.append(
            {
                "gate": "iron_condor_credit_unavailable",
                "actual": dict(economics),
                "threshold": "valid_credit",
            }
        )
    elif not MIN_CREDIT_FRACTION <= credit_fraction <= MAX_CREDIT_FRACTION:
        gates.append(
            {
                "gate": "iron_condor_credit_fraction",
                "actual": credit_fraction,
                "threshold": f"{MIN_CREDIT_FRACTION}-{MAX_CREDIT_FRACTION}",
            }
        )
    if candidate.get("spot_inside_shorts") is not True:
        gates.append(
            {
                "gate": "iron_condor_spot_outside_shorts",
                "actual": _number(_map(facts.get("spot")).get("spx")),
                "threshold": "between_short_strikes",
            }
        )
    deltas = [
        abs(delta)
        for delta in (
            _number(_map(candidate.get("put_short")).get("delta")),
            _number(_map(candidate.get("call_short")).get("delta")),
        )
        if delta is not None
    ]
    if len(deltas) != 2 or any(not SHORT_DELTA_MIN <= value <= SHORT_DELTA_MAX for value in deltas):
        gates.append(
            {
                "gate": "iron_condor_short_delta_band",
                "actual": deltas,
                "threshold": f"{SHORT_DELTA_MIN}-{SHORT_DELTA_MAX}",
            }
        )
    put_width = _number(economics.get("put_width_points"))
    call_width = _number(economics.get("call_width_points"))
    if put_width != WING_WIDTH or call_width != WING_WIDTH:
        gates.append(
            {
                "gate": "iron_condor_wing_too_wide",
                "actual": [put_width, call_width],
                "threshold": WING_WIDTH,
            }
        )
    return gates


def _score_candidate(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    probability_settings: StrategyDistributionSettings | None,
    now: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    expected_kind = {
        "UP": "terminal_above",
        "DOWN": "terminal_below",
        "NEUTRAL": "terminal_between",
    }.get(str(candidate.get("direction")))
    evidence, event, evidence_reasons = _candidate_probability_evidence(
        candidate,
        facts,
        expected_kind=expected_kind,
        data_root=data_root,
        settings=probability_settings,
        now=now,
    )
    if evidence_reasons:
        return dict(candidate), [_reason_gate(reason) for reason in evidence_reasons]
    if event.get("kind") != expected_kind:
        return dict(candidate), [_reason_gate("candidate_probability_event_mismatch")]
    q, p, low = (_number(evidence.get(key)) for key in ("q", "p_empirical", "p_interval_low"))
    if q is None or p is None or low is None:
        return dict(candidate), [_reason_gate("candidate_probability_unavailable")]
    weight = float(evidence["shrinkage_weight"])
    probability, conservative = (weight * value + (1.0 - weight) * q for value in (p, low))
    economics, quote = _map(candidate.get("economics")), _map(candidate.get("quote"))
    gain, loss = (_number(economics.get(key)) for key in ("max_gain_points", "max_loss_points"))
    if gain is None or loss is None or gain <= 0.0 or loss <= 0.0:
        return dict(candidate), [_reason_gate("candidate_payoff_unavailable")]
    expected = probability * gain - (1.0 - probability) * loss
    lower_bound = conservative * gain - (1.0 - conservative) * loss
    friction = min(abs(float(quote.get("ask", 0.0)) - float(quote.get("bid", 0.0))) / loss, 1.0)
    uncertainty, migration = 1.0 - weight, float(_map(regime.get("pin")).get("depin_risk") or 0.0)
    utility = expected / loss - 0.75 - 0.25 * friction - 0.25 * uncertainty - 0.5 * migration
    width = _number(economics.get("width_points"))
    debit = _number(economics.get("max_loss_points"))
    required_p = (1.75 * debit / width) if width and debit and width > 0 else None
    advisories: list[str] = []
    scoring = {
        "event_probability": round(probability, 6),
        "conservative_probability": round(conservative, 6),
        "expected_net_pnl": round(expected * 100.0, 2),
        "conservative_lower_bound": round(lower_bound * 100.0, 2),
        "p10_net_pnl": round(-loss * 100.0, 2),
        "p50_net_pnl": round((gain if probability >= 0.5 else -loss) * 100.0, 2),
        "p90_net_pnl": round((gain if probability >= 0.1 else -loss) * 100.0, 2),
        "expected_shortfall_10": round(loss * 100.0, 2),
        "utility": round(utility, 6),
        "liquidity_penalty": round(friction, 6),
        "model_uncertainty": round(uncertainty, 6),
        "method": "binary_payoff_bootstrap_bound.v1",
    }
    if utility <= 0.0:
        advisories.append("candidate_utility_not_positive")
    if lower_bound <= 0.0:
        advisories.append("candidate_lower_bound_not_positive")
    scored = {
        **dict(candidate),
        "probability_evidence": evidence,
        "utility": scoring,
        "edge": {
            "edge_status": "research_unvalidated",
            "utility": round(utility, 6),
            "required_p_breakeven": round(required_p, 6) if required_p is not None else None,
            "model_p": round(probability, 6),
            "advisories": list(dict.fromkeys(advisories)),
        },
    }
    return scored, []


def _candidate_probability_evidence(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    expected_kind: str | None,
    data_root: str | Path | None,
    settings: StrategyDistributionSettings | None,
    now: datetime,
) -> tuple[dict[str, Any], Mapping[str, Any], list[str]]:
    candidate_event = _map(candidate.get("probability_event"))
    if candidate_event.get("kind") == expected_kind:
        return {}, candidate_event, ["candidate_probability_unavailable"]
    probability = _map(facts.get("probability"))
    event = _map(probability.get("event"))
    if event.get("kind") == expected_kind:
        return _probability_evidence(facts), event, []
    if expected_kind != "terminal_between":
        if not event:
            return {}, {}, ["candidate_probability_unavailable"]
        return {}, event, ["candidate_probability_event_mismatch"]
    if data_root is None or settings is None:
        return {}, {}, ["pin_probability_model_unavailable"]

    economics = _map(candidate.get("economics"))
    lower = _number(economics.get("breakeven_low"))
    upper = _number(economics.get("breakeven_high"))
    spot = _number(_map(facts.get("spot")).get("spx"))
    q_mass = _terminal_range_q_mass(_map(_map(facts.get("structure")).get("q_local_mass_5pt")), lower, upper)
    session_date = _session_date(facts.get("session_date"))
    if None in (lower, upper, spot, q_mass) or session_date is None:
        return {}, {}, ["pin_probability_inputs_unavailable"]
    estimate = estimate_physical_terminal_range(
        Path(data_root).expanduser() / "features",
        now=now,
        trading_date=session_date,
        horizon_seconds=settings.horizon_seconds,
        window_days=settings.window_days,
        minimum_samples=settings.minimum_physical_samples,
        prior_alpha=settings.beta_prior_alpha,
        prior_beta=settings.beta_prior_beta,
        current_spot=float(spot),
        lower_level=float(lower),
        upper_level=float(upper),
    )
    if estimate.probability is None or estimate.interval_low is None:
        return {}, {}, ["pin_physical_probability_unavailable"]
    target_at = now + timedelta(seconds=settings.horizon_seconds)
    event = {
        "event_id": (
            "pin-range:"
            f"{_hash((session_date.isoformat(), candidate.get('opportunity_id'), round(float(lower), 4), round(float(upper), 4), target_at.isoformat()))[:24]}"
        ),
        "kind": "terminal_between",
        "target_at": target_at.isoformat(),
        "lower_level": round(float(lower), 4),
        "upper_level": round(float(upper), 4),
    }
    effective = max(estimate.effective_sample_count, 0.0)
    return {
        "q": round(float(q_mass), 6),
        "p_empirical": estimate.probability,
        "p_interval_low": estimate.interval_low,
        "n_raw": estimate.sample_count,
        "n_effective": round(effective, 6),
        "shrinkage_weight": round(effective / (effective + 20.0), 6),
        "historical_sessions": list(estimate.historical_sessions),
        "method": estimate.model_version,
    }, event, []


def _attach_policy_ev(
    candidate: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    data_root: str | Path | None,
) -> dict[str, Any]:
    annotation = _policy_ev_annotation(
        candidate,
        regime,
        data_root=data_root,
    )
    return {
        **dict(candidate),
        "edge": {
            **dict(_map(candidate.get("edge"))),
            **annotation,
        },
    }


def _policy_ev_annotation(
    candidate: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    data_root: str | Path | None,
) -> dict[str, Any]:
    table = _load_policy_ev_table(data_root)
    if table is None:
        return {
            "policy_ev": None,
            "policy_ev_n": None,
            "policy_ev_version": None,
            "policy_ev_reason": "table_unavailable",
        }
    bucket = _map(_map(table).get("buckets")).get(
        _policy_ev_bucket_key(candidate, regime)
    )
    if not isinstance(bucket, Mapping):
        return {
            "policy_ev": None,
            "policy_ev_n": None,
            "policy_ev_version": str(table.get("management_policy_version") or ""),
            "policy_ev_reason": "bucket_unavailable",
        }
    return {
        "policy_ev": _number(bucket.get("ev_points")),
        "policy_ev_n": int(n) if (n := _number(bucket.get("n"))) is not None else None,
        "policy_ev_version": str(table.get("management_policy_version") or ""),
        "policy_ev_reason": (
            str(bucket.get("reason") or "")
            or None
        ),
    }


def _policy_ev_bucket_key(
    candidate: Mapping[str, Any],
    regime: Mapping[str, Any],
) -> str:
    return "|".join(
        (
            _bucket_dimension(candidate.get("setup_kind")),
            _bucket_dimension(candidate.get("direction")),
            _bucket_dimension(regime.get("terminal_state")),
        )
    )


def _bucket_dimension(value: object) -> str:
    text = str(value or "").strip()
    return text if text else "unknown"


def _load_policy_ev_table(
    data_root: str | Path | None,
) -> Mapping[str, Any] | None:
    if data_root is None:
        return None
    path = Path(data_root).expanduser().joinpath(*_POLICY_EV_TABLE_PATH)
    try:
        mtime_ns = path.stat().st_mtime_ns
    except OSError:
        return None
    return _load_policy_ev_table_cached(str(path), mtime_ns)


@lru_cache(maxsize=16)
def _load_policy_ev_table_cached(
    path_text: str,
    mtime_ns: int,
) -> Mapping[str, Any] | None:
    del mtime_ns
    path = Path(path_text)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("schema_version") != _POLICY_EV_SCHEMA_VERSION:
        return None
    if not isinstance(payload.get("buckets"), dict):
        return None
    return payload


def _terminal_range_q_mass(values: Mapping[str, Any], lower: float | None, upper: float | None) -> float | None:
    if lower is None or upper is None or lower >= upper:
        return None
    cells = []
    for key, value in values.items():
        center, mass = _strike_number(key), _number(value)
        if center is not None and mass is not None and mass >= 0.0:
            cells.append((center - 2.5, center + 2.5, mass))
    if not cells or lower < min(cell[0] for cell in cells) or upper > max(cell[1] for cell in cells):
        return None
    probability = sum(
        mass * max(0.0, min(upper, high) - max(lower, low)) / (high - low)
        for low, high, mass in cells
    )
    return min(max(probability, 0.0), 1.0)


def _gate_from_entry_reason(
    reason: str,
    entry_quality: Mapping[str, Any],
    policy: StrategyPolicy,
    *,
    setup_kind: str = "",
) -> dict[str, Any]:
    if reason == "direction_valid_but_entry_too_late":
        failed_break = setup_kind == "FAILED_BREAK_RECLAIM"
        return {
            "gate": reason,
            "actual": {
                "target_room_ratio": entry_quality.get("target_room_ratio"),
                "debit_fraction_of_width": entry_quality.get("debit_fraction_of_width"),
                "trigger_target_progress": entry_quality.get("trigger_target_progress"),
            },
            "threshold": {
                "min_target_room_ratio": (
                    policy.failed_break_min_target_room_ratio
                    if failed_break
                    else policy.min_target_room_ratio
                ),
                "max_debit_fraction": (
                    policy.failed_break_max_debit_fraction
                    if failed_break
                    else policy.max_debit_fraction
                ),
                "max_progress": (
                    policy.failed_break_max_trigger_target_progress
                    if failed_break
                    else policy.max_trigger_target_progress
                ),
            },
        }
    if reason == "stop_distance_outside_atr_band":
        return {
            "gate": reason,
            "actual": entry_quality.get("stop_distance_atr"),
            "threshold": [policy.min_stop_atr, policy.max_stop_atr],
        }
    return _reason_gate(reason)


def _width_path_gate(
    reason: str,
    *,
    long_strike: float,
    short_strike: float,
    target: float | None,
    remaining_expected_move: float | None,
) -> dict[str, Any]:
    width = abs(short_strike - long_strike)
    if reason == "vertical_short_beyond_target":
        return {"gate": reason, "actual": short_strike, "threshold": target}
    if reason == "vertical_width_exceeds_remaining_move":
        return {"gate": reason, "actual": width, "threshold": remaining_expected_move}
    if reason == "debit_long_beyond_remaining_move":
        return {"gate": reason, "actual": long_strike, "threshold": remaining_expected_move}
    if reason == "vertical_remaining_move_unavailable":
        return {"gate": reason, "actual": remaining_expected_move, "threshold": ">0"}
    return _reason_gate(reason)


def _vertical_right(
    candidate: Mapping[str, Any],
    long: Mapping[str, Any],
    *,
    long_strike: float,
    short_strike: float,
) -> str:
    right = str(candidate.get("right") or long.get("right") or "").upper()
    if right in {"C", "P"}:
        return right
    strategy_type = str(candidate.get("strategy_type") or "").upper()
    if strategy_type.startswith("CALL_"):
        return "C"
    if strategy_type.startswith("PUT_"):
        return "P"
    return "C" if short_strike > long_strike else "P"


def _rejected(candidate: Mapping[str, Any], failed_gates: list[dict[str, Any]]) -> dict[str, Any]:
    reasons = [str(gate.get("gate")) for gate in failed_gates]
    return {
        **dict(candidate),
        "failed_gates": failed_gates,
        "rejection_reasons": reasons,
    }


def _audit_row(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": candidate.get("candidate_id"),
        "strategy_type": candidate.get("strategy_type"),
        "strikes": _candidate_strikes(candidate),
        "score": _candidate_score(candidate),
        "gate_failures": list(candidate.get("failed_gates") or ()),
    }


def _candidate_strikes(candidate: Mapping[str, Any]) -> list[float]:
    if candidate.get("legs"):
        return [
            float(strike)
            for leg in candidate.get("legs") or ()
            if (strike := _number(_map(leg).get("strike"))) is not None
        ]
    return [
        float(strike)
        for leg in (candidate.get("long"), candidate.get("short"))
        if (strike := _number(_map(leg).get("strike"))) is not None
    ]


def _candidate_score(candidate: Mapping[str, Any]) -> float:
    """Report the score that actually ranks candidates (structure, not utility)."""

    return round(float(candidate.get("selection_score") or 0.0), 6)


def _miss_sort_key(candidate: Mapping[str, Any]) -> tuple[float, float]:
    utility = _number(_map(candidate.get("utility")).get("utility"))
    lower = _number(_map(candidate.get("utility")).get("conservative_lower_bound"))
    return (
        utility if utility is not None else -999.0,
        lower if lower is not None else float(candidate.get("selection_score") or 0.0),
    )


def _reason_gate(reason: str) -> dict[str, Any]:
    return {"gate": reason, "actual": None, "threshold": "pass"}


def _probability_evidence(facts: Mapping[str, Any]) -> dict[str, Any]:
    probability = _map(facts.get("probability"))
    effective = max(_number(probability.get("n_effective")) or 0.0, 0.0)
    return {
        "q": _number(probability.get("q")),
        "p_empirical": _number(probability.get("p_empirical")),
        "p_interval_low": _number(probability.get("p_interval_low")),
        "n_raw": int(_number(probability.get("n_raw")) or 0),
        "n_effective": round(effective, 6),
        "shrinkage_weight": round(effective / (effective + 20.0), 6),
        "historical_sessions": list(probability.get("historical_sessions") or ()),
    }


def _strike_number(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0.0 else None


def _session_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _hash(value: object) -> str:
    import hashlib
    import json

    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, (str, datetime)):
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("strategy ranking time must be timezone-aware")
    return value.astimezone(timezone.utc)
