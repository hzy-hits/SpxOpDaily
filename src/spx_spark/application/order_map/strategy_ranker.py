"""Hard-gate and rank strategy candidates."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import json
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.density import summarize_strike_surface_shape
from spx_spark.analytics.options.strategy_payoff import vertical_entry_quality
from spx_spark.application.market_features.physical_followthrough import (
    estimate_physical_terminal_range,
)
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.settings.strategy_distribution import StrategyDistributionSettings

_POLICY_EV_TABLE_PATH = ("research", "policy_ev_table.v1.json")
_POLICY_EV_SCHEMA_VERSION = "policy_ev_table.v1"
_EVENT_SETTLEMENT_SETUP = "EVENT_SETTLEMENT_THRESHOLD"
_EVENT_SETTLEMENT_MAX_DEBIT_FRACTION = 0.50


@dataclass(frozen=True, slots=True)
class RankResult:
    passed: list[dict[str, Any]]
    near_misses: list[dict[str, Any]]
    gate_audit: list[dict[str, Any]]


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
        gates.extend(_vertical_hard_gates(candidate, facts, policy=policy))
    elif strategy_type.endswith("_BUTTERFLY"):
        gates.extend(_butterfly_hard_gates(candidate, facts, regime, policy=policy))
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
    *,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    if candidate.get("setup_kind") == _EVENT_SETTLEMENT_SETUP:
        return _event_settlement_vertical_hard_gates(candidate)
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
    return [_gate_from_entry_reason(reason, entry_quality, policy) for reason in reasons]


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
) -> dict[str, Any]:
    if reason == "direction_valid_but_entry_too_late":
        return {
            "gate": reason,
            "actual": {
                "target_room_ratio": entry_quality.get("target_room_ratio"),
                "debit_fraction_of_width": entry_quality.get("debit_fraction_of_width"),
                "trigger_target_progress": entry_quality.get("trigger_target_progress"),
            },
            "threshold": {
                "min_target_room_ratio": policy.min_target_room_ratio,
                "max_debit_fraction": policy.max_debit_fraction,
                "max_progress": 0.6,
            },
        }
    if reason == "stop_distance_outside_atr_band":
        return {
            "gate": reason,
            "actual": entry_quality.get("stop_distance_atr"),
            "threshold": [policy.min_stop_atr, policy.max_stop_atr],
        }
    return _reason_gate(reason)


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
