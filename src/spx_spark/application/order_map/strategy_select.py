"""Single strategy-decision authority for the Order Map payload."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.application.order_map.candidate_factory import (
    candidate_generation_reasons,
    enumerate_candidates,
)
from spx_spark.application.order_map.strategy_facts import build_market_fact_pack
from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    assess_regime,
)
from spx_spark.application.order_map.strategy_ranker import (
    RankResult,
    rank_candidates,
)
from spx_spark.settings.strategy_distribution import StrategyDistributionSettings
from spx_spark.storage import LatestState


def build_strategy_decision(
    payload: Mapping[str, Any], latest: LatestState, now: datetime, *,
    data_root: str | Path | None = None,
    probability_settings: StrategyDistributionSettings | None = None,
) -> dict[str, Any]:
    facts = build_market_fact_pack(payload, latest, now)
    regime = assess_regime(facts)
    reasons = _gate_reasons(facts, regime)
    rows: list[dict[str, Any]] = []
    rank = RankResult(passed=[], near_misses=[], gate_audit=[])
    if not reasons:
        rows = enumerate_candidates(
            payload, facts, regime, latest, now=_utc(now), policy=DEFAULT_STRATEGY_POLICY
        )
        if rows:
            rank = rank_candidates(
                rows,
                facts,
                regime,
                policy=DEFAULT_STRATEGY_POLICY,
                data_root=data_root,
                probability_settings=probability_settings,
                now=_utc(now),
            )
            if rank.passed:
                shadow_candidates, shadow_candidates_skipped = _shadow_candidates(
                    rank.passed[1:3]
                )
                return _candidate_decision(
                    facts,
                    {**regime, "entry_state": "GOOD_LOCATION"},
                    rank.passed[0],
                    candidates_considered=_candidate_summaries(rank),
                    shadow_candidates=shadow_candidates,
                    shadow_candidates_skipped=shadow_candidates_skipped,
                )
            reasons = _rank_reasons(rank)
        else:
            reasons = candidate_generation_reasons(
                payload, facts, regime, latest, now=_utc(now), policy=DEFAULT_STRATEGY_POLICY
            )
    if "direction_valid_but_entry_too_late" in reasons:
        regime = {**regime, "entry_state": "LATE_CHASE"}
    return _no_trade_decision(
        facts,
        regime,
        reasons,
        nearest_candidates=rank.near_misses,
        candidates_considered=_candidate_summaries(rank) if rows else [],
    )


def _gate_reasons(facts: Mapping[str, Any], regime: Mapping[str, Any]) -> list[str]:
    quality, event = _map(facts.get("quality")), _map(facts.get("event"))
    reasons = list(quality.get("reasons") or ()) if quality.get("status") != "ready" else []
    if regime.get("event_state") in {"SCHEDULED_EVENT_RISK", "POST_EVENT_DISCOVERY"}:
        reasons.append(f"event_gate:{str(regime['event_state']).lower()}")
    if event.get("entry_allowed") is not True:
        reasons.append("macro_entry_not_authorized")
    return list(dict.fromkeys(map(str, reasons)))


def _base_decision(facts: Mapping[str, Any], regime: Mapping[str, Any], identity: object) -> dict[str, Any]:
    from spx_spark.settings.loader import runtime_git_sha

    return {
        "schema_version": "strategy_decision.v2",
        "decision_id": f"strategy:{_hash(identity)[:24]}",
        "policy_version": DEFAULT_STRATEGY_POLICY.policy_version,
        "runtime_git_sha": runtime_git_sha(),
        "decision_at": facts["decision_at"],
        "available_at": facts["available_at"],
        "session_date": facts.get("session_date"),
        "market_facts": facts,
        "regime": regime,
        "probability_evidence": _probability_evidence(facts),
        "automatic_ordering": False,
    }


def _probability_evidence(facts: Mapping[str, Any]) -> dict[str, Any]:
    probability = _map(facts.get("probability"))
    effective = max(_number(probability.get("n_effective")) or 0.0, 0.0)
    return {
        "q": _number(probability.get("q")), "p_empirical": _number(probability.get("p_empirical")),
        "p_interval_low": _number(probability.get("p_interval_low")),
        "n_raw": int(_number(probability.get("n_raw")) or 0), "n_effective": round(effective, 6),
        "shrinkage_weight": round(effective / (effective + 20.0), 6),
        "historical_sessions": list(probability.get("historical_sessions") or ()),
    }


def _candidate_decision(
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    candidates_considered: list[dict[str, Any]],
    shadow_candidates: list[dict[str, Any]],
    shadow_candidates_skipped: list[dict[str, Any]],
) -> dict[str, Any]:
    legs = candidate.get("legs") or (candidate.get("long"), candidate.get("short"))
    available = max(str(facts["available_at"]), *(str(_map(leg).get("source_at") or "") for leg in legs))
    economics = _map(candidate.get("economics"))
    result = _base_decision(facts, regime, (facts["decision_at"], available, candidate["opportunity_id"], candidate["quote"]))
    geometry_source = _decision_geometry_source(candidate)
    result.update({
        "available_at": available,
        "geometry_source": geometry_source,
        "candidates_considered": candidates_considered[:5],
        "probability_evidence": dict(_map(candidate.get("probability_evidence"))),
        "decision_type": candidate["strategy_type"],
        "candidate": dict(candidate),
        "shadow_candidates": shadow_candidates,
        "shadow_candidates_skipped": shadow_candidates_skipped,
        "desk_view": {"state": regime["path_state"], "direction": candidate["direction"],
                      "conclusion": "MANUAL CANDIDATE", "reason": candidate["setup_kind"]},
        "why_not": {"nearest_candidate": None, "reasons": [], "reauthorize_on": None},
        "execution": {"action": "MANUAL_LIMIT", "order_type": "NET_DEBIT_LIMIT",
                      "limit": _map(candidate.get("quote")).get("ask"),
                      "quote_valid_until": candidate["quote_valid_until"],
                      "opportunity_valid_until": candidate["opportunity_valid_until"],
                      "automatic_ordering": False, "manual_action_only": True},
        "risk": {"max_loss": round(float(economics["max_loss_points"]) * 100, 2),
                 "invalidation": {"instrument": "SPX", "price": candidate["invalidation_spx"]}},
        "targets": [{"instrument": "SPX", "price": candidate["target_spx"]}],
        "data_quality": {**dict(_map(facts.get("quality"))), "quote": "ready"},
        "action_authority": "manual",
    })
    return result


def _no_trade_decision(
    facts: Mapping[str, Any], regime: Mapping[str, Any], reasons: list[str], *,
    nearest_candidates: list[Mapping[str, Any]] | None = None,
    candidates_considered: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    reasons = list(dict.fromkeys(reasons or ["no_supported_strategy_candidate"]))
    nearest_candidates = list(nearest_candidates or ())
    nearest_candidate = nearest_candidates[0] if nearest_candidates else None
    shadow = dict(nearest_candidate or {})
    if shadow:
        shadow.update({"shadow_only": True, "rejection_reasons": shadow.get("rejection_reasons") or reasons})
    shadows = []
    for item in nearest_candidates[:3]:
        row = dict(item)
        row.update({"shadow_only": True, "rejection_reasons": row.get("rejection_reasons") or reasons})
        shadows.append(row)
    legs = (
        shadow.get("legs") or (shadow.get("long"), shadow.get("short"))
        if shadow
        else ()
    )
    available_times = [
        timestamp
        for timestamp in (
            _time(facts["available_at"]),
            *(_time(_map(leg).get("source_at")) for leg in legs),
        )
        if timestamp is not None
    ]
    available = max(available_times).isoformat()
    result = _base_decision(
        facts,
        regime,
        (facts["decision_at"], available, regime, reasons, shadow.get("opportunity_id")),
    )
    refresh = "刷新 SPXW 两腿双边报价后重新计算" if "quote_refresh_required" in reasons else "等待价格触发、结构赔率和执行价格同时通过"
    result.update({
        "available_at": available,
        "geometry_source": _decision_geometry_source(shadow),
        "candidates_considered": list(candidates_considered or ())[:5],
        "decision_type": "NO_TRADE",
        "candidate": None,
        "shadow_candidates": [],
        "shadow_candidates_skipped": [],
        "desk_view": {"state": regime["path_state"], "direction": regime.get("path_direction"),
                      "conclusion": "NO TRADE", "reason": reasons[0]},
        "why_not": {"nearest_candidate": shadow or None, "nearest_candidates": shadows,
                    "reasons": reasons, "reauthorize_on": refresh},
        "execution": {"action": "WAIT", "order_type": None, "limit": None,
                      "automatic_ordering": False, "manual_action_only": True},
        "risk": {"max_loss": None, "invalidation": "没有候选被授权，不建立风险敞口"},
        "targets": [],
        "data_quality": _map(facts.get("quality")),
        "action_authority": "none",
    })
    return result


def _rank_reasons(rank: RankResult) -> list[str]:
    for candidate in rank.near_misses:
        reasons = [str(reason) for reason in candidate.get("rejection_reasons") or ()]
        if reasons:
            if "spread_leg_quote_stale" in reasons and "quote_refresh_required" not in reasons:
                reasons.insert(0, "quote_refresh_required")
            return list(dict.fromkeys(reasons))
    return ["no_supported_strategy_candidate"]


def _candidate_summaries(rank: RankResult) -> list[dict[str, Any]]:
    return [_candidate_summary(candidate) for candidate in (*rank.passed, *rank.near_misses)][:5]


def _candidate_summary(candidate: Mapping[str, Any]) -> dict[str, Any]:
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
    utility = _number(_map(candidate.get("utility")).get("utility"))
    if utility is not None:
        return round(utility, 6)
    return round(float(candidate.get("selection_score") or 0.0), 6)


def _decision_geometry_source(candidate: Mapping[str, Any]) -> str:
    source = candidate.get("geometry_source")
    return "confirmation_geometry" if source == "confirmation_geometry" else "facts_wall_ladder_fallback"


def _shadow_candidates(
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    persisted: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for candidate in candidates:
        reason = _shadow_candidate_skip_reason(candidate)
        if reason is None:
            persisted.append(dict(candidate))
            continue
        skipped.append(
            {
                "candidate_id": candidate.get("candidate_id"),
                "reason": reason,
            }
        )
    return persisted, skipped


def _shadow_candidate_skip_reason(candidate: Mapping[str, Any]) -> str | None:
    quote = _map(candidate.get("quote"))
    if _number(quote.get("bid")) is None or _number(quote.get("ask")) is None:
        return "candidate_quote_incomplete"
    raw_legs = candidate.get("legs")
    if isinstance(raw_legs, Sequence) and not isinstance(raw_legs, (str, bytes)):
        legs = tuple(_map(item) for item in raw_legs)
    else:
        legs = (_map(candidate.get("long")), _map(candidate.get("short")))
    if not legs or any(not leg for leg in legs):
        return "candidate_legs_incomplete"
    for leg in legs:
        if not str(leg.get("contract_id") or "").strip():
            return "candidate_legs_incomplete"
        if _number(leg.get("bid")) is None or _number(leg.get("ask")) is None:
            return "candidate_legs_incomplete"
        if _time(leg.get("source_at")) is None:
            return "candidate_legs_incomplete"
    return None


def _hash(value: object) -> str:
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
        raise ValueError("strategy decision time must be timezone-aware")
    return value.astimezone(timezone.utc)
