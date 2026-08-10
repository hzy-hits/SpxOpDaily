"""Candidate enumeration for strategy-decision competition."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from spx_spark.analytics.options.strategy_payoff import (
    butterfly_economics,
    conservative_butterfly_bbo,
    conservative_vertical_bbo,
    vertical_economics,
)
from spx_spark.application.market_features.market import quote_source_at
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import InstrumentId
from spx_spark.storage import LatestState

WIDTHS: tuple[float, ...] = (5.0, 10.0, 15.0, 20.0)


def enumerate_candidates(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    """Enumerate all currently supported manual-action strategy candidates."""

    now = _utc(now)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in (
        *_vertical_candidates(payload, facts, regime, latest, now=now, policy=policy),
        *_butterfly_candidates(payload, facts, regime, latest, now=now, policy=policy),
    ):
        candidate_id = str(row.get("candidate_id") or "")
        if candidate_id and candidate_id not in seen:
            rows.append(row)
            seen.add(candidate_id)
    rows.sort(key=lambda row: float(row.get("selection_score") or 0.0), reverse=True)
    return rows


def candidate_generation_reasons(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[str]:
    """Return legacy-compatible reasons when enumeration yields no rows."""

    if DEFAULT_MARKET_CALENDAR.is_rth_open(_utc(now)):
        _, reasons = _rth_evidence(payload, facts, regime, latest)
        return reasons or ["vertical_exact_spread_unavailable"]
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(_utc(now)):
        _, reasons = _gth_evidence(facts)
        return reasons or ["gth_confirmed_level_candidate_unavailable"]
    return ["session_not_open_for_spxw_strategy"]


def resolve_geometry(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    direction: str | None,
    trigger: float | None,
) -> tuple[float | None, float | None, str | None]:
    """Resolve target/stop geometry without reintroducing selector-local rules."""

    intent = _map(payload.get("trade_intent"))
    geometry = _map(intent.get("confirmation_geometry"))
    target = _number(geometry.get("target_spx"))
    stop = _number(intent.get("invalidation_spx"))
    if target is not None and stop is not None:
        return target, stop, "confirmation_geometry"

    fallback_target, fallback_stop = _facts_wall_ladder_geometry(facts, direction, trigger)
    return (
        target if target is not None else fallback_target,
        stop if stop is not None else fallback_stop,
        "confirmation_geometry" if target is not None else "facts_wall_ladder_fallback",
    )


def _vertical_candidates(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        evidence, _ = _rth_evidence(payload, facts, regime, latest)
        if not evidence:
            return []
        rows = [_vertical_candidate_from_evidence(evidence, facts, now=now, policy=policy)]
        rows.extend(_rth_width_verticals(evidence, payload, facts, latest, now=now, policy=policy))
        return [row for row in rows if row]
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        evidence, _ = _gth_evidence(facts)
        row = _vertical_candidate_from_evidence(evidence, facts, now=now, policy=policy) if evidence else {}
        return [row] if row else []
    return []


def _rth_width_verticals(
    evidence: Mapping[str, Any],
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    frame = _map(payload.get("option_structure_frame"))
    expiry = str(frame.get("front_expiry") or _expiry_from_legs(evidence) or "")
    direction = str(evidence.get("direction") or "")
    right = "C" if direction == "UP" else "P" if direction == "DOWN" else ""
    if not expiry or right not in {"C", "P"}:
        return []
    anchors = {
        _round_to_strike(_number(evidence.get("trigger_level"))),
        _round_to_strike(_number(_map(facts.get("spot")).get("spx"))),
    }
    rows = []
    for long_strike in sorted(value for value in anchors if value is not None):
        for width in WIDTHS:
            short_strike = long_strike + width if right == "C" else long_strike - width
            long = _option_leg(latest, expiry, long_strike, right, require_schwab=True)
            short = _option_leg(latest, expiry, short_strike, right, require_schwab=True)
            if not long or not short:
                continue
            row = _vertical_candidate_from_evidence(
                {**dict(evidence), "long": long, "short": short, "source": "rth_schwab_width_enumeration"},
                facts,
                now=now,
                policy=policy,
            )
            if row:
                rows.append(row)
    return rows


def _vertical_candidate_from_evidence(
    evidence: Mapping[str, Any] | None,
    facts: Mapping[str, Any],
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> dict[str, Any]:
    if not evidence:
        return {}
    long, short = _map(evidence.get("long")), _map(evidence.get("short"))
    right = str(long.get("right") or "").upper()
    strikes = (_number(long.get("strike")), _number(short.get("strike")))
    strategy_type = f"{'CALL' if right == 'C' else 'PUT'}_DEBIT_VERTICAL"
    bbo = conservative_vertical_bbo(
        long,
        short,
        now=now,
        max_quote_age_seconds=policy.quote_max_age_seconds,
        max_source_skew_seconds=policy.quote_max_skew_seconds,
    )
    economics: dict[str, Any] = {}
    if bbo.get("status") == "ready" and None not in strikes and right in {"C", "P"}:
        try:
            economics = vertical_economics(
                long_strike=float(strikes[0]),
                short_strike=float(strikes[1]),
                net_debit=float(bbo["ask"]),
                right=right,
            )
        except ValueError:
            economics = {}
    expiry = _expiry_from_contract(long.get("contract_id")) or _expiry_from_contract(short.get("contract_id"))
    candidate_id = _candidate_id(
        facts.get("session_date"),
        strategy_type,
        expiry,
        [value for value in strikes if value is not None],
        right,
    )
    quote_valid = _quote_valid_until((long, short), now=now, policy=policy)
    opportunity_valid = now + timedelta(seconds=policy.opportunity_ttl_seconds)
    if source_valid := _time(evidence.get("valid_until")):
        opportunity_valid = min(opportunity_valid, source_valid)
    identity = {
        "session_date": facts.get("session_date"),
        "candidate_id": candidate_id,
        "long_contract_id": long.get("contract_id"),
        "short_contract_id": short.get("contract_id"),
    }
    return {
        "candidate_id": candidate_id,
        "strategy_type": strategy_type,
        **{
            key: evidence.get(key)
            for key in (
                "setup_kind",
                "direction",
                "trigger_level",
                "target_spx",
                "invalidation_spx",
                "source",
                "geometry_source",
            )
        },
        "right": right,
        "opportunity_id": f"strategy-opportunity:{_hash(identity)[:24]}",
        "long": dict(long),
        "short": dict(short),
        "quote": bbo,
        "economics": economics,
        "selection_score": _vertical_selection_score(economics, bbo),
        "quote_valid_until": quote_valid.isoformat() if quote_valid else now.isoformat(),
        "opportunity_valid_until": opportunity_valid.isoformat(),
        "automatic_ordering": False,
        "manual_action_only": True,
    }


def _rth_evidence(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    latest: LatestState,
) -> tuple[dict[str, Any] | None, list[str]]:
    trigger = _map(facts.get("trigger"))
    direction, thesis = _direction(trigger.get("direction")), str(trigger.get("thesis") or "").lower()
    if trigger.get("phase") != "confirmed" or not direction:
        return None, ["confirmed_price_trigger_unavailable"]
    if thesis == "fade":
        setup = "FAILED_BREAK_RECLAIM"
    elif thesis == "breakout":
        path_state = str(regime.get("path_state") or "")
        path_direction = str(regime.get("path_direction") or "")
        if path_state == "TREND" and path_direction != direction:
            return None, ["price_trigger_conflicts_with_established_path"]
        setup = "TREND_PULLBACK" if (path_state, path_direction) == ("TREND", direction) else "BREAKOUT_ACCEPTANCE"
    else:
        return None, ["price_trigger_not_aligned_with_supported_setup"]
    source = "call_skew_spread_shadow" if direction == "UP" else "put_skew_spread_shadow"
    shadow, spread = _map(payload.get(source)), _map(_map(payload.get(source)).get("candidate"))
    if shadow.get("status") != "candidate" or not spread:
        spread = _intent_spread(payload.get("trade_intent"), latest)
        source = "legacy_trade_intent_trigger_only"
    if not spread:
        spread = _confirmed_trigger_spread(facts, direction)
        source = "rth_confirmed_trigger_exact_spread_snapshot"
    if not spread:
        return None, ["vertical_exact_spread_unavailable"]
    target, stop, geometry_source = resolve_geometry(payload, facts, direction, _number(trigger.get("level")))
    if target is None or stop is None:
        return None, ["vertical_target_or_invalidation_unavailable"]
    return {
        "setup_kind": setup,
        "direction": direction,
        "trigger_level": _number(trigger.get("level")),
        "target_spx": target,
        "invalidation_spx": stop,
        "long": _map(spread.get("long")),
        "short": _map(spread.get("short")),
        "source": source,
        "geometry_source": geometry_source,
    }, []


def _gth_evidence(facts: Mapping[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    reasons: list[str] = []
    sources = (
        (
            "gth_level_manual_candidate",
            _map(facts.get("gth_evidence")),
            "gth_confirmed_level_candidate_unavailable",
        ),
        (
            "gth_dip_reclaim_evidence",
            _map(facts.get("gth_dip_reclaim_evidence")),
            "gth_dip_reclaim_evidence_unavailable",
        ),
    )
    for source, evidence, unavailable_reason in sources:
        evidence_reasons = list(map(str, evidence.get("block_reasons") or ()))
        eligible = (
            evidence.get("manual_action_eligible") is True
            or evidence.get("selector_evidence_eligible") is True
        )
        if evidence.get("status") not in {"manual_ready", "selector_candidate"} or not eligible:
            reasons.extend(evidence_reasons or [unavailable_reason])
            continue
        path_kind = str(evidence.get("path_kind") or "")
        if path_kind.startswith("trend_transition_"):
            reasons.append("trend_background_cannot_authorize_entry")
            continue
        direction = _direction(evidence.get("direction"))
        if not direction:
            reasons.append("gth_candidate_direction_unavailable")
            continue
        target = _number(evidence.get("target_spx"))
        stop = _number(evidence.get("invalidation_spx"))
        if target is None or stop is None:
            reasons.extend(
                ["gth_spx_target_or_invalidation_unavailable", *evidence_reasons]
            )
            continue
        snapshot = _map(evidence.get("exact_spread_snapshot"))
        setup = (
            "FAILED_BREAK_RECLAIM"
            if any(token in path_kind for token in ("rejection", "reclaim", "dip"))
            else "TREND_PULLBACK"
        )
        return {
            "setup_kind": setup,
            "direction": direction,
            "trigger_level": _number(evidence.get("trigger_level")),
            "target_spx": target,
            "invalidation_spx": stop,
            "long": _gth_leg(snapshot.get("long"), evidence.get("long_contract_id")),
            "short": _gth_leg(snapshot.get("short"), evidence.get("short_contract_id")),
            "valid_until": evidence.get("valid_until"),
            "source": source,
            "geometry_source": source,
            "source_block_reasons": evidence_reasons,
            "edge_authority": evidence.get("edge_authority"),
            "edge_authority_reason": evidence.get("edge_authority_reason"),
        }, []
    return None, list(dict.fromkeys(reasons))


def _butterfly_candidates(
    payload: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    latest: LatestState,
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> list[dict[str, Any]]:
    if not DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        return []
    frame = _map(payload.get("option_structure_frame"))
    expiry = str(frame.get("front_expiry") or "")
    if not expiry:
        return []
    rows: list[dict[str, Any]] = []
    if regime.get("terminal_state") == "PIN_STABLE":
        pin = _map(regime.get("pin"))
        for ranked in pin.get("top_centers") or ():
            center = _number(_map(ranked).get("center"))
            if center is None:
                continue
            for width in WIDTHS:
                for right in ("C", "P"):
                    row = _butterfly_candidate(
                        facts,
                        latest,
                        expiry,
                        center=center,
                        width=width,
                        right=right,
                        now=now,
                        policy=policy,
                        source="stable_pin_butterfly",
                        setup_kind="STABLE_PIN",
                        direction="NEUTRAL",
                        thesis_direction="NEUTRAL",
                        payoff_shape="PIN_CONCENTRATED",
                        manual_authority_eligible=True,
                        selection_prior=float(_map(ranked).get("score") or 0.0),
                        pin=pin,
                        geometry_source=None,
                    )
                    if row:
                        rows.append(row)
    trigger = _map(facts.get("trigger"))
    direction = _direction(trigger.get("direction"))
    if trigger.get("phase") == "confirmed" and direction:
        target, stop, geometry_source = resolve_geometry(payload, facts, direction, _number(trigger.get("level")))
        center = _round_to_strike(target)
        if center is not None and stop is not None:
            for width in WIDTHS:
                for right in ("C", "P"):
                    row = _butterfly_candidate(
                        facts,
                        latest,
                        expiry,
                        center=center,
                        width=width,
                        right=right,
                        now=now,
                        policy=policy,
                        source="directional_confirmation_butterfly",
                        setup_kind="CONFIRMATION_TARGET_PIN",
                        direction="NEUTRAL",
                        thesis_direction=direction,
                        payoff_shape="TARGET_CONCENTRATED",
                        # Research alternative only: butterfly hard gates do not yet
                        # cover anti-chase / ATR band / center-migration checks, so a
                        # directional thesis must not gain manual authority here.
                        manual_authority_eligible=False,
                        selection_prior=0.0,
                        pin={},
                        geometry_source=geometry_source,
                        target_spx=center,
                        invalidation_spx=stop,
                    )
                    if row:
                        rows.append(row)
    return rows


def _butterfly_candidate(
    facts: Mapping[str, Any],
    latest: LatestState,
    expiry: str,
    *,
    center: float,
    width: float,
    right: str,
    now: datetime,
    policy: StrategyPolicy,
    source: str,
    setup_kind: str,
    direction: str,
    thesis_direction: str,
    payoff_shape: str,
    manual_authority_eligible: bool,
    selection_prior: float,
    pin: Mapping[str, Any],
    geometry_source: str | None,
    target_spx: float | None = None,
    invalidation_spx: float | list[float] | None = None,
) -> dict[str, Any]:
    legs = [
        _option_leg(latest, expiry, strike, right, require_schwab=True)
        for strike in (center - width, center, center + width)
    ]
    if any(not leg for leg in legs):
        return {}
    quote = conservative_butterfly_bbo(
        *legs,
        now=now,
        max_quote_age_seconds=policy.quote_max_age_seconds,
        max_source_skew_seconds=policy.quote_max_skew_seconds,
    )
    economics: dict[str, Any] = {}
    if quote.get("status") == "ready":
        try:
            economics = butterfly_economics(center=center, width=width, net_debit=float(quote["ask"]))
        except ValueError:
            economics = {}
    strategy_type = f"{'CALL' if right == 'C' else 'PUT'}_BUTTERFLY"
    strikes = [center - width, center, center + width]
    candidate_id = _candidate_id(facts.get("session_date"), strategy_type, expiry, strikes, right)
    quote_valid = _quote_valid_until(legs, now=now, policy=policy)
    identity = (facts.get("session_date"), candidate_id, *(leg["contract_id"] for leg in legs))
    score = selection_prior + _butterfly_selection_score(economics, quote, width)
    return {
        "candidate_id": candidate_id,
        "strategy_type": strategy_type,
        "setup_kind": setup_kind,
        "direction": direction,
        "thesis_direction": thesis_direction,
        "payoff_shape": payoff_shape,
        "manual_authority_eligible": manual_authority_eligible,
        "opportunity_id": f"strategy-opportunity:{_hash(identity)[:24]}",
        "target_spx": target_spx if target_spx is not None else center,
        "invalidation_spx": invalidation_spx if invalidation_spx is not None else [center - width, center + width],
        "center": center,
        "width": width,
        "right": right,
        "legs": legs,
        "quote": quote,
        "economics": economics,
        "selection_score": round(score, 4),
        "pin": dict(pin),
        "quote_valid_until": quote_valid.isoformat() if quote_valid else now.isoformat(),
        "opportunity_valid_until": (now + timedelta(seconds=policy.opportunity_ttl_seconds)).isoformat(),
        "source": source,
        "geometry_source": geometry_source,
        "automatic_ordering": False,
        "manual_action_only": True,
    }


def _intent_spread(value: object, latest: LatestState) -> Mapping[str, Any]:
    intent = _map(value)
    if intent.get("status") != "trade_ready":
        return {}
    parts = str(intent.get("contract_id") or "").split(":")
    if len(parts) < 6 or parts[-1] not in {"C", "P"}:
        return {}
    try:
        strike = float(parts[-2])
    except ValueError:
        return {}
    expiry, right = parts[-3], parts[-1]
    short_strike = strike + 10.0 if right == "C" else strike - 10.0
    long = _option_leg(latest, expiry, strike, right)
    short = _option_leg(latest, expiry, short_strike, right)
    return {"long": long, "short": short} if long and short else {}


def _confirmed_trigger_spread(facts: Mapping[str, Any], direction: str) -> Mapping[str, Any]:
    evidence = _map(facts.get("gth_evidence"))
    if _direction(evidence.get("direction")) != direction:
        return {}
    snapshot = _map(evidence.get("exact_spread_snapshot"))
    long = _gth_leg(snapshot.get("long"), evidence.get("long_contract_id"))
    short = _gth_leg(snapshot.get("short"), evidence.get("short_contract_id"))
    expected_right = "C" if direction == "UP" else "P"
    if any(
        not str(leg.get("contract_id") or "").startswith("option:SPX:SPXW:")
        or leg.get("right") != expected_right
        for leg in (long, short)
    ):
        return {}
    return {"long": long, "short": short}


def _facts_wall_ladder_geometry(
    facts: Mapping[str, Any], direction: str | None, trigger: float | None
) -> tuple[float | None, float | None]:
    spot, structure = _number(_map(facts.get("spot")).get("spx")), _map(facts.get("structure"))
    if spot is None or direction not in {"UP", "DOWN"}:
        return None, None
    target = _number(structure.get("call_wall" if direction == "UP" else "put_wall"))
    levels = [
        _number(structure.get("put_wall")),
        *_flip_values(structure.get("flip_zone")),
        _number(structure.get("zero_gamma")),
        _number(structure.get("call_wall")),
        trigger,
    ]
    if direction == "UP":
        stop = max((value for value in levels if value is not None and value < spot), default=None)
    else:
        stop = min((value for value in levels if value is not None and value > spot), default=None)
    return target, stop


def _option_leg(
    latest: LatestState,
    expiry: str,
    strike: float,
    right: str,
    *,
    require_schwab: bool = False,
) -> dict[str, Any]:
    contract_id = InstrumentId.option(
        "SPX",
        expiry=expiry,
        strike=strike,
        right=right,
        trading_class="SPXW",
    ).canonical_id
    quote = latest.best_quote(contract_id)
    if quote is None or (require_schwab and quote.provider.value != "schwab"):
        return {}
    return {
        "contract_id": contract_id,
        "strike": strike,
        "right": right,
        "provider": quote.provider.value,
        "bid": quote.bid,
        "ask": quote.ask,
        "source_at": quote_source_at(quote).isoformat(),
    }


def _gth_leg(value: object, contract_id: object) -> dict[str, Any]:
    leg, parts = dict(_map(value)), str(contract_id or "").split(":")
    if len(parts) >= 2:
        try:
            leg["strike"] = float(parts[-2])
        except ValueError:
            pass
        leg["right"] = parts[-1].upper()
    leg["contract_id"] = contract_id
    return leg


def _quote_valid_until(
    legs: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]],
    *,
    now: datetime,
    policy: StrategyPolicy,
) -> datetime | None:
    times = [_time(leg.get("source_at")) for leg in legs]
    if any(value is None for value in times):
        return None
    return min(value for value in times if value) + timedelta(seconds=policy.quote_max_age_seconds)


def _vertical_selection_score(economics: Mapping[str, Any], quote: Mapping[str, Any]) -> float:
    loss = _number(economics.get("max_loss_points"))
    gain = _number(economics.get("max_gain_points"))
    if loss is None or gain is None or loss <= 0.0:
        return 0.0
    spread = abs(float(quote.get("ask", 0.0)) - float(quote.get("bid", 0.0)))
    return round(gain / loss - 0.05 * spread / loss, 4)


def _butterfly_selection_score(
    economics: Mapping[str, Any], quote: Mapping[str, Any], width: float
) -> float:
    loss = _number(economics.get("max_loss_points"))
    gain = _number(economics.get("max_gain_points"))
    if loss is None or gain is None or loss <= 0.0:
        return 0.0
    spread = abs(float(quote.get("ask", 0.0)) - float(quote.get("bid", 0.0)))
    return min(gain / loss, 3.0) * 0.05 - 0.01 * width / 5.0 - 0.02 * spread / loss


def _candidate_id(
    session_date: object,
    strategy_type: str,
    expiry: str | None,
    strikes: list[float],
    right: str,
) -> str:
    return _hash((session_date, strategy_type, expiry, [round(float(value), 4) for value in strikes], right))[:16]


def _expiry_from_legs(evidence: Mapping[str, Any]) -> str | None:
    long = _map(evidence.get("long"))
    short = _map(evidence.get("short"))
    return _expiry_from_contract(long.get("contract_id")) or _expiry_from_contract(short.get("contract_id"))


def _expiry_from_contract(value: object) -> str | None:
    parts = str(value or "").split(":")
    return parts[-3] if len(parts) >= 6 else None


def _round_to_strike(value: float | None) -> float | None:
    return round(float(value) / 5.0) * 5.0 if value is not None else None


def _flip_values(value: object) -> list[float | None]:
    if isinstance(value, (list, tuple)):
        return [_number(item) for item in value[:2]]
    mapped = _map(value)
    return [_number(mapped.get("low")), _number(mapped.get("high"))] if mapped else []


def _direction(value: object) -> str | None:
    normalized = str(value or "").upper()
    return normalized if normalized in {"UP", "DOWN"} else None


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
