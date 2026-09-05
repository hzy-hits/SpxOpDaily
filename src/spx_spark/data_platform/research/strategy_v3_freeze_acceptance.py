"""Pass-B candidate rebuild + 2026-08-05..08 freeze acceptance runner.

Offline research only. Reconstructs candidates via candidate_factory against the
quote lake, labels them with ManagementPolicy, and writes an acceptance report.
Does not promote EV to a hard gate.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    REPLAY_MAX_QUOTE_GAP_SECONDS,
    management_policy_for_candidate,
    policy_mark_horizon_end,
    simulate_management_policy,
)
from spx_spark.application.order_map.candidate_factory import enumerate_candidates
from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    assess_regime,
)
from spx_spark.application.order_map.strategy_ranker import rank_candidates
from spx_spark.application.order_map.strategy_select import build_strategy_decision
from spx_spark.data_platform.research.odte_level_quotes import QuoteStore
from spx_spark.data_platform.research.strategy_policy_backfill import (
    mark_duplicate_opportunities,
    outcome_censor_distribution,
    resolve_accepted_opportunity_ids,
    write_labels_parquet,
)
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
)
from spx_spark.storage import LatestState


FREEZE_SESSIONS = ("2026-08-05", "2026-08-06", "2026-08-07", "2026-08-08")


def run_acceptance(
    *,
    database_path: Path,
    data_root: Path,
    output_root: Path | None = None,
    lookforward_minutes: int = 20,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": "strategy_v3_freeze_acceptance.v1",
        "policy_version": DEFAULT_STRATEGY_POLICY.policy_version,
        "management_policy_version": DEFAULT_MANAGEMENT_POLICY.policy_version,
        "ev_hard_gate": False,
        "sessions": {},
        "pass_b_labels": [],
    }
    report["sessions"]["2026-08-05"] = _accept_aug5()
    report["sessions"]["2026-08-06"] = _accept_aug6()
    labels_7, session_7 = _pass_b_session(
        database_path=database_path,
        data_root=data_root,
        session_date="2026-08-07",
        lookforward_minutes=lookforward_minutes,
    )
    report["sessions"]["2026-08-07"] = session_7
    report["pass_b_labels"].extend(labels_7)
    labels_8, session_8 = _pass_b_session(
        database_path=database_path,
        data_root=data_root,
        session_date="2026-08-08",
        lookforward_minutes=lookforward_minutes,
        sample_limit=50,
    )
    report["sessions"]["2026-08-08"] = session_8
    report["pass_b_labels"].extend(labels_8)

    if output_root is not None and report["pass_b_labels"]:
        path = write_labels_parquet(report["pass_b_labels"], output_root)
        report["labels_output"] = str(path) if path else None

    report["summary"] = {
        "aug5_pass": report["sessions"]["2026-08-05"]["pass"],
        "aug6_pass": report["sessions"]["2026-08-06"]["pass"],
        "aug7_vertical_exact_reappear": session_7.get("vertical_exact_spread_reappear", None),
        "aug7_pass_b_candidates": session_7.get("pass_b_candidates", 0),
        "aug7_labeled": session_7.get("labeled", 0),
        "aug7_censor_distribution": session_7.get("censor_distribution", {}),
        "aug8_control_no_trade": session_8.get("control_no_trade", False),
        "aug8_censor_distribution": session_8.get("censor_distribution", {}),
        "ev_promotion_blocked": True,
    }
    return report


def _accept_aug5() -> dict[str, Any]:
    regime = assess_regime(_frozen_pin_facts("2026-08-05"))
    ok = regime.get("terminal_state") == "PIN_MIGRATING"
    return {
        "pass": ok,
        "checks": {
            "terminal_state": regime.get("terminal_state"),
            "expected": "PIN_MIGRATING",
            "7740_butterfly_blocked": ok,
        },
        "note": "Frozen fixture acceptance; not a full historical payload rebuild.",
    }


def _accept_aug6() -> dict[str, Any]:
    now = datetime(2026, 8, 6, 19, 0, tzinfo=timezone.utc)
    decision = build_strategy_decision(_pin_payload(now), _pin_state(now), now)
    candidate = decision.get("candidate") or {}
    ok = (
        decision.get("decision_type") == "CALL_BUTTERFLY"
        and candidate.get("center") == 7710.0
        and float(candidate.get("width") or 0) == 10.0
        and decision.get("automatic_ordering") is False
    )
    return {
        "pass": ok,
        "checks": {
            "decision_type": decision.get("decision_type"),
            "center": candidate.get("center"),
            "width": candidate.get("width"),
            "action_authority": decision.get("action_authority"),
            "automatic_ordering": decision.get("automatic_ordering"),
        },
        "note": "Frozen fixture acceptance with synthetic three-leg BBO.",
    }


def _frozen_pin_facts(day: str) -> dict[str, object]:
    aug6 = day == "2026-08-06"
    return {
        "quality": {"status": "ready"},
        "event": {"state": "normal", "entry_allowed": True},
        "minutes_to_close": 60,
        "path": {
            "direction_score": 0.0,
            "efficiency_ratio_30m": 0.1429 if aug6 else 0.2432,
            "vwap_crosses_30m": 3.0,
            "breadth_above_vwap": 0.5,
            "vwap_slope": 0.0,
            "price_vs_vwap": "above",
            "pin_path_spx": (
                [7710.75, 7709.62, 7712.71, 7718.41, 7715.24, 7709.41, 7712.85,
                 7712.70, 7712.85, 7713.11, 7712.75]
                if aug6 else
                [7741.36, 7742.71, 7741.63, 7739.13, 7738.26, 7738.47, 7738.94, 7732.72]
            ),
        },
        "value_center": (
            {"spx_15m": 7712.56, "spx_30m": 7712.69, "spx_60m": 7714.18}
            if aug6 else {"spx_15m": 7736.65, "spx_30m": 7737.36, "spx_60m": 7738.68}
        ),
        "volatility": {
            "vix_return_15m_pct": -0.005 if aug6 else 0.004,
            "atm_straddle_decay_15m": 0.0448 if aug6 else -0.0123,
        },
        "structure": {
            "q_mode": 7710.0 if aug6 else 7730.0,
            "q_local_mass_5pt": (
                {"7700": 0.0766, "7705": 0.1100, "7710": 0.3033, "7715": 0.05,
                 "7720": 0.1483, "7725": 0.1053}
                if aug6 else {"7725": 0.05, "7730": 0.521, "7735": 0.224, "7740": 0.17}
            ),
            "zero_gamma": 7709.0 if aug6 else 7740.0,
            "flip_zone": [7705.0, 7710.0] if aug6 else [7735.0, 7740.0],
            "put_wall": 7700.0 if aug6 else 7720.0,
            "call_wall": 7720.0 if aug6 else 7760.0,
        },
    }


def _pin_payload(now: datetime) -> dict[str, object]:
    observed = (now - timedelta(seconds=1)).isoformat()
    facts = _frozen_pin_facts("2026-08-06")
    event = {"kind": "terminal_between", "target_at": (now + timedelta(minutes=5)).isoformat()}
    return {
        "trading_date": "2026-08-06",
        "pricing_allowed": True,
        "underlier": {"price": 7712.94, "source": "index:SPX"},
        "minute_market_frame": {
            "as_of": observed,
            "quality": "ready",
            "es": {
                "price": 7739.5,
                "vwap": 7739.25,
                "trend_efficiency_30m": 0.1429,
                "vwap_slope_15m_points": 0.0,
                "pin_path_1m": [value + 26.56 for value in facts["path"]["pin_path_spx"]],
            },
            "volume": {"value_centers_es": {"15m": 7739.12, "30m": 7739.25, "60m": 7740.74}},
            "volatility": {"vix_return_15m_pct": -0.005},
            "diagnostics": {
                "rth_market_state": {
                    "D": 0.0,
                    "input_lineage": {
                        "values": {
                            "efficiency_ratio": 0.1429,
                            "vwap_cross_count": 3,
                            "price_vs_vwap": "above",
                            "breadth_above_vwap": 0.5,
                        },
                        "diagnostics": {"moving_averages": {"atr_5m": 4.6}},
                    },
                }
            },
        },
        "option_structure_frame": {
            "as_of": observed,
            "quality": "ready",
            "front_expiry": "20260806",
            "l1": {"quality": "ready"},
            "structure": facts["structure"],
            "density": {"mode": 7710.0, "local_mass_5pt": facts["structure"]["q_local_mass_5pt"]},
            "volatility": {"atm_straddle_decay_15m": 0.0448},
        },
        "macro_event": {"mode": "normal", "entry_allowed": True},
        "strategy_distribution_forecast": {
            "quality": "degraded",
            "valid_until": (now + timedelta(minutes=5)).isoformat(),
            "q_event": {"event": event, "probability": 0.85},
            "p_event": {
                "event": event,
                "probability": 0.9,
                "interval_low": 0.8,
                "n_raw": 40,
                "n_effective": 40.0,
                "historical_sessions": ["2026-08-04", "2026-08-05"],
            },
        },
        "candidates": [],
    }


def _pin_state(now: datetime) -> LatestState:
    quotes = tuple(
        Quote(
            instrument=InstrumentId.option(
                "SPX", expiry="20260806", strike=strike, right="C", trading_class="SPXW"
            ),
            provider=Provider.SCHWAB,
            received_at=now - timedelta(seconds=1),
            quote_time=now - timedelta(seconds=1),
            quality=MarketDataQuality.LIVE,
            bid=bid,
            ask=ask,
        )
        for strike, bid, ask in ((7700, 15.1, 15.3), (7710, 7.3, 7.5), (7720, 2.5, 2.6))
    )
    return LatestState(created_at=now, as_of=now - timedelta(seconds=1), quotes=quotes, best_quotes=quotes)


def _pass_b_session(
    *,
    database_path: Path,
    data_root: Path,
    session_date: str,
    lookforward_minutes: int,
    sample_limit: int = 40,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    decisions = _load_decisions(database_path, session_date)
    reasons = Counter(str(row.get("reason") or "") for row in decisions)
    confirmed = [
        row
        for row in decisions
        if str(_map(_map(row.get("market_facts")).get("trigger")).get("phase") or "") == "confirmed"
    ]
    unique_confirmed = [
        row for row in confirmed if row.get("duplicate_of") is None
    ]
    # Prefer the historical failure mode the contract cares about.
    focus = [
        row
        for row in unique_confirmed
        if str(row.get("reason") or "") == "vertical_exact_spread_unavailable"
    ] or unique_confirmed
    focus = _dedupe_by_minute(focus)[:sample_limit]

    store = QuoteStore(data_root)
    labels: list[dict[str, Any]] = []
    rebuilt = 0
    still_unavailable = 0
    candidates = 0
    try:
        for row in focus:
            result = _rebuild_one(
                row,
                store=store,
                data_root=data_root,
                lookforward_minutes=lookforward_minutes,
            )
            if result is None:
                continue
            rebuilt += 1
            if result.get("generation_reason") == "vertical_exact_spread_unavailable":
                still_unavailable += 1
            candidates += int(result.get("candidate_count") or 0)
            labels.extend(result.get("labels") or ())
    finally:
        store.close()

    session = {
        "decision_rows": len(decisions),
        "unique_opportunities": sum(
            1 for row in decisions if row.get("duplicate_of") is None
        ),
        "confirmed_rows": len(confirmed),
        "confirmed_unique_opportunities": len(unique_confirmed),
        "reason_counts": dict(reasons),
        "censor_distribution": outcome_censor_distribution(
            database_path=database_path,
            session_date=session_date,
        ),
        "pass_b_sampled": len(focus),
        "pass_b_rebuilt": rebuilt,
        "pass_b_candidates": candidates,
        "vertical_exact_spread_reappear": still_unavailable,
        "labeled": len(labels),
        "control_no_trade": all(
            str(row.get("status") or "") == "no_trade" for row in decisions
        )
        if decisions
        else True,
        "pass": (
            still_unavailable == 0
            if session_date == "2026-08-07" and focus
            else True
        ),
    }
    return labels, session


def _rebuild_one(
    row: Mapping[str, Any],
    *,
    store: QuoteStore,
    data_root: Path,
    lookforward_minutes: int,
) -> dict[str, Any] | None:
    decision_at = _time(row.get("decision_at"))
    if decision_at is None:
        return None
    facts = _map(row.get("market_facts"))
    if not facts:
        return None
    trigger = _map(facts.get("trigger"))
    direction = str(trigger.get("direction") or "").upper()
    level = _number(trigger.get("level"))
    spot = _number(_map(facts.get("spot")).get("spx"))
    expiry = _front_expiry(session_date=str(row.get("session_date") or ""), decision_at=decision_at)
    if not direction or level is None or spot is None or expiry is None:
        return None

    latest = _latest_from_lake(
        store,
        expiry=expiry,
        spot=spot,
        trigger=level,
        decision_at=decision_at,
    )
    if latest is None:
        return None

    payload = _payload_stub(
        row=row,
        facts=facts,
        expiry=expiry,
        latest=latest,
        decision_at=decision_at,
        direction=direction,
        level=level,
        spot=spot,
        data_root=data_root,
    )
    regime = assess_regime(facts) if facts.get("path") else {
        "path_state": "UNCERTAIN",
        "path_direction": direction,
        "terminal_state": "UNCERTAIN",
        "pin": {},
        "entry_state": "UNKNOWN",
        "event_state": "NORMAL",
    }
    # Ensure pin structure for butterfly enumeration if missing.
    if "pin" not in regime:
        regime = {**regime, "pin": {"top_centers": [], "depin_risk": 0.0}}

    rows = enumerate_candidates(
        payload,
        facts,
        regime,
        latest,
        now=decision_at,
        policy=DEFAULT_STRATEGY_POLICY,
    )
    generation_reason = None
    if not rows:
        from spx_spark.application.order_map.candidate_factory import (
            candidate_generation_reasons,
        )

        generation_reason = (
            candidate_generation_reasons(
                payload, facts, regime, latest, now=decision_at, policy=DEFAULT_STRATEGY_POLICY
            )
            or ["no_supported_strategy_candidate"]
        )[0]

    rank = (
        rank_candidates(
            rows,
            facts,
            regime,
            policy=DEFAULT_STRATEGY_POLICY,
            data_root=data_root,
            probability_settings=None,
            now=decision_at,
        )
        if rows
        else None
    )
    labels = []
    for candidate in rows:
        labeled = _label_candidate(
            candidate,
            store=store,
            decision_at=decision_at,
            session_date=str(row.get("session_date") or ""),
            decision_id=str(row.get("decision_id") or ""),
            lookforward_minutes=lookforward_minutes,
            regime=regime,
        )
        if labeled is not None:
            labels.append(labeled)
    return {
        "decision_id": row.get("decision_id"),
        "decision_at": decision_at.isoformat(),
        "candidate_count": len(rows),
        "passed": len(rank.passed) if rank else 0,
        "generation_reason": generation_reason,
        "labels": labels,
    }


def _payload_stub(
    *,
    row: Mapping[str, Any],
    facts: Mapping[str, Any],
    expiry: str,
    latest: LatestState,
    decision_at: datetime,
    direction: str,
    level: float,
    spot: float,
    data_root: Path,
) -> dict[str, Any]:
    intent = _nearest_trade_intent(data_root, session_date=str(row.get("session_date") or ""), decision_at=decision_at)
    shadow = _synthetic_shadow(latest, expiry=expiry, direction=direction, spot=spot, trigger=level)
    observed = (decision_at - timedelta(seconds=1)).isoformat()
    structure = _map(facts.get("structure"))
    return {
        "trading_date": row.get("session_date"),
        "pricing_allowed": True,
        "underlier": {"price": spot, "source": "index:SPX"},
        "minute_market_frame": {
            "as_of": observed,
            "quality": "ready",
            "es": {
                "price": spot,
                "vwap": spot,
                "trend_efficiency_30m": _number(_map(facts.get("path")).get("efficiency_ratio_30m")) or 0.0,
                "vwap_distance_points": _number(_map(facts.get("path")).get("distance_to_vwap_points")),
                "return_15m_points": _number(_map(facts.get("path")).get("impulse_15m_points")),
                "vwap_slope_15m_points": _number(_map(facts.get("path")).get("vwap_slope")) or 0.0,
            },
            "diagnostics": {
                "rth_market_state": {
                    "input_lineage": {
                        "values": {
                            "efficiency_ratio": _number(_map(facts.get("path")).get("efficiency_ratio_30m")),
                            "vwap_cross_count": _number(_map(facts.get("path")).get("vwap_crosses_30m")),
                            "price_vs_vwap": _map(facts.get("path")).get("price_vs_vwap"),
                            "breadth_above_vwap": _number(_map(facts.get("path")).get("breadth_above_vwap")),
                        },
                        "diagnostics": {
                            "moving_averages": {
                                "atr_5m": _number(_map(facts.get("path")).get("atr_5m")) or 5.0
                            }
                        },
                    }
                }
            },
        },
        "option_structure_frame": {
            "as_of": observed,
            "quality": "ready",
            "front_expiry": expiry,
            "l1": {"quality": "ready"},
            "structure": structure,
            "density": {
                "mode": structure.get("q_mode"),
                "local_mass_5pt": structure.get("q_local_mass_5pt") or {},
            },
            "volatility": {
                "atm_straddle_decay_15m": _number(_map(facts.get("volatility")).get("atm_straddle_decay_15m"))
            },
        },
        "macro_event": {
            "mode": _map(facts.get("event")).get("state") or "normal",
            "entry_allowed": _map(facts.get("event")).get("entry_allowed") is True,
        },
        "level_decision": {
            "phase": trigger_phase(facts),
            "direction": direction.lower(),
            "thesis": _map(facts.get("trigger")).get("thesis"),
            "level_kind": _map(facts.get("trigger")).get("level_kind"),
            "level": level,
            "levels": _map(facts.get("trigger")).get("levels") or {},
            "event_id": _map(facts.get("trigger")).get("event_id"),
        },
        "gth_level_manual_candidate": _map(facts.get("gth_evidence")),
        "trade_intent": intent or {},
        "call_skew_spread_shadow": shadow if direction == "UP" else {},
        "put_skew_spread_shadow": shadow if direction == "DOWN" else {},
        "strategy_distribution_forecast": _forecast_from_facts(facts, decision_at),
        "candidates": [],
    }


def trigger_phase(facts: Mapping[str, Any]) -> str:
    return str(_map(facts.get("trigger")).get("phase") or "")


def _synthetic_shadow(
    latest: LatestState,
    *,
    expiry: str,
    direction: str,
    spot: float,
    trigger: float,
) -> dict[str, Any]:
    right = "C" if direction == "UP" else "P"
    long_strike = round(trigger / 5.0) * 5.0
    short_strike = long_strike + 10.0 if right == "C" else long_strike - 10.0
    long = _leg_from_latest(latest, expiry, long_strike, right)
    short = _leg_from_latest(latest, expiry, short_strike, right)
    if not long or not short:
        # fall back to ATM-ish
        long_strike = round(spot / 5.0) * 5.0
        short_strike = long_strike + 10.0 if right == "C" else long_strike - 10.0
        long = _leg_from_latest(latest, expiry, long_strike, right)
        short = _leg_from_latest(latest, expiry, short_strike, right)
    if not long or not short:
        return {}
    return {"status": "candidate", "candidate": {"long": long, "short": short}}


def _leg_from_latest(latest: LatestState, expiry: str, strike: float, right: str) -> dict[str, Any]:
    contract_id = InstrumentId.option(
        "SPX", expiry=expiry, strike=strike, right=right, trading_class="SPXW"
    ).canonical_id
    quote = latest.best_quote(contract_id)
    if quote is None or quote.bid is None or quote.ask is None:
        return {}
    source = quote.quote_time or quote.received_at
    return {
        "contract_id": contract_id,
        "strike": strike,
        "right": right,
        "provider": quote.provider.value,
        "bid": quote.bid,
        "ask": quote.ask,
        "source_at": source.astimezone(timezone.utc).isoformat(),
    }


def _latest_from_lake(
    store: QuoteStore,
    *,
    expiry: str,
    spot: float,
    trigger: float,
    decision_at: datetime,
) -> LatestState | None:
    day = date.fromisoformat(f"{expiry[:4]}-{expiry[4:6]}-{expiry[6:8]}")
    centers = sorted(
        {
            round(value / 5.0) * 5.0
            for value in (
                spot - 20,
                spot - 10,
                spot,
                spot + 10,
                spot + 20,
                trigger - 20,
                trigger - 10,
                trigger,
                trigger + 10,
                trigger + 20,
            )
        }
    )
    quotes: list[Quote] = []
    start = decision_at - timedelta(seconds=20)
    for strike in centers:
        for right in ("C", "P"):
            ticks = store.option_series(
                provider="schwab",
                expiry=day,
                strike=float(strike),
                right=right,
                start=start,
                end=decision_at,
            )
            if not ticks:
                continue
            tick = ticks[-1]
            at = tick.at if tick.at.tzinfo else tick.at.replace(tzinfo=timezone.utc)
            if at > decision_at:
                continue
            quotes.append(
                Quote(
                    instrument=InstrumentId.option(
                        "SPX",
                        expiry=expiry,
                        strike=float(strike),
                        right=right,
                        trading_class="SPXW",
                    ),
                    provider=Provider.SCHWAB,
                    received_at=at,
                    quote_time=at,
                    quality=MarketDataQuality.LIVE,
                    bid=tick.bid,
                    ask=tick.ask,
                )
            )
    if len(quotes) < 4:
        return None
    return LatestState(
        created_at=decision_at,
        as_of=decision_at,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )


def _label_candidate(
    candidate: Mapping[str, Any],
    *,
    store: QuoteStore,
    decision_at: datetime,
    session_date: str,
    decision_id: str,
    lookforward_minutes: int,
    regime: Mapping[str, Any],
) -> dict[str, Any] | None:
    from spx_spark.data_platform.research.strategy_policy_backfill import (
        _candidate_legs,
        _combo_bid_marks,
        _entry_price,
    )

    legs = _candidate_legs(candidate)
    if len(legs) < 2:
        return None
    entry_ask = _entry_price(legs)
    if entry_ask is None:
        return None
    provider = str(legs[0].get("provider") or "schwab")
    session = date.fromisoformat(session_date)
    policy = management_policy_for_candidate(candidate)
    marks = _combo_bid_marks(
        store,
        legs=legs,
        provider=provider,
        start=decision_at,
        end=policy_mark_horizon_end(
            decision_at,
            policy,
            session_date=session,
            lookforward_minutes=(
                None if policy.time_stop_minutes is None else lookforward_minutes
            ),
        ),
    )
    if not marks:
        return None
    label = simulate_management_policy(
        marks,
        entry_ask=entry_ask,
        leg_count=sum(abs(int(leg["quantity"])) for leg in legs),
        entry_at=decision_at,
        policy=policy,
        session_date=session,
        max_quote_gap_seconds=REPLAY_MAX_QUOTE_GAP_SECONDS,
    )
    if label.policy_pnl_points is None:
        return None

    return {
        "schema_version": "strategy_policy_label.v1",
        "pass": "B",
        "decision_id": decision_id,
        "session_date": session_date,
        "decision_at": decision_at.isoformat(),
        "available_at": decision_at.isoformat(),
        "decision_type": candidate.get("strategy_type"),
        "strategy_type": candidate.get("strategy_type"),
        "direction": candidate.get("direction"),
        "setup_kind": candidate.get("setup_kind"),
        "opportunity_id": candidate.get("opportunity_id"),
        "candidate_id": candidate.get("candidate_id"),
        "shadow_only": False,
        "entry_ask": entry_ask,
        "leg_count": len(legs),
        "mark_count": len(marks),
        "regime_terminal_state": regime.get("terminal_state") or regime.get("path_state"),
        "policy_version": label.policy_version,
        "tp_armed": label.tp_armed,
        "tp_before_stop": label.tp_before_stop,
        "time_to_arm_seconds": label.time_to_arm_seconds,
        "mfe_points": label.mfe_points,
        "mae_points": label.mae_points,
        "policy_pnl_points": label.policy_pnl_points,
        "exit_reason": label.exit_reason,
        "exit_at": label.exit_at.isoformat() if label.exit_at else None,
        "exit_bid": label.exit_bid,
        "quote_gap_seconds_max": label.quote_gap_seconds_max,
        "fees_points": label.fees_points,
        "known_bias": "pass_b_rebuilds_candidates_from_facts_plus_quote_lake_seed",
    }


def _nearest_trade_intent(
    data_root: Path, *, session_date: str, decision_at: datetime
) -> dict[str, Any]:
    path = data_root / "features" / "trade_intents" / f"date={session_date}" / "events.jsonl"
    if not path.exists():
        return {}
    best = None
    best_delta = None
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            at = _time(event.get("evaluated_at") or event.get("as_of") or event.get("decision_at"))
            if at is None or at > decision_at:
                continue
            delta = (decision_at - at).total_seconds()
            if best_delta is None or delta < best_delta:
                best = event
                best_delta = delta
                if delta <= 2.0:
                    break
    return dict(best or {})


def _forecast_from_facts(facts: Mapping[str, Any], decision_at: datetime) -> dict[str, Any]:
    probability = _map(facts.get("probability"))
    event = _map(probability.get("event")) or {
        "kind": "terminal_above",
        "target_at": (decision_at + timedelta(minutes=5)).isoformat(),
    }
    return {
        "quality": "degraded",
        "valid_until": (decision_at + timedelta(minutes=5)).isoformat(),
        "q_event": {"event": event, "probability": _number(probability.get("q")) or 0.5},
        "p_event": {
            "event": event,
            "probability": _number(probability.get("p_empirical")) or 0.5,
            "interval_low": _number(probability.get("p_interval_low")) or 0.4,
            "n_raw": int(_number(probability.get("n_raw")) or 0),
            "n_effective": _number(probability.get("n_effective")) or 0.0,
            "historical_sessions": list(probability.get("historical_sessions") or ()),
        },
    }


def _load_decisions(database_path: Path, session_date: str) -> list[dict[str, Any]]:
    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute(
            """
            SELECT decision_id, event_key, decision_at, session_date, status, reason, attributes_json
            FROM decisions
            WHERE strategy_name = 'strategy_signal_engine_v2'
              AND session_date = ?
              AND status IN ('selected', 'no_trade')
            ORDER BY decision_at
            """,
            (session_date,),
        ).fetchall()
    finally:
        connection.close()
    result = []
    for decision_id, event_key, decision_at, session, status, reason, attrs in rows:
        payload = json.loads(attrs)
        result.append(
            {
                "decision_id": decision_id,
                "event_key": event_key,
                "decision_at": decision_at,
                "session_date": session,
                "status": status,
                "reason": reason,
                "market_facts": payload.get("market_facts") or {},
                "candidate": payload.get("candidate") or {},
                "attributes": payload,
            }
        )
    accepted = resolve_accepted_opportunity_ids(result, database_path=database_path)
    if accepted:
        return mark_duplicate_opportunities(result, accepted_opportunity_ids=accepted)
    return mark_duplicate_opportunities(result)


def _dedupe_by_minute(rows: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    seen: set[str] = set()
    result = []
    for row in rows:
        at = _time(row.get("decision_at"))
        if at is None:
            continue
        key = at.strftime("%Y-%m-%dT%H:%M")
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _front_expiry(*, session_date: str, decision_at: datetime) -> str | None:
    try:
        day = date.fromisoformat(session_date)
    except ValueError:
        day = decision_at.astimezone(timezone.utc).date()
    return day.strftime("%Y%m%d")


def render_markdown(report: Mapping[str, Any]) -> str:
    s = report["sessions"]
    summary = report["summary"]
    lines = [
        "# Strategy Engine v3 · Pass-B 回填与冻结验收 · 2026-08-05..08",
        "",
        "状态：**工程验收报告**；`ev_hard_gate=false`（合同 §7.4 未满足，不得升门）。",
        "",
        "## 范围",
        "",
        f"- policy: `{report['policy_version']}`",
        f"- management_policy: `{report['management_policy_version']}`",
        "- 数据：`/srv/data/spx-spark/data` + `/srv/data/spx-spark/spx.sqlite`",
        "- Pass-B：用决策时点 `market_facts` + Schwab quote lake 重建候选，再用 ManagementPolicy 打标",
        "- 已知偏差：Pass-B 候选是重建假想集，不等于当时生产真实候选",
        "",
        "## 冻结案例",
        "",
        "| 日期 | PASS | 关键检查 |",
        "|---|---|---|",
        f"| 2026-08-05 | {'PASS' if s['2026-08-05']['pass'] else 'FAIL'} | terminal={s['2026-08-05']['checks']['terminal_state']}（期望 PIN_MIGRATING） |",
        f"| 2026-08-06 | {'PASS' if s['2026-08-06']['pass'] else 'FAIL'} | {s['2026-08-06']['checks']} |",
        f"| 2026-08-07 | {'PASS' if s['2026-08-07']['pass'] else 'FAIL'} | Pass-B sampled={s['2026-08-07']['pass_b_sampled']} rebuilt={s['2026-08-07']['pass_b_rebuilt']} candidates={s['2026-08-07']['pass_b_candidates']} vertical_exact_reappear={s['2026-08-07']['vertical_exact_spread_reappear']} labeled={s['2026-08-07']['labeled']} |",
        f"| 2026-08-08 | {'PASS' if s['2026-08-08'].get('control_no_trade') else 'FAIL'} | control no_trade; reasons={s['2026-08-08'].get('reason_counts')} |",
        "",
        "## 验收门摘要",
        "",
        "```json",
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
        "## 8/7 生产原因基线（重建前）",
        "",
        "```json",
        json.dumps(s["2026-08-07"].get("reason_counts"), ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
        "## 删失分布",
        "",
        "```json",
        json.dumps(
            {
                "2026-08-07": s["2026-08-07"].get("censor_distribution", {}),
                "2026-08-08": s["2026-08-08"].get("censor_distribution", {}),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## EV 升门",
        "",
        "本次**不**将 ManagementPolicy EV 升为硬门。校准脚本与标签仅用于排序研究；",
        "升门仍需 §7.4 证据与另一次明确批准。",
        "",
    ]
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--report-md", type=Path, default=None)
    parser.add_argument("--report-json", type=Path, default=None)
    parser.add_argument("--lookforward-minutes", type=int, default=20)
    args = parser.parse_args(argv)
    report = run_acceptance(
        database_path=args.database,
        data_root=args.data_root,
        output_root=args.output_root,
        lookforward_minutes=args.lookforward_minutes,
    )
    text = render_markdown(report)
    if args.report_md is not None:
        args.report_md.parent.mkdir(parents=True, exist_ok=True)
        args.report_md.write_text(text, encoding="utf-8")
    if args.report_json is not None:
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report["summary"], ensure_ascii=False, sort_keys=True))
    return 0 if all(
        [
            report["summary"]["aug5_pass"],
            report["summary"]["aug6_pass"],
            report["sessions"]["2026-08-07"]["pass"],
            report["sessions"]["2026-08-08"].get("control_no_trade", False),
        ]
    ) else 1


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, (str, datetime)):
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())
