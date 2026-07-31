from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from spx_spark.application.market_features.trade_candidate import (
    PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
    PUT_SHADOW_EXACT_QUOTE_POLICY_VERSION,
)
from spx_spark.application.market_features.trade_intent import trade_intent_policy_version
from spx_spark.data_platform.research.strategy_readiness import (
    DEFAULT_THRESHOLDS,
    _contract_audit,
    _exact_spread_snapshot,
    _material_contract_issues,
    _put_shadow_record,
    _record_role,
    _select_policy_bundle,
    build_strategy_readiness,
    measure_session_completeness,
    validate_strategy_contract,
)
from spx_spark.data_platform.research.strategy_readiness_evidence import (
    _exact_spread_decision,
    _exact_put_shadow_entry,
    count_put_exact_entries,
    duplicate_audit,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.order_map import OrderMapPolicy
from spx_spark.strategy_contract import policy_version


ROLE_POLICIES = {
    "gth_detector_runtime": "gth_detector_runtime_v3_frozen",
    "gth_signal": "gth_signal_v3_frozen",
    "trade_intent": "trade_intent_v3_frozen",
    "trade_candidate": "trade_candidate_v3_frozen",
    "virtual_entry_decision": "virtual_entry_decision_v3_frozen",
    "virtual_lifecycle": "virtual_lifecycle_v3_frozen",
}
PUT_SHADOW_WINDOW_CONTRACT_VERSION = "rth_lanes_0945_1300_put_shadow.v1"


@pytest.mark.parametrize("status", ("trade_ready", "virtual_ready"))
def test_gth_virtual_entry_decision_dual_reads_legacy_and_new_status(
    status: str,
) -> None:
    at = datetime(2026, 7, 15, 3, 0, tzinfo=timezone.utc)
    payload = {
        **_envelope(
            at,
            role="virtual_entry_decision",
            kind="option_spread",
            instrument_id="option:SPX:SPXW:20260715:7500:C|-option:SPX:SPXW:20260715:7520:C",
        ),
        "event": "virtual_entry_decision",
        "decision_id": f"virtual-entry:{status}",
        "source_signal_id": f"gth:{status}",
        "source_kind": "gth_dip_reclaim_call",
        "evaluated_at": at.isoformat(),
        "status": status,
        "terminal": True,
        "position_type": "call_debit_spread",
        "exact_spread_snapshot": _spread_snapshot(at),
        "episode_id": f"virtual:{status}",
        "automatic_ordering": False,
    }
    if status == "virtual_ready":
        payload.update(
            {
                "simulation_only": True,
                "execution_eligible": False,
            }
        )
    record = SimpleNamespace(source="virtual_strategy", payload=payload)

    assert _record_role(record) == "virtual_entry_decision"
    assert _exact_spread_decision(payload)


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _read_rows(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _health_window(day: date) -> tuple[datetime, datetime, datetime, datetime]:
    session = DEFAULT_MARKET_CALENDAR.session(day)
    assert session is not None
    gth_start = datetime.combine(day - timedelta(days=1), datetime.min.time(), tzinfo=ET)
    gth_start = gth_start.replace(hour=20, minute=15)
    gth_end = datetime.combine(day, datetime.min.time(), tzinfo=ET).replace(hour=9, minute=25)
    return gth_start, gth_end, session.open_at, session.close_at


def _put_shadow_window(day: date) -> tuple[datetime, datetime]:
    return (
        datetime.combine(day, datetime.min.time(), tzinfo=ET)
        .replace(hour=9, minute=45)
        .astimezone(timezone.utc),
        datetime.combine(day, datetime.min.time(), tzinfo=ET)
        .replace(hour=13)
        .astimezone(timezone.utc),
    )


def _minute_samples(start: datetime, end: datetime, *, take: int | None = None) -> list[datetime]:
    count = int((end - start).total_seconds() // 60)
    if take is not None:
        count = min(count, take)
    return [start + timedelta(minutes=index, seconds=5) for index in range(count)]


def _write_health(
    root: Path,
    day: date,
    *,
    gth_minutes: int | None = None,
    rth_minutes: int | None = None,
) -> None:
    gth_start, gth_end, rth_start, rth_end = _health_window(day)
    samples = [
        *_minute_samples(gth_start, gth_end, take=gth_minutes),
        *_minute_samples(rth_start, rth_end, take=rth_minutes),
    ]
    _write_rows(
        root / "level_decision_health" / f"date={day.isoformat()}" / "samples.jsonl",
        [
            {
                "at": sample.astimezone(timezone.utc).isoformat(),
                "session_date": day.isoformat(),
                "session_mode": "rth" if rth_start <= sample < rth_end else "globex",
                # Completeness must not use this mixed strategy/data-quality field.
                "quality_ok": False,
                "quality_reason": "structure_change_pending",
            }
            for sample in samples
        ],
    )


def _write_detector_health(root: Path, day: date, *, gth_minutes: int) -> None:
    gth_start, gth_end, _, _ = _health_window(day)
    _write_rows(
        root / "gth_detector_health" / f"date={day.isoformat()}" / "samples.jsonl",
        [
            {
                "schema_version": 1,
                "policy_version": ROLE_POLICIES["gth_detector_runtime"],
                "at": sample.isoformat(),
                "session_date": day.isoformat(),
            }
            for sample in _minute_samples(gth_start, gth_end, take=gth_minutes)
        ],
    )


def _envelope(
    at: datetime,
    *,
    role: str,
    kind: str,
    instrument_id: str,
    valid_for: timedelta = timedelta(minutes=30),
    valid_until: datetime | None = None,
) -> dict[str, object]:
    return {
        "schema_version": 3,
        "policy_version": ROLE_POLICIES[role],
        "valid_until": (valid_until or at + valid_for).isoformat(),
        "coordinate": {"kind": kind, "instrument_id": instrument_id},
        "block_reasons": [],
    }


def _leg_snapshot(at: datetime, *, bid: float, mid: float, ask: float) -> dict[str, object]:
    return {
        "bid": bid,
        "mid": mid,
        "ask": ask,
        "source_at": (at - timedelta(seconds=1)).isoformat(),
        "quality": {"status": "ok"},
    }


def _spread_snapshot(at: datetime, *, bid: float = 4.0, mid: float = 5.0) -> dict[str, object]:
    return {
        "at": at.isoformat(),
        "bid": bid,
        "mid": mid,
        "ask": 6.0,
        "source_at": (at - timedelta(seconds=1)).isoformat(),
        "quality": {"status": "ok"},
        "long": _leg_snapshot(at, bid=10.0, mid=10.5, ask=11.0),
        "short": _leg_snapshot(at, bid=5.0, mid=5.5, ask=6.0),
    }


def _single_snapshot(at: datetime) -> dict[str, object]:
    return _leg_snapshot(at, bid=10.0, mid=10.5, ask=11.0)


def _write_put_shadow_evidence(
    root: Path,
    day: date,
    *,
    index: int,
    write_candidate: bool = True,
    candidate_policy_version: str | None = None,
) -> None:
    _, _, rth_start, _ = _health_window(day)
    intent_at = (rth_start + timedelta(minutes=30)).astimezone(timezone.utc)
    terminal_at = intent_at + timedelta(seconds=1)
    expiry = day.strftime("%Y%m%d")
    intent_id = f"intent:put:{index}"
    contract_id = f"option:SPX:SPXW:{expiry}:7500:P"
    lane = "long_0dte_rth_flip_low_breakdown_put_shadow"
    semantic_key = f"{day.isoformat()}|level_breakout_put|7500.0000|{contract_id}"
    entry_window_start_at, hard_exit_at = _put_shadow_window(day)
    intent = {
        **_envelope(
            intent_at,
            role="trade_intent",
            kind="official_spx",
            instrument_id="index:SPX",
        ),
        "status": "shadow_ready",
        "intent_id": intent_id,
        "event_id": f"level:put:{index}",
        "semantic_key": semantic_key,
        "session_id": day.isoformat(),
        "evaluated_at": intent_at.isoformat(),
        "direction": "down",
        "play": "level_breakout_put",
        "thesis": "breakout",
        "level_kind": "flip_low",
        "contract_id": contract_id,
        "entry_limit": 10.1,
        "strategy_lane": lane,
        "shadow_mode": True,
        "execution_eligible": False,
        "quote_observation_eligible": True,
        "automatic_ordering": False,
        "trade_intent_contract_version": PUT_SHADOW_WINDOW_CONTRACT_VERSION,
        "entry_window_start_at": entry_window_start_at.isoformat(),
        "hard_exit_at": hard_exit_at.isoformat(),
    }
    _write_rows(
        root / "trade_intents" / f"date={day.isoformat()}" / "events.jsonl",
        [intent],
    )
    if not write_candidate:
        return

    candidate_envelope = _envelope(
        intent_at,
        role="trade_candidate",
        kind="option_contract",
        instrument_id=contract_id,
        valid_until=datetime.fromisoformat(str(intent["valid_until"])),
    )
    if candidate_policy_version is not None:
        candidate_envelope["policy_version"] = candidate_policy_version
    armed = {
        **candidate_envelope,
        "event": "candidate_armed",
        "phase": "armed",
        "candidate_id": f"{intent_id}|level:put:{index}",
        "intent_id": intent_id,
        "event_id": intent["event_id"],
        "semantic_key": semantic_key,
        "session_id": day.isoformat(),
        "direction": "down",
        "play": intent["play"],
        "thesis": intent["thesis"],
        "level_kind": intent["level_kind"],
        "strategy_lane": lane,
        "shadow_mode": True,
        "contract_id": contract_id,
        "entry_limit": 10.1,
        "trade_intent_contract_version": PUT_SHADOW_WINDOW_CONTRACT_VERSION,
        "entry_window_start_at": entry_window_start_at.isoformat(),
        "hard_exit_at": hard_exit_at.isoformat(),
        "armed_at": intent_at.isoformat(),
        "automatic_ordering": False,
        "broker_order_state": "not_connected",
        "source_intent": dict(intent),
    }
    terminal = {
        **armed,
        "event": "candidate_terminal",
        "phase": "quote_reached_entry",
        "terminal_at": terminal_at.isoformat(),
        "execution_claim": "none",
        "entry_observation": {
            "at": terminal_at.isoformat(),
            "contract_id": contract_id,
            "entry_limit": 10.1,
            "provider": "schwab",
            "bid": 9.8,
            "mid": 10.0,
            "ask": 10.1,
            "quote_quality": "live",
            "quote_source_at": terminal_at.isoformat(),
            "quote_transport_at": terminal_at.isoformat(),
            "quote_source_age_seconds": 0.0,
            "quote_transport_age_seconds": 0.0,
            "quote_pricing_allowed": True,
            "exact_quote_freshness_ok": True,
            "exact_quote_policy_version": PUT_SHADOW_EXACT_QUOTE_POLICY_VERSION,
            "exact_quote_max_age_seconds": PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
            "entry_condition": "displayed_ask_at_or_below_limit",
        },
    }
    _write_rows(
        root / "trade_candidates" / f"date={day.isoformat()}" / "events.jsonl",
        [armed, terminal],
    )


def _trading_days(start: date, count: int) -> list[date]:
    days: list[date] = []
    current = start
    while len(days) < count:
        if DEFAULT_MARKET_CALENDAR.is_trading_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


def _write_complete_forward_cohort(root: Path, days: list[date]) -> datetime:
    for index, day in enumerate(days):
        _write_health(root, day)
        _write_detector_health(root, day, gth_minutes=790)
        gth_start, _, _, _ = _health_window(day)
        signal_at = (gth_start + timedelta(minutes=60)).astimezone(timezone.utc)
        spread_open_at = signal_at + timedelta(minutes=1)
        spread_close_at = signal_at + timedelta(minutes=10)
        expiry = day.strftime("%Y%m%d")
        event_id = f"gth:{index}"
        episode_id = f"virtual:gth:{index}"
        long_contract = f"option:SPX:SPXW:{expiry}:7500:C"
        short_contract = f"option:SPX:SPXW:{expiry}:7520:C"
        signal = {
            **_envelope(
                signal_at,
                role="gth_signal",
                kind="raw_es",
                instrument_id="future:ES",
            ),
            "event_id": event_id,
            "kind": "gth_dip_reclaim_call",
            "session_date": day.isoformat(),
            "confirmed_at": signal_at.isoformat(),
            "spread": {
                "expiry_date": day.isoformat(),
                "right": "C",
                "long_strike": 7500.0,
                "short_strike": 7520.0,
                "width_points": 20.0,
            },
        }
        _write_rows(
            root / "gth_dip_reclaim" / f"date={day.isoformat()}" / "events.jsonl",
            [signal],
        )

        entry_snapshot = _spread_snapshot(spread_open_at)
        entry_decision = {
            **_envelope(
                spread_open_at,
                role="virtual_entry_decision",
                kind="option_spread",
                instrument_id=f"{long_contract}|-{short_contract}",
            ),
            "event": "virtual_entry_decision",
            "decision_id": f"virtual-entry:{event_id}",
            "source_signal_id": event_id,
            "source_kind": "gth_dip_reclaim_call",
            "session_id": day.isoformat(),
            "evaluated_at": spread_open_at.isoformat(),
            "status": "trade_ready",
            "terminal": True,
            "position_type": "call_debit_spread",
            "exact_spread_snapshot": entry_snapshot,
            "episode_id": episode_id,
        }
        opened = {
            **_envelope(
                spread_open_at,
                role="virtual_lifecycle",
                kind="option_spread",
                instrument_id=f"{long_contract}|-{short_contract}",
            ),
            "event": "virtual_opened",
            "episode_id": episode_id,
            "source_signal_id": event_id,
            "source_kind": "gth_dip_reclaim_call",
            "session_date": day.isoformat(),
            "opened_at": spread_open_at.isoformat(),
            "position_type": "call_debit_spread",
            "contract_id": f"{long_contract}|-{short_contract}",
            "long_contract_id": long_contract,
            "short_contract_id": short_contract,
            "spread_width_points": 20.0,
            "entry_bid": 4.0,
            "entry_mid": 5.0,
            "entry_ask": 6.0,
            "entry_snapshot": entry_snapshot,
            "last": entry_snapshot,
        }
        exit_snapshot = _spread_snapshot(spread_close_at)
        closed = {
            **_envelope(
                spread_close_at,
                role="virtual_lifecycle",
                kind="option_spread",
                instrument_id=f"{long_contract}|-{short_contract}",
                valid_until=spread_close_at,
            ),
            **{
                key: value
                for key, value in opened.items()
                if key
                not in {
                    "schema_version",
                    "policy_version",
                    "valid_until",
                    "coordinate",
                    "block_reasons",
                    "event",
                    "last",
                }
            },
            "event": "virtual_closed",
            "opened_at": spread_open_at.isoformat(),
            "closed_at": spread_close_at.isoformat(),
            "exit_reason": "time_stop",
            "exit_snapshot": exit_snapshot,
            "last": exit_snapshot,
        }

        _write_put_shadow_evidence(root, day, index=index)
        _write_rows(
            root / "virtual_strategy" / f"date={day.isoformat()}" / "events.jsonl",
            [entry_decision, opened, closed],
        )
    last_session = DEFAULT_MARKET_CALENDAR.session(days[-1])
    assert last_session is not None
    return last_session.close_at.astimezone(timezone.utc) + timedelta(minutes=1)


def _write_rth_put_shadow_cohort(
    root: Path,
    days: list[date],
    *,
    incomplete_rth_index: int | None = None,
    write_candidate: bool = True,
) -> datetime:
    for index, day in enumerate(days):
        _write_health(
            root,
            day,
            gth_minutes=0,
            rth_minutes=350 if index == incomplete_rth_index else None,
        )
        _write_put_shadow_evidence(
            root,
            day,
            index=index,
            write_candidate=write_candidate,
        )
    last_session = DEFAULT_MARKET_CALENDAR.session(days[-1])
    assert last_session is not None
    return last_session.close_at.astimezone(timezone.utc) + timedelta(minutes=1)


def test_session_completeness_uses_minute_windows_and_ignores_quality(tmp_path: Path) -> None:
    good = date(2026, 7, 15)
    bad = date(2026, 7, 16)
    missing = date(2026, 7, 17)
    _write_health(tmp_path, good, gth_minutes=711, rth_minutes=351)
    _write_health(tmp_path, bad, gth_minutes=711, rth_minutes=351)
    _write_health(tmp_path, missing, gth_minutes=711, rth_minutes=351)
    _write_detector_health(tmp_path, bad, gth_minutes=710)
    cutoff = datetime(2026, 7, 18, 0, 0, tzinfo=timezone.utc)

    rows = measure_session_completeness(tmp_path, cutoff_at=cutoff)

    assert rows[0]["session_date"] == good.isoformat()
    assert rows[0]["complete"] is True
    assert rows[0]["gth"]["coverage_ratio"] == 0.9
    assert rows[0]["rth"]["coverage_ratio"] == 0.9
    assert rows[0]["gth_detector_health"] is None
    assert rows[1]["complete"] is False
    assert rows[1]["reasons"] == ["gth_detector_health_coverage_below_90_percent"]
    assert rows[1]["gth_detector_health"]["coverage_ratio"] == 0.898734
    assert rows[2]["complete"] is False
    assert rows[2]["gth_detector_health"]["observed_minutes"] == 0


def test_version_three_contract_requires_all_five_fields() -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    valid = _envelope(
        at,
        role="trade_intent",
        kind="official_spx",
        instrument_id="index:SPX",
    )
    assert validate_strategy_contract(valid, event_at=at) == ()

    invalid = dict(valid)
    invalid.pop("coordinate")
    invalid["valid_until"] = at.isoformat()
    invalid["block_reasons"] = [""]
    assert validate_strategy_contract(invalid, event_at=at) == (
        "coordinate_missing_or_invalid",
        "block_reasons_missing_or_invalid",
    )

    terminal = dict(valid)
    terminal["valid_until"] = (at - timedelta(minutes=1)).isoformat()
    assert validate_strategy_contract(terminal, event_at=at) == ()


def test_trade_ready_delivery_diagnostics_are_retained_but_not_executable() -> None:
    day = date(2026, 7, 15)
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    contract = "option:SPX:SPXW:20260715:7500:P"
    base = {
        **_envelope(
            at,
            role="trade_intent",
            kind="official_spx",
            instrument_id="index:SPX",
        ),
        "signal_status": "trade_ready",
        "intent_id": "intent:delivery-diagnostic",
        "event_id": "level:delivery-diagnostic",
        "semantic_key": f"{day.isoformat()}|level_breakout_put|7500|{contract}",
        "session_id": day.isoformat(),
        "evaluated_at": at.isoformat(),
        "direction": "down",
        "contract_id": contract,
        "execution_eligible": False,
    }
    records = [
        _readiness_record(
            "trade_intents",
            {
                **base,
                "status": "ready_pending_delivery",
                "notification_status": "pending",
                "block_reasons": ["notification:delivery_in_progress"],
            },
            line=1,
            at=at,
        ),
        _readiness_record(
            "trade_intents",
            {
                **base,
                "status": "delivery_blocked",
                "notification_status": "blocked",
                "block_reasons": ["notification:source_coordinate_unavailable"],
            },
            line=2,
            at=at,
        ),
    ]

    assert [_record_role(record) for record in records] == ["trade_intent", "trade_intent"]
    audit = _contract_audit(
        records,
        selected_policies={"trade_intent": ROLE_POLICIES["trade_intent"]},
        cohort_start_session=day,
        policy_start_session=day,
        rollout_boundary_at=at - timedelta(minutes=1),
    )
    exact = count_put_exact_entries(records, eligible_sessions={day.isoformat()})

    assert audit["forward_records"] == 2
    assert audit["compliant_records_count"] == 2
    assert audit["trade_ready_delivery_diagnostics"] == {
        "total": 2,
        "compliant": 2,
        "by_status": {"delivery_blocked": 1, "ready_pending_delivery": 1},
        "compliant_by_status": {
            "delivery_blocked": 1,
            "ready_pending_delivery": 1,
        },
        "executable_samples": 0,
        "rule": (
            "signal_status=trade_ready delivery projections remain diagnostic evidence; "
            "only status=trade_ready can enter an executable cohort"
        ),
    }
    assert duplicate_audit(audit["compliant_records"])["duplicate_records"] == 0
    assert exact == {
        "count": 0,
        "eligible_trade_ready_puts": 0,
        "eligible_shadow_ready_puts": 0,
        "exact_virtual_opens": 0,
        "exact_shadow_quote_entries": 0,
        "unmatched_or_inexact_puts": 0,
        "excluded_incomplete_session": 0,
    }


def test_exact_spread_rejects_stale_and_skewed_leg_quotes() -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    stale = _spread_snapshot(at)
    stale_time = (at - timedelta(seconds=6)).isoformat()
    stale["long"]["source_at"] = stale_time
    stale["short"]["source_at"] = stale_time
    assert _exact_spread_snapshot(stale, at=at) is False

    skewed = _spread_snapshot(at)
    skewed["long"]["source_at"] = (at + timedelta(seconds=1)).isoformat()
    skewed["short"]["source_at"] = (at - timedelta(seconds=5)).isoformat()
    assert _exact_spread_snapshot(skewed, at=at) is False


def test_put_exact_entry_counts_shadow_quote_and_compound_virtual_source_id() -> None:
    day = date(2026, 7, 15)
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    contract = "option:SPX:SPXW:20260715:7500:P"
    entry_window_start_at, hard_exit_at = _put_shadow_window(day)
    production_intent = {
        "status": "trade_ready",
        "intent_id": "intent:production-put",
        "session_id": day.isoformat(),
        "direction": "down",
        "play": "level_breakout_put",
        "thesis": "breakout",
        "level_kind": "flip_low",
        "contract_id": contract,
    }
    production_open = {
        "event": "virtual_opened",
        "source_signal_id": "intent:production-put|level:production-put",
        "contract_id": contract,
        "position_type": "single_option",
        "opened_at": at.isoformat(),
        "last": {
            "bid": 9.8,
            "mid": 10.0,
            "ask": 10.2,
            "source_at": at.isoformat(),
            "quality": {"status": "ok"},
        },
    }
    shadow_intent = {
        "policy_version": "trade_intent_v3_frozen",
        "status": "shadow_ready",
        "intent_id": "intent:shadow-put",
        "event_id": "level:shadow-put",
        "semantic_key": f"{day.isoformat()}|level_breakout_put|7500|{contract}",
        "session_id": day.isoformat(),
        "direction": "down",
        "play": "level_breakout_put",
        "thesis": "breakout",
        "level_kind": "flip_low",
        "contract_id": contract,
        "entry_limit": 10.1,
        "evaluated_at": (at - timedelta(seconds=1)).isoformat(),
        "valid_until": (at + timedelta(minutes=30)).isoformat(),
        "strategy_lane": "long_0dte_rth_flip_low_breakdown_put_shadow",
        "shadow_mode": True,
        "execution_eligible": False,
        "quote_observation_eligible": True,
        "automatic_ordering": False,
        "trade_intent_contract_version": PUT_SHADOW_WINDOW_CONTRACT_VERSION,
        "entry_window_start_at": entry_window_start_at.isoformat(),
        "hard_exit_at": hard_exit_at.isoformat(),
    }
    shadow_terminal = {
        "event": "candidate_terminal",
        "candidate_id": f"{shadow_intent['intent_id']}|{shadow_intent['event_id']}",
        "event_id": shadow_intent["event_id"],
        "intent_id": shadow_intent["intent_id"],
        "semantic_key": shadow_intent["semantic_key"],
        "direction": "down",
        "phase": "quote_reached_entry",
        "terminal_at": at.isoformat(),
        "contract_id": contract,
        "entry_limit": shadow_intent["entry_limit"],
        "valid_until": shadow_intent["valid_until"],
        "trade_intent_contract_version": PUT_SHADOW_WINDOW_CONTRACT_VERSION,
        "entry_window_start_at": entry_window_start_at.isoformat(),
        "hard_exit_at": hard_exit_at.isoformat(),
        "strategy_lane": shadow_intent["strategy_lane"],
        "play": shadow_intent["play"],
        "thesis": shadow_intent["thesis"],
        "level_kind": shadow_intent["level_kind"],
        "shadow_mode": True,
        "automatic_ordering": False,
        "execution_claim": "none",
        "broker_order_state": "not_connected",
        "source_intent": dict(shadow_intent),
        "entry_observation": {
            "at": at.isoformat(),
            "contract_id": contract,
            "entry_limit": shadow_intent["entry_limit"],
            "provider": "schwab",
            "bid": 9.8,
            "mid": 10.0,
            "ask": 10.1,
            "quote_quality": "live",
            "quote_source_at": at.isoformat(),
            "quote_transport_at": at.isoformat(),
            "quote_source_age_seconds": 0.0,
            "quote_transport_age_seconds": 0.0,
            "quote_pricing_allowed": True,
            "exact_quote_freshness_ok": True,
            "exact_quote_policy_version": PUT_SHADOW_EXACT_QUOTE_POLICY_VERSION,
            "exact_quote_max_age_seconds": PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
            "entry_condition": "displayed_ask_at_or_below_limit",
        },
    }

    def record(source: str, payload: dict[str, object], line: int) -> SimpleNamespace:
        return SimpleNamespace(
            source=source,
            payload=payload,
            path=f"{source}.jsonl",
            line_number=line,
            at=at,
            session_date=day,
        )

    result = count_put_exact_entries(
        [
            record("trade_intents", production_intent, 1),
            record("virtual_strategy", production_open, 2),
            record("trade_intents", shadow_intent, 3),
            record("trade_candidates", shadow_terminal, 4),
        ],
        eligible_sessions={day.isoformat()},
    )

    assert result == {
        "count": 2,
        "eligible_trade_ready_puts": 1,
        "eligible_shadow_ready_puts": 1,
        "exact_virtual_opens": 1,
        "exact_shadow_quote_entries": 1,
        "unmatched_or_inexact_puts": 0,
        "excluded_incomplete_session": 0,
    }


@pytest.mark.parametrize("age_seconds", [6.0, 15.0])
def test_put_shadow_readiness_accepts_exact_quotes_through_fifteen_seconds(
    age_seconds: float,
) -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=age_seconds,
        transport_age_seconds=age_seconds,
    )

    assert _exact_put_shadow_entry(intent, candidate) is True


@pytest.mark.parametrize(
    ("scope", "field", "wrong_value"),
    [
        ("candidate", "candidate_id", "intent:other|level:other"),
        ("observation", "contract_id", "option:SPX:SPXW:20260715:7510:P"),
        ("observation", "entry_limit", 10.2),
        ("observation", "at", "2026-07-15T14:00:01+00:00"),
    ],
)
def test_put_shadow_readiness_binds_candidate_and_observation_lineage(
    scope: str,
    field: str,
    wrong_value: object,
) -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    target = candidate
    if scope == "observation":
        observation = candidate["entry_observation"]
        assert isinstance(observation, dict)
        target = observation
    target[field] = wrong_value

    assert _exact_put_shadow_entry(intent, candidate) is False


@pytest.mark.parametrize(
    ("source_age_seconds", "transport_age_seconds"),
    [
        (None, 0.0),
        (15.001, 0.0),
        (0.0, 15.001),
        (-0.001, 0.0),
        (0.0, -0.001),
    ],
)
def test_put_shadow_readiness_rejects_stale_or_future_exact_quotes(
    source_age_seconds: float | None,
    transport_age_seconds: float,
) -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=source_age_seconds,
        transport_age_seconds=transport_age_seconds,
    )

    assert _exact_put_shadow_entry(intent, candidate) is False


@pytest.mark.parametrize(
    ("policy_version", "max_age"),
    [
        ("put_shadow_exact_quote.v2", 15.0),
        ("put_shadow_exact_quote.v1", 14.0),
    ],
)
def test_put_shadow_readiness_rejects_unknown_or_drifted_quote_policy(
    policy_version: str,
    max_age: float,
) -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    observation = candidate["entry_observation"]
    assert isinstance(observation, dict)
    observation["exact_quote_policy_version"] = policy_version
    observation["exact_quote_max_age_seconds"] = max_age

    assert _exact_put_shadow_entry(intent, candidate) is False


@pytest.mark.parametrize("scope", ["intent", "source_intent"])
@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("execution_eligible", True),
        ("quote_observation_eligible", False),
        ("automatic_ordering", True),
    ],
)
@pytest.mark.parametrize("missing", [False, True], ids=["wrong", "missing"])
def test_put_shadow_readiness_requires_non_executable_safety_contract(
    scope: str,
    field: str,
    wrong_value: bool,
    *,
    missing: bool,
) -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    target = intent
    if scope == "source_intent":
        source = candidate["source_intent"]
        assert isinstance(source, dict)
        target = source
    if missing:
        target.pop(field)
    else:
        target[field] = wrong_value

    assert _exact_put_shadow_entry(intent, candidate) is False


@pytest.mark.parametrize(
    "terminal_at",
    [
        datetime(2026, 7, 15, 13, 44, 59, tzinfo=timezone.utc),
        datetime(2026, 7, 15, 17, 0, tzinfo=timezone.utc),
    ],
    ids=["before_0945_et", "at_1300_et"],
)
def test_put_shadow_readiness_rejects_terminal_outside_entry_window(
    terminal_at: datetime,
) -> None:
    intent, candidate = _put_shadow_readiness_fixture(
        terminal_at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )

    assert _exact_put_shadow_entry(intent, candidate) is False


def test_put_shadow_readiness_rejects_terminal_before_intent_or_after_ttl() -> None:
    terminal_at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        terminal_at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    after_terminal = (terminal_at + timedelta(seconds=1)).isoformat()
    intent["evaluated_at"] = after_terminal
    source = candidate["source_intent"]
    assert isinstance(source, dict)
    source["evaluated_at"] = after_terminal
    assert _exact_put_shadow_entry(intent, candidate) is False

    intent, candidate = _put_shadow_readiness_fixture(
        terminal_at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    terminal_ttl = terminal_at.isoformat()
    intent["valid_until"] = terminal_ttl
    source = candidate["source_intent"]
    assert isinstance(source, dict)
    source["valid_until"] = terminal_ttl
    candidate["valid_until"] = terminal_ttl
    assert _exact_put_shadow_entry(intent, candidate) is False


def test_put_shadow_readiness_rejects_intent_or_ttl_outside_policy_window() -> None:
    terminal_at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        terminal_at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    before_window = datetime(2026, 7, 15, 13, 0, tzinfo=timezone.utc).isoformat()
    intent["evaluated_at"] = before_window
    source = candidate["source_intent"]
    assert isinstance(source, dict)
    source["evaluated_at"] = before_window
    assert _exact_put_shadow_entry(intent, candidate) is False

    intent, candidate = _put_shadow_readiness_fixture(
        terminal_at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    after_hard_exit = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc).isoformat()
    intent["valid_until"] = after_hard_exit
    source = candidate["source_intent"]
    assert isinstance(source, dict)
    source["valid_until"] = after_hard_exit
    candidate["valid_until"] = after_hard_exit
    assert _exact_put_shadow_entry(intent, candidate) is False


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("trade_intent_contract_version", "rth_put_shadow.v2"),
        ("entry_window_start_at", "2026-07-15T13:46:00+00:00"),
        ("hard_exit_at", "2026-07-15T17:01:00+00:00"),
        ("level_kind", "call_wall"),
    ],
)
def test_put_shadow_readiness_rejects_unknown_or_drifted_window_contract(
    field: str,
    wrong_value: str,
) -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    intent[field] = wrong_value
    source = candidate["source_intent"]
    assert isinstance(source, dict)
    source[field] = wrong_value
    candidate[field] = wrong_value

    assert _exact_put_shadow_entry(intent, candidate) is False


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("policy_version", "trade_intent_drift"),
        ("evaluated_at", "2026-07-15T13:59:58+00:00"),
        ("valid_until", "2026-07-15T14:29:59+00:00"),
        ("entry_limit", 10.0),
        ("trade_intent_contract_version", "rth_put_shadow.v2"),
        ("entry_window_start_at", "2026-07-15T13:46:00+00:00"),
        ("hard_exit_at", "2026-07-15T17:01:00+00:00"),
        ("play", "level_fade_put"),
    ],
)
def test_put_shadow_readiness_rejects_source_intent_lineage_drift(
    field: str,
    wrong_value: object,
) -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    source = candidate["source_intent"]
    assert isinstance(source, dict)
    source[field] = wrong_value
    if field in {
        "valid_until",
        "entry_limit",
        "trade_intent_contract_version",
        "entry_window_start_at",
        "hard_exit_at",
    }:
        candidate[field] = wrong_value

    assert _exact_put_shadow_entry(intent, candidate) is False


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("event_id", "level:wrong"),
        ("intent_id", "intent:wrong"),
        ("semantic_key", "wrong|semantic|key"),
        ("strategy_lane", "long_0dte_rth_upper_rejection_put_shadow"),
        ("contract_id", "option:SPX:SPXW:20260715:7495:P"),
        ("entry_limit", 10.0),
        ("trade_intent_contract_version", "rth_put_shadow.v2"),
        ("entry_window_start_at", "2026-07-15T13:46:00+00:00"),
        ("hard_exit_at", "2026-07-15T17:01:00+00:00"),
    ],
)
def test_put_shadow_readiness_rejects_candidate_lineage_drift(
    field: str,
    wrong_value: object,
) -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    candidate[field] = wrong_value

    assert _exact_put_shadow_entry(intent, candidate) is False


def test_put_shadow_readiness_rejects_unregistered_lane() -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, candidate = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    made_up_lane = "long_0dte_rth_made_up_put_shadow"
    intent["strategy_lane"] = made_up_lane
    candidate["strategy_lane"] = made_up_lane
    source = candidate["source_intent"]
    assert isinstance(source, dict)
    source["strategy_lane"] = made_up_lane

    assert _exact_put_shadow_entry(intent, candidate) is False


def test_unregistered_put_shadow_lane_is_a_contract_violation_not_silently_filtered() -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    payload = {
        **_envelope(
            at,
            role="trade_intent",
            kind="official_spx",
            instrument_id="index:SPX",
        ),
        "status": "observing",
        "strategy_lane": "long_0dte_rth_new_put_shadow",
        "shadow_mode": True,
    }
    record = _readiness_record("trade_intents", payload, line=1, at=at)

    assert _put_shadow_record(record) is True
    assert "put_shadow_lane_unregistered" in _material_contract_issues(
        record,
        role="trade_intent",
    )
    audit = _contract_audit(
        [record],
        selected_policies={"trade_intent": payload["policy_version"]},
        cohort_start_session=at.date(),
        policy_start_session=at.date(),
        rollout_boundary_at=None,
        included_roles=("trade_intent",),
        record_filter=_put_shadow_record,
    )
    assert audit["invalid_records"] == 1
    assert audit["issues"]["put_shadow_lane_unregistered"] == 1


@pytest.mark.parametrize("position_type", [None, "unknown"])
def test_put_readiness_rejects_missing_or_unknown_production_position_type(
    position_type: str | None,
) -> None:
    day = date(2026, 7, 15)
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    contract = "option:SPX:SPXW:20260715:7500:P"
    intent = {
        "status": "trade_ready",
        "intent_id": "intent:production-put",
        "session_id": day.isoformat(),
        "direction": "down",
        "play": "level_breakout_put",
        "thesis": "breakout",
        "level_kind": "flip_low",
        "contract_id": contract,
    }
    opened = {
        "event": "virtual_opened",
        "source_signal_id": "intent:production-put",
        "contract_id": contract,
        "opened_at": at.isoformat(),
        "last": {
            "bid": 9.8,
            "mid": 10.0,
            "ask": 10.2,
            "source_at": at.isoformat(),
            "quality": {"status": "ok"},
        },
    }
    if position_type is not None:
        opened["position_type"] = position_type

    records = [
        _readiness_record("trade_intents", intent, line=1, at=at),
        _readiness_record("virtual_strategy", opened, line=2, at=at),
    ]
    result = count_put_exact_entries(records, eligible_sessions={day.isoformat()})

    assert result["count"] == 0
    assert result["exact_virtual_opens"] == 0
    assert result["unmatched_or_inexact_puts"] == 1


def test_distinct_shadow_events_are_separate_without_duplicate_anomaly() -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    intent, terminal = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=6.0,
        transport_age_seconds=6.0,
    )
    rearmed = {
        **intent,
        "intent_id": "intent:shadow-put-restarted",
        "event_id": "level:rearmed",
    }
    armed = {
        **terminal,
        "event": "candidate_armed",
        "phase": "armed",
        "armed_at": (at - timedelta(seconds=10)).isoformat(),
    }
    armed.pop("terminal_at")
    armed.pop("entry_observation")
    records = [
        _readiness_record("trade_intents", intent, line=1, at=at),
        _readiness_record("trade_intents", rearmed, line=2, at=at),
        _readiness_record("trade_candidates", armed, line=3, at=at),
        _readiness_record("trade_candidates", terminal, line=4, at=at),
    ]

    audit = duplicate_audit(records)
    result = count_put_exact_entries(
        records,
        eligible_sessions={at.date().isoformat()},
    )

    assert audit == {"duplicate_records": 0, "keys": [], "sessions": set()}
    assert result["count"] == 1
    assert result["eligible_shadow_ready_puts"] == 2
    assert result["exact_shadow_quote_entries"] == 1
    assert result["unmatched_or_inexact_puts"] == 1


def test_same_shadow_event_retries_dedupe_and_match_later_intent() -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    first, terminal = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    retry = {
        **first,
        "intent_id": "intent:shadow-put-retry",
    }
    terminal["candidate_id"] = "intent:shadow-put-retry|level:first"
    terminal["intent_id"] = retry["intent_id"]
    terminal["source_intent"] = dict(retry)
    records = [
        _readiness_record("trade_intents", first, line=1, at=at),
        _readiness_record("trade_intents", retry, line=2, at=at),
        _readiness_record("trade_candidates", terminal, line=3, at=at),
    ]

    result = count_put_exact_entries(
        records,
        eligible_sessions={at.date().isoformat()},
    )

    assert result["count"] == 1
    assert result["eligible_shadow_ready_puts"] == 1
    assert result["unmatched_or_inexact_puts"] == 0


def test_distinct_shadow_events_with_same_semantic_key_both_count() -> None:
    at = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    first, first_terminal = _put_shadow_readiness_fixture(
        at,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )
    second = {
        **first,
        "intent_id": "intent:shadow-put-second",
        "event_id": "level:second",
    }
    second_terminal = {
        **first_terminal,
        "candidate_id": "intent:shadow-put-second|level:second",
        "intent_id": second["intent_id"],
        "event_id": second["event_id"],
        "source_intent": dict(second),
    }
    records = [
        _readiness_record("trade_intents", first, line=1, at=at),
        _readiness_record("trade_candidates", first_terminal, line=2, at=at),
        _readiness_record("trade_intents", second, line=3, at=at),
        _readiness_record("trade_candidates", second_terminal, line=4, at=at),
    ]

    assert duplicate_audit(records) == {
        "duplicate_records": 0,
        "keys": [],
        "sessions": set(),
    }
    result = count_put_exact_entries(
        records,
        eligible_sessions={at.date().isoformat()},
    )
    assert result["count"] == 2
    assert result["eligible_shadow_ready_puts"] == 2
    assert result["exact_shadow_quote_entries"] == 2


def _put_shadow_readiness_fixture(
    at: datetime,
    *,
    source_age_seconds: float | None,
    transport_age_seconds: float,
) -> tuple[dict[str, object], dict[str, object]]:
    day = at.date()
    contract = f"option:SPX:SPXW:{day.strftime('%Y%m%d')}:7500:P"
    semantic_key = f"{day.isoformat()}|level_breakout_put|7500|{contract}"
    entry_window_start_at, hard_exit_at = _put_shadow_window(day)
    intent: dict[str, object] = {
        "policy_version": "trade_intent_v3_frozen",
        "status": "shadow_ready",
        "intent_id": "intent:shadow-put",
        "event_id": "level:first",
        "semantic_key": semantic_key,
        "session_id": day.isoformat(),
        "direction": "down",
        "play": "level_breakout_put",
        "thesis": "breakout",
        "level_kind": "flip_low",
        "contract_id": contract,
        "entry_limit": 10.1,
        "evaluated_at": (at - timedelta(seconds=1)).isoformat(),
        "valid_until": (at + timedelta(minutes=30)).isoformat(),
        "strategy_lane": "long_0dte_rth_flip_low_breakdown_put_shadow",
        "shadow_mode": True,
        "execution_eligible": False,
        "quote_observation_eligible": True,
        "automatic_ordering": False,
        "trade_intent_contract_version": PUT_SHADOW_WINDOW_CONTRACT_VERSION,
        "entry_window_start_at": entry_window_start_at.isoformat(),
        "hard_exit_at": hard_exit_at.isoformat(),
    }
    candidate: dict[str, object] = {
        "event": "candidate_terminal",
        "candidate_id": "intent:shadow-put|level:first",
        "intent_id": intent["intent_id"],
        "event_id": intent["event_id"],
        "semantic_key": semantic_key,
        "direction": "down",
        "phase": "quote_reached_entry",
        "terminal_at": at.isoformat(),
        "contract_id": contract,
        "entry_limit": intent["entry_limit"],
        "valid_until": intent["valid_until"],
        "trade_intent_contract_version": PUT_SHADOW_WINDOW_CONTRACT_VERSION,
        "entry_window_start_at": entry_window_start_at.isoformat(),
        "hard_exit_at": hard_exit_at.isoformat(),
        "strategy_lane": intent["strategy_lane"],
        "play": intent["play"],
        "thesis": intent["thesis"],
        "level_kind": intent["level_kind"],
        "shadow_mode": True,
        "automatic_ordering": False,
        "execution_claim": "none",
        "broker_order_state": "not_connected",
        "source_intent": dict(intent),
        "entry_observation": {
            "at": at.isoformat(),
            "contract_id": contract,
            "entry_limit": intent["entry_limit"],
            "provider": "schwab",
            "bid": 9.8,
            "mid": 10.0,
            "ask": 10.1,
            "quote_quality": "live",
            "quote_source_at": (at - timedelta(seconds=source_age_seconds)).isoformat()
            if source_age_seconds is not None
            else None,
            "quote_transport_at": (at - timedelta(seconds=transport_age_seconds)).isoformat(),
            "quote_source_age_seconds": source_age_seconds,
            "quote_transport_age_seconds": transport_age_seconds,
            "quote_pricing_allowed": True,
            "exact_quote_freshness_ok": True,
            "exact_quote_policy_version": PUT_SHADOW_EXACT_QUOTE_POLICY_VERSION,
            "exact_quote_max_age_seconds": PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
            "entry_condition": "displayed_ask_at_or_below_limit",
        },
    }
    return intent, candidate


def _readiness_record(
    source: str,
    payload: dict[str, object],
    *,
    line: int,
    at: datetime,
) -> SimpleNamespace:
    return SimpleNamespace(
        source=source,
        payload=payload,
        path=f"{source}.jsonl",
        line_number=line,
        at=at,
        session_date=at.date(),
        partition_date=at.date(),
        malformed_json=False,
    )


def test_twenty_clean_forward_sessions_and_exact_entries_are_review_ready(
    tmp_path: Path,
) -> None:
    days = _trading_days(date(2026, 6, 22), DEFAULT_THRESHOLDS.complete_sessions)
    cutoff = _write_complete_forward_cohort(tmp_path, days)

    result = build_strategy_readiness(
        tmp_path,
        cutoff_at=cutoff,
        policy_versions=ROLE_POLICIES,
        generated_at=cutoff,
    )

    assert result["status"] == "ready_for_review"
    assert result["automatic_promotion"] is False
    assert result["sessions"]["health_complete"] == 20
    assert result["sessions"]["contract_consistent_complete"] == 20
    assert result["contract"]["coverage_ratio"] == 1.0
    assert result["contract"]["invalid_records"] == 0
    assert result["contract"]["duplicate_records"] == 0
    assert result["cohorts"]["gth_exact_entry"]["count"] == 20
    assert result["cohorts"]["put_exact_entry"]["count"] == 20
    assert result["cohorts"]["exact_spread_complete_exit"]["count"] == 20
    assert result["blockers"] == []


def test_put_readiness_uses_rth_only_while_overall_waits_for_gth(
    tmp_path: Path,
) -> None:
    days = _trading_days(date(2026, 6, 22), DEFAULT_THRESHOLDS.complete_sessions)
    cutoff = _write_rth_put_shadow_cohort(tmp_path, days)

    result = build_strategy_readiness(
        tmp_path,
        cutoff_at=cutoff,
        policy_versions=ROLE_POLICIES,
        generated_at=cutoff,
    )

    put = result["cohorts"]["put_exact_entry"]
    assert put["status"] == "ready"
    assert put["count"] == 20
    assert put["eligible_shadow_ready_puts"] == 20
    assert put["exact_shadow_quote_entries"] == 20
    assert put["blockers"] == []
    assert result["cohort_sessions"]["put_exact_entry"] == {
        "observed": 20,
        "rth_complete": 20,
        "contract_consistent_complete": 20,
        "target": 20,
        "dates": [day.isoformat() for day in days],
    }
    assert result["sessions"]["health_complete"] == 0
    assert result["status"] == "collecting"
    assert result["cohorts"]["gth_exact_entry"]["status"] == "collecting"
    assert result["cohorts"]["exact_spread_complete_exit"]["status"] == "collecting"


def test_gth_and_call_anomalies_do_not_change_put_readiness(tmp_path: Path) -> None:
    days = _trading_days(date(2026, 6, 22), DEFAULT_THRESHOLDS.complete_sessions)
    cutoff = _write_rth_put_shadow_cohort(tmp_path, days)
    baseline = build_strategy_readiness(
        tmp_path,
        cutoff_at=cutoff,
        policy_versions=ROLE_POLICIES,
        generated_at=cutoff,
    )

    day = days[-1]
    gth_start, _, rth_start, _ = _health_window(day)
    anomaly_at = (gth_start + timedelta(minutes=30)).astimezone(timezone.utc)
    bad_gth = {
        **_envelope(
            anomaly_at,
            role="gth_signal",
            kind="raw_es",
            instrument_id="future:ES",
        ),
        "event_id": "gth:invalid-duplicate",
        "session_date": day.isoformat(),
        "confirmed_at": anomaly_at.isoformat(),
    }
    bad_gth.pop("coordinate")
    _write_rows(
        tmp_path / "gth_dip_reclaim" / f"date={day.isoformat()}" / "anomalies.jsonl",
        [bad_gth, bad_gth],
    )
    call_at = (rth_start + timedelta(minutes=60)).astimezone(timezone.utc)
    bad_call = {
        **_envelope(
            call_at,
            role="trade_intent",
            kind="official_spx",
            instrument_id="index:SPX",
        ),
        "status": "blocked",
        "intent_id": "intent:bad-call",
        "event_id": "level:bad-call",
        "session_id": day.isoformat(),
        "evaluated_at": call_at.isoformat(),
        "direction": "up",
        "contract_id": f"option:SPX:SPXW:{day.strftime('%Y%m%d')}:7500:C",
        "strategy_lane": "long_0dte_rth_upside_breakout_pilot",
        "shadow_mode": False,
    }
    bad_call.pop("coordinate")
    _write_rows(
        tmp_path / "trade_intents" / f"date={day.isoformat()}" / "call-anomaly.jsonl",
        [bad_call],
    )

    after = build_strategy_readiness(
        tmp_path,
        cutoff_at=cutoff,
        policy_versions=ROLE_POLICIES,
        generated_at=cutoff,
    )

    assert after["cohorts"]["put_exact_entry"] == baseline["cohorts"]["put_exact_entry"]
    assert (
        after["cohort_sessions"]["put_exact_entry"]
        == baseline["cohort_sessions"]["put_exact_entry"]
    )
    assert (
        after["cohort_contracts"]["put_exact_entry"]
        == baseline["cohort_contracts"]["put_exact_entry"]
    )


def test_incomplete_rth_session_is_excluded_from_put_readiness(tmp_path: Path) -> None:
    days = _trading_days(date(2026, 6, 22), DEFAULT_THRESHOLDS.complete_sessions)
    cutoff = _write_rth_put_shadow_cohort(
        tmp_path,
        days,
        incomplete_rth_index=len(days) - 1,
    )

    result = build_strategy_readiness(
        tmp_path,
        cutoff_at=cutoff,
        policy_versions=ROLE_POLICIES,
        generated_at=cutoff,
    )

    put = result["cohorts"]["put_exact_entry"]
    assert put["status"] == "collecting"
    assert put["count"] == 19
    assert put["excluded_incomplete_session"] == 1
    assert result["cohort_sessions"]["put_exact_entry"]["contract_consistent_complete"] == 19
    assert "put_rth_contract_consistent_complete_sessions_below_20" in put["blockers"]
    assert "put_exact_entries_below_20" in put["blockers"]


def test_missing_candidate_policy_role_blocks_put_readiness(tmp_path: Path) -> None:
    days = _trading_days(date(2026, 6, 22), DEFAULT_THRESHOLDS.complete_sessions)
    cutoff = _write_rth_put_shadow_cohort(
        tmp_path,
        days,
        write_candidate=False,
    )

    result = build_strategy_readiness(
        tmp_path,
        cutoff_at=cutoff,
        policy_versions=ROLE_POLICIES,
        generated_at=cutoff,
    )

    put = result["cohorts"]["put_exact_entry"]
    assert put["status"] == "collecting"
    assert put["count"] == 0
    assert "policy_role_unavailable:trade_candidate" in put["blockers"]
    assert result["cohort_policy_bundles"]["put_exact_entry"]["started_session"] is None


def test_candidate_policy_drift_blocks_put_readiness(tmp_path: Path) -> None:
    days = _trading_days(date(2026, 6, 22), DEFAULT_THRESHOLDS.complete_sessions)
    cutoff = _write_rth_put_shadow_cohort(tmp_path, days)
    drift_day = days[-1]
    path = tmp_path / "trade_candidates" / f"date={drift_day.isoformat()}" / "events.jsonl"
    rows = _read_rows(path)
    for row in rows:
        row["policy_version"] = "trade_candidate_v3_drift"
    _write_rows(path, rows)

    result = build_strategy_readiness(
        tmp_path,
        cutoff_at=cutoff,
        policy_versions=ROLE_POLICIES,
        generated_at=cutoff,
    )

    put = result["cohorts"]["put_exact_entry"]
    assert put["status"] == "collecting"
    assert put["count"] == 19
    assert "put_role_policy_version_drift_present" in put["blockers"]
    assert (
        result["cohort_contracts"]["put_exact_entry"]["issues"]["role_policy_version_mismatch"] == 2
    )


def test_candidate_terminal_cannot_roll_back_latest_armed_policy() -> None:
    start = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    _, template = _put_shadow_readiness_fixture(
        start,
        source_age_seconds=0.0,
        transport_age_seconds=0.0,
    )

    def candidate(
        *,
        event: str,
        policy: str,
        at: datetime,
        suffix: str,
    ) -> SimpleNamespace:
        payload = {
            **template,
            "schema_version": 3,
            "policy_version": policy,
            "event": event,
            "candidate_id": f"candidate:{suffix}",
            "armed_at": at.isoformat(),
            "terminal_at": at.isoformat(),
        }
        return _readiness_record("trade_candidates", payload, line=1, at=at)

    records = [
        candidate(
            event="candidate_armed",
            policy="candidate-policy-a",
            at=start,
            suffix="a",
        ),
        candidate(
            event="candidate_armed",
            policy="candidate-policy-b",
            at=start + timedelta(minutes=1),
            suffix="b",
        ),
        candidate(
            event="candidate_terminal",
            policy="candidate-policy-b",
            at=start + timedelta(minutes=2),
            suffix="b",
        ),
        candidate(
            event="candidate_terminal",
            policy="candidate-policy-a",
            at=start + timedelta(minutes=3),
            suffix="a",
        ),
    ]

    selected = _select_policy_bundle(
        records,
        requested=None,
        roles=("trade_candidate",),
        required_roles=("trade_candidate",),
        record_filter=_put_shadow_record,
    )

    assert selected["versions"]["trade_candidate"] == "candidate-policy-b"
    assert (
        selected["role_started_at"]["trade_candidate"] == (start + timedelta(minutes=1)).isoformat()
    )


def test_legacy_is_excluded_but_forward_invalid_and_duplicate_rows_block(
    tmp_path: Path,
) -> None:
    day = date(2026, 7, 15)
    _write_health(tmp_path, day)
    _write_detector_health(tmp_path, day, gth_minutes=790)
    gth_start, _, _, rth_end = _health_window(day)
    legacy_at = (gth_start - timedelta(minutes=10)).astimezone(timezone.utc)
    signal_at = (gth_start + timedelta(minutes=10)).astimezone(timezone.utc)
    expiry = day.isoformat()
    spread = {
        "expiry_date": expiry,
        "right": "C",
        "long_strike": 7500.0,
        "short_strike": 7520.0,
        "width_points": 20.0,
    }
    legacy = {
        "schema_version": 2,
        "event_id": "legacy",
        "session_date": expiry,
        "confirmed_at": legacy_at.isoformat(),
    }
    pre_rollout_v3 = {
        **_envelope(
            legacy_at,
            role="gth_signal",
            kind="raw_es",
            instrument_id="future:ES",
        ),
        "event_id": "pre-rollout-v3",
        "session_date": expiry,
        "confirmed_at": legacy_at.isoformat(),
    }
    signal = {
        **_envelope(
            signal_at,
            role="gth_signal",
            kind="raw_es",
            instrument_id="future:ES",
        ),
        "event_id": "gth:duplicate",
        "session_date": expiry,
        "confirmed_at": signal_at.isoformat(),
        "spread": spread,
    }
    _write_rows(
        tmp_path / "gth_dip_reclaim" / f"date={expiry}" / "events.jsonl",
        [legacy, pre_rollout_v3, signal, signal],
    )
    invalid_at = signal_at + timedelta(minutes=1)
    invalid_intent = {
        **_envelope(
            invalid_at,
            role="trade_intent",
            kind="official_spx",
            instrument_id="index:SPX",
        ),
        "status": "trade_ready",
        "intent_id": "invalid-put",
        "event_id": "level:invalid-put",
        "session_id": expiry,
        "evaluated_at": invalid_at.isoformat(),
        "direction": "down",
        "contract_id": "option:SPX:SPXW:20260715:7500:P",
    }
    invalid_intent.pop("coordinate")
    telemetry = {
        **_envelope(
            invalid_at,
            role="trade_intent",
            kind="official_spx",
            instrument_id="index:SPX",
            valid_until=invalid_at,
        ),
        "status": "observing",
        "event_id": None,
        "session_id": expiry,
        "evaluated_at": invalid_at.isoformat(),
    }
    telemetry["coordinate"] = {"kind": "unavailable", "instrument_id": None}
    _write_rows(
        tmp_path / "trade_intents" / f"date={expiry}" / "events.jsonl",
        [telemetry, invalid_intent],
    )
    cutoff = rth_end.astimezone(timezone.utc) + timedelta(minutes=1)

    result = build_strategy_readiness(
        tmp_path,
        cutoff_at=cutoff,
        policy_versions=ROLE_POLICIES,
        generated_at=cutoff,
    )

    assert result["legacy_exclusion"]["total"] == 1
    assert result["legacy_exclusion"]["other_policy_before_cohort"] == 1
    assert result["contract"]["forward_records"] == 3
    assert result["contract"]["telemetry_excluded"]["total"] == 1
    assert result["contract"]["compliant_records_count"] == 2
    assert result["contract"]["invalid_records"] == 1
    assert result["contract"]["coverage_ratio"] == 0.666667
    assert result["contract"]["duplicate_records"] == 1
    assert result["sessions"]["health_complete"] == 1
    assert result["sessions"]["contract_consistent_complete"] == 0
    assert "contract_compliance_below_100_percent" in result["blockers"]
    assert "duplicate_forward_samples_present" in result["blockers"]


def test_observing_policy_declaration_reopens_auto_cohort_and_explicit_drift_blocks(
    tmp_path: Path,
) -> None:
    days = _trading_days(date(2026, 7, 13), 3)
    cutoff = _write_complete_forward_cohort(tmp_path, days)
    feature_policy = MarketFeatureSettings(trade_confirmed_pilot_enabled=True)
    order_policy = OrderMapPolicy()
    legacy = policy_version(
        "rth_trade_intent.v3",
        {"market_features": feature_policy, "order_map": order_policy},
    )
    changed = trade_intent_policy_version(feature_policy, order_policy)
    assert changed != legacy

    for day in days:
        path = tmp_path / "trade_intents" / f"date={day.isoformat()}" / "events.jsonl"
        rows = _read_rows(path)
        for row in rows:
            row["policy_version"] = legacy
        _write_rows(path, rows)

    observing_path = tmp_path / "trade_intents" / f"date={days[1].isoformat()}" / "events.jsonl"
    observing = _read_rows(observing_path)[0]
    observing.update(
        {
            "policy_version": changed,
            "status": "observing",
            "event_id": None,
            "intent_id": None,
            "valid_until": observing["evaluated_at"],
            "coordinate": {"kind": "unavailable", "instrument_id": None},
        }
    )
    _write_rows(observing_path, [observing])

    changed_path = tmp_path / "trade_intents" / f"date={days[2].isoformat()}" / "events.jsonl"
    changed_intent = _read_rows(changed_path)[0]
    changed_intent["policy_version"] = changed
    _write_rows(changed_path, [changed_intent])

    automatic = build_strategy_readiness(tmp_path, cutoff_at=cutoff, generated_at=cutoff)
    assert automatic["policy_versions"]["trade_intent"] == changed
    assert automatic["policy_bundle"]["version_reset_session"] == days[1].isoformat()
    assert automatic["policy_bundle"]["effective_started_session"] == days[2].isoformat()
    assert automatic["contract"]["issues"].get("role_policy_version_mismatch", 0) == 0

    explicit = build_strategy_readiness(
        tmp_path,
        cutoff_at=cutoff,
        policy_versions={**ROLE_POLICIES, "trade_intent": legacy},
        generated_at=cutoff,
    )
    assert explicit["contract"]["telemetry_excluded"]["total"] == 1
    assert explicit["contract"]["issues"]["role_policy_version_mismatch"] == 1
    assert "same_role_policy_version_drift_present" in explicit["blockers"]
