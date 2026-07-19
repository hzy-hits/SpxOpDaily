from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spx_spark.data_platform.research.strategy_readiness import (
    DEFAULT_THRESHOLDS,
    _exact_spread_snapshot,
    build_strategy_readiness,
    measure_session_completeness,
    validate_strategy_contract,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET


ROLE_POLICIES = {
    "gth_detector_runtime": "gth_detector_runtime_v3_frozen",
    "gth_signal": "gth_signal_v3_frozen",
    "trade_intent": "trade_intent_v3_frozen",
    "virtual_entry_decision": "virtual_entry_decision_v3_frozen",
    "virtual_lifecycle": "virtual_lifecycle_v3_frozen",
}


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
        gth_start, _, rth_start, _ = _health_window(day)
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

        intent_at = (rth_start + timedelta(minutes=30)).astimezone(timezone.utc)
        put_open_at = intent_at + timedelta(minutes=1)
        intent_id = f"intent:put:{index}"
        put_contract = f"option:SPX:SPXW:{expiry}:7500:P"
        intent = {
            **_envelope(
                intent_at,
                role="trade_intent",
                kind="official_spx",
                instrument_id="index:SPX",
            ),
            "status": "trade_ready",
            "intent_id": intent_id,
            "event_id": f"level:put:{index}",
            "session_id": day.isoformat(),
            "evaluated_at": intent_at.isoformat(),
            "direction": "down",
            "contract_id": put_contract,
        }
        _write_rows(
            root / "trade_intents" / f"date={day.isoformat()}" / "events.jsonl",
            [intent],
        )
        put_snapshot = _single_snapshot(put_open_at)
        put_open = {
            **_envelope(
                put_open_at,
                role="virtual_lifecycle",
                kind="option_contract",
                instrument_id=put_contract,
            ),
            "event": "virtual_opened",
            "episode_id": f"virtual:put:{index}",
            "source_signal_id": intent_id,
            "source_kind": "trade_intent",
            "session_date": day.isoformat(),
            "opened_at": put_open_at.isoformat(),
            "position_type": "single_option",
            "contract_id": put_contract,
            "entry_mid": 10.5,
            "last": put_snapshot,
        }
        _write_rows(
            root / "virtual_strategy" / f"date={day.isoformat()}" / "events.jsonl",
            [entry_decision, opened, closed, put_open],
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
    changed = "trade_intent_v3_changed"

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
        policy_versions=ROLE_POLICIES,
        generated_at=cutoff,
    )
    assert explicit["contract"]["telemetry_excluded"]["total"] == 1
    assert explicit["contract"]["issues"]["role_policy_version_mismatch"] == 1
    assert "same_role_policy_version_drift_present" in explicit["blockers"]
