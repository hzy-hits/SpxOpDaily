from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from spx_spark.data_platform.research.odte_level_replay import (
    RuleExit,
    SpreadRound,
    next_replay_exit_clock,
    pair_spread_rounds,
    parse_statement_fills,
    render_replay_report,
    replay_expiry_close,
    replay_spread_rule,
    replay_spread_rule_attempt,
)
from spx_spark.data_platform.research.odte_level_signals import OptionTick

ENTRY = datetime(2026, 7, 2, 6, 41, 39, tzinfo=timezone.utc)  # 02:41:39 EDT


def _row(symbol: str, at: str, qty: int, price: float, realized: float, flags: str) -> str:
    proceeds = -qty * price * 100
    return (
        f'交易,Data,Order,股票和指数期权,USD,{symbol},"{at}",{qty},{price},0,'
        f"{proceeds},-1.63028,0,{realized},0,{flags}"
    )


def _statement(rows: list[str]) -> str:
    header = (
        "Statement,Header,域名称,域值\n"
        "Statement,Data,BrokerName,Interactive Brokers LLC\n"
        "交易,Header,DataDiscriminator,资产分类,货币,代码,日期/时间,数量,交易价格,"
        "收盘价格,收益,佣金/税,基础,已实现的损益,按市值计算的损益,代码\n"
    )
    return header + "\n".join(rows) + "\n"


def test_parse_statement_fills_reads_spxw_options(tmp_path: Path) -> None:
    path = tmp_path / "statement.csv"
    path.write_text(
        _statement(
            [
                _row("SPXW 02JUL26 7480 C", "2026-07-02, 02:41:39", 1, 25.55, 0, "O"),
                _row("SPXW 02JUL26 7480 C", "2026-07-02, 09:35:32", -1, 41.55, 1596.74, "C"),
                _row("CBRS 10JUL26 247.5 C", "2026-06-30, 15:56:54", -1, 8, 0, "O"),  # not SPXW
            ]
        ),
        encoding="utf-8",
    )
    fills = parse_statement_fills(path)
    assert len(fills) == 2
    first = fills[0]
    assert first.expiry.isoformat() == "2026-07-02"
    assert first.strike == 7480.0
    assert first.right == "C"
    assert first.at == ENTRY  # EDT -> UTC
    assert first.is_open
    assert not fills[1].is_open


def test_pair_spread_rounds_debit_and_naked(tmp_path: Path) -> None:
    rows = [
        _row("SPXW 02JUL26 7480 C", "2026-07-02, 02:41:39", 1, 25.55, 0, "O"),
        _row("SPXW 02JUL26 7540 C", "2026-07-02, 02:41:39", -1, 4.05, 0, "O"),
        _row("SPXW 02JUL26 7480 C", "2026-07-02, 09:35:32", -1, 41.55, 1596.74, "C"),
        _row("SPXW 02JUL26 7540 C", "2026-07-02, 09:35:32", 1, 5.55, -153.26, "C"),
        _row("SPXW 08JUL26 7430 C", "2026-07-08, 07:23:35", 1, 24, 0, "O"),  # naked
        _row("SPXW 08JUL26 7430 C", "2026-07-08, 08:29:28", -1, 42, 1796.74, "C"),
    ]
    path = tmp_path / "statement.csv"
    path.write_text(_statement(rows), encoding="utf-8")
    fills = parse_statement_fills(path)
    rounds, naked = pair_spread_rounds(fills)
    assert naked == 1
    assert len(rounds) == 1
    spread = rounds[0]
    assert spread.kind == "debit"
    assert spread.width == 60.0
    assert spread.units == 1
    assert spread.entry_per_unit == 21.5
    assert spread.actual_pnl == 1596.74 - 153.26
    assert spread.close_at == datetime(2026, 7, 2, 13, 35, 32, tzinfo=timezone.utc)


def test_pair_spread_rounds_credit_kind(tmp_path: Path) -> None:
    rows = [
        _row("SPXW 16JUL26 7600 C", "2026-07-16, 10:19:22", 5, 1.0, 0, "O"),
        _row("SPXW 16JUL26 7585 C", "2026-07-16, 10:19:22", -5, 6.0, 0, "O"),
        _row("SPXW 16JUL26 7600 C", "2026-07-16, 13:37:52", -5, 0.5, -250.0, "C"),
        _row("SPXW 16JUL26 7585 C", "2026-07-16, 13:37:52", 5, 3.0, -750.0, "C"),
    ]
    path = tmp_path / "statement.csv"
    path.write_text(_statement(rows), encoding="utf-8")
    fills = parse_statement_fills(path)
    rounds, naked = pair_spread_rounds(fills)
    assert naked == 0
    assert len(rounds) == 1
    assert rounds[0].kind == "credit"
    assert rounds[0].entry_per_unit == 5.0  # 6.0 received - 1.0 paid
    assert rounds[0].width == 15.0


def _tick(at: datetime, bid: float, ask: float) -> OptionTick:
    return OptionTick(at=at, bid=bid, ask=ask, mid=(bid + ask) / 2)


def test_replay_sat85_triggers_for_debit_spread() -> None:
    stop = ENTRY + timedelta(hours=3)
    long_series = [
        _tick(ENTRY, 24.8, 25.0),  # value 24.9 - 3.95 = 20.95 < 51
        _tick(ENTRY + timedelta(minutes=30), 55.0, 55.2),  # value 55.1-3.95 = 51.15 >= 51
    ]
    short_series = [_tick(ENTRY, 3.9, 4.0), _tick(ENTRY + timedelta(minutes=30), 3.9, 4.0)]
    result = replay_spread_rule(
        rule="sat85",
        kind="debit",
        width=60.0,
        entry_per_unit=21.5,
        entry_at=ENTRY,
        stop_at=stop,
        long_series=long_series,
        short_series=short_series,
    )
    assert result is not None
    assert result.reason == "saturation"
    assert result.exit_at == ENTRY + timedelta(minutes=30)


def test_replay_trail33_arms_and_gives_back() -> None:
    stop = ENTRY + timedelta(hours=3)
    long_series = [
        _tick(ENTRY, 24.8, 25.0),
        _tick(ENTRY + timedelta(minutes=10), 35.0, 35.2),  # value 31.15 >= 30: arm
        _tick(ENTRY + timedelta(minutes=20), 28.0, 28.2),  # value 24.15, pnl 2.65 <= 2/3*9.65
    ]
    short_series = [
        _tick(ENTRY, 3.9, 4.0),
        _tick(ENTRY + timedelta(minutes=10), 3.9, 4.0),
        _tick(ENTRY + timedelta(minutes=20), 3.9, 4.0),
    ]
    result = replay_spread_rule(
        rule="trail33",
        kind="debit",
        width=60.0,
        entry_per_unit=21.5,
        entry_at=ENTRY,
        stop_at=stop,
        long_series=long_series,
        short_series=short_series,
    )
    assert result is not None
    assert result.reason == "trailing_tp"
    assert result.exit_at == ENTRY + timedelta(minutes=20)


def test_replay_clock_exits_at_stop_and_no_path_when_empty() -> None:
    stop = ENTRY + timedelta(hours=2)
    long_series = [_tick(ENTRY + timedelta(minutes=m), 24.8, 25.0) for m in (0, 60, 130)]
    short_series = [_tick(ENTRY + timedelta(minutes=m), 3.9, 4.0) for m in (0, 60, 130)]
    result = replay_spread_rule(
        rule="clock",
        kind="debit",
        width=60.0,
        entry_per_unit=21.5,
        entry_at=ENTRY,
        stop_at=stop,
        long_series=long_series,
        short_series=short_series,
    )
    assert result is not None
    assert result.reason == "time_stop"
    assert result.exit_at == ENTRY + timedelta(minutes=130)
    missing = replay_spread_rule(
        rule="clock",
        kind="debit",
        width=60.0,
        entry_per_unit=21.5,
        entry_at=ENTRY,
        stop_at=stop,
        long_series=[],
        short_series=short_series,
    )
    assert missing is None


def test_replay_sat85_mirrors_for_credit_spread() -> None:
    stop = ENTRY + timedelta(hours=3)
    # credit: liability = short_mid - long_mid; saturate when liability <= 15% of width
    long_series = [
        _tick(ENTRY, 0.9, 1.0),  # liability 5.95 - 0.95 = 5.0
        _tick(ENTRY + timedelta(minutes=30), 0.4, 0.5),  # liability 4.55 - 0.45 = 4.1
        _tick(ENTRY + timedelta(minutes=60), 3.9, 4.0),  # liability 2.05 - 3.95 <= 2.25
    ]
    short_series = [
        _tick(ENTRY, 5.9, 6.0),
        _tick(ENTRY + timedelta(minutes=30), 4.5, 4.6),
        _tick(ENTRY + timedelta(minutes=60), 2.0, 2.1),
    ]
    result = replay_spread_rule(
        rule="sat85",
        kind="credit",
        width=15.0,
        entry_per_unit=5.0,
        entry_at=ENTRY,
        stop_at=stop,
        long_series=long_series,
        short_series=short_series,
    )
    assert result is not None
    assert result.reason == "saturation"
    assert result.exit_at == ENTRY + timedelta(minutes=60)


def test_replay_credit_sat85_uses_entry_credit_as_max_profit() -> None:
    stop = ENTRY + timedelta(hours=2)
    long_series = [
        _tick(ENTRY, 0.9, 1.1),
        _tick(ENTRY + timedelta(minutes=10), 0.9, 1.1),
        _tick(ENTRY + timedelta(minutes=20), 0.9, 1.1),
    ]
    short_series = [
        _tick(ENTRY, 5.9, 6.1),  # liability=5.0, PnL=0
        _tick(ENTRY + timedelta(minutes=10), 1.9, 2.1),  # liability=1.0, PnL=4.0 < 4.25
        _tick(ENTRY + timedelta(minutes=20), 1.6, 1.8),  # liability=0.7, PnL=4.3
    ]
    result = replay_spread_rule(
        rule="sat85",
        kind="credit",
        width=100.0,
        entry_per_unit=5.0,
        entry_at=ENTRY,
        stop_at=stop,
        long_series=long_series,
        short_series=short_series,
    )
    assert result is not None
    assert result.reason == "saturation"
    assert result.exit_at == ENTRY + timedelta(minutes=20)


def test_replay_credit_trail_does_not_arm_from_width_alone() -> None:
    stop = ENTRY + timedelta(minutes=20)
    long_series = [
        _tick(ENTRY, 0.9, 1.1),
        _tick(ENTRY + timedelta(minutes=10), 0.9, 1.1),
        _tick(ENTRY + timedelta(minutes=20), 0.9, 1.1),
    ]
    short_series = [
        _tick(ENTRY, 5.9, 6.1),  # PnL=0
        _tick(ENTRY + timedelta(minutes=10), 4.9, 5.1),  # PnL=1, below 50% credit
        _tick(ENTRY + timedelta(minutes=20), 5.4, 5.6),  # giveback without arming
    ]
    result = replay_spread_rule(
        rule="trail33",
        kind="credit",
        width=15.0,
        entry_per_unit=5.0,
        entry_at=ENTRY,
        stop_at=stop,
        long_series=long_series,
        short_series=short_series,
    )
    assert result is not None
    assert result.reason == "time_stop"
    assert result.exit_at == stop


def test_replay_rejects_future_short_at_entry_with_audit_reason() -> None:
    attempt = replay_spread_rule_attempt(
        rule="clock",
        kind="debit",
        width=60.0,
        entry_per_unit=21.5,
        entry_at=ENTRY,
        stop_at=ENTRY + timedelta(hours=1),
        long_series=[_tick(ENTRY, 24.8, 25.0)],
        short_series=[_tick(ENTRY + timedelta(seconds=1), 3.9, 4.0)],
        max_entry_quote_age=timedelta(seconds=30),
        max_mark_quote_age=timedelta(seconds=30),
        max_leg_skew=timedelta(seconds=5),
    )
    assert attempt.exit is None
    assert attempt.skip_reason == "entry_short_missing_at_or_before"


def test_replay_rejects_stale_or_skewed_entry_pair() -> None:
    stale = replay_spread_rule_attempt(
        rule="clock",
        kind="debit",
        width=60.0,
        entry_per_unit=21.5,
        entry_at=ENTRY,
        stop_at=ENTRY + timedelta(hours=1),
        long_series=[_tick(ENTRY - timedelta(seconds=31), 24.8, 25.0)],
        short_series=[_tick(ENTRY - timedelta(seconds=31), 3.9, 4.0)],
        max_entry_quote_age=timedelta(seconds=30),
        max_mark_quote_age=timedelta(seconds=30),
        max_leg_skew=timedelta(seconds=5),
    )
    assert stale.exit is None
    assert stale.skip_reason is not None
    assert stale.skip_reason.startswith("entry_long_stale(")

    skewed = replay_spread_rule_attempt(
        rule="clock",
        kind="debit",
        width=60.0,
        entry_per_unit=21.5,
        entry_at=ENTRY,
        stop_at=ENTRY + timedelta(hours=1),
        long_series=[_tick(ENTRY, 24.8, 25.0)],
        short_series=[_tick(ENTRY - timedelta(seconds=6), 3.9, 4.0)],
        max_entry_quote_age=timedelta(seconds=30),
        max_mark_quote_age=timedelta(seconds=30),
        max_leg_skew=timedelta(seconds=5),
    )
    assert skewed.exit is None
    assert skewed.skip_reason is not None
    assert skewed.skip_reason.startswith("entry_leg_skew(")


def test_replay_does_not_ffill_stale_short_to_clock_exit() -> None:
    stop = ENTRY + timedelta(minutes=2)
    attempt = replay_spread_rule_attempt(
        rule="clock",
        kind="debit",
        width=60.0,
        entry_per_unit=21.5,
        entry_at=ENTRY,
        stop_at=stop,
        long_series=[_tick(ENTRY, 24.8, 25.0), _tick(stop, 30.0, 30.2)],
        short_series=[_tick(ENTRY, 3.9, 4.0)],
        max_entry_quote_age=timedelta(seconds=30),
        max_mark_quote_age=timedelta(seconds=30),
        max_leg_skew=timedelta(minutes=5),
    )
    assert attempt.exit is None
    assert attempt.skip_reason is not None
    assert attempt.skip_reason.startswith("no_fresh_exit_mark_before_grace(")
    assert "stale=1" in attempt.skip_reason


def test_replay_clocks_follow_new_york_dst_and_expiry_close() -> None:
    summer = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
    winter = datetime(2026, 12, 15, 13, 0, tzinfo=timezone.utc)
    assert next_replay_exit_clock(summer) == datetime(2026, 7, 15, 13, 45, tzinfo=timezone.utc)
    assert next_replay_exit_clock(winter) == datetime(2026, 12, 15, 14, 45, tzinfo=timezone.utc)
    assert replay_expiry_close(summer.date()) == datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)
    assert replay_expiry_close(winter.date()) == datetime(2026, 12, 15, 21, 0, tzinfo=timezone.utc)


def _spread_round(
    *, round_id: str, open_at: datetime, actual_pnl: float, commissions: float
) -> SpreadRound:
    return SpreadRound(
        round_id=round_id,
        expiry=open_at.date(),
        right="C",
        kind="debit",
        pos_strike=7500.0,
        neg_strike=7550.0,
        width=50.0,
        units=1,
        open_at=open_at,
        entry_per_unit=10.0,
        close_at=open_at + timedelta(hours=1),
        actual_pnl=actual_pnl,
        commissions=commissions,
        fills=(),
    )


def test_report_compares_only_common_rounds_and_splits_cohorts() -> None:
    gth = _spread_round(
        round_id="gth",
        open_at=datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc),
        actual_pnl=100.0,
        commissions=-4.0,
    )
    rth = _spread_round(
        round_id="rth",
        open_at=datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc),
        actual_pnl=200.0,
        commissions=-6.0,
    )
    exit_at = datetime(2026, 7, 15, 13, 45, tzinfo=timezone.utc)
    all_rules = {
        rule: RuleExit(rule, exit_at, pnl_per_unit=1.0, reason="time_stop")
        for rule in ("sat85", "trail33", "clock")
    }
    report = render_replay_report(
        [gth, rth],
        outcomes={
            "gth": all_rules,
            "rth": {"sat85": RuleExit("sat85", exit_at, 9.0, "saturation")},
        },
        skips={"rth": {"trail33": "stale_mark", "clock": "stale_mark"}},
        naked_groups=0,
    )
    assert "| GTH Call debit 0DTE | 1 | 1 | +100 | -4 | +100 / +96" in report
    assert "| GTH debit | 1 | 1 | +100 | -4 | +100 / +96" in report
    assert "| all | 2 | 1 | +100 | -4 | +100 / +96" in report
    assert "全部 2 价差回合的实际净盈亏 +300$" in report
    assert "`stale_mark` | 2" in report
