from __future__ import annotations

from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone

import pytest

from spx_spark.data_platform.research.odte_level_signals import OptionTick, UnderlierTick
from spx_spark.data_platform.research.odte_tick_signals import (
    EXITS,
    RULE_SPECS,
    DeferredRichCandidate,
    build_report,
    fit_credit_q80,
    fit_ret1m_q20,
    iter_opportunities,
    materialize_deferred_rich,
    metrics,
    mine_session,
    passes_gate,
)
from spx_spark.market_calendar import ET


SESSION_DATE = date(2026, 8, 17)


def _at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 8, 17, hour, minute, second, tzinfo=ET).astimezone(timezone.utc)


def _tick(at: datetime, bid: float, ask: float) -> OptionTick:
    return OptionTick(
        at=at,
        bid=bid,
        ask=ask,
        mid=(bid + ask) / 2.0,
        source_at=at,
        delta=None,
        implied_vol=None,
    )


class FakeStore:
    def __init__(self, *, schwab: bool = True) -> None:
        self.load_calls = 0
        self.snapshot_times: list[datetime] = []
        self.series: dict[tuple[str, float, str], list[OptionTick]] = {}
        providers = ("schwab", "ibkr") if schwab else ("ibkr",)
        put_times = (
            _at(9, 30, 1),
            _at(12, 59, 59),
            _at(13, 30),
            _at(13, 59, 59),
            _at(14, 30),
            _at(15, 44),
        )
        call_times = (_at(13, 0, 2), _at(14, 30), _at(15, 44))
        condor_times = (_at(14, 0, 3), _at(14, 30), _at(15, 44))
        for provider in providers:
            for strike, right, bid, ask in (
                (6000.0, "P", 6.0, 6.2),
                (5990.0, "P", 2.0, 2.2),
            ):
                self.series[(provider, strike, right)] = [_tick(at, bid, ask) for at in put_times]
            for strike, right, bid, ask in (
                (6000.0, "C", 6.0, 6.2),
                (6010.0, "C", 2.0, 2.2),
            ):
                self.series[(provider, strike, right)] = [_tick(at, bid, ask) for at in call_times]
            for strike, right, bid, ask in (
                (5975.0, "P", 0.4, 0.6),
                (5985.0, "P", 1.5, 1.7),
                (6015.0, "C", 1.4, 1.6),
                (6025.0, "C", 0.3, 0.5),
            ):
                self.series[(provider, strike, right)] = [
                    _tick(at, bid, ask) for at in condor_times
                ]

    def load_option_window(self, **_kwargs) -> int:
        self.load_calls += 1
        return sum(len(ticks) for ticks in self.series.values())

    def option_series(
        self,
        *,
        provider: str,
        strike: float,
        right: str,
        start: datetime,
        end: datetime,
        **_kwargs,
    ) -> list[OptionTick]:
        return [
            tick
            for tick in self.series.get((provider, strike, right), ())
            if start <= tick.at <= end
        ]

    def option_snapshot(
        self,
        *,
        as_of: datetime,
        max_age_seconds: float,
        strikes,
        **_kwargs,
    ) -> dict[tuple[str, float, str], OptionTick]:
        self.snapshot_times.append(as_of)
        wanted = {float(strike) for strike in strikes}
        result = {}
        for key, ticks in self.series.items():
            if key[1] not in wanted:
                continue
            index = bisect_right(ticks, as_of, key=lambda tick: tick.at) - 1
            if index >= 0 and ticks[index].at >= as_of - timedelta(seconds=max_age_seconds):
                result[key] = ticks[index]
        return result


def _spx() -> list[UnderlierTick]:
    times = (
        _at(9, 30),
        _at(9, 31),
        _at(12, 59),
        _at(13, 0),
        _at(13, 59),
        _at(14, 0),
        _at(14, 29, 59),
        _at(15, 43, 59),
    )
    return [UnderlierTick(at, 6000.0) for at in times]


def _metric_rows(n: int, pnl: float) -> list[dict[str, object]]:
    return [
        {
            "session_date": f"2026-07-{index + 1:02d}",
            "pnl_hold_1545": pnl,
            "pnl_giveback_50": pnl,
        }
        for index in range(n)
    ]


def test_manifest_is_frozen_to_eight_rules_and_sixteen_hypotheses() -> None:
    assert len(RULE_SPECS) == 8
    assert len({rule.name for rule in RULE_SPECS}) == 8
    assert len(RULE_SPECS) * len(EXITS) == 16


def test_event_clock_uses_only_received_timestamps_and_prefers_schwab() -> None:
    store = FakeStore()
    events = list(
        iter_opportunities(
            store,  # type: ignore[arg-type]
            session_date=SESSION_DATE,
            spx=_spx(),
            structure_name="put_credit_vertical",
            start=_at(9, 30),
            end=_at(9, 31),
        )
    )

    assert [event.at for event in events] == [_at(9, 30), _at(9, 30, 1), _at(9, 31)]
    executable = next(event for event in events if event.entries["put_credit_vertical"])
    entry = executable.entries["put_credit_vertical"]
    assert entry is not None
    assert entry["provider"] == "schwab"
    assert entry["entry_value"] == pytest.approx(3.8)


def test_event_clock_falls_back_to_ibkr_when_schwab_legs_are_missing() -> None:
    events = list(
        iter_opportunities(
            FakeStore(schwab=False),  # type: ignore[arg-type]
            session_date=SESSION_DATE,
            spx=_spx(),
            structure_name="put_credit_vertical",
            start=_at(9, 30),
            end=_at(9, 31),
        )
    )

    entry = next(
        event.entries["put_credit_vertical"]
        for event in events
        if event.entries["put_credit_vertical"] is not None
    )
    assert entry is not None and entry["provider"] == "ibkr"


def test_complete_but_invalid_schwab_bbo_does_not_silently_use_ibkr() -> None:
    store = FakeStore()
    store.series[("schwab", 5990.0, "P")] = [
        OptionTick(at, 2.0, None, None, at, None, None)
        for at in (_at(9, 30, 1), _at(9, 31))
    ]

    events = list(
        iter_opportunities(
            store,  # type: ignore[arg-type]
            session_date=SESSION_DATE,
            spx=_spx(),
            structure_name="put_credit_vertical",
            start=_at(9, 30),
            end=_at(9, 31),
        )
    )

    assert all(event.entries["put_credit_vertical"] is None for event in events)


def test_session_loads_one_window_and_opens_each_rule_at_most_once() -> None:
    store = FakeStore()
    result = mine_session(
        store,  # type: ignore[arg-type]
        session_date=SESSION_DATE,
        spx=_spx(),
        ret1m_q20=0.0,
        credit_q80=0.35,
    )

    assert store.load_calls == 1
    assert result.coverage["option_window_loads"] == 1
    assert len(result.rows) == 8
    assert len({str(row["entry_rule"]) for row in result.rows}) == 8
    assert all(row["provider"] == "schwab" for row in result.rows)
    assert all(row["entry_price_basis"].endswith("no_mid") for row in result.rows)
    by_rule = {str(row["entry_rule"]): row for row in result.rows}
    assert by_rule["put_credit_open"]["decision_at"] == _at(9, 30, 1).isoformat()
    assert by_rule["call_credit_after_1300"]["decision_at"] == _at(13, 0, 2).isoformat()
    assert by_rule["iron_condor_after_1400"]["decision_at"] == _at(14, 0, 3).isoformat()


def test_train_thresholds_ignore_holdout_and_deferred_rich_uses_first_crossing() -> None:
    train_open = datetime(2026, 7, 6, 9, 30, tzinfo=ET).astimezone(timezone.utc)
    train_ticks = [
        UnderlierTick(train_open, 100.0),
        UnderlierTick(train_open + timedelta(minutes=1), 90.0),
        UnderlierTick(train_open + timedelta(minutes=2), 99.0),
    ]
    holdout_ticks = [
        UnderlierTick(datetime(2026, 8, 3, 13, 30, tzinfo=timezone.utc), 1.0),
        UnderlierTick(datetime(2026, 8, 3, 13, 31, tzinfo=timezone.utc), 1000.0),
    ]
    threshold, count = fit_ret1m_q20(
        {date(2026, 7, 6): train_ticks, date(2026, 8, 3): holdout_ticks}
    )
    assert count == 2
    assert threshold == pytest.approx(-0.06)
    assert fit_credit_q80([0.1, 0.2, 0.3, 0.4, 0.5]) == pytest.approx(0.42)

    base = {
        "session_date": "2026-07-06",
        "decision_at": _at(10, 0).isoformat(),
        "pnl_hold_1545": 1.0,
        "pnl_giveback_50": 1.0,
    }
    candidates = [
        DeferredRichCandidate(
            "2026-07-06",
            "put_credit_rich",
            premium,
            _at(10, minute).isoformat(),
            {**base, "decision_at": _at(10, minute).isoformat()},
        )
        for minute, premium in ((0, 0.2), (1, 0.5), (2, 0.8))
    ]
    selected = materialize_deferred_rich(candidates, credit_q80=0.4)
    assert len(selected) == 1
    assert selected[0]["decision_at"] == _at(10, 1).isoformat()


def test_daily_gate_uses_frozen_session_minima_and_positive_lcb() -> None:
    eight = metrics(_metric_rows(8, 1.0), "hold_1545")
    seven = metrics(_metric_rows(7, 1.0), "hold_1545")
    five = metrics(_metric_rows(5, 1.0), "hold_1545")

    assert passes_gate(eight, train=True)
    assert not passes_gate(seven, train=True)
    assert passes_gate(five, train=False)
    assert not passes_gate(metrics(_metric_rows(8, -1.0), "hold_1545"), train=True)


def test_report_has_sixteen_session_hypotheses_and_no_live_write(tmp_path) -> None:
    report = build_report(
        [],
        thresholds={
            "ret1m_q20": -0.001,
            "ret1m_sample_count": 10,
            "put_credit_fraction_q80": 0.4,
            "credit_sample_count": 20,
        },
        coverages=[],
        prior_rows_path=tmp_path / "missing.jsonl",
    )

    assert report["hypothesis_count"] == 16
    assert report["funnel"]["robust_survivors"] == 0
    assert report["honesty"]["live_path_written"] is False
    assert report["honesty"]["one_trade"].startswith("per rule per RTH session")
