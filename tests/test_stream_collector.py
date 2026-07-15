from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from stream_test_helpers import patch_stream


import spx_spark.ibkr.stream.deps as stream_collector_module
from spx_spark.config import IbkrBrokerSettings, SamplingSettings
from spx_spark.ibkr.adapter import snapshot_from_rows
from spx_spark.ibkr.verifier import VerifyRow
from spx_spark.ibkr.stream_collector import (
    OptionSubscriptionPlan,
    ReconnectPolicy,
    StreamAction,
    StreamCollector,
    StreamRuntime,
    build_option_subscription_plan,
    decide_after_flush,
    effective_hot_flush_sleep_seconds,
    lifecycle_has_qualification_budget,
    merge_cached_option_rows,
    option_spec_label,
    reference_quote_from_row,
    sleep_until_reconnect,
    subscription_outage_reason,
    should_replan,
    update_option_cache,
)
from spx_spark.sampling import OptionContractSpec


def make_sampling_settings(**overrides) -> SamplingSettings:
    values = {
        "strike_step": 5,
        "window_points": 200,
        "hot_window_points": 50,
        "group_count": 4,
        "group_interval_seconds": 4,
        "degraded_group_count": 20,
        "degraded_group_interval_seconds": 3,
        "group_strategy": "interleaved",
        "hot_human_cadence_seconds": 8,
        "hot_execution_cadence_seconds": 2,
        "include_next_expiry": False,
        "default_mode": "human_alert",
    }
    values.update(overrides)
    return SamplingSettings(**values)


def test_lifecycle_budget_reserves_one_bounded_qualification() -> None:
    assert lifecycle_has_qualification_budget(100.0, now_monotonic=100.49)
    assert lifecycle_has_qualification_budget(100.0, now_monotonic=100.5)
    assert not lifecycle_has_qualification_budget(100.0, now_monotonic=100.51)


def test_position_shadow_failure_never_breaks_market_data_or_overwrites_snapshot(
    monkeypatch,
) -> None:
    collector = object.__new__(StreamCollector)
    collector.broker_settings = IbkrBrokerSettings(
        account_read_enabled=True,
        position_shadow_enabled=True,
        position_shadow_interval_seconds=60,
        position_shadow_path="shadow.json",
        execution_mode="manual",
    )
    collector.last_position_shadow_at = None
    collector.ib = object()
    collector.storage_settings = object()
    writes: list[object] = []
    patch_stream(monkeypatch, "fetch_positions", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("shadow failed")),
    )
    patch_stream(monkeypatch, "write_snapshot", lambda *args, **kwargs: writes.append(args),
    )

    event = collector.flush_position_shadow_if_due(now_monotonic=100.0)

    assert event == {
        "task": "ibkr_stream",
        "event": "position_shadow_failed",
        "ok": False,
        "error_type": "RuntimeError",
    }
    assert writes == []
    assert collector.flush_position_shadow_if_due(now_monotonic=120.0) is None


def test_account_visibility_keeps_connection_without_enabling_market_data() -> None:
    collector = object.__new__(StreamCollector)
    collector.broker_settings = SimpleNamespace(
        account_read_enabled=True,
        execution_mode="manual",
    )
    collector.market_data_allowed = lambda: False

    assert collector.connection_required() is True

    collector.broker_settings = SimpleNamespace(
        account_read_enabled=False,
        execution_mode="manual",
    )
    assert collector.connection_required() is False


def test_account_read_uses_position_capable_socket_even_when_shadow_write_is_off(
    monkeypatch,
) -> None:
    collector = object.__new__(StreamCollector)
    collector.ib = object()
    collector.ibkr_settings = object()
    collector.stream_settings = SimpleNamespace(client_id=172)
    collector.broker_settings = SimpleNamespace(
        account_read_enabled=True,
        position_shadow_enabled=False,
    )
    collector.market_data_allowed = lambda: False
    calls: list[int] = []
    patch_stream(monkeypatch, "connect_broker_readonly_with_positions", lambda _ib, _settings, *, client_id: calls.append(client_id),
    )

    collector.open_session()

    assert calls == [172]
    assert collector.last_position_shadow_at is None


def test_competing_session_cooldown_suppresses_only_market_data(
    monkeypatch,
) -> None:
    now = 100.0
    collector = object.__new__(StreamCollector)
    collector.force = True
    collector.market_data_retry_not_before = 0.0
    collector.market_data_retry_reason = None
    collector.broker_settings = SimpleNamespace(
        account_read_enabled=True,
        execution_mode="manual",
    )
    monkeypatch.setattr(stream_collector_module.time, "monotonic", lambda: now)

    collector.defer_market_data_after_conflict(seconds=30.0)

    assert collector.market_data_allowed() is False
    assert collector.connection_required() is True
    assert collector.market_data_block_reason() == (
        "competing live session owns shared market data (IBKR 10197)"
    )
    assert collector.market_data_retry_delay_seconds() == pytest.approx(30.0)
    now = 131.0
    assert collector.market_data_allowed() is True
    assert collector.market_data_block_reason() is None
    assert collector.market_data_retry_delay_seconds() is None


def test_runtime_preserves_competing_session_reason_during_cooldown(monkeypatch) -> None:
    class BlockedCollector:
        def connection_required(self) -> bool:
            return False

        def market_data_block_reason(self) -> str:
            return "competing live session owns shared market data (IBKR 10197)"

        def market_data_retry_delay_seconds(self) -> float:
            return 15.0

    runtime = StreamRuntime(
        collector=BlockedCollector(),  # type: ignore[arg-type]
        stream_settings=SimpleNamespace(
            reconnect_min_seconds=1.0,
            reconnect_max_seconds=2.0,
            policy_check_seconds=30.0,
        ),
        storage_settings=object(),
        runtime_policy=object(),
    )
    persisted: list[object] = []
    events: list[dict[str, object]] = []
    sleeps: list[float] = []
    patch_stream(monkeypatch, "persist_state_only", lambda state, _storage: persisted.append(state),
    )
    patch_stream(monkeypatch, "log_event", events.append)
    def stop_after_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        runtime.deadline = 0.0

    runtime.sleep = stop_after_sleep

    assert runtime.run() == 0
    assert persisted[0].reason == (
        "competing live session owns shared market data (IBKR 10197)"
    )
    assert events[0]["reason"] == persisted[0].reason
    assert events[0]["retry_in_seconds"] == 15.0
    assert sleeps == [15.0]


def test_account_standby_reconnects_into_market_mode_without_subscribing_in_place(
    monkeypatch,
) -> None:
    position_flushes: list[float] = []

    class FakeIb:
        def sleep(self, _seconds: float) -> None:
            return None

        def isConnected(self) -> bool:  # noqa: N802 - mirrors ib_async
            return True

    class FakeCollector:
        ib = FakeIb()
        tws_connectivity_lost = False

        def flush_position_shadow_if_due(self, *, now_monotonic: float):
            position_flushes.append(now_monotonic)
            return {"event": "position_shadow_written"}

        def connection_required(self) -> bool:
            return True

        def market_data_allowed(self) -> bool:
            return True

    runtime = StreamRuntime(
        collector=FakeCollector(),  # type: ignore[arg-type]
        stream_settings=SimpleNamespace(
            reconnect_min_seconds=1.0,
            reconnect_max_seconds=2.0,
            policy_check_seconds=0.1,
        ),
        storage_settings=object(),
        runtime_policy=object(),
    )
    events: list[dict[str, object]] = []
    patch_stream(monkeypatch, "log_event", events.append)

    assert runtime.account_standby_loop() is False
    assert len(position_flushes) == 1
    assert runtime.session_had_healthy_flush is True
    assert any(event.get("event") == "market_data_activation_requested" for event in events)


def test_subscription_lifecycle_gives_due_slow_poll_priority(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.option_plan = SimpleNamespace(expiry="20260710")
    calls: list[tuple[str, object]] = []
    collector.advance_slow_poll = lambda **kwargs: calls.append(
        ("slow", kwargs["allow_start"])
    )
    collector.ensure_option_plan = lambda rows: calls.append(("option", rows))
    collector.ensure_spy_option_plan = lambda rows, *, expiry: calls.append(
        ("spy", expiry)
    )
    collector.slow_poll_start_due = lambda: True
    collector.rotate_options = lambda: calls.append(("rotate", None))
    patch_stream(monkeypatch, "lifecycle_has_qualification_budget", lambda *_args, **_kwargs: True,
    )

    rows: list[VerifyRow] = []
    collector._advance_subscription_lifecycle(rows, lifecycle_started=100.0)

    assert calls == [
        ("slow", False),
        ("option", rows),
        ("spy", "20260710"),
        ("slow", True),
        ("slow", False),
    ]


def test_subscription_lifecycle_rotates_when_slow_poll_is_not_due(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.option_plan = SimpleNamespace(expiry="20260710")
    calls: list[tuple[str, object]] = []
    collector.advance_slow_poll = lambda **kwargs: calls.append(
        ("slow", kwargs["allow_start"])
    )
    collector.ensure_option_plan = lambda rows: calls.append(("option", rows))
    collector.ensure_spy_option_plan = lambda rows, *, expiry: calls.append(
        ("spy", expiry)
    )
    collector.slow_poll_start_due = lambda: False
    collector.rotate_options = lambda: calls.append(("rotate", None))
    patch_stream(monkeypatch, "lifecycle_has_qualification_budget", lambda *_args, **_kwargs: True,
    )

    rows: list[VerifyRow] = []
    collector._advance_subscription_lifecycle(rows, lifecycle_started=100.0)

    assert calls == [
        ("slow", False),
        ("option", rows),
        ("spy", "20260710"),
        ("rotate", None),
        ("slow", False),
    ]


def test_subscription_lifecycle_starts_no_new_work_without_budget(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.option_plan = SimpleNamespace(expiry="20260710")
    calls: list[tuple[str, object]] = []
    collector.advance_slow_poll = lambda **kwargs: calls.append(
        ("slow", kwargs["allow_start"])
    )
    collector.ensure_option_plan = lambda rows: calls.append(("option", rows))
    collector.ensure_spy_option_plan = lambda rows, *, expiry: calls.append(
        ("spy", expiry)
    )
    collector.slow_poll_start_due = lambda: True
    collector.rotate_options = lambda: calls.append(("rotate", None))
    patch_stream(monkeypatch, "lifecycle_has_qualification_budget", lambda *_args, **_kwargs: False,
    )

    rows: list[VerifyRow] = []
    collector._advance_subscription_lifecycle(rows, lifecycle_started=100.0)

    assert calls == [
        ("slow", False),
        ("option", rows),
        ("slow", False),
    ]


def test_subscription_lifecycle_pauses_during_tws_connectivity_loss() -> None:
    collector = object.__new__(StreamCollector)
    collector.tws_connectivity_lost = True
    collector.subscriptions_lost = False
    calls: list[str] = []
    collector.advance_slow_poll = lambda **kwargs: calls.append("slow")
    collector.ensure_option_plan = lambda rows: calls.append("option")
    collector.ensure_spy_option_plan = lambda rows, *, expiry: calls.append("spy")
    collector.rotate_options = lambda: calls.append("rotate")

    collector._advance_subscription_lifecycle([], lifecycle_started=100.0)

    assert calls == []

    collector.tws_connectivity_lost = False
    collector.subscriptions_lost = True
    collector._advance_subscription_lifecycle([], lifecycle_started=100.0)

    assert calls == []


def test_hot_flush_sleep_is_capped_for_twelve_second_reliability_budget() -> None:
    assert effective_hot_flush_sleep_seconds(2.0) == 2.0
    assert effective_hot_flush_sleep_seconds(5.0) == 5.0
    assert effective_hot_flush_sleep_seconds(30.0) == 5.0


def test_subscription_outage_reason_prioritizes_lost_subscription_state() -> None:
    assert subscription_outage_reason(
        tws_connectivity_lost=True,
        subscriptions_lost=False,
    ) == "TWS upstream connectivity lost; subscription lifecycle paused"
    assert subscription_outage_reason(
        tws_connectivity_lost=False,
        subscriptions_lost=True,
    ) == "TWS restored without market-data subscriptions; rebuilding"
    assert subscription_outage_reason(
        tws_connectivity_lost=False,
        subscriptions_lost=False,
    ) is None


def test_snapshot_from_rows_can_request_provider_replace():
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    rows = [
        VerifyRow(
            label="index:SPX",
            kind="index",
            symbol="SPX",
            exchange="CBOE",
            market_data_type=1,
            market_price=7524.0,
            ticker_time=now.isoformat(),
        )
    ]

    snapshot = snapshot_from_rows(
        rows,
        received_at=now,
        stale_after_seconds=15.0,
        connected=True,
        authenticated=True,
        latency_ms=12.0,
        replace_provider_quotes=True,
    )

    assert snapshot.metadata["replace_provider_quotes"] is True


def test_atm_reference_uses_source_time_and_rejects_frozen_feed() -> None:
    row = VerifyRow(
        label="index:SPX",
        kind="index",
        symbol="SPX",
        close=7500.0,
        market_data_type=2,
        stale=False,
        ticker_time="2026-07-10T14:00:00+00:00",
        last_update_at="2026-07-10T14:01:00+00:00",
    )

    reference = reference_quote_from_row(row)

    assert reference is not None
    assert reference.freshness == "frozen"
    assert reference.observed_at == datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)

    close_only = reference_quote_from_row(
        VerifyRow(
            label="index:SPX",
            kind="index",
            symbol="SPX",
            close=7500.0,
            market_data_type=1,
            stale=False,
            ticker_time="2026-07-10T14:00:00+00:00",
        ),
        as_of=datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc),
    )
    assert close_only is not None
    assert close_only.freshness == "close_only"
    assert close_only.is_fresh is False


def test_atm_reference_fails_closed_for_unknown_feed_and_future_source_time() -> None:
    decision_at = datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)
    unknown_feed = reference_quote_from_row(
        VerifyRow(
            label="index:SPX",
            kind="index",
            symbol="SPX",
            last=7500.0,
            market_data_type=None,
            stale=False,
            ticker_time=decision_at.isoformat(),
        ),
        as_of=decision_at,
    )
    future_tick = reference_quote_from_row(
        VerifyRow(
            label="index:SPX",
            kind="index",
            symbol="SPX",
            last=7500.0,
            market_data_type=1,
            stale=False,
            ticker_time="2026-07-10T14:00:06+00:00",
        ),
        as_of=decision_at,
    )

    assert unknown_feed is not None and unknown_feed.freshness == "unknown"
    assert future_tick is not None and future_tick.freshness == "unknown"


def test_stale_spx_bootstrap_reference_uses_close_not_stale_last() -> None:
    decision_at = datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)
    reference = reference_quote_from_row(
        VerifyRow(
            label="index:SPX",
            kind="index",
            symbol="SPX",
            last=7000.0,
            close=7500.0,
            market_data_type=1,
            stale=True,
            ticker_time=decision_at.isoformat(),
        ),
        as_of=decision_at,
    )

    assert reference is not None
    assert reference.freshness == "stale"
    assert reference.value == 7500.0


def test_option_plan_respects_line_budget_and_keeps_pairs():
    plan = build_option_subscription_plan(
        atm_reference=7500.0,
        expiry="20260706",
        next_expiry=None,
        mode="human_alert",
        sampling_settings=make_sampling_settings(),
        max_option_lines=60,
        hot_lane_share=0.7,
    )

    assert len(plan.hot) == 42  # 60 * 0.7 = 42, already an even pair count
    assert len(plan.hot) % 2 == 0
    for rotation in plan.rotations:
        assert 0 < len(rotation) <= 18
    # Hot lane is centered on ATM.
    strikes = {spec.strike for spec in plan.hot}
    assert 7500 in strikes
    assert max(abs(strike - 7500) for strike in strikes) <= 50


def test_option_plan_never_splits_pairs_across_two_expiries() -> None:
    plan = build_option_subscription_plan(
        atm_reference=7500.0,
        expiry="20260706",
        next_expiry="20260707",
        mode="human_alert",
        sampling_settings=make_sampling_settings(
            include_next_expiry=True,
            next_expiry_hot_window_points=25,
            next_expiry_window_points=50,
        ),
        max_option_lines=18,
        hot_lane_share=0.7,
    )

    for lane in (plan.hot, *plan.rotations):
        rights_by_contract: dict[tuple[str, int], set[str]] = {}
        for spec in lane:
            rights_by_contract.setdefault((spec.expiry, spec.strike), set()).add(spec.right)
        assert all(rights == {"C", "P"} for rights in rights_by_contract.values())


def test_option_plan_normalizes_odd_and_tiny_line_budgets() -> None:
    odd = build_option_subscription_plan(
        atm_reference=7500.0,
        expiry="20260706",
        next_expiry="20260707",
        mode="human_alert",
        sampling_settings=make_sampling_settings(include_next_expiry=True),
        max_option_lines=61,
        hot_lane_share=0.7,
    )
    assert len(odd.hot) + max((len(group) for group in odd.rotations), default=0) <= 61
    for lane in (odd.hot, *odd.rotations):
        rights: dict[tuple[str, int], set[str]] = {}
        for spec in lane:
            rights.setdefault((spec.expiry, spec.strike), set()).add(spec.right)
        assert all(value == {"C", "P"} for value in rights.values())

    tiny = build_option_subscription_plan(
        atm_reference=7500.0,
        expiry="20260706",
        next_expiry=None,
        mode="human_alert",
        sampling_settings=make_sampling_settings(),
        max_option_lines=1,
        hot_lane_share=2.0,
    )
    assert tiny.hot == ()
    assert tiny.rotations == ()


def test_option_plan_rotations_exclude_hot_contracts():
    plan = build_option_subscription_plan(
        atm_reference=7500.0,
        expiry="20260706",
        next_expiry=None,
        mode="human_alert",
        sampling_settings=make_sampling_settings(),
        max_option_lines=40,
        hot_lane_share=0.5,
    )

    hot_keys = {(spec.expiry, spec.strike, spec.right) for spec in plan.hot}
    for rotation in plan.rotations:
        for spec in rotation:
            assert (spec.expiry, spec.strike, spec.right) not in hot_keys


def test_option_plan_rotations_cover_full_window():
    plan = build_option_subscription_plan(
        atm_reference=7500.0,
        expiry="20260706",
        next_expiry=None,
        mode="human_alert",
        sampling_settings=make_sampling_settings(),
        max_option_lines=60,
        hot_lane_share=0.7,
    )

    rotation_strikes = {spec.strike for rotation in plan.rotations for spec in rotation}
    hot_strikes = {spec.strike for spec in plan.hot}
    all_strikes = rotation_strikes | hot_strikes
    assert min(all_strikes) == 7300
    assert max(all_strikes) == 7700


def _option_row(label: str, *, subscribed: bool = True) -> VerifyRow:
    return VerifyRow(label=label, kind="option", symbol="SPX", subscribed=subscribed)


def test_option_cache_carries_rotated_strikes_across_flushes():
    cache: dict[str, tuple[float, VerifyRow]] = {}
    slice_a = [_option_row("option:SPXW:20260708:7350:P"), _option_row("option:SPXW:20260708:7350:C")]
    update_option_cache(cache, slice_a, now_monotonic=100.0, expiry="20260708")

    # Next flush: rotation moved on to another slice; 7350 must still be
    # merged so walls see the whole chain, not the live slice only.
    slice_b = [_option_row("option:SPXW:20260708:7550:P")]
    update_option_cache(cache, slice_b, now_monotonic=105.0, expiry="20260708")
    rows = merge_cached_option_rows(list(slice_b), cache, {"option:SPXW:20260708:7550:P"})
    labels = {row.label for row in rows}
    assert "option:SPXW:20260708:7350:P" in labels
    assert "option:SPXW:20260708:7350:C" in labels
    # No duplicate for the currently subscribed label.
    assert sum(1 for row in rows if row.label == "option:SPXW:20260708:7550:P") == 1


def test_option_cache_evicts_expired_and_rolled_expiry_rows():
    cache: dict[str, tuple[float, VerifyRow]] = {}
    update_option_cache(
        cache,
        [_option_row("option:SPXW:20260708:7350:P")],
        now_monotonic=0.0,
        expiry="20260708",
    )
    # Past TTL -> evicted.
    update_option_cache(cache, [], now_monotonic=901.0, expiry="20260708")
    assert not cache


def test_option_cache_keeps_every_active_expiry() -> None:
    cache: dict[str, tuple[float, VerifyRow]] = {}
    rows = [
        _option_row("option:SPXW:20260708:7350:P"),
        _option_row("option:SPXW:20260709:7350:P"),
    ]

    update_option_cache(
        cache,
        rows,
        now_monotonic=100.0,
        expiry="20260708",
        active_expiries=frozenset({"20260708", "20260709"}),
    )

    assert set(cache) == {row.label for row in rows}

    update_option_cache(
        cache,
        [_option_row("option:SPXW:20260708:7350:P")],
        now_monotonic=1000.0,
        expiry="20260708",
    )
    # Expiry rollover -> old-expiry rows dropped.
    update_option_cache(cache, [], now_monotonic=1001.0, expiry="20260709")
    assert not cache

    # Unsubscribed rows (failed subscriptions) never enter the cache.
    update_option_cache(
        cache,
        [_option_row("option:SPXW:20260709:7400:P", subscribed=False)],
        now_monotonic=1002.0,
        expiry="20260709",
    )
    assert not cache


def test_should_replan_triggers_on_drift_and_expiry_roll():
    plan = OptionSubscriptionPlan(
        atm_strike=7500,
        expiry="20260706",
        hot=(),
        rotations=(),
    )

    assert should_replan(None, 7500.0, replan_drift_points=10.0, today_expiry="20260706")
    assert not should_replan(plan, 7505.0, replan_drift_points=10.0, today_expiry="20260706")
    assert should_replan(plan, 7512.0, replan_drift_points=10.0, today_expiry="20260706")
    assert should_replan(plan, 7500.0, replan_drift_points=10.0, today_expiry="20260707")
    assert not should_replan(plan, None, replan_drift_points=10.0, today_expiry="20260707")


def test_option_reconcile_adds_replacements_before_removing_obsolete(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.ib = object()
    collector.ibkr_settings = SimpleNamespace(qualify_contracts=False)
    collector.rotation_subs = {
        "rotation": (object(), VerifyRow(label="rotation", kind="option", symbol="SPX"))
    }
    collector.hot_subs = {
        "option:SPXW:20260708:7500:C": (
            object(),
            VerifyRow(label="option:SPXW:20260708:7500:C", kind="option", symbol="SPX"),
        ),
        "option:SPXW:20260708:7495:C": (
            object(),
            VerifyRow(label="option:SPXW:20260708:7495:C", kind="option", symbol="SPX"),
        ),
    }
    collector.option_plan = None
    collector.rotation_index = 3
    call_order: list[tuple[str, set[str]]] = []

    def fake_contracts(specs):
        return [
            (option_spec_label(spec), "option", object())
            for spec in specs
        ]

    def fake_qualify(ib, contracts, *, qualify=False):
        labels = {label for label, _, _ in contracts}
        call_order.append(("add", labels))
        return {
            label: (
                object(),
                VerifyRow(label=label, kind=kind, symbol="SPX", subscribed=True),
            )
            for label, kind, _ in contracts
        }

    def fake_cancel(ib, subscriptions):
        if subscriptions:
            call_order.append(("cancel", set(subscriptions)))

    patch_stream(monkeypatch, "option_contracts_from_specs", fake_contracts)
    patch_stream(monkeypatch, "qualify_and_subscribe", fake_qualify)
    patch_stream(monkeypatch, "cancel_subscriptions", fake_cancel)
    plan = OptionSubscriptionPlan(
        atm_strike=7505,
        expiry="20260708",
        hot=(
            OptionContractSpec(expiry="20260708", strike=7500, right="C", lane="hot"),
            OptionContractSpec(expiry="20260708", strike=7505, right="C", lane="hot"),
        ),
        rotations=(),
    )

    assert collector.reconcile_option_plan(plan) is True
    assert call_order == [
        ("cancel", {"rotation"}),
        ("add", {"option:SPXW:20260708:7505:C"}),
        ("cancel", {"option:SPXW:20260708:7495:C"}),
    ]
    assert set(collector.hot_subs) == {
        "option:SPXW:20260708:7500:C",
        "option:SPXW:20260708:7505:C",
    }
    assert collector.option_plan == plan


def test_option_reconcile_partial_failure_keeps_accepted_hot_plan(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.ib = object()
    collector.ibkr_settings = SimpleNamespace(qualify_contracts=False)
    collector.rotation_subs = {}
    old_plan = OptionSubscriptionPlan(
        atm_strike=7500,
        expiry="20260708",
        hot=(OptionContractSpec(expiry="20260708", strike=7500, right="C", lane="hot"),),
        rotations=(),
    )
    old_subs = {
        "option:SPXW:20260708:7500:C": (
            object(),
            VerifyRow(label="option:SPXW:20260708:7500:C", kind="option", symbol="SPX"),
        )
    }
    collector.hot_subs = old_subs.copy()
    collector.option_plan = old_plan
    collector.rotation_index = 0
    patch_stream(monkeypatch, "option_contracts_from_specs", lambda specs: [(option_spec_label(spec), "option", object()) for spec in specs],
    )
    patch_stream(monkeypatch, "qualify_and_subscribe", lambda ib, contracts, qualify=False: {
            label: (
                None,
                VerifyRow(label=label, kind=kind, symbol="SPX", subscribed=False),
            )
            for label, kind, _ in contracts
        },
    )
    patch_stream(monkeypatch, "cancel_subscriptions", lambda *args: None)
    new_plan = OptionSubscriptionPlan(
        atm_strike=7520,
        expiry="20260708",
        hot=(OptionContractSpec(expiry="20260708", strike=7520, right="C", lane="hot"),),
        rotations=(),
    )

    assert collector.reconcile_option_plan(new_plan) is False
    assert collector.hot_subs == old_subs
    assert collector.option_plan == old_plan


def test_option_reconcile_releases_capacity_before_zero_overlap_cutover(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.ib = object()
    collector.ibkr_settings = SimpleNamespace(qualify_contracts=False)
    collector.stream_settings = SimpleNamespace(max_option_lines=2)
    collector.rotation_subs = {}
    collector.rotation_index = 0
    collector.hot_subs = {
        label: (
            SimpleNamespace(contract=object()),
            VerifyRow(label=label, kind="option", symbol="SPX", subscribed=True),
        )
        for label in (
            "option:SPXW:20260708:7495:C",
            "option:SPXW:20260708:7500:C",
        )
    }
    collector.option_plan = None
    active = set(collector.hot_subs)
    peak = len(active)

    def fake_contracts(specs):
        return [(option_spec_label(spec), "option", object()) for spec in specs]

    def fake_cancel(ib, subscriptions):
        active.difference_update(subscriptions)

    def fake_qualify(ib, contracts, *, qualify=False):
        nonlocal peak
        assert len(active) + len(contracts) <= 2
        active.update(label for label, _, _ in contracts)
        peak = max(peak, len(active))
        return {
            label: (
                SimpleNamespace(contract=contract),
                VerifyRow(label=label, kind=kind, symbol="SPX", subscribed=True),
            )
            for label, kind, contract in contracts
        }

    patch_stream(monkeypatch, "option_contracts_from_specs", fake_contracts)
    patch_stream(monkeypatch, "cancel_subscriptions", fake_cancel)
    patch_stream(monkeypatch, "qualify_and_subscribe", fake_qualify)
    new_plan = OptionSubscriptionPlan(
        atm_strike=7520,
        expiry="20260709",
        hot=(
            OptionContractSpec(expiry="20260709", strike=7520, right="C", lane="hot"),
            OptionContractSpec(expiry="20260709", strike=7520, right="P", lane="hot"),
        ),
        rotations=(),
    )

    assert collector.reconcile_option_plan(new_plan) is True
    assert peak == 2
    assert active == {
        "option:SPXW:20260709:7520:C",
        "option:SPXW:20260709:7520:P",
    }


def test_option_reconcile_correlates_async_rejection_to_added_request(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.ibkr_settings = SimpleNamespace(qualify_contracts=False)
    collector.stream_settings = SimpleNamespace(max_option_lines=2)
    collector.rotation_subs = {}
    collector.rotation_index = 0
    collector.errors = []
    collector.subscription_rejection_sequence = 0
    collector.subscription_rejection_log = []
    collector.subscription_rows_by_req_id = {}
    collector.subscription_health_failed = False
    collector.farm_health = SimpleNamespace(observe=lambda *args: None)
    old_plan = OptionSubscriptionPlan(
        atm_strike=7500,
        expiry="20260708",
        hot=(OptionContractSpec(expiry="20260708", strike=7500, right="C", lane="hot"),),
        rotations=(),
    )
    collector.hot_subs = {
        "option:SPXW:20260708:7500:C": (
            SimpleNamespace(contract=object()),
            VerifyRow(
                label="option:SPXW:20260708:7500:C",
                kind="option",
                symbol="SPX",
                subscribed=True,
            ),
        )
    }
    collector.option_plan = old_plan

    class FakeIB:
        def sleep(self, seconds):
            collector._on_error(501, 101, "max tickers", None)

    collector.ib = FakeIB()
    patch_stream(monkeypatch, "option_contracts_from_specs", lambda specs: [(option_spec_label(spec), "option", object()) for spec in specs],
    )
    patch_stream(monkeypatch, "qualify_and_subscribe", lambda ib, contracts, qualify=False: {
            label: (
                SimpleNamespace(contract=contract),
                VerifyRow(
                    label=label,
                    kind=kind,
                    symbol="SPX",
                    subscribed=True,
                    request_id=501,
                ),
            )
            for label, kind, contract in contracts
        },
    )
    patch_stream(monkeypatch, "cancel_subscriptions", lambda *args: None)
    new_plan = OptionSubscriptionPlan(
        atm_strike=7505,
        expiry="20260708",
        hot=(
            OptionContractSpec(expiry="20260708", strike=7500, right="C", lane="hot"),
            OptionContractSpec(expiry="20260708", strike=7505, right="C", lane="hot"),
        ),
        rotations=(),
    )

    assert collector.reconcile_option_plan(new_plan) is False
    assert collector.option_plan == old_plan
    assert set(collector.hot_subs) == {"option:SPXW:20260708:7500:C"}


def test_confirmation_outage_aborts_batch_and_discards_only_local_state(
    monkeypatch,
) -> None:
    collector = object.__new__(StreamCollector)
    collector.errors = []
    collector.subscription_rejection_sequence = 0
    collector.subscription_rejection_log = []
    collector.subscription_health_failed = False
    collector.tws_connectivity_lost = False
    collector.subscriptions_lost = False
    collector.tws_connectivity_loss_sequence = 0
    collector.farm_health = SimpleNamespace(observe=lambda *args: None)

    row = VerifyRow(
        label="option:SPXW:20260710:7510:C",
        kind="option",
        symbol="SPX",
        subscribed=True,
        request_id=501,
    )
    ticker = SimpleNamespace(contract=object())
    subscriptions = {row.label: (ticker, row)}
    collector.subscription_rows_by_req_id = {501: row}
    collector.subscription_lane_by_req_id = {501: "rotation"}

    class FakeWrapper:
        def __init__(self) -> None:
            self.ended: list[tuple[object, str]] = []

        def endTicker(self, item: object, tick_type: str) -> None:
            self.ended.append((item, tick_type))

    class FakeIB:
        def __init__(self) -> None:
            self.wrapper = FakeWrapper()
            self.server_cancels: list[object] = []

        def isConnected(self) -> bool:  # noqa: N802 - mirrors ib_async
            return True

        def sleep(self, _seconds: float) -> None:
            collector._on_error(-1, 1100, "Connectivity between IBKR and TWS has been lost", None)
            collector._on_error(-1, 1102, "Connectivity restored; data maintained", None)

        def cancelMktData(self, contract: object) -> None:
            self.server_cancels.append(contract)

    collector.ib = FakeIB()
    patch_stream(monkeypatch, "log_event", lambda *args: None)

    assert not collector._subscription_batch_succeeded(
        subscriptions,
        expected_count=1,
        rejection_sequence=0,
        connectivity_sequence=0,
        confirm_seconds=0.5,
        lane="rotation",
    )
    assert not collector._cancel_batch(subscriptions)

    assert collector.ib.server_cancels == []
    assert collector.ib.wrapper.ended == [(ticker, "mktData")]
    assert collector.subscription_rows_by_req_id == {}
    assert collector.subscription_lane_by_req_id == {}
    assert collector.subscriptions_lost is True
    assert collector.subscription_health_failed is True

    def unexpected_restore(*args, **kwargs):
        raise AssertionError("restore attempted during connectivity outage")

    patch_stream(monkeypatch, "qualify_and_subscribe", unexpected_restore)
    assert collector._restore_subscriptions(subscriptions, lane="rotation") == {}


def test_option_reconcile_restores_released_coverage_after_sync_failure(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.ib = object()
    collector.ibkr_settings = SimpleNamespace(qualify_contracts=False)
    collector.stream_settings = SimpleNamespace(max_option_lines=1)
    collector.rotation_subs = {}
    collector.rotation_index = 0
    old_label = "option:SPXW:20260708:7500:C"
    old_plan = OptionSubscriptionPlan(
        atm_strike=7500,
        expiry="20260708",
        hot=(OptionContractSpec(expiry="20260708", strike=7500, right="C", lane="hot"),),
        rotations=(),
    )
    old_contract = object()
    collector.hot_subs = {
        old_label: (
            SimpleNamespace(contract=old_contract),
            VerifyRow(label=old_label, kind="option", symbol="SPX", subscribed=True),
        )
    }
    collector.option_plan = old_plan
    calls = 0

    def fake_qualify(ib, contracts, *, qualify=False):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {
                label: (
                    None,
                    VerifyRow(label=label, kind=kind, symbol="SPX", subscribed=False),
                )
                for label, kind, _ in contracts
            }
        return {
            label: (
                SimpleNamespace(contract=contract),
                VerifyRow(label=label, kind=kind, symbol="SPX", subscribed=True),
            )
            for label, kind, contract in contracts
        }

    patch_stream(monkeypatch, "option_contracts_from_specs", lambda specs: [(option_spec_label(spec), "option", object()) for spec in specs],
    )
    patch_stream(monkeypatch, "qualify_and_subscribe", fake_qualify)
    patch_stream(monkeypatch, "cancel_subscriptions", lambda *args: None)
    new_plan = OptionSubscriptionPlan(
        atm_strike=7520,
        expiry="20260709",
        hot=(OptionContractSpec(expiry="20260709", strike=7520, right="C", lane="hot"),),
        rotations=(),
    )

    assert collector.reconcile_option_plan(new_plan) is False
    assert collector.option_plan == old_plan
    assert set(collector.hot_subs) == {old_label}
    assert collector.hot_subs[old_label][1].subscribed is True


def test_option_spec_label_matches_snapshot_collector_format():
    spec = OptionContractSpec(expiry="20260706", strike=7500, right="C", lane="hot")

    assert option_spec_label(spec) == "option:SPXW:20260706:7500:C"


def test_option_contract_qualification_is_batched_and_cached_per_session() -> None:
    collector = object.__new__(StreamCollector)
    collector.qualified_option_contracts = {}

    class FakeIB:
        RequestTimeout = 30.0

        def __init__(self) -> None:
            self.calls = 0

        def qualifyContracts(self, *contracts):
            self.calls += 1
            for index, contract in enumerate(contracts, start=1):
                contract.conId = 8000 + index
            return list(contracts)

    collector.ib = FakeIB()
    specs = (
        OptionContractSpec(expiry="20260708", strike=7500, right="C", lane="hot"),
        OptionContractSpec(expiry="20260708", strike=7500, right="P", lane="hot"),
    )

    first = collector._resolve_option_definitions(
        stream_collector_module.option_contracts_from_specs(specs)
    )
    second = collector._resolve_option_definitions(
        stream_collector_module.option_contracts_from_specs(specs)
    )

    assert collector.ib.calls == 1
    assert [contract.conId for _, _, contract in first] == [8001, 8002]
    assert [contract.conId for _, _, contract in second] == [8001, 8002]
    assert collector.ib.RequestTimeout == 30.0


def test_base_request_id_rejection_marks_session_for_reconnect() -> None:
    collector = object.__new__(StreamCollector)
    collector.errors = []
    collector.subscription_rejection_sequence = 0
    collector.subscription_rejection_log = []
    collector.subscription_rows_by_req_id = {}
    collector.subscription_lane_by_req_id = {}
    collector.subscription_health_failed = False
    collector.qualified_option_contracts = {}
    collector.farm_health = SimpleNamespace(observe=lambda *args: None)
    row = VerifyRow(
        label="index:SPX",
        kind="index",
        symbol="SPX",
        subscribed=True,
        request_id=17,
    )
    collector._register_subscription_rows(
        {"index:SPX": (SimpleNamespace(contract=object()), row)},
        lane="base",
    )

    collector._on_error(17, 354, "not subscribed", None)

    assert row.subscribed is False
    assert collector.subscription_health_failed is True


def test_teardown_clears_prior_session_errors(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.ib = SimpleNamespace(isConnected=lambda: False)
    collector.stream_settings = SimpleNamespace(replan_drift_points=20.0)
    collector.base_subs = {}
    collector.hot_subs = {}
    collector.rotation_subs = {}
    collector.spy_subs = {}
    collector.slow_active_subs = {}
    collector.errors = [
        stream_collector_module.IbkrError(
            req_id=-1,
            error_code=10197,
            message="competing session",
            contract=None,
            ts="2026-07-10T00:00:00+00:00",
        )
    ]
    discard_calls: list[dict[str, object]] = []
    patch_stream(monkeypatch, "discard_subscriptions", lambda _ib, subscriptions: discard_calls.append(subscriptions) or True,
    )

    def unexpected_cancel(*args) -> bool:
        raise AssertionError("server cancel while disconnected")

    patch_stream(monkeypatch, "cancel_subscriptions", unexpected_cancel)

    collector.teardown()

    assert collector.errors == []
    assert len(discard_calls) == 5


def test_base_rejection_during_subscription_is_reconciled_after_registration(
    monkeypatch,
) -> None:
    collector = object.__new__(StreamCollector)
    collector.ib = SimpleNamespace(sleep=lambda _seconds: None)
    collector.ibkr_settings = SimpleNamespace(
        qualify_contracts=False,
        quote_wait_seconds=0.0,
    )
    collector.stream_settings = SimpleNamespace(
        slow_poll_labels=(),
        slow_poll_chunk_size=6,
        slow_poll_interval_seconds=300.0,
        slow_poll_hold_seconds=10.0,
    )
    collector.errors = []
    collector.subscription_rejection_sequence = 0
    collector.subscription_rejection_log = []
    collector.subscription_rows_by_req_id = {}
    collector.subscription_lane_by_req_id = {}
    collector.subscription_health_failed = False
    collector.qualified_option_contracts = {}
    collector.farm_health = SimpleNamespace(observe=lambda *args: None)
    collector.slow_cache = {}
    collector.slow_qualified_contracts = {}
    collector.slow_unresolved_contracts = set()

    patch_stream(monkeypatch, "build_base_contracts", lambda _settings: [
            ("index:SPX", "index", object()),
            ("stock:SPY", "stock", object()),
        ],
    )

    def subscribe_with_early_rejection(*args, **kwargs):
        collector._on_error(77, 354, "not subscribed", None)
        return {
            "index:SPX": (
                SimpleNamespace(contract=object()),
                VerifyRow(
                    label="index:SPX",
                    kind="index",
                    symbol="SPX",
                    subscribed=True,
                    request_id=77,
                ),
            ),
            "stock:SPY": (
                SimpleNamespace(contract=object()),
                VerifyRow(
                    label="stock:SPY",
                    kind="stock",
                    symbol="SPY",
                    subscribed=True,
                    request_id=78,
                ),
            ),
        }

    patch_stream(monkeypatch, "qualify_and_subscribe", subscribe_with_early_rejection,
    )
    patch_stream(monkeypatch, "log_event", lambda *args: None)

    collector.subscribe_base()

    row = collector.base_subs["index:SPX"][1]
    assert row.subscribed is False
    assert "354" in (row.error or "")
    assert collector.subscription_health_failed is True


def test_base_subscription_rebuilds_if_connectivity_changes_during_setup(
    monkeypatch,
) -> None:
    collector = object.__new__(StreamCollector)
    collector.ib = SimpleNamespace(
        isConnected=lambda: True,
        sleep=lambda _seconds: None,
    )
    collector.ibkr_settings = SimpleNamespace(
        qualify_contracts=False,
        quote_wait_seconds=0.0,
    )
    collector.stream_settings = SimpleNamespace(
        slow_poll_labels=(),
        slow_poll_chunk_size=6,
        slow_poll_interval_seconds=300.0,
        slow_poll_hold_seconds=10.0,
    )
    collector.errors = []
    collector.subscription_rejection_sequence = 0
    collector.subscription_rejection_log = []
    collector.subscription_rows_by_req_id = {}
    collector.subscription_lane_by_req_id = {}
    collector.subscription_health_failed = False
    collector.tws_connectivity_lost = False
    collector.subscriptions_lost = False
    collector.tws_connectivity_loss_sequence = 0
    collector.qualified_option_contracts = {}
    collector.farm_health = SimpleNamespace(observe=lambda *args: None)
    collector.slow_cache = {}
    collector.slow_qualified_contracts = {}
    collector.slow_unresolved_contracts = set()

    patch_stream(monkeypatch, "build_base_contracts", lambda _settings: [("index:SPX", "index", object())],
    )

    def subscribe_across_outage(*args, **kwargs):
        collector._on_error(-1, 1100, "Connectivity between IBKR and TWS has been lost", None)
        collector._on_error(-1, 1102, "Connectivity restored; data maintained", None)
        return {
            "index:SPX": (
                SimpleNamespace(contract=object()),
                VerifyRow(
                    label="index:SPX",
                    kind="index",
                    symbol="SPX",
                    subscribed=True,
                    request_id=77,
                ),
            )
        }

    patch_stream(monkeypatch, "qualify_and_subscribe", subscribe_across_outage,
    )
    patch_stream(monkeypatch, "log_event", lambda *args: None)

    with pytest.raises(RuntimeError, match="base_subscribe"):
        collector.subscribe_base()

    assert collector.subscription_rows_by_req_id == {}
    assert collector.subscriptions_lost is True
    assert collector.subscription_health_failed is True


def test_tws_connectivity_error_state_pauses_resumes_or_rebuilds() -> None:
    collector = object.__new__(StreamCollector)
    collector.errors = []
    collector.subscription_rejection_sequence = 0
    collector.subscription_rejection_log = []
    collector.subscription_rows_by_req_id = {}
    collector.subscription_lane_by_req_id = {}
    collector.subscription_health_failed = False
    collector.tws_connectivity_lost = False
    collector.subscriptions_lost = False
    collector.farm_health = SimpleNamespace(observe=lambda *args: None)

    collector._on_error(-1, 1100, "Connectivity between IBKR and TWS has been lost", None)

    assert collector.tws_connectivity_lost is True
    assert collector.subscriptions_lost is False
    assert collector.subscription_health_failed is False

    collector._on_error(-1, 1102, "Connectivity restored; data maintained", None)

    assert collector.tws_connectivity_lost is False
    assert collector.subscriptions_lost is False
    assert collector.subscription_health_failed is False

    collector._on_error(-1, 2110, "TWS connectivity to server is broken", None)

    assert collector.tws_connectivity_lost is True

    collector._on_error(-1, 1102, "Connectivity restored; data maintained", None)
    collector._on_error(-1, 1101, "Connectivity restored; data lost", None)

    assert collector.tws_connectivity_lost is False
    assert collector.subscriptions_lost is True
    assert collector.subscription_health_failed is True

    collector._on_error(-1, 1102, "Connectivity restored; data maintained", None)

    assert collector.subscriptions_lost is True
    assert collector.subscription_health_failed is True


def test_decide_after_flush_priorities():
    assert (
        decide_after_flush(connected=True, allowed=True, competing_session=False)
        is StreamAction.CONTINUE
    )
    assert (
        decide_after_flush(connected=True, allowed=True, competing_session=True)
        is StreamAction.CONFLICT_WAIT
    )
    assert (
        decide_after_flush(
            connected=True,
            allowed=True,
            competing_session=False,
            gateway_restart=True,
        )
        is StreamAction.GATEWAY_RESTART
    )
    assert (
        decide_after_flush(connected=False, allowed=True, competing_session=False)
        is StreamAction.RECONNECT
    )
    assert (
        decide_after_flush(connected=True, allowed=False, competing_session=False)
        is StreamAction.POLICY_BLOCKED
    )
    # Competing session wins over disconnect: probe wait, not tight reconnect.
    assert (
        decide_after_flush(connected=False, allowed=True, competing_session=True)
        is StreamAction.CONFLICT_WAIT
    )


def test_reconnect_policy_backs_off_exponentially_and_resets():
    policy = ReconnectPolicy(min_seconds=5.0, max_seconds=60.0)

    assert policy.next_delay() == 5.0
    assert policy.next_delay() == 10.0
    assert policy.next_delay() == 20.0
    assert policy.next_delay() == 40.0
    assert policy.next_delay() == 60.0
    assert policy.next_delay() == 60.0

    policy.reset()
    assert policy.next_delay() == 5.0


def test_connect_backoff_is_honored_when_tcp_port_is_already_open(monkeypatch) -> None:
    sleeps: list[float] = []
    patch_stream(monkeypatch, "api_port_open", lambda *args: True)
    monkeypatch.setattr(stream_collector_module.time, "sleep", sleeps.append)

    sleep_until_reconnect(host="127.0.0.1", port=4002, delay_seconds=7.0)

    assert sleeps == [7.0]


def test_established_but_unhealthy_sessions_keep_reconnect_backoff(monkeypatch) -> None:
    class FakeIb:
        def sleep(self, _seconds: float) -> None:
            return None

        def isConnected(self) -> bool:  # noqa: N802 - mirrors ib_async
            return False

    class FakeCollector:
        def __init__(self) -> None:
            self.ib = FakeIb()
            self.ibkr_settings = SimpleNamespace(host="127.0.0.1", port=4002)
            self.subscription_health_failed = False
            self.opens = 0
            self.teardowns = 0

        def allowed(self) -> bool:
            return True

        def market_data_allowed(self) -> bool:
            return True

        def connection_required(self) -> bool:
            return True

        def open_session(self) -> None:
            self.opens += 1

        def subscribe_base(self) -> None:
            return None

        def flush(self) -> dict[str, object]:
            return {"event": "flush"}

        def flush_position_shadow_if_due(self, *, now_monotonic: float):
            del now_monotonic
            return None

        def drain_new_errors(self) -> list[object]:
            return []

        def teardown(self) -> None:
            self.teardowns += 1

    collector = FakeCollector()
    stream_settings = SimpleNamespace(
        reconnect_min_seconds=5.0,
        reconnect_max_seconds=60.0,
        flush_interval_seconds=0.0,
        auto_restart_gateway_on_farm_broken=False,
    )
    runtime = StreamRuntime(
        collector=collector,
        stream_settings=stream_settings,
        storage_settings=object(),
        runtime_policy=object(),
    )
    delays: list[float] = []

    def stop_after_two_delays(seconds: float) -> None:
        delays.append(seconds)
        if len(delays) == 2:
            runtime.deadline = 0.0

    runtime.sleep = stop_after_two_delays
    patch_stream(monkeypatch, "persist_state_only", lambda *args: None)
    patch_stream(monkeypatch, "log_event", lambda *args: None)
    patch_stream(monkeypatch, "probe_data_plane", lambda *args: SimpleNamespace(ok=True, to_log_event=lambda: {}),
    )

    assert runtime.run() == 0
    assert delays == [5.0, 10.0]
    assert collector.opens == 2
    assert collector.teardowns == 2


def _flush_test_collector(*, connected: bool = True) -> StreamCollector:
    collector = object.__new__(StreamCollector)
    collector.ib = SimpleNamespace(isConnected=lambda: connected, sleep=lambda _s: None)
    collector.ibkr_settings = SimpleNamespace(
        stale_after_seconds=10.0,
        slow_index_stale_after_seconds=60.0,
        slow_index_labels=(),
    )
    collector.stream_settings = SimpleNamespace(
        freeze_quotes_on_connectivity_loss=True,
        replan_drift_points=20.0,
    )
    collector.storage_settings = object()
    collector.base_subs = {}
    collector.hot_subs = {}
    collector.rotation_subs = {}
    collector.spy_subs = {}
    collector.slow_cache = {}
    collector.option_cache = {}
    collector.option_plan = None
    collector.errors = []
    collector.tws_connectivity_lost = False
    collector.subscriptions_lost = False
    collector.subscription_health_failed = False
    collector.rotation_index = 0
    collector.connection_generation = 1
    collector.farm_health = stream_collector_module.FarmHealthTracker()
    collector._advance_subscription_lifecycle = lambda *_args, **_kwargs: None
    return collector


def test_teardown_clears_option_cache(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.ib = SimpleNamespace(isConnected=lambda: False)
    collector.stream_settings = SimpleNamespace(replan_drift_points=20.0)
    collector.base_subs = {}
    collector.hot_subs = {}
    collector.rotation_subs = {}
    collector.spy_subs = {}
    collector.slow_active_subs = {}
    collector.errors = []
    collector.subscription_rejection_sequence = 0
    collector.subscription_rejection_log = []
    collector.subscription_rows_by_req_id = {}
    collector.subscription_lane_by_req_id = {}
    collector.subscription_health_failed = False
    collector.tws_connectivity_lost = False
    collector.subscriptions_lost = False
    collector.slow_cache = {}
    collector.slow_contracts = []
    collector.slow_chunks = []
    collector.slow_scheduler = None
    collector.slow_qualified_contracts = {}
    collector.slow_unresolved_contracts = set()
    collector.qualified_option_contracts = {}
    collector.option_plan = None
    collector.spy_plan_key = None
    collector.spy_retry_at = 0.0
    collector.rotation_index = 0
    collector.rotation_retry_at = 0.0
    old = VerifyRow(
        label="option:SPXW:20260711:6900:C",
        kind="option",
        symbol="SPXW",
        subscribed=True,
        last=12.0,
    )
    collector.option_cache = {
        old.label: (100.0, old),
        "option:SPXW:20260711:6900:P": (100.0, replace_row(old, "option:SPXW:20260711:6900:P")),
    }
    patch_stream(monkeypatch, "discard_subscriptions", lambda *_args, **_kwargs: True,
    )
    patch_stream(monkeypatch, "cancel_subscriptions", lambda *_args, **_kwargs: True,
    )

    collector.teardown()

    assert collector.option_cache == {}


def replace_row(row: VerifyRow, label: str) -> VerifyRow:
    return VerifyRow(
        label=label,
        kind=row.kind,
        symbol=row.symbol,
        subscribed=row.subscribed,
        last=row.last,
    )


def test_first_flush_after_reconnect_excludes_pre_disconnect_option_rows(monkeypatch) -> None:
    collector = _flush_test_collector()
    old_label = "option:SPXW:20260711:6900:C"
    collector.option_cache = {
        old_label: (
            0.0,
            VerifyRow(
                label=old_label,
                kind="option",
                symbol="SPXW",
                subscribed=True,
                market_data_type=1,
                last=11.5,
                ticker_time="2026-07-11T10:00:00+00:00",
                stale=False,
            ),
        )
    }
    # Simulate teardown clearing the pre-disconnect cache before the next session.
    collector.option_cache = {}
    collector.base_subs = {
        "index:SPX": (
            object(),
            VerifyRow(
                label="index:SPX",
                kind="index",
                symbol="SPX",
                subscribed=True,
                market_data_type=1,
                last=6901.0,
                ticker_time="2026-07-11T14:00:00+00:00",
            ),
        )
    }
    snapshots: list[object] = []

    def capture_snapshot(snapshot, _storage):
        snapshots.append(snapshot)
        return SimpleNamespace(best_quote_count=len(snapshot.quotes))

    patch_stream(monkeypatch, "persist_provider_snapshot", capture_snapshot)
    patch_stream(monkeypatch, "snapshot_rows", lambda subscriptions, *_args, **_kwargs: [row for _, row in subscriptions.values()],
    )

    collector.flush()

    assert snapshots
    labels = {quote.provider_symbol for quote in snapshots[0].quotes}
    assert old_label not in labels
    assert "index:SPX" in labels


def test_flush_skips_quote_persistence_when_socket_disconnected(monkeypatch) -> None:
    collector = _flush_test_collector(connected=False)
    state_calls: list[object] = []
    snapshot_calls: list[object] = []

    def capture_state(state, _storage):
        state_calls.append(state)

    def capture_snapshot(snapshot, _storage):
        snapshot_calls.append(snapshot)
        return SimpleNamespace(best_quote_count=0)

    patch_stream(monkeypatch, "persist_state_only", capture_state)
    patch_stream(monkeypatch, "persist_provider_snapshot", capture_snapshot)

    event = collector.flush()

    assert event["quotes"] == 0
    assert len(state_calls) == 1
    assert state_calls[0].status.value == "unavailable"
    assert "disconnected" in (state_calls[0].reason or "").lower()
    assert snapshot_calls == []


def test_flush_marks_all_rows_stale_during_tws_connectivity_loss(monkeypatch) -> None:
    collector = _flush_test_collector()
    collector.errors = []
    collector.farm_health = stream_collector_module.FarmHealthTracker()
    collector.subscription_rows_by_req_id = {}
    collector.subscription_lane_by_req_id = {}
    collector.subscription_rejection_sequence = 0
    collector.subscription_rejection_log = []
    collector._on_error = StreamCollector._on_error.__get__(collector, StreamCollector)
    collector._on_error(-1, 1100, "Connectivity between IB and TWS has been lost", None)
    assert collector.tws_connectivity_lost is True

    collector.base_subs = {
        "index:SPX": (
            object(),
            VerifyRow(
                label="index:SPX",
                kind="index",
                symbol="SPX",
                subscribed=True,
                market_data_type=1,
                last=6900.0,
                ticker_time="2026-07-11T14:00:00+00:00",
                stale=False,
            ),
        )
    }
    snapshots: list[object] = []

    def capture_snapshot(snapshot, _storage):
        snapshots.append(snapshot)
        return SimpleNamespace(best_quote_count=len(snapshot.quotes))

    patch_stream(monkeypatch, "persist_provider_snapshot", capture_snapshot)
    patch_stream(monkeypatch, "snapshot_rows", lambda subscriptions, *_args, **_kwargs: [row for _, row in subscriptions.values()],
    )

    collector.flush()

    assert snapshots
    assert all(quote.quality.value == "stale" for quote in snapshots[0].quotes)


def test_session_loop_reconnect_path_persists_unavailable(monkeypatch) -> None:
    class FakeIb:
        def sleep(self, _seconds: float) -> None:
            return None

        def isConnected(self) -> bool:  # noqa: N802
            return False

    class FakeCollector:
        def __init__(self) -> None:
            self.ib = FakeIb()
            self.subscription_health_failed = False
            self.tws_connectivity_lost = False

        def flush(self) -> dict[str, object]:
            return {"event": "flush", "quotes": 0}

        def flush_position_shadow_if_due(self, *, now_monotonic: float):
            del now_monotonic
            return None

        def drain_new_errors(self) -> list[object]:
            return []

        def market_data_allowed(self) -> bool:
            return True

    persisted: list[object] = []
    patch_stream(monkeypatch, "persist_state_only", lambda state, _storage: persisted.append(state),
    )
    patch_stream(monkeypatch, "log_event", lambda _event: None)
    patch_stream(monkeypatch, "has_competing_session_error", lambda _errors: False,
    )

    runtime = StreamRuntime(
        collector=FakeCollector(),  # type: ignore[arg-type]
        stream_settings=SimpleNamespace(
            reconnect_min_seconds=1.0,
            reconnect_max_seconds=2.0,
            flush_interval_seconds=0.0,
            auto_restart_gateway_on_farm_broken=False,
        ),
        storage_settings=object(),
        runtime_policy=object(),
    )

    assert runtime.session_loop() is True
    assert len(persisted) == 1
    assert "disconnected" in (persisted[0].reason or "").lower()


def test_quote_to_dict_round_trips_source_session() -> None:
    from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote, quote_from_dict

    now = datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc)
    with_session = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        last=6900.0,
        source_session="ibkr-stream:3",
    )
    without_session = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        last=6900.0,
    )

    round_trip = quote_from_dict(with_session.to_dict())
    legacy = quote_from_dict(without_session.to_dict())

    assert "source_session" in with_session.to_dict()
    assert "source_session" not in without_session.to_dict()
    assert round_trip.source_session == "ibkr-stream:3"
    assert legacy.source_session is None


def test_flush_stamps_connection_generation_on_quotes(monkeypatch) -> None:
    collector = _flush_test_collector()
    collector.connection_generation = 4
    collector.base_subs = {
        "index:SPX": (
            object(),
            VerifyRow(
                label="index:SPX",
                kind="index",
                symbol="SPX",
                subscribed=True,
                market_data_type=1,
                last=6900.0,
                ticker_time="2026-07-11T14:00:00+00:00",
            ),
        )
    }
    snapshots: list[object] = []

    def capture_snapshot(snapshot, _storage):
        snapshots.append(snapshot)
        return SimpleNamespace(best_quote_count=len(snapshot.quotes))

    patch_stream(monkeypatch, "persist_provider_snapshot", capture_snapshot)
    patch_stream(monkeypatch, "snapshot_rows", lambda subscriptions, *_args, **_kwargs: [row for _, row in subscriptions.values()],
    )

    collector.flush()
    assert snapshots
    assert all(quote.source_session == "ibkr-stream:4" for quote in snapshots[0].quotes)

    opens = 0

    def fake_connect(*_args, **_kwargs):
        nonlocal opens
        opens += 1

    collector.broker_settings = SimpleNamespace(account_read_enabled=False)
    collector.ibkr_settings = SimpleNamespace(market_data_type=1)
    collector.stream_settings = SimpleNamespace(
        client_id=172,
        freeze_quotes_on_connectivity_loss=True,
    )
    collector.ib = SimpleNamespace(
        isConnected=lambda: True,
        reqMarketDataType=lambda *_args: None,
    )
    patch_stream(monkeypatch, "connect_market_data_only", fake_connect)
    patch_stream(monkeypatch, "log_event", lambda _event: None)
    patch_stream(monkeypatch, "replace_client_id", lambda settings, client_id: settings,
    )
    collector.market_data_allowed = lambda: True
    before = collector.connection_generation
    collector.open_session()
    collector.open_session()
    assert collector.connection_generation == before + 2
    assert opens == 2


def test_close_only_live_row_downgrades_to_unknown_quality() -> None:
    from spx_spark.ibkr.adapter import quote_from_ibkr_row
    from spx_spark.marketdata import MarketDataQuality

    row = VerifyRow(
        label="index:SPX",
        kind="index",
        symbol="SPX",
        subscribed=True,
        market_data_type=1,
        close=6900.0,
        last=None,
        bid=None,
        ask=None,
        ticker_time=None,
    )
    quote = quote_from_ibkr_row(row, received_at=datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc))
    assert quote.quality is MarketDataQuality.UNKNOWN


def test_flush_reports_outage_while_farm_not_ready(monkeypatch) -> None:
    collector = _flush_test_collector()
    collector.farm_health.observe(2119, "Market data farm is connecting:usfarm.nj", now=1.0)
    collector.base_subs = {
        "index:SPX": (
            object(),
            VerifyRow(
                label="index:SPX",
                kind="index",
                symbol="SPX",
                subscribed=True,
                market_data_type=1,
                last=6900.0,
                ticker_time="2026-07-11T14:00:00+00:00",
            ),
        )
    }
    snapshots: list[object] = []

    def capture_snapshot(snapshot, _storage):
        snapshots.append(snapshot)
        return SimpleNamespace(best_quote_count=len(snapshot.quotes))

    patch_stream(monkeypatch, "persist_provider_snapshot", capture_snapshot)
    patch_stream(monkeypatch, "snapshot_rows", lambda subscriptions, *_args, **_kwargs: [row for _, row in subscriptions.values()],
    )
    patch_stream(monkeypatch, "log_event", lambda _event: None)

    event = collector.flush()

    assert snapshots
    state = snapshots[0].provider_state
    assert state is not None
    assert state.status.value == "degraded"
    assert "farm" in (state.reason or "").lower()
    assert event["provider_status"] == "degraded"
