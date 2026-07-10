from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace

import spx_spark.ibkr.stream_collector as stream_collector_module
from spx_spark.config import (
    DEFAULT_SLOW_POLL_LABELS,
    IbkrSettings,
    IbkrStreamSettings,
    RuntimePolicySettings,
    SamplingSettings,
    StorageSettings,
    parse_hhmm,
)
from spx_spark.ibkr.stream_collector import (
    StreamCollector,
    chunked,
    merge_slow_rows,
    split_base_contracts,
)
from spx_spark.ibkr.slow_poll import SlowPollScheduler
from spx_spark.ibkr.verifier import VerifyRow


def test_split_base_contracts_partitions_by_label() -> None:
    contracts = [
        ("index:SPX", "index", object()),
        ("index:VIX", "index", object()),
        ("stock:SPY", "stock", object()),
        ("stock:QQQ", "stock", object()),
        ("future:ES", "future", object()),
    ]

    persistent, slow = split_base_contracts(contracts, DEFAULT_SLOW_POLL_LABELS)

    assert [label for label, _, _ in persistent] == ["index:SPX", "stock:SPY", "future:ES"]
    assert [label for label, _, _ in slow] == ["index:VIX", "stock:QQQ"]


def test_chunked_sizes() -> None:
    items = list(range(19))

    assert chunked(items, 6) == [list(range(0, 6)), list(range(6, 12)), list(range(12, 18)), [18]]
    assert chunked(items, 0) == [[index] for index in items]


def test_stream_settings_slow_poll_env(monkeypatch) -> None:
    monkeypatch.delenv("IBKR_STREAM_SLOW_POLL_LABELS", raising=False)
    settings = IbkrStreamSettings.from_env()
    assert len(settings.slow_poll_labels) == 19
    assert settings.slow_poll_labels == DEFAULT_SLOW_POLL_LABELS

    monkeypatch.setenv("IBKR_STREAM_SLOW_POLL_LABELS", "")
    settings = IbkrStreamSettings.from_env()
    assert settings.slow_poll_labels == ()


def test_flush_merges_slow_cache_rows() -> None:
    cached = VerifyRow(label="index:VIX", kind="index", symbol="VIX")
    subscribed = VerifyRow(label="index:SPX", kind="index", symbol="SPX")

    without_overlap = merge_slow_rows([subscribed], {"index:VIX": cached}, {"index:SPX"})
    assert subscribed in without_overlap
    assert cached in without_overlap

    with_overlap = merge_slow_rows([subscribed], {"index:SPX": cached}, {"index:SPX"})
    assert with_overlap == [subscribed]


@dataclass
class FakeEvent:
    handlers: list[object] = field(default_factory=list)

    def __iadd__(self, handler: object) -> FakeEvent:
        self.handlers.append(handler)
        return self


@dataclass
class FakeIB:
    sleep_calls: list[float] = field(default_factory=list)
    errorEvent: FakeEvent = field(default_factory=FakeEvent)

    def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)

    def isConnected(self) -> bool:
        return True


def make_stream_collector(
    *,
    slow_contracts: list[tuple[str, str, object]],
    slow_poll_chunk_size: int = 6,
) -> StreamCollector:
    ibkr_settings = IbkrSettings(
        host="127.0.0.1",
        port=4002,
        client_id=171,
        market_data_type=1,
        es_expiry="202609",
        mes_expiry="202609",
        verify_indexes=["SPX"],
        verify_stocks=["SPY"],
        verify_futures=["ES"],
        option_expiry="20260707",
        option_strike_window_points=50,
        option_strike_step=5,
        max_option_lines=40,
        quote_wait_seconds=8.0,
        stale_after_seconds=10.0,
        qualify_contracts=False,
        request_timeout_seconds=30.0,
    )
    stream_settings = IbkrStreamSettings(
        client_id=172,
        flush_interval_seconds=5.0,
        policy_check_seconds=30.0,
        replan_drift_points=10.0,
        max_option_lines=60,
        hot_lane_share=0.7,
        reconnect_min_seconds=5.0,
        reconnect_max_seconds=300.0,
        skip_options=True,
        farm_broken_restart_seconds=180.0,
        gateway_restart_cooldown_seconds=120.0,
        auto_restart_gateway_on_farm_broken=True,
        slow_poll_labels=DEFAULT_SLOW_POLL_LABELS,
        slow_poll_chunk_size=slow_poll_chunk_size,
    )
    sampling_settings = SamplingSettings(
        strike_step=5,
        window_points=200,
        hot_window_points=50,
        group_count=4,
        group_interval_seconds=4,
        degraded_group_count=20,
        degraded_group_interval_seconds=3,
        group_strategy="interleaved",
        hot_human_cadence_seconds=8,
        hot_execution_cadence_seconds=2,
        include_next_expiry=False,
        default_mode="human_alert",
    )
    storage_settings = StorageSettings(
        data_root="data",
        latest_state_path="data/latest/state.json",
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=900.0,
        slow_index_labels=frozenset(),
    )
    runtime_policy = RuntimePolicySettings(
        ibkr_schedule_enabled=False,
        ibkr_schedule_timezone="Asia/Shanghai",
        ibkr_schedule_start=parse_hhmm("00:00"),
        ibkr_schedule_stop=parse_hhmm("00:00"),
        ibkr_connect_retry_seconds=60,
        ibkr_conflict_retry_minutes=0,
        ibkr_conflict_probe_seconds=60,
        ibkr_fallback_provider="schwab",
        strict_no_session_fight=True,
        weekend_maintenance_mode=True,
        runtime_mode_path="runtime/mode.json",
        agent_override_default_ttl_minutes=120,
    )
    collector = StreamCollector(
        FakeIB(),
        ibkr_settings=ibkr_settings,
        stream_settings=stream_settings,
        sampling_settings=sampling_settings,
        storage_settings=storage_settings,
        runtime_policy=runtime_policy,
        skip_options=True,
    )
    collector.slow_contracts = slow_contracts
    return collector


def test_slow_poll_is_cooperative_and_eventually_covers_all_chunks(monkeypatch) -> None:
    slow_contracts = [(f"index:VIX{index}", "index", object()) for index in range(7)]
    collector = make_stream_collector(slow_contracts=slow_contracts, slow_poll_chunk_size=3)
    cancel_calls: list[dict[str, tuple[object, VerifyRow]]] = []
    qualify_calls: list[list[tuple[str, str, object]]] = []

    def fake_qualify_and_subscribe(
        ib: object,
        contracts: list[tuple[str, str, object]],
        *,
        qualify: bool = False,
        on_progress: object | None = None,
    ) -> dict[str, tuple[object, VerifyRow]]:
        qualify_calls.append(contracts)
        return {
            label: (
                object(),
                VerifyRow(label=label, kind=kind, symbol=label, subscribed=True),
            )
            for label, kind, _ in contracts
        }

    def fake_snapshot_rows(
        subscriptions: dict[str, tuple[object, VerifyRow]],
        stale_after_seconds: float,
        *,
        slow_index_stale_after_seconds: float | None = None,
        slow_index_labels: frozenset[str] | None = None,
    ) -> list[VerifyRow]:
        return [row for _, row in subscriptions.values()]

    def fake_cancel_subscriptions(
        ib: object,
        subscriptions: dict[str, tuple[object, VerifyRow]],
    ) -> None:
        cancel_calls.append(subscriptions)

    monkeypatch.setattr(stream_collector_module, "qualify_and_subscribe", fake_qualify_and_subscribe)
    monkeypatch.setattr(stream_collector_module, "snapshot_rows", fake_snapshot_rows)
    monkeypatch.setattr(stream_collector_module, "cancel_subscriptions", fake_cancel_subscriptions)

    collector.slow_chunks = chunked(slow_contracts, 3)
    collector.slow_scheduler = SlowPollScheduler(
        chunk_count=3,
        cycle_seconds=300.0,
        hold_seconds=10.0,
    )
    collector.slow_scheduler.reset(now=0.0)
    collector.slow_qualified_contracts = {
        label: (label, kind, contract)
        for label, kind, contract in slow_contracts
    }

    for started_at in (0.0, 100.0, 200.0):
        collector.advance_slow_poll(now_monotonic=started_at)
        assert len(cancel_calls) == len(qualify_calls) - 1
        collector.advance_slow_poll(now_monotonic=started_at + 5.0)
        assert len(cancel_calls) == len(qualify_calls) - 1
        collector.advance_slow_poll(now_monotonic=started_at + 10.0)

    assert len(collector.slow_cache) == 7
    assert len(qualify_calls) == 3
    assert len(cancel_calls) == 3
    assert collector.ib.sleep_calls == []


def test_slow_poll_hold_starts_after_qualification_completes(monkeypatch) -> None:
    contract = SimpleNamespace(conId=0)
    chunk = [("index:VIX", "index", contract)]
    collector = make_stream_collector(slow_contracts=chunk, slow_poll_chunk_size=1)
    collector.slow_chunks = [chunk]
    collector.slow_scheduler = SlowPollScheduler(
        chunk_count=1,
        cycle_seconds=60.0,
        hold_seconds=10.0,
    )
    collector.slow_scheduler.reset(now=0.0)
    clock = {"now": 0.0}

    def resolve_after_five_seconds(_chunk):
        clock["now"] = 5.0
        return [("index:VIX", "index", SimpleNamespace(conId=1001))]

    collector._resolve_slow_definitions = resolve_after_five_seconds
    monkeypatch.setattr(stream_collector_module.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(
        stream_collector_module,
        "qualify_and_subscribe",
        lambda ib, contracts, **kwargs: {
            "index:VIX": (
                SimpleNamespace(contract=contracts[0][2]),
                VerifyRow(
                    label="index:VIX",
                    kind="index",
                    symbol="VIX",
                    subscribed=True,
                ),
            )
        },
    )
    monkeypatch.setattr(stream_collector_module, "log_event", lambda *args: None)

    collector.advance_slow_poll()

    assert collector.slow_scheduler.hold_deadline == 15.0


def test_slow_poll_start_due_only_when_idle_and_ready() -> None:
    contract = object()
    collector = make_stream_collector(
        slow_contracts=[("index:VIX", "index", contract)],
        slow_poll_chunk_size=1,
    )
    collector.slow_chunks = [[("index:VIX", "index", contract)]]
    collector.slow_scheduler = None
    assert not collector.slow_poll_start_due(now_monotonic=10.0)

    collector.slow_scheduler = SlowPollScheduler(
        chunk_count=1,
        cycle_seconds=60.0,
        hold_seconds=10.0,
    )
    collector.slow_scheduler.reset(now=10.0)

    assert collector.slow_poll_start_due(now_monotonic=10.0)

    collector.slow_scheduler.next_start_at = None
    assert collector.slow_poll_start_due(now_monotonic=10.0)

    collector.slow_scheduler.next_start_at = 20.0
    assert not collector.slow_poll_start_due(now_monotonic=19.99)
    assert collector.slow_poll_start_due(now_monotonic=20.0)

    collector.slow_scheduler.active_chunk_index = 0
    assert not collector.slow_poll_start_due(now_monotonic=30.0)

    collector.slow_scheduler.active_chunk_index = None
    collector.slow_chunks = []
    assert not collector.slow_poll_start_due(now_monotonic=30.0)


def test_slow_contract_qualification_is_reused_within_session(monkeypatch) -> None:
    original_contract = object()
    resolved_contract = object()
    slow_contracts = [("index:VIX", "index", original_contract)]
    collector = make_stream_collector(slow_contracts=slow_contracts, slow_poll_chunk_size=1)
    seen_contracts: list[object] = []

    class FakeTicker:
        contract = resolved_contract

    def fake_qualify_and_subscribe(
        ib: object,
        contracts: list[tuple[str, str, object]],
        *,
        qualify: bool = False,
        on_progress: object | None = None,
    ) -> dict[str, tuple[object, VerifyRow]]:
        label, kind, contract = contracts[0]
        seen_contracts.append(contract)
        return {
            label: (
                FakeTicker(),
                VerifyRow(label=label, kind=kind, symbol=label, subscribed=True),
            )
        }

    monkeypatch.setattr(stream_collector_module, "qualify_and_subscribe", fake_qualify_and_subscribe)
    monkeypatch.setattr(
        stream_collector_module,
        "snapshot_rows",
        lambda subscriptions, stale_after_seconds, **kwargs: [
            row for _, row in subscriptions.values()
        ],
    )
    monkeypatch.setattr(stream_collector_module, "cancel_subscriptions", lambda *args: None)
    collector.slow_chunks = [slow_contracts]
    collector.slow_scheduler = SlowPollScheduler(
        chunk_count=1,
        cycle_seconds=60.0,
        hold_seconds=10.0,
    )
    collector.slow_scheduler.reset(now=0.0)
    collector.slow_qualified_contracts = {
        "index:VIX": ("index:VIX", "index", original_contract)
    }

    collector.advance_slow_poll(now_monotonic=0.0)
    collector.advance_slow_poll(now_monotonic=10.0)
    collector.advance_slow_poll(now_monotonic=60.0)

    assert seen_contracts == [original_contract, resolved_contract]


def test_slow_contracts_are_batch_qualified_before_scheduler() -> None:
    collector = make_stream_collector(slow_contracts=[])
    input_contracts = [
        SimpleNamespace(
            secType="STK",
            symbol=symbol,
            lastTradeDateOrContractMonth="",
            strike=0.0,
            right="",
            multiplier="",
            currency="USD",
            conId=0,
        )
        for symbol in ("QQQ", "IWM")
    ]
    qualified_contracts = [
        SimpleNamespace(**(vars(contract) | {"conId": 1000 + index}))
        for index, contract in enumerate(input_contracts)
    ]
    calls: list[tuple[object, ...]] = []

    def qualify_contracts(*contracts):
        calls.append(contracts)
        return qualified_contracts

    collector.ib.qualifyContracts = qualify_contracts
    collector.slow_contracts = [
        (f"stock:{contract.symbol}", "stock", contract)
        for contract in input_contracts
    ]

    collector._qualify_slow_contracts()

    assert len(calls) == 1
    assert len(calls[0]) == 2
    assert [
        collector.slow_qualified_contracts[label][2].conId
        for label in ("stock:QQQ", "stock:IWM")
    ] == [1000, 1001]
    assert collector.slow_unresolved_contracts == set()


def test_slow_async_rejection_retries_chunk_without_reconnecting_hot_lane(
    monkeypatch,
) -> None:
    contract = SimpleNamespace(conId=1001)
    slow_contracts = [("index:VIX", "index", contract)]
    collector = make_stream_collector(slow_contracts=slow_contracts, slow_poll_chunk_size=1)
    canceled: list[set[str]] = []

    def fake_qualify(ib, contracts, *, qualify=False, on_progress=None):
        return {
            label: (
                SimpleNamespace(contract=item),
                VerifyRow(
                    label=label,
                    kind=kind,
                    symbol="VIX",
                    subscribed=True,
                    request_id=77,
                ),
            )
            for label, kind, item in contracts
        }

    def fake_cancel(ib, subscriptions):
        canceled.append(set(subscriptions))

    monkeypatch.setattr(stream_collector_module, "qualify_and_subscribe", fake_qualify)
    monkeypatch.setattr(stream_collector_module, "cancel_subscriptions", fake_cancel)
    collector.slow_chunks = [slow_contracts]
    collector.slow_qualified_contracts = {
        "index:VIX": ("index:VIX", "index", contract)
    }
    collector.slow_scheduler = SlowPollScheduler(
        chunk_count=1,
        cycle_seconds=60.0,
        hold_seconds=10.0,
    )
    collector.slow_scheduler.reset(now=0.0)

    collector.advance_slow_poll(now_monotonic=0.0)
    collector._on_error(77, 354, "not subscribed", None)
    collector.advance_slow_poll(now_monotonic=10.0)

    assert collector.subscription_health_failed is False
    assert collector.slow_active_subs == {}
    assert canceled == [{"index:VIX"}]
    assert collector.slow_scheduler.next_start_at == 70.0


def test_unresolvable_slow_label_does_not_starve_later_chunks(monkeypatch) -> None:
    bad = ("index:BAD", "index", object())
    good_contract = SimpleNamespace(conId=2002)
    good = ("index:VIX", "index", good_contract)
    collector = make_stream_collector(slow_contracts=[bad, good], slow_poll_chunk_size=1)

    monkeypatch.setattr(
        stream_collector_module,
        "qualify_and_subscribe",
        lambda ib, contracts, qualify=False: {
            label: (
                SimpleNamespace(contract=contract),
                VerifyRow(label=label, kind=kind, symbol=label, subscribed=True),
            )
            for label, kind, contract in contracts
        },
    )
    monkeypatch.setattr(
        stream_collector_module,
        "snapshot_rows",
        lambda subscriptions, stale_after_seconds, **kwargs: [
            row for _, row in subscriptions.values()
        ],
    )
    monkeypatch.setattr(stream_collector_module, "cancel_subscriptions", lambda *args: None)
    collector.slow_chunks = [[bad], [good]]
    collector.slow_qualified_contracts = {"index:VIX": good}
    collector.slow_scheduler = SlowPollScheduler(
        chunk_count=2,
        cycle_seconds=100.0,
        hold_seconds=10.0,
    )
    collector.slow_scheduler.reset(now=0.0)

    collector.advance_slow_poll(now_monotonic=0.0)
    collector.advance_slow_poll(now_monotonic=50.0)
    collector.advance_slow_poll(now_monotonic=60.0)

    assert "index:VIX" in collector.slow_cache
    assert collector.slow_scheduler.next_chunk_index == 0
