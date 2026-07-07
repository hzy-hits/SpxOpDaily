from __future__ import annotations

from dataclasses import dataclass, field

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


def test_poll_slow_contracts_caches_and_cancels(monkeypatch) -> None:
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
            label: (object(), VerifyRow(label=label, kind=kind, symbol=label))
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

    before = collector.last_slow_poll
    collector.poll_slow_contracts()

    assert len(collector.slow_cache) == 7
    assert len(qualify_calls) == 3
    assert len(cancel_calls) == 3
    assert collector.last_slow_poll > before
    assert collector.ib.sleep_calls == [10.0, 10.0, 10.0]
