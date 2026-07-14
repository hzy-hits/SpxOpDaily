import asyncio
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.config import SchwabStreamSettings, StorageSettings
from spx_spark.schwab.stream_runtime import (
    SchwabStreamRuntime,
    option_subscription_changed,
    stream_option_symbols,
    stream_storage_settings,
    stream_symbols,
)
from spx_spark.schwab.collector_state import (
    CollectorBudgetState,
    collector_state_path,
    save_collector_budget_state,
)


def storage(tmp_path: Path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path),
        latest_state_path=str(tmp_path / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=900.0,
        slow_index_labels=frozenset(),
        delayed_stale_after_seconds=60.0,
        provider_priority=("schwab", "ibkr"),
    )


def stream_settings(tmp_path: Path, *, mode: str) -> SchwabStreamSettings:
    return SchwabStreamSettings(
        mode=mode,
        canonical_symbols=("SPX", "SPY", "RSP", "ES", "MES"),
        flush_interval_seconds=1.0,
        symbol_refresh_interval_seconds=300.0,
        reconnect_min_seconds=2.0,
        reconnect_max_seconds=60.0,
        websocket_open_timeout_seconds=10.0,
        shadow_latest_path=str(tmp_path / "latest" / "schwab-stream-shadow.json"),
        option_hot_symbol_limit=64,
        option_symbol_refresh_seconds=5.0,
        option_plan_max_age_seconds=120.0,
    )


def test_shadow_mode_uses_separate_latest_state(tmp_path: Path) -> None:
    base = storage(tmp_path)
    resolved = stream_storage_settings(base, stream_settings(tmp_path, mode="shadow"))

    assert resolved.latest_state_path.endswith("schwab-stream-shadow.json")
    assert resolved.data_root == base.data_root


def test_live_mode_uses_production_latest_state(tmp_path: Path) -> None:
    base = storage(tmp_path)

    assert stream_storage_settings(base, stream_settings(tmp_path, mode="live")) is base


def test_option_subscription_membership_ignores_order() -> None:
    current = ["SPXW  260713C07500000", "SPXW  260713P07500000"]

    assert not option_subscription_changed(current, list(reversed(current)))
    assert option_subscription_changed(current, [current[0]])


def test_stream_symbols_split_equity_and_concrete_futures() -> None:
    equities, futures = stream_symbols(("SPX", "SPY", "RSP", "ES", "MES"))

    assert equities == ["$SPX", "SPY", "RSP"]
    assert len(futures) == 2
    assert futures[0].startswith("/ES")
    assert futures[1].startswith("/MES")


def test_runtime_flushes_stream_message_to_shadow_storage(tmp_path: Path) -> None:
    persisted = []
    subscribed_options: list[list[str]] = []

    class FakeManager:
        def client_for_streaming(self):
            return object()

    class FakeStream:
        def __init__(self, client, *, enforce_enums):
            del client, enforce_enums
            self.equity_handler = None
            self.future_handler = None
            self.option_handler = None
            self.equities = []
            self.futures = []
            self.emitted = False

        def add_level_one_equity_handler(self, handler):
            self.equity_handler = handler

        def add_level_one_futures_handler(self, handler):
            self.future_handler = handler

        def add_level_one_option_handler(self, handler):
            self.option_handler = handler

        async def login(self, args):
            assert args["open_timeout"] > 0

        async def level_one_equity_subs(self, symbols):
            self.equities = symbols

        async def level_one_futures_subs(self, symbols):
            self.futures = symbols

        async def level_one_option_subs(self, symbols):
            subscribed_options.append(list(symbols))

        async def level_one_option_unsubs(self, symbols):
            del symbols

        async def handle_message(self):
            if not self.emitted:
                self.emitted = True
                now = datetime.now(tz=timezone.utc)
                self.equity_handler(
                    {
                        "service": "LEVELONE_EQUITIES",
                        "content": [
                            {
                                "key": "$SPX",
                                "MARK": 7500.0,
                                "QUOTE_TIME_MILLIS": int(now.timestamp() * 1000),
                            }
                        ],
                    }
                )
            await asyncio.sleep(10)

        async def logout(self):
            return None

    cfg = SchwabStreamSettings(
        mode="shadow",
        canonical_symbols=("SPX", "ES"),
        flush_interval_seconds=0.01,
        symbol_refresh_interval_seconds=300.0,
        reconnect_min_seconds=0.01,
        reconnect_max_seconds=0.02,
        websocket_open_timeout_seconds=1.0,
        shadow_latest_path=str(tmp_path / "latest" / "shadow.json"),
        option_hot_symbol_limit=64,
        option_symbol_refresh_seconds=5.0,
        option_plan_max_age_seconds=120.0,
    )
    runtime = None

    def persist(snapshot, storage_settings):
        persisted.append((snapshot, storage_settings))
        assert runtime is not None
        runtime.close()

    runtime = SchwabStreamRuntime(
        FakeManager(),  # type: ignore[arg-type]
        cfg,
        storage(tmp_path),
        stream_client_factory=FakeStream,
        persist_snapshot=persist,
        option_symbol_resolver=lambda: ["SPXW  260713C07500000"],
    )

    asyncio.run(runtime._run_session())

    assert len(persisted) == 1
    snapshot, resolved_storage = persisted[0]
    assert snapshot.quotes[0].instrument.canonical_id == "index:SPX"
    assert resolved_storage.latest_state_path.endswith("shadow.json")
    assert subscribed_options == [["SPXW  260713C07500000"]]
    health = runtime.health()
    assert health["subscribed_option_count"] == 1
    assert health["option_subscription_accepted_at"] is not None
    assert health["message_counts"] == {"LEVELONE_EQUITIES": 1}
    assert health["row_counts"] == {"LEVELONE_EQUITIES": 1}
    assert health["normalized_quote_counts"] == {"LEVELONE_EQUITIES": 1}
    assert health["live_quote_counts"] == {"LEVELONE_EQUITIES": 1}
    assert health["last_source_at"]["LEVELONE_EQUITIES"] is not None


def test_runtime_tracks_controlled_futures_and_single_option_probe(tmp_path: Path) -> None:
    persisted = []
    future_subscriptions: list[list[str]] = []
    futures_option_subscriptions: list[list[str]] = []

    class FakeManager:
        def client_for_streaming(self):
            return object()

    class FakeStream:
        def __init__(self, client, *, enforce_enums):
            del client, enforce_enums
            self.future_handler = None
            self.futures_option_handler = None
            self.emitted = False

        def add_level_one_equity_handler(self, handler):
            del handler

        def add_level_one_futures_handler(self, handler):
            self.future_handler = handler

        def add_level_one_option_handler(self, handler):
            del handler

        def add_level_one_futures_options_handler(self, handler):
            self.futures_option_handler = handler

        async def login(self, args):
            assert args["open_timeout"] > 0

        async def level_one_equity_subs(self, symbols):
            assert symbols == ["$SPX"]

        async def level_one_futures_subs(self, symbols):
            future_subscriptions.append(list(symbols))

        async def level_one_option_subs(self, symbols):
            del symbols

        async def level_one_option_unsubs(self, symbols):
            del symbols

        async def level_one_futures_options_subs(self, symbols):
            futures_option_subscriptions.append(list(symbols))

        async def handle_message(self):
            if not self.emitted:
                self.emitted = True
                now = datetime.now(tz=timezone.utc)
                rows = [
                    {
                        "key": symbol,
                        "BID_PRICE": 100.0,
                        "ASK_PRICE": 100.25,
                        "QUOTE_TIME_MILLIS": int(now.timestamp() * 1000),
                    }
                    for symbol in ("/NQU26", "/RTYU26", "/YMU26")
                ]
                self.future_handler({"service": "LEVELONE_FUTURES", "content": rows})
                self.futures_option_handler(
                    {
                        "service": "LEVELONE_FUTURES_OPTIONS",
                        "content": [
                            {
                                "key": "./ESU26C7600",
                                "BID_PRICE": 31.0,
                                "ASK_PRICE": 31.5,
                                "UNDERLYING_SYMBOL": "/ESU26",
                                "STRIKE_PRICE": 7600.0,
                                "FUTURE_EXPIRATION_DATE": int(
                                    datetime(2026, 9, 18, tzinfo=timezone.utc).timestamp()
                                    * 1000
                                ),
                                "CONTRACT_TYPE": "C",
                                "QUOTE_TIME_MILLIS": int(now.timestamp() * 1000),
                            }
                        ],
                    }
                )
            await asyncio.sleep(10)

        async def logout(self):
            return None

    def resolve(symbols):
        if symbols == ("NQ", "RTY", "YM"):
            return [], ["/NQU26", "/RTYU26", "/YMU26"]
        return ["$SPX"], ["/ESU26", "/NQU26", "/RTYU26", "/YMU26"]

    cfg = SchwabStreamSettings(
        mode="shadow",
        canonical_symbols=("SPX", "ES"),
        flush_interval_seconds=0.01,
        symbol_refresh_interval_seconds=300.0,
        reconnect_min_seconds=0.01,
        reconnect_max_seconds=0.02,
        websocket_open_timeout_seconds=1.0,
        shadow_latest_path=str(tmp_path / "latest" / "shadow.json"),
        option_hot_symbol_limit=64,
        option_symbol_refresh_seconds=5.0,
        option_plan_max_age_seconds=120.0,
        validation_future_symbols=("NQ", "RTY", "YM"),
        futures_option_probe_symbol="./ESU26C7600",
    )
    runtime = None

    def persist(snapshot, storage_settings):
        del storage_settings
        persisted.append(snapshot)
        assert runtime is not None
        runtime.close()

    runtime = SchwabStreamRuntime(
        FakeManager(),  # type: ignore[arg-type]
        cfg,
        storage(tmp_path),
        stream_client_factory=FakeStream,
        persist_snapshot=persist,
        symbol_resolver=resolve,
        option_symbol_resolver=lambda: [],
    )

    asyncio.run(runtime._run_session())

    assert future_subscriptions == [["/ESU26", "/NQU26", "/RTYU26", "/YMU26"]]
    assert futures_option_subscriptions == [["./ESU26C7600"]]
    assert len(persisted) == 1
    health = runtime.health()
    assert health["subscribed_futures_option_count"] == 1
    assert set(health["validation"]) == {
        "/NQU26",
        "/RTYU26",
        "/YMU26",
        "./ESU26C7600",
    }
    assert all(item["status"] == "live" for item in health["validation"].values())
    by_symbol = {
        quote.provider_symbol: quote for quote in persisted[0].quotes
    }
    assert by_symbol["/NQU26"].sampling_mode == "schwab_stream_validation"
    assert (
        by_symbol["./ESU26C7600"].sampling_mode
        == "schwab_stream_futures_option_probe"
    )


def test_runtime_reconnects_and_resubscribes_when_future_contract_changes(
    tmp_path: Path,
    capsys,
) -> None:
    subscribed_futures: list[list[str]] = []
    streams = []
    resolver_calls = 0
    monotonic_values = iter((0.0, 301.0, 301.0))

    class FakeManager:
        def client_for_streaming(self):
            return object()

    class FakeStream:
        def __init__(self, client, *, enforce_enums):
            del client, enforce_enums
            self.logged_out = False
            streams.append(self)

        def add_level_one_equity_handler(self, handler):
            del handler

        def add_level_one_futures_handler(self, handler):
            del handler

        def add_level_one_option_handler(self, handler):
            del handler

        async def login(self, args):
            assert args["open_timeout"] > 0

        async def level_one_equity_subs(self, symbols):
            assert symbols == ["$SPX"]

        async def level_one_futures_subs(self, symbols):
            subscribed_futures.append(list(symbols))
            if symbols == ["/ESZ26"]:
                runtime.close()

        async def level_one_option_subs(self, symbols):
            del symbols

        async def level_one_option_unsubs(self, symbols):
            del symbols

        async def handle_message(self):
            await asyncio.sleep(10)

        async def logout(self):
            self.logged_out = True

    def resolve_symbols(canonical_symbols):
        nonlocal resolver_calls
        assert canonical_symbols == ("SPX", "ES")
        resolver_calls += 1
        if resolver_calls == 1:
            return ["$SPX"], ["/ESU26"]
        return ["$SPX"], ["/ESZ26"]

    cfg = SchwabStreamSettings(
        mode="shadow",
        canonical_symbols=("SPX", "ES"),
        flush_interval_seconds=0.01,
        symbol_refresh_interval_seconds=300.0,
        reconnect_min_seconds=0.01,
        reconnect_max_seconds=0.02,
        websocket_open_timeout_seconds=1.0,
        shadow_latest_path=str(tmp_path / "latest" / "shadow.json"),
        option_hot_symbol_limit=64,
        option_symbol_refresh_seconds=5.0,
        option_plan_max_age_seconds=120.0,
    )
    runtime = SchwabStreamRuntime(
        FakeManager(),  # type: ignore[arg-type]
        cfg,
        storage(tmp_path),
        stream_client_factory=FakeStream,
        symbol_resolver=resolve_symbols,
        monotonic=lambda: next(monotonic_values),
    )

    asyncio.run(runtime._supervise())

    assert subscribed_futures == [["/ESU26"], ["/ESZ26"]]
    assert resolver_calls == 3
    assert len(streams) == 2
    assert all(stream.logged_out for stream in streams)
    assert '"event": "schwab_stream_symbols_changed"' in capsys.readouterr().out


def test_stream_option_symbols_uses_only_fresh_matching_hot_plan(tmp_path: Path) -> None:
    cfg = storage(tmp_path)
    now = datetime(2026, 7, 13, 5, 0, tzinfo=timezone.utc)
    state = CollectorBudgetState(
        chain_last_fetched_at={"SPX:front": now},
        hot_symbols=[
            "SPXW  260713C07500000",
            "SPXW  260713P07500000",
            "SPXW  260714C07500000",
            "SPY",
        ],
        hot_expiry="20260713",
    )
    save_collector_budget_state(collector_state_path(cfg), state)

    symbols = stream_option_symbols(
        cfg,
        limit=1,
        max_plan_age_seconds=120.0,
        now=now,
    )

    assert symbols == ["SPXW  260713C07500000"]
    assert stream_option_symbols(
        cfg,
        limit=64,
        max_plan_age_seconds=120.0,
        now=now.replace(minute=3),
    ) == []
