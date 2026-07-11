import asyncio
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.config import SchwabStreamSettings, StorageSettings
from spx_spark.schwab.stream_runtime import (
    SchwabStreamRuntime,
    stream_storage_settings,
    stream_symbols,
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
    )


def test_shadow_mode_uses_separate_latest_state(tmp_path: Path) -> None:
    base = storage(tmp_path)
    resolved = stream_storage_settings(base, stream_settings(tmp_path, mode="shadow"))

    assert resolved.latest_state_path.endswith("schwab-stream-shadow.json")
    assert resolved.data_root == base.data_root


def test_live_mode_uses_production_latest_state(tmp_path: Path) -> None:
    base = storage(tmp_path)

    assert stream_storage_settings(base, stream_settings(tmp_path, mode="live")) is base


def test_stream_symbols_split_equity_and_concrete_futures() -> None:
    equities, futures = stream_symbols(("SPX", "SPY", "RSP", "ES", "MES"))

    assert equities == ["$SPX", "SPY", "RSP"]
    assert len(futures) == 2
    assert futures[0].startswith("/ES")
    assert futures[1].startswith("/MES")


def test_runtime_flushes_stream_message_to_shadow_storage(tmp_path: Path) -> None:
    persisted = []

    class FakeManager:
        def client_for_streaming(self):
            return object()

    class FakeStream:
        def __init__(self, client, *, enforce_enums):
            del client, enforce_enums
            self.equity_handler = None
            self.future_handler = None
            self.equities = []
            self.futures = []
            self.emitted = False

        def add_level_one_equity_handler(self, handler):
            self.equity_handler = handler

        def add_level_one_futures_handler(self, handler):
            self.future_handler = handler

        async def login(self, args):
            assert args["open_timeout"] > 0

        async def level_one_equity_subs(self, symbols):
            self.equities = symbols

        async def level_one_futures_subs(self, symbols):
            self.futures = symbols

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
    )

    asyncio.run(runtime._run_session())

    assert len(persisted) == 1
    snapshot, resolved_storage = persisted[0]
    assert snapshot.quotes[0].instrument.canonical_id == "index:SPX"
    assert resolved_storage.latest_state_path.endswith("shadow.json")


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

        async def login(self, args):
            assert args["open_timeout"] > 0

        async def level_one_equity_subs(self, symbols):
            assert symbols == ["$SPX"]

        async def level_one_futures_subs(self, symbols):
            subscribed_futures.append(list(symbols))
            if symbols == ["/ESZ26"]:
                runtime.close()

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
