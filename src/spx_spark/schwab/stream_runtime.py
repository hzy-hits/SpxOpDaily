"""Supervise the Schwab WebSocket inside the single OAuth token-owner process."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from typing import Any

from schwab.streaming import StreamClient

from spx_spark.config import SchwabStreamSettings, StorageSettings
from spx_spark.market_calendar import ET
from spx_spark.provider_adapter import ProviderSnapshot, persist_provider_snapshot
from spx_spark.schwab.gateway import SchwabSessionManager
from spx_spark.schwab.stream_collector import SchwabStreamQuoteAssembler
from spx_spark.schwab.symbols import (
    find_schwab_instrument,
    resolved_schwab_canonical_quote_symbols,
)


PersistSnapshot = Callable[[ProviderSnapshot, StorageSettings], object]
StreamClientFactory = Callable[..., Any]
SymbolResolver = Callable[[tuple[str, ...]], tuple[list[str], list[str]]]


class SchwabStreamRuntime:
    def __init__(
        self,
        manager: SchwabSessionManager,
        settings: SchwabStreamSettings,
        storage_settings: StorageSettings,
        *,
        stream_client_factory: StreamClientFactory = StreamClient,
        persist_snapshot: PersistSnapshot = persist_provider_snapshot,
        symbol_resolver: SymbolResolver | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.manager = manager
        self.settings = settings
        self.storage_settings = stream_storage_settings(storage_settings, settings)
        self.stream_client_factory = stream_client_factory
        self.persist_snapshot = persist_snapshot
        self.symbol_resolver = symbol_resolver or stream_symbols
        self.monotonic = monotonic
        self._stop = threading.Event()

    def run_forever(self) -> None:
        if self.settings.mode == "off":
            return
        asyncio.run(self._supervise())

    def close(self) -> None:
        self._stop.set()

    async def _supervise(self) -> None:
        delay = self.settings.reconnect_min_seconds
        while not self._stop.is_set():
            try:
                await self._run_session()
                delay = self.settings.reconnect_min_seconds
            except Exception as exc:  # noqa: BLE001 - never log provider/token details
                print(
                    json.dumps(
                        {
                            "event": "schwab_stream_reconnect",
                            "ok": False,
                            "error_type": type(exc).__name__,
                            "retry_in_seconds": delay,
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                await self._wait_for_stop(delay)
                delay = min(self.settings.reconnect_max_seconds, delay * 2)

    async def _run_session(self) -> None:
        client = self.manager.client_for_streaming()
        stream = self.stream_client_factory(client, enforce_enums=False)
        assembler = SchwabStreamQuoteAssembler()
        listener: asyncio.Task[None] | None = None
        try:
            stream.add_level_one_equity_handler(assembler.ingest)
            stream.add_level_one_futures_handler(assembler.ingest)
            await stream.login(
                {"open_timeout": self.settings.websocket_open_timeout_seconds}
            )
            equities, futures = self.symbol_resolver(self.settings.canonical_symbols)
            if equities:
                await stream.level_one_equity_subs(equities)
            if futures:
                await stream.level_one_futures_subs(futures)
            if not equities and not futures:
                raise ValueError(
                    "Schwab streaming symbol list resolved to no supported instruments"
                )
            print(
                json.dumps(
                    {
                        "event": "schwab_stream_connected",
                        "ok": True,
                        "mode": self.settings.mode,
                        "equity_symbols": len(equities),
                        "future_symbols": len(futures),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            listener = asyncio.create_task(self._listen(stream))
            next_symbol_refresh_at = (
                self.monotonic() + self.settings.symbol_refresh_interval_seconds
            )
            while not self._stop.is_set():
                await asyncio.sleep(self.settings.flush_interval_seconds)
                if listener.done():
                    listener.result()
                if self.monotonic() >= next_symbol_refresh_at:
                    refreshed_equities, refreshed_futures = self.symbol_resolver(
                        self.settings.canonical_symbols
                    )
                    if (refreshed_equities, refreshed_futures) != (equities, futures):
                        print(
                            json.dumps(
                                {
                                    "event": "schwab_stream_symbols_changed",
                                    "ok": True,
                                    "old_equity_symbols": len(equities),
                                    "new_equity_symbols": len(refreshed_equities),
                                    "old_future_symbols": len(futures),
                                    "new_future_symbols": len(refreshed_futures),
                                },
                                sort_keys=True,
                            ),
                            flush=True,
                        )
                        return
                    next_symbol_refresh_at = (
                        self.monotonic()
                        + self.settings.symbol_refresh_interval_seconds
                    )
                snapshot = assembler.drain_snapshot()
                if snapshot is not None:
                    self.persist_snapshot(snapshot, self.storage_settings)
        finally:
            if listener is not None:
                listener.cancel()
                try:
                    await listener
                except asyncio.CancelledError:
                    pass
                except Exception:  # noqa: BLE001 - original error drives reconnect
                    pass
            await close_stream_client(
                stream,
                timeout_seconds=self.settings.websocket_open_timeout_seconds,
            )

    async def _listen(self, stream: Any) -> None:
        while not self._stop.is_set():
            await stream.handle_message()

    async def _wait_for_stop(self, seconds: float) -> None:
        remaining = seconds
        while remaining > 0 and not self._stop.is_set():
            interval = min(remaining, self.settings.flush_interval_seconds)
            await asyncio.sleep(interval)
            remaining -= interval


def stream_storage_settings(
    storage_settings: StorageSettings,
    stream_settings: SchwabStreamSettings,
) -> StorageSettings:
    if stream_settings.mode == "live":
        return storage_settings
    return replace(storage_settings, latest_state_path=stream_settings.shadow_latest_path)


def stream_symbols(canonical_symbols: tuple[str, ...]) -> tuple[list[str], list[str]]:
    resolved = resolved_schwab_canonical_quote_symbols(
        canonical_symbols,
        now=datetime.now(tz=ET),
    )
    equities: list[str] = []
    futures: list[str] = []
    for symbol in resolved:
        instrument = find_schwab_instrument(symbol)
        if instrument is None:
            continue
        if instrument.instrument_type == "future":
            futures.append(symbol)
        elif instrument.instrument_type in {"index", "equity"}:
            equities.append(symbol)
    return equities, futures


async def close_stream_client(stream: Any, *, timeout_seconds: float) -> None:
    try:
        await asyncio.wait_for(stream.logout(), timeout=timeout_seconds)
    except Exception:  # noqa: BLE001 - best-effort close after disconnect
        socket = getattr(stream, "_socket", None)
        close = getattr(socket, "close", None)
        if callable(close):
            try:
                result = close()
                if asyncio.iscoroutine(result):
                    await asyncio.wait_for(result, timeout=timeout_seconds)
            except Exception:  # noqa: BLE001 - process shutdown is the final boundary
                pass
