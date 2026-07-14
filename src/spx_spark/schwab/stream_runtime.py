"""Supervise the Schwab WebSocket inside the single OAuth token-owner process."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from schwab.streaming import StreamClient

from spx_spark.config import SchwabStreamSettings, StorageSettings
from spx_spark.market_calendar import ET
from spx_spark.provider_adapter import ProviderSnapshot, persist_provider_snapshot
from spx_spark.schwab.gateway import SchwabSessionManager
from spx_spark.schwab.collector_state import collector_state_path, load_collector_budget_state
from spx_spark.schwab.adapter import option_instrument_from_schwab_symbol
from spx_spark.schwab.stream_collector import SchwabStreamQuoteAssembler
from spx_spark.schwab.symbols import (
    find_schwab_instrument,
    resolved_schwab_canonical_quote_symbols,
)


PersistSnapshot = Callable[[ProviderSnapshot, StorageSettings], object]
StreamClientFactory = Callable[..., Any]
SymbolResolver = Callable[[tuple[str, ...]], tuple[list[str], list[str]]]
OptionSymbolResolver = Callable[[], list[str]]


class SchwabStreamTelemetry:
    """Thread-safe, redacted evidence that accepted subscriptions produce data."""

    def __init__(self, *, mode: str) -> None:
        self.mode = mode
        self._lock = threading.Lock()
        self._connected = False
        self._connected_at: datetime | None = None
        self._option_subscription_accepted_at: datetime | None = None
        self._subscribed_option_count = 0
        self._message_counts: Counter[str] = Counter()
        self._row_counts: Counter[str] = Counter()
        self._last_message_at: dict[str, datetime] = {}
        self._normalized_quote_counts: Counter[str] = Counter()
        self._live_quote_counts: Counter[str] = Counter()
        self._last_source_at: dict[str, datetime] = {}
        self._retained_symbol_counts: dict[str, int] = {}
        self._evicted_option_symbol_count = 0
        self._reconnect_count = 0
        self._last_error_type: str | None = None

    def connected(self) -> None:
        now = datetime.now(tz=timezone.utc)
        with self._lock:
            self._connected = True
            self._connected_at = now
            self._last_error_type = None

    def disconnected(self) -> None:
        with self._lock:
            self._connected = False

    def option_subscription_accepted(self, count: int) -> None:
        now = datetime.now(tz=timezone.utc)
        with self._lock:
            self._subscribed_option_count = count
            self._option_subscription_accepted_at = now

    def message(self, service: str, rows: int) -> None:
        now = datetime.now(tz=timezone.utc)
        with self._lock:
            self._message_counts[service] += 1
            self._row_counts[service] += rows
            self._last_message_at[service] = now

    def reconnect(self, error_type: str) -> None:
        with self._lock:
            self._connected = False
            self._reconnect_count += 1
            self._last_error_type = error_type

    def snapshot(self, snapshot: ProviderSnapshot) -> None:
        with self._lock:
            for quote in snapshot.quotes:
                service = str(quote.raw.get("service") or "UNKNOWN").upper()
                self._normalized_quote_counts[service] += 1
                if quote.quality.value == "live":
                    self._live_quote_counts[service] += 1
                source_at = quote.quote_time or quote.trade_time
                if source_at is not None:
                    previous = self._last_source_at.get(service)
                    if previous is None or source_at > previous:
                        self._last_source_at[service] = source_at

    def cache(self, counts: dict[str, int], *, evicted_options: int = 0) -> None:
        with self._lock:
            self._retained_symbol_counts = dict(counts)
            self._evicted_option_symbol_count += evicted_options

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return {
                "mode": self.mode,
                "connected": self._connected,
                "connected_at": _iso(self._connected_at),
                "subscribed_option_count": self._subscribed_option_count,
                "option_subscription_accepted_at": _iso(
                    self._option_subscription_accepted_at
                ),
                "message_counts": dict(self._message_counts),
                "row_counts": dict(self._row_counts),
                "normalized_quote_counts": dict(self._normalized_quote_counts),
                "live_quote_counts": dict(self._live_quote_counts),
                "last_message_at": {
                    service: observed_at.isoformat()
                    for service, observed_at in self._last_message_at.items()
                },
                "last_source_at": {
                    service: observed_at.isoformat()
                    for service, observed_at in self._last_source_at.items()
                },
                "retained_symbol_counts": dict(self._retained_symbol_counts),
                "evicted_option_symbol_count": self._evicted_option_symbol_count,
                "reconnect_count": self._reconnect_count,
                "last_error_type": self._last_error_type,
            }


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
        option_symbol_resolver: OptionSymbolResolver | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.manager = manager
        self.settings = settings
        self.storage_settings = stream_storage_settings(storage_settings, settings)
        self.stream_client_factory = stream_client_factory
        self.persist_snapshot = persist_snapshot
        self.symbol_resolver = symbol_resolver or stream_symbols
        self.option_symbol_resolver = option_symbol_resolver or (
            lambda: stream_option_symbols(
                storage_settings,
                limit=settings.option_hot_symbol_limit,
                max_plan_age_seconds=settings.option_plan_max_age_seconds,
            )
        )
        self.monotonic = monotonic
        self._stop = threading.Event()
        self.telemetry = SchwabStreamTelemetry(mode=settings.mode)

    def health(self) -> dict[str, Any]:
        return self.telemetry.to_dict()

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
                self.telemetry.reconnect(type(exc).__name__)
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
            def handler(message: dict[str, Any]) -> int:
                return self._ingest(assembler, message)

            stream.add_level_one_equity_handler(handler)
            stream.add_level_one_futures_handler(handler)
            stream.add_level_one_option_handler(handler)
            await stream.login(
                {"open_timeout": self.settings.websocket_open_timeout_seconds}
            )
            equities, futures = self.symbol_resolver(self.settings.canonical_symbols)
            options = self.option_symbol_resolver()
            if equities:
                await stream.level_one_equity_subs(equities)
            if futures:
                await stream.level_one_futures_subs(futures)
            if options:
                await stream.level_one_option_subs(options)
                self.telemetry.option_subscription_accepted(len(options))
            assembler.retain_option_symbols(options)
            self.telemetry.cache(assembler.retained_symbol_counts())
            if not equities and not futures and not options:
                raise ValueError(
                    "Schwab streaming symbol list resolved to no supported instruments"
                )
            self.telemetry.connected()
            print(
                json.dumps(
                    {
                        "event": "schwab_stream_connected",
                        "ok": True,
                        "mode": self.settings.mode,
                        "equity_symbols": len(equities),
                        "future_symbols": len(futures),
                        "option_symbols": len(options),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            listener = asyncio.create_task(self._listen(stream))
            clock_now = self.monotonic()
            next_symbol_refresh_at = clock_now + self.settings.symbol_refresh_interval_seconds
            next_option_refresh_at = (
                clock_now + self.settings.option_symbol_refresh_seconds
            )
            while not self._stop.is_set():
                await asyncio.sleep(self.settings.flush_interval_seconds)
                if listener.done():
                    listener.result()
                clock_now = self.monotonic()
                if clock_now >= next_symbol_refresh_at:
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
                if clock_now >= next_option_refresh_at:
                    refreshed_options = self.option_symbol_resolver()
                    if option_subscription_changed(options, refreshed_options):
                        old_set = set(options)
                        refreshed_set = set(refreshed_options)
                        if refreshed_options:
                            await stream.level_one_option_subs(refreshed_options)
                        elif options:
                            await stream.level_one_option_unsubs(options)
                        self.telemetry.option_subscription_accepted(len(refreshed_options))
                        evicted = assembler.retain_option_symbols(refreshed_options)
                        self.telemetry.cache(
                            assembler.retained_symbol_counts(),
                            evicted_options=evicted,
                        )
                        print(
                            json.dumps(
                                {
                                    "event": "schwab_stream_option_symbols_changed",
                                    "ok": True,
                                    "old_option_symbols": len(options),
                                    "new_option_symbols": len(refreshed_options),
                                    "added_option_symbols": len(refreshed_set - old_set),
                                    "removed_option_symbols": len(old_set - refreshed_set),
                                },
                                sort_keys=True,
                            ),
                            flush=True,
                        )
                        options = refreshed_options
                    next_option_refresh_at = (
                        self.monotonic() + self.settings.option_symbol_refresh_seconds
                    )
                snapshot = assembler.drain_snapshot()
                if snapshot is not None:
                    self.telemetry.snapshot(snapshot)
                    self.telemetry.cache(assembler.retained_symbol_counts())
                    self.persist_snapshot(snapshot, self.storage_settings)
        finally:
            self.telemetry.disconnected()
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

    def _ingest(
        self,
        assembler: SchwabStreamQuoteAssembler,
        message: dict[str, Any],
    ) -> int:
        accepted = assembler.ingest(message)
        service = str(message.get("service") or "UNKNOWN").upper()
        self.telemetry.message(service, accepted)
        return accepted

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


def option_subscription_changed(current: list[str], refreshed: list[str]) -> bool:
    """Subscription order is irrelevant; only membership changes require SUBS."""

    return set(current) != set(refreshed)


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


def stream_option_symbols(
    storage_settings: StorageSettings,
    *,
    limit: int,
    max_plan_age_seconds: float,
    now: datetime | None = None,
) -> list[str]:
    """Read the collector's current hot plan without coupling the two runtimes."""

    observed_at = now or datetime.now(tz=timezone.utc)
    state = load_collector_budget_state(collector_state_path(storage_settings))
    planned_at = state.chain_last_fetched_at.get("SPX:front")
    if planned_at is None:
        return []
    if (observed_at - planned_at).total_seconds() > max_plan_age_seconds:
        return []
    symbols: list[str] = []
    for raw_symbol in state.hot_symbols:
        symbol = raw_symbol.strip().upper()
        instrument = option_instrument_from_schwab_symbol(symbol)
        if instrument is None or instrument.expiry != state.hot_expiry:
            continue
        symbols.append(symbol)
        if len(symbols) >= limit:
            break
    return symbols


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


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None
