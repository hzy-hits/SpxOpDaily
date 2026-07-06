from __future__ import annotations

import fcntl
import json
from collections import defaultdict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    MarketDataQuality,
    Provider,
    ProviderState,
    Quote,
    as_utc,
    choose_best_quote,
    parse_timestamp,
    provider_state_from_dict,
    quote_from_dict,
)


@dataclass(frozen=True)
class RawWriteResult:
    row_count: int
    path_counts: dict[str, int]

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(sorted(self.path_counts))


@dataclass(frozen=True)
class LatestState:
    created_at: datetime
    as_of: datetime
    quotes: tuple[Quote, ...]
    best_quotes: tuple[Quote, ...]
    provider_states: tuple[ProviderState, ...] = ()

    def best_quote(self, instrument_id: str) -> Quote | None:
        for quote in self.best_quotes:
            if quote.instrument.canonical_id == instrument_id:
                return quote
        return None

    def to_dict(self) -> dict[str, object]:
        return {
            "created_at": self.created_at.isoformat(),
            "as_of": self.as_of.isoformat(),
            "quotes": [quote.to_dict() for quote in self.quotes],
            "best_quotes": [quote.to_dict() for quote in self.best_quotes],
            "provider_states": [state.to_dict() for state in self.provider_states],
        }


@dataclass(frozen=True)
class LatestUpdateResult:
    path: str
    provider_quote_count: int
    best_quote_count: int
    updated_quote_count: int


class JsonlQuoteWriter:
    def __init__(self, settings: StorageSettings) -> None:
        self.settings = settings
        self.data_root = Path(settings.data_root)

    def write_quotes(self, quotes: Iterable[Quote]) -> RawWriteResult:
        path_rows: dict[Path, list[Quote]] = defaultdict(list)
        for quote in quotes:
            path_rows[self.raw_quote_path(quote)].append(quote)

        path_counts: dict[str, int] = {}
        for path, rows in path_rows.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                for quote in rows:
                    handle.write(
                        json.dumps(
                            quote.to_dict(include_raw=self.settings.include_raw_payload),
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                    )
                    handle.write("\n")
            path_counts[str(path)] = len(rows)

        return RawWriteResult(
            row_count=sum(path_counts.values()),
            path_counts=path_counts,
        )

    def raw_quote_path(self, quote: Quote) -> Path:
        timestamp = as_utc(quote.received_at)
        date_part = timestamp.strftime("%Y-%m-%d")
        hour_part = timestamp.strftime("%H")
        return (
            self.data_root
            / "raw"
            / f"provider={quote.provider.value}"
            / f"date={date_part}"
            / f"hour={hour_part}"
            / self.settings.raw_file_name
        )


class LatestStateStore:
    def __init__(self, settings: StorageSettings) -> None:
        self.settings = settings
        self.path = Path(settings.latest_state_path)
        self.lock_path = self.path.with_name(self.path.name + ".lock")

    @contextmanager
    def exclusive_lock(self) -> Iterator[None]:
        """Serialize read-modify-write cycles across processes.

        update() is load -> merge -> write. The tmp+rename write is atomic on
        its own, but two concurrent updaters (24h loop, manual collector,
        stream collector) would each merge against the same base state and the
        second rename would silently drop the first writer's quotes.
        """
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def load(self, *, now: datetime | None = None, refresh_quality: bool = True) -> LatestState:
        now = as_utc(now or datetime.now(tz=timezone.utc))
        if not self.path.exists():
            return LatestState(created_at=now, as_of=now, quotes=(), best_quotes=())

        payload = json.loads(self.path.read_text(encoding="utf-8"))
        quotes_payload = payload.get("quotes") if isinstance(payload, dict) else []
        best_payload = payload.get("best_quotes") if isinstance(payload, dict) else []
        provider_states_payload = payload.get("provider_states") if isinstance(payload, dict) else []
        quotes = tuple(
            quote_from_dict(item) for item in quotes_payload if isinstance(item, dict)
        )
        best_quotes = tuple(
            quote_from_dict(item) for item in best_payload if isinstance(item, dict)
        )
        provider_states = tuple(
            provider_state_from_dict(item)
            for item in provider_states_payload
            if isinstance(item, dict)
        )
        created_at = as_utc_from_payload(payload.get("created_at")) if isinstance(payload, dict) else now
        as_of = now if refresh_quality else (
            as_utc_from_payload(payload.get("as_of")) if isinstance(payload, dict) else now
        )
        if refresh_quality:
            quotes = tuple(
                degrade_stale_quote(
                    quote,
                    as_of=as_of,
                    stale_after_seconds=self.settings.latest_stale_after_seconds,
                )
                for quote in quotes
            )
            best_quotes = select_best_quotes(quotes, as_of=as_of)
        return LatestState(
            created_at=created_at,
            as_of=as_of,
            quotes=quotes,
            best_quotes=best_quotes,
            provider_states=provider_states,
        )

    def update(
        self,
        quotes: Iterable[Quote],
        *,
        now: datetime | None = None,
        provider_states: Iterable[ProviderState] = (),
        replace_providers: Iterable[Provider] = (),
    ) -> LatestUpdateResult:
        now = as_utc(now or datetime.now(tz=timezone.utc))
        incoming = tuple(quotes)
        with self.exclusive_lock():
            existing_state = self.load(now=now)
            replacement_providers = set(replace_providers)
            existing_quotes = tuple(
                quote for quote in existing_state.quotes if quote.provider not in replacement_providers
            )
            provider_latest = latest_by_provider(existing_quotes + incoming)
            provider_states_latest = latest_provider_states(
                existing_state.provider_states + tuple(provider_states)
            )
            aged_quotes = tuple(
                degrade_stale_quote(
                    quote,
                    as_of=now,
                    stale_after_seconds=self.settings.latest_stale_after_seconds,
                )
                for quote in provider_latest
            )
            best_quotes = select_best_quotes(aged_quotes, as_of=now)
            state = LatestState(
                created_at=datetime.now(tz=timezone.utc),
                as_of=now,
                quotes=tuple(sorted(aged_quotes, key=quote_sort_key)),
                best_quotes=tuple(
                    sorted(best_quotes, key=lambda quote: quote.instrument.canonical_id)
                ),
                provider_states=provider_states_latest,
            )
            self.write(state)
        return LatestUpdateResult(
            path=str(self.path),
            provider_quote_count=len(state.quotes),
            best_quote_count=len(state.best_quotes),
            updated_quote_count=len(incoming),
        )

    def write(self, state: LatestState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        temp_path.write_text(json.dumps(state.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        temp_path.replace(self.path)


def latest_by_provider(quotes: Iterable[Quote]) -> tuple[Quote, ...]:
    result: dict[tuple[str, str], Quote] = {}
    for quote in quotes:
        key = (quote.instrument.canonical_id, quote.provider.value)
        previous = result.get(key)
        if previous is None or as_utc(quote.received_at) >= as_utc(previous.received_at):
            result[key] = quote
    return tuple(result.values())


def latest_provider_states(states: Iterable[ProviderState]) -> tuple[ProviderState, ...]:
    result: dict[Provider, ProviderState] = {}
    for state in states:
        previous = result.get(state.provider)
        if previous is None or as_utc(state.checked_at) >= as_utc(previous.checked_at):
            result[state.provider] = state
    return tuple(sorted(result.values(), key=lambda item: item.provider.value))


def select_best_quotes(quotes: Iterable[Quote], *, as_of: datetime | None = None) -> tuple[Quote, ...]:
    grouped: dict[str, list[Quote]] = defaultdict(list)
    for quote in quotes:
        grouped[quote.instrument.canonical_id].append(quote)

    best: list[Quote] = []
    for instrument_id in sorted(grouped):
        quote = choose_best_quote(grouped[instrument_id], as_of=as_of)
        if quote is not None:
            best.append(quote)
    return tuple(best)


def degrade_stale_quote(quote: Quote, *, as_of: datetime, stale_after_seconds: float) -> Quote:
    if quote.quality not in {MarketDataQuality.LIVE, MarketDataQuality.FROZEN}:
        return quote
    age_ms = quote.quote_age_ms(as_of)
    if age_ms is None:
        return quote
    if age_ms <= stale_after_seconds * 1000.0:
        return quote
    return replace(quote, quality=MarketDataQuality.STALE)


def quote_sort_key(quote: Quote) -> tuple[str, str]:
    return (quote.instrument.canonical_id, quote.provider.value)


def as_utc_from_payload(value: object) -> datetime:
    return parse_timestamp(value) or datetime.now(tz=timezone.utc)
