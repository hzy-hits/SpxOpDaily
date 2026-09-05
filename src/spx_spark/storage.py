from __future__ import annotations

import fcntl
import json
import logging
import os
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path

from spx_spark.config import StorageSettings, current_storage_settings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.market_data_policy import pricing_candidates, pricing_provider_priority
from spx_spark.marketdata import (
    DEFAULT_PROVIDER_PRIORITY,
    InstrumentType,
    MarketDataQuality,
    Provider,
    ProviderState,
    Quote,
    QuoteFreshness,
    QuoteUseDecision,
    as_utc,
    choose_best_quote,
    instrument_matches_id,
    parse_timestamp,
    provider_state_from_dict,
    quality_from_market_data_type,
    quote_from_dict,
    quote_use_decision,
)

LOGGER = logging.getLogger(__name__)

# Drop provider_states that no longer have a writer. Deleted collectors (e.g.
# Polymarket) otherwise remain "available" forever via last-write-wins merge.
PROVIDER_STATE_MAX_AGE_SECONDS = 6 * 60 * 60


@contextmanager
def _raw_quote_path_lock(path: Path) -> Iterator[None]:
    """Fence open/write against replay quarantine using a stable path lock."""

    lock_path = path.with_name(f".{path.name}.lock")
    with lock_path.open("a", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


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
    failover_mode: str | None = None

    def best_quote(self, instrument_id: str) -> Quote | None:
        for quote in self.best_quotes:
            if instrument_matches_id(quote.instrument, instrument_id):
                return quote
        return None

    def to_dict(self) -> dict[str, object]:
        return {
            "created_at": self.created_at.isoformat(),
            "as_of": self.as_of.isoformat(),
            "quotes": [quote.to_dict() for quote in self.quotes],
            "best_quotes": [quote.to_dict() for quote in self.best_quotes],
            "provider_states": [state.to_dict() for state in self.provider_states],
            "failover_mode": self.failover_mode,
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
            with _raw_quote_path_lock(path):
                with path.open("a", encoding="utf-8") as handle:
                    # Retain the inode lock for compatibility with older
                    # writers during rollout; the stable adjacent path lock
                    # also fences replay quarantine/rename before open().
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
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

        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            # A corrupt state file must not crash collectors in a loop: move it
            # aside for forensics and rebuild from an empty projection so the
            # next update() writes a fresh file.
            quarantine_path = self._quarantine_corrupt_state(now=now)
            LOGGER.warning(
                "latest state unreadable (%s); quarantined %s to %s, rebuilding empty",
                exc,
                self.path,
                quarantine_path,
            )
            return LatestState(created_at=now, as_of=now, quotes=(), best_quotes=())
        quotes_payload = payload.get("quotes") if isinstance(payload, dict) else []
        best_payload = payload.get("best_quotes") if isinstance(payload, dict) else []
        provider_states_payload = payload.get("provider_states") if isinstance(payload, dict) else []
        quotes = tuple(
            quote_from_dict(item) for item in quotes_payload if isinstance(item, dict)
        )
        quotes = prune_expired_option_quotes(quotes, now=now)
        best_quotes = (
            ()
            if refresh_quality
            else tuple(
                quote_from_dict(item) for item in best_payload if isinstance(item, dict)
            )
        )
        provider_states = tuple(
            provider_state_from_dict(item)
            for item in provider_states_payload
            if isinstance(item, dict)
        )
        created_at = as_utc_from_payload(payload.get("created_at")) if isinstance(payload, dict) else now
        as_of = as_utc_from_payload(payload.get("as_of")) if isinstance(payload, dict) else now
        failover_mode = str(payload.get("failover_mode") or "") or None
        state = LatestState(
            created_at=created_at,
            as_of=as_of,
            quotes=quotes,
            best_quotes=best_quotes,
            provider_states=provider_states,
            failover_mode=failover_mode,
        )
        if refresh_quality:
            return self.refresh_quality(state, now=now)
        return state

    def refresh_quality(
        self,
        state: LatestState,
        *,
        now: datetime | None = None,
    ) -> LatestState:
        """Re-evaluate freshness without reparsing an unchanged projection."""

        as_of = as_utc(now or datetime.now(tz=timezone.utc))
        quotes = prune_expired_option_quotes(state.quotes, now=as_of)
        quotes = tuple(
            degrade_stale_quote(
                quote,
                as_of=as_of,
                stale_after_seconds=self.settings.latest_stale_after_seconds,
                delayed_stale_after_seconds=self.settings.delayed_stale_after_seconds,
                slow_stale_after_seconds=self.settings.slow_index_stale_after_seconds,
                slow_labels=self.settings.slow_index_labels,
                rotation_stale_after_seconds=self.settings.rotation_stale_after_seconds,
            )
            for quote in quotes
        )
        failover_mode = self._provider_failover_mode(now=as_of)
        return LatestState(
            created_at=state.created_at,
            as_of=as_of,
            quotes=quotes,
            best_quotes=select_best_quotes(
                quotes,
                as_of=as_of,
                provider_priority=self.settings.provider_priority,
                failover_mode=failover_mode,
            ),
            provider_states=latest_provider_states(state.provider_states, now=as_of),
            failover_mode=failover_mode,
        )

    def _quarantine_corrupt_state(self, *, now: datetime) -> Path | None:
        """Move an unreadable state file aside so the next write can self-heal."""
        quarantine_path = self.path.with_name(
            f"{self.path.name}.corrupt-{int(now.timestamp())}"
        )
        try:
            self.path.rename(quarantine_path)
        except OSError:
            return None
        return quarantine_path

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
            provider_latest = prune_expired_option_quotes(provider_latest, now=now)
            provider_states_latest = latest_provider_states(
                existing_state.provider_states + tuple(provider_states),
                now=now,
            )
            aged_quotes = tuple(
                degrade_stale_quote(
                    quote,
                    as_of=now,
                    stale_after_seconds=self.settings.latest_stale_after_seconds,
                    delayed_stale_after_seconds=self.settings.delayed_stale_after_seconds,
                    slow_stale_after_seconds=self.settings.slow_index_stale_after_seconds,
                    slow_labels=self.settings.slow_index_labels,
                    rotation_stale_after_seconds=self.settings.rotation_stale_after_seconds,
                )
                for quote in provider_latest
            )
            best_quotes = select_best_quotes(
                aged_quotes,
                as_of=now,
                provider_priority=self.settings.provider_priority,
                failover_mode=(failover_mode := self._provider_failover_mode(now=now)),
            )
            state = LatestState(
                created_at=datetime.now(tz=timezone.utc),
                as_of=now,
                quotes=tuple(sorted(aged_quotes, key=quote_sort_key)),
                best_quotes=tuple(
                    sorted(best_quotes, key=lambda quote: quote.instrument.canonical_id)
                ),
                provider_states=provider_states_latest,
                failover_mode=failover_mode,
            )
            self.write(state)
        return LatestUpdateResult(
            path=str(self.path),
            provider_quote_count=len(state.quotes),
            best_quote_count=len(state.best_quotes),
            updated_quote_count=len(incoming),
        )

    def purge_provider_quotes(
        self,
        provider: Provider,
        *,
        now: datetime | None = None,
    ) -> LatestUpdateResult:
        now = as_utc(now or datetime.now(tz=timezone.utc))
        with self.exclusive_lock():
            existing_state = self.load(now=now, refresh_quality=False)
            remaining_quotes = tuple(
                quote for quote in existing_state.quotes if quote.provider != provider
            )
            best_quotes = select_best_quotes(
                remaining_quotes,
                as_of=now,
                provider_priority=self.settings.provider_priority,
                failover_mode=(failover_mode := self._provider_failover_mode(now=now)),
            )
            state = LatestState(
                created_at=datetime.now(tz=timezone.utc),
                as_of=now,
                quotes=tuple(sorted(remaining_quotes, key=quote_sort_key)),
                best_quotes=tuple(
                    sorted(best_quotes, key=lambda quote: quote.instrument.canonical_id)
                ),
                provider_states=existing_state.provider_states,
                failover_mode=failover_mode,
            )
            self.write(state)
        return LatestUpdateResult(
            path=str(self.path),
            provider_quote_count=len(state.quotes),
            best_quote_count=len(state.best_quotes),
            updated_quote_count=0,
        )

    def write(self, state: LatestState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(state.to_dict(), indent=2, sort_keys=True))
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(self.path)
        _fsync_directory(self.path.parent)

    def _provider_failover_mode(self, *, now: datetime) -> str | None:
        """Resolve fresh control state; fail closed during monitored sessions."""

        configured_path = self.settings.provider_failover_state_path.strip()
        if not configured_path:
            return None
        monitored_session = (
            DEFAULT_MARKET_CALENDAR.is_rth_open(now)
            or DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now)
        )
        try:
            raw = json.loads(Path(configured_path).read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            return "blocked" if monitored_session else None
        if not isinstance(raw, dict):
            return "blocked" if monitored_session else None
        updated_at = parse_timestamp(raw.get("updated_at"))
        age_seconds = (
            (as_utc(now) - updated_at).total_seconds()
            if updated_at is not None
            else None
        )
        fresh = (
            age_seconds is not None
            and 0 <= age_seconds
            <= self.settings.provider_failover_control_max_age_seconds
        )
        mode = str(raw.get("mode") or "")
        if (
            raw.get("monitoring_active") is True
            and fresh
            and mode
            in {
                "schwab_primary",
                "ibkr_fallback",
                "recovery_pending",
                "both_unavailable",
            }
        ):
            return mode
        return "blocked" if monitored_session else None



def latest_by_provider(quotes: Iterable[Quote]) -> tuple[Quote, ...]:
    result: dict[tuple[str, str], Quote] = {}
    for quote in quotes:
        key = (quote.instrument.canonical_id, quote.provider.value)
        previous = result.get(key)
        if (
            previous is not None
            and quote.instrument.instrument_type is InstrumentType.OPTION
            and previous.instrument.instrument_type is InstrumentType.OPTION
        ):
            result[key] = merge_option_observations(previous, quote)
        elif previous is None or as_utc(quote.received_at) >= as_utc(previous.received_at):
            result[key] = quote
    return tuple(result.values())


def merge_option_observations(left: Quote, right: Quote) -> Quote:
    """Merge independent pricing and structure clocks for one provider option."""

    if left.instrument.canonical_id != right.instrument.canonical_id:
        raise ValueError("cannot merge different option instruments")
    if left.provider is not right.provider:
        raise ValueError("cannot merge option observations from different providers")

    def pricing_time(quote: Quote) -> datetime:
        return as_utc(quote.quote_time or quote.trade_time or quote.received_at)

    def field_time(quote: Quote, field: str) -> datetime:
        raw = quote.raw if isinstance(quote.raw, Mapping) else {}
        explicit = parse_timestamp(raw.get(f"{field}_observed_at"))
        if explicit is not None:
            return as_utc(explicit)
        field_provider = raw.get(f"{field}_provider")
        if field_provider is not None and str(field_provider) != quote.provider.value:
            # A merged field belongs to a different provider, so neither the
            # top-level structure clock nor this flush's receipt time proves
            # that provider re-observed it. Retain the value for forensics but
            # give it a deterministically stale clock until explicit
            # provenance is available.
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        if quote.structure_time is not None:
            return as_utc(quote.structure_time)
        if (
            field in {"greeks", "open_interest"}
            and quote.provider is Provider.IBKR
            and quote.sampling_mode is not None
        ):
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        return as_utc(quote.received_at)

    def pricing_observed_at(quote: Quote) -> datetime:
        raw = quote.raw if isinstance(quote.raw, Mapping) else {}
        explicit = parse_timestamp(raw.get("pricing_observed_at"))
        return as_utc(explicit or quote.last_update_at or quote.received_at)

    def bool_or_none(value: object) -> bool | None:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in {0, 1}:
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes"}:
                return True
            if normalized in {"false", "0", "no"}:
                return False
        return None

    def generic_delayed(raw: Mapping[str, object]) -> bool | None:
        values: list[bool] = []
        for key, value in raw.items():
            normalized = "".join(
                character for character in str(key).lower() if character.isalnum()
            )
            if normalized not in {"isdelayed", "delayed"}:
                continue
            parsed = bool_or_none(value)
            if parsed is not None:
                values.append(parsed)
        if True in values:
            return True
        if False in values:
            return False
        return None

    def field_provenance(
        quote: Quote,
        field: str,
        *,
        observed_at: datetime,
        source_at: datetime | None = None,
    ) -> dict[str, object]:
        source_raw = quote.raw if isinstance(quote.raw, Mapping) else {}
        provider_value = source_raw.get(f"{field}_provider")
        provider = str(provider_value or quote.provider.value)
        same_provider = provider == quote.provider.value
        sampling = source_raw.get(f"{field}_sampling_mode")
        if sampling is None and same_provider:
            sampling = quote.sampling_mode
        market_data_type = source_raw.get(f"{field}_market_data_type")
        if market_data_type is None and same_provider:
            market_data_type = quote.market_data_type
        explicit_delayed = bool_or_none(
            source_raw.get(f"{field}_explicit_delayed")
        )
        if explicit_delayed is None and same_provider:
            explicit_delayed = generic_delayed(source_raw)
        entitlement = bool_or_none(source_raw.get(f"{field}_live_entitlement"))
        entitlement_source = source_raw.get(f"{field}_live_entitlement_source")
        feed_mode = quality_from_market_data_type(market_data_type)
        if explicit_delayed is True:
            entitlement = False
            entitlement_source = "explicit_delayed"
        elif feed_mode in {
            MarketDataQuality.FROZEN,
            MarketDataQuality.DELAYED,
            MarketDataQuality.DELAYED_FROZEN,
        }:
            entitlement = False
            entitlement_source = f"market_data_type_{feed_mode.value}"
        elif same_provider and quote.quality in {
            MarketDataQuality.FROZEN,
            MarketDataQuality.DELAYED,
            MarketDataQuality.DELAYED_FROZEN,
        }:
            entitlement = False
            entitlement_source = f"quality_{quote.quality.value}"
        elif entitlement is None and explicit_delayed is False:
            entitlement = True
            entitlement_source = (
                "schwab_explicit_not_delayed"
                if provider == Provider.SCHWAB.value
                else "explicit_not_delayed"
            )
        elif entitlement is None and feed_mode is MarketDataQuality.LIVE:
            entitlement = True
            entitlement_source = "market_data_type_live"
        elif (
            entitlement is None
            and same_provider
            and quote.quality is MarketDataQuality.LIVE
        ):
            entitlement = True
            entitlement_source = "quality_live"
        payload: dict[str, object] = {
            f"{field}_provider": provider,
            f"{field}_sampling_mode": sampling,
            f"{field}_market_data_type": market_data_type,
            f"{field}_explicit_delayed": explicit_delayed,
            f"{field}_live_entitlement": entitlement,
            f"{field}_live_entitlement_source": entitlement_source,
            f"{field}_observed_at": observed_at.isoformat(),
        }
        if source_at is not None:
            payload[f"{field}_source_at"] = source_at.isoformat()
        return payload

    # A priceless row (e.g. a Schwab MISSING placeholder from a partial batch
    # response) must not displace last-known-good pricing; the kept quote_time
    # lets quote_use_decision still mark the merged row stale.
    pricing = max(
        (left, right),
        key=lambda quote: (
            quote.has_price,
            pricing_time(quote),
            pricing_observed_at(quote),
            quote.mid is not None,
        ),
    )
    greek_candidates = [quote for quote in (left, right) if quote.greeks is not None]
    oi_candidates = [quote for quote in (left, right) if quote.open_interest is not None]
    greek_source = (
        max(greek_candidates, key=lambda quote: field_time(quote, "greeks"))
        if greek_candidates
        else None
    )
    oi_source = (
        max(oi_candidates, key=lambda quote: field_time(quote, "open_interest"))
        if oi_candidates
        else None
    )
    latest_received = max(as_utc(left.received_at), as_utc(right.received_at))
    raw = dict(pricing.raw or {})
    raw.update(
        field_provenance(
            pricing,
            "pricing",
            observed_at=pricing_observed_at(pricing),
            source_at=pricing_time(pricing),
        )
    )
    if greek_source is not None:
        raw.update(
            field_provenance(
                greek_source,
                "greeks",
                observed_at=field_time(greek_source, "greeks"),
            )
        )
    if oi_source is not None:
        raw.update(
            field_provenance(
                oi_source,
                "open_interest",
                observed_at=field_time(oi_source, "open_interest"),
            )
        )
    if greek_source is None and oi_source is None:
        return replace(pricing, received_at=latest_received, raw=raw)
    structure_times = [
        field_time(source, field)
        for source, field in (
            (greek_source, "greeks"),
            (oi_source, "open_interest"),
        )
        if source is not None
    ]
    return replace(
        pricing,
        received_at=latest_received,
        open_interest=oi_source.open_interest if oi_source is not None else None,
        greeks=greek_source.greeks if greek_source is not None else None,
        structure_time=max(structure_times),
        volume=(
            pricing.volume
            if pricing.volume is not None
            else oi_source.volume
            if oi_source is not None
            else None
        ),
        raw=raw,
    )


def latest_provider_states(
    states: Iterable[ProviderState],
    *,
    now: datetime | None = None,
    max_age_seconds: float = PROVIDER_STATE_MAX_AGE_SECONDS,
) -> tuple[ProviderState, ...]:
    result: dict[Provider, ProviderState] = {}
    for state in states:
        previous = result.get(state.provider)
        if previous is None or as_utc(state.checked_at) >= as_utc(previous.checked_at):
            result[state.provider] = state
    as_of = as_utc(now or datetime.now(tz=timezone.utc))
    kept = [
        state
        for state in result.values()
        if (as_of - as_utc(state.checked_at)).total_seconds() <= max_age_seconds
    ]
    return tuple(sorted(kept, key=lambda item: item.provider.value))


def select_best_quotes(
    quotes: Iterable[Quote],
    *,
    as_of: datetime | None = None,
    provider_priority: Iterable[Provider | str] = DEFAULT_PROVIDER_PRIORITY,
    failover_mode: str | None = None,
) -> tuple[Quote, ...]:
    grouped: dict[str, list[Quote]] = defaultdict(list)
    for quote in quotes:
        grouped[quote.instrument.canonical_id].append(quote)

    selection_time = as_utc(as_of or datetime.now(tz=timezone.utc))
    configured_priority = tuple(provider_priority)
    best: list[Quote] = []
    for instrument_id in sorted(grouped):
        candidates = pricing_candidates(
            grouped[instrument_id],
            as_of=selection_time,
            failover_mode=failover_mode,
        )
        if not candidates:
            continue
        quote = choose_best_quote(
            candidates,
            as_of=selection_time,
            provider_priority=pricing_provider_priority(
                candidates[0].instrument,
                as_of=selection_time,
                configured=configured_priority,
                failover_mode=failover_mode,
            ),
        )
        if quote is not None:
            best.append(quote)
    return tuple(best)


def parse_option_expiry_date(expiry: str | None) -> date | None:
    if not expiry:
        return None
    text = str(expiry).strip()
    if not text:
        return None
    if len(text) >= 8 and text[:8].isdigit():
        try:
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        except ValueError:
            return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None
    return None


def prune_expired_option_quotes(quotes: Iterable[Quote], *, now: datetime) -> tuple[Quote, ...]:
    """Discard option rows before the active 17:00 ET research expiry."""
    active_expiry = DEFAULT_MARKET_CALENDAR.research_expiry(now)
    kept: list[Quote] = []
    for quote in quotes:
        if quote.instrument.instrument_type != InstrumentType.OPTION:
            kept.append(quote)
            continue
        expiry_date = parse_option_expiry_date(quote.instrument.expiry)
        if expiry_date is None or expiry_date >= active_expiry:
            kept.append(quote)
    return tuple(kept)


def resolve_stale_after_seconds(
    instrument_id: str,
    *,
    default_seconds: float,
    slow_seconds: float,
    slow_labels: frozenset[str],
) -> float:
    return slow_seconds if instrument_id in slow_labels else default_seconds


def _is_ibkr_rotated_option(quote: Quote) -> bool:
    """IBKR stream option rows refresh on a round-robin cycle, not every tick."""

    return (
        quote.provider == Provider.IBKR
        and quote.instrument.instrument_type == InstrumentType.OPTION
    )


def degrade_stale_quote(
    quote: Quote,
    *,
    as_of: datetime,
    stale_after_seconds: float,
    delayed_stale_after_seconds: float = 60.0,
    slow_stale_after_seconds: float | None = None,
    slow_labels: frozenset[str] | None = None,
    rotation_stale_after_seconds: float | None = None,
) -> Quote:
    threshold = stale_after_seconds
    if rotation_stale_after_seconds is not None and _is_ibkr_rotated_option(quote):
        # Rotating slices are only re-subscribed once per cycle (~25s); the
        # generic 15s window would always mark the oldest slice stale even
        # though the row is as fresh as the rotation design allows. Callers
        # that deliberately widen the window (e.g. GTH analytics at 90s) win
        # over the rotation default.
        threshold = max(rotation_stale_after_seconds, stale_after_seconds)
        delayed_stale_after_seconds = threshold
    elif slow_stale_after_seconds is not None and slow_labels:
        threshold = resolve_stale_after_seconds(
            quote.instrument.canonical_id,
            default_seconds=stale_after_seconds,
            slow_seconds=slow_stale_after_seconds,
            slow_labels=slow_labels,
        )
    decision = quote_use_decision(
        quote,
        as_of=as_of,
        stale_after_seconds=threshold,
        delayed_stale_after_seconds=threshold
        if slow_labels and quote.instrument.canonical_id in slow_labels
        else delayed_stale_after_seconds,
        # Rotating/quiet stream options re-confirm on every subscription
        # slice even when the price is unchanged; judge them by tick recency
        # (quote_time), not by the price-change fingerprint.
        prefer_quote_time=_is_ibkr_rotated_option(quote),
    )
    if decision.freshness != QuoteFreshness.STALE:
        if decision.freshness == QuoteFreshness.UNKNOWN and quote.quality in {
            MarketDataQuality.LIVE,
            MarketDataQuality.FROZEN,
        }:
            return replace(quote, quality=MarketDataQuality.UNKNOWN)
        return quote
    return replace(quote, quality=MarketDataQuality.STALE)


def configured_quote_use_decision(
    quote: Quote,
    *,
    as_of: datetime,
    settings: StorageSettings | None = None,
    allow_frozen: bool = False,
) -> QuoteUseDecision:
    settings = settings or current_storage_settings()
    is_slow = quote.instrument.canonical_id in settings.slow_index_labels
    if _is_ibkr_rotated_option(quote):
        # See degrade_stale_quote: rotation rows use the wider of the rotation
        # window and the caller-configured latest window (GTH analytics
        # deliberately sets 90s).
        stale_after_seconds = max(
            settings.rotation_stale_after_seconds,
            settings.latest_stale_after_seconds,
        )
        delayed_stale_after_seconds = stale_after_seconds
    else:
        stale_after_seconds = (
            settings.slow_index_stale_after_seconds
            if is_slow
            else settings.latest_stale_after_seconds
        )
        delayed_stale_after_seconds = (
            settings.slow_index_stale_after_seconds
            if is_slow
            else settings.delayed_stale_after_seconds
        )
    decision = quote_use_decision(
        quote,
        as_of=as_of,
        stale_after_seconds=stale_after_seconds,
        delayed_stale_after_seconds=delayed_stale_after_seconds,
        allow_frozen=allow_frozen,
        prefer_quote_time=_is_ibkr_rotated_option(quote),
    )
    if isinstance(quote.raw, Mapping) and quote.raw.get("analytical_only") is True:
        return replace(
            decision,
            alert_allowed=False,
            pricing_allowed=False,
            reason="analytical_only_non_executable",
        )
    if not _is_ibkr_rotated_option(quote) or not decision.research_usable:
        return decision
    pricing_decision = quote_use_decision(
        quote,
        as_of=as_of,
        stale_after_seconds=settings.latest_stale_after_seconds,
        delayed_stale_after_seconds=settings.delayed_stale_after_seconds,
        allow_frozen=allow_frozen,
        prefer_quote_time=True,
    )
    return replace(
        decision,
        alert_allowed=pricing_decision.alert_allowed,
        pricing_allowed=pricing_decision.pricing_allowed,
        reason=pricing_decision.reason if not pricing_decision.pricing_allowed else decision.reason,
    )


def quote_sort_key(quote: Quote) -> tuple[str, str]:
    return (quote.instrument.canonical_id, quote.provider.value)


def as_utc_from_payload(value: object) -> datetime:
    return parse_timestamp(value) or datetime.now(tz=timezone.utc)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        directory_fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    except OSError:
        pass
    finally:
        os.close(directory_fd)
