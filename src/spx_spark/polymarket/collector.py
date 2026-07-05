from __future__ import annotations

import argparse
import json
import re
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urljoin
from urllib.request import Request, urlopen

from spx_spark.config import PolymarketSettings, StorageSettings
from spx_spark.marketdata import (
    InstrumentId,
    InstrumentType,
    MarketDataQuality,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
    as_utc,
    clean_float,
    parse_timestamp,
)
from spx_spark.provider_adapter import ProviderSnapshot, persist_provider_snapshot


SPX_TERMS = ("SPY", "S&P", "S&P 500", "SPX", "STOCK MARKET")
MACRO_TERMS = ("FED", "FOMC", "CPI", "PPI", "NFP", "POWELL", "JOBS", "INFLATION")
CATEGORY_PRIORITY = {
    "spx_spy": 0,
    "macro_event": 1,
    "other": 9,
}


@dataclass(frozen=True)
class PolymarketRecord:
    search_term: str
    category: str
    event_id: str | None
    event_slug: str | None
    event_title: str | None
    market_id: str
    market_slug: str
    question: str
    condition_id: str | None
    end_date: str | None
    active: bool
    closed: bool
    enable_order_book: bool
    outcomes: tuple[str, ...]
    outcome_prices: tuple[float | None, ...]
    yes_price: float | None
    no_price: float | None
    liquidity: float | None
    volume: float | None
    volume_24h: float | None
    open_interest: float | None
    clob_token_ids: tuple[str, ...]
    relevance_score: float
    received_at: datetime

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["received_at"] = self.received_at.isoformat()
        return payload


class PolymarketClient:
    def __init__(self, settings: PolymarketSettings) -> None:
        self.settings = settings

    def get(self, path: str, params: Mapping[str, object] | None = None) -> Any:
        base = self.settings.gamma_api_base_url.rstrip("/") + "/"
        url = urljoin(base, path.lstrip("/"))
        if params:
            url += "?" + urlencode(params)
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": self.settings.user_agent,
            },
        )
        with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else None


def parse_json_array(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def text_blob(*values: object) -> str:
    return " ".join(str(value or "") for value in values).upper()


def classify_category(search_term: str, market: Mapping[str, Any], event: Mapping[str, Any] | None) -> str:
    blob = text_blob(
        search_term,
        market.get("question"),
        market.get("slug"),
        event.get("title") if event else None,
        event.get("slug") if event else None,
    )
    if any(term in blob for term in SPX_TERMS):
        return "spx_spy"
    if any(term in blob for term in MACRO_TERMS):
        return "macro_event"
    return "other"


def relevance_score(search_term: str, market: Mapping[str, Any], event: Mapping[str, Any] | None) -> float:
    blob = text_blob(
        search_term,
        market.get("question"),
        market.get("description"),
        market.get("slug"),
        event.get("title") if event else None,
        event.get("description") if event else None,
    )
    score = 0.0
    if any(term in blob for term in SPX_TERMS):
        score += 0.55
    if any(term in blob for term in MACRO_TERMS):
        score += 0.35
    if "PYTH" in blob or "BLS" in blob or "FEDERAL RESERVE" in blob:
        score += 0.10
    return min(score, 1.0)


def yes_no_prices(outcomes: list[object], prices: list[object]) -> tuple[float | None, float | None]:
    yes_price = None
    no_price = None
    for outcome, raw_price in zip(outcomes, prices):
        label = str(outcome).strip().lower()
        price = clean_float(raw_price)
        if label == "yes":
            yes_price = price
        elif label == "no":
            no_price = price
    return yes_price, no_price


def slug_to_symbol(slug: str, market_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "-", slug).strip("-").upper()
    return f"POLY:{market_id}:{cleaned[:48] or 'MARKET'}"


def record_from_market(
    *,
    search_term: str,
    event: Mapping[str, Any] | None,
    market: Mapping[str, Any],
    received_at: datetime,
) -> PolymarketRecord | None:
    market_id = str(market.get("id") or "")
    market_slug = str(market.get("slug") or "")
    question = str(market.get("question") or "")
    if not market_id or not market_slug or not question:
        return None
    outcomes = parse_json_array(market.get("outcomes"))
    outcome_prices_raw = parse_json_array(market.get("outcomePrices"))
    outcome_prices = tuple(clean_float(value) for value in outcome_prices_raw)
    yes_price, no_price = yes_no_prices(outcomes, outcome_prices_raw)
    return PolymarketRecord(
        search_term=search_term,
        category=classify_category(search_term, market, event),
        event_id=str(event.get("id")) if event and event.get("id") is not None else None,
        event_slug=str(event.get("slug")) if event and event.get("slug") is not None else None,
        event_title=str(event.get("title")) if event and event.get("title") is not None else None,
        market_id=market_id,
        market_slug=market_slug,
        question=question,
        condition_id=str(market.get("conditionId")) if market.get("conditionId") is not None else None,
        end_date=str(market.get("endDate") or market.get("endDateIso") or "") or None,
        active=truthy(market.get("active")),
        closed=truthy(market.get("closed")),
        enable_order_book=truthy(market.get("enableOrderBook")),
        outcomes=tuple(str(value) for value in outcomes),
        outcome_prices=outcome_prices,
        yes_price=yes_price,
        no_price=no_price,
        liquidity=clean_float(market.get("liquidityNum") or market.get("liquidity")),
        volume=clean_float(market.get("volumeNum") or market.get("volume")),
        volume_24h=clean_float(market.get("volume24hr") or market.get("volume24hrNum")),
        open_interest=clean_float(market.get("openInterest")),
        clob_token_ids=tuple(str(value) for value in parse_json_array(market.get("clobTokenIds"))),
        relevance_score=relevance_score(search_term, market, event),
        received_at=received_at,
    )


def record_passes_filters(
    record: PolymarketRecord,
    settings: PolymarketSettings,
    *,
    now: datetime,
) -> bool:
    if not settings.include_closed:
        if not record.active or record.closed:
            return False
        end_time = parse_timestamp(record.end_date)
        if end_time is not None and end_time < now:
            return False
    return (
        record.relevance_score >= settings.min_relevance_score
        and (record.liquidity or 0.0) >= settings.min_liquidity
        and (record.volume_24h or 0.0) >= settings.min_volume_24h
    )


def quote_from_record(record: PolymarketRecord) -> Quote:
    price = record.yes_price
    quality = (
        MarketDataQuality.LIVE
        if record.active and not record.closed and price is not None
        else MarketDataQuality.MISSING
    )
    return Quote(
        instrument=InstrumentId(
            symbol=slug_to_symbol(record.market_slug, record.market_id),
            instrument_type=InstrumentType.PREDICTION_MARKET,
            provider_symbol=record.market_slug,
            exchange="Polymarket",
            currency="USDC",
        ),
        provider=Provider.POLYMARKET,
        provider_symbol=record.market_slug,
        received_at=record.received_at,
        quality=quality,
        mark=price,
        volume=record.volume,
        open_interest=record.open_interest,
        quote_time=record.received_at,
        market_data_type="research_only",
        raw=record.to_dict(),
    )


def event_markets(event: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    markets = event.get("markets")
    return [market for market in markets if isinstance(market, Mapping)] if isinstance(markets, list) else []


def normalize_event_payload(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [event for event in payload if isinstance(event, Mapping)]
    if isinstance(payload, Mapping):
        events = payload.get("events")
        if isinstance(events, list):
            return [event for event in events if isinstance(event, Mapping)]
        return [payload]
    return []


def normalize_market_payload(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [market for market in payload if isinstance(market, Mapping)]
    if isinstance(payload, Mapping):
        return [payload]
    return []


def collect_records(client: PolymarketClient, settings: PolymarketSettings) -> tuple[PolymarketRecord, ...]:
    received_at = datetime.now(tz=timezone.utc)
    records: dict[str, PolymarketRecord] = {}

    for term in settings.search_terms:
        payload = client.get(
            "public-search",
            {"q": term, "limit": settings.max_results_per_query},
        )
        for event in normalize_event_payload(payload):
            for market in event_markets(event):
                record = record_from_market(search_term=term, event=event, market=market, received_at=received_at)
                if record is not None:
                    records[record.market_id] = record

    for slug in settings.event_slugs:
        payload = client.get(f"events/slug/{quote(slug)}")
        for event in normalize_event_payload(payload):
            for market in event_markets(event):
                record = record_from_market(search_term=f"event:{slug}", event=event, market=market, received_at=received_at)
                if record is not None:
                    records[record.market_id] = record

    for slug in settings.market_slugs:
        payload = client.get(f"markets/slug/{quote(slug)}")
        for market in normalize_market_payload(payload):
            record = record_from_market(search_term=f"market:{slug}", event=None, market=market, received_at=received_at)
            if record is not None:
                records[record.market_id] = record

    filtered = [
        record
        for record in records.values()
        if record_passes_filters(record, settings, now=received_at)
    ]
    sorted_records = sorted(
        filtered,
        key=lambda item: (
            CATEGORY_PRIORITY.get(item.category, 9),
            -item.relevance_score,
            -(item.liquidity or 0.0),
            item.market_slug,
        ),
    )
    if settings.max_markets_per_run > 0:
        sorted_records = sorted_records[: settings.max_markets_per_run]
    return tuple(sorted_records)


def provider_state_from_records(
    records: tuple[PolymarketRecord, ...],
    *,
    checked_at: datetime,
    latency_ms: float,
) -> ProviderState:
    if records:
        status = ProviderStatus.AVAILABLE
        reason = None
    else:
        status = ProviderStatus.DEGRADED
        reason = "Polymarket query returned no relevant active markets"
    return ProviderState(
        provider=Provider.POLYMARKET,
        status=status,
        checked_at=checked_at,
        reason=reason,
        connected=True,
        authenticated=None,
        latency_ms=latency_ms,
        priority=4,
    )


def unavailable_state(*, checked_at: datetime, latency_ms: float, reason: str) -> ProviderState:
    return ProviderState(
        provider=Provider.POLYMARKET,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=checked_at,
        reason=reason,
        connected=False,
        authenticated=None,
        latency_ms=latency_ms,
        priority=4,
    )


def post_error(exc: BaseException) -> str:
    if isinstance(exc, HTTPError):
        try:
            body = exc.read().decode("utf-8")[:500]
        except Exception:  # noqa: BLE001
            body = ""
        return f"HTTP {exc.code}: {exc.reason}; {body}".strip()
    return str(exc)


def context_path(storage_settings: StorageSettings, received_at: datetime) -> Path:
    timestamp = as_utc(received_at)
    return (
        Path(storage_settings.data_root)
        / "context"
        / "provider=polymarket"
        / f"date={timestamp.strftime('%Y-%m-%d')}"
        / f"hour={timestamp.strftime('%H')}"
        / "markets.jsonl"
    )


def features_path(storage_settings: StorageSettings, received_at: datetime) -> Path:
    timestamp = as_utc(received_at)
    return (
        Path(storage_settings.data_root)
        / "features"
        / "polymarket"
        / f"date={timestamp.strftime('%Y-%m-%d')}"
        / f"hour={timestamp.strftime('%H')}"
        / "snapshots.jsonl"
    )


def latest_context_path(storage_settings: StorageSettings) -> Path:
    return Path(storage_settings.data_root) / "latest" / "polymarket_context.json"


def build_features(records: tuple[PolymarketRecord, ...], *, as_of: datetime) -> dict[str, object]:
    by_category: dict[str, int] = {}
    for record in records:
        by_category[record.category] = by_category.get(record.category, 0) + 1
    top_records = sorted(
        records,
        key=lambda item: (item.relevance_score, item.liquidity or 0.0, item.volume_24h or 0.0),
        reverse=True,
    )[:12]
    return {
        "as_of": as_of.isoformat(),
        "research_only": True,
        "human_visible": False,
        "usage_gate": "context_only_no_kelly_no_direct_alert",
        "market_count": len(records),
        "category_counts": by_category,
        "top_markets": [
            {
                "category": record.category,
                "market_id": record.market_id,
                "slug": record.market_slug,
                "question": record.question,
                "yes_price": record.yes_price,
                "liquidity": record.liquidity,
                "volume_24h": record.volume_24h,
                "relevance_score": record.relevance_score,
                "end_date": record.end_date,
            }
            for record in top_records
        ],
    }


def write_jsonl(path: Path, payloads: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for payload in payloads:
            handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")))
            handle.write("\n")
    return path


def write_latest(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)
    return path


def collect_once(
    client: PolymarketClient,
    settings: PolymarketSettings,
) -> tuple[tuple[PolymarketRecord, ...], tuple[Quote, ...], ProviderState, dict[str, object]]:
    started = time.perf_counter()
    records = collect_records(client, settings)
    received_at = records[0].received_at if records else datetime.now(tz=timezone.utc)
    quotes = tuple(quote_from_record(record) for record in records)
    latency_ms = (time.perf_counter() - started) * 1000.0
    state = provider_state_from_records(records, checked_at=received_at, latency_ms=latency_ms)
    features = build_features(records, as_of=received_at)
    return records, quotes, state, features


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect public Polymarket research probabilities.")
    parser.add_argument("--print-config", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = PolymarketSettings.from_env()
    storage_settings = StorageSettings.from_env()
    if args.print_config:
        print(
            json.dumps(
                {"polymarket": asdict(settings), "storage": asdict(storage_settings)},
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    started = time.perf_counter()
    client = PolymarketClient(settings)
    try:
        records, quotes, state, features = collect_once(client, settings)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        checked_at = datetime.now(tz=timezone.utc)
        state = unavailable_state(
            checked_at=checked_at,
            latency_ms=(time.perf_counter() - started) * 1000.0,
            reason=post_error(exc),
        )
        write_result = persist_provider_snapshot(
            ProviderSnapshot.from_state(Provider.POLYMARKET, state, received_at=checked_at),
            storage_settings,
        )
        if args.json:
            print(
                json.dumps(
                    {
                        "provider_state": state.to_dict(),
                        "latest_state": write_result.latest_state,
                        "quotes_collected": 0,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print(f"Polymarket unavailable: {state.reason}")
        return 1

    received_at = state.checked_at
    context_file = write_jsonl(context_path(storage_settings, received_at), [record.to_dict() for record in records])
    features_file = write_jsonl(features_path(storage_settings, received_at), [features])
    latest_file = write_latest(latest_context_path(storage_settings), features)
    snapshot = ProviderSnapshot(
        provider=Provider.POLYMARKET,
        received_at=received_at,
        quotes=quotes,
        provider_states=(state,),
        metadata={
            "context_path": str(context_file),
            "features_path": str(features_file),
            "latest_context_path": str(latest_file),
            "research_only": True,
            "replace_provider_quotes": True,
        },
    )
    write_result = persist_provider_snapshot(snapshot, storage_settings)
    summary = {
        "provider_state": state.to_dict(),
        "quotes_collected": len(quotes),
        "features": features,
        "raw_paths": write_result.raw_paths,
        "context_path": str(context_file),
        "features_path": str(features_file),
        "latest_context_path": str(latest_file),
        "latest_state": write_result.latest_state,
        "provider_quote_count": write_result.provider_quote_count,
        "best_quote_count": write_result.best_quote_count,
    }
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(f"Polymarket: {state.status.value} markets={len(records)}")
        print(f"context: {context_file}")
        print(f"features: {features_file}")
        print(f"latest: {latest_file}")
    return 0 if state.status != ProviderStatus.UNAVAILABLE else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
