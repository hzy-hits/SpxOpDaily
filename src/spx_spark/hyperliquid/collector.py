from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from spx_spark.config import HyperliquidSettings, StorageSettings
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


COIN_ALIASES: dict[str, tuple[str, str]] = {
    "S&P500-USDC": ("xyz", "xyz:SP500"),
    "S&P500/USDC": ("xyz", "xyz:SP500"),
    "SP500-USDC": ("xyz", "xyz:SP500"),
    "SP500/USDC": ("xyz", "xyz:SP500"),
    "SP500": ("xyz", "xyz:SP500"),
    "S&P500": ("xyz", "xyz:SP500"),
}


@dataclass(frozen=True)
class HyperliquidTradeStats:
    last_price: float | None
    last_size: float | None
    last_trade_time: datetime | None
    trade_count: int
    buy_notional: float
    sell_notional: float
    large_trade_count: int
    large_trade_notional: float

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["last_trade_time"] = self.last_trade_time.isoformat() if self.last_trade_time else None
        return payload


@dataclass(frozen=True)
class HyperliquidAssetContext:
    coin: str
    dex: str
    requested_coin: str
    received_at: datetime
    mid_px: float | None
    mark_px: float | None
    oracle_px: float | None
    best_bid: float | None
    best_ask: float | None
    bid_size: float | None
    ask_size: float | None
    funding: float | None
    open_interest: float | None
    day_notional_volume: float | None
    premium: float | None
    premium_bps: float | None
    book_imbalance: float | None
    trade_stats: HyperliquidTradeStats
    symbol_warning: str | None = None
    raw_context: Mapping[str, Any] | None = None

    def to_dict(self, *, include_raw: bool = False) -> dict[str, Any]:
        payload = {
            "coin": self.coin,
            "dex": self.dex,
            "requested_coin": self.requested_coin,
            "received_at": self.received_at.isoformat(),
            "mid_px": self.mid_px,
            "mark_px": self.mark_px,
            "oracle_px": self.oracle_px,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "funding": self.funding,
            "open_interest": self.open_interest,
            "day_notional_volume": self.day_notional_volume,
            "premium": self.premium,
            "premium_bps": self.premium_bps,
            "book_imbalance": self.book_imbalance,
            "trade_stats": self.trade_stats.to_dict(),
            "symbol_warning": self.symbol_warning,
        }
        if include_raw:
            payload["raw_context"] = self.raw_context
        return payload


class HyperliquidClient:
    def __init__(self, settings: HyperliquidSettings) -> None:
        self.settings = settings

    def info(self, payload: Mapping[str, Any]) -> Any:
        url = urljoin(self.settings.api_base_url.rstrip("/") + "/", "info")
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = Request(
            url,
            data=body,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else None


def normalize_alias_key(value: str) -> str:
    return value.strip().upper().replace(" ", "")


def resolve_market(requested_coin: str, requested_dex: str) -> tuple[str, str]:
    normalized = normalize_alias_key(requested_coin)
    if normalized in COIN_ALIASES:
        return COIN_ALIASES[normalized]
    if ":" in requested_coin and not requested_dex:
        return requested_coin.split(":", 1)[0], requested_coin
    return requested_dex, requested_coin


def with_dex(payload: Mapping[str, Any], dex: str) -> dict[str, Any]:
    result = dict(payload)
    if dex:
        result["dex"] = dex
    return result


def first_present(*values: float | None) -> float | None:
    for value in values:
        if value is not None and value > 0:
            return value
    return None


def post_error(exc: BaseException) -> str:
    if isinstance(exc, HTTPError):
        try:
            body = exc.read().decode("utf-8")[:500]
        except Exception:  # noqa: BLE001
            body = ""
        return f"HTTP {exc.code}: {exc.reason}; {body}".strip()
    return str(exc)


def find_asset_context(
    meta_and_contexts: Any,
    coin: str,
) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]:
    if not isinstance(meta_and_contexts, list) or len(meta_and_contexts) < 2:
        return None, None
    meta = meta_and_contexts[0] if isinstance(meta_and_contexts[0], Mapping) else {}
    contexts = meta_and_contexts[1] if isinstance(meta_and_contexts[1], list) else []
    universe = meta.get("universe") if isinstance(meta.get("universe"), list) else []
    for index, asset in enumerate(universe):
        if not isinstance(asset, Mapping):
            continue
        if str(asset.get("name", "")).upper() == coin.upper():
            context = contexts[index] if index < len(contexts) and isinstance(contexts[index], Mapping) else None
            return asset, context
    return None, None


def parse_levels(book: Any, *, depth: int) -> tuple[float | None, float | None, float | None, float | None, float | None]:
    if not isinstance(book, Mapping):
        return None, None, None, None, None
    levels = book.get("levels")
    if not isinstance(levels, list) or len(levels) < 2:
        return None, None, None, None, None

    bids = [level for level in levels[0] if isinstance(level, Mapping)]
    asks = [level for level in levels[1] if isinstance(level, Mapping)]
    bid_prices = [(clean_float(level.get("px")), clean_float(level.get("sz"))) for level in bids]
    ask_prices = [(clean_float(level.get("px")), clean_float(level.get("sz"))) for level in asks]
    bid_prices = [(price, size) for price, size in bid_prices if price is not None and size is not None]
    ask_prices = [(price, size) for price, size in ask_prices if price is not None and size is not None]

    best_bid, bid_size = max(bid_prices, key=lambda item: item[0]) if bid_prices else (None, None)
    best_ask, ask_size = min(ask_prices, key=lambda item: item[0]) if ask_prices else (None, None)
    bid_depth = sum(size for _, size in bid_prices[:depth])
    ask_depth = sum(size for _, size in ask_prices[:depth])
    denominator = bid_depth + ask_depth
    imbalance = (bid_depth - ask_depth) / denominator if denominator > 0 else None
    return best_bid, best_ask, bid_size, ask_size, imbalance


def recent_trade_stats(
    trades: Any,
    *,
    large_trade_notional_threshold: float,
) -> HyperliquidTradeStats:
    if not isinstance(trades, list):
        trades = []

    parsed: list[tuple[float, float, datetime | None, str]] = []
    for trade in trades:
        if not isinstance(trade, Mapping):
            continue
        price = clean_float(trade.get("px"))
        size = clean_float(trade.get("sz"))
        if price is None or size is None:
            continue
        trade_time = parse_timestamp(trade.get("time"))
        side = str(trade.get("side", "")).upper()
        parsed.append((price, size, trade_time, side))

    if not parsed:
        return HyperliquidTradeStats(
            last_price=None,
            last_size=None,
            last_trade_time=None,
            trade_count=0,
            buy_notional=0.0,
            sell_notional=0.0,
            large_trade_count=0,
            large_trade_notional=0.0,
        )

    parsed.sort(key=lambda item: item[2] or datetime.min.replace(tzinfo=timezone.utc))
    last_price, last_size, last_time, _ = parsed[-1]
    buy_notional = 0.0
    sell_notional = 0.0
    large_count = 0
    large_notional = 0.0
    for price, size, _, side in parsed:
        notional = price * size
        if side == "B":
            buy_notional += notional
        elif side == "A":
            sell_notional += notional
        if notional >= large_trade_notional_threshold:
            large_count += 1
            large_notional += notional

    return HyperliquidTradeStats(
        last_price=last_price,
        last_size=last_size,
        last_trade_time=last_time,
        trade_count=len(parsed),
        buy_notional=buy_notional,
        sell_notional=sell_notional,
        large_trade_count=large_count,
        large_trade_notional=large_notional,
    )


def build_asset_context(
    *,
    coin: str,
    dex: str,
    requested_coin: str,
    all_mids: Any,
    meta_and_contexts: Any,
    book: Any,
    trades: Any,
    received_at: datetime,
    book_depth_levels: int,
    large_trade_notional_threshold: float,
) -> HyperliquidAssetContext:
    _, asset_context = find_asset_context(meta_and_contexts, coin)
    mid_px = clean_float(all_mids.get(coin)) if isinstance(all_mids, Mapping) else None
    mark_px = clean_float(asset_context.get("markPx")) if asset_context else None
    oracle_px = clean_float(asset_context.get("oraclePx")) if asset_context else None
    funding = clean_float(asset_context.get("funding")) if asset_context else None
    open_interest = clean_float(asset_context.get("openInterest")) if asset_context else None
    day_notional_volume = clean_float(asset_context.get("dayNtlVlm")) if asset_context else None
    best_bid, best_ask, bid_size, ask_size, imbalance = parse_levels(book, depth=book_depth_levels)
    trade_stats = recent_trade_stats(
        trades,
        large_trade_notional_threshold=large_trade_notional_threshold,
    )
    mark_or_mid = first_present(mark_px, mid_px)
    premium = mark_or_mid - oracle_px if mark_or_mid is not None and oracle_px is not None else None
    premium_bps = premium / oracle_px * 10_000.0 if premium is not None and oracle_px else None
    symbol_warning = infer_symbol_warning(coin, first_present(mark_px, mid_px, oracle_px))
    return HyperliquidAssetContext(
        coin=coin,
        dex=dex,
        requested_coin=requested_coin,
        received_at=received_at,
        mid_px=mid_px,
        mark_px=mark_px,
        oracle_px=oracle_px,
        best_bid=best_bid,
        best_ask=best_ask,
        bid_size=bid_size,
        ask_size=ask_size,
        funding=funding,
        open_interest=open_interest,
        day_notional_volume=day_notional_volume,
        premium=premium,
        premium_bps=premium_bps,
        book_imbalance=imbalance,
        trade_stats=trade_stats,
        symbol_warning=symbol_warning,
        raw_context=asset_context,
    )


def infer_symbol_warning(coin: str, price: float | None) -> str | None:
    if coin.upper() == "SPX" and price is not None and price < 100.0:
        return (
            "Hyperliquid coin SPX is trading far below the S&P 500 index level; "
            "treat it as a Hyperliquid crypto/perp asset, not official Cboe SPX."
        )
    return None


def quote_from_context(context: HyperliquidAssetContext) -> Quote:
    instrument = InstrumentId(
        symbol=context.coin,
        instrument_type=InstrumentType.CRYPTO_PERP,
        provider_symbol=f"hyperliquid:{context.coin}",
        exchange=f"Hyperliquid:{context.dex}" if context.dex else "Hyperliquid",
        currency="USD",
    )
    price = first_present(context.mark_px, context.mid_px, context.trade_stats.last_price)
    quality = MarketDataQuality.LIVE if price is not None else MarketDataQuality.MISSING
    return Quote(
        instrument=instrument,
        provider=Provider.HYPERLIQUID,
        provider_symbol=f"hyperliquid:{context.coin}",
        received_at=context.received_at,
        quality=quality,
        bid=context.best_bid,
        ask=context.best_ask,
        last=context.trade_stats.last_price,
        mark=price,
        bid_size=context.bid_size,
        ask_size=context.ask_size,
        last_size=context.trade_stats.last_size,
        volume=context.day_notional_volume,
        open_interest=context.open_interest,
        quote_time=context.received_at,
        trade_time=context.trade_stats.last_trade_time,
        raw=context.to_dict(include_raw=True),
    )


def provider_state_from_quote(
    quote: Quote,
    *,
    checked_at: datetime,
    latency_ms: float,
    reason: str | None = None,
) -> ProviderState:
    if quote.is_usable:
        status = ProviderStatus.AVAILABLE
    else:
        status = ProviderStatus.DEGRADED
        reason = reason or "Hyperliquid quote missing usable price"
    return ProviderState(
        provider=Provider.HYPERLIQUID,
        status=status,
        checked_at=checked_at,
        reason=reason,
        connected=True,
        authenticated=None,
        latency_ms=latency_ms,
        priority=2,
    )


def unavailable_state(*, checked_at: datetime, latency_ms: float, reason: str) -> ProviderState:
    return ProviderState(
        provider=Provider.HYPERLIQUID,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=checked_at,
        reason=reason,
        connected=False,
        authenticated=None,
        latency_ms=latency_ms,
        priority=2,
    )


def context_path(storage_settings: StorageSettings, context: HyperliquidAssetContext) -> Path:
    timestamp = as_utc(context.received_at)
    return (
        Path(storage_settings.data_root)
        / "context"
        / "provider=hyperliquid"
        / f"dex={context.dex or 'default'}"
        / f"coin={context.coin}"
        / f"date={timestamp.strftime('%Y-%m-%d')}"
        / f"hour={timestamp.strftime('%H')}"
        / "asset-context.jsonl"
    )


def write_context(
    storage_settings: StorageSettings,
    context: HyperliquidAssetContext,
) -> Path:
    path = context_path(storage_settings, context)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(context.to_dict(), sort_keys=True, separators=(",", ":")))
        handle.write("\n")
    return path


def collect_once(
    client: HyperliquidClient,
    settings: HyperliquidSettings,
    *,
    coin: str,
    dex: str,
    requested_coin: str,
) -> tuple[HyperliquidAssetContext, Quote, ProviderState, float]:
    start = time.perf_counter()
    all_mids = client.info(with_dex({"type": "allMids"}, dex))
    meta_and_contexts = client.info(with_dex({"type": "metaAndAssetCtxs"}, dex))
    book = client.info(with_dex({"type": "l2Book", "coin": coin}, dex)) if settings.include_book else None
    trades = (
        client.info(with_dex({"type": "recentTrades", "coin": coin}, dex))
        if settings.include_trades
        else None
    )
    received_at = datetime.now(tz=timezone.utc)
    context = build_asset_context(
        coin=coin,
        dex=dex,
        requested_coin=requested_coin,
        all_mids=all_mids,
        meta_and_contexts=meta_and_contexts,
        book=book,
        trades=trades,
        received_at=received_at,
        book_depth_levels=settings.book_depth_levels,
        large_trade_notional_threshold=settings.large_trade_notional_threshold,
    )
    quote = quote_from_context(context)
    latency_ms = (time.perf_counter() - start) * 1000.0
    state = provider_state_from_quote(
        quote,
        checked_at=received_at,
        latency_ms=latency_ms,
        reason=context.symbol_warning,
    )
    return context, quote, state, latency_ms


def list_coins(client: HyperliquidClient, *, dex: str) -> list[str]:
    all_mids = client.info(with_dex({"type": "allMids"}, dex))
    if isinstance(all_mids, Mapping):
        return sorted(str(coin) for coin in all_mids)
    return []


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect one Hyperliquid public SPX perp snapshot.")
    parser.add_argument("--coin", help="Hyperliquid perp coin, default from HYPERLIQUID_COIN.")
    parser.add_argument("--dex", help="Hyperliquid perp dex, default from HYPERLIQUID_DEX.")
    parser.add_argument("--print-config", action="store_true")
    parser.add_argument("--list-coins", action="store_true", help="Print available allMids coin names.")
    parser.add_argument("--skip-book", action="store_true")
    parser.add_argument("--skip-trades", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = HyperliquidSettings.from_env()
    if args.skip_book:
        settings = HyperliquidSettings(
            **(asdict(settings) | {"include_book": False})
        )
    if args.skip_trades:
        settings = HyperliquidSettings(
            **(asdict(settings) | {"include_trades": False})
        )
    storage_settings = StorageSettings.from_env()
    requested_coin = args.coin or settings.coin
    requested_dex = args.dex if args.dex is not None else settings.dex
    dex, coin = resolve_market(requested_coin, requested_dex)
    client = HyperliquidClient(settings)

    if args.print_config:
        print(
            json.dumps(
                {
                    "hyperliquid": asdict(settings),
                    "storage": asdict(storage_settings),
                    "coin": coin,
                    "dex": dex,
                    "requested_coin": requested_coin,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.list_coins:
        try:
            coins = list_coins(client, dex=dex)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            print(f"Failed to list Hyperliquid coins: {post_error(exc)}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps({"coins": coins}, indent=2, sort_keys=True))
        else:
            print("\n".join(coins))
        return 0

    started = time.perf_counter()
    try:
        context, quote, state, _ = collect_once(
            client,
            settings,
            coin=coin,
            dex=dex,
            requested_coin=requested_coin,
        )
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        checked_at = datetime.now(tz=timezone.utc)
        state = unavailable_state(
            checked_at=checked_at,
            latency_ms=(time.perf_counter() - started) * 1000.0,
            reason=post_error(exc),
        )
        write_result = persist_provider_snapshot(
            ProviderSnapshot.from_state(Provider.HYPERLIQUID, state, received_at=checked_at),
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
            print(f"Hyperliquid unavailable: {state.reason}")
        return 1

    context_file = write_context(storage_settings, context)
    snapshot = ProviderSnapshot(
        provider=Provider.HYPERLIQUID,
        received_at=context.received_at,
        quotes=(quote,),
        provider_states=(state,),
        metadata={"context_path": str(context_file)},
    )
    write_result = persist_provider_snapshot(snapshot, storage_settings)
    summary = {
        "provider_state": state.to_dict(),
        "quote": quote.to_dict(),
        "context": context.to_dict(),
        "raw_paths": write_result.raw_paths,
        "context_path": str(context_file),
        "latest_state": write_result.latest_state,
    }
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(f"Hyperliquid {coin}: {state.status.value}")
        print(f"price={quote.effective_price} bid={quote.bid} ask={quote.ask}")
        print(
            "context: "
            f"oracle={context.oracle_px} funding={context.funding} "
            f"oi={context.open_interest} premium_bps={context.premium_bps}"
        )
        print(f"raw: {next(iter(write_result.raw_paths))}")
        print(f"context: {context_file}")
        print(f"latest: {write_result.latest_state}")
    return 0 if state.status != ProviderStatus.UNAVAILABLE else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
