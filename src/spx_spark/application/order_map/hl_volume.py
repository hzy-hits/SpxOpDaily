"""Hyperliquid volume context helpers for order-map payloads."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.models import HL_SP500_PROXY_ID
from spx_spark.application.order_map.spot import hyperliquid_sp500_price
from spx_spark.application.order_map.volume_machine import (
    ES_VOLUME_ELEVATED_RATIO,
    ES_VOLUME_MAX_WINDOW_MINUTES,
    ES_VOLUME_MIN_WINDOW_MINUTES,
    ES_VOLUME_QUIET_RATIO,
    _parse_sample,
    load_es_volume_samples,
    save_es_volume_state,
)
from spx_spark.config import StorageSettings
from spx_spark.storage import LatestState

HL_VOLUME_MAX_QUOTE_AGE_SECONDS = 900.0


def default_hl_volume_sample_path(settings: StorageSettings) -> str:
    return os.getenv("SPX_HL_VOLUME_SAMPLE_PATH") or str(
        Path(settings.data_root) / "latest" / "hl_volume_samples.json"
    )


def _latest_hl_context(settings: StorageSettings, now: datetime) -> dict[str, Any] | None:
    """Last Hyperliquid asset-context record; carries the aggressor buy/sell
    split and book imbalance that the latest-state quote drops."""
    base = (
        Path(settings.data_root) / "context" / "provider=hyperliquid" / "dex=xyz" / "coin=xyz:SP500"
    )
    for offset_hours in (0, 1):
        stamp = (now - timedelta(hours=offset_hours)).astimezone(timezone.utc)
        path = (
            base
            / f"date={stamp.strftime('%Y-%m-%d')}"
            / f"hour={stamp.strftime('%H')}"
            / "asset-context.jsonl"
        )
        try:
            last = ""
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        last = line
            if last:
                record = json.loads(last)
                if isinstance(record, dict):
                    return record
        except (OSError, json.JSONDecodeError):
            continue
    return None


def hl_volume_signal(
    cumulative: float | None,
    samples: list[dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any] | None:
    """Pace signal from the HL SP500 perp rolling-24h notional volume."""
    if cumulative is None or cumulative <= 0:
        return None
    signal: dict[str, Any] = {
        "cumulative_notional": round(cumulative),
        "delta_notional": None,
        "window_minutes": None,
        "recent_pace_per_min": None,
        "baseline_pace_per_min": None,
        "pace_ratio": None,
        "label": "no_baseline",
        "basis": "rolling_24h_notional",
    }
    points = [parsed for sample in samples if (parsed := _parse_sample(sample)) is not None]
    points.sort(key=lambda item: item[0])
    if not points:
        return signal
    last_at, last_volume, _last_price = points[-1]
    window_minutes = (now - last_at).total_seconds() / 60.0
    if not (ES_VOLUME_MIN_WINDOW_MINUTES <= window_minutes <= ES_VOLUME_MAX_WINDOW_MINUTES):
        return signal
    # Rolling window: a decline means the 24h-ago tail outweighs fresh prints;
    # clamp to zero, which honestly reads as "quiet now".
    delta = max(0.0, cumulative - last_volume)
    recent_pace = delta / window_minutes

    history: list[float] = []
    for (prev_at, prev_volume, _), (cur_at, cur_volume, _) in zip(points, points[1:]):
        minutes = (cur_at - prev_at).total_seconds() / 60.0
        if not (ES_VOLUME_MIN_WINDOW_MINUTES <= minutes <= ES_VOLUME_MAX_WINDOW_MINUTES):
            continue
        history.append(max(0.0, cur_volume - prev_volume) / minutes)
    if len(history) < 2:
        signal.update({"delta_notional": round(delta), "window_minutes": round(window_minutes, 1)})
        return signal
    ordered = sorted(history)
    mid = len(ordered) // 2
    baseline = ordered[mid] if len(ordered) % 2 == 1 else (ordered[mid - 1] + ordered[mid]) / 2.0
    if baseline <= 0:
        signal.update({"delta_notional": round(delta), "window_minutes": round(window_minutes, 1)})
        return signal
    ratio = recent_pace / baseline
    if ratio >= ES_VOLUME_ELEVATED_RATIO:
        label = "elevated"
    elif ratio <= ES_VOLUME_QUIET_RATIO:
        label = "quiet"
    else:
        label = "normal"
    signal.update(
        {
            "delta_notional": round(delta),
            "window_minutes": round(window_minutes, 1),
            "recent_pace_per_min": round(recent_pace),
            "baseline_pace_per_min": round(baseline),
            "pace_ratio": round(ratio, 2),
            "label": label,
        }
    )
    return signal


def attach_hl_volume_signal(
    payload: dict[str, Any],
    state: LatestState,
    *,
    storage_settings: StorageSettings,
    sample_path: str,
    now: datetime,
    persist: bool = True,
) -> None:
    quote = state.best_quote(HL_SP500_PROXY_ID)
    cumulative = finite_float(quote.volume) if quote is not None else None
    age_ms = quote.quote_age_ms(now) if quote is not None else None
    if age_ms is not None and age_ms > HL_VOLUME_MAX_QUOTE_AGE_SECONDS * 1000.0:
        cumulative = None

    samples = load_es_volume_samples(sample_path)  # same {at, volume, price} schema
    signal = hl_volume_signal(cumulative, samples, now=now)
    if signal is not None:
        context = _latest_hl_context(storage_settings, now)
        if context:
            trade_stats = (
                context.get("trade_stats") if isinstance(context.get("trade_stats"), dict) else {}
            )
            buy = finite_float(trade_stats.get("buy_notional")) or 0.0
            sell = finite_float(trade_stats.get("sell_notional")) or 0.0
            if buy + sell > 0:
                signal["aggressor_buy_ratio"] = round(buy / (buy + sell), 2)
            book_imbalance = finite_float(context.get("book_imbalance"))
            if book_imbalance is not None:
                signal["book_imbalance"] = round(book_imbalance, 2)
    payload["hl_volume"] = signal

    if persist and cumulative is not None:
        sample: dict[str, Any] = {"at": now.isoformat(), "volume": cumulative}
        price = hyperliquid_sp500_price(state)
        if price is not None:
            sample["price"] = price
        samples.append(sample)
        save_es_volume_state(sample_path, samples)
