"""Fallback normalized ES samples seeded from the durable trend state."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from spx_spark.application.market_features.market import session_segment
from spx_spark.marketdata import as_utc
from spx_spark.settings.market_features import MarketFeatureSettings


def seed_samples_from_trend(
    trend: dict[str, Any],
    policy: MarketFeatureSettings,
) -> list[dict[str, Any]]:
    session_id = str(trend.get("session_id") or "").split(":", 1)[0]
    rows: list[dict[str, Any]] = []
    for item in trend.get("samples") or []:
        if not isinstance(item, dict):
            continue
        at = item.get("at")
        price = item.get("price")
        if not isinstance(at, str) or not isinstance(price, int | float):
            continue
        try:
            observed_at = as_utc(datetime.fromisoformat(at))
        except ValueError:
            continue
        rows.append(
            {
                "at": observed_at.isoformat(),
                "session_id": session_id,
                "segment": session_segment(observed_at, policy=policy),
                "instruments": {
                    "future:ES": {
                        "price": float(price),
                        "provider": item.get("provider"),
                        "source_at": item.get("source_at") or at,
                        "volume": None,
                        "quality": "live",
                    }
                },
                "es_by_provider": {},
            }
        )
    return rows


__all__ = ["seed_samples_from_trend"]
