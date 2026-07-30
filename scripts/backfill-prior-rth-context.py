#!/usr/bin/env python3
"""Backfill the durable prior-RTH context from compacted Schwab quotes."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from spx_spark.application.market_features.prior_rth_context import (
    build_prior_rth_context,
    prior_rth_context_path,
)
from spx_spark.application.market_features.state import save_json
from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill the completed RTH path used by the next GTH session."
    )
    parser.add_argument(
        "--as-of",
        help="ISO timestamp whose preceding RTH session should be built; defaults to now.",
    )
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = (
        as_utc(datetime.fromisoformat(args.as_of))
        if args.as_of
        else datetime.now(tz=timezone.utc)
    )
    storage = StorageSettings.from_env()
    data_root = Path(storage.data_root)
    trading_date = DEFAULT_MARKET_CALENDAR.research_expiry(now)
    prior_date = DEFAULT_MARKET_CALENDAR.previous_trading_day(trading_date)
    session = DEFAULT_MARKET_CALENDAR.session(prior_date)
    if session is None:
        raise SystemExit(f"no RTH session for {prior_date.isoformat()}")

    quote_glob = (
        data_root
        / "lake"
        / "quotes"
        / "schema=v1"
        / f"date={prior_date.isoformat()}"
        / "provider=schwab"
        / "hour=*"
        / "quotes.parquet"
    )
    next_glob = (
        data_root
        / "lake"
        / "quotes"
        / "schema=v1"
        / f"date={trading_date.isoformat()}"
        / "provider=schwab"
        / "hour=*"
        / "quotes.parquet"
    )
    con = duckdb.connect()
    rows = con.execute(
        """
        WITH minute_quotes AS (
          SELECT
            date_trunc('minute', quote_time) AS minute_at,
            arg_max(effective_price, quote_time) AS price,
            arg_max(close, quote_time) AS reference_close
          FROM read_parquet(?)
          WHERE instrument_id = 'index:SPX'
            AND quote_time >= ?
            AND quote_time <= ?
            AND effective_price > 0
          GROUP BY minute_at
        )
        SELECT minute_at, price, reference_close
        FROM minute_quotes
        ORDER BY minute_at
        """,
        [
            str(quote_glob),
            session.open_at,
            session.close_at,
        ],
    ).fetchall()
    if not rows:
        raise SystemExit(f"no compacted SPX quotes found under {quote_glob}")

    official_close = None
    if list(next_glob.parents[1].glob("hour=*/quotes.parquet")):
        official_close = con.execute(
            """
            SELECT arg_min(close, quote_time)
            FROM read_parquet(?)
            WHERE instrument_id = 'index:SPX' AND close > 0
            """,
            [str(next_glob)],
        ).fetchone()[0]
    samples = [
        {
            "at": at.isoformat(),
            "instruments": {
                "index:SPX": {
                    "price": float(price),
                    "reference_close": (
                        float(reference_close)
                        if reference_close is not None
                        else None
                    ),
                }
            },
        }
        for at, price, reference_close in rows
    ]
    context = build_prior_rth_context(
        samples,
        now=now,
        official_close=(
            float(official_close) if official_close is not None else None
        ),
    )
    if context.get("status") != "ready":
        raise SystemExit(
            "prior RTH context failed validation: "
            + ",".join(str(item) for item in context.get("reasons") or ())
        )
    path = prior_rth_context_path(data_root)
    save_json(path, context)
    payload = {"ok": True, "path": str(path), "context": context}
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True) if args.json else path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
