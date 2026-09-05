"""DuckDB/parquet quote-lake access for the 0DTE level backtest."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json
import math

import duckdb

from .odte_level_signals import (
    DELTA_MAX,
    DELTA_MIN,
    DELTA_TARGET,
    MAX_ENTRY_QUOTE_AGE,
    PROVIDERS,
    OptionTick,
    UnderlierTick,
)

KNOWLEDGE_TIME_GUARD_SQL = """
    received_at IS NOT NULL
    AND source_at IS NOT NULL
    AND quality = 'live'
    AND lower(coalesce(market_data_type, '')) IN ('live', '1')
    AND source_at >= received_at - INTERVAL '30 seconds'
    AND source_at <= received_at + INTERVAL '5 seconds'
    AND (
        quote_time IS NULL
        OR quote_time <= received_at + INTERVAL '5 seconds'
    )
"""


class QuoteStore:
    """DuckDB-backed quote loader over the parquet lake with in-memory caching."""

    def __init__(self, data_root: Path) -> None:
        self._root = Path(data_root)
        self._con = duckdb.connect()
        self._con.execute("SET TimeZone='UTC'")
        self._con.execute("SET enable_progress_bar=false")
        self._options: dict[tuple, list[OptionTick]] = {}
        self._underlier: dict[tuple, list[UnderlierTick]] = {}
        self._snapshot_only = False

    def close(self) -> None:
        self._con.close()

    def write_snapshot(self, path: Path) -> str:
        """Freeze the exact received/source-time series used by this research run."""
        digest = hashlib.sha256()
        with path.open("wb") as handle:
            for key, ticks in sorted(self._options.items(), key=lambda item: str(item[0])):
                row = {"provider": key[1], "expiry": key[2], "strike": key[3], "right": key[4],
                       "start": key[5], "end": key[6], "ticks": [tick._asdict() for tick in ticks]}
                data = (json.dumps(row, sort_keys=True, default=str) + "\n").encode()
                digest.update(data)
                handle.write(data)
        return digest.hexdigest()

    def load_snapshot(self, path: Path) -> None:
        """Restore captured inputs; a missing series cannot fall back to a mutable lake."""
        self._options.clear()
        self._snapshot_only = True
        with path.open() as handle:
            for line in handle:
                row = json.loads(line)
                start, end = (datetime.fromisoformat(row[key]) for key in ("start", "end"))
                key = ("opt", row["provider"], date.fromisoformat(row["expiry"]), row["strike"],
                       row["right"], start, end, self._day_hours(start, end))
                self._options[key] = [OptionTick(
                    datetime.fromisoformat(tick["at"]), tick["bid"], tick["ask"], tick["mid"],
                    datetime.fromisoformat(tick["source_at"]) if tick["source_at"] else None,
                ) for tick in row["ticks"]]

    @staticmethod
    def _day_hours(start: datetime, end: datetime) -> tuple[tuple[date, tuple[str, ...]], ...]:
        """Split a window into (partition date, UTC hour strings) hive filters."""
        start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
        parts: list[tuple[date, tuple[str, ...]]] = []
        day = start.date()
        while day <= end.date():
            day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
            day_end = day_start + timedelta(days=1) - timedelta(microseconds=1)
            cursor = max(start, day_start).replace(minute=0, second=0, microsecond=0)
            hours: set[str] = set()
            while cursor <= min(end, day_end):
                hours.add(cursor.strftime("%H"))
                cursor += timedelta(hours=1)
            parts.append((day, tuple(sorted(hours))))
            day += timedelta(days=1)
        return tuple(parts)

    def _glob(self, day: date, provider: str) -> str:
        return str(
            self._root
            / "lake/quotes/schema=v1"
            / f"date={day.isoformat()}"
            / f"provider={provider}/hour=*/quotes.parquet"
        )

    def option_series(
        self,
        *,
        provider: str,
        expiry: date,
        strike: float,
        right: str,
        start: datetime,
        end: datetime,
    ) -> list[OptionTick]:
        """SPXW BBO ticks and invalidations in the knowledge-time window.

        The lake's source_at may fall back to a trade or receipt timestamp; it
        cannot certify the age of an option bid/ask. Invalid arrivals retain a
        null BBO so consumers cannot carry the previous live price through them.
        Legacy Schwab REST rows without a feed mode have request-start receipt
        clocks; they cannot invalidate the separately observed streaming book.
        """
        windows = self._day_hours(start, end)
        key = ("opt", provider, expiry, strike, right, start, end, windows)
        if key in self._options:
            return self._options[key]
        for cached, ticks in self._options.items():
            if cached[1:5] == key[1:5] and cached[5] <= start and cached[6] >= end:
                return [tick for tick in ticks if start <= tick.at <= end]
        if self._snapshot_only:
            raise ValueError("quote_snapshot_series_unavailable")
        ticks: list[OptionTick] = []
        for day, hours in windows:
            hour_list = ",".join(f"'{hour}'" for hour in hours)
            query = (
                "SELECT received_at, bid, ask, mid, quote_time, "
                f"coalesce(({KNOWLEDGE_TIME_GUARD_SQL}) "
                "AND quote_time IS NOT NULL AND quote_time<=received_at "
                "AND quote_time>=received_at-INTERVAL '30 seconds' "
                "AND isfinite(bid) AND isfinite(ask) AND bid>=0 AND ask>=bid AND ask>0, false) "
                "FROM read_parquet(?, hive_partitioning=true) "
                "WHERE trading_class='SPXW' AND expiry=? AND strike=? "
                "AND NOT (provider='schwab' AND market_data_type IS NULL) "
                f'AND "right"=? AND hour IN ({hour_list}) '
                "AND received_at BETWEEN ? AND ? ORDER BY received_at, source_at"
            )
            try:
                rows = self._con.execute(
                    query, [self._glob(day, provider), expiry, strike, right, start, end]
                ).fetchall()
            except duckdb.IOException:
                continue  # missing partition (provider gap or holiday)
            ticks.extend(OptionTick(row[0], row[1], row[2], row[3], row[4]) if row[5]
                         else OptionTick(row[0], None, None, None, None) for row in rows)
        ticks.sort(key=lambda tick: tick.at)
        self._options[key] = ticks
        return ticks

    def underlier_series(
        self, *, instrument_id: str, start: datetime, end: datetime
    ) -> list[UnderlierTick]:
        """Underlier ticks; price = COALESCE(mid, last, effective_price).

        schwab populates ``mid`` for index:SPX while ibkr leaves it NULL but fills
        ``last``/``effective_price``. Generic future:ES also resolves dated IBKR
        contracts. Freeze its first usable provider/contract for this window so
        provider basis and a futures roll cannot create artificial price moves.
        Other instruments retain the existing provider-merged behavior.
        """
        windows = self._day_hours(start, end)
        key = ("und", instrument_id, start, end, windows)
        if key in self._underlier:
            return self._underlier[key]
        observations = []
        for day, hours in windows:
            hour_list = ",".join(f"'{hour}'" for hour in hours)
            glob = str(
                self._root
                / "lake/quotes/schema=v1"
                / f"date={day.isoformat()}"
                / "provider=*/hour=*/quotes.parquet"
            )
            query = (
                "SELECT received_at, COALESCE(mid, last, effective_price), provider, instrument_id "
                "FROM read_parquet(?, hive_partitioning=true) "
                "WHERE (instrument_id=? OR (?='future:ES' AND starts_with(instrument_id,'future:ES:'))) "
                f"AND {KNOWLEDGE_TIME_GUARD_SQL} AND source_at<=received_at "
                f"AND hour IN ({hour_list}) "
                "AND received_at BETWEEN ? AND ? ORDER BY received_at, source_at"
            )
            try:
                rows = self._con.execute(query, [glob, instrument_id, instrument_id, start, end]).fetchall()
            except duckdb.IOException:
                continue
            observations.extend(
                row
                for row in rows
                if row[0] is not None and row[1] is not None and math.isfinite(row[1]) and row[1]>0
            )
        observations.sort(key=lambda row: (row[0], row[2], row[3]))
        selected = observations[0][2:] if observations and instrument_id=="future:ES" else None
        ticks = [UnderlierTick(at=row[0], price=row[1]) for row in observations
                 if selected is None or row[2:]==selected]
        self._underlier[key] = ticks
        return ticks

    def select_delta_strike(
        self,
        *,
        expiry: date,
        right: str,
        t0: datetime,
        delta_min: float = DELTA_MIN,
        delta_max: float = DELTA_MAX,
        delta_target: float = DELTA_TARGET,
    ) -> float | None:
        """Production strike rule: delta in [delta_min, delta_max] closest to target."""
        # Delta selection is point-in-time: only rows received at or before the
        # decision may select a strike. The one-minute lookback tolerates a
        # quiet contract without admitting a future chain snapshot.
        start, end = t0 - timedelta(seconds=60), t0
        nearest: dict[tuple[str, float], tuple[float, float]] = {}
        for day, hours in self._day_hours(start, end):
            hour_list = ",".join(f"'{hour}'" for hour in hours)
            glob = str(
                self._root
                / "lake/quotes/schema=v1"
                / f"date={day.isoformat()}"
                / "provider=*/hour=*/quotes.parquet"
            )
            query = (
                "SELECT provider, strike, delta, received_at "
                "FROM read_parquet(?, hive_partitioning=true) "
                "WHERE trading_class='SPXW' AND expiry=? "
                f'AND "right"=? AND delta IS NOT NULL AND hour IN ({hour_list}) '
                f"AND {KNOWLEDGE_TIME_GUARD_SQL} "
                "AND received_at BETWEEN ? AND ?"
            )
            try:
                rows = self._con.execute(query, [glob, expiry, right, start, end]).fetchall()
            except duckdb.IOException:
                continue
            for provider, strike, delta, received_at in rows:
                distance = (t0 - received_at).total_seconds()
                slot = (provider, strike)
                if slot not in nearest or distance < nearest[slot][0]:
                    nearest[slot] = (distance, delta)
        candidates = [
            (abs(abs(delta) - delta_target), strike)
            for (_, strike), (_, delta) in nearest.items()
            if delta_min <= abs(delta) <= delta_max
        ]
        if not candidates:
            return None
        return min(candidates)[1]


def pick_provider(
    store: QuoteStore,
    *,
    expiry: date,
    strike: float,
    right: str,
    t0: datetime,
    quote_side: str = "ask",
) -> str | None:
    """Pick the provider with the earliest executable entry quote.

    Provider choice is made solely from the entry window; later path coverage
    cannot influence it. ``quote_side`` is ``ask`` for a bought leg and ``bid``
    for a sold leg.
    """
    if quote_side not in {"ask", "bid"}:
        raise ValueError("quote_side must be 'ask' or 'bid'")
    end = t0 + MAX_ENTRY_QUOTE_AGE
    candidates: list[tuple[datetime, int, str]] = []
    for provider in PROVIDERS:
        series = store.option_series(
            provider=provider,
            expiry=expiry,
            strike=strike,
            right=right,
            start=t0,
            end=end,
        )
        executable = next(
            (
                tick
                for tick in series
                if tick.at >= t0
                and getattr(tick, quote_side) is not None
                and getattr(tick, quote_side) > 0
            ),
            None,
        )
        if executable is not None:
            candidates.append((executable.at, PROVIDERS.index(provider), provider))
    return min(candidates)[2] if candidates else None
