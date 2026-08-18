"""DuckDB/parquet quote-lake access for the 0DTE level backtest."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
    choose_best_quote,
)
from spx_spark.storage import LatestState

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
        self._options: dict[tuple, list[OptionTick]] = {}
        self._underlier: dict[tuple, list[UnderlierTick]] = {}
        self._option_window_bounds: tuple[date, float, float, datetime, datetime] | None = None
        self._option_window: dict[tuple[str, float, str], list[OptionTick]] = {}

    def close(self) -> None:
        self._con.close()

    @staticmethod
    def _day_hours(start: datetime, end: datetime) -> tuple[tuple[date, tuple[str, ...]], ...]:
        """Split a window into (partition date, UTC hour strings) hive filters."""
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
        """All received ticks for one SPXW contract in the knowledge-time window."""
        if self._option_window_covers(
            expiry=expiry,
            strike=strike,
            start=start,
            end=end,
        ):
            ticks = self._option_window.get((provider, float(strike), right), ())
            left = bisect_left(ticks, start, key=lambda tick: tick.at)
            right_index = bisect_right(ticks, end, key=lambda tick: tick.at)
            return list(ticks[left:right_index])
        windows = self._day_hours(start, end)
        key = ("opt", provider, expiry, strike, right, start, end, windows)
        if key in self._options:
            return self._options[key]
        ticks: list[OptionTick] = []
        for day, hours in windows:
            hour_list = ",".join(f"'{hour}'" for hour in hours)
            query = (
                "SELECT received_at, bid, ask, mid, source_at, delta "
                "FROM read_parquet(?, hive_partitioning=true) "
                "WHERE trading_class='SPXW' AND expiry=? AND strike=? "
                f"AND {KNOWLEDGE_TIME_GUARD_SQL} "
                f'AND "right"=? AND hour IN ({hour_list}) '
                "AND received_at BETWEEN ? AND ? ORDER BY received_at, source_at"
            )
            try:
                rows = self._con.execute(
                    query, [self._glob(day, provider), expiry, strike, right, start, end]
                ).fetchall()
            except duckdb.IOException:
                continue  # missing partition (provider gap or holiday)
            ticks.extend(
                OptionTick(
                    at=row[0],
                    bid=row[1],
                    ask=row[2],
                    mid=row[3],
                    source_at=row[4],
                    delta=row[5],
                )
                for row in rows
            )
        ticks.sort(key=lambda tick: tick.at)
        self._options[key] = ticks
        return ticks

    def load_option_window(
        self,
        *,
        expiry: date,
        strike_min: float,
        strike_max: float,
        start: datetime,
        end: datetime,
    ) -> int:
        """Load one expiry/strike-band window for fast causal as-of snapshots."""

        if strike_min > strike_max:
            raise ValueError("strike_min must not exceed strike_max")
        grouped: defaultdict[tuple[str, float, str], list[OptionTick]] = defaultdict(list)
        for day, hours in self._day_hours(start, end):
            hour_list = ",".join(f"'{hour}'" for hour in hours)
            glob = str(
                self._root
                / "lake/quotes/schema=v1"
                / f"date={day.isoformat()}"
                / "provider=*/hour=*/quotes.parquet"
            )
            query = (
                'SELECT provider, strike, "right", received_at, bid, ask, mid, '
                "source_at, delta "
                "FROM read_parquet(?, hive_partitioning=true) "
                "WHERE trading_class='SPXW' AND expiry=? "
                "AND provider IN ('schwab', 'ibkr') "
                f"AND {KNOWLEDGE_TIME_GUARD_SQL} "
                f"AND \"right\" IN ('C', 'P') AND hour IN ({hour_list}) "
                "AND strike BETWEEN ? AND ? "
                "AND received_at BETWEEN ? AND ? "
                'ORDER BY provider, strike, "right", received_at, source_at'
            )
            try:
                rows = self._con.execute(
                    query,
                    [glob, expiry, strike_min, strike_max, start, end],
                ).fetchall()
            except duckdb.IOException:
                continue
            for provider, strike, right, received_at, bid, ask, mid, source_at, delta in rows:
                grouped[(str(provider), float(strike), str(right))].append(
                    OptionTick(
                        at=received_at,
                        bid=bid,
                        ask=ask,
                        mid=mid,
                        source_at=source_at,
                        delta=delta,
                    )
                )
        self._options.clear()
        self._option_window_bounds = (
            expiry,
            float(strike_min),
            float(strike_max),
            start,
            end,
        )
        self._option_window = dict(grouped)
        return sum(len(ticks) for ticks in grouped.values())

    def option_snapshot(
        self,
        *,
        expiry: date,
        as_of: datetime,
        max_age_seconds: float,
        strikes: Iterable[float] | None = None,
    ) -> Mapping[tuple[str, float, str], OptionTick]:
        """Latest causal tick per provider/contract from the loaded window."""

        bounds = self._option_window_bounds
        loaded_for_request = (
            bounds is not None
            and bounds[0] == expiry
            and bounds[3] <= as_of - timedelta(seconds=max_age_seconds)
            and as_of <= bounds[4]
        )
        if not loaded_for_request:
            if strikes is None:
                raise ValueError("strikes are required before an option window is loaded")
            wanted = tuple(float(strike) for strike in strikes)
            if not wanted:
                return {}
            self.load_option_window(
                expiry=expiry,
                strike_min=min(wanted),
                strike_max=max(wanted),
                start=as_of - timedelta(seconds=max_age_seconds),
                end=as_of,
            )
        cutoff = as_of - timedelta(seconds=max_age_seconds)
        wanted_strikes = None if strikes is None else {float(value) for value in strikes}
        result: dict[tuple[str, float, str], OptionTick] = {}
        for key, ticks in self._option_window.items():
            provider, strike, _right = key
            if provider not in PROVIDERS or (
                wanted_strikes is not None and strike not in wanted_strikes
            ):
                continue
            index = bisect_right(ticks, as_of, key=lambda tick: tick.at) - 1
            if index >= 0 and ticks[index].at >= cutoff:
                result[key] = ticks[index]
        return result

    def _option_window_covers(
        self,
        *,
        expiry: date,
        strike: float,
        start: datetime,
        end: datetime,
    ) -> bool:
        if self._option_window_bounds is None:
            return False
        cached_expiry, strike_min, strike_max, cached_start, cached_end = self._option_window_bounds
        return (
            expiry == cached_expiry
            and strike_min <= float(strike) <= strike_max
            and cached_start <= start
            and end <= cached_end
        )

    def underlier_series(
        self, *, instrument_id: str, start: datetime, end: datetime
    ) -> list[UnderlierTick]:
        """Provider-merged underlier ticks; price = COALESCE(mid, last, effective_price).

        schwab populates ``mid`` for index:SPX while ibkr leaves it NULL but fills
        ``last``/``effective_price``; future:ES has ``mid`` for both providers.
        """
        windows = self._day_hours(start, end)
        key = ("und", instrument_id, start, end, windows)
        if key in self._underlier:
            return self._underlier[key]
        ticks: list[UnderlierTick] = []
        for day, hours in windows:
            hour_list = ",".join(f"'{hour}'" for hour in hours)
            glob = str(
                self._root
                / "lake/quotes/schema=v1"
                / f"date={day.isoformat()}"
                / "provider=*/hour=*/quotes.parquet"
            )
            query = (
                "SELECT received_at, COALESCE(mid, last, effective_price) "
                "FROM read_parquet(?, hive_partitioning=true) "
                f"WHERE instrument_id=? AND {KNOWLEDGE_TIME_GUARD_SQL} "
                f"AND hour IN ({hour_list}) "
                "AND received_at BETWEEN ? AND ? ORDER BY received_at, source_at"
            )
            try:
                rows = self._con.execute(query, [glob, instrument_id, start, end]).fetchall()
            except duckdb.IOException:
                continue
            ticks.extend(
                UnderlierTick(at=row[0], price=row[1])
                for row in rows
                if row[0] is not None and row[1] is not None
            )
        ticks.sort(key=lambda tick: tick.at)
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


def latest_state_from_lake(
    store: QuoteStore,
    *,
    expiry: str,
    spot: float,
    trigger: float | None,
    decision_at: datetime,
    max_age_seconds: float = 20.0,
) -> LatestState | None:
    """Build a causal SPXW LatestState from one batched quote-lake window."""

    expiry_day = date.fromisoformat(expiry)
    anchors = (spot,) if trigger is None else (spot, trigger)
    centers = sorted(
        {
            round(value / 5.0) * 5.0
            for anchor in anchors
            for value in (
                anchor - 20.0,
                anchor - 10.0,
                anchor,
                anchor + 10.0,
                anchor + 20.0,
            )
        }
    )
    try:
        snapshot = store.option_snapshot(
            expiry=expiry_day,
            as_of=decision_at,
            max_age_seconds=max_age_seconds,
            strikes=None,
        )
    except ValueError:
        snapshot = store.option_snapshot(
            expiry=expiry_day,
            as_of=decision_at,
            max_age_seconds=max_age_seconds,
            strikes=centers,
        )
    quotes: list[Quote] = []
    for (provider_name, strike, right), tick in snapshot.items():
        if tick.bid is None or tick.ask is None:
            continue
        provider = Provider.SCHWAB if provider_name == "schwab" else Provider.IBKR
        received_at = tick.at.astimezone(timezone.utc)
        source_at = (tick.source_at or tick.at).astimezone(timezone.utc)
        quotes.append(
            Quote(
                instrument=InstrumentId.option(
                    "SPX",
                    expiry=expiry,
                    strike=strike,
                    right=right,
                    trading_class="SPXW",
                ),
                provider=provider,
                received_at=received_at,
                quote_time=source_at,
                quality=MarketDataQuality.LIVE,
                bid=tick.bid,
                ask=tick.ask,
                greeks=(OptionGreeks(delta=tick.delta) if tick.delta is not None else None),
            )
        )
    if len(quotes) < 4:
        return None
    by_contract: defaultdict[str, list[Quote]] = defaultdict(list)
    for quote in quotes:
        by_contract[quote.instrument.canonical_id].append(quote)
    best_quotes = tuple(
        best
        for contract_quotes in by_contract.values()
        if (
            best := choose_best_quote(
                contract_quotes,
                provider_priority=(Provider.SCHWAB, Provider.IBKR),
                as_of=decision_at,
            )
        )
        is not None
    )
    return LatestState(
        created_at=decision_at,
        as_of=decision_at,
        quotes=tuple(quotes),
        best_quotes=best_quotes,
    )
