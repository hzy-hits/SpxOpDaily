from __future__ import annotations

import json
from collections import Counter, deque
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.state_io import atomic_write_json_secure

_BAR_SCHEMA_VERSION = "spx_bars.v0.1"
_MAX_1M_BARS = 240
_MAX_5M_BARS = 96
_MIN_OK_SAMPLES_1M = 6


@dataclass(frozen=True)
class SpxBar:
    bar_start: datetime
    interval_seconds: int
    open: float
    high: float
    low: float
    close: float
    sample_count: int
    quality: str
    gap_before: bool
    provider: str


@dataclass
class _OpenBar:
    bar_start: datetime
    interval_seconds: int
    open: float
    high: float
    low: float
    close: float
    sample_count: int = 0
    providers: list[str] = field(default_factory=list)
    gap_before: bool = False

    def add(self, price: float, provider: str) -> None:
        if self.sample_count == 0:
            self.open = self.high = self.low = self.close = price
        else:
            self.high = max(self.high, price)
            self.low = min(self.low, price)
            self.close = price
        self.sample_count += 1
        self.providers.append(provider)

    def finalize(self) -> SpxBar:
        quality = "ok" if self.sample_count >= _MIN_OK_SAMPLES_1M else "partial"
        provider = Counter(self.providers).most_common(1)[0][0] if self.providers else "unknown"
        return SpxBar(
            bar_start=self.bar_start,
            interval_seconds=self.interval_seconds,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            sample_count=self.sample_count,
            quality=quality,
            gap_before=self.gap_before,
            provider=provider,
        )


def _bar_start(observed_at: datetime, interval_seconds: int) -> datetime:
    epoch = int(observed_at.timestamp())
    aligned = (epoch // interval_seconds) * interval_seconds
    return datetime.fromtimestamp(aligned, tz=timezone.utc)


def bar_to_dict(bar: SpxBar) -> dict[str, Any]:
    payload = asdict(bar)
    payload["bar_start"] = bar.bar_start.isoformat()
    return payload


def bars_payload(
    bars: Sequence[SpxBar],
    *,
    interval_seconds: int,
    as_of: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": _BAR_SCHEMA_VERSION,
        "interval_seconds": interval_seconds,
        "updated_at": as_of.isoformat(),
        "bars": [bar_to_dict(bar) for bar in bars],
    }


def bar_hold(bars: Sequence[SpxBar], level: float, side: str, n: int) -> bool:
    if n <= 0 or len(bars) < n:
        return False
    window = list(bars)[-n:]
    if any(bar.quality != "ok" for bar in window):
        return False
    for index, bar in enumerate(window):
        if index > 0 and bar.gap_before:
            return False
    if side == "above":
        return all(bar.close > level for bar in window)
    if side == "below":
        return all(bar.close < level for bar in window)
    raise ValueError(f"unsupported side: {side}")


class SpxBarBuilder:
    def __init__(self) -> None:
        self._open_1m: _OpenBar | None = None
        self._closed_1m: deque[SpxBar] = deque(maxlen=_MAX_1M_BARS)
        self._closed_5m: deque[SpxBar] = deque(maxlen=_MAX_5M_BARS)
        self._five_min_bucket: list[SpxBar] = []
        self._five_min_start: datetime | None = None
        self._pending_lake_1m: list[SpxBar] = []
        self._pending_lake_5m: list[SpxBar] = []

    def ingest(self, observed_at: datetime, price: float, provider: str) -> list[SpxBar]:
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        newly_closed: list[SpxBar] = []
        newly_closed.extend(self._ingest_1m(observed_at, price, provider))
        newly_closed.extend(self._ingest_5m_from_closed())
        return newly_closed

    def _ingest_1m(self, observed_at: datetime, price: float, provider: str) -> list[SpxBar]:
        bar_start = _bar_start(observed_at, 60)
        newly_closed: list[SpxBar] = []

        if self._open_1m is None:
            self._open_1m = _OpenBar(
                bar_start=bar_start,
                interval_seconds=60,
                open=price,
                high=price,
                low=price,
                close=price,
            )
            self._open_1m.sample_count = 0
            self._open_1m.providers = []
            self._open_1m.add(price, provider)
            return newly_closed

        if bar_start > self._open_1m.bar_start:
            closed = self._open_1m.finalize()
            self._closed_1m.append(closed)
            self._pending_lake_1m.append(closed)
            newly_closed.append(closed)
            gap = bar_start > self._open_1m.bar_start + timedelta(seconds=60)
            self._open_1m = _OpenBar(
                bar_start=bar_start,
                interval_seconds=60,
                open=price,
                high=price,
                low=price,
                close=price,
                gap_before=gap,
            )
            self._open_1m.sample_count = 0
            self._open_1m.providers = []
            self._open_1m.add(price, provider)
            return newly_closed

        if bar_start == self._open_1m.bar_start:
            self._open_1m.add(price, provider)
        return newly_closed

    def _ingest_5m_from_closed(self) -> list[SpxBar]:
        if not self._closed_1m:
            return []
        latest = self._closed_1m[-1]
        five_start = _bar_start(latest.bar_start, 300)
        newly_closed: list[SpxBar] = []

        if self._five_min_start is None:
            self._five_min_start = five_start
            self._five_min_bucket = [latest]
            return newly_closed

        if five_start != self._five_min_start:
            if self._five_min_bucket:
                five_bar = self._aggregate_5m(self._five_min_bucket, self._five_min_start)
                self._closed_5m.append(five_bar)
                self._pending_lake_5m.append(five_bar)
                newly_closed.append(five_bar)
            self._five_min_start = five_start
            self._five_min_bucket = [latest]
            return newly_closed

        if latest.bar_start not in {bar.bar_start for bar in self._five_min_bucket}:
            self._five_min_bucket.append(latest)
        return newly_closed

    def _aggregate_5m(self, bars: list[SpxBar], bar_start: datetime) -> SpxBar:
        ordered = sorted(bars, key=lambda bar: bar.bar_start)
        quality = "ok" if all(bar.quality == "ok" for bar in ordered) and len(ordered) == 5 else "partial"
        providers = [bar.provider for bar in ordered]
        provider = Counter(providers).most_common(1)[0][0] if providers else "unknown"
        return SpxBar(
            bar_start=bar_start,
            interval_seconds=300,
            open=ordered[0].open,
            high=max(bar.high for bar in ordered),
            low=min(bar.low for bar in ordered),
            close=ordered[-1].close,
            sample_count=sum(bar.sample_count for bar in ordered),
            quality=quality,
            gap_before=ordered[0].gap_before,
            provider=provider,
        )

    def closed_bars_1m(self) -> tuple[SpxBar, ...]:
        return tuple(self._closed_1m)

    def closed_bars_5m(self) -> tuple[SpxBar, ...]:
        return tuple(self._closed_5m)

    def persist(self, data_root: Path, *, as_of: datetime, trading_date: str) -> None:
        latest_dir = data_root / "latest"
        latest_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json_secure(
            latest_dir / "spx_bars_1m.json",
            bars_payload(self.closed_bars_1m(), interval_seconds=60, as_of=as_of),
        )
        atomic_write_json_secure(
            latest_dir / "spx_bars_5m.json",
            bars_payload(self.closed_bars_5m(), interval_seconds=300, as_of=as_of),
        )

        lake_dir = data_root / "lake" / "steven" / "bars" / f"date={trading_date}"
        lake_dir.mkdir(parents=True, exist_ok=True)
        lake_1m = lake_dir / "spx_bars_1m.jsonl"
        with lake_1m.open("a", encoding="utf-8") as handle:
            for bar in self._pending_lake_1m:
                handle.write(json.dumps(bar_to_dict(bar), sort_keys=True) + "\n")
        self._pending_lake_1m.clear()

        lake_5m = lake_dir / "spx_bars_5m.jsonl"
        with lake_5m.open("a", encoding="utf-8") as handle:
            for bar in self._pending_lake_5m:
                handle.write(json.dumps(bar_to_dict(bar), sort_keys=True) + "\n")
        self._pending_lake_5m.clear()
