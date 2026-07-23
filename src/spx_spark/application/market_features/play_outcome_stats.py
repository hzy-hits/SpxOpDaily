"""Rolling historical win-rate stats from the system's own pricing outcomes."""

from __future__ import annotations

import json
import logging
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Iterable, Mapping

from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.state_io import atomic_write_json_secure, read_json_object
from spx_spark.strategy_contract import pricing_outcome_semantic_key


LOGGER = logging.getLogger(__name__)
CACHE_SCHEMA_VERSION = 2
CACHE_FILENAME = "play_outcome_stats_cache.json"


@dataclass(frozen=True)
class PlayOutcomeStats:
    play: str
    level_kind: str
    sample_count: int
    winrate: float
    avg_return: float
    median_return: float
    window_days: int
    horizon: str
    as_of: str


def load_play_outcome_stats(
    features_root: Path,
    *,
    window_days: int,
    horizon: str,
    now: datetime,
) -> dict[tuple[str, str], PlayOutcomeStats]:
    """Aggregate touched, completed outcomes into per-(play, level_kind) stats."""

    now = _utc(now)
    horizon_key = str(horizon)
    earliest = now - timedelta(days=window_days)
    returns: dict[tuple[str, str], list[float]] = {}
    seen_semantic_keys: set[str] = set()
    for row in _outcome_rows(
        features_root,
        earliest=earliest.date(),
        latest=now.date(),
    ):
        if row.get("touched") is not True:
            continue
        completed_at = _datetime(row.get("completed_at"))
        if completed_at is None or completed_at < earliest or completed_at > now:
            continue
        value = _horizon_return(row.get("horizons"), horizon_key)
        if value is None:
            continue
        play = str(row.get("play") or "")
        level_kind = str(row.get("level_kind") or "")
        if not play or not level_kind:
            continue
        semantic_key = pricing_outcome_semantic_key(row) or str(row.get("key") or "").strip()
        if semantic_key and semantic_key in seen_semantic_keys:
            continue
        if semantic_key:
            seen_semantic_keys.add(semantic_key)
        returns.setdefault((play, level_kind), []).append(value)
    as_of = now.isoformat()
    return {
        key: PlayOutcomeStats(
            play=key[0],
            level_kind=key[1],
            sample_count=len(values),
            winrate=sum(1 for value in values if value > 0) / len(values),
            avg_return=sum(values) / len(values),
            median_return=median(values),
            window_days=window_days,
            horizon=horizon_key,
            as_of=as_of,
        )
        for key, values in returns.items()
        if values
    }


class PlayOutcomeStatsProvider:
    """TTL-cached, fail-open lookup over the pricing-outcomes store."""

    def __init__(
        self,
        features_root: Path,
        *,
        settings: MarketFeatureSettings,
        cache_path: Path | None = None,
    ) -> None:
        self._features_root = Path(features_root)
        self._window_days = settings.play_stats_window_days
        self._horizon = settings.play_stats_horizon
        self._min_samples = settings.play_stats_min_samples
        self._refresh_seconds = settings.play_stats_refresh_seconds
        self._cache_path = (
            Path(cache_path) if cache_path else self._features_root / ".cache" / CACHE_FILENAME
        )
        self._loaded_at: datetime | None = None
        self._stats: dict[tuple[str, str], PlayOutcomeStats] = {}
        self._disk_cache_checked = False

    def lookup(self, play: str, level_kind: str) -> PlayOutcomeStats | None:
        """Return cached stats, or None when unavailable or below min_samples."""

        try:
            self._refresh(datetime.now(tz=timezone.utc))
        except Exception as exc:  # Fail open: keep serving the previous snapshot.
            LOGGER.warning("play outcome stats refresh failed: %s", exc)
        stats = self._stats.get((play, level_kind))
        if stats is None or stats.sample_count < self._min_samples:
            return None
        return stats

    def _refresh(self, now: datetime) -> None:
        if not self._disk_cache_checked:
            self._load_disk_cache()
            self._disk_cache_checked = True
        if (
            self._loaded_at is not None
            and 0 <= (now - self._loaded_at).total_seconds() < self._refresh_seconds
        ):
            return
        store_root = self._features_root / "pricing_outcomes"
        if self._stats and not store_root.is_dir():
            raise FileNotFoundError(f"play outcome stats store unavailable: {store_root}")
        refreshed = load_play_outcome_stats(
            self._features_root,
            window_days=self._window_days,
            horizon=self._horizon,
            now=now,
        )
        self._stats = refreshed
        self._loaded_at = now
        try:
            self._write_disk_cache()
        except OSError as exc:
            LOGGER.warning("play outcome stats cache write failed: %s", exc)

    def _load_disk_cache(self) -> None:
        payload = read_json_object(self._cache_path)
        if (
            payload.get("schema_version") != CACHE_SCHEMA_VERSION
            or payload.get("window_days") != self._window_days
            or str(payload.get("horizon") or "") != self._horizon
        ):
            return
        loaded_at = _datetime(payload.get("loaded_at"))
        rows = payload.get("stats")
        if loaded_at is None or not isinstance(rows, list):
            return
        parsed: dict[tuple[str, str], PlayOutcomeStats] = {}
        try:
            for row in rows:
                if not isinstance(row, Mapping):
                    return
                stats = PlayOutcomeStats(
                    play=str(row["play"]),
                    level_kind=str(row["level_kind"]),
                    sample_count=int(row["sample_count"]),
                    winrate=float(row["winrate"]),
                    avg_return=float(row["avg_return"]),
                    median_return=float(row["median_return"]),
                    window_days=int(row["window_days"]),
                    horizon=str(row["horizon"]),
                    as_of=str(row["as_of"]),
                )
                parsed[(stats.play, stats.level_kind)] = stats
        except (KeyError, TypeError, ValueError):
            return
        self._stats = parsed
        self._loaded_at = loaded_at

    def _write_disk_cache(self) -> None:
        if self._loaded_at is None:
            return
        atomic_write_json_secure(
            self._cache_path,
            {
                "schema_version": CACHE_SCHEMA_VERSION,
                "loaded_at": self._loaded_at.isoformat(),
                "window_days": self._window_days,
                "horizon": self._horizon,
                "stats": [asdict(stats) for _, stats in sorted(self._stats.items())],
            },
        )


def _outcome_rows(
    features_root: Path,
    *,
    earliest: date | None = None,
    latest: date | None = None,
) -> Iterable[Mapping[str, object]]:
    root = Path(features_root) / "pricing_outcomes"
    for path in sorted(root.glob("date=*/outcomes.jsonl")):
        partition = _partition_date(path)
        if partition is not None and (
            (earliest is not None and partition < earliest)
            or (latest is not None and partition > latest)
        ):
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            LOGGER.warning("play outcome stats source unreadable: %s", path)
            raise
        for line in lines:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload


def _partition_date(path: Path) -> date | None:
    try:
        return date.fromisoformat(path.parent.name.removeprefix("date="))
    except ValueError:
        return None


def _horizon_return(horizons: object, horizon: str) -> float | None:
    if not isinstance(horizons, Mapping):
        return None
    entry = horizons.get(horizon)
    if not isinstance(entry, Mapping):
        return None
    value = entry.get("return_fraction")
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
