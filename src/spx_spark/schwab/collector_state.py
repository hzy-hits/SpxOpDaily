"""Durable Schwab collection cadence, hot-plan, and quota state."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import StorageSettings
from spx_spark.marketdata import as_utc
from spx_spark.schwab.request_models import QuotaMode


COLLECTOR_STATE_FILE_NAME = "schwab_collector_state.json"


@dataclass
class CollectorBudgetState:
    chain_last_fetched_at: dict[str, datetime] = field(default_factory=dict)
    request_timestamps: list[float] = field(default_factory=list)
    hot_symbols: list[str] = field(default_factory=list)
    hot_expiry: str | None = None
    hot_reference_spot: float | None = None
    strike_counts: dict[str, int] = field(default_factory=dict)
    last_spot: float | None = None
    burst_until: datetime | None = None
    quota_mode: str = QuotaMode.NORMAL.value
    quota_consecutive_successes: int = 0
    quota_stable_windows: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "chain_last_fetched_at": {
                symbol: stamp.isoformat()
                for symbol, stamp in sorted(self.chain_last_fetched_at.items())
            },
            "request_timestamps": list(self.request_timestamps),
            "hot_symbols": list(self.hot_symbols),
            "hot_expiry": self.hot_expiry,
            "hot_reference_spot": self.hot_reference_spot,
            "strike_counts": dict(sorted(self.strike_counts.items())),
            "last_spot": self.last_spot,
            "burst_until": self.burst_until.isoformat() if self.burst_until else None,
            "quota_mode": self.quota_mode,
            "quota_consecutive_successes": self.quota_consecutive_successes,
            "quota_stable_windows": self.quota_stable_windows,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "CollectorBudgetState":
        chain_last = _datetime_map(payload.get("chain_last_fetched_at"))
        timestamps = _float_list(payload.get("request_timestamps"))
        hot_symbols_raw = payload.get("hot_symbols", [])
        strike_counts_raw = payload.get("strike_counts", {})
        return cls(
            chain_last_fetched_at=chain_last,
            request_timestamps=timestamps,
            hot_symbols=[str(item) for item in hot_symbols_raw]
            if isinstance(hot_symbols_raw, list)
            else [],
            hot_expiry=str(payload["hot_expiry"]) if payload.get("hot_expiry") else None,
            hot_reference_spot=float_or_none(payload.get("hot_reference_spot")),
            strike_counts={
                str(key): int(value)
                for key, value in strike_counts_raw.items()
                if isinstance(key, str) and positive_int(value) is not None
            }
            if isinstance(strike_counts_raw, dict)
            else {},
            last_spot=float_or_none(payload.get("last_spot")),
            burst_until=parse_iso_datetime(payload.get("burst_until")),
            quota_mode=_quota_mode_value(payload.get("quota_mode")),
            quota_consecutive_successes=max(
                int(payload.get("quota_consecutive_successes", 0)), 0
            ),
            quota_stable_windows=max(int(payload.get("quota_stable_windows", 0)), 0),
        )


def collector_state_path(storage_settings: StorageSettings) -> Path:
    return Path(storage_settings.data_root).expanduser() / "latest" / COLLECTOR_STATE_FILE_NAME


def load_collector_budget_state(path: Path) -> CollectorBudgetState:
    if not path.is_file():
        return CollectorBudgetState()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return CollectorBudgetState()
    return CollectorBudgetState.from_dict(payload) if isinstance(payload, dict) else CollectorBudgetState()


def save_collector_budget_state(path: Path, state: CollectorBudgetState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(f"{path.suffix}.tmp")
    temp.write_text(json.dumps(state.to_dict(), sort_keys=True, indent=2) + "\n", encoding="utf-8")
    temp.replace(path)


def chain_is_due(
    *,
    last_fetched_at: datetime | None,
    now: datetime,
    interval_seconds: int,
) -> bool:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if last_fetched_at is None:
        return True
    return (as_utc(now) - as_utc(last_fetched_at)).total_seconds() >= float(interval_seconds)


def prune_request_timestamps(
    timestamps: list[float],
    *,
    now_epoch: float,
    window_seconds: float = 60.0,
) -> list[float]:
    cutoff = now_epoch - window_seconds
    return [stamp for stamp in timestamps if stamp >= cutoff]


def record_requests(state: CollectorBudgetState, *, count: int, now: datetime) -> int:
    if count < 0:
        raise ValueError("request count cannot be negative")
    now_epoch = as_utc(now).timestamp()
    state.request_timestamps = prune_request_timestamps(
        state.request_timestamps,
        now_epoch=now_epoch,
    )
    state.request_timestamps.extend([now_epoch] * count)
    return len(state.request_timestamps)


def parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return as_utc(stamp)


def float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _datetime_map(value: Any) -> dict[str, datetime]:
    result: dict[str, datetime] = {}
    if not isinstance(value, dict):
        return result
    for key, raw in value.items():
        stamp = parse_iso_datetime(raw)
        if stamp is not None:
            result[_normalize_state_key(str(key))] = stamp
    return result


def _float_list(value: Any) -> list[float]:
    if not isinstance(value, list):
        return []
    result: list[float] = []
    for item in value:
        parsed = float_or_none(item)
        if parsed is not None:
            result.append(parsed)
    return result


def _quota_mode_value(value: Any) -> str:
    try:
        return QuotaMode(str(value)).value
    except ValueError:
        return QuotaMode.NORMAL.value


def _normalize_state_key(value: str) -> str:
    text = value.strip()
    upper = text.upper()
    if upper == "QUOTES:HOT_CONTEXT":
        return "quotes:hot_context"
    if ":" in text:
        symbol, lane = text.split(":", 1)
        if lane.upper() in {"FRONT", "NEXT"}:
            return f"{symbol.upper()}:{lane.lower()}"
    return upper
