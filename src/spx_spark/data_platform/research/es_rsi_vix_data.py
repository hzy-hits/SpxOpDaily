"""Build minute-level ES/VIX research series from durable alert snapshots."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np


EASTERN = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
INSTRUMENTS = ("future:ES", "index:VIX", "index:VIX1D")


@dataclass(frozen=True)
class SessionSeries:
    session_date: str
    times: tuple[datetime, ...]
    es: np.ndarray
    vix: np.ndarray
    vix1d: np.ndarray
    observed: dict[str, np.ndarray]


@dataclass(frozen=True)
class SessionCoverage:
    session_date: str
    total_minutes: int
    observed_es_minutes: int
    observed_vix_minutes: int
    observed_vix1d_minutes: int
    usable_es_minutes: int
    usable_vix_minutes: int
    usable_vix1d_minutes: int
    longest_es_gap_minutes: int


def _parse_at(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _rth_minute(at: datetime) -> bool:
    local = at.astimezone(EASTERN)
    minute = local.hour * 60 + local.minute
    return local.weekday() < 5 and 9 * 60 + 30 <= minute <= 16 * 60


def _entry_prices(payload: dict[str, object]) -> dict[str, float]:
    market_context = payload.get("market_context")
    if not isinstance(market_context, dict):
        return {}
    entries = market_context.get("entries")
    if not isinstance(entries, list):
        return {}
    prices: dict[str, float] = {}
    for raw in entries:
        if not isinstance(raw, dict):
            continue
        instrument_id = raw.get("instrument_id")
        price = raw.get("price")
        if instrument_id not in INSTRUMENTS or not isinstance(price, (int, float)):
            continue
        if raw.get("freshness") != "fresh" or raw.get("research_usable") is False:
            continue
        prices[str(instrument_id)] = float(price)
    return prices


def _forward_fill(values: np.ndarray, *, max_gap_minutes: int) -> np.ndarray:
    filled = values.copy()
    last_value = np.nan
    gap = max_gap_minutes + 1
    for index, value in enumerate(filled):
        if np.isfinite(value):
            last_value = value
            gap = 0
            continue
        gap += 1
        if np.isfinite(last_value) and gap <= max_gap_minutes:
            filled[index] = last_value
    return filled


def _longest_gap(observed: np.ndarray) -> int:
    longest = current = 0
    for value in observed:
        if value:
            current = 0
        else:
            current += 1
            longest = max(longest, current)
    return longest


def load_outbox_sessions(
    path: str | Path,
    *,
    max_forward_fill_minutes: int = 2,
) -> tuple[list[SessionSeries], list[SessionCoverage]]:
    """Read fresh RTH snapshots without mutating the live outbox."""

    outbox = Path(path).expanduser().resolve()
    connection = sqlite3.connect(f"file:{outbox}?mode=ro", uri=True)
    by_session: dict[str, dict[datetime, dict[str, float]]] = {}
    try:
        rows = connection.execute(
            "SELECT source_at, payload_json FROM domain_event_outbox ORDER BY rowid"
        )
        for source_at, payload_json in rows:
            at = _parse_at(str(source_at)).replace(second=0, microsecond=0)
            if not _rth_minute(at):
                continue
            prices = _entry_prices(json.loads(payload_json))
            if not prices:
                continue
            session_date = at.astimezone(EASTERN).date().isoformat()
            by_session.setdefault(session_date, {}).setdefault(at, {}).update(prices)
    finally:
        connection.close()

    sessions: list[SessionSeries] = []
    coverage: list[SessionCoverage] = []
    for session_date, minute_rows in sorted(by_session.items()):
        local_start = datetime.fromisoformat(f"{session_date}T09:30:00").replace(tzinfo=EASTERN)
        local_end = datetime.fromisoformat(f"{session_date}T16:00:00").replace(tzinfo=EASTERN)
        times: list[datetime] = []
        at = local_start.astimezone(UTC)
        end = local_end.astimezone(UTC)
        while at <= end:
            times.append(at)
            at += timedelta(minutes=1)

        arrays: dict[str, np.ndarray] = {}
        masks: dict[str, np.ndarray] = {}
        for instrument_id in INSTRUMENTS:
            values = np.array(
                [minute_rows.get(at, {}).get(instrument_id, np.nan) for at in times],
                dtype=float,
            )
            masks[instrument_id] = np.isfinite(values)
            arrays[instrument_id] = _forward_fill(
                values,
                max_gap_minutes=max_forward_fill_minutes,
            )

        sessions.append(
            SessionSeries(
                session_date=session_date,
                times=tuple(times),
                es=arrays["future:ES"],
                vix=arrays["index:VIX"],
                vix1d=arrays["index:VIX1D"],
                observed=masks,
            )
        )
        coverage.append(
            SessionCoverage(
                session_date=session_date,
                total_minutes=len(times),
                observed_es_minutes=int(masks["future:ES"].sum()),
                observed_vix_minutes=int(masks["index:VIX"].sum()),
                observed_vix1d_minutes=int(masks["index:VIX1D"].sum()),
                usable_es_minutes=int(np.isfinite(arrays["future:ES"]).sum()),
                usable_vix_minutes=int(np.isfinite(arrays["index:VIX"]).sum()),
                usable_vix1d_minutes=int(np.isfinite(arrays["index:VIX1D"]).sum()),
                longest_es_gap_minutes=_longest_gap(masks["future:ES"]),
            )
        )
    return sessions, coverage
