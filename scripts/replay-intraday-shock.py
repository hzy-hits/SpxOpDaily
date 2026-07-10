#!/usr/bin/env python3
"""Offline JSONL replay for the deterministic SPX/ES shock monitor.

The script never loads notification settings and never writes monitor state.
It accepts raw quote JSONL files, or stdin when no files are supplied.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spx_spark.config import NY_TZ, load_dotenv
from spx_spark.intraday_shock import (
    IntradayShockSettings,
    PriceSample,
    advance_monitor_state,
    empty_monitor_state,
    mark_alert_attempts,
    rth_session_date,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


def parse_at(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iter_lines(paths: list[str]) -> Iterator[str]:
    if not paths:
        yield from sys.stdin
        return
    for raw_path in paths:
        with Path(raw_path).open(encoding="utf-8") as handle:
            yield from handle


@dataclass(frozen=True)
class ReplayQuote:
    price: float
    received_at: datetime
    source_at: datetime
    provider: str
    quality: str
    market_data_type: object


def paired_samples(
    lines: Iterable[str],
    settings: IntradayShockSettings,
) -> list[PriceSample]:
    batches: dict[datetime, dict[str, ReplayQuote]] = {}
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        instrument_id = str(row.get("instrument_id") or "")
        if instrument_id not in {"index:SPX", "future:ES"}:
            continue
        price = row.get("effective_price")
        received_at = parse_at(row.get("received_at"))
        source_at = parse_at(row.get("quote_time") or row.get("trade_time") or row.get("received_at"))
        if not isinstance(price, int | float) or received_at is None or source_at is None:
            continue
        batch = batches.setdefault(received_at, {})
        batch[instrument_id] = ReplayQuote(
            price=float(price),
            received_at=received_at,
            source_at=source_at,
            provider=str(row.get("provider") or "").lower(),
            quality=str(row.get("quality") or "").lower(),
            market_data_type=row.get("market_data_type"),
        )

    samples: list[PriceSample] = []
    latest: dict[str, ReplayQuote] = {}
    for received_at, batch in sorted(batches.items()):
        latest.update(batch)
        if "index:SPX" not in latest or "future:ES" not in latest:
            continue
        spx_quote = latest["index:SPX"]
        es_quote = latest["future:ES"]
        if any(
            quote.provider != "ibkr"
            or quote.quality != "live"
            or quote.market_data_type != 1
            for quote in (spx_quote, es_quote)
        ):
            continue
        spx_at = spx_quote.source_at
        es_at = es_quote.source_at
        if (received_at - spx_at).total_seconds() > settings.max_spx_age_seconds:
            continue
        if (received_at - es_at).total_seconds() > settings.max_es_age_seconds:
            continue
        if abs((spx_at - es_at).total_seconds()) > settings.max_anchor_skew_seconds:
            continue
        sample_at = max(spx_at, es_at)
        if rth_session_date(sample_at) is None:
            continue
        samples.append(
            PriceSample(
                # Production evaluates horizons on quote source time, not on
                # collector receipt time. Keep replay behavior identical.
                at=sample_at,
                spx=spx_quote.price,
                es=es_quote.price,
                spx_source_at=spx_at,
                es_source_at=es_at,
            )
        )
    samples.sort(key=lambda sample: sample.at)
    return samples


def session_open_for_replay(at: datetime) -> datetime | None:
    at_et = at.astimezone(NY_TZ)
    session = DEFAULT_MARKET_CALENDAR.session(at_et.date())
    if session is None or at_et < session.open_at:
        return None
    return session.open_at.astimezone(timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", help="Raw quote JSONL files; stdin if omitted.")
    parser.add_argument("--start", help="Inclusive ISO timestamp.")
    parser.add_argument("--end", help="Inclusive ISO timestamp.")
    parser.add_argument(
        "--warmup-seconds",
        type=int,
        help="Explicit pre-roll before --start; default replays from the RTH open.",
    )
    args = parser.parse_args()

    load_dotenv()
    settings = replace(IntradayShockSettings.from_env(), state_path="<replay>")
    samples = paired_samples(iter_lines(args.paths), settings)
    start = parse_at(args.start)
    end = parse_at(args.end)
    replay_start: datetime | None = None
    if start is not None:
        session_open = session_open_for_replay(start)
        if args.warmup_seconds is None:
            replay_start = session_open
        else:
            replay_start = start - timedelta(
                seconds=max(args.warmup_seconds, settings.three_minute_seconds)
            )
            if session_open is not None:
                replay_start = max(replay_start, session_open)
    replay_samples = [
        sample
        for sample in samples
        if (replay_start is None or sample.at >= replay_start)
        and (end is None or sample.at <= end)
    ]
    analysis_samples = [
        sample for sample in replay_samples if start is None or sample.at >= start
    ]
    required_coverage_start = replay_start
    coverage_start = replay_samples[0].at if replay_samples else None
    coverage_complete = (
        required_coverage_start is None
        or (
            coverage_start is not None
            and coverage_start
            <= required_coverage_start + timedelta(seconds=settings.max_spx_age_seconds)
        )
    )
    if not analysis_samples:
        print(
            json.dumps(
                {
                    "sample_count": 0,
                    "processed_sample_count": len(replay_samples),
                    "coverage_start": coverage_start.isoformat() if coverage_start else None,
                    "required_coverage_start": required_coverage_start.isoformat()
                    if required_coverage_start
                    else None,
                    "coverage_complete": coverage_complete,
                    "events": [],
                },
                sort_keys=True,
            )
        )
        return

    first_session_date = rth_session_date(replay_samples[0].at)
    assert first_session_date is not None
    session_date = first_session_date
    state = empty_monitor_state(session_date)
    events: list[dict[str, object]] = []
    for sample in replay_samples:
        sample_session_date = rth_session_date(sample.at)
        if sample_session_date is None:
            continue
        if sample_session_date != session_date:
            session_date = sample_session_date
            state = empty_monitor_state(session_date)
        state, alerts = advance_monitor_state(state, sample, settings)
        for alert in alerts:
            if start is not None and sample.at < start:
                continue
            active = state.get("active_event")
            event = active if isinstance(active, dict) else {}
            events.append(
                {
                    "at": sample.at.isoformat(),
                    "kind": alert.kind,
                    "event_id": alert.event_id,
                    "direction": event.get("direction"),
                    "spx": sample.spx,
                    "es": sample.es,
                    "shock_spx_bps": event.get("shock_spx_bps"),
                    "shock_es_bps": event.get("shock_es_bps"),
                    "spx_recovery_fraction": event.get("spx_recovery_fraction"),
                    "es_recovery_fraction": event.get("es_recovery_fraction"),
                }
            )
        if alerts:
            state = mark_alert_attempts(state, alerts, at=sample.at, delivered=True)

    print(
        json.dumps(
            {
                "sample_count": len(analysis_samples),
                "processed_sample_count": len(replay_samples),
                "coverage_start": coverage_start.isoformat() if coverage_start else None,
                "required_coverage_start": required_coverage_start.isoformat()
                if required_coverage_start
                else None,
                "coverage_complete": coverage_complete,
                "events": events,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
