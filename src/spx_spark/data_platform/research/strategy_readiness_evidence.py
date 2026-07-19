"""Exact execution evidence counters for strategy readiness reviews.

The readiness scorecard treats detector/session health separately from trade
opportunities.  This module owns the stricter side of that boundary: duplicate
opportunity detection and exact quote/spread joins used by the frozen cohorts.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any, Protocol


class ReadinessRecord(Protocol):
    """Structural record contract shared with ``strategy_readiness``."""

    source: str
    payload: Mapping[str, object]
    path: str
    line_number: int
    at: datetime | None
    session_date: date | None


def duplicate_audit(records: Sequence[ReadinessRecord]) -> dict[str, Any]:
    """Find repeated semantic opportunities without counting evaluations."""

    keyed: dict[str, list[ReadinessRecord]] = defaultdict(list)
    for record in records:
        key = _semantic_record_key(record)
        if key is not None:
            keyed[key].append(record)
    duplicate_records = 0
    duplicate_keys: list[str] = []
    sessions: set[str] = set()
    for key, rows in sorted(keyed.items()):
        if len(rows) <= 1:
            continue
        duplicate_records += len(rows) - 1
        duplicate_keys.append(key)
        sessions.update(
            row.session_date.isoformat() for row in rows if row.session_date is not None
        )
    return {
        "duplicate_records": duplicate_records,
        "keys": duplicate_keys,
        "sessions": sessions,
    }


def count_gth_exact_entries(
    records: Sequence[ReadinessRecord], *, eligible_sessions: set[str]
) -> dict[str, Any]:
    """Count one exact call-spread entry per eligible GTH signal."""

    signals = [record for record in records if record.source == "gth_dip_reclaim"]
    opens = [
        record
        for record in records
        if record.source == "virtual_strategy" and record.payload.get("event") == "virtual_opened"
    ]
    decisions = [
        record
        for record in records
        if record.source == "virtual_strategy"
        and record.payload.get("event") == "virtual_entry_decision"
        and record.payload.get("status") == "trade_ready"
        and record.payload.get("terminal") is True
    ]
    opens_by_source: dict[str, ReadinessRecord] = {}
    for record in sorted(
        opens, key=lambda item: item.at or datetime.min.replace(tzinfo=timezone.utc)
    ):
        source_id = record.payload.get("source_signal_id")
        if _nonempty_string(source_id):
            opens_by_source.setdefault(str(source_id), record)
    decisions_by_source: dict[str, ReadinessRecord] = {}
    for record in sorted(decisions, key=_record_sort_key):
        source_id = record.payload.get("source_signal_id")
        if _nonempty_string(source_id):
            decisions_by_source.setdefault(str(source_id), record)

    successes: set[str] = set()
    episodes: set[str] = set()
    eligible = 0
    exact_structures = 0
    excluded_incomplete = 0
    for signal in signals:
        session_id = signal.session_date.isoformat() if signal.session_date else ""
        if session_id not in eligible_sessions:
            excluded_incomplete += 1
            continue
        eligible += 1
        if not _exact_gth_structure(signal.payload):
            continue
        exact_structures += 1
        event_id = signal.payload.get("event_id")
        decision = decisions_by_source.get(str(event_id))
        opened = opens_by_source.get(str(event_id))
        if (
            decision is None
            or not _exact_spread_decision(decision.payload)
            or opened is None
            or not _exact_spread_open(signal.payload, opened.payload)
            or not _same_spread_snapshot(
                decision.payload.get("exact_spread_snapshot"),
                _entry_snapshot(opened.payload),
            )
            or (
                _nonempty_string(decision.payload.get("episode_id"))
                and decision.payload.get("episode_id") != opened.payload.get("episode_id")
            )
        ):
            continue
        successes.add(str(event_id))
        episode_id = opened.payload.get("episode_id")
        if _nonempty_string(episode_id):
            episodes.add(str(episode_id))
    return {
        "count": len(successes),
        "episode_ids": sorted(episodes),
        "eligible_signals": eligible,
        "signals_with_exact_structure": exact_structures,
        "unmatched_or_inexact_signals": eligible - len(successes),
        "excluded_incomplete_session": excluded_incomplete,
    }


def count_put_exact_entries(
    records: Sequence[ReadinessRecord], *, eligible_sessions: set[str]
) -> dict[str, int]:
    """Count exact single-leg put entries joined to trade-ready intents."""

    intents = [
        record
        for record in records
        if record.source == "trade_intents"
        and record.payload.get("status") == "trade_ready"
        and record.payload.get("direction") == "down"
    ]
    opens = [
        record
        for record in records
        if record.source == "virtual_strategy" and record.payload.get("event") == "virtual_opened"
    ]
    opens_by_source = {
        str(record.payload.get("source_signal_id")): record
        for record in opens
        if _nonempty_string(record.payload.get("source_signal_id"))
    }
    successes: set[str] = set()
    eligible = 0
    excluded_incomplete = 0
    for intent in intents:
        session_id = intent.session_date.isoformat() if intent.session_date else ""
        if session_id not in eligible_sessions:
            excluded_incomplete += 1
            continue
        eligible += 1
        intent_id = str(intent.payload.get("intent_id") or "")
        opened = opens_by_source.get(intent_id)
        if opened is not None and _exact_put_open(intent.payload, opened.payload):
            successes.add(intent_id)
    return {
        "count": len(successes),
        "eligible_trade_ready_puts": eligible,
        "unmatched_or_inexact_puts": eligible - len(successes),
        "excluded_incomplete_session": excluded_incomplete,
    }


def count_exact_spread_exits(
    records: Sequence[ReadinessRecord],
    *,
    eligible_sessions: set[str],
    successful_gth_episodes: set[str],
) -> dict[str, int]:
    """Count exact closes belonging to an already-qualified GTH entry."""

    closes = [
        record
        for record in records
        if record.source == "virtual_strategy"
        and record.payload.get("event") == "virtual_closed"
        and str(record.payload.get("episode_id") or "") in successful_gth_episodes
    ]
    successes: set[str] = set()
    excluded_incomplete = 0
    ineligible_exact = 0
    for record in closes:
        session_id = record.session_date.isoformat() if record.session_date else ""
        if session_id not in eligible_sessions:
            excluded_incomplete += 1
            continue
        episode_id = str(record.payload.get("episode_id") or "")
        if _exact_spread_close(record.payload):
            successes.add(episode_id)
        else:
            ineligible_exact += 1
    missing = max(len(successful_gth_episodes) - len(successes), 0)
    return {
        "count": len(successes),
        "unmatched_or_inexact_exits": max(missing, ineligible_exact),
        "excluded_incomplete_session": excluded_incomplete,
    }


def cohort_result(
    *,
    count: int,
    target: int,
    count_blocker: str,
    common_blockers: Sequence[str],
    details: Mapping[str, object],
) -> dict[str, object]:
    """Render one frozen cohort result with stable blocker ordering."""

    blockers = list(common_blockers)
    if count < target:
        blockers.append(count_blocker)
    blockers = _unique(blockers)
    return {
        "status": "ready" if not blockers else "collecting",
        "count": count,
        "target": target,
        "blockers": blockers,
        **details,
    }


def _semantic_record_key(record: ReadinessRecord) -> str | None:
    row = record.payload
    if record.source == "gth_dip_reclaim":
        event_id = row.get("event_id")
        return f"gth_signal:{event_id}" if _nonempty_string(event_id) else None
    if record.source == "trade_intents" and row.get("status") == "trade_ready":
        intent_id = row.get("intent_id")
        return f"trade_ready:{intent_id}" if _nonempty_string(intent_id) else None
    if record.source == "confirmed_gate_results":
        record_key = row.get("record_key") or row.get("event_id")
        return f"confirmed_gate:{record_key}" if _nonempty_string(record_key) else None
    if record.source == "trade_candidates":
        candidate_id = row.get("candidate_id")
        event = row.get("event")
        return f"trade_candidate:{event}:{candidate_id}" if _nonempty_string(candidate_id) else None
    if record.source == "virtual_strategy" and row.get("event") == "virtual_entry_decision":
        decision_id = row.get("decision_id") or row.get("source_signal_id")
        return f"virtual_entry_decision:{decision_id}" if _nonempty_string(decision_id) else None
    if record.source == "virtual_strategy" and row.get("event") == "virtual_opened":
        source_id = row.get("source_signal_id") or row.get("episode_id")
        return f"virtual_opened:{source_id}" if _nonempty_string(source_id) else None
    if record.source == "virtual_strategy" and row.get("event") == "virtual_closed":
        episode_id = row.get("episode_id")
        return f"virtual_closed:{episode_id}" if _nonempty_string(episode_id) else None
    return None


def _exact_gth_structure(payload: Mapping[str, object]) -> bool:
    spread = payload.get("spread")
    session_id = str(payload.get("session_date") or "")
    if not isinstance(spread, Mapping) or spread.get("right") != "C":
        return False
    long_strike = _number(spread.get("long_strike"))
    short_strike = _number(spread.get("short_strike"))
    width = _number(spread.get("width_points"))
    return bool(
        long_strike is not None
        and short_strike is not None
        and width is not None
        and short_strike > long_strike > 0
        and math.isclose(short_strike - long_strike, width, abs_tol=1e-6)
        and spread.get("expiry_date") == session_id
    )


def _exact_spread_decision(payload: Mapping[str, object]) -> bool:
    snapshot = payload.get("exact_spread_snapshot")
    return bool(
        payload.get("status") == "trade_ready"
        and payload.get("terminal") is True
        and payload.get("position_type") == "call_debit_spread"
        and _nonempty_string(payload.get("source_signal_id"))
        and _nonempty_string(payload.get("episode_id"))
        and isinstance(snapshot, Mapping)
        and _exact_spread_snapshot(snapshot, at=_event_at(payload))
    )


def _exact_spread_open(signal: Mapping[str, object], opened: Mapping[str, object]) -> bool:
    if opened.get("position_type") != "call_debit_spread":
        return False
    spread = signal.get("spread")
    if not isinstance(spread, Mapping):
        return False
    long_contract = _parse_option_contract(opened.get("long_contract_id"))
    short_contract = _parse_option_contract(opened.get("short_contract_id"))
    if long_contract is None or short_contract is None:
        return False
    expiry = _parse_date(spread.get("expiry_date"))
    long_strike = _number(spread.get("long_strike"))
    short_strike = _number(spread.get("short_strike"))
    if (
        expiry is None
        or long_strike is None
        or short_strike is None
        or long_contract != (expiry, long_strike, "C")
        or short_contract != (expiry, short_strike, "C")
    ):
        return False
    snapshot = _entry_snapshot(opened)
    width = _number(spread.get("width_points"))
    ask = _number(snapshot.get("ask"))
    return bool(
        width is not None
        and ask is not None
        and 0 < ask < width
        and _exact_spread_snapshot(snapshot, at=_event_at(opened))
    )


def _exact_put_open(intent: Mapping[str, object], opened: Mapping[str, object]) -> bool:
    contract_id = str(intent.get("contract_id") or "")
    session_id = _parse_date(intent.get("session_id"))
    contract = _parse_option_contract(contract_id)
    if (
        contract is None
        or session_id is None
        or contract[0] != session_id
        or contract[2] != "P"
        or opened.get("contract_id") != contract_id
        or opened.get("position_type") == "call_debit_spread"
    ):
        return False
    snapshot = _entry_snapshot(opened)
    return _exact_quote_snapshot(snapshot, at=_event_at(opened), require_quality=True)


def _exact_spread_close(payload: Mapping[str, object]) -> bool:
    if payload.get("position_type") != "call_debit_spread":
        return False
    opened_at = _parse_time(payload.get("opened_at"))
    closed_at = _parse_time(payload.get("closed_at"))
    if opened_at is None or closed_at is None or closed_at < opened_at:
        return False
    entry = _entry_snapshot(payload)
    exit_snapshot = payload.get("exit_snapshot")
    return bool(
        isinstance(exit_snapshot, Mapping)
        and _exact_spread_snapshot(entry, at=opened_at)
        and _exact_spread_snapshot(exit_snapshot, at=closed_at)
    )


def _entry_snapshot(payload: Mapping[str, object]) -> dict[str, object]:
    nested = payload.get("entry_snapshot")
    if not isinstance(nested, Mapping):
        nested = payload.get("last") if isinstance(payload.get("last"), Mapping) else {}
    result = dict(nested)
    for name in ("bid", "mid", "ask"):
        top_value = payload.get(f"entry_{name}")
        if _number(top_value) is not None:
            result[name] = top_value
    return result


def _exact_spread_snapshot(snapshot: Mapping[str, object], *, at: datetime | None) -> bool:
    long = snapshot.get("long")
    short = snapshot.get("short")
    quality = snapshot.get("quality")
    long_source_at = _parse_time(long.get("source_at")) if isinstance(long, Mapping) else None
    short_source_at = _parse_time(short.get("source_at")) if isinstance(short, Mapping) else None
    return bool(
        _valid_nbbo(snapshot)
        and isinstance(long, Mapping)
        and isinstance(short, Mapping)
        and _exact_quote_snapshot(long, at=at, require_quality=True)
        and _exact_quote_snapshot(short, at=at, require_quality=True)
        and long_source_at is not None
        and short_source_at is not None
        and abs((long_source_at - short_source_at).total_seconds()) <= 5.0
        and isinstance(quality, Mapping)
        and quality.get("status") == "ok"
    )


def _exact_quote_snapshot(
    snapshot: Mapping[str, object],
    *,
    at: datetime | None,
    require_quality: bool,
) -> bool:
    if not _valid_nbbo(snapshot):
        return False
    source_at = _parse_time(snapshot.get("source_at"))
    if source_at is None or at is None:
        return False
    quote_age = (at - source_at).total_seconds()
    if quote_age < -1.0 or quote_age > 5.0:
        return False
    if require_quality:
        quality = snapshot.get("quality")
        if not isinstance(quality, Mapping) or quality.get("status") != "ok":
            return False
    return True


def _valid_nbbo(snapshot: Mapping[str, object]) -> bool:
    bid = _number(snapshot.get("bid"))
    mid = _number(snapshot.get("mid"))
    ask = _number(snapshot.get("ask"))
    return bool(
        bid is not None
        and mid is not None
        and ask is not None
        and 0 <= bid
        and mid > 0
        and ask > 0
        and bid <= mid <= ask
    )


def _same_spread_snapshot(left: object, right: object) -> bool:
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return False
    for leg in ("long", "short"):
        left_leg = left.get(leg)
        right_leg = right.get(leg)
        if not isinstance(left_leg, Mapping) or not isinstance(right_leg, Mapping):
            return False
        for field in ("bid", "mid", "ask", "source_at"):
            if left_leg.get(field) != right_leg.get(field):
                return False
    return True


def _parse_option_contract(value: object) -> tuple[date, float, str] | None:
    if not isinstance(value, str):
        return None
    parts = value.split(":")
    if len(parts) != 6 or parts[:3] != ["option", "SPX", "SPXW"]:
        return None
    try:
        expiry = datetime.strptime(parts[3], "%Y%m%d").date()
        strike = float(parts[4])
    except ValueError:
        return None
    if strike <= 0 or parts[5] not in {"C", "P"}:
        return None
    return expiry, strike, parts[5]


def _record_sort_key(record: ReadinessRecord) -> tuple[datetime, str, int]:
    return (
        record.at or datetime.min.replace(tzinfo=timezone.utc),
        record.path,
        record.line_number,
    )


def _event_at(payload: Mapping[str, object]) -> datetime | None:
    event = payload.get("event")
    fields = {
        "virtual_closed": ("closed_at",),
        "virtual_opened": ("opened_at",),
        "virtual_horizon_outcome": ("observed_at",),
    }.get(str(event), ())
    for field in (
        *fields,
        "evaluated_at",
        "terminal_at",
        "armed_at",
        "confirmed_at",
        "closed_at",
        "opened_at",
        "observed_at",
        "at",
        "updated_at",
    ):
        parsed = _parse_time(payload.get(field))
        if parsed is not None:
            return parsed
    return None


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _parse_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _unique(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))
