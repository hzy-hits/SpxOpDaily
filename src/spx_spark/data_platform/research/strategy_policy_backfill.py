"""Offline Pass-A backfill: ManagementPolicy labels from persisted decisions + quote lake.

One-shot research script. Not a systemd service. Writes parquet under
``features/strategy_policy_labels/`` when ``--output-root`` is provided.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    PolicyMark,
    management_policy_for_candidate,
    policy_mark_horizon_end,
    simulate_management_policy,
)
from spx_spark.data_platform.research.odte_level_quotes import QuoteStore


def backfill_policy_labels(
    *,
    database_path: Path,
    data_root: Path,
    session_date: str | None = None,
    lookforward_minutes: int = 20,
) -> list[dict[str, Any]]:
    """Label each persisted decision (selected or nearest shadow) with policy PnL."""

    decisions = _read_persisted_decisions(
        database_path=database_path, session_date=session_date
    )
    store = QuoteStore(data_root)
    rows: list[dict[str, Any]] = []
    try:
        for decision in decisions:
            labeled = _label_decision(
                decision, store=store, lookforward_minutes=lookforward_minutes
            )
            if labeled is not None:
                rows.append(labeled)
    finally:
        store.close()
    accepted = resolve_accepted_opportunity_ids(rows)
    # Enforce outbox-first only when the outbox actually holds accepted cards.
    # Empty/unavailable outbox (typical offline historical backfill) falls back
    # to earliest-by-time so research labeling remains usable.
    if accepted:
        return mark_duplicate_opportunities(rows, accepted_opportunity_ids=accepted)
    return mark_duplicate_opportunities(rows)


def _label_decision(
    decision: Mapping[str, Any],
    *,
    store: QuoteStore,
    lookforward_minutes: int,
) -> dict[str, Any] | None:
    decision_at = _time(decision.get("decision_at"))
    if decision_at is None:
        return None
    candidate = _map(decision.get("candidate")) or _map(
        _map(decision.get("why_not")).get("nearest_candidate")
    )
    if not candidate:
        return None
    legs = _candidate_legs(candidate)
    if len(legs) < 2:
        return None
    entry_ask = _entry_ask(legs)
    if entry_ask is None:
        return None
    provider = str(legs[0].get("provider") or "schwab")
    session = _session_date(decision.get("session_date"))
    policy = management_policy_for_candidate(candidate)
    end = policy_mark_horizon_end(
        decision_at,
        policy,
        session_date=session,
        lookforward_minutes=(
            None if policy.time_stop_minutes is None else lookforward_minutes
        ),
    )
    marks = _combo_bid_marks(
        store,
        legs=legs,
        provider=provider,
        start=decision_at,
        end=end,
    )
    if not marks:
        return None
    label = simulate_management_policy(
        marks,
        entry_ask=entry_ask,
        leg_count=len(legs),
        entry_at=decision_at,
        policy=policy,
        session_date=session,
    )
    regime = _map(decision.get("regime"))
    return {
        "schema_version": "strategy_policy_label.v1",
        "pass": "A",
        "decision_id": decision.get("decision_id"),
        "event_key": decision.get("event_key"),
        "session_date": decision.get("session_date"),
        "decision_at": decision_at.isoformat(),
        "available_at": decision.get("available_at"),
        "decision_type": decision.get("decision_type"),
        "strategy_type": candidate.get("strategy_type"),
        "direction": candidate.get("direction"),
        "setup_kind": candidate.get("setup_kind"),
        "opportunity_id": candidate.get("opportunity_id"),
        "shadow_only": bool(candidate.get("shadow_only")),
        "entry_ask": entry_ask,
        "leg_count": len(legs),
        "mark_count": len(marks),
        "regime_terminal_state": regime.get("terminal_state") or regime.get("path_state"),
        "policy_version": label.policy_version,
        "tp_armed": label.tp_armed,
        "tp_before_stop": label.tp_before_stop,
        "time_to_arm_seconds": label.time_to_arm_seconds,
        "mfe_points": label.mfe_points,
        "mae_points": label.mae_points,
        "policy_pnl_points": label.policy_pnl_points,
        "exit_reason": label.exit_reason,
        "exit_at": label.exit_at.isoformat() if label.exit_at else None,
        "exit_bid": label.exit_bid,
        "quote_gap_seconds_max": label.quote_gap_seconds_max,
        "fees_points": label.fees_points,
        "known_bias": "pass_a_uses_persisted_candidate_or_nearest_shadow",
    }


def _combo_bid_marks(
    store: QuoteStore,
    *,
    legs: Sequence[Mapping[str, Any]],
    provider: str,
    start: datetime,
    end: datetime,
) -> list[PolicyMark]:
    series = []
    for leg in legs:
        expiry = _expiry(leg)
        strike = _number(leg.get("strike"))
        right = str(leg.get("right") or "").upper()
        quantity = _number(leg.get("quantity"))
        if expiry is None or strike is None or right not in {"C", "P"} or quantity is None:
            return []
        ticks = store.option_series(
            provider=provider,
            expiry=expiry,
            strike=strike,
            right=right,
            start=start,
            end=end,
        )
        series.append((quantity, ticks))
    if any(not ticks for _, ticks in series):
        return []

    # Align on union of timestamps; use last-known bid/ask per leg.
    times = sorted({tick.at for _, ticks in series for tick in ticks})
    cursors = [0] * len(series)
    last: list[tuple[float | None, float | None]] = [(None, None)] * len(series)
    marks: list[PolicyMark] = []
    for at in times:
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        complete = True
        for index, (quantity, ticks) in enumerate(series):
            while cursors[index] < len(ticks) and ticks[cursors[index]].at <= at:
                tick = ticks[cursors[index]]
                last[index] = (tick.bid, tick.ask)
                cursors[index] += 1
            bid, ask = last[index]
            if bid is None or ask is None:
                complete = False
                break
        if not complete:
            continue
        credit = 0.0
        for (quantity, _), (bid, ask) in zip(series, last, strict=True):
            assert bid is not None and ask is not None
            credit += float(quantity) * (bid if quantity > 0 else ask)
        marks.append(PolicyMark(at=at.astimezone(timezone.utc), combo_bid=max(credit, 0.0)))
    return marks


def _candidate_legs(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = candidate.get("legs")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)) and raw:
        legs = [dict(_map(item)) for item in raw]
        if all(_number(leg.get("quantity")) is None for leg in legs) and len(legs) == 3:
            for leg, quantity in zip(legs, (1.0, -2.0, 1.0), strict=True):
                leg["quantity"] = quantity
        return legs
    long, short = dict(_map(candidate.get("long"))), dict(_map(candidate.get("short")))
    if not long or not short:
        return []
    long["quantity"] = 1.0
    short["quantity"] = -1.0
    return [long, short]


def _entry_ask(legs: Sequence[Mapping[str, Any]]) -> float | None:
    values = []
    for leg in legs:
        quantity, bid, ask = (_number(leg.get(key)) for key in ("quantity", "bid", "ask"))
        if quantity is None or bid is None or ask is None:
            return None
        values.append(quantity * (ask if quantity > 0 else bid))
    debit = sum(values)
    return round(debit, 4) if debit > 0 else None


def write_labels_parquet(rows: Sequence[Mapping[str, Any]], output_root: Path) -> Path | None:
    if not rows:
        return None

    by_session: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        session = str(row.get("session_date") or "unknown")
        by_session.setdefault(session, []).append(dict(row))
    written = None
    for session, items in sorted(by_session.items()):
        directory = (
            output_root
            / "features"
            / "strategy_policy_labels"
            / f"date={session}"
        )
        directory.mkdir(parents=True, exist_ok=True)
        jsonl = directory / "labels.jsonl"
        with jsonl.open("w", encoding="utf-8") as handle:
            for item in items:
                handle.write(json.dumps(item, ensure_ascii=False, sort_keys=True, default=str) + "\n")
        path = directory / "labels.parquet"
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.Table.from_pylist(items)
            pq.write_table(table, path)
            written = path
        except ImportError:
            import duckdb

            con = duckdb.connect()
            try:
                con.execute(
                    f"COPY (SELECT * FROM read_json_auto('{jsonl.as_posix()}')) "
                    f"TO '{path.as_posix()}' (FORMAT PARQUET)"
                )
            finally:
                con.close()
            written = path
    return written


def build_policy_ev_table(
    rows: Sequence[Mapping[str, Any]],
    *,
    database_path: Path,
    session_date: str | None = None,
) -> dict[str, Any]:
    decisions = _deduped_persisted_decisions(
        database_path=database_path,
        session_date=session_date,
    )
    labeled_ids = {
        str(row.get("decision_id") or "")
        for row in rows
        if _row_counts_for_policy_ev(row)
    }
    censored_ids = _censored_decision_ids(
        database_path=database_path,
        session_date=session_date,
    )
    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"values": [], "n_censored": 0}
    )
    source_sessions = {
        str(row.get("session_date") or "")
        for row in rows
        if str(row.get("session_date") or "").strip()
    }
    source_sessions.update(
        str(decision.get("session_date") or "")
        for decision in decisions
        if str(decision.get("session_date") or "").strip()
    )

    for row in rows:
        if not _row_counts_for_policy_ev(row):
            continue
        policy_pnl = _number(row.get("policy_pnl_points"))
        if policy_pnl is None:
            continue
        key = _policy_ev_bucket_key(row)
        buckets[key]["values"].append(float(policy_pnl))

    for decision in decisions:
        decision_id = str(decision.get("decision_id") or "")
        if (
            not decision_id
            or decision_id in labeled_ids
            or decision_id not in censored_ids
            or not _row_counts_for_policy_ev(decision)
        ):
            continue
        key = _policy_ev_bucket_key(decision)
        buckets[key]["n_censored"] += 1

    return {
        "schema_version": "policy_ev_table.v1",
        "management_policy_version": DEFAULT_MANAGEMENT_POLICY.policy_version,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "source_sessions": sorted(source_sessions),
        "buckets": {
            key: _policy_ev_bucket_summary(value["values"], value["n_censored"])
            for key, value in sorted(buckets.items())
        },
    }


def write_policy_ev_table(table: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(table, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def outcome_censor_distribution(
    *,
    database_path: Path,
    session_date: str | None = None,
) -> dict[str, int]:
    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute(
            """
            SELECT o.status, o.attributes_json
            FROM outcomes o
            LEFT JOIN decisions d ON d.decision_id = o.decision_id
            WHERE (? IS NULL OR d.session_date = ?)
            """,
            (session_date, session_date),
        ).fetchall()
    finally:
        connection.close()
    counts: dict[str, int] = {}
    for status, attributes_json in rows:
        normalized, censor_kind = _normalized_outcome_status(
            status, _json_map(attributes_json)
        )
        if normalized != "censored" or censor_kind is None:
            continue
        counts[censor_kind] = counts.get(censor_kind, 0) + 1
    return counts


def resolve_accepted_opportunity_ids(
    rows: Sequence[Mapping[str, Any]],
) -> set[str] | None:
    """Return opportunity ids accepted by the notification outbox.

    Returns ``None`` when the outbox cannot be consulted (disabled / missing
    settings), so callers can fall back to earliest-by-time dedupe.
    """

    try:
        from spx_spark.config import NotificationSettings
        from spx_spark.notifier.dispatcher import notification_event_exists
    except Exception:
        return None
    try:
        settings = NotificationSettings.from_env()
    except Exception:
        return None
    accepted: set[str] = set()
    for row in rows:
        opportunity_id = _opportunity_id(row)
        if not opportunity_id:
            continue
        try:
            if notification_event_exists(settings, f"{opportunity_id}:ready"):
                accepted.add(opportunity_id)
        except Exception:
            return None
    return accepted


def mark_duplicate_opportunities(
    rows: Sequence[Mapping[str, Any]],
    *,
    accepted_opportunity_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Mark later decisions that share an opportunity identity.

    When ``accepted_opportunity_ids`` is provided, only opportunities that the
    outbox actually accepted get a primary row (the earliest decision_at).
    Decisions for unaccepted opportunities keep ``duplicate_of=None`` but set
    ``outbox_accepted=False`` so aggregates can exclude them. When the set is
    omitted, fall back to earliest-by-time (research offline mode).
    """

    ordered = sorted(
        rows,
        key=lambda item: (str(item.get("decision_at") or ""), str(item.get("decision_id") or "")),
    )
    first_by_event: dict[str, str] = {}
    for row in ordered:
        event_key = str(row.get("event_key") or "").strip()
        decision_id = str(row.get("decision_id") or "").strip()
        if not event_key or not decision_id:
            continue
        opportunity_id = _opportunity_id(row)
        if accepted_opportunity_ids is not None and opportunity_id not in accepted_opportunity_ids:
            continue
        first_by_event.setdefault(event_key, decision_id)
    result: list[dict[str, Any]] = []
    for row in ordered:
        event_key = str(row.get("event_key") or "").strip()
        decision_id = str(row.get("decision_id") or "").strip()
        opportunity_id = _opportunity_id(row)
        labeled = dict(row)
        primary = first_by_event.get(event_key)
        if primary is None:
            labeled["duplicate_of"] = None
            labeled["outbox_accepted"] = False if accepted_opportunity_ids is not None else None
        elif primary == decision_id:
            labeled["duplicate_of"] = None
            labeled["outbox_accepted"] = True if accepted_opportunity_ids is not None else None
        else:
            labeled["duplicate_of"] = primary
            labeled["outbox_accepted"] = True if accepted_opportunity_ids is not None else None
        if opportunity_id:
            labeled.setdefault("opportunity_id", opportunity_id)
        result.append(labeled)
    return result


def _opportunity_id(row: Mapping[str, Any]) -> str:
    direct = str(row.get("opportunity_id") or "").strip()
    if direct:
        return direct
    candidate = row.get("candidate") if isinstance(row.get("candidate"), Mapping) else {}
    from_candidate = str(candidate.get("opportunity_id") or "").strip()
    if from_candidate:
        return from_candidate
    event_key = str(row.get("event_key") or "").strip()
    prefix = "strategy-opportunity:"
    if event_key.startswith(prefix):
        # event_key = strategy-opportunity:{session_date}:{opportunity_id}
        # opportunity_id itself may contain colons; strip session_date only.
        rest = event_key[len(prefix) :]
        parts = rest.split(":", 1)
        if len(parts) == 2:
            return parts[1]
    return ""


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--session-date", type=str, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--emit-ev-table", type=Path, default=None)
    parser.add_argument("--lookforward-minutes", type=int, default=20)
    args = parser.parse_args(argv)
    rows = backfill_policy_labels(
        database_path=args.database,
        data_root=args.data_root,
        session_date=args.session_date,
        lookforward_minutes=args.lookforward_minutes,
    )
    summary = {
        "labeled": len(rows),
        "session_date": args.session_date,
        "policy_version": DEFAULT_MANAGEMENT_POLICY.policy_version,
        "censor_distribution": outcome_censor_distribution(
            database_path=args.database,
            session_date=args.session_date,
        ),
    }
    if args.output_root is not None:
        path = write_labels_parquet(rows, args.output_root)
        summary["output"] = str(path) if path else None
    if args.emit_ev_table is not None:
        path = write_policy_ev_table(
            build_policy_ev_table(
                rows,
                database_path=args.database,
                session_date=args.session_date,
            ),
            args.emit_ev_table,
        )
        summary["ev_table_output"] = str(path)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _read_persisted_decisions(
    *,
    database_path: Path,
    session_date: str | None,
) -> list[dict[str, Any]]:
    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute(
            """
            SELECT decision_id, event_key, attributes_json
            FROM decisions
            WHERE strategy_name = 'strategy_signal_engine_v2'
              AND status IN ('selected', 'no_trade')
              AND (? IS NULL OR session_date = ?)
            ORDER BY decision_at, decision_id
            """,
            (session_date, session_date),
        ).fetchall()
    finally:
        connection.close()
    result: list[dict[str, Any]] = []
    for decision_id, event_key, attributes_json in rows:
        payload = _json_map(attributes_json)
        result.append(
            {
                **payload,
                "decision_id": decision_id,
                "event_key": event_key,
            }
        )
    return result


def _json_map(value: object) -> Mapping[str, Any]:
    if not isinstance(value, str):
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return _map(decoded)


def _normalized_outcome_status(
    status: object,
    attributes: Mapping[str, Any],
) -> tuple[str, str | None]:
    text = str(status or "").strip().lower()
    if text == "censored":
        censor_kind = str(attributes.get("censor_kind") or "").strip().lower() or None
        return "censored", censor_kind
    if text == "exit_quote_unavailable":
        return "censored", "quote_gap"
    return text or "unknown", None


def _deduped_persisted_decisions(
    *,
    database_path: Path,
    session_date: str | None,
) -> list[dict[str, Any]]:
    decisions = _read_persisted_decisions(
        database_path=database_path,
        session_date=session_date,
    )
    accepted = resolve_accepted_opportunity_ids(decisions)
    # Match backfill_policy_labels: empty/unavailable outbox falls back to
    # earliest-by-time so offline historical EV tables remain usable.
    if accepted:
        return mark_duplicate_opportunities(
            decisions,
            accepted_opportunity_ids=accepted,
        )
    return mark_duplicate_opportunities(decisions)


def _censored_decision_ids(
    *,
    database_path: Path,
    session_date: str | None,
) -> set[str]:
    """Decisions censored at the ManagementPolicy time-stop horizon (20m).

    Shorter-horizon transient quote gaps do not count toward n_censored.
    """

    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute(
            """
            SELECT o.decision_id, o.status, o.attributes_json
            FROM outcomes o
            LEFT JOIN decisions d ON d.decision_id = o.decision_id
            WHERE o.horizon_minutes = ?
              AND (? IS NULL OR d.session_date = ?)
            """,
            (DEFAULT_MANAGEMENT_POLICY.time_stop_minutes, session_date, session_date),
        ).fetchall()
    finally:
        connection.close()
    censored: set[str] = set()
    for decision_id, status, attributes_json in rows:
        normalized, _ = _normalized_outcome_status(
            status,
            _json_map(attributes_json),
        )
        if normalized == "censored" and decision_id:
            censored.add(str(decision_id))
    return censored


def _row_counts_for_policy_ev(row: Mapping[str, Any]) -> bool:
    duplicate_of = str(row.get("duplicate_of") or "").strip()
    outbox_accepted = row.get("outbox_accepted")
    return not duplicate_of and outbox_accepted is not False


def _policy_ev_bucket_key(row: Mapping[str, Any]) -> str:
    candidate = _map(row.get("candidate")) or _map(_map(row.get("why_not")).get("nearest_candidate"))
    regime = _map(row.get("regime"))
    return "|".join(
        (
            _bucket_dimension(row.get("setup_kind") or candidate.get("setup_kind")),
            _bucket_dimension(row.get("direction") or candidate.get("direction")),
            _bucket_dimension(
                row.get("regime_terminal_state")
                or regime.get("terminal_state")
                or regime.get("path_state")
            ),
        )
    )


def _bucket_dimension(value: object) -> str:
    text = str(value or "").strip()
    return text if text else "unknown"


def _policy_ev_bucket_summary(
    values: Sequence[float],
    n_censored: int,
) -> dict[str, Any]:
    usable = sorted(float(value) for value in values)
    n = len(usable)
    if n < 20:
        return {
            "n": n,
            "ev_points": None,
            "p25": None,
            "p75": None,
            "n_censored": n_censored,
            "reason": "low_sample",
        }
    return {
        "n": n,
        "ev_points": round(sum(usable) / n, 6),
        "p25": round(_percentile(usable, 0.25), 6),
        "p75": round(_percentile(usable, 0.75), 6),
        "n_censored": n_censored,
        "reason": None,
    }


def _percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    if len(values) == 1:
        return float(values[0])
    position = (len(values) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return float(values[lower]) * (1.0 - weight) + float(values[upper]) * weight


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, (str, datetime)):
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _session_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _expiry(leg: Mapping[str, Any]) -> date | None:
    raw = leg.get("expiry")
    if raw:
        text = str(raw)
        try:
            if len(text) == 8 and text.isdigit():
                return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
            return date.fromisoformat(text)
        except ValueError:
            pass
    contract = str(leg.get("contract_id") or "")
    parts = contract.split(":")
    if len(parts) >= 6:
        text = parts[-3]
        if len(text) == 8 and text.isdigit():
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return None


if __name__ == "__main__":
    raise SystemExit(main())
