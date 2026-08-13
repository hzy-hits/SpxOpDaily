"""SQLAlchemy Core persistence for operational strategy decisions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from sqlalchemy import Engine
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from spx_spark.app_settings import get_settings
from spx_spark.infrastructure.notifications import create_database_engine


metadata = sa.MetaData()
NEW_YORK = ZoneInfo("America/New_York")
sessions = sa.Table(
    "sessions",
    metadata,
    sa.Column("session_date", sa.Text(), primary_key=True),
    sa.Column("market", sa.Text(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False),
    sa.Column("opened_at", sa.Text()),
    sa.Column("closed_at", sa.Text()),
    sa.Column("data_quality", sa.Text(), nullable=False),
    sa.Column("metadata_json", sa.Text(), nullable=False),
    sa.Column("created_at", sa.Text(), nullable=False),
    sa.Column("updated_at", sa.Text(), nullable=False),
)
events = sa.Table(
    "events",
    metadata,
    sa.Column("event_key", sa.Text(), primary_key=True),
    sa.Column("event_type", sa.Text(), nullable=False),
    sa.Column("session_date", sa.Text(), nullable=False),
    sa.Column("source_at", sa.Text(), nullable=False),
    sa.Column("available_at", sa.Text(), nullable=False),
    sa.Column("received_at", sa.Text()),
    sa.Column("phase", sa.Text()),
    sa.Column("direction", sa.Text()),
    sa.Column("data_quality", sa.Text(), nullable=False),
    sa.Column("schema_version", sa.Integer(), nullable=False),
    sa.Column("attributes_json", sa.Text(), nullable=False),
    sa.Column("created_at", sa.Text(), nullable=False),
)
decisions = sa.Table(
    "decisions",
    metadata,
    sa.Column("decision_id", sa.Text(), primary_key=True),
    sa.Column("event_key", sa.Text()),
    sa.Column("feature_snapshot_id", sa.Text()),
    sa.Column("session_date", sa.Text()),
    sa.Column("strategy_name", sa.Text(), nullable=False),
    sa.Column("strategy_version", sa.Text(), nullable=False),
    sa.Column("decision_at", sa.Text(), nullable=False),
    sa.Column("available_at", sa.Text(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False),
    sa.Column("action", sa.Text(), nullable=False),
    sa.Column("side", sa.Text(), nullable=False),
    sa.Column("reason", sa.Text()),
    sa.Column("gamma_regime", sa.Text()),
    sa.Column("attributes_json", sa.Text(), nullable=False),
    sa.Column("created_at", sa.Text(), nullable=False),
)
decision_legs = sa.Table(
    "decision_legs",
    metadata,
    sa.Column("decision_id", sa.Text(), primary_key=True),
    sa.Column("leg_index", sa.Integer(), primary_key=True),
    sa.Column("instrument_id", sa.Text(), nullable=False),
    sa.Column("right_code", sa.Text()),
    sa.Column("expiry", sa.Text()),
    sa.Column("strike", sa.Float()),
    sa.Column("quantity", sa.Float()),
    sa.Column("bid", sa.Float()),
    sa.Column("ask", sa.Float()),
    sa.Column("delta", sa.Float()),
    sa.Column("gamma", sa.Float()),
    sa.Column("theta", sa.Float()),
    sa.Column("vega", sa.Float()),
    sa.Column("quote_source_at", sa.Text(), nullable=False),
    sa.Column("quote_available_at", sa.Text(), nullable=False),
    sa.Column("attributes_json", sa.Text(), nullable=False),
    sa.Column("created_at", sa.Text(), nullable=False),
)
outcomes = sa.Table(
    "outcomes",
    metadata,
    sa.Column("outcome_id", sa.Text(), primary_key=True),
    sa.Column("event_key", sa.Text(), nullable=False),
    sa.Column("decision_id", sa.Text()),
    sa.Column("horizon_minutes", sa.Integer(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False),
    sa.Column("target_at", sa.Text(), nullable=False),
    sa.Column("sampled_at", sa.Text()),
    sa.Column("hypothesis_direction", sa.Text()),
    sa.Column("spx_return_bps", sa.Float()),
    sa.Column("spx_mfe_bps", sa.Float()),
    sa.Column("spx_mae_bps", sa.Float()),
    sa.Column("option_return_bps", sa.Float()),
    sa.Column("option_pnl", sa.Float()),
    sa.Column("attributes_json", sa.Text(), nullable=False),
    sa.Column("created_at", sa.Text(), nullable=False),
)
compaction_manifests = sa.Table(
    "compaction_manifests",
    metadata,
    sa.Column("manifest_id", sa.Text(), primary_key=True),
    sa.Column("source_path", sa.Text(), nullable=False),
    sa.Column("source_sha256", sa.Text(), nullable=False),
    sa.Column("source_size", sa.Integer(), nullable=False),
    sa.Column("source_mtime_ns", sa.Integer(), nullable=False),
    sa.Column("output_path", sa.Text()),
    sa.Column("output_sha256", sa.Text()),
    sa.Column("row_count", sa.Integer(), nullable=False),
    sa.Column("min_received_at", sa.Text()),
    sa.Column("max_received_at", sa.Text()),
    sa.Column("schema_version", sa.Text(), nullable=False),
    sa.Column("writer_version", sa.Text(), nullable=False),
    sa.Column("dataset", sa.Text(), nullable=False),
    sa.Column("completed_at", sa.Text(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False),
    sa.Column("created_at", sa.Text(), nullable=False),
)


class OperationalDecisionConflict(RuntimeError):
    """A stable decision id was retried with different immutable content."""


@lru_cache(maxsize=8)
def _engine(path: str) -> Engine:
    return create_database_engine(Path(path))


def persist_strategy_decision(
    decision: Mapping[str, object],
    *,
    database_path: str | Path | None = None,
) -> str:
    """Atomically persist one strategy decision and its frozen execution legs."""

    row, legs = _decision_rows(decision)
    event_row = _strategy_opportunity_event_row(decision, created_at=str(row["created_at"]))
    path = Path(database_path) if database_path is not None else get_settings().data_root / "spx.sqlite"
    engine = _engine(str(path))
    with engine.begin() as connection:
        if event_row is not None:
            connection.execute(sqlite_insert(events).values(event_row).on_conflict_do_nothing())
        connection.execute(sqlite_insert(decisions).values(row).on_conflict_do_nothing())
        stored = connection.execute(
            sa.select(decisions).where(decisions.c.decision_id == row["decision_id"])
        ).mappings().one()
        _assert_same("decision", stored, row)
        for leg in legs:
            connection.execute(
                sqlite_insert(decision_legs).values(leg).on_conflict_do_nothing()
            )
            stored_leg = connection.execute(
                sa.select(decision_legs).where(
                    decision_legs.c.decision_id == leg["decision_id"],
                    decision_legs.c.leg_index == leg["leg_index"],
                )
            ).mappings().one()
            _assert_same("decision leg", stored_leg, leg)
        stored_count = connection.execute(
            sa.select(sa.func.count()).select_from(decision_legs).where(
                decision_legs.c.decision_id == row["decision_id"]
            )
        ).scalar_one()
        if stored_count != len(legs):
            raise OperationalDecisionConflict("conflicting immutable decision leg set")
    return str(row["decision_id"])


def read_strategy_decisions(
    *,
    database_path: str | Path,
    session_date: str | None = None,
) -> tuple[dict[str, object], ...]:
    """Read persisted strategy payloads for replay without consulting JSON exports."""

    statement = (
        sa.select(decisions.c.attributes_json)
        .where(decisions.c.status.in_(("selected", "no_trade")))
        .order_by(decisions.c.decision_at)
    )
    if session_date is not None:
        statement = statement.where(decisions.c.session_date == session_date)
    with _engine(str(database_path)).begin() as connection:
        rows = connection.execute(statement).scalars().all()
    return tuple(json.loads(value) for value in rows)


def persist_strategy_shadow_candidates(
    decision: Mapping[str, object],
    *,
    database_path: str | Path | None = None,
) -> tuple[str, ...]:
    """Persist rank-2/3 shadow candidates as immutable decision rows."""

    raw_candidates = decision.get("shadow_candidates")
    if not isinstance(raw_candidates, Sequence) or isinstance(raw_candidates, (str, bytes)):
        return ()
    candidates = tuple(_mapping(item) for item in raw_candidates if _mapping(item))
    if not candidates:
        return ()
    path = Path(database_path) if database_path is not None else get_settings().data_root / "spx.sqlite"
    engine = _engine(str(path))
    persisted_ids: list[str] = []
    with engine.begin() as connection:
        for rank, candidate in enumerate(candidates, start=1):
            try:
                row, legs = _shadow_candidate_rows(decision, candidate, rank=rank)
            except ValueError:
                continue
            event_row = _strategy_opportunity_event_row(
                decision, created_at=str(row["created_at"])
            )
            if event_row is not None:
                connection.execute(
                    sqlite_insert(events).values(event_row).on_conflict_do_nothing()
                )
            connection.execute(sqlite_insert(decisions).values(row).on_conflict_do_nothing())
            stored = connection.execute(
                sa.select(decisions).where(decisions.c.decision_id == row["decision_id"])
            ).mappings().one()
            _assert_same("shadow decision", stored, row)
            for leg in legs:
                connection.execute(
                    sqlite_insert(decision_legs).values(leg).on_conflict_do_nothing()
                )
                stored_leg = connection.execute(
                    sa.select(decision_legs).where(
                        decision_legs.c.decision_id == leg["decision_id"],
                        decision_legs.c.leg_index == leg["leg_index"],
                    )
                ).mappings().one()
                _assert_same("shadow decision leg", stored_leg, leg)
            stored_count = connection.execute(
                sa.select(sa.func.count()).select_from(decision_legs).where(
                    decision_legs.c.decision_id == row["decision_id"]
                )
            ).scalar_one()
            if stored_count != len(legs):
                raise OperationalDecisionConflict("conflicting immutable shadow decision leg set")
            persisted_ids.append(str(row["decision_id"]))
    return tuple(persisted_ids)


def recent_selected_strategy_cards(
    *,
    session_date: str,
    exclude_decision_id: str | None = None,
    database_path: str | Path | None = None,
) -> tuple[dict[str, object], ...]:
    """Return prior selected candidates for flood control.

    Rows carry the candidate flood identity only. Whether a card actually
    reached the human (outbox accepted) is decided by the delivery layer;
    decisions that were produced but never accepted must not consume quota.
    """

    path = Path(database_path) if database_path is not None else get_settings().data_root / "spx.sqlite"
    with _engine(str(path)).begin() as connection:
        rows = connection.execute(
            sa.select(
                decisions.c.decision_id,
                decisions.c.decision_at,
                decisions.c.attributes_json,
            ).where(
                decisions.c.strategy_name == "strategy_signal_engine_v2",
                decisions.c.session_date == session_date,
                decisions.c.status == "selected",
            )
        ).mappings().all()
    cards: list[dict[str, object]] = []
    for row in rows:
        if exclude_decision_id and str(row["decision_id"]) == exclude_decision_id:
            continue
        payload = json.loads(row["attributes_json"])
        candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
        trigger = candidate.get("trigger_level")
        cards.append(
            {
                "decision_id": str(row["decision_id"]),
                "decision_at": _time(row["decision_at"], "decision_at"),
                "opportunity_id": str(candidate.get("opportunity_id") or ""),
                "direction": str(candidate.get("direction") or ""),
                "setup_kind": str(candidate.get("setup_kind") or ""),
                "trigger_level": float(trigger) if isinstance(trigger, (int, float)) else None,
                "session_mode": _strategy_card_session_mode(payload, candidate),
            }
        )
    return tuple(cards)


def _strategy_card_session_mode(
    payload: Mapping[str, object], candidate: Mapping[str, object]
) -> str:
    facts = payload.get("market_facts") if isinstance(payload.get("market_facts"), dict) else {}
    session = facts.get("session") if isinstance(facts, dict) and isinstance(facts.get("session"), dict) else {}
    mode = str(session.get("mode") or "").strip().lower()
    if mode in {"gth", "rth"}:
        return mode
    setup = str(candidate.get("setup_kind") or "")
    if setup.startswith("GTH_"):
        return "gth"
    return "rth"


def read_due_strategy_observations(
    *,
    now: datetime,
    horizon_minutes: int | Sequence[int] = 5,
    maximum_lag_seconds: float = 90.0,
    limit: int = 100,
    database_path: str | Path | None = None,
) -> tuple[dict[str, object], ...]:
    """Load recent decisions with frozen legs that need causal exit marks.

    ``horizon_minutes`` may be a single int (legacy) or a sequence. One SQL
    window covers all horizons; each (decision, horizon) pair that is due and
    not yet observed is returned.
    """

    horizons = _normalize_horizons(horizon_minutes)
    if maximum_lag_seconds <= 0 or limit <= 0:
        raise ValueError("strategy observation window must be positive")
    observed_at = now.astimezone(timezone.utc)
    max_horizon = max(horizons)
    # Bound the scan so multi-day service_gap backlog cannot starve fresh marks.
    # Lower bound = max horizon + lag + a short retention window: long enough for
    # overdue pairs to be labeled once as service_gap, short enough that a cold
    # start after weekend downtime cannot dump thousands of ancient rows into
    # the live path. Fresh (uncensored) rows are preferred when applying limit.
    service_gap_retention_seconds = 30 * 60.0
    earliest_decision = observed_at - timedelta(
        minutes=max_horizon,
        seconds=maximum_lag_seconds + service_gap_retention_seconds,
    )
    path = Path(database_path) if database_path is not None else get_settings().data_root / "spx.sqlite"
    has_legs = sa.exists(
        sa.select(decision_legs.c.decision_id).where(
            decision_legs.c.decision_id == decisions.c.decision_id
        )
    )
    statement = (
        sa.select(decisions.c.decision_id, decisions.c.decision_at, decisions.c.attributes_json)
        .where(
            decisions.c.strategy_name == "strategy_signal_engine_v2",
            decisions.c.decision_at <= _utc_text(observed_at),
            decisions.c.decision_at >= _utc_text(earliest_decision),
            has_legs,
        )
        .order_by(decisions.c.decision_at.desc())
        .limit(max(limit * len(horizons), limit))
    )
    fresh: list[dict[str, object]] = []
    censored: list[dict[str, object]] = []
    with _engine(str(path)).begin() as connection:
        rows = list(connection.execute(statement).mappings())
        if not rows:
            return ()
        decision_ids = [row["decision_id"] for row in rows]
        observed_pairs = {
            (item["decision_id"], int(item["horizon_minutes"]))
            for item in connection.execute(
                sa.select(outcomes.c.decision_id, outcomes.c.horizon_minutes).where(
                    outcomes.c.decision_id.in_(decision_ids),
                    outcomes.c.horizon_minutes.in_(list(horizons)),
                )
            ).mappings()
        }
        legs_by_decision: dict[str, list[dict[str, object]]] = {decision_id: [] for decision_id in decision_ids}
        for leg in connection.execute(
            sa.select(decision_legs)
            .where(decision_legs.c.decision_id.in_(decision_ids))
            .order_by(decision_legs.c.decision_id, decision_legs.c.leg_index)
        ).mappings():
            legs_by_decision[str(leg["decision_id"])].append(dict(leg))
        for row in rows:
            decision_at = _time(row["decision_at"], "decision_at")
            legs = legs_by_decision.get(str(row["decision_id"]), [])
            if not legs:
                continue
            decision_payload = json.loads(row["attributes_json"])
            for horizon in horizons:
                if (row["decision_id"], horizon) in observed_pairs:
                    continue
                target_at = decision_at + timedelta(minutes=horizon)
                session_close = _session_close_utc(decision_payload.get("session_date"))
                session_end_before_horizon = (
                    session_close is not None
                    and target_at > session_close
                    and observed_at >= session_close
                )
                lag = (observed_at - target_at).total_seconds()
                if lag < 0 and not session_end_before_horizon:
                    continue
                censor_hint = None
                if lag > maximum_lag_seconds:
                    censor_hint = "service_gap"
                elif session_end_before_horizon:
                    censor_hint = "session_end_before_horizon"
                item = {
                    "decision": decision_payload,
                    "decision_at": row["decision_at"],
                    "target_at": target_at.isoformat(),
                    "horizon_minutes": horizon,
                    "legs": legs,
                    "censor_hint": censor_hint,
                }
                if censor_hint is None:
                    fresh.append(item)
                else:
                    censored.append(item)
    # Fresh marks first so a burst of overdue service_gap rows cannot consume
    # the entire limit and starve the live labeling path.
    return tuple((fresh + censored)[:limit])


def _normalize_horizons(value: int | Sequence[int]) -> tuple[int, ...]:
    if isinstance(value, int):
        horizons = (value,)
    else:
        horizons = tuple(int(item) for item in value)
    if not horizons or any(item <= 0 for item in horizons):
        raise ValueError("strategy observation horizon must be positive")
    return tuple(dict.fromkeys(horizons))


def _session_close_utc(session_date: object) -> datetime | None:
    try:
        day = date.fromisoformat(str(session_date))
    except ValueError:
        return None
    return datetime.combine(day, time(16, 0), tzinfo=NEW_YORK).astimezone(timezone.utc)


def persist_strategy_outcome(
    value: Mapping[str, object],
    *,
    database_path: str | Path | None = None,
) -> str:
    """Atomically persist an immutable strategy mark and its audit event."""

    decision_id = _required_text(value, "decision_id")
    horizon_minutes = int(value.get("horizon_minutes") or 0)
    if horizon_minutes <= 0:
        raise ValueError("strategy outcome horizon must be positive")
    target_at = _time(value.get("target_at"), "target_at")
    sampled_at = _time(value.get("sampled_at"), "sampled_at")
    attributes = _mapping(value.get("attributes"))
    censor_kind = str(attributes.get("censor_kind") or "")
    if sampled_at < target_at and not (
        str(value.get("status") or "") == "censored"
        and censor_kind == "session_end_before_horizon"
    ):
        raise ValueError("strategy outcome cannot be sampled before target")
    outcome_id = f"strategy-outcome:{decision_id}:{horizon_minutes}m"
    event_key = f"strategy-observation:{decision_id}:{horizon_minutes}m"
    path = Path(database_path) if database_path is not None else get_settings().data_root / "spx.sqlite"
    now_text = _utc_text(datetime.now(tz=timezone.utc))
    with _engine(str(path)).begin() as connection:
        decision = connection.execute(
            sa.select(decisions).where(decisions.c.decision_id == decision_id)
        ).mappings().one()
        event_source_at = (
            sampled_at
            if str(value.get("status") or "") == "censored"
            and censor_kind == "session_end_before_horizon"
            else target_at
        )
        event_row = {
            "event_key": event_key,
            "event_type": "strategy_outcome_observation",
            "session_date": str(decision["session_date"] or ""),
            "source_at": _utc_text(event_source_at),
            "available_at": _utc_text(sampled_at),
            "received_at": _utc_text(sampled_at),
            "phase": str(value.get("status") or "unavailable"),
            "direction": str(value.get("hypothesis_direction") or "none"),
            "data_quality": "ready" if value.get("status") == "observed" else "degraded",
            "schema_version": 1,
            "attributes_json": _json(
                {"decision_id": decision_id, "horizon_minutes": horizon_minutes}
            ),
            "created_at": now_text,
        }
        connection.execute(sqlite_insert(events).values(event_row).on_conflict_do_nothing())
        stored_event = connection.execute(
            sa.select(events).where(events.c.event_key == event_key)
        ).mappings().one()
        _assert_same("strategy outcome event", stored_event, event_row)
        outcome_row = {
            "outcome_id": outcome_id,
            "event_key": event_key,
            "decision_id": decision_id,
            "horizon_minutes": horizon_minutes,
            "status": str(value.get("status") or "unavailable"),
            "target_at": _utc_text(target_at),
            "sampled_at": _utc_text(sampled_at),
            "hypothesis_direction": str(value.get("hypothesis_direction") or "none"),
            "spx_return_bps": _number(value.get("spx_return_bps")),
            "spx_mfe_bps": None,
            "spx_mae_bps": None,
            "option_return_bps": _number(value.get("option_return_bps")),
            "option_pnl": None,
            "attributes_json": _json(attributes),
            "created_at": now_text,
        }
        connection.execute(sqlite_insert(outcomes).values(outcome_row).on_conflict_do_nothing())
        stored = connection.execute(
            sa.select(outcomes).where(outcomes.c.outcome_id == outcome_id)
        ).mappings().one()
        _assert_same("strategy outcome", stored, outcome_row)
    return outcome_id


def _decision_rows(
    value: Mapping[str, object],
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    decision_id = _required_text(value, "decision_id")
    decision_at = _time(value.get("decision_at"), "decision_at")
    available_at = _time(value.get("available_at"), "available_at")
    if available_at > decision_at:
        raise ValueError("strategy decision used facts unavailable at decision time")
    candidate = _mapping(value.get("candidate"))
    execution = _mapping(value.get("execution"))
    regime = _mapping(value.get("regime"))
    why_not = _mapping(value.get("why_not"))
    nearest_candidate = _mapping(why_not.get("nearest_candidate"))
    reasons = why_not.get("reasons")
    reason = next((str(item) for item in reasons if item), None) if isinstance(reasons, list) else None
    now_text = _utc_text(datetime.now(tz=timezone.utc))
    row = {
        "decision_id": decision_id,
        "event_key": _strategy_opportunity_event_key(value),
        "session_date": str(value.get("session_date") or "") or None,
        "strategy_name": "strategy_signal_engine_v2",
        "strategy_version": _required_text(value, "policy_version"),
        "decision_at": _utc_text(decision_at),
        "available_at": _utc_text(available_at),
        "status": "selected" if candidate else "no_trade",
        "action": str(execution.get("action") or "WAIT").lower(),
        "side": str(candidate.get("direction") or "none").lower(),
        "reason": reason or str(_mapping(value.get("desk_view")).get("reason") or "") or None,
        "gamma_regime": str(regime.get("terminal_state") or regime.get("path_state") or "") or None,
        "attributes_json": _json(value),
        "created_at": now_text,
    }
    return row, _leg_rows(candidate or nearest_candidate, decision_id, available_at, now_text)


def _shadow_candidate_rows(
    parent: Mapping[str, object],
    candidate: Mapping[str, object],
    *,
    rank: int,
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    parent_id = _required_text(parent, "decision_id")
    decision_id = f"{parent_id}:cand{rank}"
    decision_at = _time(parent.get("decision_at"), "decision_at")
    available_at = _shadow_available_at(parent, candidate)
    if available_at > decision_at:
        raise ValueError("strategy decision used facts unavailable at decision time")
    policy_version = _required_text(parent, "policy_version")
    regime = _mapping(parent.get("regime"))
    execution = _mapping(parent.get("execution"))
    candidate_payload = dict(candidate)
    candidate_payload.setdefault("shadow_only", True)
    now_text = _utc_text(datetime.now(tz=timezone.utc))
    payload = {
        "schema_version": str(parent.get("schema_version") or "strategy_decision.v2"),
        "decision_id": decision_id,
        "parent_decision_id": parent_id,
        "shadow_rank": rank,
        "policy_version": policy_version,
        "decision_at": _utc_text(decision_at),
        "available_at": _utc_text(available_at),
        "session_date": str(parent.get("session_date") or "") or None,
        "decision_type": candidate.get("strategy_type"),
        "market_facts": dict(_mapping(parent.get("market_facts"))),
        "regime": dict(regime),
        "candidate": candidate_payload,
        "action_authority": "none",
        "execution": {
            "action": str(execution.get("action") or "MANUAL_LIMIT"),
            "automatic_ordering": False,
            "manual_action_only": True,
        },
    }
    row = {
        "decision_id": decision_id,
        "event_key": _strategy_opportunity_event_key(parent),
        "session_date": str(parent.get("session_date") or "") or None,
        "strategy_name": "strategy_signal_engine_v2",
        "strategy_version": policy_version,
        "decision_at": _utc_text(decision_at),
        "available_at": _utc_text(available_at),
        "status": "shadow_candidate",
        "action": str(execution.get("action") or "MANUAL_LIMIT").lower(),
        "side": str(candidate.get("direction") or "none").lower(),
        "reason": "shadow_candidate",
        "gamma_regime": str(regime.get("terminal_state") or regime.get("path_state") or "") or None,
        "attributes_json": _json(payload),
        "created_at": now_text,
    }
    return row, _leg_rows(candidate, decision_id, available_at, now_text)


def _shadow_available_at(parent: Mapping[str, object], candidate: Mapping[str, object]) -> datetime:
    available_at = _time(parent.get("available_at"), "available_at")
    raw_legs = candidate.get("legs")
    if isinstance(raw_legs, Sequence) and not isinstance(raw_legs, (str, bytes)):
        legs = tuple(_mapping(item) for item in raw_legs)
    else:
        legs = (_mapping(candidate.get("long")), _mapping(candidate.get("short")))
    times = [available_at]
    for leg in legs:
        if leg and leg.get("source_at") is not None:
            times.append(_time(leg.get("source_at"), "leg source_at"))
    return max(times)


def _strategy_opportunity_event_key(value: Mapping[str, object]) -> str | None:
    candidate = _mapping(value.get("candidate"))
    session_date = str(value.get("session_date") or "").strip()
    opportunity_id = str(candidate.get("opportunity_id") or "").strip()
    if not candidate or not session_date or not opportunity_id:
        return None
    return f"strategy-opportunity:{session_date}:{opportunity_id}"


def _strategy_opportunity_event_row(
    value: Mapping[str, object],
    *,
    created_at: str,
) -> dict[str, object] | None:
    event_key = _strategy_opportunity_event_key(value)
    if event_key is None:
        return None
    decision_at = _time(value.get("decision_at"), "decision_at")
    candidate = _mapping(value.get("candidate"))
    return {
        "event_key": event_key,
        "event_type": "strategy_opportunity",
        "session_date": str(value.get("session_date") or ""),
        "source_at": _utc_text(decision_at),
        "available_at": _utc_text(decision_at),
        "received_at": _utc_text(decision_at),
        "phase": "selected",
        "direction": str(candidate.get("direction") or "none").lower(),
        "data_quality": "ready",
        "schema_version": 1,
        "attributes_json": _json(
            {
                "decision_id": _required_text(value, "decision_id"),
                "opportunity_id": candidate.get("opportunity_id"),
                "strategy_type": candidate.get("strategy_type"),
            }
        ),
        "created_at": created_at,
    }


def _leg_rows(
    candidate: Mapping[str, object],
    decision_id: str,
    available_at: datetime,
    created_at: str,
) -> tuple[dict[str, object], ...]:
    if not candidate:
        return ()
    raw_legs = candidate.get("legs")
    if isinstance(raw_legs, Sequence) and not isinstance(raw_legs, (str, bytes)):
        legs = tuple(_mapping(item) for item in raw_legs)
        quantities = (1.0, -2.0, 1.0)
    else:
        legs = (_mapping(candidate.get("long")), _mapping(candidate.get("short")))
        quantities = (1.0, -1.0)
    if not legs or any(not leg for leg in legs) or len(legs) != len(quantities):
        raise ValueError("selected strategy decision requires a complete execution leg set")
    rows = []
    for index, (leg, quantity) in enumerate(zip(legs, quantities, strict=True)):
        instrument = str(leg.get("contract_id") or "").strip()
        if not instrument:
            raise ValueError("strategy decision leg contract_id is required")
        source_at = _time(leg.get("source_at"), "leg source_at")
        if source_at > available_at:
            raise ValueError("strategy decision leg quote was unavailable at decision time")
        expiry, strike, right = _contract_fields(instrument, leg)
        bid, ask = _number(leg.get("bid")), _number(leg.get("ask"))
        if bid is None or ask is None:
            raise ValueError("strategy decision leg requires a two-sided quote")
        rows.append(
            {
                "decision_id": decision_id,
                "leg_index": index,
                "instrument_id": instrument,
                "right_code": right,
                "expiry": expiry,
                "strike": strike,
                "quantity": quantity,
                "bid": bid,
                "ask": ask,
                "delta": _number(leg.get("delta")),
                "gamma": _number(leg.get("gamma")),
                "theta": _number(leg.get("theta")),
                "vega": _number(leg.get("vega")),
                "quote_source_at": _utc_text(source_at),
                "quote_available_at": _utc_text(available_at),
                "attributes_json": _json(
                    {"provider": leg.get("provider"), "role": "long" if quantity > 0 else "short"}
                ),
                "created_at": created_at,
            }
        )
    return tuple(rows)


def _contract_fields(
    instrument: str, leg: Mapping[str, object]
) -> tuple[str | None, float | None, str | None]:
    parts = instrument.split(":")
    expiry = str(leg.get("expiry") or "") or (parts[-3] if len(parts) >= 6 else None)
    strike = _number(leg.get("strike"))
    if strike is None and len(parts) >= 6:
        try:
            strike = float(parts[-2])
        except ValueError:
            pass
    right = str(leg.get("right") or "").upper() or (parts[-1].upper() if len(parts) >= 6 else None)
    return expiry, strike, right


def _assert_same(kind: str, stored: Mapping[str, object], expected: Mapping[str, object]) -> None:
    if any(stored[key] != value for key, value in expected.items() if key != "created_at"):
        raise OperationalDecisionConflict(f"conflicting immutable {kind}")


def _required_text(value: Mapping[str, object], key: str) -> str:
    result = str(value.get(key) or "").strip()
    if not result:
        raise ValueError(f"strategy decision {key} is required")
    return result


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _time(value: object, name: str) -> datetime:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"strategy decision {name} is invalid") from exc
    if result.tzinfo is None:
        raise ValueError(f"strategy decision {name} must be timezone-aware")
    return result.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def _json(value: Mapping[str, object]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
