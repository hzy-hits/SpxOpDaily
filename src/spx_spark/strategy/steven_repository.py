"""Durable state and episode repository for Steven guidance."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from spx_spark.state_io import atomic_write_json_secure
from spx_spark.strategy.steven_models import (
    EPISODE_SCHEMA_VERSION,
    RETROSPECTIVE_SOURCES_ALLOWED,
    STATE_SCHEMA_VERSION,
    StevenSettings,
    StevenSignal,
    _as_utc,
)
from spx_spark.strategy.steven import episode_id_for


def load_steven_state(path: Path | str) -> tuple[dict[str, Any] | None, str | None]:
    """Load steven_state.json. Returns (payload, reset_reason)."""
    state_path = Path(path)
    if not state_path.exists():
        return None, "missing"
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"corrupt:{type(exc).__name__}"
    if not isinstance(payload, dict):
        return None, "corrupt:not_object"
    if payload.get("schema_version") != STATE_SCHEMA_VERSION:
        return None, "schema_mismatch"
    return payload, None


def _parse_optional_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return _as_utc(datetime.fromisoformat(str(value)))
    except (TypeError, ValueError):
        return None


def persist_steven_state(
    signal: StevenSignal,
    *,
    data_root: Path | str,
    trading_date: str,
    episode_seq_last: int,
    previous_payload: Mapping[str, Any] | None = None,
    transition_rule: str | None = None,
) -> dict[str, Any]:
    path = Path(data_root) / "latest" / "steven_state.json"
    history: list[dict[str, Any]] = []
    if isinstance(previous_payload, Mapping):
        raw_history = previous_payload.get("transition_history")
        if isinstance(raw_history, list):
            history = [row for row in raw_history if isinstance(row, dict)]
    prev_state = None
    if isinstance(previous_payload, Mapping):
        prev_state = previous_payload.get("machine_state")
    if transition_rule and prev_state and prev_state != signal.machine_state:
        history.append(
            {
                "at": signal.as_of.isoformat(),
                "from": prev_state,
                "to": signal.machine_state,
                "rule": transition_rule,
            }
        )
    history = history[-50:]
    state_since = signal.as_of.isoformat()
    if (
        isinstance(previous_payload, Mapping)
        and previous_payload.get("machine_state") == signal.machine_state
        and previous_payload.get("state_since")
    ):
        state_since = str(previous_payload.get("state_since"))
    payload = {
        "schema_version": STATE_SCHEMA_VERSION,
        "trading_date": trading_date,
        "machine_state": signal.machine_state,
        "state_since": state_since,
        "updated_at": signal.as_of.isoformat(),
        "episode_id": episode_id_for(trading_date),
        "episode_seq_last": episode_seq_last,
        "daily_setup_count": signal.daily_setup_count,
        "consumed_event_tags": list(signal.consumed_event_tags),
        "lockout_until": signal.lockout_until.isoformat() if signal.lockout_until else None,
        "data_healthy_since": (
            signal.data_healthy_since.isoformat() if signal.data_healthy_since else None
        ),
        "watch_exit_since": (
            signal.watch_exit_since.isoformat() if signal.watch_exit_since else None
        ),
        "contract": signal.to_dict(),
        "transition_history": history,
    }
    atomic_write_json_secure(path, payload)
    return payload


def append_episode_event(
    *,
    data_root: Path | str,
    trading_date: str,
    seq: int,
    recorded_at: datetime,
    event_kind: str,
    from_state: str | None,
    to_state: str,
    contract: Mapping[str, Any],
    note: str,
) -> dict[str, Any]:
    recorded = _as_utc(recorded_at)
    contract_as_of = _parse_optional_dt(contract.get("as_of"))
    if contract_as_of is not None and recorded < contract_as_of:
        raise ValueError("retrospective episode timestamps are not allowed")
    if RETROSPECTIVE_SOURCES_ALLOWED:
        raise ValueError("retrospective sources must remain disabled")
    event = {
        "schema_version": EPISODE_SCHEMA_VERSION,
        "episode_id": episode_id_for(trading_date),
        "trading_date": trading_date,
        "seq": seq,
        "recorded_at": recorded.isoformat(),
        "event_kind": event_kind,
        "from_state": from_state,
        "to_state": to_state,
        "contract": dict(contract),
        "note": note,
    }
    directory = Path(data_root) / "lake" / "steven" / "episodes" / f"date={trading_date}"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "episode.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return event


def _map_level_moved(
    previous_map: Mapping[str, Any] | None,
    current_map: Mapping[str, Any],
    *,
    min_move: float,
) -> bool:
    if not isinstance(previous_map, Mapping):
        return False
    for key in ("support", "resistance", "acceleration"):
        prev_levels = list(previous_map.get(key) or [])
        curr_levels = list(current_map.get(key) or [])
        if not prev_levels or not curr_levels:
            continue
        if abs(float(prev_levels[0]) - float(curr_levels[0])) >= min_move:
            return True
    prev_pin = previous_map.get("pin")
    curr_pin = current_map.get("pin")
    if (
        prev_pin is not None
        and curr_pin is not None
        and abs(float(prev_pin) - float(curr_pin)) >= min_move
    ):
        return True
    return False


def maybe_append_episode_revision(
    *,
    data_root: Path | str,
    trading_date: str,
    signal: StevenSignal,
    previous_payload: Mapping[str, Any] | None,
    settings: StevenSettings,
) -> int:
    """Append episode row on edges / map moves. Returns new episode_seq_last."""
    seq_last = -1
    prev_state = None
    prev_contract = None
    if isinstance(previous_payload, Mapping):
        prev_date = previous_payload.get("trading_date")
        if prev_date == trading_date:
            raw_seq = previous_payload.get("episode_seq_last")
            if isinstance(raw_seq, int):
                seq_last = raw_seq
            prev_state = previous_payload.get("machine_state")
            prev_contract = previous_payload.get("contract")
        # Trading-date rollover starts a fresh episode file/seq.
    contract = signal.to_dict()
    if seq_last < 0:
        append_episode_event(
            data_root=data_root,
            trading_date=trading_date,
            seq=0,
            recorded_at=signal.as_of,
            event_kind="pre_market_map",
            from_state=None,
            to_state=signal.machine_state,
            contract=contract,
            note="initial daily evaluation",
        )
        seq_last = 0
        if prev_state and prev_state != signal.machine_state:
            # still record transition if we came from a prior day reset with same file
            pass
        return seq_last

    events: list[tuple[str, str]] = []
    if prev_state and prev_state != signal.machine_state:
        kind = "state_transition"
        if signal.machine_state == "SETUP_CONFIRMED":
            kind = "trigger"
        if signal.machine_state == "LOCKOUT_OR_REMAP" and prev_state == "EXIT_REVIEW":
            kind = "final_state"
        note = signal.transition_rule or f"{prev_state}->{signal.machine_state}"
        events.append((kind, note))
    elif isinstance(prev_contract, Mapping) and _map_level_moved(
        prev_contract.get("map") if isinstance(prev_contract.get("map"), Mapping) else None,
        signal.map,
        min_move=settings.episode_revision_min_level_move_points,
    ):
        events.append(("map_revision", "map level moved beyond threshold"))

    for kind, note in events:
        seq_last += 1
        append_episode_event(
            data_root=data_root,
            trading_date=trading_date,
            seq=seq_last,
            recorded_at=signal.as_of,
            event_kind=kind,
            from_state=str(prev_state) if prev_state else None,
            to_state=signal.machine_state,
            contract=contract,
            note=note,
        )
    return seq_last


def fold_episode_summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not events:
        return {
            "episode_id": None,
            "trading_date": None,
            "pre_market_map": None,
            "triggers": [],
            "revisions": [],
            "final_state": None,
            "setup_count": 0,
            "forward_metrics": None,
        }
    first = events[0]
    pre_market = None
    for event in events:
        if event.get("event_kind") == "pre_market_map":
            contract = event.get("contract") if isinstance(event.get("contract"), Mapping) else {}
            pre_market = {
                "map": contract.get("map"),
                "regime": contract.get("regime"),
                "data_quality": contract.get("data_quality"),
            }
            break
    triggers = [
        (event.get("contract") or {}).get("trigger")
        for event in events
        if event.get("event_kind") == "trigger"
    ]
    revisions = [
        {
            "seq": event.get("seq"),
            "from_state": event.get("from_state"),
            "to_state": event.get("to_state"),
            "recorded_at": event.get("recorded_at"),
        }
        for event in events
        if event.get("event_kind") in {"state_transition", "trigger", "map_revision", "final_state"}
    ]
    final_state = None
    for event in reversed(events):
        if event.get("event_kind") == "final_state":
            final_state = event.get("to_state")
            break
    setup_count = sum(1 for event in events if event.get("to_state") == "SETUP_CONFIRMED")
    return {
        "episode_id": first.get("episode_id"),
        "trading_date": first.get("trading_date"),
        "pre_market_map": pre_market,
        "triggers": triggers,
        "revisions": revisions,
        "final_state": final_state,
        "setup_count": setup_count,
        "forward_metrics": None,
    }
