from __future__ import annotations

import json
import stat
from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.position_alerts import has_open_spxw_positions
from spx_spark.position_events import (
    BOOK_PNL_EVENT_KIND,
    ObservedPosition,
    PositionEventStore,
    PositionEventStoreCorrupt,
    PositionObservation,
)


NOW = datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)


def position(strike: int, qty: float = 1.0) -> ObservedPosition:
    instrument_id = f"option:SPX:SPXW:20260710:{strike}:C"
    return ObservedPosition(
        key=f"U1|{instrument_id}",
        instrument_id=instrument_id,
        label=f"SPXW 20260710 {strike}C",
        qty=qty,
    )


def observation(
    snapshot_id: str,
    *,
    at: datetime = NOW,
    positions: tuple[ObservedPosition, ...] = (),
    fetch_complete: bool = True,
    book_pnl: float | None = None,
    book_pnl_complete: bool = False,
) -> PositionObservation:
    return PositionObservation(
        snapshot_id=snapshot_id,
        observed_at=at.isoformat(),
        fetch_complete=fetch_complete,
        positions=positions,
        book_pnl=book_pnl,
        book_pnl_pct=None,
        book_pnl_complete=book_pnl_complete,
        book_detail="SPX 7500",
    )


def test_pending_structural_event_id_is_stable_across_retry(tmp_path) -> None:
    state_path = tmp_path / "position-events.json"
    store = PositionEventStore(state_path)
    current = observation("snapshot-1", positions=(position(7500),))

    first = store.prepare(current, as_of=NOW)
    second = store.prepare(current, as_of=NOW + timedelta(seconds=1))

    assert first.accepted_snapshot is True
    assert second.accepted_snapshot is False
    assert second.rejection_reason == "snapshot_duplicate"
    assert len(first.pending_events) == 1
    assert second.pending_events[0].event_id == first.pending_events[0].event_id
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600


def test_version_one_state_migrates_without_historical_open(tmp_path) -> None:
    state_path = tmp_path / "position-events.json"
    held = position(7500)
    state_path.write_text(
        json.dumps(
            {
                "fetched_at": (NOW - timedelta(seconds=60)).isoformat(),
                "previous_qty": {held.key: held.qty},
                "book_pnl": -100.0,
            }
        ),
        encoding="utf-8",
    )
    store = PositionEventStore(state_path)

    batch = store.prepare(
        observation(
            "snapshot-2",
            at=NOW,
            positions=(held,),
            book_pnl=-100.0,
            book_pnl_complete=True,
        ),
        as_of=NOW,
    )

    assert batch.pending_events == ()
    assert batch.state.schema_version == 2
    assert batch.state.last_acknowledged_book_pnl == -100.0


def test_corrupt_state_fails_closed_without_replacing_bytes(tmp_path) -> None:
    state_path = tmp_path / "position-events.json"
    original = b"{not-json"
    state_path.write_bytes(original)
    store = PositionEventStore(state_path)

    with pytest.raises(PositionEventStoreCorrupt):
        store.prepare(
            observation("snapshot-1", positions=(position(7500),)),
            as_of=NOW,
        )

    assert state_path.read_bytes() == original


def test_malformed_state_fields_fail_closed_without_replacing_bytes(tmp_path) -> None:
    state_path = tmp_path / "position-events.json"
    original = b'{"schema_version":"bad","pending_events":[]}'
    state_path.write_bytes(original)
    store = PositionEventStore(state_path)

    with pytest.raises(PositionEventStoreCorrupt):
        store.prepare(
            observation("snapshot-1", positions=(position(7500),)),
            as_of=NOW,
        )

    assert state_path.read_bytes() == original


@pytest.mark.parametrize(
    "pending_events",
    (
        [42],
        [{}],
    ),
)
def test_invalid_pending_event_entries_fail_closed(tmp_path, pending_events) -> None:
    state_path = tmp_path / "position-events.json"
    original = json.dumps(
        {
            "schema_version": 2,
            "observed_positions": [],
            "pending_events": pending_events,
        }
    ).encode()
    state_path.write_bytes(original)
    store = PositionEventStore(state_path)

    with pytest.raises(PositionEventStoreCorrupt):
        store.prepare(None, as_of=NOW)

    assert state_path.read_bytes() == original


def test_acknowledge_removes_only_matching_event(tmp_path) -> None:
    store = PositionEventStore(tmp_path / "position-events.json")
    batch = store.prepare(
        observation("snapshot-1", positions=(position(7500), position(7510))),
        as_of=NOW,
    )
    first_id, second_id = (event.event_id for event in batch.pending_events)

    state = store.acknowledge((first_id,), as_of=NOW + timedelta(seconds=1))

    assert [event.event_id for event in state.pending_events] == [second_id]
    assert state.last_acknowledged_book_pnl is None


def test_incomplete_empty_snapshot_does_not_close_but_complete_empty_does(tmp_path) -> None:
    store = PositionEventStore(tmp_path / "position-events.json")
    opened = store.prepare(
        observation("snapshot-1", positions=(position(7500),)),
        as_of=NOW,
    )
    store.acknowledge(
        tuple(event.event_id for event in opened.pending_events),
        as_of=NOW + timedelta(seconds=1),
    )

    incomplete = store.prepare(
        observation(
            "snapshot-2",
            at=NOW + timedelta(seconds=10),
            fetch_complete=False,
        ),
        as_of=NOW + timedelta(seconds=10),
    )
    complete = store.prepare(
        observation("snapshot-3", at=NOW + timedelta(seconds=20)),
        as_of=NOW + timedelta(seconds=20),
    )

    assert incomplete.rejection_reason == "snapshot_incomplete"
    assert incomplete.pending_events == ()
    assert [event.kind for event in complete.pending_events] == ["spxw_position_closed"]


def test_pending_book_pnl_coalesces_and_ack_advances_baseline(tmp_path) -> None:
    store = PositionEventStore(tmp_path / "position-events.json")
    first = store.prepare(
        observation(
            "snapshot-1",
            positions=(position(7500),),
            book_pnl=-500.0,
            book_pnl_complete=True,
        ),
        as_of=NOW,
    )
    structural_ids = tuple(
        event.event_id for event in first.pending_events if event.kind != BOOK_PNL_EVENT_KIND
    )
    store.acknowledge(structural_ids, as_of=NOW + timedelta(seconds=1))
    first_pnl = next(
        event for event in store.load().pending_events if event.kind == BOOK_PNL_EVENT_KIND
    )

    second = store.prepare(
        observation(
            "snapshot-2",
            at=NOW + timedelta(seconds=10),
            positions=(position(7500),),
            book_pnl=-650.0,
            book_pnl_complete=True,
        ),
        as_of=NOW + timedelta(seconds=10),
    )
    second_pnl = next(event for event in second.pending_events if event.kind == BOOK_PNL_EVENT_KIND)

    assert second_pnl.event_id != first_pnl.event_id
    assert len([event for event in second.pending_events if event.kind == BOOK_PNL_EVENT_KIND]) == 1
    assert second.state.last_acknowledged_book_pnl is None

    acknowledged = store.acknowledge(
        (second_pnl.event_id,),
        as_of=NOW + timedelta(seconds=11),
    )

    assert acknowledged.pending_events == ()
    assert acknowledged.last_acknowledged_book_pnl == -650.0

    unchanged = store.prepare(
        observation(
            "snapshot-3",
            at=NOW + timedelta(seconds=20),
            positions=(position(7500),),
            book_pnl=-650.0,
            book_pnl_complete=True,
        ),
        as_of=NOW + timedelta(seconds=20),
    )

    assert unchanged.pending_events == ()


def test_disabled_event_classes_advance_baseline_without_enqueuing(tmp_path) -> None:
    store = PositionEventStore(tmp_path / "position-events.json")

    batch = store.prepare(
        observation(
            "snapshot-1",
            positions=(position(7500),),
            book_pnl=-500.0,
            book_pnl_complete=True,
        ),
        as_of=NOW,
        structural_enabled=False,
        pnl_enabled=False,
    )

    assert batch.pending_events == ()
    assert batch.state.observed_positions == (position(7500),)
    assert batch.state.last_acknowledged_book_pnl == -500.0


def test_stale_snapshot_retries_existing_pending_without_new_derivation(tmp_path) -> None:
    store = PositionEventStore(tmp_path / "position-events.json")
    first = store.prepare(
        observation("snapshot-1", positions=(position(7500),)),
        as_of=NOW,
    )

    stale = store.prepare(
        observation(
            "snapshot-2",
            at=NOW - timedelta(minutes=10),
            positions=(),
        ),
        as_of=NOW + timedelta(seconds=5),
        max_snapshot_age_seconds=180.0,
    )

    assert stale.rejection_reason == "snapshot_stale"
    assert stale.pending_events == first.pending_events


def test_incomplete_empty_snapshot_uses_durable_position_exposure(
    tmp_path,
    monkeypatch,
) -> None:
    snapshot_path = tmp_path / "positions.json"
    state_path = tmp_path / "position-events.json"
    monkeypatch.setenv("IBKR_POSITIONS_SNAPSHOT_PATH", str(snapshot_path))
    monkeypatch.setenv("IBKR_POSITIONS_STATE_PATH", str(state_path))
    PositionEventStore(state_path).prepare(
        observation("snapshot-1", positions=(position(7500),)),
        as_of=NOW,
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "snapshot_id": "snapshot-incomplete",
                "fetched_at": (NOW + timedelta(seconds=10)).isoformat(),
                "fetch_complete": False,
                "positions": [],
            }
        ),
        encoding="utf-8",
    )

    assert has_open_spxw_positions() is True

    snapshot_path.write_text(
        json.dumps(
                {
                    "schema_version": 2,
                    "snapshot_id": "snapshot-flat",
                    "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
                "fetch_complete": True,
                "positions": [],
            }
        ),
        encoding="utf-8",
    )

    assert has_open_spxw_positions() is False


def test_stale_flat_snapshot_is_unknown_when_account_tracking_is_expected(
    tmp_path,
    monkeypatch,
) -> None:
    snapshot_path = tmp_path / "positions.json"
    state_path = tmp_path / "position-events.json"
    monkeypatch.setenv("IBKR_POSITIONS_SNAPSHOT_PATH", str(snapshot_path))
    monkeypatch.setenv("IBKR_POSITIONS_STATE_PATH", str(state_path))
    monkeypatch.setenv("IBKR_BROKER_ACCOUNT_READ_ENABLED", "true")
    monkeypatch.setenv("IBKR_POSITIONS_MAX_SNAPSHOT_AGE_SECONDS", "180")
    PositionEventStore(state_path).prepare(
        observation("snapshot-flat", positions=()),
        as_of=NOW,
    )
    snapshot_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "snapshot_id": "snapshot-flat",
                "fetched_at": NOW.isoformat(),
                "fetch_complete": True,
                "positions": [],
            }
        ),
        encoding="utf-8",
    )

    assert has_open_spxw_positions() is True

    monkeypatch.setenv("IBKR_BROKER_ACCOUNT_READ_ENABLED", "false")
    assert has_open_spxw_positions() is False
