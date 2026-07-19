from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Iterable

import pytest

import spx_spark.surface_live_session_worker as live_worker
from spx_spark.surface_artifact import canonical_sha256
from spx_spark.surface_live_session_http import LiveAPI
from spx_spark.surface_live_session_models import (
    LiveSelector,
    LiveSessionError,
    iso,
    signed_payload,
)
from spx_spark.surface_live_session_store import LiveSessionStateStore
from spx_spark.surface_live_session_worker import LiveInput, LiveSessionAccumulator


UTC = timezone.utc
SESSION_DATE = date(2026, 7, 17)
SESSION_OPEN = datetime(2026, 7, 17, 13, 30, tzinfo=UTC)
SESSION_CLOSE = datetime(2026, 7, 17, 20, 0, tzinfo=UTC)
FRONT_EXPIRY = "20260717"
NEXT_EXPIRY = "20260720"
SELECTOR = LiveSelector()


@dataclass
class MutableClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value


def _at(hour: int, minute: int, second: int = 0, microsecond: int = 0) -> datetime:
    return datetime(2026, 7, 17, hour, minute, second, microsecond, tzinfo=UTC)


def _strike_ladder(*, scale: float = 1.0) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, strike in enumerate((4995.0, 5000.0, 5005.0)):
        call_oi = scale * (80.0 + 20.0 * index)
        put_oi = scale * (140.0 - 20.0 * index)
        rows.append(
            {
                "strike": strike,
                "quality": "ready",
                "call": {
                    "iv": 0.18 + index * 0.005,
                    "open_interest": call_oi,
                    "volume": scale * (15.0 + index),
                },
                "put": {
                    "iv": 0.19 + index * 0.005,
                    "open_interest": put_oi,
                    "volume": scale * (20.0 + index),
                },
                "weightings": {
                    "oi_weighted": {
                        "quality": "ready",
                        "metrics": {"signed_gamma": call_oi - put_oi},
                    },
                    "volume_weighted": {
                        "quality": "ready",
                        "metrics": {"signed_gamma": scale * (index - 1)},
                    },
                },
            }
        )
    return rows


def _frame(
    role: str,
    *,
    accepted_at: datetime,
    model_as_of: datetime,
    valid_until: datetime,
    scale: float = 1.0,
    sequence: int = 1,
) -> dict[str, object]:
    expiry = FRONT_EXPIRY if role == "front" else NEXT_EXPIRY
    expiry_close = SESSION_CLOSE if role == "front" else SESSION_CLOSE + timedelta(days=3)
    snapshot_hash = canonical_sha256(
        {
            "kind": "synthetic_live_input",
            "role": role,
            "sequence": sequence,
            "accepted_at": iso(accepted_at),
        }
    )
    return signed_payload(
        {
            "schema_version": 1,
            "kind": "spxw_live_session_frame",
            "role": role,
            "expiry": expiry,
            "expiry_close": iso(expiry_close),
            "accepted_at": iso(accepted_at),
            "source_at": iso(model_as_of - timedelta(milliseconds=300)),
            "known_at": iso(model_as_of - timedelta(milliseconds=100)),
            "model_as_of": iso(model_as_of),
            "valid_until": iso(valid_until),
            "reference_spot": 5000.0,
            "quality": "ready",
            "warnings": [],
            "providers": ["schwab"],
            "input_clocks": {
                "selection_as_of": iso(model_as_of),
                "max_source_at": iso(model_as_of - timedelta(milliseconds=300)),
                "max_known_at": iso(model_as_of - timedelta(milliseconds=100)),
                "contract_clock_count": 6,
                "future_clock_count": 0,
            },
            "strike_ladder": _strike_ladder(scale=scale),
            "source_snapshot_sha256": snapshot_hash,
        }
    )


def _live_input(
    *,
    accepted_at: datetime,
    valid_until: datetime,
    roles: Iterable[str] = ("front", "next"),
    role_valid_until: dict[str, datetime] | None = None,
    scale: float = 1.0,
    sequence: int = 1,
    spot: float = 5000.0,
) -> LiveInput:
    model_as_of = accepted_at - timedelta(milliseconds=500)
    per_role_valid = role_valid_until or {}
    frames = {
        role: _frame(
            role,
            accepted_at=accepted_at,
            model_as_of=model_as_of,
            valid_until=per_role_valid.get(role, valid_until),
            scale=scale,
            sequence=sequence,
        )
        for role in roles
    }
    artifact = canonical_sha256(
        {
            "kind": "synthetic_live_input",
            "sequence": sequence,
            "accepted_at": iso(accepted_at),
            "roles": sorted(frames),
            "scale": scale,
        }
    )
    return LiveInput(
        artifact_sha256=artifact,
        as_of=model_as_of,
        valid_until=valid_until,
        spot=spot,
        spot_source_at=model_as_of - timedelta(milliseconds=250),
        spot_provider="schwab",
        frames=frames,
        providers=("schwab",),
    )


def _publisher_snapshot(
    *,
    source_as_of: datetime,
    valid_until: datetime,
) -> dict[str, object]:
    ladder = _strike_ladder()
    known_at = source_as_of - timedelta(milliseconds=100)
    source_at = source_as_of - timedelta(milliseconds=300)
    expiry_rows: list[dict[str, object]] = []
    for role, expiry, expiry_close in (
        ("front", FRONT_EXPIRY, SESSION_CLOSE),
        ("next", NEXT_EXPIRY, SESSION_CLOSE + timedelta(days=3)),
    ):
        expiry_rows.append(
            {
                "role": role,
                "expiry": expiry,
                "expiry_close": iso(expiry_close),
                "contract_count": 6,
                "quality": "ready",
                "warnings": [],
                "providers": ["schwab"],
                "input_clocks": {
                    "selection_as_of": iso(source_as_of),
                    "max_source_at": iso(source_at),
                    "max_known_at": iso(known_at),
                    "contract_clock_count": 6,
                    "future_clock_count": 0,
                },
                "surface": {
                    "as_of": iso(source_as_of),
                    "reference_spot": 5000.0,
                    "quality": "ready",
                    "strike_ladder": ladder,
                },
            }
        )
    return signed_payload(
        {
            "schema_version": 1,
            "kind": "spxw_surface_dashboard",
            "surface_version": "test",
            "status": "ready",
            "as_of": iso(source_as_of),
            "created_at": iso(source_as_of + timedelta(milliseconds=50)),
            "valid_until": iso(valid_until),
            "automatic_ordering": False,
            "session": {
                "state": "rth",
                "rth_open": True,
                "globex_open": False,
                "spx_gth_open": False,
                "research_expiries": [FRONT_EXPIRY, NEXT_EXPIRY],
            },
            "underlier": {
                "source": "index:SPX",
                "provider": "schwab",
                "price": 5000.0,
                "source_at": iso(source_at),
                "quality": "ready",
            },
            "source_state": {
                "created_at": iso(source_as_of - timedelta(seconds=1)),
                "selection_as_of": iso(source_as_of),
            },
            "quality": {
                "status": "ready",
                "lease_seconds": 30.0,
                "refresh_interval_seconds": 5.0,
                "published_expiry_count": 2,
                "requested_expiry_count": 2,
                "reasons": [],
            },
            "expiries": expiry_rows,
        }
    )


def _accumulator(
    tmp_path: Path,
    clock: MutableClock,
    *,
    state_name: str = "live-state",
) -> LiveSessionAccumulator:
    return LiveSessionAccumulator(
        snapshot_path=tmp_path / "snapshot.json",
        state_store=LiveSessionStateStore(tmp_path / state_name),
        utcnow=clock,
    )


def _accept(
    accumulator: LiveSessionAccumulator,
    *,
    accepted_at: datetime,
    valid_until: datetime,
    roles: Iterable[str] = ("front", "next"),
    role_valid_until: dict[str, datetime] | None = None,
    scale: float = 1.0,
    sequence: int = 1,
    spot: float = 5000.0,
) -> None:
    assert accumulator.accept(
        _live_input(
            accepted_at=accepted_at,
            valid_until=valid_until,
            roles=roles,
            role_valid_until=role_valid_until,
            scale=scale,
            sequence=sequence,
            spot=spot,
        ),
        accepted_at=accepted_at,
    )


def _state_with_boundaries(
    tmp_path: Path,
    *,
    boundary_count: int,
) -> tuple[LiveSessionStateStore, MutableClock, list[dict[str, Any]]]:
    accepted = _at(13, 30, 10)
    clock = MutableClock(accepted)
    store = LiveSessionStateStore(tmp_path / "live-state")
    accumulator = LiveSessionAccumulator(
        snapshot_path=tmp_path / "snapshot.json",
        state_store=store,
        utcnow=clock,
    )
    _accept(
        accumulator,
        accepted_at=accepted,
        valid_until=_at(13, 55),
    )
    freeze_at = SESSION_OPEN + timedelta(minutes=5 * boundary_count, seconds=1)
    clock.value = freeze_at
    assert accumulator._freeze_due(freeze_at)  # exercise the persisted boundary transaction
    boundaries = list(store.load_boundaries(SESSION_DATE))
    assert len(boundaries) == boundary_count
    return store, clock, boundaries


def test_poll_stamps_validation_completion_and_cannot_backfill_crossed_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before_boundary = _at(13, 34, 59, 999_000)
    validation_finished = _at(13, 35, 0, 10_000)
    clock = MutableClock(before_boundary)
    accumulator = _accumulator(tmp_path, clock)
    payload = _publisher_snapshot(
        source_as_of=_at(13, 34, 59, 700_000),
        valid_until=_at(13, 35, 20),
    )
    (tmp_path / "snapshot.json").write_text(json.dumps(payload), encoding="utf-8")

    original_validate = live_worker.validate_live_snapshot

    def validation_crossing_boundary(payload: Any) -> Any:
        result = original_validate(payload)
        clock.value = validation_finished
        return result

    monkeypatch.setattr(live_worker, "validate_live_snapshot", validation_crossing_boundary)
    assert accumulator.poll_once()
    accumulator.poll_once(now=validation_finished)

    runtime = accumulator.store.load_runtime(SESSION_DATE)
    assert runtime is not None
    front = runtime["candidate_by_role"]["front"]
    assert front["accepted_at"] == iso(validation_finished)
    first = accumulator.store.load_boundaries(SESSION_DATE)[0]
    assert first["end_at"] == iso(_at(13, 35))
    assert first["frame_by_role"]["front"] is None
    assert first["frozen_columns"]["front"] is None
    assert first["missing"]["surface_by_role"]["front"] == (
        "validated_surface_unavailable_at_bucket_end"
    )


def test_frozen_derived_column_survives_new_input_and_restart(tmp_path: Path) -> None:
    accepted = _at(13, 34, 50)
    clock = MutableClock(accepted)
    accumulator = _accumulator(tmp_path, clock)
    _accept(
        accumulator,
        accepted_at=accepted,
        valid_until=_at(13, 35, 30),
        scale=1.0,
        sequence=1,
    )
    clock.value = _at(13, 35, 1)
    assert accumulator._freeze_due(clock.value)
    frozen = accumulator.store.load_boundaries(SESSION_DATE)[0]
    frozen_column = frozen["frozen_columns"]["front"]["oi_weighted"]

    second_at = _at(13, 35, 5)
    _accept(
        accumulator,
        accepted_at=second_at,
        valid_until=_at(13, 35, 40),
        scale=50.0,
        sequence=2,
        spot=5001.0,
    )
    clock.value = _at(13, 35, 6)
    before_restart = accumulator.session_surface(SELECTOR, now=clock.value)
    assert before_restart["surface_columns"][0]["source_frame_sha256"] == (
        frozen_column["source_frame_sha256"]
    )
    assert before_restart["gamma_surface"][0] == frozen_column["metrics"]["signed_gamma"]
    assert before_restart["charm_surface"][0] == frozen_column["metrics"]["charm"]

    restarted = _accumulator(tmp_path, clock)
    after_restart = restarted.session_surface(SELECTOR, now=clock.value)
    assert after_restart["surface_columns"][0] == before_restart["surface_columns"][0]
    assert after_restart["gamma_surface"][0] == before_restart["gamma_surface"][0]
    assert after_restart["charm_surface"][0] == before_restart["charm_surface"][0]
    assert restarted.store.load_boundaries(SESSION_DATE)[0]["artifact_sha256"] == (
        frozen["artifact_sha256"]
    )


def test_mid_session_missing_boundaries_are_never_backfilled(tmp_path: Path) -> None:
    started = _at(13, 40, 1)
    clock = MutableClock(started)
    accumulator = _accumulator(tmp_path, clock)
    _accept(
        accumulator,
        accepted_at=started,
        valid_until=_at(13, 45, 30),
        sequence=1,
    )
    boundaries_before = accumulator.store.load_boundaries(SESSION_DATE)
    assert [row["end_at"] for row in boundaries_before] == [
        iso(_at(13, 35)),
        iso(_at(13, 40)),
    ]
    assert all(row["frozen_columns"]["front"] is None for row in boundaries_before)

    clock.value = _at(13, 40, 2)
    surface = accumulator.session_surface(SELECTOR, now=clock.value)
    assert [row["kind"] for row in surface["surface_columns"][:2]] == [
        "missing",
        "missing",
    ]
    assert all(all(value is None for value in row) for row in surface["gamma_surface"][:2])
    assert accumulator.store.load_boundaries(SESSION_DATE) == boundaries_before


@pytest.mark.parametrize("deletion", ["all", "tip", "middle"])
def test_boundary_chain_deletion_fails_closed(tmp_path: Path, deletion: str) -> None:
    store, clock, boundaries = _state_with_boundaries(tmp_path, boundary_count=3)
    paths = [
        store.boundary_path(
            SESSION_DATE,
            datetime.fromisoformat(str(row["end_at"])),
        )
        for row in boundaries
    ]
    if deletion == "all":
        selected = paths
    elif deletion == "tip":
        selected = [paths[-1]]
    else:
        selected = [paths[1]]
    for path in selected:
        path.unlink()

    with pytest.raises(LiveSessionError):
        _accumulator(tmp_path, clock)


@pytest.mark.parametrize("persisted_boundary_count", [0, 1])
def test_restart_repairs_only_when_disk_chain_strictly_extends_runtime_tip(
    tmp_path: Path,
    persisted_boundary_count: int,
) -> None:
    store, clock, boundaries = _state_with_boundaries(tmp_path, boundary_count=2)
    runtime = store.load_runtime(SESSION_DATE)
    assert runtime is not None
    if persisted_boundary_count:
        runtime["boundary_tip_sha256"] = boundaries[0]["artifact_sha256"]
        runtime["history_frozen_through"] = boundaries[0]["end_at"]
    else:
        runtime["boundary_tip_sha256"] = None
        runtime["history_frozen_through"] = None
    store.write_runtime(SESSION_DATE, runtime)

    restarted = _accumulator(tmp_path, clock)
    repaired = restarted.store.load_runtime(SESSION_DATE)
    assert repaired is not None
    assert repaired["boundary_tip_sha256"] == boundaries[-1]["artifact_sha256"]
    assert repaired["history_frozen_through"] == boundaries[-1]["end_at"]


def test_restart_rejects_mismatch_when_disk_does_not_extend_runtime_tip(
    tmp_path: Path,
) -> None:
    store, clock, boundaries = _state_with_boundaries(tmp_path, boundary_count=2)
    runtime = store.load_runtime(SESSION_DATE)
    assert runtime is not None
    runtime["boundary_tip_sha256"] = boundaries[-1]["artifact_sha256"]
    runtime["history_frozen_through"] = boundaries[0]["end_at"]
    store.write_runtime(SESSION_DATE, runtime)

    with pytest.raises(LiveSessionError):
        _accumulator(tmp_path, clock)


def test_selector_and_spot_leases_are_exclusive_and_role_scoped(tmp_path: Path) -> None:
    first_at = _at(13, 34, 40)
    role_expiry = _at(13, 35)
    clock = MutableClock(first_at)
    accumulator = _accumulator(tmp_path, clock)
    _accept(
        accumulator,
        accepted_at=first_at,
        valid_until=role_expiry,
        role_valid_until={"front": role_expiry, "next": role_expiry},
        sequence=1,
    )
    second_at = _at(13, 34, 50)
    _accept(
        accumulator,
        accepted_at=second_at,
        valid_until=_at(13, 35, 30),
        roles=("next",),
        sequence=2,
        spot=5002.0,
    )

    clock.value = role_expiry
    front = accumulator.session_surface(SELECTOR, now=role_expiry)
    assert front["valid_until"] == iso(role_expiry)
    assert front["live_status"] == "lease_expired"
    assert front["availability"]["projection_available"] is False
    assert front["availability"]["current_spot_available"] is False
    assert front["spot"] is None
    assert all(row["current_proxy"] is None for row in front["strike_profile"])

    next_surface = accumulator.session_surface(
        LiveSelector(role="next"),
        now=role_expiry,
    )
    assert next_surface["live_status"] == "ready"
    assert next_surface["valid_until"] == iso(_at(13, 35, 30))
    assert next_surface["availability"]["projection_available"] is True
    assert next_surface["spot"] == 5002.0


def test_missing_spot_blocks_projection_despite_valid_role_frame(tmp_path: Path) -> None:
    accepted = _at(13, 34, 50)
    clock = MutableClock(accepted)
    accumulator = _accumulator(tmp_path, clock)
    _accept(
        accumulator,
        accepted_at=accepted,
        valid_until=_at(13, 35, 20),
        sequence=1,
    )
    runtime = accumulator.store.load_runtime(SESSION_DATE)
    assert runtime is not None
    assert runtime["candidate_by_role"]["front"] is not None
    runtime["latest_spot"] = None
    accumulator.store.write_runtime(SESSION_DATE, runtime)

    clock.value = _at(13, 34, 51)
    restarted = _accumulator(tmp_path, clock)
    surface = restarted.session_surface(SELECTOR, now=clock.value)
    assert surface["spot"] is None
    assert surface["availability"]["current_spot_available"] is False
    assert surface["availability"]["projection_available"] is False
    assert surface["availability"]["current_strike_profile_available"] is False
    assert all(row["kind"] != "projection" for row in surface["surface_columns"])


def test_event_sampled_candle_freezes_ohlc_and_expiry_removes_partial(
    tmp_path: Path,
) -> None:
    clock = MutableClock(_at(13, 30, 1))
    accumulator = _accumulator(tmp_path, clock)
    for sequence, (accepted_at, spot) in enumerate(
        (
            (_at(13, 30, 1), 5000.0),
            (_at(13, 31, 1), 5002.0),
            (_at(13, 32, 1), 4998.0),
            (_at(13, 34, 59), 5001.0),
        ),
        start=1,
    ):
        _accept(
            accumulator,
            accepted_at=accepted_at,
            valid_until=_at(13, 35, 20),
            sequence=sequence,
            spot=spot,
        )

    clock.value = _at(13, 35, 1)
    assert accumulator._freeze_due(clock.value)
    frozen = accumulator.store.load_boundaries(SESSION_DATE)[0]
    assert frozen["candle"] == {
        "start_at": iso(_at(13, 30)),
        "end_at": iso(_at(13, 35)),
        "open": 5000.0,
        "high": 5002.0,
        "low": 4998.0,
        "close": 5001.0,
        "sample_count": 4,
        "complete": True,
        "source_at": iso(_at(13, 34, 58, 250_000)),
        "known_at": iso(_at(13, 34, 59)),
        "quality": "event_sampled",
        "providers": ["schwab"],
    }

    late_at = _at(13, 35, 2)
    late = replace(
        _live_input(
            accepted_at=late_at,
            valid_until=_at(13, 36),
            sequence=10,
            spot=4990.0,
        ),
        spot_source_at=_at(13, 34, 59, 500_000),
    )
    assert accumulator.accept(late, accepted_at=late_at)
    assert accumulator.store.load_boundaries(SESSION_DATE)[0]["candle"] == frozen["candle"]

    partial_at = _at(13, 35, 5)
    _accept(
        accumulator,
        accepted_at=partial_at,
        valid_until=_at(13, 35, 10),
        sequence=11,
        spot=5003.0,
    )
    clock.value = _at(13, 35, 9)
    fresh = accumulator.session_surface(SELECTOR, now=clock.value)
    assert any(candle["complete"] is False for candle in fresh["candles"])

    clock.value = _at(13, 35, 10)
    expired = accumulator.session_surface(SELECTOR, now=clock.value)
    assert all(candle["complete"] is True for candle in expired["candles"])


def test_projection_finishing_at_valid_until_clears_all_dynamic_fields(
    tmp_path: Path,
) -> None:
    accepted = _at(13, 34, 50)
    valid_until = _at(13, 34, 55)
    clock = MutableClock(accepted)
    accumulator = _accumulator(tmp_path, clock)
    _accept(
        accumulator,
        accepted_at=accepted,
        valid_until=valid_until,
        sequence=1,
    )

    request_at = _at(13, 34, 54)
    clock.value = valid_until  # deterministic projection completion clock
    surface = accumulator.session_surface(SELECTOR, now=request_at)
    assert surface["server_time"] == iso(valid_until)
    assert surface["valid_until"] == iso(valid_until)
    assert surface["live_status"] == "lease_expired"
    assert surface["spot"] is None
    assert surface["spot_source_at"] is None
    assert surface["spot_known_at"] is None
    assert surface["availability"]["projection_available"] is False
    assert surface["availability"]["current_strike_profile_available"] is False
    assert all(row["kind"] != "projection" for row in surface["surface_columns"])
    assert all(row["current_proxy"] is None for row in surface["strike_profile"])


def test_acceptance_clock_regression_is_rejected_without_replacing_runtime(
    tmp_path: Path,
) -> None:
    accepted = _at(13, 34, 50)
    clock = MutableClock(accepted)
    accumulator = _accumulator(tmp_path, clock)
    _accept(
        accumulator,
        accepted_at=accepted,
        valid_until=_at(13, 35, 20),
        sequence=1,
    )
    before = accumulator.store.load_runtime(SESSION_DATE)

    regressed = accepted - timedelta(microseconds=1)
    with pytest.raises(LiveSessionError, match="live_acceptance_clock_regressed"):
        accumulator.accept(
            _live_input(
                accepted_at=regressed,
                valid_until=_at(13, 35, 20),
                sequence=2,
            ),
            accepted_at=regressed,
        )

    assert accumulator.store.load_runtime(SESSION_DATE) == before


def test_lease_is_rechecked_after_expensive_projection_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import spx_spark.surface_live_session_projection as projection

    accepted = _at(13, 34, 50)
    valid_until = _at(13, 34, 55)
    clock = MutableClock(_at(13, 34, 54))
    accumulator = _accumulator(tmp_path, clock)
    _accept(
        accumulator,
        accepted_at=accepted,
        valid_until=valid_until,
        sequence=1,
    )
    original = projection._strike_profile

    def crossing_strike_profile(*args: object, **kwargs: object):
        result = original(*args, **kwargs)
        clock.value = valid_until
        return result

    monkeypatch.setattr(projection, "_strike_profile", crossing_strike_profile)
    surface = accumulator.session_surface(SELECTOR, now=_at(13, 34, 54))

    assert surface["server_time"] == iso(valid_until)
    assert surface["live_status"] == "lease_expired"
    assert surface["spot"] is None
    assert surface["availability"]["projection_available"] is False
    assert all(row["kind"] != "projection" for row in surface["surface_columns"])


def test_new_session_date_never_serves_previous_session_state(tmp_path: Path) -> None:
    accepted = _at(13, 34, 50)
    clock = MutableClock(accepted)
    accumulator = _accumulator(tmp_path, clock)
    _accept(
        accumulator,
        accepted_at=accepted,
        valid_until=_at(13, 35, 20),
        sequence=1,
    )
    monday = datetime(2026, 7, 20, 13, 31, tzinfo=UTC)
    clock.value = monday

    with pytest.raises(LiveSessionError, match="live_session_unavailable"):
        accumulator.session_surface(SELECTOR, now=monday)
    assert accumulator.health_payload()["active_session"] is None


def test_missing_selector_role_keeps_coherent_root_clock_but_no_dynamic_data(
    tmp_path: Path,
) -> None:
    accepted = _at(13, 34, 50)
    clock = MutableClock(accepted)
    accumulator = _accumulator(tmp_path, clock)
    _accept(
        accumulator,
        accepted_at=accepted,
        valid_until=_at(13, 35, 30),
        roles=("front",),
        sequence=1,
    )
    clock.value = _at(13, 35, 1)
    surface = accumulator.session_surface(LiveSelector(role="next"), now=clock.value)

    assert surface["history_frozen_through"] == iso(_at(13, 35))
    assert surface["source_as_of"] == iso(accepted - timedelta(milliseconds=500))
    assert surface["accepted_at"] == iso(accepted)
    assert surface["valid_until"] == iso(_at(13, 35, 30))
    assert surface["live_status"] == "unavailable"
    assert not any(surface["availability"].values())
    assert surface["spot"] is None
    assert all(row["kind"] == "missing" for row in surface["surface_columns"])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("policy_version", "spxw_session_surface.legacy"),
        ("bucket_minutes", 1),
        ("price_step", 2.5),
        ("price_extent_points_each_side", 200.0),
        ("session_end", iso(_at(19, 55))),
    ],
)
def test_restart_rejects_persisted_manifest_contract_drift(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    store, clock, _boundaries = _state_with_boundaries(tmp_path, boundary_count=1)
    manifest = store.load_manifest(SESSION_DATE)
    assert manifest is not None
    manifest[field] = value
    manifest = signed_payload(manifest)
    store.manifest_path(SESSION_DATE).write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(LiveSessionError, match="live_persisted_contract_drift"):
        _accumulator(tmp_path, clock)


def test_http_server_time_header_exactly_matches_signed_body(tmp_path: Path) -> None:
    accepted = _at(13, 34, 50)
    clock = MutableClock(accepted)
    accumulator = _accumulator(tmp_path, clock)
    _accept(
        accumulator,
        accepted_at=accepted,
        valid_until=_at(13, 35, 20),
        sequence=1,
    )
    request_at = _at(13, 34, 51, 123_456)
    clock.value = request_at
    api = LiveAPI(accumulator, utcnow=clock)
    response = api.dispatch(
        "GET",
        "/api/v1/live/session-surface"
        "?role=front&weighting=oi_weighted&bucket_minutes=5&price_step=5",
    )

    assert response.status == HTTPStatus.OK
    headers = dict(response.headers)
    assert headers["X-SPXW-Server-Time"] == response.payload["server_time"]
    assert response.payload["created_at"] == response.payload["server_time"]
    unsigned = dict(response.payload)
    artifact = unsigned.pop("artifact_sha256")
    assert artifact == canonical_sha256(unsigned)
