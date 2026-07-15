from datetime import datetime, timedelta, timezone

from spx_spark.application.order_map.stable_structure import advance_stable_structure


NOW = datetime(2026, 7, 14, 13, 0, tzinfo=timezone.utc)


def structure(put: float, call: float) -> dict[str, object]:
    return {"levels": {"put_wall": put, "call_wall": call}, "expiry": "20260714"}


def test_wall_switch_requires_distinct_fifteen_minute_confirmations() -> None:
    state, stable = advance_stable_structure(
        None,
        structure(7500, 7600),
        now=NOW,
        interval_seconds=900,
        required_confirmations=3,
        band_half_width_points=5,
        switch_min_points=10,
    )
    assert stable["levels"]["put_wall"] == 7500
    assert stable["level_bands"]["put_wall"] == {"center": 7500.0, "low": 7495.0, "high": 7505.0}

    for index, put in enumerate((7475, 7476), start=1):
        state, stable = advance_stable_structure(
            state,
            structure(put, 7600),
            now=NOW + timedelta(minutes=15 * index),
            interval_seconds=900,
            required_confirmations=3,
            band_half_width_points=5,
            switch_min_points=10,
        )
        assert stable["levels"]["put_wall"] == 7500

    state, stable = advance_stable_structure(
        state,
        structure(7474, 7600),
        now=NOW + timedelta(minutes=45),
        interval_seconds=900,
        required_confirmations=3,
        band_half_width_points=5,
        switch_min_points=10,
    )
    assert stable["levels"]["put_wall"] == 7475
    assert state["promotion_reason"] == "multi_bucket_confirmation"


def test_small_wall_motion_extends_existing_structure() -> None:
    state, _stable = advance_stable_structure(
        None,
        structure(7500, 7600),
        now=NOW,
        interval_seconds=900,
        required_confirmations=3,
        band_half_width_points=5,
        switch_min_points=10,
    )
    state, stable = advance_stable_structure(
        state,
        structure(7505, 7595),
        now=NOW + timedelta(minutes=15),
        interval_seconds=900,
        required_confirmations=3,
        band_half_width_points=5,
        switch_min_points=10,
    )
    assert stable["levels"] == {"put_wall": 7500.0, "call_wall": 7600.0}
    assert state["candidate"] is None


def test_expiry_rollover_promotes_new_structure_immediately() -> None:
    state, _stable = advance_stable_structure(
        None,
        structure(7500, 7600),
        now=NOW,
        interval_seconds=900,
        required_confirmations=3,
        band_half_width_points=5,
        switch_min_points=10,
    )
    next_expiry = {
        "levels": {"put_wall": 7525, "call_wall": 7625},
        "expiry": "20260715",
    }

    state, stable = advance_stable_structure(
        state,
        next_expiry,
        now=NOW + timedelta(minutes=15),
        interval_seconds=900,
        required_confirmations=3,
        band_half_width_points=5,
        switch_min_points=10,
    )

    assert stable["expiry"] == "20260715"
    assert stable["levels"]["put_wall"] == 7525
    assert state["promotion_reason"] == "expiry_rollover"
