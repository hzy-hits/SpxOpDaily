from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.market_features.spring_gamma_operator import (
    spring_gamma_operator_view,
)


NOW = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


def test_fresh_direction_remains_research_only() -> None:
    view = spring_gamma_operator_view(_shadow(), now=NOW, expected_expiry="20260730")

    assert view["status"] == "ready"
    assert view["preferred_side"] == "PUT"
    assert view["non_blocking"] is True
    assert view["execution_authority"] is False


def test_abstain_remains_a_non_blocking_research_view() -> None:
    shadow = _shadow()
    shadow["status"] = "abstain"
    shadow["direction"] = {"decision": "abstain", "composite_score": 0.02}

    view = spring_gamma_operator_view(shadow, now=NOW, expected_expiry="20260730")

    assert view["status"] == "abstain"
    assert view["preferred_side"] is None
    assert view["non_blocking"] is True


def test_stale_or_cross_expiry_model_is_not_shown_as_direction() -> None:
    stale = _shadow(as_of=NOW - timedelta(minutes=3))
    cross_expiry = _shadow()

    assert spring_gamma_operator_view(
        stale,
        now=NOW,
        expected_expiry="20260730",
    )["status"] == "stale"
    assert spring_gamma_operator_view(
        cross_expiry,
        now=NOW,
        expected_expiry="20260731",
    )["status"] == "stale"


def test_model_cannot_acquire_execution_authority_through_card_overlay() -> None:
    unsafe = _shadow()
    unsafe["actionable"] = True
    unsafe["action_authority"] = "production"

    view = spring_gamma_operator_view(unsafe, now=NOW, expected_expiry="20260730")

    assert view["status"] == "unsafe"
    assert view["execution_authority"] is False


def _shadow(*, as_of: datetime = NOW) -> dict[str, object]:
    return {
        "status": "ready",
        "as_of": as_of.isoformat(),
        "expiry": "20260730",
        "actionable": False,
        "automatic_ordering": False,
        "action_authority": "none",
        "direction": {
            "decision": "down",
            "composite_score": -0.62,
        },
    }
