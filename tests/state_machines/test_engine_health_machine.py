"""EngineHealth transition / readiness evaluation tests."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spx_spark.application.realtime.health import evaluate_engine_health
from spx_spark.domain.health import EngineMode, HealthFactor


NOW = datetime(2026, 7, 11, 15, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        (
            dict(
                tradfi_anchor_usable=True,
                front_chain_fresh=True,
                analytics_succeeded=True,
                outbox_writable=True,
                critical_tasks_healthy=True,
            ),
            EngineMode.READY,
        ),
        (
            dict(
                tradfi_anchor_usable=True,
                front_chain_fresh=False,
                analytics_succeeded=True,
                outbox_writable=True,
                critical_tasks_healthy=True,
            ),
            EngineMode.DEGRADED,
        ),
        (
            dict(
                tradfi_anchor_usable=False,
                front_chain_fresh=True,
                analytics_succeeded=True,
                outbox_writable=True,
                critical_tasks_healthy=True,
            ),
            EngineMode.BLOCKED,
        ),
        (
            dict(
                tradfi_anchor_usable=True,
                front_chain_fresh=True,
                analytics_succeeded=False,
                outbox_writable=True,
                critical_tasks_healthy=True,
            ),
            EngineMode.BLOCKED,
        ),
        (
            dict(
                tradfi_anchor_usable=True,
                front_chain_fresh=True,
                analytics_succeeded=True,
                outbox_writable=False,
                critical_tasks_healthy=True,
            ),
            EngineMode.BLOCKED,
        ),
        (
            dict(
                tradfi_anchor_usable=True,
                front_chain_fresh=True,
                analytics_succeeded=True,
                outbox_writable=True,
                critical_tasks_healthy=False,
            ),
            EngineMode.BLOCKED,
        ),
    ],
)
def test_evaluate_engine_health_table(flags: dict, expected: EngineMode) -> None:
    health = evaluate_engine_health(checked_at=NOW, **flags)
    assert health.mode is expected
    assert health.ok is (expected in {EngineMode.READY, EngineMode.DEGRADED})
    assert health.actionable is (expected is EngineMode.READY)
    assert HealthFactor.TRADFI_ANCHOR.value in health.factors


def test_stale_provider_and_analytics_and_outbox_modes() -> None:
    stale = evaluate_engine_health(
        tradfi_anchor_usable=True,
        front_chain_fresh=False,
        analytics_succeeded=True,
        outbox_writable=True,
        critical_tasks_healthy=True,
        checked_at=NOW,
    )
    assert stale.mode is EngineMode.DEGRADED
    assert "front_chain_fresh_failed" in stale.reasons

    analytics = evaluate_engine_health(
        tradfi_anchor_usable=True,
        front_chain_fresh=True,
        analytics_succeeded=False,
        outbox_writable=True,
        critical_tasks_healthy=True,
        checked_at=NOW,
    )
    assert analytics.mode is EngineMode.BLOCKED
    assert "analytics_ok_failed" in analytics.reasons

    outbox = evaluate_engine_health(
        tradfi_anchor_usable=True,
        front_chain_fresh=True,
        analytics_succeeded=True,
        outbox_writable=False,
        critical_tasks_healthy=True,
        checked_at=NOW,
    )
    assert outbox.mode is EngineMode.BLOCKED
    assert "outbox_writable_failed" in outbox.reasons


def test_evaluate_engine_health_starting_before_warm() -> None:
    health = evaluate_engine_health(
        tradfi_anchor_usable=True,
        front_chain_fresh=True,
        analytics_succeeded=True,
        outbox_writable=True,
        critical_tasks_healthy=True,
        checked_at=NOW,
        warmed_up=False,
        any_critical_success=False,
    )
    assert health.mode is EngineMode.STARTING
    assert health.ok is False


def test_evaluate_engine_health_warming_partial() -> None:
    health = evaluate_engine_health(
        tradfi_anchor_usable=True,
        front_chain_fresh=True,
        analytics_succeeded=True,
        outbox_writable=True,
        critical_tasks_healthy=True,
        checked_at=NOW,
        warmed_up=False,
        any_critical_success=True,
    )
    assert health.mode is EngineMode.WARMING
    assert health.ok is False


def test_engine_failed_overrides_other_factors() -> None:
    health = evaluate_engine_health(
        tradfi_anchor_usable=True,
        front_chain_fresh=True,
        analytics_succeeded=True,
        outbox_writable=True,
        critical_tasks_healthy=True,
        checked_at=NOW,
        engine_failed=True,
    )
    assert health.mode is EngineMode.FAILED
    assert health.ok is False


def test_globex_context_is_formal_non_actionable_engine_mode() -> None:
    health = evaluate_engine_health(
        tradfi_anchor_usable=True,
        front_chain_fresh=False,
        analytics_succeeded=False,
        outbox_writable=True,
        critical_tasks_healthy=True,
        checked_at=NOW,
        cash_session_open=False,
        globex_context_usable=True,
    )

    assert health.mode is EngineMode.GLOBEX_CONTEXT
    assert health.ok is True
    assert health.actionable is False
    assert health.factors[HealthFactor.GLOBEX_CONTEXT_USABLE.value] is True
    assert health.reasons == ("cash_session_closed", "options_analytics_non_authoritative")


def test_live_gth_option_chain_is_actionable_outside_cash_session() -> None:
    health = evaluate_engine_health(
        tradfi_anchor_usable=True,
        front_chain_fresh=True,
        analytics_succeeded=True,
        outbox_writable=True,
        critical_tasks_healthy=True,
        checked_at=NOW,
        cash_session_open=False,
        globex_context_usable=True,
        gth_option_session_open=True,
    )

    assert health.mode is EngineMode.READY
    assert health.ok is True
    assert health.actionable is True
    assert health.reasons == ("cash_session_closed_live_option_chain",)
