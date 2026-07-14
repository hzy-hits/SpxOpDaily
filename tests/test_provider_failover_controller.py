import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
)
from spx_spark.provider_failover import (
    FailoverMode,
    FailoverThresholds,
    control_allows_new_entries,
    control_requires_ibkr_market_data,
)
from spx_spark.provider_failover_controller import (
    ProviderFailoverSettings,
    evaluate_and_persist,
    provider_health,
    transport_health,
)
from spx_spark.storage import LatestState


UTC = timezone.utc


def quote(instrument: InstrumentId, provider: Provider, at: datetime) -> Quote:
    return Quote(
        instrument=instrument,
        provider=provider,
        provider_symbol=f"{provider.value}:{instrument.canonical_id}",
        received_at=at,
        quote_time=at,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
    )


def latest(at: datetime, *quotes: Quote) -> LatestState:
    return LatestState(
        created_at=at,
        as_of=at,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )


def settings(tmp_path) -> ProviderFailoverSettings:
    return ProviderFailoverSettings(
        enabled=True,
        state_path=str(tmp_path / "failover.json"),
        required_instruments=("index:SPX", "future:ES"),
        globex_required_instruments=("future:ES",),
        provider_state_max_age_seconds=45.0,
        quote_max_age_seconds=30.0,
        control_state_max_age_seconds=60.0,
        transition_alert_max_age_seconds=300.0,
        monitor_rth_only=True,
        thresholds=FailoverThresholds(
            schwab_unhealthy_observations=2,
            schwab_recovery_observations=2,
            ibkr_unhealthy_observations=2,
        ),
    )


def test_transport_health_does_not_reuse_quote_quality_reason() -> None:
    now = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    state = replace(
        latest(now),
        provider_states=(
            ProviderState(
                provider=Provider.SCHWAB,
                status=ProviderStatus.DEGRADED,
                checked_at=now,
                reason="connected but all priced quotes are stale",
                connected=True,
                authenticated=True,
            ),
        ),
    )

    health = transport_health(
        state,
        Provider.SCHWAB,
        provider_state_max_age_seconds=45.0,
    )

    assert health.healthy is True
    assert health.reason == "transport connected and authenticated (degraded)"


def test_controller_activates_ibkr_after_confirmed_schwab_failure(tmp_path) -> None:
    cfg = settings(tmp_path)
    now = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    first_quotes = (
        quote(InstrumentId.index("SPX"), Provider.IBKR, now),
        quote(InstrumentId.future("ES"), Provider.IBKR, now),
    )

    state = evaluate_and_persist(latest(now, *first_quotes), cfg)
    assert state.mode == FailoverMode.SCHWAB_PRIMARY

    later = now + timedelta(seconds=15)
    second_quotes = (
        quote(InstrumentId.index("SPX"), Provider.IBKR, later),
        quote(InstrumentId.future("ES"), Provider.IBKR, later),
    )
    state = evaluate_and_persist(latest(later, *second_quotes), cfg)
    raw = json.loads((tmp_path / "failover.json").read_text(encoding="utf-8"))

    assert state.mode == FailoverMode.IBKR_FALLBACK
    assert raw["monitoring_active"] is True
    assert raw["ibkr_market_data_required"] is True
    assert raw["new_entries_allowed"] is True
    assert control_requires_ibkr_market_data(raw, now=later, max_age_seconds=60.0)
    assert control_allows_new_entries(raw, now=later, max_age_seconds=60.0)


def test_controller_never_activates_ibkr_outside_rth(tmp_path) -> None:
    cfg = settings(tmp_path)
    saturday = datetime(2026, 7, 11, 14, 0, tzinfo=UTC)

    state = evaluate_and_persist(latest(saturday), cfg)
    raw = json.loads((tmp_path / "failover.json").read_text(encoding="utf-8"))

    assert state.mode == FailoverMode.SCHWAB_PRIMARY
    assert raw["monitoring_active"] is False
    assert raw["ibkr_market_data_required"] is False
    assert raw["new_entries_allowed"] is False


def test_controller_monitors_live_es_before_gth_without_requiring_spx(tmp_path) -> None:
    cfg = replace(settings(tmp_path), monitor_rth_only=False)
    sunday_reopen = datetime(2026, 7, 12, 22, 30, tzinfo=UTC)
    schwab_es = quote(InstrumentId.future("ES"), Provider.SCHWAB, sunday_reopen)

    state = evaluate_and_persist(latest(sunday_reopen, schwab_es), cfg)
    raw = json.loads((tmp_path / "failover.json").read_text(encoding="utf-8"))

    assert state.mode == FailoverMode.SCHWAB_PRIMARY
    assert raw["monitoring_active"] is True
    assert raw["monitoring_context"] == "globex"
    assert state.schwab_unhealthy_streak == 0


def test_controller_requires_live_es_and_spxw_pairs_during_gth(tmp_path) -> None:
    cfg = replace(
        settings(tmp_path),
        monitor_rth_only=False,
        gth_min_live_option_contracts=2,
    )
    gth = datetime(2026, 7, 13, 1, 30, tzinfo=UTC)
    rows = [quote(InstrumentId.future("ES"), Provider.SCHWAB, gth)]
    for right in ("C", "P"):
        rows.append(
            quote(
                InstrumentId.option(
                    "SPX",
                    expiry="20260713",
                    strike=7500,
                    right=right,
                    trading_class="SPXW",
                ),
                Provider.SCHWAB,
                gth,
            )
        )

    state = evaluate_and_persist(latest(gth, *rows), cfg)
    raw = json.loads((tmp_path / "failover.json").read_text(encoding="utf-8"))

    assert state.mode == FailoverMode.SCHWAB_PRIMARY
    assert raw["monitoring_context"] == "gth"
    assert state.schwab_unhealthy_streak == 0
    assert "2 SPXW contracts/1 pairs" in state.last_schwab_reason


def test_controller_rejects_es_only_during_gth(tmp_path) -> None:
    cfg = replace(
        settings(tmp_path),
        monitor_rth_only=False,
        gth_min_live_option_contracts=2,
    )
    gth = datetime(2026, 7, 13, 1, 30, tzinfo=UTC)
    schwab_es = quote(InstrumentId.future("ES"), Provider.SCHWAB, gth)

    state = evaluate_and_persist(latest(gth, schwab_es), cfg)
    raw = json.loads((tmp_path / "failover.json").read_text(encoding="utf-8"))

    assert state.schwab_unhealthy_streak == 1
    assert "GTH SPXW coverage 0/2" in state.last_schwab_reason
    assert raw["health_dimensions"]["schwab"]["anchors"]["healthy"] is True
    assert raw["health_dimensions"]["schwab"]["gth_options"] == {
        "healthy": False,
        "required": True,
        "reason": "GTH SPXW coverage 0/2 contracts, 0/1 complete pairs",
    }


def test_outside_rth_resets_prior_mode_streaks_and_transition(tmp_path) -> None:
    cfg = settings(tmp_path)
    friday = datetime(2026, 7, 10, 19, 0, tzinfo=UTC)
    failover = {
        "mode": "both_unavailable",
        "updated_at": friday.isoformat(),
        "sequence": 4,
        "schwab_unhealthy_streak": 8,
        "schwab_recovery_streak": 0,
        "ibkr_unhealthy_streak": 6,
        "transition": {
            "transition_id": "provider-failover:4:both_unavailable",
            "sequence": 4,
            "previous_mode": "recovery_pending",
            "mode": "both_unavailable",
            "occurred_at": friday.isoformat(),
            "reason": "test",
        },
    }
    (tmp_path / "failover.json").write_text(json.dumps(failover), encoding="utf-8")
    saturday = datetime(2026, 7, 11, 14, 0, tzinfo=UTC)

    state = evaluate_and_persist(latest(saturday), cfg)

    assert state.mode == FailoverMode.SCHWAB_PRIMARY
    assert state.sequence == 0
    assert state.schwab_unhealthy_streak == 0
    assert state.ibkr_unhealthy_streak == 0
    assert state.transition is None


def test_stale_control_state_cannot_hold_ibkr_market_data_on() -> None:
    now = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    raw = {
        "monitoring_active": True,
        "ibkr_market_data_required": True,
        "updated_at": (now - timedelta(minutes=5)).isoformat(),
    }

    assert not control_requires_ibkr_market_data(raw, now=now, max_age_seconds=60.0)


def test_entry_control_fails_closed_for_both_unavailable_or_stale_state() -> None:
    now = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    both_unavailable = {
        "monitoring_active": True,
        "new_entries_allowed": False,
        "mode": "both_unavailable",
        "updated_at": now.isoformat(),
    }
    stale_healthy = {
        "monitoring_active": True,
        "new_entries_allowed": True,
        "mode": "schwab_primary",
        "updated_at": (now - timedelta(minutes=5)).isoformat(),
    }

    assert not control_allows_new_entries(
        both_unavailable,
        now=now,
        max_age_seconds=60.0,
    )
    assert not control_allows_new_entries(
        stale_healthy,
        now=now,
        max_age_seconds=60.0,
    )


def test_provider_health_tolerates_scheduler_jitter_but_rejects_delayed_feed() -> None:
    source_at = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    observed_at = source_at + timedelta(seconds=20)
    spx = replace(
        quote(InstrumentId.index("SPX"), Provider.SCHWAB, source_at),
        quality=MarketDataQuality.STALE,
    )
    es = replace(
        quote(InstrumentId.future("ES"), Provider.SCHWAB, source_at),
        quality=MarketDataQuality.STALE,
    )

    health = provider_health(
        latest(observed_at, spx, es),
        Provider.SCHWAB,
        required_instruments=("index:SPX", "future:ES"),
        provider_state_max_age_seconds=45.0,
        quote_max_age_seconds=30.0,
    )

    assert health.healthy is True

    delayed_spx = replace(
        spx,
        quality=MarketDataQuality.DELAYED,
        market_data_type="delayed",
    )
    delayed = provider_health(
        latest(observed_at, delayed_spx, es),
        Provider.SCHWAB,
        required_instruments=("index:SPX", "future:ES"),
        provider_state_max_age_seconds=45.0,
        quote_max_age_seconds=30.0,
    )

    assert delayed.healthy is False


def test_provider_health_rejects_close_only_anchor() -> None:
    now = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    close_only_spx = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.UNKNOWN,
        close=6900.0,
        market_data_type=1,
        last_update_at=now,
        quote_time=None,
    )
    es = quote(InstrumentId.future("ES"), Provider.IBKR, now)

    health = provider_health(
        latest(now, close_only_spx, es),
        Provider.IBKR,
        required_instruments=("index:SPX", "future:ES"),
        provider_state_max_age_seconds=45.0,
        quote_max_age_seconds=30.0,
    )

    assert health.healthy is False
    assert "index:SPX" in health.reason
