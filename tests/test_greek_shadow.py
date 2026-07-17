from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.greek_reference import YEAR_SECONDS, bs_delta, bs_gamma, bs_price
from spx_spark.greek_shadow import (
    SIGNED_GEX_METHOD,
    sample_zero_dte_greeks_shadow,
)
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.storage import LatestState


def make_quote(
    *,
    now: datetime,
    expiry: str = "20260710",
    strike: float = 6000.0,
    right: str = "C",
    updated_at: datetime | None = None,
    quote_time: datetime | None = None,
) -> Quote:
    spot = 6000.0
    iv = 0.20
    tau_seconds = (datetime(2026, 7, 10, 20, 0, tzinfo=timezone.utc) - now).total_seconds()
    tau_years = max(tau_seconds, 0.0) / YEAR_SECONDS
    model = bs_price(spot, strike, iv, tau_years, right)
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        bid=max(model - 0.05, 0.01),
        ask=max(model + 0.05, 0.06),
        quote_time=quote_time or now,
        last_update_at=updated_at or now,
        market_data_type=1,
        open_interest=100.0 if right == "C" else 60.0,
        volume=10_000.0 if right == "P" else 0.0,
        greeks=OptionGreeks(
            implied_vol=iv,
            delta=bs_delta(spot, strike, iv, tau_years, right),
            gamma=bs_gamma(spot, strike, iv, tau_years),
            underlier_price=spot,
        ),
    )


def make_state(
    now: datetime,
    *,
    expiry: str = "20260710",
    stale_put: bool = False,
) -> LatestState:
    rows = tuple(
        make_quote(
            now=now,
            expiry=expiry,
            strike=strike,
            right=right,
            quote_time=(now - timedelta(seconds=46) if stale_put and right == "P" else now),
        )
        for strike in (5995.0, 6000.0, 6005.0)
        for right in ("C", "P")
    )
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=rows,
        best_quotes=rows,
    )


def options_map(expiry: str = "20260710", *, source: str = "chain_implied") -> SimpleNamespace:
    front = SimpleNamespace(
        expiry=expiry,
        net_gex=123.0,
        abs_gex=456.0,
        net_gamma_ratio=0.27,
        gamma_state="positive_gamma_pin",
        gex_weighting="oi_plus_volume",
    )
    return SimpleNamespace(
        underlier=SimpleNamespace(price=6000.0, source=source),
        expiries=(front,),
    )


def latest_payload(tmp_path) -> dict:
    return json.loads(
        (tmp_path / "latest" / "spxw_0dte_greeks_reference.json").read_text(encoding="utf-8")
    )


def test_periodic_shadow_writes_reference_only_snapshot_with_signed_oi_proxy(tmp_path) -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)

    result = sample_zero_dte_greeks_shadow(
        make_state(now),
        data_root=tmp_path,
        options_map=options_map(),
    )

    assert result.status == "written"
    assert result.reference_status == "ok"
    payload = latest_payload(tmp_path)
    assert payload["shadow_sample"]["mode"] == "research_shadow_only"
    assert payload["shadow_sample"]["trigger"] == {"kind": "periodic"}
    assert payload["shadow_sample"]["notification_allowed"] is False
    assert payload["shadow_sample"]["order_placement_allowed"] is False
    assert payload["signed_gex_proxy"]["method"] == SIGNED_GEX_METHOD
    assert payload["signed_gex_proxy"]["weighting"] == "open_interest_only"
    assert payload["signed_gex_proxy"]["dealer_position_sign"] == "unknown"
    assert payload["signed_gex_proxy"]["call_gex"] > 0
    assert payload["signed_gex_proxy"]["put_gex"] < 0
    # Very high put volume must not enter the OI-only proxy.
    assert payload["signed_gex_proxy"]["net_gex"] > 0
    assert payload["intraday_map_gex_context"]["weighting"] == "oi_plus_volume"
    assert payload["intraday_map_gex_context"]["dealer_position_sign"] == "unknown"


def test_event_shadow_keeps_shock_identity_and_scalar_metadata(tmp_path) -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)

    result = sample_zero_dte_greeks_shadow(
        make_state(now),
        data_root=tmp_path,
        trigger_kind="shock",
        event_id="shock-123",
        event_at=now - timedelta(seconds=5),
        trigger_metadata={"direction": "down", "move_pct": -0.008},
        options_map=options_map(),
    )

    assert result.status == "written"
    trigger = latest_payload(tmp_path)["shadow_sample"]["trigger"]
    assert trigger["kind"] == "shock"
    assert trigger["event_id"] == "shock-123"
    assert trigger["event_at"] == (now - timedelta(seconds=5)).isoformat()
    assert trigger["metadata"] == {"direction": "down", "move_pct": -0.008}


def test_all_stale_exact_expiry_quotes_fail_closed_but_record_health_snapshot(tmp_path) -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    state = make_state(now)
    stale = tuple(
        replace(quote, quote_time=now - timedelta(seconds=46)) for quote in state.best_quotes
    )
    state = replace(state, quotes=stale, best_quotes=stale)

    result = sample_zero_dte_greeks_shadow(
        state,
        data_root=tmp_path,
        options_map=options_map(),
    )

    assert result.status == "blocked"
    assert result.reason == "exact_same_day_quotes_stale_or_unusable"
    payload = latest_payload(tmp_path)
    assert payload["status"] == "unavailable"
    assert payload["aggregate"] is None
    assert payload["signed_gex_proxy"]["quality"] == "unavailable"
    assert payload["shadow_sample"]["freshness"]["fresh_pricing_contract_count"] == 0


def test_partial_stale_chain_is_degraded_and_proxy_uses_only_fresh_contracts(tmp_path) -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)

    result = sample_zero_dte_greeks_shadow(
        make_state(now, stale_put=True),
        data_root=tmp_path,
        options_map=options_map(),
    )

    assert result.status == "written_degraded"
    assert result.reason == "partial_exact_expiry_stale_or_unusable"
    payload = latest_payload(tmp_path)
    assert payload["status"] == "degraded"
    assert "partial_exact_expiry_stale_or_unusable" in payload["warnings"]
    assert payload["signed_gex_proxy"]["fresh_contract_count"] == 3
    assert payload["signed_gex_proxy"]["put_gex"] == pytest.approx(0.0)
    assert payload["shadow_sample"]["freshness"]["stale_or_unusable_contract_count"] == 3


def test_high_coverage_partial_chain_remains_usable_with_warning(tmp_path) -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    rows = tuple(
        make_quote(
            now=now,
            strike=strike,
            right=right,
            quote_time=(
                now - timedelta(seconds=46)
                if strike == 5980.0 and right == "P"
                else now
            ),
        )
        for strike in (5980.0, 5990.0, 6000.0, 6010.0, 6020.0)
        for right in ("C", "P")
    )
    state = LatestState(created_at=now, as_of=now, quotes=rows, best_quotes=rows)

    result = sample_zero_dte_greeks_shadow(
        state,
        data_root=tmp_path,
        options_map=options_map(),
    )

    assert result.status == "written"
    assert result.reference_status == "ok"
    payload = latest_payload(tmp_path)
    assert payload["coverage"]["usable_contract_count"] == 9
    assert "partial_exact_expiry_stale_or_unusable" in payload["warnings"]


def test_underlier_mismatch_fails_closed_even_when_vendor_spot_exists(tmp_path) -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)

    result = sample_zero_dte_greeks_shadow(
        make_state(now),
        data_root=tmp_path,
        options_map=options_map(source="future:ES"),
    )

    assert result.status == "blocked"
    assert result.reason == "underlier_mismatch:future:ES"
    payload = latest_payload(tmp_path)
    assert payload["status"] == "unavailable"
    assert payload["aggregate"] is None
    assert payload["shadow_sample"]["strategy_action_allowed"] is False


def test_next_expiry_never_substitutes_for_literal_zero_dte(tmp_path) -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)

    result = sample_zero_dte_greeks_shadow(
        make_state(now, expiry="20260713"),
        data_root=tmp_path,
        options_map=options_map(expiry="20260713"),
    )

    assert result.status == "blocked"
    assert result.expiry == "20260710"
    assert result.reason == "exact_same_day_expiry_unavailable"
    payload = latest_payload(tmp_path)
    assert payload["expiry"] == "20260710"
    assert payload["contracts"] == []


def test_invalid_trigger_returns_error_without_writing(tmp_path) -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)

    result = sample_zero_dte_greeks_shadow(
        make_state(now),
        data_root=tmp_path,
        trigger_kind="trade",
        options_map=options_map(),
    )

    assert result.status == "error"
    assert "unsupported Greek shadow trigger" in (result.reason or "")
    assert not (tmp_path / "latest" / "spxw_0dte_greeks_reference.json").exists()


def test_naive_latest_state_timestamp_fails_closed_without_crashing(tmp_path) -> None:
    aware = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    state = replace(make_state(aware), as_of=aware.replace(tzinfo=None))

    result = sample_zero_dte_greeks_shadow(state, data_root=tmp_path)

    assert result.status == "blocked"
    assert result.reason == "latest_state_as_of_timezone_missing"
    assert latest_payload(tmp_path)["status"] == "unavailable"


def test_periodic_shadow_outside_rth_records_blocked_health_snapshot(tmp_path) -> None:
    now = datetime(2026, 7, 10, 22, 0, tzinfo=timezone.utc)

    result = sample_zero_dte_greeks_shadow(
        make_state(now),
        data_root=tmp_path,
        options_map=options_map(),
    )

    assert result.status == "blocked"
    assert result.reason == "outside_spx_trading_session"
    assert latest_payload(tmp_path)["status"] == "unavailable"


def test_periodic_shadow_accepts_active_monday_expiry_during_sunday_gth(tmp_path) -> None:
    now = datetime(2026, 7, 13, 1, 30, tzinfo=timezone.utc)

    result = sample_zero_dte_greeks_shadow(
        make_state(now, expiry="20260713"),
        data_root=tmp_path,
        options_map=options_map(expiry="20260713"),
    )

    assert result.status == "written_degraded"
    assert result.expiry == "20260713"
    assert latest_payload(tmp_path)["status"] == "degraded"
