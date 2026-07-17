from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.config import StorageSettings
from spx_spark.greek_reference import (
    GreekInputs,
    SCHEMA_VERSION,
    YEAR_SECONDS,
    bs_delta,
    bs_gamma,
    bs_price,
    bs_vega,
    build_zero_dte_greeks_reference,
    calculate_contract_reference,
    inputs_from_quote,
    is_spxw_zero_dte,
    load_zero_dte_greeks_snapshots,
    summarize_zero_dte_greeks_session,
    write_zero_dte_greeks_snapshot,
)
from spx_spark.market_calendar import ET
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.options_map import actionable_chain_implied_spot
from spx_spark.storage import LatestState


def storage_settings() -> StorageSettings:
    return StorageSettings(
        data_root="data",
        latest_state_path="data/latest/state.json",
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset(),
        delayed_stale_after_seconds=60.0,
    )


def make_quote(
    *,
    now: datetime,
    expiry: str = "20260710",
    strike: float = 6000.0,
    right: str = "C",
    spot: float = 6000.0,
    iv: float = 0.20,
    open_interest: float = 100.0,
    updated_at: datetime | None = None,
) -> Quote:
    tau_seconds = (datetime(2026, 7, 10, 20, 0, tzinfo=timezone.utc) - now).total_seconds()
    tau_years = max(tau_seconds, 0.0) / YEAR_SECONDS
    model = bs_price(spot, strike, iv, tau_years, right)
    bid = max(model - 0.05, 0.01)
    ask = max(model + 0.05, 0.06)
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
        bid=bid,
        ask=ask,
        quote_time=now,
        last_update_at=updated_at or now,
        market_data_type=1,
        open_interest=open_interest,
        greeks=OptionGreeks(
            implied_vol=iv,
            delta=bs_delta(spot, strike, iv, tau_years, right),
            gamma=bs_gamma(spot, strike, iv, tau_years),
            theta=None,
            vega=bs_vega(spot, strike, iv, tau_years) * 0.01,
            underlier_price=spot,
            model="ibkr_model",
        ),
    )


def test_exact_same_day_filter_and_configured_freshness() -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    quote = make_quote(now=now)
    assert is_spxw_zero_dte(quote, as_of=now) is True

    inputs, quality = inputs_from_quote(
        quote,
        as_of=now,
        storage_settings=storage_settings(),
    )
    assert inputs is not None
    assert quality.status == "ok"
    assert inputs.expiry == "20260710"
    assert inputs.tau_seconds == pytest.approx(3600.0)

    next_expiry = replace(
        quote,
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260713",
            strike=6000.0,
            right="C",
            trading_class="SPXW",
        ),
    )
    assert is_spxw_zero_dte(next_expiry, as_of=now) is False
    missing, blocked = inputs_from_quote(
        next_expiry,
        as_of=now,
        storage_settings=storage_settings(),
    )
    assert missing is None
    assert blocked.reasons == ("not_exact_same_day_spxw",)

    stale = make_quote(now=now, updated_at=now - timedelta(seconds=46))
    missing, blocked = inputs_from_quote(
        stale,
        as_of=now,
        storage_settings=storage_settings(),
    )
    assert missing is None
    assert blocked.status == "blocked"
    assert "transport_stale_after_45s" in blocked.reasons[0]


def test_zero_dte_boundary_includes_preceding_gth_and_early_close() -> None:
    previous_evening = datetime(2026, 7, 9, 23, 0, tzinfo=ET)
    midnight = datetime(2026, 7, 10, 0, 0, tzinfo=ET)
    july_quote = make_quote(
        now=previous_evening.astimezone(timezone.utc),
        expiry="20260710",
    )
    assert is_spxw_zero_dte(july_quote, as_of=previous_evening) is True
    assert is_spxw_zero_dte(july_quote, as_of=midnight) is True
    assert (
        is_spxw_zero_dte(
            july_quote,
            as_of=datetime(2026, 7, 10, 9, 30, tzinfo=ET),
        )
        is True
    )

    before_early_close = datetime(2026, 11, 27, 12, 59, tzinfo=ET)
    at_early_close = datetime(2026, 11, 27, 13, 0, tzinfo=ET)
    early_close_quote = replace(
        july_quote,
        instrument=InstrumentId.option(
            "SPX",
            expiry="20261127",
            strike=6000.0,
            right="C",
            trading_class="SPXW",
        ),
    )
    assert is_spxw_zero_dte(early_close_quote, as_of=before_early_close) is True
    assert is_spxw_zero_dte(early_close_quote, as_of=at_early_close) is False


def test_contract_reference_units_stability_and_scenario_serialization() -> None:
    now = datetime(2026, 7, 10, 19, 45, tzinfo=timezone.utc)
    tau_seconds = 15.0 * 60.0
    tau_years = tau_seconds / YEAR_SECONDS
    inputs = GreekInputs(
        contract_id="option:SPX:SPXW:20260710:6000:C",
        as_of=now,
        expiry="20260710",
        spot=6000.0,
        strike=6000.0,
        right="C",
        iv=0.20,
        tau_seconds=tau_seconds,
        mid=bs_price(6000.0, 6000.0, 0.20, tau_years, "C"),
        spread_bps=100.0,
        open_interest=100.0,
        vendor_delta=bs_delta(6000.0, 6000.0, 0.20, tau_years, "C"),
        vendor_gamma=bs_gamma(6000.0, 6000.0, 0.20, tau_years),
        vendor_underlier=6000.0,
    )
    reference = calculate_contract_reference(inputs)

    assert reference.delta == pytest.approx(inputs.vendor_delta, abs=1e-12)
    assert reference.gamma_per_point == pytest.approx(inputs.vendor_gamma, abs=1e-12)
    assert reference.charm_delta_per_minute < 0
    assert reference.speed_gamma_per_point < 0
    assert reference.vanna_delta_per_vol_point > 0
    assert reference.quality.step_stability_max_rel_error < 0.20

    payload = reference.to_dict()
    scenarios = {row["name"]: row for row in payload["scenarios"]}
    assert set(scenarios) == {
        "spot_down_0_50pct",
        "spot_down_0_25pct",
        "spot_up_0_25pct",
        "spot_up_0_50pct",
        "clock_plus_5m",
        "clock_plus_15m",
        "clock_plus_30m",
        "iv_down_3vol",
        "iv_down_1vol",
        "iv_up_1vol",
        "iv_up_3vol",
    }
    assert scenarios["clock_plus_30m"]["tau_seconds"] == 0.0
    assert scenarios["clock_plus_30m"]["bounded"] is True
    assert scenarios["spot_up_0_50pct"]["spot"] == pytest.approx(6030.0)
    assert scenarios["iv_down_3vol"]["iv"] == pytest.approx(0.17)
    assert all(row["reference_price"] >= 0 for row in payload["scenarios"])


def test_atm_call_put_higher_order_magnitudes_are_symmetric() -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    common = {
        "as_of": now,
        "expiry": "20260710",
        "spot": 6000.0,
        "strike": 6000.0,
        "iv": 0.20,
        "tau_seconds": 3600.0,
        "open_interest": 100.0,
    }
    call = calculate_contract_reference(
        GreekInputs(
            contract_id="option:SPX:SPXW:20260710:6000:C",
            right="C",
            **common,
        )
    )
    put = calculate_contract_reference(
        GreekInputs(
            contract_id="option:SPX:SPXW:20260710:6000:P",
            right="P",
            **common,
        )
    )

    assert call.gamma_per_point == pytest.approx(put.gamma_per_point)
    assert call.theta_per_minute == pytest.approx(put.theta_per_minute)
    assert call.vega_per_vol_point == pytest.approx(put.vega_per_vol_point)
    assert call.charm_delta_per_minute == pytest.approx(put.charm_delta_per_minute)
    assert call.color_gamma_per_minute == pytest.approx(put.color_gamma_per_minute)
    assert call.speed_gamma_per_point == pytest.approx(put.speed_gamma_per_point)
    assert call.vanna_delta_per_vol_point == pytest.approx(put.vanna_delta_per_vol_point)
    assert call.vomma_price_per_vol_point2 == pytest.approx(put.vomma_price_per_vol_point2)
    assert call.zomma_gamma_per_vol_point == pytest.approx(put.zomma_gamma_per_vol_point)


def test_clock_scenario_at_expiry_equals_intrinsic_and_respects_upper_bound() -> None:
    now = datetime(2026, 7, 10, 19, 50, tzinfo=timezone.utc)
    inputs = GreekInputs(
        contract_id="option:SPX:SPXW:20260710:5990:C",
        as_of=now,
        expiry="20260710",
        spot=6000.0,
        strike=5990.0,
        right="C",
        iv=0.20,
        tau_seconds=10.0 * 60.0,
        mid=25.0,
        spread_bps=100.0,
        open_interest=100.0,
    )

    scenarios = {row.name: row for row in calculate_contract_reference(inputs).scenarios}

    assert scenarios["clock_plus_15m"].tau_seconds == 0.0
    assert scenarios["clock_plus_15m"].reference_price == 10.0
    assert all(row.reference_price <= row.spot for row in scenarios.values())


def test_extreme_market_mid_is_degraded_and_clipped_scenarios_are_labeled() -> None:
    inputs = GreekInputs(
        contract_id="option:SPX:SPXW:20260710:6000:C",
        as_of=datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc),
        expiry="20260710",
        spot=6000.0,
        strike=6000.0,
        right="C",
        iv=0.20,
        tau_seconds=3600.0,
        mid=10_000.0,
        spread_bps=1.0,
        open_interest=100.0,
        vendor_theta=999.0,
        vendor_vega=999.0,
    )

    reference = calculate_contract_reference(inputs)

    assert reference.quality.status == "degraded"
    assert "market_mid_no_arbitrage_violation" in reference.quality.reasons
    assert "market_mid_model_ratio_extreme" in reference.quality.reasons
    assert "vendor_theta_mismatch" in reference.quality.reasons
    assert "vendor_vega_mismatch" in reference.quality.reasons
    assert any(row.bounded for row in reference.scenarios)
    assert all(row.reference_price <= row.spot for row in reference.scenarios)


def test_builder_aggregates_all_usable_but_serializes_at_most_six() -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    quotes = tuple(
        make_quote(now=now, strike=strike, right=right)
        for strike in (5995.0, 6000.0, 6005.0, 6010.0)
        for right in ("C", "P")
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=quotes,
        best_quotes=quotes,
    )
    options_map = SimpleNamespace(
        underlier=SimpleNamespace(price=6050.0, source="future:ES"),
        expiries=(SimpleNamespace(expiry="20260710"),),
    )
    focus_id = quotes[-1].instrument.canonical_id
    payload = build_zero_dte_greeks_reference(
        state,
        options_map=options_map,
        focus_contract_ids=(focus_id,),
        max_serialized_contracts=2,
    )

    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["mode"] == "reference_only"
    assert payload["direction"] == "unknown"
    assert payload["position_sign"] == "unknown"
    assert (
        payload["signed_gex_proxy"]["sign_method"]
        == "call_positive_put_negative_oi_proxy_not_dealer_position"
    )
    assert payload["signed_gex_proxy"]["dealer_position_sign"] == "unknown"
    assert payload["signed_gex_proxy"]["direction"] == "unknown"
    assert payload["aggregate_scope"] == "currently_actionable_exact_expiry_contracts_oi_only"
    assert payload["aggregate"]["contract_count"] == 8
    assert payload["aggregate"]["usable_count"] == 8
    assert payload["aggregate"]["quality"] == "ok"
    assert payload["aggregate"]["gross_color_5m_abs"] is not None
    assert payload["aggregate"]["gross_vomma_per_vol_point2_abs"] is not None
    assert payload["aggregate"]["gross_zomma_1vol_abs"] is not None
    assert payload["coverage"]["usable_ratio"] == 1.0
    assert payload["model"]["spot_source"] == "spxw_put_call_parity"
    assert payload["model"]["spot"] == 6000.0
    assert payload["serialized_contract_count"] == 2
    assert len(payload["contracts"]) == 2
    assert payload["contracts"][0]["contract_id"] == focus_id
    assert len(payload["contracts"][0]["scenarios"]) == 5
    assert len(json.dumps(payload, separators=(",", ":"))) < 8_000

    prompt_compact = build_zero_dte_greeks_reference(
        state,
        options_map=options_map,
        max_serialized_contracts=0,
    )
    assert prompt_compact["contracts"] == []
    assert len(json.dumps(prompt_compact, separators=(",", ":"))) < 5_000


def test_builder_returns_unavailable_instead_of_next_expiry_fallback() -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    quote = make_quote(now=now)
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(quote,),
        best_quotes=(quote,),
    )
    options_map = SimpleNamespace(
        underlier=SimpleNamespace(price=6000.0),
        expiries=(SimpleNamespace(expiry="20260713"),),
    )

    payload = build_zero_dte_greeks_reference(state, options_map=options_map)

    assert payload["status"] == "unavailable"
    assert payload["expiry"] == "20260710"
    assert payload["reason"] == "exact_same_day_expiry_unavailable"
    assert payload["contracts"] == []
    assert payload["aggregate"] is None


def test_normal_slow_rotation_reports_partial_coverage_without_using_stale_greeks() -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    fresh = tuple(
        make_quote(now=now, strike=strike, right=right)
        for strike in (5995.0, 6000.0, 6005.0)
        for right in ("C", "P")
    )
    stale = tuple(
        make_quote(
            now=now,
            strike=strike,
            right=right,
            updated_at=now - timedelta(seconds=46),
        )
        for strike in (5940.0, 5950.0, 5960.0, 6040.0, 6050.0, 6060.0, 6070.0)
        for right in ("C", "P")
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(*fresh, *stale),
        best_quotes=(*fresh, *stale),
    )
    options_map = SimpleNamespace(
        underlier=SimpleNamespace(price=6000.0, source="index:SPX"),
        expiries=(SimpleNamespace(expiry="20260710"),),
    )

    payload = build_zero_dte_greeks_reference(state, options_map=options_map)

    assert payload["status"] == "degraded"
    assert payload["aggregate"]["quality"] == "insufficient"
    assert payload["coverage"]["exact_expiry_contract_count"] == 20
    assert payload["coverage"]["usable_contract_count"] == 6
    assert payload["coverage"]["usable_ratio"] == pytest.approx(0.30)
    assert sum(payload["blocked_counts"].values()) == 14


def test_gth_greeks_use_recent_ibkr_rotation_rows_for_analytics() -> None:
    now = datetime(2026, 7, 9, 23, 0, tzinfo=ET).astimezone(timezone.utc)
    quotes = tuple(
        replace(
            make_quote(now=now, strike=strike, right=right),
            quality=MarketDataQuality.STALE,
            quote_time=now - timedelta(seconds=60),
            last_update_at=now - timedelta(seconds=60),
        )
        for strike in (5995.0, 6000.0, 6005.0)
        for right in ("C", "P")
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=quotes,
        best_quotes=quotes,
    )
    options_map = SimpleNamespace(
        underlier=SimpleNamespace(price=6000.0, source="future:ES"),
        expiries=(SimpleNamespace(expiry="20260710"),),
    )

    payload = build_zero_dte_greeks_reference(state, options_map=options_map)

    assert payload["coverage"]["usable_contract_count"] == 6
    assert payload["coverage"]["usable_ratio"] == 1.0
    assert payload["blocked_counts"] == {}
    assert payload["model"]["spot_source"] == "spxw_model_underlier_median"


def test_aggregate_uses_open_interest_only_and_never_infers_direction_from_volume() -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    quotes = tuple(
        make_quote(now=now, strike=strike, right=right)
        for strike in (5995.0, 6000.0, 6005.0)
        for right in ("C", "P")
    )
    high_volume = tuple(replace(quote, volume=1_000_000.0) for quote in quotes)
    options_map = SimpleNamespace(
        underlier=SimpleNamespace(price=6000.0, source="index:SPX"),
        expiries=(SimpleNamespace(expiry="20260710"),),
    )

    def build(rows: tuple[Quote, ...]) -> dict:
        state = LatestState(
            created_at=now,
            as_of=now,
            quotes=rows,
            best_quotes=rows,
        )
        return build_zero_dte_greeks_reference(state, options_map=options_map)

    base = build(quotes)
    with_volume = build(high_volume)
    assert with_volume["aggregate"] == base["aggregate"]
    assert with_volume["direction"] == "unknown"
    assert with_volume["position_sign"] == "unknown"

    no_oi = build(tuple(replace(quote, open_interest=None) for quote in high_volume))
    assert no_oi["status"] == "degraded"
    assert no_oi["aggregate"]["gross_gamma_abs"] is None
    assert no_oi["direction"] == "unknown"


def test_builder_uses_chain_anchor_and_degrades_on_stale_cash_level_divergence() -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    quotes = tuple(
        make_quote(now=now, strike=strike, right=right)
        for strike in (5995.0, 6000.0, 6005.0)
        for right in ("C", "P")
    )
    cash_spx = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=5982.0,
        quote_time=now,
        last_update_at=now,
        market_data_type=1,
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(cash_spx, *quotes),
        best_quotes=(cash_spx, *quotes),
    )
    options_map = SimpleNamespace(
        underlier=SimpleNamespace(price=5982.0, source="index:SPX"),
        expiries=(SimpleNamespace(expiry="20260710"),),
    )

    payload = build_zero_dte_greeks_reference(state, options_map=options_map)

    assert payload["model"]["spot"] == pytest.approx(5982.0)
    assert payload["model"]["spot_source"] == "index:SPX"
    assert payload["status"] == "degraded"
    assert "spx_anchor_divergence_over_20bps" in payload["warnings"]

    divergent_cash = replace(cash_spx, mark=5952.0)
    blocked_state = replace(
        state,
        quotes=(divergent_cash, *quotes),
        best_quotes=(divergent_cash, *quotes),
    )
    blocked = build_zero_dte_greeks_reference(
        blocked_state,
        options_map=options_map,
    )
    assert blocked["status"] == "unavailable"
    assert "spx_anchor_divergence_over_50bps" in blocked["warnings"]


def test_chain_anchor_requires_call_put_source_time_cofreshness() -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    quotes = tuple(
        replace(
            make_quote(now=now, strike=strike, right=right),
            quote_time=now - timedelta(seconds=3) if right == "P" else now,
            last_update_at=now,
        )
        for strike in (5995.0, 6000.0, 6005.0)
        for right in ("C", "P")
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=quotes,
        best_quotes=quotes,
    )
    options_map = SimpleNamespace(
        underlier=SimpleNamespace(price=6000.0, source="future:ES"),
        expiries=(SimpleNamespace(expiry="20260710"),),
    )

    assert actionable_chain_implied_spot(
        state,
        expiry="20260710",
        as_of=now,
    ) == pytest.approx(6000.0)
    assert (
        actionable_chain_implied_spot(
            state,
            expiry="20260710",
            as_of=now,
            max_leg_skew_seconds=2.0,
        )
        is None
    )

    payload = build_zero_dte_greeks_reference(state, options_map=options_map)

    assert payload["model"]["spot_source"] == "spxw_model_underlier_median"


def test_snapshot_persistence_and_session_summary(tmp_path) -> None:
    now = datetime(2026, 7, 10, 19, 0, tzinfo=timezone.utc)
    quotes = tuple(
        make_quote(now=now, strike=strike, right=right)
        for strike in (5995.0, 6000.0, 6005.0)
        for right in ("C", "P")
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=quotes,
        best_quotes=quotes,
    )
    options_map = SimpleNamespace(
        underlier=SimpleNamespace(price=6050.0, source="future:ES"),
        expiries=(SimpleNamespace(expiry="20260710"),),
    )
    first = build_zero_dte_greeks_reference(state, options_map=options_map)
    paths = write_zero_dte_greeks_snapshot(first, data_root=tmp_path)
    assert paths is not None

    second = json.loads(json.dumps(first))
    second["as_of"] = "2026-07-10T19:15:00+00:00"
    second["aggregate"]["gross_gamma_abs"] *= 1.5
    assert write_zero_dte_greeks_snapshot(second, data_root=tmp_path) is not None

    unavailable = {
        "schema_version": SCHEMA_VERSION,
        "kind": "snapshot",
        "mode": "reference_only",
        "status": "unavailable",
        "as_of": "2026-07-10T19:30:00+00:00",
        "expiry": "20260710",
        "direction": "unknown",
        "position_sign": "unknown",
        "reason": "test_data_gap",
        "aggregate": None,
        "contracts": [],
    }
    assert write_zero_dte_greeks_snapshot(unavailable, data_root=tmp_path) is not None

    loaded = load_zero_dte_greeks_snapshots(
        data_root=tmp_path,
        trading_date="2026-07-10",
    )
    summary = summarize_zero_dte_greeks_session(loaded, expiry="20260710")

    assert len(loaded) == 3
    assert summary["kind"] == "session_summary"
    assert summary["position_sign"] == "unknown"
    assert summary["snapshot_count"] == 3
    assert summary["usable_snapshot_count"] == 2
    assert summary["quality_counts"]["unavailable"] == 1
    assert summary["status"] == "degraded"
    gamma = summary["metrics"]["gross_gamma_abs"]
    assert gamma["last"] == pytest.approx(gamma["first"] * 1.5)
    assert gamma["peak"] == gamma["last"]
