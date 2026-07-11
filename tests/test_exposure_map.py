from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.features import exposure_map as exposure_module
from spx_spark.features.exposure_map import (
    ExposureInputRow,
    build_exposure_map,
    bs_charm_per_minute,
    bs_vanna_per_vol_point,
    exposure_input_row_from_quote,
    gex_weight,
    net_dex_proxy_by_expiry,
    signed_gex,
    strike_exposure_values,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.options_map import build_options_map
from spx_spark.storage import LatestState

AS_OF = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)
SPOT = 7500.0
TAU = 0.01
IV = 0.20

# Full-precision BS (r=q=0) vendor greeks matching greeks-definitions §0.8 closed form.
# Truncated table digits are insufficient for expiry aggregates at rel=1e-9.
GAMMA_7500 = 0.002659482225240548
DELTA_CALL_7500 = 0.5039893563146316
DELTA_PUT_7500 = -0.4960106436853684
GAMMA_7550 = 0.002525063694907947
DELTA_CALL_7550 = 0.37364031420482935
DELTA_PUT_7550 = -0.6263596857951707

VANNA_7500 = 0.00019946116689304114
VANNA_7550 = 0.006481089872683743
CHARM_7500 = -3.7949232666103715e-07
CHARM_7550 = -1.2330840701453088e-05


def make_option(
    *,
    expiry: str,
    strike: float,
    right: str,
    mark: float,
    iv: float | None,
    gamma: float,
    delta: float,
    open_interest: float | None,
    volume: float | None = None,
    now: datetime = AS_OF,
    provider: Provider = Provider.IBKR,
    quote_time: datetime | None = None,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=provider,
        provider_symbol=f"SPXW:{expiry}:{strike}:{right}",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        bid=mark - 0.1,
        ask=mark + 0.1,
        mark=mark,
        open_interest=open_interest,
        volume=volume,
        quote_time=quote_time or now,
        greeks=OptionGreeks(
            implied_vol=iv,
            delta=delta,
            gamma=gamma,
            theta=-1.0,
            vega=0.3,
            model="test",
        ),
    )


def make_golden_state(*, as_of: datetime = AS_OF) -> LatestState:
    research = DEFAULT_MARKET_CALENDAR.research_expiry(as_of)
    expiry = research.strftime("%Y%m%d")
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=as_of,
        quality=MarketDataQuality.LIVE,
        mark=SPOT,
        quote_time=as_of,
    )
    quotes = [
        underlier,
        make_option(
            expiry=expiry,
            strike=7500.0,
            right="C",
            mark=10.0,
            iv=IV,
            gamma=GAMMA_7500,
            delta=DELTA_CALL_7500,
            open_interest=1000.0,
            volume=500.0,
            now=as_of,
        ),
        make_option(
            expiry=expiry,
            strike=7500.0,
            right="P",
            mark=11.0,
            iv=IV,
            gamma=GAMMA_7500,
            delta=DELTA_PUT_7500,
            open_interest=800.0,
            volume=2000.0,
            now=as_of,
        ),
        make_option(
            expiry=expiry,
            strike=7550.0,
            right="C",
            mark=7.5,
            iv=IV,
            gamma=GAMMA_7550,
            delta=DELTA_CALL_7550,
            open_interest=600.0,
            volume=1500.0,
            now=as_of,
        ),
        make_option(
            expiry=expiry,
            strike=7550.0,
            right="P",
            mark=8.0,
            iv=IV,
            gamma=GAMMA_7550,
            delta=DELTA_PUT_7550,
            open_interest=200.0,
            volume=100.0,
            now=as_of,
        ),
    ]
    return LatestState(
        created_at=as_of,
        as_of=as_of,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )


def _golden_rows(strike: float) -> tuple[ExposureInputRow, ...]:
    state = make_golden_state()
    rows = []
    for quote in state.quotes:
        if not quote.instrument.strike or quote.instrument.strike != strike:
            continue
        row = exposure_input_row_from_quote(quote, as_of=AS_OF)
        assert row is not None
        rows.append(row)
    return tuple(rows)


def test_options_map_golden_unchanged_after_extraction() -> None:
    golden_path = Path(__file__).parent / "golden" / "options_map_pre_extraction.json"
    golden = json.loads(golden_path.read_text(encoding="utf-8"))
    payload = build_options_map(make_golden_state()).to_dict()
    payload.pop("created_at", None)
    assert json.dumps(payload, sort_keys=True) == json.dumps(golden, sort_keys=True)


def test_options_map_reexports_extracted_symbols() -> None:
    import spx_spark.options_map as options_map

    assert options_map.build_gex_by_strike is exposure_module.build_gex_by_strike
    assert options_map.build_wall_ladder is exposure_module.build_wall_ladder
    assert options_map.gex_weight is exposure_module.gex_weight
    assert options_map.signed_gex is exposure_module.signed_gex


def test_intraday_oi_plus_volume_weight_preserved() -> None:
    quote = make_option(
        expiry="20260713",
        strike=7500.0,
        right="C",
        mark=10.0,
        iv=IV,
        gamma=0.003,
        delta=0.5,
        open_interest=100.0,
        volume=50.0,
    )
    assert gex_weight(quote, intraday=True) == 150.0
    assert gex_weight(quote, intraday=False) == 100.0
    intraday = signed_gex(quote, sign=1.0, underlier=7500.0, intraday=True)
    oi_only = signed_gex(quote, sign=1.0, underlier=7500.0, intraday=False)
    assert intraday == pytest.approx((oi_only or 0.0) * 1.5, rel=1e-9)


def test_exposure_map_oi_and_volume_weighted_coexist() -> None:
    exposure = build_exposure_map(make_golden_state())
    expiry = exposure.expiries[0]
    assert expiry.oi_weighted.net_gex is not None
    assert expiry.volume_weighted.net_gex is not None
    assert expiry.oi_weighted.net_gex == pytest.approx(86733108.169385, rel=1e-9)
    assert expiry.oi_weighted.abs_gex == pytest.approx(382900441.576463, rel=1e-9)
    assert expiry.oi_weighted.net_gamma_ratio == pytest.approx(0.226516082907, rel=1e-9)
    assert expiry.volume_weighted.net_gex == pytest.approx(-25545046.780670, rel=1e-9)
    assert expiry.volume_weighted.abs_gex == pytest.approx(601245420.466167, rel=1e-9)
    assert expiry.volume_weighted.net_gamma_ratio == pytest.approx(-0.042486887902, rel=1e-9)
    assert expiry.oi_weighted.net_dex_proxy == pytest.approx(1545698.195477, rel=1e-9)
    assert expiry.volume_weighted.net_dex_proxy == pytest.approx(-1816515.798643, rel=1e-9)
    assert expiry.volume_weighted.dagex_proxy == pytest.approx(-25545046.780670, rel=1e-9)


def test_exposure_map_strike_rows_sorted_and_paired() -> None:
    exposure = build_exposure_map(make_golden_state())
    expiry = exposure.expiries[0]
    assert [strike.strike for strike in expiry.strikes] == [7500.0, 7550.0]
    front = expiry.strikes[0]
    assert front.call_open_interest == 1000.0
    assert front.put_open_interest == 800.0
    assert front.call_volume == 500.0
    assert front.put_volume == 2000.0
    assert front.call_iv == IV
    assert front.put_iv == IV
    assert front.call_delta == pytest.approx(DELTA_CALL_7500, rel=1e-9)
    assert front.put_delta == pytest.approx(DELTA_PUT_7500, rel=1e-9)
    assert front.call_gamma == pytest.approx(GAMMA_7500, rel=1e-9)
    assert front.put_gamma == pytest.approx(GAMMA_7500, rel=1e-9)


def test_net_dex_proxy_by_expiry_weighting_selector() -> None:
    exposure = build_exposure_map(make_golden_state())
    expiry = DEFAULT_MARKET_CALENDAR.research_expiry(AS_OF).strftime("%Y%m%d")
    assert net_dex_proxy_by_expiry(exposure, weighting="oi_weighted")[expiry] == pytest.approx(
        1545698.195477, rel=1e-9
    )
    assert net_dex_proxy_by_expiry(exposure, weighting="volume_weighted")[expiry] == pytest.approx(
        -1816515.798643, rel=1e-9
    )
    with pytest.raises(ValueError):
        net_dex_proxy_by_expiry(exposure, weighting="invalid")


def test_exposure_map_serialization_carries_sign_convention_fields() -> None:
    payload = build_exposure_map(make_golden_state()).to_dict()
    expiry = payload["expiries"][0]
    assert expiry["sign_convention"] == "calls_positive_puts_negative"
    assert expiry["dealer_position_sign"] == "unknown"
    assert expiry["direction"] == "unknown"
    assert expiry["model"] == "bs_r0_q0"
    assert expiry["proxy_disclaimer"]


def test_gex_strike_rows_match_golden() -> None:
    rows_7500 = _golden_rows(7500.0)
    oi = strike_exposure_values(rows_7500, spot=SPOT, tau_years=TAU, weighting="oi_weighted")
    vol = strike_exposure_values(rows_7500, spot=SPOT, tau_years=TAU, weighting="volume_weighted")
    assert oi.call_gex == pytest.approx(149595875.169781, rel=1e-9)
    assert oi.put_gex == pytest.approx(-119676700.135825, rel=1e-9)
    assert oi.net_gex == pytest.approx(29919175.033956, rel=1e-9)
    assert oi.abs_gex == pytest.approx(269272575.305605, rel=1e-9)
    assert vol.call_gex == pytest.approx(74797937.584890, rel=1e-9)
    assert vol.put_gex == pytest.approx(-299191750.339562, rel=1e-9)
    assert vol.net_gex == pytest.approx(-224393812.754671, rel=1e-9)
    assert vol.abs_gex == pytest.approx(373989687.924452, rel=1e-9)

    rows_7550 = _golden_rows(7550.0)
    oi_7550 = strike_exposure_values(rows_7550, spot=SPOT, tau_years=TAU, weighting="oi_weighted")
    vol_7550 = strike_exposure_values(
        rows_7550, spot=SPOT, tau_years=TAU, weighting="volume_weighted"
    )
    assert oi_7550.call_gex == pytest.approx(85220899.703143, rel=1e-9)
    assert oi_7550.put_gex == pytest.approx(-28406966.567714, rel=1e-9)
    assert oi_7550.net_gex == pytest.approx(56813933.135429, rel=1e-9)
    assert oi_7550.abs_gex == pytest.approx(113627866.270858, rel=1e-9)
    assert vol_7550.call_gex == pytest.approx(213052249.257858, rel=1e-9)
    assert vol_7550.put_gex == pytest.approx(-14203483.283857, rel=1e-9)
    assert vol_7550.net_gex == pytest.approx(198848765.974001, rel=1e-9)
    assert vol_7550.abs_gex == pytest.approx(227255732.541715, rel=1e-9)


def test_net_dex_proxy_strike_rows_match_golden() -> None:
    rows_7500 = _golden_rows(7500.0)
    assert strike_exposure_values(
        rows_7500, spot=SPOT, tau_years=TAU, weighting="oi_weighted"
    ).net_dex_proxy == pytest.approx(803856.310248, rel=1e-9)
    assert strike_exposure_values(
        rows_7500, spot=SPOT, tau_years=TAU, weighting="volume_weighted"
    ).net_dex_proxy == pytest.approx(-5550199.569101, rel=1e-9)

    rows_7550 = _golden_rows(7550.0)
    assert strike_exposure_values(
        rows_7550, spot=SPOT, tau_years=TAU, weighting="oi_weighted"
    ).net_dex_proxy == pytest.approx(741841.885229, rel=1e-9)
    assert strike_exposure_values(
        rows_7550, spot=SPOT, tau_years=TAU, weighting="volume_weighted"
    ).net_dex_proxy == pytest.approx(3733683.770458, rel=1e-9)


def test_dagex_proxy_and_divergence_match_golden() -> None:
    exposure = build_exposure_map(make_golden_state())
    expiry = exposure.expiries[0]
    assert expiry.volume_weighted.dagex_proxy == pytest.approx(-25545046.780670, rel=1e-9)
    assert expiry.volume_weighted.net_gamma_ratio == pytest.approx(-0.042486887902, rel=1e-9)
    assert expiry.oi_weighted.net_gamma_ratio == pytest.approx(0.226516082907, rel=1e-9)
    assert expiry.gex_weighting_divergence == pytest.approx(-0.269002970809, rel=1e-9)


def test_vanna_per_vol_point_matches_closed_form() -> None:
    assert bs_vanna_per_vol_point(SPOT, 7500.0, IV, TAU) == pytest.approx(VANNA_7500, rel=1e-9)
    assert bs_vanna_per_vol_point(SPOT, 7550.0, IV, TAU) == pytest.approx(VANNA_7550, rel=1e-9)


def test_charm_per_minute_matches_closed_form() -> None:
    assert bs_charm_per_minute(SPOT, 7500.0, IV, TAU) == pytest.approx(CHARM_7500, rel=1e-9)
    assert bs_charm_per_minute(SPOT, 7550.0, IV, TAU) == pytest.approx(CHARM_7550, rel=1e-9)


def test_vex_proxy_matches_golden() -> None:
    rows_7500 = _golden_rows(7500.0)
    rows_7550 = _golden_rows(7550.0)
    assert strike_exposure_values(
        rows_7500, spot=SPOT, tau_years=TAU, weighting="oi_weighted"
    ).vex_proxy == pytest.approx(299.191750339562, rel=1e-9)
    assert strike_exposure_values(
        rows_7500, spot=SPOT, tau_years=TAU, weighting="volume_weighted"
    ).vex_proxy == pytest.approx(-2243.938127546713, rel=1e-9)
    assert strike_exposure_values(
        rows_7550, spot=SPOT, tau_years=TAU, weighting="oi_weighted"
    ).vex_proxy == pytest.approx(19443.26961805123, rel=1e-9)
    assert strike_exposure_values(
        rows_7550, spot=SPOT, tau_years=TAU, weighting="volume_weighted"
    ).vex_proxy == pytest.approx(68051.4436631793, rel=1e-9)
    oi_expiry = sum(
        strike_exposure_values(rows, spot=SPOT, tau_years=TAU, weighting="oi_weighted").vex_proxy
        or 0.0
        for rows in (rows_7500, rows_7550)
    )
    vol_expiry = sum(
        strike_exposure_values(
            rows, spot=SPOT, tau_years=TAU, weighting="volume_weighted"
        ).vex_proxy
        or 0.0
        for rows in (rows_7500, rows_7550)
    )
    assert oi_expiry == pytest.approx(19742.46136839079, rel=1e-9)
    assert vol_expiry == pytest.approx(65807.50553563258, rel=1e-9)


def test_cex_proxy_matches_golden() -> None:
    rows_7500 = _golden_rows(7500.0)
    rows_7550 = _golden_rows(7550.0)
    assert strike_exposure_values(
        rows_7500, spot=SPOT, tau_years=TAU, weighting="oi_weighted"
    ).cex_proxy == pytest.approx(-0.569238489991556, rel=1e-9)
    assert strike_exposure_values(
        rows_7500, spot=SPOT, tau_years=TAU, weighting="volume_weighted"
    ).cex_proxy == pytest.approx(4.26928867493667, rel=1e-9)
    assert strike_exposure_values(
        rows_7550, spot=SPOT, tau_years=TAU, weighting="oi_weighted"
    ).cex_proxy == pytest.approx(-36.99252210435926, rel=1e-9)
    assert strike_exposure_values(
        rows_7550, spot=SPOT, tau_years=TAU, weighting="volume_weighted"
    ).cex_proxy == pytest.approx(-129.47382736525742, rel=1e-9)
    oi_expiry = sum(
        strike_exposure_values(rows, spot=SPOT, tau_years=TAU, weighting="oi_weighted").cex_proxy
        or 0.0
        for rows in (rows_7500, rows_7550)
    )
    vol_expiry = sum(
        strike_exposure_values(
            rows, spot=SPOT, tau_years=TAU, weighting="volume_weighted"
        ).cex_proxy
        or 0.0
        for rows in (rows_7500, rows_7550)
    )
    assert oi_expiry == pytest.approx(-37.56176059435082, rel=1e-9)
    assert vol_expiry == pytest.approx(-125.20453869032076, rel=1e-9)


def test_bs_edge_cases_return_none() -> None:
    assert bs_vanna_per_vol_point(0, 7500.0, IV, TAU) is None
    assert bs_vanna_per_vol_point(SPOT, 0, IV, TAU) is None
    assert bs_vanna_per_vol_point(SPOT, 7500.0, 0, TAU) is None
    assert bs_vanna_per_vol_point(SPOT, 7500.0, IV, 0) is None
    assert bs_charm_per_minute(SPOT, 7500.0, 0, TAU) is None


def test_tau_floored_contract_excluded_from_cex() -> None:
    as_of = datetime(2026, 7, 13, 19, 50, tzinfo=timezone.utc)
    exposure = build_exposure_map(make_golden_state(as_of=as_of))
    expiry = exposure.expiries[0]
    assert any(warning.startswith("tau_floored:") for warning in expiry.warnings)
    assert expiry.oi_weighted.cex_proxy is None
    assert expiry.volume_weighted.cex_proxy is None


def test_missing_oi_disables_oi_weighted_only() -> None:
    state = make_golden_state()
    quotes = []
    for quote in state.quotes:
        if quote.instrument.instrument_type.value == "index":
            quotes.append(quote)
            continue
        quotes.append(
            make_option(
                expiry=quote.instrument.expiry or "20260713",
                strike=float(quote.instrument.strike or 0),
                right=quote.instrument.right.value if quote.instrument.right else "C",
                mark=quote.mark or 10.0,
                iv=IV,
                gamma=quote.greeks.gamma if quote.greeks else 0.003,
                delta=quote.greeks.delta if quote.greeks else 0.5,
                open_interest=0.0,
                volume=quote.volume,
            )
        )
    exposure = build_exposure_map(
        LatestState(
            created_at=AS_OF,
            as_of=AS_OF,
            quotes=tuple(quotes),
            best_quotes=tuple(quotes),
        )
    )
    expiry = exposure.expiries[0]
    assert expiry.oi_quality == "stale_or_zero"
    assert expiry.quality == "no_open_interest"
    assert expiry.oi_weighted.net_gex is None
    assert expiry.volume_weighted.net_gex is not None


def test_schwab_oi_flags_unverified_warning() -> None:
    state = make_golden_state()
    quotes = []
    for quote in state.quotes:
        if quote.instrument.instrument_type.value == "index":
            quotes.append(quote)
            continue
        quotes.append(
            make_option(
                expiry=quote.instrument.expiry or "20260713",
                strike=float(quote.instrument.strike or 0),
                right=quote.instrument.right.value if quote.instrument.right else "C",
                mark=quote.mark or 10.0,
                iv=IV,
                gamma=quote.greeks.gamma if quote.greeks else 0.003,
                delta=quote.greeks.delta if quote.greeks else 0.5,
                open_interest=quote.open_interest,
                volume=quote.volume,
                provider=Provider.SCHWAB,
            )
        )
    exposure = build_exposure_map(
        LatestState(
            created_at=AS_OF,
            as_of=AS_OF,
            quotes=tuple(quotes),
            best_quotes=tuple(quotes),
        )
    )
    expiry = exposure.expiries[0]
    assert expiry.oi_quality == "schwab_unverified"
    assert expiry.oi_weighted.net_gex is not None
    assert "schwab_oi_unverified" in expiry.warnings


def test_missing_iv_disables_vanna_family_only() -> None:
    state = make_golden_state()
    quotes = []
    for quote in state.quotes:
        if quote.instrument.instrument_type.value == "index":
            quotes.append(quote)
            continue
        quotes.append(
            make_option(
                expiry=quote.instrument.expiry or "20260713",
                strike=float(quote.instrument.strike or 0),
                right=quote.instrument.right.value if quote.instrument.right else "C",
                mark=quote.mark or 10.0,
                iv=None,
                gamma=quote.greeks.gamma if quote.greeks else 0.003,
                delta=quote.greeks.delta if quote.greeks else 0.5,
                open_interest=quote.open_interest,
                volume=quote.volume,
            )
        )
    exposure = build_exposure_map(
        LatestState(
            created_at=AS_OF,
            as_of=AS_OF,
            quotes=tuple(quotes),
            best_quotes=tuple(quotes),
        )
    )
    expiry = exposure.expiries[0]
    assert expiry.iv_source == "missing"
    assert expiry.oi_weighted.vex_proxy is None
    assert expiry.oi_weighted.cex_proxy is None
    assert expiry.oi_weighted.net_gex is not None
    assert expiry.oi_weighted.net_dex_proxy is not None


def test_stale_snapshot_marks_expiry_unavailable() -> None:
    stale_time = AS_OF - timedelta(minutes=20)
    state = make_golden_state()
    quotes = []
    for quote in state.quotes:
        if quote.instrument.instrument_type.value == "index":
            quotes.append(quote)
            continue
        quotes.append(
            make_option(
                expiry=quote.instrument.expiry or "20260713",
                strike=float(quote.instrument.strike or 0),
                right=quote.instrument.right.value if quote.instrument.right else "C",
                mark=quote.mark or 10.0,
                iv=IV,
                gamma=quote.greeks.gamma if quote.greeks else 0.003,
                delta=quote.greeks.delta if quote.greeks else 0.5,
                open_interest=quote.open_interest,
                volume=quote.volume,
                quote_time=stale_time,
            )
        )
    exposure = build_exposure_map(
        LatestState(
            created_at=AS_OF,
            as_of=AS_OF,
            quotes=tuple(quotes),
            best_quotes=tuple(quotes),
        )
    )
    expiry = exposure.expiries[0]
    assert expiry.quality == "unavailable"
    assert expiry.oi_weighted.net_gex is None
    assert expiry.volume_weighted.net_gex is None


def test_low_delta_coverage_nulls_net_dex_proxy() -> None:
    state = make_golden_state()
    quotes = []
    missing_delta = 0
    for quote in state.quotes:
        if quote.instrument.instrument_type.value == "index":
            quotes.append(quote)
            continue
        missing_delta += 1
        quotes.append(
            make_option(
                expiry=quote.instrument.expiry or "20260713",
                strike=float(quote.instrument.strike or 0),
                right=quote.instrument.right.value if quote.instrument.right else "C",
                mark=quote.mark or 10.0,
                iv=IV,
                gamma=quote.greeks.gamma if quote.greeks else 0.003,
                delta=quote.greeks.delta if quote.greeks and missing_delta == 1 else None,
                open_interest=quote.open_interest,
                volume=quote.volume,
            )
        )
    exposure = build_exposure_map(
        LatestState(
            created_at=AS_OF,
            as_of=AS_OF,
            quotes=tuple(quotes),
            best_quotes=tuple(quotes),
        )
    )
    expiry = exposure.expiries[0]
    assert expiry.delta_coverage_ratio == pytest.approx(0.25, rel=1e-9)
    assert expiry.oi_weighted.net_dex_proxy is None
    assert "low_delta_coverage" in expiry.warnings
    assert expiry.oi_weighted.net_gex is not None


def test_early_session_volume_warning() -> None:
    as_of = datetime(2026, 7, 13, 13, 40, tzinfo=timezone.utc)
    exposure = build_exposure_map(make_golden_state(as_of=as_of))
    assert "early_session_low_volume" in exposure.expiries[0].warnings
    assert exposure.expiries[0].volume_weighted.dagex_proxy is not None
