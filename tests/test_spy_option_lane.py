from __future__ import annotations

from spx_spark.config import IbkrStreamSettings
from spx_spark.ibkr.stream_collector import (
    build_spy_option_strikes,
    estimate_spy_reference,
    spy_option_contracts,
    spy_option_spec_label,
)
from spx_spark.ibkr.verifier import VerifyRow


def test_build_spy_option_strikes_symmetric_window() -> None:
    strikes = build_spy_option_strikes(628.3, lines=16, step=2)

    assert len(strikes) == 8
    assert 628 in strikes
    assert min(strikes) == 620
    assert max(strikes) == 634
    assert all(strike % 2 == 0 for strike in strikes)


def test_build_spy_option_strikes_minimum_two_lines() -> None:
    strikes = build_spy_option_strikes(628.3, lines=2, step=2)

    assert len(strikes) == 1
    assert strikes[0] == 628


def test_spy_option_contracts_labels_and_trading_class() -> None:
    contracts = spy_option_contracts("20260707", [628])

    assert len(contracts) == 2
    call_label, call_kind, call_contract = contracts[0]
    put_label, put_kind, put_contract = contracts[1]

    assert call_label == spy_option_spec_label("20260707", 628, "C")
    assert put_label == spy_option_spec_label("20260707", 628, "P")
    assert call_label == "option:SPY:20260707:628:C"
    assert call_kind == "option"
    assert put_kind == "option"
    assert call_contract.symbol == "SPY"
    assert put_contract.symbol == "SPY"
    assert call_contract.tradingClass == "SPY"
    assert put_contract.tradingClass == "SPY"


def test_estimate_spy_reference_prefers_market_price_then_mid() -> None:
    with_market_price = VerifyRow(
        label="stock:SPY",
        kind="stock",
        symbol="SPY",
        market_price=628.5,
        bid=627.0,
        ask=629.0,
    )
    assert estimate_spy_reference([with_market_price]) == 628.5

    with_mid = VerifyRow(
        label="stock:SPY",
        kind="stock",
        symbol="SPY",
        bid=627.0,
        ask=629.0,
    )
    assert estimate_spy_reference([with_mid]) == 628.0


def test_stream_settings_read_spy_lane_env(monkeypatch) -> None:
    monkeypatch.setenv("IBKR_STREAM_SPY_OPTION_LINES", "0")
    settings = IbkrStreamSettings.from_env()
    assert settings.spy_option_lines == 0
