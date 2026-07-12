
from __future__ import annotations

from stream_test_helpers import patch_stream

from types import SimpleNamespace

from spx_spark.config import IbkrStreamSettings
from spx_spark.ibkr.stream_collector import (
    StreamCollector,
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


def test_unchanged_spy_plan_is_not_requalified(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.skip_options = False
    collector.stream_settings = SimpleNamespace(spy_option_lines=4, spy_strike_step=2)
    collector.ibkr_settings = SimpleNamespace(qualify_contracts=False)
    collector.ib = object()
    collector.spy_subs = {}
    collector.spy_plan_key = None
    qualify_calls: list[list[tuple[str, str, object]]] = []

    def fake_qualify(
        ib,
        contracts,
        *,
        qualify=False,
    ):
        qualify_calls.append(contracts)
        return {
            label: (
                SimpleNamespace(contract=contract),
                VerifyRow(label=label, kind=kind, symbol="SPY", subscribed=True),
            )
            for label, kind, contract in contracts
        }

    patch_stream(monkeypatch, "qualify_and_subscribe", fake_qualify)
    patch_stream(monkeypatch, "cancel_subscriptions", lambda *args: None)
    rows = [
        VerifyRow(
            label="stock:SPY",
            kind="stock",
            symbol="SPY",
            market_price=628.3,
        )
    ]

    collector.ensure_spy_option_plan(rows, expiry="20260707")
    collector.ensure_spy_option_plan(rows, expiry="20260707")

    assert len(qualify_calls) == 1
    assert collector.spy_plan_key == ("20260707", 628)


def test_changed_spy_plan_retains_overlap_and_qualifies_only_additions(monkeypatch) -> None:
    collector = object.__new__(StreamCollector)
    collector.skip_options = False
    collector.stream_settings = SimpleNamespace(spy_option_lines=4, spy_strike_step=2)
    collector.ibkr_settings = SimpleNamespace(qualify_contracts=False)
    collector.ib = object()
    collector.spy_subs = {}
    collector.spy_plan_key = None
    qualify_labels: list[list[str]] = []
    canceled_labels: list[set[str]] = []

    def fake_qualify(ib, contracts, *, qualify=False):
        qualify_labels.append([label for label, _, _ in contracts])
        return {
            label: (
                SimpleNamespace(contract=contract),
                VerifyRow(label=label, kind=kind, symbol="SPY", subscribed=True),
            )
            for label, kind, contract in contracts
        }

    def fake_cancel(ib, subscriptions):
        canceled_labels.append(set(subscriptions))

    patch_stream(monkeypatch, "qualify_and_subscribe", fake_qualify)
    patch_stream(monkeypatch, "cancel_subscriptions", fake_cancel)
    first_rows = [
        VerifyRow(label="stock:SPY", kind="stock", symbol="SPY", market_price=628.3)
    ]
    second_rows = [
        VerifyRow(label="stock:SPY", kind="stock", symbol="SPY", market_price=630.3)
    ]

    collector.ensure_spy_option_plan(first_rows, expiry="20260707")
    retained = {
        label: subscription
        for label, subscription in collector.spy_subs.items()
        if ":628:" in label
    }
    collector.ensure_spy_option_plan(second_rows, expiry="20260707")

    assert len(qualify_labels) == 2
    assert set(qualify_labels[1]) == {
        "option:SPY:20260707:630:C",
        "option:SPY:20260707:630:P",
    }
    assert {label for labels in canceled_labels for label in labels} == {
        "option:SPY:20260707:626:C",
        "option:SPY:20260707:626:P",
    }
    assert all(collector.spy_subs[label] is subscription for label, subscription in retained.items())
