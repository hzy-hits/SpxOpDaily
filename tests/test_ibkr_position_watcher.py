from __future__ import annotations

from spx_spark.ibkr.position_watcher import (
    build_canonical_id,
    is_spxw_contract,
    normalize_expiry,
)


class FakeContract:
    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def test_build_canonical_id_for_spxw_call():
    canonical = build_canonical_id("20260706", 7480.0, "C")
    assert canonical.startswith("option:SPX:SPXW:20260706:7480:")


def test_is_spxw_from_trading_class():
    contract = FakeContract(symbol="SPX", secType="OPT", tradingClass="SPXW", localSymbol="")
    assert is_spxw_contract(contract) is True


def test_is_spxw_from_local_symbol_prefix():
    contract = FakeContract(symbol="SPX", secType="OPT", tradingClass="", localSymbol="SPXW  260706C07480000")
    assert is_spxw_contract(contract) is True
