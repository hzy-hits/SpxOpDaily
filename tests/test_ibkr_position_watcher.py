from __future__ import annotations

import stat
from datetime import datetime, timezone

import pytest

from spx_spark.ibkr.position_watcher import (
    PositionSnapshot,
    SpxwPosition,
    build_canonical_id,
    is_spxw_contract,
    load_snapshot,
    managed_account_ids,
    position_unrealized_metrics,
    snapshot_book_metrics,
    write_snapshot,
)
from spx_spark.config import IbkrPositionSettings


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


def make_position(**overrides) -> SpxwPosition:
    values = {
        "account": "U1",
        "symbol": "SPX",
        "expiry": "20260710",
        "strike": 7500.0,
        "right": "C",
        "qty": 2.0,
        "avg_cost": 3200.0,
        "con_id": 123,
        "trading_class": "SPXW",
        "local_symbol": "SPXW  260710C07500000",
        "canonical_id": "option:SPX:SPXW:20260710:7500:C",
        "multiplier": 100.0,
        "market_price": 25.0,
        "mark_pricing_allowed": True,
        "unrealized_pnl": -1400.0,
        "unrealized_pnl_pct": -21.875,
    }
    values.update(overrides)
    return SpxwPosition(**values)


def test_position_pnl_multiplies_average_cost_by_signed_quantity() -> None:
    long_pnl, long_pct = position_unrealized_metrics(
        qty=2.0,
        avg_cost=3200.0,
        mark=25.0,
        multiplier=100.0,
    )
    short_pnl, short_pct = position_unrealized_metrics(
        qty=-2.0,
        avg_cost=3200.0,
        mark=25.0,
        multiplier=100.0,
    )

    assert long_pnl == -1400.0
    assert long_pct == -21.875
    assert short_pnl == 1400.0
    assert short_pct == 21.875


def test_snapshot_book_cost_uses_absolute_quantity() -> None:
    position = make_position()

    book_pnl, book_cost, book_pnl_pct = snapshot_book_metrics((position,))

    assert book_pnl == -1400.0
    assert book_cost == 6400.0
    assert book_pnl_pct == -21.875


def test_snapshot_v2_is_written_owner_only_and_round_trips(tmp_path) -> None:
    position = make_position()
    snapshot = PositionSnapshot(
        fetched_at=datetime(2026, 7, 10, 14, tzinfo=timezone.utc).isoformat(),
        account_count=1,
        positions=(position,),
        spx_reference_price=7500.0,
        spx_reference_source="index:SPX",
        book_unrealized_pnl=-1400.0,
        book_cost_basis=6400.0,
        book_unrealized_pnl_pct=-21.875,
        snapshot_id="snapshot-1",
        managed_account_count=1,
        raw_position_count=1,
        filtered_spxw_count=1,
        priced_leg_count=1,
        total_leg_count=1,
        book_pnl_complete=True,
    )
    path = tmp_path / "positions.json"

    write_snapshot(snapshot, str(path))
    restored = load_snapshot(str(path))

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert restored == snapshot


def test_legacy_snapshot_without_fetch_complete_fails_closed(tmp_path) -> None:
    path = tmp_path / "legacy-positions.json"
    path.write_text(
        """{
          "fetched_at": "2026-07-10T14:00:00+00:00",
          "account_count": 0,
          "positions": [],
          "spx_reference_price": null,
          "spx_reference_source": null
        }""",
        encoding="utf-8",
    )

    restored = load_snapshot(str(path))

    assert restored is not None
    assert restored.fetch_complete is False
    assert restored.book_pnl_complete is True


@pytest.mark.parametrize(
    "payload",
    (
        "[]",
        '{"schema_version":"bogus","positions":[]}',
        '{"schema_version":2,"positions":{}}',
    ),
)
def test_semantically_invalid_snapshot_fails_closed(tmp_path, payload: str) -> None:
    path = tmp_path / "invalid-positions.json"
    path.write_text(payload, encoding="utf-8")

    assert load_snapshot(str(path)) is None


def test_managed_account_count_uses_broker_account_list() -> None:
    class FakeIB:
        def managedAccounts(self):
            return ["U1", "U2"]

    assert managed_account_ids(FakeIB(), ()) == ("U1", "U2")


def test_blank_position_state_path_uses_data_root_default(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MARKET_DATA_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("IBKR_POSITIONS_STATE_PATH", "")

    settings = IbkrPositionSettings.from_env()

    assert settings.state_path == str(tmp_path / "latest" / "ibkr_position_state.json")
