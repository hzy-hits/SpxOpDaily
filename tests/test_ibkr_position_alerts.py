from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.alert_engine import Alert
from spx_spark.alert_profile import active_window
from spx_spark.ibkr.position_alerts import (
    PositionAlertState,
    evaluate_position_alerts,
)
from spx_spark.ibkr.position_watcher import (
    PositionSnapshot,
    SpxwPosition,
    is_spxw_contract,
    normalize_expiry,
)


class FakeContract:
    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def test_is_spxw_contract_detects_spxw_option():
    contract = FakeContract(symbol="SPX", secType="OPT", tradingClass="SPXW", localSymbol="SPXW  260706C07480000")
    assert is_spxw_contract(contract) is True


def test_is_spxw_contract_rejects_stock():
    contract = FakeContract(symbol="SPY", secType="STK", tradingClass="SPY", localSymbol="SPY")
    assert is_spxw_contract(contract) is False


def test_normalize_expiry_handles_yyyymmdd():
    assert normalize_expiry("20260706") == "20260706"
    assert normalize_expiry("20260706143000") == "20260706"


def make_position(**overrides) -> SpxwPosition:
    values = {
        "account": "U1",
        "symbol": "SPX",
        "expiry": "20260706",
        "strike": 7480.0,
        "right": "C",
        "qty": 1.0,
        "avg_cost": 3200.0,
        "con_id": 123,
        "trading_class": "SPXW",
        "local_symbol": "SPXW  260706C07480000",
        "canonical_id": "option:SPX:SPXW:20260706:7480:C",
        "market_price": 25.0,
        "unrealized_pnl": -700.0,
        "unrealized_pnl_pct": -21.9,
        "distance_from_spx_points": 3.0,
    }
    values.update(overrides)
    return SpxwPosition(**values)


def test_position_near_expiry_alert(monkeypatch, tmp_path):
    monkeypatch.setenv("ALERT_POSITIONS_ENABLED", "true")
    snapshot = PositionSnapshot(
        fetched_at=datetime(2026, 7, 6, 12, tzinfo=timezone.utc).isoformat(),
        account_count=1,
        positions=(make_position(),),
        spx_reference_price=7483.0,
        spx_reference_source="index:SPX",
    )
    from spx_spark.storage import LatestState

    state = LatestState(
        created_at=datetime(2026, 7, 6, 12, tzinfo=timezone.utc),
        as_of=datetime(2026, 7, 6, 12, tzinfo=timezone.utc),
        quotes=(),
        best_quotes=(),
    )
    window = active_window(datetime(2026, 7, 6, 14, tzinfo=timezone.utc))
    alerts = evaluate_position_alerts(
        snapshot,
        previous=PositionAlertState(positions={}),
        state=state,
        options_map=None,
        window=window,
        persist_state=False,
    )
    kinds = {alert.kind for alert in alerts}
    assert "spxw_position_near_expiry" in kinds
    assert "spxw_position_opened" in kinds


def test_position_qty_change_alert(monkeypatch):
    monkeypatch.setenv("ALERT_POSITIONS_ENABLED", "true")
    position = make_position(qty=2.0)
    snapshot = PositionSnapshot(
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        account_count=1,
        positions=(position,),
        spx_reference_price=7483.0,
        spx_reference_source="index:SPX",
    )
    from spx_spark.storage import LatestState

    state = LatestState(
        created_at=datetime(2026, 7, 6, 12, tzinfo=timezone.utc),
        as_of=datetime(2026, 7, 6, 12, tzinfo=timezone.utc),
        quotes=(),
        best_quotes=(),
    )
    window = active_window(datetime(2026, 7, 6, 14, tzinfo=timezone.utc))
    alerts = evaluate_position_alerts(
        snapshot,
        previous=PositionAlertState(
            positions={f"{position.account}|{position.canonical_id}": 1.0}
        ),
        state=state,
        options_map=None,
        window=window,
        persist_state=False,
    )
    assert any(alert.kind == "spxw_position_qty_changed" for alert in alerts)
