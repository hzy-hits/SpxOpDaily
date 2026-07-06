from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.alert_profile import active_window
from spx_spark.position_alerts import (
    PositionAlertState,
    evaluate_position_alerts,
    format_book_detail,
)
from spx_spark.ibkr.position_watcher import (
    PositionSnapshot,
    SpxwPosition,
    is_spxw_contract,
    normalize_expiry,
    snapshot_book_metrics,
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


def make_snapshot(*positions: SpxwPosition) -> PositionSnapshot:
    book_pnl, book_cost, book_pnl_pct = snapshot_book_metrics(positions)
    return PositionSnapshot(
        fetched_at=datetime(2026, 7, 6, 12, tzinfo=timezone.utc).isoformat(),
        account_count=1,
        positions=positions,
        spx_reference_price=7483.0,
        spx_reference_source="index:SPX",
        book_unrealized_pnl=book_pnl,
        book_cost_basis=book_cost,
        book_unrealized_pnl_pct=book_pnl_pct,
    )


def test_book_pnl_alert_on_material_loss(monkeypatch):
    monkeypatch.setenv("ALERT_POSITIONS_ENABLED", "true")
    monkeypatch.setenv("ALERT_POSITION_PNL_LOSS_USD", "400")
    long_leg = make_position(unrealized_pnl=-620.0, unrealized_pnl_pct=-19.4)
    short_leg = make_position(
        strike=7535.0,
        qty=-1.0,
        avg_cost=397.0,
        canonical_id="option:SPX:SPXW:20260706:7535:C",
        unrealized_pnl=182.0,
        unrealized_pnl_pct=45.9,
        market_price=2.15,
    )
    snapshot = make_snapshot(long_leg, short_leg)
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
        previous=PositionAlertState(positions={}, leg_pnl={}, book_pnl=None),
        state=state,
        options_map=None,
        window=window,
        persist_state=False,
    )

    book_alerts = [alert for alert in alerts if alert.kind == "spxw_position_book_pnl"]
    assert len(book_alerts) == 1
    assert "$-438" in book_alerts[0].title
    assert "7480C" in book_alerts[0].detail
    assert "7535C" in book_alerts[0].detail


def test_book_pnl_alert_waits_for_meaningful_change(monkeypatch):
    monkeypatch.setenv("ALERT_POSITIONS_ENABLED", "true")
    monkeypatch.setenv("ALERT_POSITION_PNL_CHANGE_USD", "200")
    position = make_position(unrealized_pnl=-100.0, unrealized_pnl_pct=-3.1)
    snapshot = make_snapshot(position)
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
        previous=PositionAlertState(positions={}, leg_pnl={}, book_pnl=-95.0),
        state=state,
        options_map=None,
        window=window,
        persist_state=False,
    )

    assert not any(alert.kind == "spxw_position_book_pnl" for alert in alerts)


def test_position_qty_change_alert(monkeypatch):
    monkeypatch.setenv("ALERT_POSITIONS_ENABLED", "true")
    position = make_position(qty=2.0)
    snapshot = make_snapshot(position)
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
            positions={f"{position.account}|{position.canonical_id}": 1.0},
            leg_pnl={},
            book_pnl=-100.0,
        ),
        state=state,
        options_map=None,
        window=window,
        persist_state=False,
    )
    assert any(alert.kind == "spxw_position_qty_changed" for alert in alerts)


def test_format_book_detail_includes_leg_pnl():
    snapshot = make_snapshot(
        make_position(unrealized_pnl=-620.0, unrealized_pnl_pct=-19.4),
        make_position(
            strike=7535.0,
            qty=-1.0,
            avg_cost=397.0,
            canonical_id="option:SPX:SPXW:20260706:7535:C",
            unrealized_pnl=182.0,
            unrealized_pnl_pct=45.9,
            market_price=2.15,
        ),
    )
    detail = format_book_detail(snapshot, book_pnl_pct=-11.7)
    assert "SPX 7483" in detail
    assert "$-620" in detail
    assert "$+182" in detail
