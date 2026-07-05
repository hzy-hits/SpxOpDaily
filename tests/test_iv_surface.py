from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.config import IvSurfaceSettings
from spx_spark.iv_surface import (
    build_iv_surface_snapshot,
    load_latest_snapshot,
    load_recent_snapshots,
    summarize_surface_history,
    write_snapshot,
)
from spx_spark.marketdata import InstrumentId, MarketDataQuality, OptionGreeks, Provider, Quote
from spx_spark.storage import LatestState


def make_settings(tmp_path) -> IvSurfaceSettings:
    return IvSurfaceSettings(
        data_root=str(tmp_path / "data"),
        latest_surface_path=str(tmp_path / "data" / "latest" / "iv_surface.json"),
        raw_file_name="snapshots.jsonl",
        wide_quote_spread_bps=250.0,
    )


def make_option(
    *,
    expiry: str,
    strike: float,
    right: str,
    mark: float,
    iv: float,
    now: datetime,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:{expiry}:{strike}:{right}",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        bid=mark - 0.1,
        ask=mark + 0.1,
        mark=mark,
        open_interest=1000,
        quote_time=now,
        greeks=OptionGreeks(
            implied_vol=iv,
            delta=0.5 if right == "C" else -0.5,
            gamma=0.003,
            theta=-1.0,
            vega=0.3,
            model="test",
        ),
    )


def make_state(*quotes: Quote, now: datetime) -> LatestState:
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        quote_time=now,
    )
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=(underlier, *quotes),
        best_quotes=(underlier, *quotes),
    )


def test_iv_surface_computes_term_gap_and_change_metrics(tmp_path) -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    settings = make_settings(tmp_path)
    previous_state = make_state(
        make_option(expiry="20260706", strike=7500, right="C", mark=10, iv=0.20, now=now),
        make_option(expiry="20260706", strike=7500, right="P", mark=11, iv=0.22, now=now),
        make_option(expiry="20260707", strike=7500, right="C", mark=20, iv=0.18, now=now),
        make_option(expiry="20260707", strike=7500, right="P", mark=21, iv=0.19, now=now),
        now=now,
    )
    previous = build_iv_surface_snapshot(previous_state, settings=settings)
    current_state = make_state(
        make_option(expiry="20260706", strike=7500, right="C", mark=12, iv=0.25, now=now),
        make_option(expiry="20260706", strike=7500, right="P", mark=13, iv=0.27, now=now),
        make_option(expiry="20260707", strike=7500, right="C", mark=20, iv=0.19, now=now),
        make_option(expiry="20260707", strike=7500, right="P", mark=21, iv=0.20, now=now),
        now=now,
    )

    snapshot = build_iv_surface_snapshot(current_state, settings=settings, previous=previous)
    front = snapshot.expiries[0]

    assert snapshot.front_expiry == "20260706"
    assert snapshot.next_expiry == "20260707"
    assert round(snapshot.front_vs_next_atm_iv_gap or 0.0, 3) == 0.065
    assert round(front.atm_iv_jump_5m or 0.0, 3) == 0.050
    assert front.surface_fit_quality == "raw_grid"


def test_iv_surface_write_round_trips_latest_snapshot(tmp_path) -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    settings = make_settings(tmp_path)
    state = make_state(
        make_option(expiry="20260706", strike=7500, right="C", mark=10, iv=0.20, now=now),
        make_option(expiry="20260706", strike=7500, right="P", mark=11, iv=0.22, now=now),
        now=now,
    )
    snapshot = build_iv_surface_snapshot(state, settings=settings)

    paths = write_snapshot(settings, snapshot)
    loaded = load_latest_snapshot(settings.latest_surface_path)

    assert paths["raw_path"].endswith("snapshots.jsonl")
    assert loaded is not None
    assert loaded.front_expiry == "20260706"


def test_iv_surface_summarizes_one_hour_history(tmp_path) -> None:
    start = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    settings = make_settings(tmp_path)
    first_state = make_state(
        make_option(expiry="20260706", strike=7500, right="C", mark=10, iv=0.20, now=start),
        make_option(expiry="20260706", strike=7500, right="P", mark=11, iv=0.22, now=start),
        now=start,
    )
    first = build_iv_surface_snapshot(first_state, settings=settings)
    write_snapshot(settings, first)

    current_time = start + timedelta(minutes=45)
    current_state = make_state(
        make_option(expiry="20260706", strike=7500, right="C", mark=12, iv=0.26, now=current_time),
        make_option(expiry="20260706", strike=7500, right="P", mark=13, iv=0.28, now=current_time),
        now=current_time,
    )
    current = build_iv_surface_snapshot(current_state, settings=settings, previous=first)
    write_snapshot(settings, current)

    history = load_recent_snapshots(settings, as_of=current.as_of, lookback_minutes=60)
    summary = summarize_surface_history(current, history)

    assert len(history) == 2
    assert summary is not None
    assert summary["snapshot_count"] == 2
    expiry = summary["expiries"][0]
    assert expiry["expiry"] == "20260706"
    assert round(expiry["atm_iv_change_1h"], 3) == 0.06
