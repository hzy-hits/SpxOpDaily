"""Phase 4: Steven forward metrics, baselines, and post-close audit hooks."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.post_close_review import (
    ReviewCompletenessPolicy,
    build_review_payload_from_data,
)
from spx_spark.steven_validation import (
    assert_gex_only_ignores_dex,
    baseline_unconditional_metrics,
    bars_to_jsonl_lines,
    build_replay_payload,
    compute_forward_metrics,
    fold_episode_events,
    gex_only_direction,
    gex_only_direction_from_walls_payload,
    load_bars_jsonl,
    make_bar,
    opening_range_direction,
    return_bps,
    sort_bars,
)

UTC = timezone.utc


def _ts(hour: int, minute: int, second: int = 0, day: int = 13, month: int = 7) -> datetime:
    return datetime(2026, month, day, hour, minute, second, tzinfo=UTC)


def _contract(
    *,
    as_of: datetime,
    machine_state: str = "OBSERVE_ONLY",
    trigger: dict | None = None,
    regime: str = "unknown",
) -> dict:
    return {
        "schema_version": "steven_guidance_contract.v0.1",
        "source": "steven_spx_options_framework_house_proxy",
        "created_at": as_of.isoformat(),
        "as_of": as_of.isoformat(),
        "status": "observe_only",
        "machine_state": machine_state,
        "regime": regime,
        "regime_breadth": {
            "expiries_total": 0,
            "expiries_bullish": 0,
            "expiries_bearish": 0,
            "agreement_ratio": None,
            "weighting": "oi_weighted",
        },
        "map": {"support": [7490.0], "resistance": [7520.0], "pin": None, "acceleration": []},
        "trigger": trigger
        or {
            "kind": "none",
            "level": None,
            "direction": "none",
            "confirmed": False,
            "confirmed_at": None,
            "source_event_id": None,
        },
        "invalidation": {"level": None, "side": "none", "reason": ""},
        "expression_family": "none",
        "confidence": "low",
        "flow_confirmation": {"status": "none", "sources": [], "quality": "weak_proxy"},
        "data_quality": {
            "anchor_ok": True,
            "exposure_quality": "ok",
            "oi_quality": "ibkr_ok",
            "iv_source": "vendor_ibkr",
            "snapshot_age_seconds": 5.0,
        },
        "warnings": [],
    }


def _episode_events_with_setup(
    *,
    confirmed_at: datetime,
    direction: str = "up",
    level: float = 7490.0,
) -> list[dict]:
    pre_at = confirmed_at - timedelta(minutes=30)
    return [
        {
            "schema_version": "steven_episode_event.v0.1",
            "episode_id": "steven:2026-07-13",
            "trading_date": "2026-07-13",
            "seq": 0,
            "recorded_at": pre_at.isoformat(),
            "event_kind": "pre_market_map",
            "from_state": None,
            "to_state": "OBSERVE_ONLY",
            "contract": _contract(as_of=pre_at),
            "note": "pre_market_map",
        },
        {
            "schema_version": "steven_episode_event.v0.1",
            "episode_id": "steven:2026-07-13",
            "trading_date": "2026-07-13",
            "seq": 1,
            "recorded_at": confirmed_at.isoformat(),
            "event_kind": "state_transition",
            "from_state": "BULLISH_DIP_WATCH",
            "to_state": "SETUP_CONFIRMED",
            "contract": _contract(
                as_of=confirmed_at,
                machine_state="SETUP_CONFIRMED",
                regime="bullish",
                trigger={
                    "kind": "dip_hold",
                    "level": level,
                    "direction": direction,
                    "confirmed": True,
                    "confirmed_at": confirmed_at.isoformat(),
                    "source_event_id": "test:setup",
                },
            ),
            "note": "T9",
        },
    ]


def test_horizon_returns_from_synthetic_bars() -> None:
    confirmed_at = _ts(14, 31, 5)
    # Reference bar closes at 14:31:00 with close=7500.
    bars = [
        make_bar(_ts(14, 30, 0), close=7500.0),
        make_bar(_ts(14, 36, 0), close=7506.0),
        make_bar(_ts(14, 46, 0), close=7494.0),
        make_bar(_ts(15, 1, 0), close=7512.0),
        make_bar(_ts(15, 31, 0), close=7488.0),
        make_bar(_ts(19, 59, 0), close=7503.0),  # near regular close 20:00 UTC
    ]
    episode = fold_episode_events(_episode_events_with_setup(confirmed_at=confirmed_at))
    assert episode is not None
    metrics = compute_forward_metrics(episode, bars)
    assert metrics["reference_price"] == pytest.approx(7500.0, rel=1e-9)
    assert metrics["horizons"]["t_plus_5m"]["return_bps"] == pytest.approx(8.0, rel=1e-9)
    assert metrics["horizons"]["t_plus_15m"]["return_bps"] == pytest.approx(-8.0, rel=1e-9)
    assert metrics["horizons"]["t_plus_30m"]["return_bps"] == pytest.approx(
        return_bps(7512.0, 7500.0), rel=1e-9
    )
    assert metrics["horizons"]["t_plus_60m"]["return_bps"] == pytest.approx(
        return_bps(7488.0, 7500.0), rel=1e-9
    )
    assert metrics["horizons"]["t_close"]["price"] == pytest.approx(7503.0, rel=1e-9)
    assert metrics["quality"] == "ok"


def test_horizon_null_when_bar_gap_exceeds_limit() -> None:
    confirmed_at = _ts(14, 31, 5)
    bars = [
        make_bar(_ts(14, 30, 0), close=7500.0),
        make_bar(_ts(14, 36, 0), close=7506.0),
        # +15m target 14:46:05 missing — nearest far away
        make_bar(_ts(14, 50, 0), close=7490.0),
        make_bar(_ts(15, 1, 0), close=7510.0),
        make_bar(_ts(15, 31, 0), close=7500.0),
        make_bar(_ts(19, 59, 0), close=7500.0),
    ]
    episode = fold_episode_events(_episode_events_with_setup(confirmed_at=confirmed_at))
    assert episode is not None
    metrics = compute_forward_metrics(episode, bars)
    horizon = metrics["horizons"]["t_plus_15m"]
    assert horizon["price"] is None
    assert horizon["return_bps"] is None
    assert horizon["sample_gap_seconds"] is None
    assert metrics["quality"] == "partial_bars"


def test_mfe_mae_direction_up_and_down() -> None:
    confirmed_at = _ts(14, 31, 5)
    bars = [
        make_bar(_ts(14, 30, 0), close=7500.0, high=7500.0, low=7500.0),
        make_bar(_ts(14, 35, 0), close=7505.0, high=7516.5, low=7498.0),
        make_bar(_ts(14, 40, 0), close=7497.0, high=7502.0, low=7495.125),
        make_bar(_ts(15, 20, 0), close=7501.0, high=7508.0, low=7496.0),
    ]
    for direction, mfe, mae in (("up", 22.0, -6.5), ("down", 6.5, -22.0)):
        episode = fold_episode_events(
            _episode_events_with_setup(confirmed_at=confirmed_at, direction=direction)
        )
        assert episode is not None
        metrics = compute_forward_metrics(episode, bars)
        assert metrics["mfe_bps"] == pytest.approx(mfe, rel=1e-9)
        assert metrics["mae_bps"] == pytest.approx(mae, rel=1e-9)


def test_touch_reclaim_accept_judgments() -> None:
    """Support-style level: accepted = hold below; reclaimed = hold above after touch.

    Path: wick/break through 7490 → two closes below (accepted) → two closes above (reclaimed).
    """
    confirmed_at = _ts(14, 31, 5)
    level = 7490.0
    bars = [
        make_bar(_ts(14, 30, 0), close=7500.0),
        make_bar(_ts(14, 32, 0), close=7489.0, high=7492.0, low=7488.0),  # touch
        make_bar(_ts(14, 33, 0), close=7487.0, high=7489.0, low=7485.0),
        make_bar(_ts(14, 34, 0), close=7486.0, high=7488.0, low=7484.0),  # accepted
        make_bar(_ts(14, 35, 0), close=7492.0, high=7493.0, low=7488.0),
        make_bar(_ts(14, 36, 0), close=7495.0, high=7496.0, low=7491.0),  # reclaimed
    ]
    episode = fold_episode_events(
        _episode_events_with_setup(confirmed_at=confirmed_at, direction="up", level=level)
    )
    assert episode is not None
    metrics = compute_forward_metrics(episode, bars)
    outcomes = metrics["level_outcomes"]
    assert outcomes["touched"] is True
    assert outcomes["accepted"] is True
    assert outcomes["reclaimed"] is True
    touched_at = datetime.fromisoformat(outcomes["touched_at"])
    accepted_at = datetime.fromisoformat(outcomes["accepted_at"])
    reclaimed_at = datetime.fromisoformat(outcomes["reclaimed_at"])
    assert touched_at < accepted_at < reclaimed_at


def test_close_horizon_uses_session_close_bar() -> None:
    # Day after Thanksgiving 2026-11-27 is an early close (13:00 ET = 18:00 UTC).
    trading_date = "2026-11-27"
    session = DEFAULT_MARKET_CALENDAR.session(date.fromisoformat(trading_date))
    assert session is not None and session.early_close
    confirmed_at = datetime(2026, 11, 27, 15, 0, 5, tzinfo=UTC)
    bars = [
        make_bar(datetime(2026, 11, 27, 14, 59, 0, tzinfo=UTC), close=7500.0),
        make_bar(datetime(2026, 11, 27, 17, 58, 0, tzinfo=UTC), close=7511.0),
        make_bar(datetime(2026, 11, 27, 17, 59, 0, tzinfo=UTC), close=7515.0),
        # Would be used on a full day, but is after early close.
        make_bar(datetime(2026, 11, 27, 19, 59, 0, tzinfo=UTC), close=7400.0),
    ]
    events = _episode_events_with_setup(confirmed_at=confirmed_at)
    for event in events:
        event["trading_date"] = trading_date
        event["episode_id"] = f"steven:{trading_date}"
    episode = fold_episode_events(events)
    assert episode is not None
    metrics = compute_forward_metrics(episode, bars)
    assert metrics["horizons"]["t_close"]["price"] == pytest.approx(7515.0, rel=1e-9)


def test_forward_metrics_recomputable_from_lake_bars(tmp_path: Path) -> None:
    confirmed_at = _ts(14, 31, 5)
    bars = [
        make_bar(_ts(14, 30, 0), close=7500.0),
        make_bar(_ts(14, 36, 0), close=7506.0),
        make_bar(_ts(14, 46, 0), close=7494.0),
        make_bar(_ts(15, 1, 0), close=7512.0),
        make_bar(_ts(15, 31, 0), close=7488.0),
        make_bar(_ts(19, 59, 0), close=7503.0),
    ]
    shuffled = list(reversed(bars))
    lake_dir = tmp_path / "lake" / "steven" / "bars" / "date=2026-07-13"
    lake_dir.mkdir(parents=True)
    lake_path = lake_dir / "spx_bars_1m.jsonl"
    lake_path.write_text(bars_to_jsonl_lines(shuffled), encoding="utf-8")

    episodes_dir = tmp_path / "lake" / "steven" / "episodes" / "date=2026-07-13"
    episodes_dir.mkdir(parents=True)
    events = _episode_events_with_setup(confirmed_at=confirmed_at)
    with (episodes_dir / "episode.jsonl").open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, sort_keys=True) + "\n")

    first = build_replay_payload(trading_date="2026-07-13", data_root=tmp_path)
    second = build_replay_payload(trading_date="2026-07-13", data_root=tmp_path)
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)

    loaded = load_bars_jsonl(lake_path)
    assert [bar.bar_start for bar in loaded] == [bar.bar_start for bar in sort_bars(bars)]
    episode = fold_episode_events(events)
    assert episode is not None
    metrics_a = compute_forward_metrics(episode, shuffled)
    metrics_b = compute_forward_metrics(episode, bars)
    assert metrics_a == metrics_b


def test_no_setup_episode_gets_range_baseline_metrics() -> None:
    as_of = _ts(14, 0, 0)
    events = [
        {
            "schema_version": "steven_episode_event.v0.1",
            "episode_id": "steven:2026-07-13",
            "trading_date": "2026-07-13",
            "seq": 0,
            "recorded_at": as_of.isoformat(),
            "event_kind": "pre_market_map",
            "from_state": None,
            "to_state": "OBSERVE_ONLY",
            "contract": _contract(as_of=as_of),
            "note": "pre_market_map",
        }
    ]
    bars = [
        make_bar(_ts(13, 59, 0), close=7500.0, high=7502.0, low=7498.0),
        make_bar(_ts(14, 5, 0), close=7504.0, high=7510.0, low=7499.0),
        make_bar(_ts(14, 15, 0), close=7496.0, high=7501.0, low=7490.0),
        make_bar(_ts(19, 59, 0), close=7501.0),
    ]
    episode = fold_episode_events(events)
    assert episode is not None
    metrics = compute_forward_metrics(episode, bars)
    assert metrics["direction_hypothesis"] == "range"
    assert metrics["reference_price"] == pytest.approx(7500.0, rel=1e-9)
    # range sign convention: mfe = max |deviation|; mae = -mfe
    assert metrics["mfe_bps"] is not None and metrics["mfe_bps"] > 0
    assert metrics["mae_bps"] == pytest.approx(-metrics["mfe_bps"], rel=1e-9)


def test_post_close_review_attaches_steven_episode_block() -> None:
    trading_date = date(2026, 7, 13)
    confirmed_at = _ts(14, 31, 5)
    events = tuple(_episode_events_with_setup(confirmed_at=confirmed_at))
    bars = tuple(
        [
            make_bar(_ts(14, 30, 0), close=7500.0),
            make_bar(_ts(14, 36, 0), close=7506.0),
            make_bar(_ts(14, 46, 0), close=7494.0),
            make_bar(_ts(15, 1, 0), close=7512.0),
            make_bar(_ts(15, 31, 0), close=7488.0),
            make_bar(_ts(19, 59, 0), close=7503.0),
        ]
    )
    without = build_review_payload_from_data(
        trading_date=trading_date,
        quotes=(),
        snapshots=(),
        now=_ts(21, 0, 0),
        policy=ReviewCompletenessPolicy(),
    )
    with_steven = build_review_payload_from_data(
        trading_date=trading_date,
        quotes=(),
        snapshots=(),
        steven_episode_events=events,
        steven_bars_1m=bars,
        now=_ts(21, 0, 0),
        policy=ReviewCompletenessPolicy(),
    )
    assert "steven_episode" not in without
    assert without["verdict"] == with_steven["verdict"]
    block = with_steven["steven_episode"]
    assert block["forward_metrics"] is not None
    assert block["forward_metrics"]["horizons"]["t_plus_5m"]["return_bps"] == pytest.approx(
        8.0, rel=1e-9
    )


def test_baseline_unconditional_matches_manual_example() -> None:
    day = date(2026, 7, 13)
    entry = datetime(2026, 7, 13, 9, 35, tzinfo=ET).astimezone(UTC)
    # Reference bar: minute before entry wall-clock if entry falls mid-bar alignment.
    ref_start = entry.replace(second=0, microsecond=0) - timedelta(minutes=1)
    bars = [
        make_bar(ref_start, close=7500.0),
        make_bar(entry.replace(second=0, microsecond=0) + timedelta(minutes=5), close=7507.5),
        make_bar(entry.replace(second=0, microsecond=0) + timedelta(minutes=15), close=7492.5),
        make_bar(entry.replace(second=0, microsecond=0) + timedelta(minutes=30), close=7515.0),
        make_bar(entry.replace(second=0, microsecond=0) + timedelta(minutes=60), close=7485.0),
        make_bar(_ts(19, 59, 0), close=7501.5, high=7520.0, low=7470.0),
    ]
    # Fill path highs/lows inside 60m for MFE/MAE.
    bars[1] = make_bar(bars[1].bar_start, close=7507.5, high=7516.5, low=7499.0)
    bars[2] = make_bar(bars[2].bar_start, close=7492.5, high=7505.0, low=7495.125)
    metrics = baseline_unconditional_metrics(bars, trading_date=day)
    assert metrics["direction_hypothesis"] == "up"
    assert metrics["reference_price"] == pytest.approx(7500.0, rel=1e-9)
    assert metrics["horizons"]["t_plus_5m"]["return_bps"] == pytest.approx(10.0, rel=1e-9)
    assert metrics["horizons"]["t_plus_15m"]["return_bps"] == pytest.approx(-10.0, rel=1e-9)
    assert metrics["mfe_bps"] == pytest.approx(22.0, rel=1e-9)
    assert metrics["mae_bps"] == pytest.approx(-6.5, rel=1e-9)


def test_baseline_opening_range_direction_rules() -> None:
    day = date(2026, 7, 13)
    open_et = datetime(2026, 7, 13, 9, 30, tzinfo=ET).astimezone(UTC)
    bars = []
    # Opening range 09:30–10:00 ET: high 7510, low 7490
    for minute in range(30):
        close = 7500.0
        high = 7510.0 if minute == 10 else close
        low = 7490.0 if minute == 20 else close
        bars.append(make_bar(open_et + timedelta(minutes=minute), close=close, high=high, low=low))
    # Break up
    up_as_of = open_et + timedelta(minutes=35)
    bars.append(make_bar(up_as_of.replace(second=0, microsecond=0), close=7515.0))
    assert opening_range_direction(bars, trading_date=day, as_of=up_as_of) == "up"
    # Break down
    down_bars = list(bars[:-1])
    down_as_of = open_et + timedelta(minutes=40)
    down_bars.append(make_bar(down_as_of.replace(second=0, microsecond=0), close=7480.0))
    assert opening_range_direction(down_bars, trading_date=day, as_of=down_as_of) == "down"
    # Inside
    inside_as_of = open_et + timedelta(minutes=45)
    inside_bars = list(bars[:-1])
    inside_bars.append(make_bar(inside_as_of.replace(second=0, microsecond=0), close=7505.0))
    assert opening_range_direction(inside_bars, trading_date=day, as_of=inside_as_of) == "range"


def test_baseline_gex_only_uses_walls_without_dex() -> None:
    assert_gex_only_ignores_dex()
    assert gex_only_direction(spot=7500.0, put_walls=(7485.0,), call_walls=(7550.0,)) == "up"
    assert gex_only_direction(spot=7500.0, put_walls=(7400.0,), call_walls=(7510.0,)) == "down"
    assert gex_only_direction(spot=7500.0, put_walls=(7400.0,), call_walls=(7600.0,)) == "range"
    # Payload path still ignores any net_dex_proxy field if present.
    direction = gex_only_direction_from_walls_payload(
        {
            "put_walls": [{"strike": 7490.0}],
            "call_walls": [{"strike": 7600.0}],
            "net_dex_proxy": 1e18,
        },
        spot=7500.0,
    )
    assert direction == "up"


def test_post_close_review_without_episodes_is_unchanged() -> None:
    trading_date = date(2026, 7, 13)
    payload = build_review_payload_from_data(
        trading_date=trading_date,
        quotes=(),
        snapshots=(),
        now=_ts(21, 0, 0),
        policy=ReviewCompletenessPolicy(),
    )
    assert "steven_episode" not in payload
    assert "verdict" in payload
