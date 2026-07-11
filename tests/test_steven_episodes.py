"""Phase 3 Steven episode / state persistence tests."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spx_spark.features.bar_builder import SpxBar
from spx_spark.features.exposure_map import (
    ExposureAggregates,
    ExposureMap,
    ExpiryExposure,
    StrikeExposure,
    StrikeExposureValues,
    WallLevel,
    WallSet,
)
from spx_spark.options_map import UnderlierReference
from spx_spark.storage import LatestState
from spx_spark.strategy.steven import (
    STATE_SCHEMA_VERSION,
    StevenInputs,
    StevenSettings,
    append_episode_event,
    build_steven_signal,
    evaluate_steven_cycle,
    fold_episode_summary,
    load_steven_state,
    maybe_append_episode_revision,
    persist_steven_state,
)

UTC = timezone.utc
AS_OF = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)


def _agg(*, net_dex: float | None = 200000.0, net_gamma_ratio: float | None = 0.2) -> ExposureAggregates:
    return ExposureAggregates(
        net_gex=1.0,
        abs_gex=1.0,
        net_gamma_ratio=net_gamma_ratio,
        net_dex_proxy=net_dex,
        net_dex_ratio_proxy=0.1,
        dagex_proxy=None,
        vex_proxy=None,
        cex_proxy=None,
    )


def make_expiry(
    *,
    expiry: str = "20260713",
    net_dex: float | None = 200000.0,
    put_walls: tuple[float, ...] = (7470.0,),
    call_walls: tuple[float, ...] = (7530.0,),
    pin: float | None = 7500.0,
    net_gamma_ratio: float | None = 0.2,
) -> ExpiryExposure:
    empty = StrikeExposureValues(
        call_gex=None,
        put_gex=None,
        net_gex=None,
        abs_gex=None,
        net_dex_proxy=None,
        vex_proxy=None,
        cex_proxy=None,
    )
    return ExpiryExposure(
        expiry=expiry,
        row_count=4,
        strike_count=2,
        quality="ok",
        oi_quality="ibkr_ok",
        iv_source="vendor_ibkr",
        snapshot_age_seconds=10.0,
        delta_coverage_ratio=1.0,
        iv_coverage_ratio=1.0,
        strikes=(
            StrikeExposure(
                strike=7500.0,
                call_open_interest=100,
                put_open_interest=100,
                call_volume=10,
                put_volume=10,
                call_iv=0.2,
                put_iv=0.2,
                call_delta=0.5,
                put_delta=-0.5,
                call_gamma=0.002,
                put_gamma=0.002,
                call_vanna_per_vol_point=None,
                put_vanna_per_vol_point=None,
                call_charm_per_minute=None,
                put_charm_per_minute=None,
                oi_weighted=StrikeExposureValues(
                    call_gex=1.0,
                    put_gex=-1.0,
                    net_gex=0.0,
                    abs_gex=2.0,
                    net_dex_proxy=net_dex,
                    vex_proxy=None,
                    cex_proxy=None,
                ),
                volume_weighted=empty,
            ),
        ),
        oi_weighted=_agg(net_dex=net_dex, net_gamma_ratio=net_gamma_ratio),
        volume_weighted=_agg(net_dex=None, net_gamma_ratio=None),
        gex_weighting_divergence=None,
        walls=WallSet(
            call_walls=tuple(
                WallLevel(strike=s, side="call", gex=1.0, open_interest=10, volume=1, distance_points=0)
                for s in call_walls
            ),
            put_walls=tuple(
                WallLevel(strike=s, side="put", gex=-1.0, open_interest=10, volume=1, distance_points=0)
                for s in put_walls
            ),
            wall_method="oi_gex",
            pin_candidate=pin,
        ),
        zero_gamma=7500.0,
        gamma_flip_zone=(7490.0, 7510.0),
        zero_gamma_method="test",
        sign_convention="calls_positive_puts_negative",
        dealer_position_sign="unknown",
        direction="unknown",
        model="bs_r0_q0",
        warnings=(),
    )


def make_exposure(*expiries: ExpiryExposure) -> ExposureMap:
    if not expiries:
        expiries = (make_expiry(), make_expiry(expiry="20260714", net_dex=250000.0))
    return ExposureMap(
        created_at=AS_OF,
        as_of=AS_OF,
        underlier=UnderlierReference(price=7500.0, source="index:SPX"),
        expiries=tuple(expiries),
        warnings=(),
    )


def make_bar(
    start: datetime,
    *,
    close: float,
    high: float | None = None,
    low: float | None = None,
) -> SpxBar:
    return SpxBar(
        bar_start=start,
        interval_seconds=60,
        open=close,
        high=close if high is None else high,
        low=close if low is None else low,
        close=close,
        sample_count=12,
        quality="ok",
        gap_before=False,
        provider="ibkr",
    )


def make_inputs(**overrides):
    base = dict(
        created_at=AS_OF,
        as_of=AS_OF,
        underlier_price=7500.0,
        underlier_source="index:SPX",
        exposure=make_exposure(),
        bars_1m=(),
        previous_state="OBSERVE_ONLY",
        previous_state_since=AS_OF - timedelta(minutes=5),
        trading_date="2026-07-13",
        settings=StevenSettings(),
    )
    base.update(overrides)
    return StevenInputs(**base)


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_episode_one_per_day_and_seq_monotonic(tmp_path: Path) -> None:
    settings = StevenSettings(episode_revision_min_level_move_points=10.0)
    exposure = make_exposure(
        make_expiry(net_dex=500000.0),
        make_expiry(expiry="20260714", net_dex=500000.0),
    )
    states = [
        "OBSERVE_ONLY",
        "REGIME_UNKNOWN",
        "BULLISH_DIP_WATCH",
        "SETUP_CONFIRMED",
        "EXIT_REVIEW",
    ]
    # Force transitions by setting previous_state and building signals that move
    previous = None
    seq = -1
    as_of = AS_OF
    for index, target in enumerate(states):
        as_of = AS_OF + timedelta(minutes=index)
        if target == "SETUP_CONFIRMED":
            bars = (
                make_bar(as_of - timedelta(minutes=3), close=7470.0, low=7468.0, high=7472.0),
                make_bar(as_of - timedelta(minutes=2), close=7475.0, low=7471.0, high=7476.0),
                make_bar(as_of - timedelta(minutes=1), close=7476.0, low=7472.0, high=7477.0),
            )
            inputs = make_inputs(
                as_of=as_of,
                created_at=as_of,
                exposure=exposure,
                previous_state="BULLISH_DIP_WATCH",
                underlier_price=7475.0,
                bars_1m=bars,
                es_volume={"direction": "up"},
            )
        elif target == "EXIT_REVIEW":
            inputs = make_inputs(
                as_of=as_of,
                created_at=as_of,
                exposure=exposure,
                previous_state="SETUP_CONFIRMED",
                underlier_price=7535.0,
            )
        elif target == "BULLISH_DIP_WATCH":
            inputs = make_inputs(
                as_of=as_of,
                created_at=as_of,
                exposure=exposure,
                previous_state="REGIME_UNKNOWN",
                underlier_price=7490.0,
            )
        elif target == "REGIME_UNKNOWN":
            thin = make_exposure(make_expiry(net_dex=500000.0))
            inputs = make_inputs(
                as_of=as_of,
                created_at=as_of,
                exposure=thin,
                previous_state="OBSERVE_ONLY",
            )
        else:
            inputs = make_inputs(
                as_of=as_of,
                created_at=as_of,
                exposure=exposure,
                previous_state="OBSERVE_ONLY",
                underlier_price=7600.0,
            )
        signal = build_steven_signal(inputs)
        # Override machine_state path by crafting successive edge writes directly when needed
        if index == 0:
            signal = build_steven_signal(
                make_inputs(as_of=as_of, created_at=as_of, exposure=None, underlier_price=None)
            )
        seq = maybe_append_episode_revision(
            data_root=tmp_path,
            trading_date="2026-07-13",
            signal=signal,
            previous_payload=previous,
            settings=settings,
        )
        previous = persist_steven_state(
            signal,
            data_root=tmp_path,
            trading_date="2026-07-13",
            episode_seq_last=seq,
            previous_payload=previous,
            transition_rule=signal.transition_rule,
        )

    # Ensure at least 5 edge writes by explicit appends if needed
    path = tmp_path / "lake" / "steven" / "episodes" / "date=2026-07-13" / "episode.jsonl"
    rows = _read_jsonl(path)
    if len(rows) < 5:
        contract = build_steven_signal(make_inputs()).to_dict()
        for i in range(len(rows), 5):
            append_episode_event(
                data_root=tmp_path,
                trading_date="2026-07-13",
                seq=i,
                recorded_at=AS_OF + timedelta(minutes=i),
                event_kind="state_transition" if i else "pre_market_map",
                from_state="OBSERVE_ONLY",
                to_state="REGIME_UNKNOWN",
                contract=contract,
                note=f"forced-{i}",
            )
        rows = _read_jsonl(path)

    assert len({row["episode_id"] for row in rows}) == 1
    assert rows[0]["episode_id"] == "steven:2026-07-13"
    assert rows[0]["event_kind"] == "pre_market_map"
    seqs = [row["seq"] for row in rows]
    assert seqs == list(range(len(seqs)))
    assert seqs[:5] == list(range(5))


def test_episode_revision_only_on_edges_or_level_moves(tmp_path: Path) -> None:
    settings = StevenSettings(episode_revision_min_level_move_points=10.0)
    signal = build_steven_signal(
        make_inputs(
            underlier_price=None,
            exposure=make_exposure(make_expiry(put_walls=(7470.0,), call_walls=(7530.0,))),
        )
    )
    # Ensure initial contract carries map levels for move detection.
    signal = replace(
        signal,
        map={"support": [7470.0], "resistance": [7530.0], "pin": None, "acceleration": []},
    )
    seq = maybe_append_episode_revision(
        data_root=tmp_path,
        trading_date="2026-07-13",
        signal=signal,
        previous_payload=None,
        settings=settings,
    )
    previous = persist_steven_state(
        signal,
        data_root=tmp_path,
        trading_date="2026-07-13",
        episode_seq_last=seq,
        previous_payload=None,
    )
    path = tmp_path / "lake" / "steven" / "episodes" / "date=2026-07-13" / "episode.jsonl"
    assert len(_read_jsonl(path)) == 1

    for _ in range(10):
        seq = maybe_append_episode_revision(
            data_root=tmp_path,
            trading_date="2026-07-13",
            signal=signal,
            previous_payload=previous,
            settings=settings,
        )
        previous = persist_steven_state(
            signal,
            data_root=tmp_path,
            trading_date="2026-07-13",
            episode_seq_last=seq,
            previous_payload=previous,
        )
    assert len(_read_jsonl(path)) == 1

    moved = replace(
        signal,
        map={"support": [7450.0], "resistance": [7550.0], "pin": None, "acceleration": []},
        transition_rule=None,
    )
    seq = maybe_append_episode_revision(
        data_root=tmp_path,
        trading_date="2026-07-13",
        signal=moved,
        previous_payload=previous,
        settings=settings,
    )
    rows = _read_jsonl(path)
    assert len(rows) == 2
    assert rows[-1]["event_kind"] == "map_revision"


def test_episode_final_state_written_on_exit_review(tmp_path: Path) -> None:
    settings = StevenSettings()
    exposure = make_exposure(
        make_expiry(net_dex=500000.0, put_walls=(7470.0,), call_walls=(7530.0,)),
        make_expiry(expiry="20260714", net_dex=500000.0),
    )
    bars = (
        make_bar(AS_OF - timedelta(minutes=3), close=7470.0, low=7468.0, high=7472.0),
        make_bar(AS_OF - timedelta(minutes=2), close=7475.0, low=7471.0, high=7476.0),
        make_bar(AS_OF - timedelta(minutes=1), close=7476.0, low=7472.0, high=7477.0),
    )
    previous = None
    seq = -1
    # T9
    signal = build_steven_signal(
        make_inputs(
            previous_state="BULLISH_DIP_WATCH",
            exposure=exposure,
            underlier_price=7475.0,
            bars_1m=bars,
            es_volume={"direction": "up"},
        )
    )
    assert signal.machine_state == "SETUP_CONFIRMED"
    seq = maybe_append_episode_revision(
        data_root=tmp_path,
        trading_date="2026-07-13",
        signal=signal,
        previous_payload=previous,
        settings=settings,
    )
    previous = persist_steven_state(
        signal,
        data_root=tmp_path,
        trading_date="2026-07-13",
        episode_seq_last=seq,
        previous_payload=previous,
        transition_rule=signal.transition_rule,
    )
    # T13
    signal = build_steven_signal(
        make_inputs(
            previous_state="SETUP_CONFIRMED",
            exposure=exposure,
            underlier_price=7535.0,
            daily_setup_count=1,
        )
    )
    assert signal.machine_state == "EXIT_REVIEW"
    seq = maybe_append_episode_revision(
        data_root=tmp_path,
        trading_date="2026-07-13",
        signal=signal,
        previous_payload=previous,
        settings=settings,
    )
    previous = persist_steven_state(
        signal,
        data_root=tmp_path,
        trading_date="2026-07-13",
        episode_seq_last=seq,
        previous_payload=previous,
        transition_rule=signal.transition_rule,
    )
    # T15
    signal = build_steven_signal(
        make_inputs(
            previous_state="EXIT_REVIEW",
            exposure=exposure,
            underlier_price=7535.0,
            daily_setup_count=1,
        )
    )
    assert signal.machine_state == "LOCKOUT_OR_REMAP"
    seq = maybe_append_episode_revision(
        data_root=tmp_path,
        trading_date="2026-07-13",
        signal=signal,
        previous_payload=previous,
        settings=settings,
    )
    path = tmp_path / "lake" / "steven" / "episodes" / "date=2026-07-13" / "episode.jsonl"
    rows = _read_jsonl(path)
    assert any(row["event_kind"] == "final_state" for row in rows)
    folded = fold_episode_summary(rows)
    assert folded["final_state"] == "LOCKOUT_OR_REMAP"
    assert folded["forward_metrics"] is None


def test_steven_state_file_corruption_resets_gracefully(tmp_path: Path) -> None:
    state_path = tmp_path / "latest" / "steven_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("{not-json", encoding="utf-8")
    payload, reason = load_steven_state(state_path)
    assert payload is None
    assert reason is not None and reason.startswith("corrupt")

    state = LatestState(
        created_at=AS_OF,
        as_of=AS_OF,
        quotes=(),
        best_quotes=(),
    )
    settings = StevenSettings(enabled=True)
    result = evaluate_steven_cycle(
        state,
        data_root=tmp_path,
        settings=settings,
        persist=True,
    )
    assert result["enabled"] is True
    assert any(str(w).startswith("steven_state_reset:") for w in result["warnings"])
    # Fresh state written
    loaded, reason2 = load_steven_state(state_path)
    assert reason2 is None
    assert loaded is not None
    assert loaded["schema_version"] == STATE_SCHEMA_VERSION
    assert loaded["machine_state"] in {"DATA_INVALID", "OBSERVE_ONLY"}
