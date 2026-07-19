from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import spx_spark.application.market_features.virtual_strategy as virtual_strategy
from spx_spark.application.market_features.virtual_strategy import (
    _episode,
    _evaluate_gth_spread_entry,
    _evaluate_trade_intent_entry,
    _exit_decision,
    _gth_spread_contract_ids,
    _gth_time_stop,
    _new_gth_spread_episode,
    _record_entry_decision,
    _should_replace_with_gth_spread,
    _spread_snapshot,
    _spread_snapshot_decision,
    _trade_intent_action_snapshot,
)
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.settings.market_features import MarketFeatureSettings


UTC = timezone.utc
NOW = datetime(2026, 7, 15, 15, 50, tzinfo=UTC)


def test_trade_episode_preserves_put_direction() -> None:
    episode = _episode(
        source_id="intent:put",
        source_kind="trade_intent",
        direction="down",
        contract_id="option:SPX:SPXW:20260715:7560:P",
        snapshot={"mid": 14.7, "underlier": 7551.0},
        now=NOW,
        stop=NOW + timedelta(minutes=15),
        invalidation_spx=7563.0,
        target_spx=7550.0,
        invalidation_es=None,
    )

    assert episode["direction"] == "down"
    assert episode["execution_assumption"] == "none"


def test_long_put_uses_downside_target_and_upside_invalidation() -> None:
    active = {
        "direction": "down",
        "source_kind": "trade_intent",
        "entry_mid": 14.7,
        "invalidation_spx": 7563.0,
        "target_spx": 7550.0,
        "time_stop_at": (NOW + timedelta(minutes=15)).isoformat(),
    }
    latest = SimpleNamespace(best_quote=lambda _instrument_id: None, as_of=NOW)
    common = {
        "latest": latest,
        "option_structure": {"call_wall": 7560.0},
        "macro_event": {},
        "greek_decision": {},
        "now": NOW,
        "policy": MarketFeatureSettings(),
    }

    assert _exit_decision(active, {"mid": 14.7, "underlier": 7551.0}, **common) == (
        None,
        None,
    )
    assert _exit_decision(active, {"mid": 14.7, "underlier": 7563.0}, **common) == (
        "strategy_invalidation",
        "exit",
    )
    assert _exit_decision(active, {"mid": 14.7, "underlier": 7549.0}, **common) == (
        "underlier_target_reached",
        "take_profit",
    )


def test_long_call_keeps_upside_target_and_downside_invalidation() -> None:
    active = {
        "direction": "up",
        "source_kind": "trade_intent",
        "entry_mid": 10.0,
        "invalidation_spx": 7547.0,
        "target_spx": 7575.0,
        "time_stop_at": (NOW + timedelta(minutes=15)).isoformat(),
    }
    latest = SimpleNamespace(best_quote=lambda _instrument_id: None, as_of=NOW)
    common = {
        "latest": latest,
        "option_structure": {"call_wall": 7550.0},
        "macro_event": {},
        "greek_decision": {},
        "now": NOW,
        "policy": MarketFeatureSettings(),
    }

    assert _exit_decision(active, {"mid": 10.0, "underlier": 7547.0}, **common) == (
        "strategy_invalidation",
        "exit",
    )
    assert _exit_decision(active, {"mid": 10.0, "underlier": 7575.0}, **common) == (
        "underlier_target_reached",
        "take_profit",
    )


def test_gth_time_stop_uses_summer_dst_exit_clock() -> None:
    policy = MarketFeatureSettings()
    now = datetime(2026, 7, 15, 3, 0, tzinfo=UTC)
    assert _gth_time_stop(now, policy=policy) == datetime(2026, 7, 15, 13, 45, tzinfo=UTC)


def test_gth_time_stop_covers_full_session_from_twenty_fifteen_et() -> None:
    policy = MarketFeatureSettings()
    now = datetime(2026, 7, 15, 0, 15, tzinfo=UTC)
    assert _gth_time_stop(now, policy=policy) == datetime(2026, 7, 15, 13, 45, tzinfo=UTC)


def test_gth_time_stop_uses_winter_dst_exit_clock() -> None:
    policy = MarketFeatureSettings()
    now = datetime(2026, 12, 15, 3, 0, tzinfo=UTC)
    assert _gth_time_stop(now, policy=policy) == datetime(2026, 12, 15, 14, 45, tzinfo=UTC)


def test_gth_time_stop_never_rolls_to_next_expiry() -> None:
    policy = replace(MarketFeatureSettings(), virtual_gth_time_stop_minutes=60 * 48)
    now = datetime(2026, 7, 15, 14, 0, tzinfo=UTC)
    assert _gth_time_stop(now, policy=policy) == now


def test_gth_time_stop_at_cutoff_does_not_cross_expiry() -> None:
    policy = replace(MarketFeatureSettings(), virtual_gth_time_stop_minutes=60 * 48)
    now = datetime(2026, 7, 15, 13, 45, tzinfo=UTC)
    assert _gth_time_stop(now, policy=policy) == now


def test_gth_time_stop_respects_minutes_backstop() -> None:
    policy = replace(MarketFeatureSettings(), virtual_gth_time_stop_minutes=60)
    now = datetime(2026, 7, 15, 3, 0, tzinfo=UTC)
    assert _gth_time_stop(now, policy=policy) == datetime(2026, 7, 15, 4, 0, tzinfo=UTC)


def test_gth_spread_contract_ids_use_exact_signal_legs() -> None:
    assert _gth_spread_contract_ids(
        {"right": "C", "long_strike": 7505, "short_strike": 7545},
        session_date="2026-07-15",
    ) == (
        "option:SPX:SPXW:20260715:7505:C",
        "option:SPX:SPXW:20260715:7545:C",
    )


def test_spread_snapshot_tracks_two_leg_net_value(monkeypatch: pytest.MonkeyPatch) -> None:
    observed_at = datetime(2026, 7, 15, 3, 0, tzinfo=UTC)
    snapshots = {
        "long": {
            "mid": 20.0,
            "bid": 19.5,
            "ask": 20.5,
            "iv": 0.20,
            "underlier": 7507.0,
            "delta": 0.55,
            "gamma_per_point": 0.04,
            "color_gamma_per_minute": -0.002,
            "speed_gamma_per_point": -0.001,
            "theta_per_minute": -0.03,
            "vanna_delta_per_vol_point": 0.01,
            "quality": {"status": "ok"},
            "provider": "ibkr",
            "source_at": observed_at.isoformat(),
            "transport_at": observed_at.isoformat(),
        },
        "short": {
            "mid": 8.0,
            "bid": 7.5,
            "ask": 8.5,
            "iv": 0.21,
            "underlier": 7507.0,
            "delta": 0.25,
            "gamma_per_point": 0.02,
            "color_gamma_per_minute": -0.001,
            "speed_gamma_per_point": -0.0004,
            "theta_per_minute": -0.02,
            "vanna_delta_per_vol_point": 0.004,
            "quality": {"status": "ok"},
            "provider": "ibkr",
            "source_at": (observed_at - timedelta(seconds=2)).isoformat(),
            "transport_at": (observed_at - timedelta(seconds=1)).isoformat(),
        },
    }
    monkeypatch.setattr(
        virtual_strategy,
        "_contract_snapshot",
        lambda _latest, contract_id, *, now: snapshots[contract_id],
    )

    result = _spread_snapshot(
        SimpleNamespace(),
        long_contract_id="long",
        short_contract_id="short",
        now=observed_at,
        max_quote_age_seconds=5.0,
        max_quote_skew_seconds=5.0,
    )

    assert result["mid"] == 12.0
    assert result["bid"] == 11.0
    assert result["ask"] == 13.0
    assert result["delta"] == pytest.approx(0.30)
    assert result["gamma_per_point"] == pytest.approx(0.02)
    assert result["leg_source_skew_seconds"] == 2.0
    assert result["leg_transport_skew_seconds"] == 1.0
    assert result["long"] == snapshots["long"]
    assert result["short"] == snapshots["short"]


@pytest.mark.parametrize(
    ("long_age", "short_age"),
    ((6, 0), (0, 6)),
)
def test_spread_snapshot_rejects_stale_leg(
    monkeypatch: pytest.MonkeyPatch,
    long_age: int,
    short_age: int,
) -> None:
    observed_at = datetime(2026, 7, 15, 3, 0, tzinfo=UTC)
    snapshots = {
        "long": {
            "mid": 20.0,
            "provider": "ibkr",
            "source_at": (observed_at - timedelta(seconds=long_age)).isoformat(),
            "transport_at": observed_at.isoformat(),
            "quality": {"status": "ok"},
        },
        "short": {
            "mid": 8.0,
            "provider": "ibkr",
            "source_at": (observed_at - timedelta(seconds=short_age)).isoformat(),
            "transport_at": observed_at.isoformat(),
            "quality": {"status": "ok"},
        },
    }
    monkeypatch.setattr(
        virtual_strategy,
        "_contract_snapshot",
        lambda _latest, contract_id, *, now: snapshots[contract_id],
    )
    assert not _spread_snapshot(
        SimpleNamespace(),
        long_contract_id="long",
        short_contract_id="short",
        now=observed_at,
        max_quote_age_seconds=5.0,
        max_quote_skew_seconds=5.0,
    )


def test_spread_snapshot_rejects_leg_timestamp_skew(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 7, 15, 3, 0, tzinfo=UTC)
    snapshots = {
        "long": {
            "mid": 20.0,
            "provider": "ibkr",
            "source_at": observed_at.isoformat(),
            "transport_at": observed_at.isoformat(),
            "quality": {"status": "ok"},
        },
        "short": {
            "mid": 8.0,
            "provider": "ibkr",
            "source_at": (observed_at - timedelta(seconds=4)).isoformat(),
            "transport_at": observed_at.isoformat(),
            "quality": {"status": "ok"},
        },
    }
    monkeypatch.setattr(
        virtual_strategy,
        "_contract_snapshot",
        lambda _latest, contract_id, *, now: snapshots[contract_id],
    )
    assert not _spread_snapshot(
        SimpleNamespace(),
        long_contract_id="long",
        short_contract_id="short",
        now=observed_at,
        max_quote_age_seconds=5.0,
        max_quote_skew_seconds=3.0,
    )


def test_spread_snapshot_rejects_blocked_leg_quality(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 7, 15, 3, 0, tzinfo=UTC)
    snapshots = {
        "long": {
            "mid": 20.0,
            "provider": "ibkr",
            "source_at": observed_at.isoformat(),
            "transport_at": observed_at.isoformat(),
            "quality": {"status": "ok"},
        },
        "short": {
            "mid": 8.0,
            "provider": "ibkr",
            "source_at": observed_at.isoformat(),
            "transport_at": observed_at.isoformat(),
            "quality": {"status": "blocked"},
        },
    }
    monkeypatch.setattr(
        virtual_strategy,
        "_contract_snapshot",
        lambda _latest, contract_id, *, now: snapshots[contract_id],
    )

    assert not _spread_snapshot(
        SimpleNamespace(),
        long_contract_id="long",
        short_contract_id="short",
        now=observed_at,
        max_quote_age_seconds=5.0,
        max_quote_skew_seconds=5.0,
    )


def test_exact_spread_requires_exchange_source_time_not_transport_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshots = {
        "long": _leg_snapshot(NOW, source_missing=True),
        "short": _leg_snapshot(NOW),
    }
    monkeypatch.setattr(
        virtual_strategy,
        "_contract_snapshot",
        lambda _latest, contract_id, *, now: snapshots[contract_id],
    )

    snapshot, reasons = _spread_snapshot_decision(
        SimpleNamespace(),
        long_contract_id="long",
        short_contract_id="short",
        now=NOW,
        max_quote_age_seconds=5.0,
        max_quote_skew_seconds=5.0,
        required_provider="ibkr",
    )

    assert snapshot == {}
    assert reasons == ["spread_leg_source_time_unavailable"]


def test_exact_spread_rejects_stale_transport_and_transport_skew(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshots = {
        "long": _leg_snapshot(NOW, transport_at=NOW - timedelta(seconds=6)),
        "short": _leg_snapshot(NOW),
    }
    monkeypatch.setattr(
        virtual_strategy,
        "_contract_snapshot",
        lambda _latest, contract_id, *, now: snapshots[contract_id],
    )

    snapshot, reasons = _spread_snapshot_decision(
        SimpleNamespace(),
        long_contract_id="long",
        short_contract_id="short",
        now=NOW,
        max_quote_age_seconds=5.0,
        max_quote_skew_seconds=5.0,
        required_provider="ibkr",
    )

    assert snapshot == {}
    assert "long_leg_transport_stale" in reasons
    assert "spread_leg_transport_timestamp_skew" in reasons


@pytest.mark.parametrize(
    ("long_provider", "short_provider", "reason"),
    (
        ("ibkr", "schwab", "spread_leg_provider_mismatch"),
        ("schwab", "schwab", "spread_provider_not_ibkr"),
    ),
)
def test_gth_exact_spread_requires_same_ibkr_provider(
    monkeypatch: pytest.MonkeyPatch,
    long_provider: str,
    short_provider: str,
    reason: str,
) -> None:
    snapshots = {
        "long": _leg_snapshot(NOW, provider=long_provider),
        "short": _leg_snapshot(NOW, provider=short_provider),
    }
    monkeypatch.setattr(
        virtual_strategy,
        "_contract_snapshot",
        lambda _latest, contract_id, *, now: snapshots[contract_id],
    )

    snapshot, reasons = _spread_snapshot_decision(
        SimpleNamespace(),
        long_contract_id="long",
        short_contract_id="short",
        now=NOW,
        max_quote_age_seconds=5.0,
        max_quote_skew_seconds=5.0,
        required_provider="ibkr",
    )

    assert snapshot == {}
    assert reasons == [reason]


def test_rth_action_snapshot_rechecks_fresh_quote_and_entry_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = InstrumentId.option(
        "SPX",
        expiry="20260715",
        strike=7550,
        right="C",
        trading_class="SPXW",
    )
    quote = Quote(
        instrument=instrument,
        provider=Provider.IBKR,
        received_at=NOW,
        last_update_at=NOW,
        quote_time=NOW,
        quality=MarketDataQuality.LIVE,
        bid=9.8,
        ask=10.0,
    )
    latest = SimpleNamespace(best_quote=lambda _contract_id: quote)
    intent = {
        "contract_id": instrument.canonical_id,
        "provider": "ibkr",
        "entry_limit": 10.1,
        "entry_observation": {
            "at": NOW.isoformat(),
            "contract_id": instrument.canonical_id,
            "entry_limit": 10.1,
            "entry_condition": "displayed_ask_at_or_below_limit",
        },
    }
    monkeypatch.setattr(
        virtual_strategy,
        "_contract_snapshot",
        lambda *_args, **_kwargs: {
            "mid": 9.9,
            "bid": 9.8,
            "ask": 10.0,
            "source_at": NOW.isoformat(),
            "quality": {"status": "ok"},
        },
    )

    snapshot, reasons = _trade_intent_action_snapshot(
        latest,
        trade_intent=intent,
        now=NOW,
        max_quote_age_seconds=5.0,
        future_tolerance_seconds=1.0,
    )

    assert reasons == []
    assert snapshot["entry_limit_satisfied"] is True
    assert snapshot["source_age_seconds"] == 0.0
    assert snapshot["action_revalidated_at"] == NOW.isoformat()

    moved_quote = replace(quote, bid=10.0, ask=10.2)
    moved_latest = SimpleNamespace(best_quote=lambda _contract_id: moved_quote)
    snapshot, reasons = _trade_intent_action_snapshot(
        moved_latest,
        trade_intent=intent,
        now=NOW,
        max_quote_age_seconds=5.0,
        future_tolerance_seconds=1.0,
    )
    assert snapshot == {}
    assert reasons == ["action_entry_limit_not_reached"]


def test_gth_episode_uses_exact_debit_spread_and_net_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        virtual_strategy,
        "_spread_snapshot_decision",
        lambda *_args, **_kwargs: (
            {
                "mid": 12.0,
                "bid": 11.0,
                "ask": 13.0,
                "iv": 0.20,
                "gamma_per_point": 0.02,
                "delta": 0.30,
                "quality": {"status": "ok"},
            },
            [],
        ),
    )
    monkeypatch.setattr(
        virtual_strategy,
        "_action_underlier_snapshot",
        lambda _latest, *, instrument_id, **_kwargs: (
            {
                "instrument_id": instrument_id,
                "price": 7530.0 if instrument_id == "index:SPX" else 7552.0,
                "provider": "ibkr",
            },
            [],
        ),
    )
    episode = _new_gth_spread_episode(
        SimpleNamespace(),
        gth_signal={
            **_gth_contract(),
            "kind": "gth_dip_reclaim_call",
            "event_id": "gth-dip:test",
            "session_date": "2026-07-15",
            "confirmed_at": "2026-07-15T02:59:30+00:00",
            "valid_until": "2026-07-15T03:09:30+00:00",
            "trough": 7546.0,
            "spread": {
                "right": "C",
                "expiry_date": "2026-07-15",
                "long_strike": 7505,
                "short_strike": 7545,
                "width_points": 40,
                "target_wall": 7545.0,
                "exit_at": "2026-07-15T13:45:00+00:00",
            },
        },
        now=datetime(2026, 7, 15, 3, 0, tzinfo=UTC),
        policy=MarketFeatureSettings(),
    )

    assert episode["position_type"] == "call_debit_spread"
    assert episode["schema_version"] == 3
    assert str(episode["policy_version"]).startswith("virtual_strategy_lifecycle.v3+")
    assert episode["coordinate"]["kind"] == "raw_es"
    assert episode["block_reasons"] == []
    assert episode["entry_mid"] == 12.0
    assert episode["entry_bid"] == 11.0
    assert episode["entry_ask"] == 13.0
    assert episode["signal_age_seconds"] == 30.0
    assert episode["decision_evaluated_at"] == "2026-07-15T02:59:30+00:00"
    assert episode["action_revalidated_at"] == "2026-07-15T03:00:00+00:00"
    assert episode["long_contract_id"].endswith(":7505:C")
    assert episode["short_contract_id"].endswith(":7545:C")
    assert episode["target_spx"] == 7545.0
    assert episode["time_stop_at"] == "2026-07-15T13:45:00+00:00"


@pytest.mark.parametrize(
    ("spx", "reason"),
    (
        (7575.0, "target_reached_before_entry_quote"),
        (7547.0, "invalidation_reached_before_entry_quote"),
    ),
)
def test_rth_action_underlier_guard_is_terminal_before_episode(
    monkeypatch: pytest.MonkeyPatch,
    spx: float,
    reason: str,
) -> None:
    intent = _rth_action_contract()
    monkeypatch.setattr(
        virtual_strategy,
        "_trade_intent_action_snapshot",
        lambda *_args, **_kwargs: ({"mid": 10.0, "bid": 9.9, "ask": 10.1}, []),
    )
    monkeypatch.setattr(
        virtual_strategy,
        "_action_underlier_snapshot",
        lambda *_args, **_kwargs: ({"instrument_id": "index:SPX", "price": spx}, []),
    )

    episode, decision = _evaluate_trade_intent_entry(
        SimpleNamespace(created_at=NOW),
        trade_intent=intent,
        now=NOW,
        policy=MarketFeatureSettings(),
        expected_policy_version=str(intent["policy_version"]),
    )

    assert episode == {}
    assert decision["terminal"] is True
    assert decision["block_reasons"] == [reason]
    assert decision["action_quote_snapshot"]["action_underlier"]["price"] == spx


@pytest.mark.parametrize(
    ("spx", "es", "reason"),
    (
        (7545.0, 7552.0, "target_reached_before_entry_quote"),
        (7530.0, 7546.0, "invalidation_reached_before_entry_quote"),
    ),
)
def test_gth_action_underlier_guard_is_terminal_before_episode(
    monkeypatch: pytest.MonkeyPatch,
    spx: float,
    es: float,
    reason: str,
) -> None:
    action_now = datetime(2026, 7, 15, 3, 0, tzinfo=UTC)
    signal = {
        **_gth_contract(),
        "kind": "gth_dip_reclaim_call",
        "event_id": "gth-dip:guard",
        "session_date": "2026-07-15",
        "confirmed_at": "2026-07-15T02:59:30+00:00",
        "trough": 7546.0,
        "spread": {
            "right": "C",
            "expiry_date": "2026-07-15",
            "long_strike": 7505,
            "short_strike": 7545,
            "width_points": 40,
            "target_wall": 7545.0,
            "invalidation_es": 7546.0,
        },
    }
    monkeypatch.setattr(
        virtual_strategy,
        "_spread_snapshot_decision",
        lambda *_args, **_kwargs: ({"mid": 12.0, "bid": 11.0, "ask": 13.0}, []),
    )
    monkeypatch.setattr(
        virtual_strategy,
        "_action_underlier_snapshot",
        lambda _latest, *, instrument_id, **_kwargs: (
            {"instrument_id": instrument_id, "price": spx if instrument_id == "index:SPX" else es},
            [],
        ),
    )

    episode, decision = _evaluate_gth_spread_entry(
        SimpleNamespace(created_at=action_now),
        gth_signal=signal,
        now=action_now,
        policy=MarketFeatureSettings(),
    )

    assert episode == {}
    assert decision["terminal"] is True
    assert decision["block_reasons"] == [reason]


@pytest.mark.parametrize(
    ("confirmed_at", "valid_until"),
    (
        (None, "2026-07-15T03:10:00+00:00"),
        ("2026-07-15T02:50:00+00:00", None),
        ("2026-07-15T02:50:00+00:00", "2026-07-15T02:59:59+00:00"),
        ("2026-07-15T03:00:06+00:00", "2026-07-15T03:10:00+00:00"),
    ),
)
def test_gth_episode_rejects_missing_expired_or_future_signal(
    monkeypatch: pytest.MonkeyPatch,
    confirmed_at: str | None,
    valid_until: str | None,
) -> None:
    monkeypatch.setattr(
        virtual_strategy,
        "_spread_snapshot_decision",
        lambda *_args, **_kwargs: ({"mid": 12.0, "bid": 11.0, "ask": 13.0}, []),
    )
    signal = {
        **_gth_contract(),
        "kind": "gth_dip_reclaim_call",
        "event_id": "gth-dip:test",
        "session_date": "2026-07-15",
        "confirmed_at": confirmed_at,
        "valid_until": valid_until,
        "spread": {
            "right": "C",
            "expiry_date": "2026-07-15",
            "long_strike": 7505,
            "short_strike": 7545,
            "width_points": 40,
        },
    }
    assert not _new_gth_spread_episode(
        SimpleNamespace(),
        gth_signal=signal,
        now=datetime(2026, 7, 15, 3, 0, tzinfo=UTC),
        policy=MarketFeatureSettings(),
    )


def test_gth_episode_rejects_non_executable_debit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        virtual_strategy,
        "_spread_snapshot_decision",
        lambda *_args, **_kwargs: ({"mid": 39.0, "bid": 38.0, "ask": 40.0}, []),
    )
    signal = {
        **_gth_contract(),
        "kind": "gth_dip_reclaim_call",
        "event_id": "gth-dip:test",
        "session_date": "2026-07-15",
        "confirmed_at": "2026-07-15T02:59:30+00:00",
        "valid_until": "2026-07-15T03:09:30+00:00",
        "spread": {
            "right": "C",
            "expiry_date": "2026-07-15",
            "long_strike": 7505,
            "short_strike": 7545,
            "width_points": 40,
        },
    }
    assert not _new_gth_spread_episode(
        SimpleNamespace(),
        gth_signal=signal,
        now=datetime(2026, 7, 15, 3, 0, tzinfo=UTC),
        policy=MarketFeatureSettings(),
    )


def test_gth_spread_can_supersede_legacy_naked_episode() -> None:
    signal = {"kind": "gth_dip_reclaim_call", "spread": {"long_strike": 7505}}
    assert _should_replace_with_gth_spread(
        {"source_kind": "gth_dip_reclaim_call", "contract_id": "legacy-call"}, signal
    )
    assert not _should_replace_with_gth_spread(
        {
            "source_kind": "gth_dip_reclaim_call",
            "position_type": "call_debit_spread",
        },
        signal,
    )
    assert not _should_replace_with_gth_spread(
        {"source_kind": "trade_intent", "contract_id": "rth-option"}, signal
    )


def test_gth_spread_exits_at_eighty_five_percent_of_width() -> None:
    active = {
        "direction": "up",
        "source_kind": "gth_dip_reclaim_call",
        "position_type": "call_debit_spread",
        "entry_mid": 10.0,
        "spread_width_points": 40.0,
        "time_stop_at": (NOW + timedelta(minutes=15)).isoformat(),
    }
    latest = SimpleNamespace(best_quote=lambda _instrument_id: None, as_of=NOW)
    common = {
        "latest": latest,
        "option_structure": {},
        "macro_event": {},
        "greek_decision": {},
        "now": NOW,
        "policy": MarketFeatureSettings(),
    }

    assert _exit_decision(active, {"mid": 33.9}, **common) == (None, None)
    assert _exit_decision(active, {"mid": 34.0}, **common) == (
        "spread_value_saturation",
        "take_profit_or_exit",
    )


def test_gth_entry_decision_records_quote_blockers_then_terminal_expiry(tmp_path) -> None:
    signal = {
        **_gth_contract(),
        "kind": "gth_dip_reclaim_call",
        "event_id": "gth-dip:audit",
        "session_date": "2026-07-15",
        "confirmed_at": "2026-07-15T02:59:30+00:00",
        "trough": 7546.0,
        "spread": {
            "right": "C",
            "expiry_date": "2026-07-15",
            "long_strike": 7505,
            "short_strike": 7545,
            "width_points": 40,
        },
    }
    latest = SimpleNamespace(best_quote=lambda _instrument_id: None)
    episode, observing = _evaluate_gth_spread_entry(
        latest,
        gth_signal=signal,
        now=datetime(2026, 7, 15, 3, 0, tzinfo=UTC),
        policy=MarketFeatureSettings(),
    )
    assert episode == {}
    assert observing["status"] == "observing"
    assert observing["terminal"] is False
    assert observing["block_reasons"] == [
        "long_leg_quote_unavailable",
        "short_leg_quote_unavailable",
    ]

    _, expired = _evaluate_gth_spread_entry(
        latest,
        gth_signal=signal,
        now=datetime(2026, 7, 15, 3, 9, 30, tzinfo=UTC),
        policy=MarketFeatureSettings(),
    )
    assert expired["status"] == "blocked"
    assert expired["terminal"] is True
    assert expired["block_reasons"] == ["signal_expired"]

    storage = SimpleNamespace(data_root=str(tmp_path))
    state: dict[str, dict[str, object]] = {}
    _record_entry_decision(
        storage,
        observing,
        entry_decisions=state,
        now=datetime(2026, 7, 15, 3, 0, tzinfo=UTC),
    )
    _record_entry_decision(
        storage,
        observing,
        entry_decisions=state,
        now=datetime(2026, 7, 15, 3, 0, 5, tzinfo=UTC),
    )
    _record_entry_decision(
        storage,
        expired,
        entry_decisions=state,
        now=datetime(2026, 7, 15, 3, 9, 30, tzinfo=UTC),
    )
    audit_path = (
        tmp_path / "features" / "virtual_strategy" / "date=2026-07-15" / "events.jsonl"
    )
    rows = [json.loads(line) for line in audit_path.read_text().splitlines()]
    assert len(rows) == 2
    assert rows[-1]["block_reasons"] == [
        "long_leg_quote_unavailable",
        "short_leg_quote_unavailable",
        "signal_expired",
    ]


def _leg_snapshot(
    observed_at: datetime,
    *,
    provider: str = "ibkr",
    source_missing: bool = False,
    transport_at: datetime | None = None,
) -> dict[str, object]:
    resolved_transport = transport_at or observed_at
    return {
        "mid": 12.0,
        "bid": 11.5,
        "ask": 12.5,
        "provider": provider,
        "source_at": None if source_missing else observed_at.isoformat(),
        "transport_at": resolved_transport.isoformat(),
        "quality": {"status": "ok"},
    }


def _rth_action_contract() -> dict[str, object]:
    contract_id = "option:SPX:SPXW:20260715:7550:C"
    return {
        "schema_version": 3,
        "policy_version": "rth_trade_intent.v3+sha256:test",
        "valid_until": (NOW + timedelta(seconds=90)).isoformat(),
        "coordinate": {
            "kind": "official_spx",
            "instrument_id": "index:SPX",
            "observed_value": 7554.0,
            "target_value": 7550.0,
            "spx_observed_value": 7554.0,
            "basis_points": 0.0,
            "as_of": NOW.isoformat(),
        },
        "block_reasons": [],
        "status": "trade_ready",
        "intent_id": "candidate:rth-action",
        "evaluated_at": NOW.isoformat(),
        "direction": "up",
        "contract_id": contract_id,
        "provider": "ibkr",
        "entry_limit": 10.1,
        "entry_observation": {
            "at": NOW.isoformat(),
            "contract_id": contract_id,
            "entry_limit": 10.1,
            "entry_condition": "displayed_ask_at_or_below_limit",
        },
        "invalidation_spx": 7547.0,
        "target_spx": 7575.0,
        "time_stop_at": (NOW + timedelta(minutes=15)).isoformat(),
    }


def _gth_contract() -> dict[str, object]:
    return {
        "schema_version": 3,
        "policy_version": "gth_dip_reclaim.v4+sha256:test",
        "valid_until": "2026-07-15T03:09:30+00:00",
        "coordinate": {
            "kind": "raw_es",
            "instrument_id": "future:ES",
            "observed_value": 7552.0,
            "target_value": 7550.0,
            "spx_observed_value": None,
            "basis_points": 0.0,
            "as_of": "2026-07-15T02:59:30+00:00",
        },
        "block_reasons": [],
    }
