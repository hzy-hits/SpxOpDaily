from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.application.globex_trend import service as globex_service
from spx_spark.application.globex_trend.machine import (
    advance_trend_state,
    initial_state,
)
from spx_spark.application.globex_trend.service import (
    acknowledge_advisory_delivery,
    alert_from_event,
    continuation_alert_from_event,
    gth_advisory_allowed,
    pending_event,
    reconcile_acknowledged_advisory,
    select_live_es,
    trend_context_id,
)
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.notifier import direct_push_alerts
from spx_spark.settings.globex_trend import GlobexTrendSettings
from spx_spark.storage import LatestState


UTC = timezone.utc


def test_trend_context_resets_at_gth_and_rth_boundaries() -> None:
    assert trend_context_id(datetime(2026, 7, 13, 23, 0, tzinfo=UTC)).endswith(":globex")
    assert trend_context_id(datetime(2026, 7, 14, 0, 30, tzinfo=UTC)).endswith(":gth")
    assert trend_context_id(datetime(2026, 7, 14, 13, 30, tzinfo=UTC)).endswith(":rth")


def test_new_gth_context_can_confirm_a_short_impulse_before_sixty_minutes() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 14, 0, 15, tzinfo=UTC)
    state = initial_state("2026-07-14:gth")
    transition = None
    for minute, price in ((0, 7600.0), (15, 7590.0), (16, 7589.0)):
        observed_at = start + timedelta(minutes=minute)
        state, transition = advance_trend_state(
            state,
            session_id="2026-07-14:gth",
            at=observed_at,
            price=price,
            provider="ibkr",
            source_at=observed_at,
            policy=policy,
        )

    assert transition is not None
    assert transition["to_regime"] == "bearish"
    assert transition["reason"] == "initial_short_impulse"


def test_globex_replay_detects_down_up_down_without_churn() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 13, 0, 0, tzinfo=UTC)
    anchors = (
        (0, 7600.0),
        (60, 7590.0),
        (120, 7580.0),
        (180, 7570.0),
        (240, 7585.0),
        (300, 7605.0),
        (360, 7605.0),
        (390, 7590.0),
    )
    state = initial_state("2026-07-13")
    transitions: list[dict[str, object]] = []
    for minute, price in _minute_path(anchors):
        observed_at = start + timedelta(minutes=minute)
        state, transition = advance_trend_state(
            state,
            session_id="2026-07-13",
            at=observed_at,
            price=price,
            provider="schwab",
            source_at=observed_at,
            policy=policy,
        )
        if transition is not None and transition.get("event_type") != "continuation":
            transitions.append(transition)

    assert [event["to_regime"] for event in transitions] == [
        "bearish",
        "bullish",
        "bearish",
    ]
    assert [event["reason"] for event in transitions] == [
        "multi_horizon_downtrend",
        "confirmed_reversal_from_regime_low",
        "confirmed_reversal_from_regime_high",
    ]


def test_duplicate_source_timestamp_is_not_sampled_twice() -> None:
    policy = GlobexTrendSettings()
    observed_at = datetime(2026, 7, 13, 0, 0, tzinfo=UTC)
    state, _ = advance_trend_state(
        initial_state("2026-07-13"),
        session_id="2026-07-13",
        at=observed_at,
        price=7600.0,
        provider="schwab",
        source_at=observed_at,
        policy=policy,
    )
    repeated, transition = advance_trend_state(
        state,
        session_id="2026-07-13",
        at=observed_at + timedelta(minutes=1),
        price=7590.0,
        provider="schwab",
        source_at=observed_at,
        policy=policy,
    )

    assert transition is None
    assert len(repeated["samples"]) == 1


def test_live_es_selection_uses_freshest_vendor_quote_and_falls_back() -> None:
    now = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    schwab = _es_quote(Provider.SCHWAB, 7599.0, now - timedelta(seconds=8), now)
    ibkr = _es_quote(Provider.IBKR, 7600.0, now - timedelta(seconds=1), now)
    state = LatestState(now, now, (schwab, ibkr), (schwab,))

    selected = select_live_es(state, now=now, policy=GlobexTrendSettings())
    assert selected is not None
    assert selected.quote is ibkr

    stale_ibkr = _es_quote(Provider.IBKR, 7600.0, now - timedelta(seconds=120), now)
    fallback_state = LatestState(now, now, (schwab, stale_ibkr), (schwab,))
    selected = select_live_es(fallback_state, now=now, policy=GlobexTrendSettings())
    assert selected is not None
    assert selected.quote is schwab


def test_live_es_selection_uses_field_clock_and_future_family() -> None:
    now = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    expiring = Quote(
        instrument=InstrumentId.future("ES", expiry="202609"),
        provider=Provider.IBKR,
        received_at=now,
        last_update_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        bid=7547.0,
        ask=7549.0,
        mark=9000.0,
    )
    state = LatestState(now, now, (expiring,), (expiring,))

    selected = select_live_es(state, now=now, policy=GlobexTrendSettings())

    assert selected is not None
    assert selected.price == 7548.0
    assert selected.price_kind == "mid"
    assert selected.quote is expiring

    close_only = replace(
        expiring,
        bid=None,
        ask=None,
        mark=None,
        close=9001.0,
    )
    assert (
        select_live_es(
            LatestState(now, now, (close_only,), (close_only,)),
            now=now,
            policy=GlobexTrendSettings(),
        )
        is None
    )


def test_live_es_selection_rejects_future_source_timestamp() -> None:
    now = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    future = _es_quote(Provider.SCHWAB, 7600.0, now + timedelta(seconds=6), now)
    state = LatestState(now, now, (future,), (future,))

    assert select_live_es(state, now=now, policy=GlobexTrendSettings()) is None


def test_confirmed_globex_transition_is_audit_only() -> None:
    alert = {
        "kind": "globex_trend_transition",
        "instrument_id": "future:ES",
        "research_only": False,
    }

    assert direct_push_alerts([alert]) == []


def test_confirmed_gth_advisory_events_are_audit_only() -> None:
    for kind in ("gth_directional_advisory", "gth_advisory_management"):
        alert = {
            "kind": kind,
            "instrument_id": "future:ES",
            "research_only": False,
        }

        assert direct_push_alerts([alert]) == []


def test_advisory_invalidation_remains_direct() -> None:
    alert = {
        "kind": "gth_advisory_invalidated",
        "instrument_id": "future:ES",
        "research_only": False,
    }

    assert direct_push_alerts([alert]) == [alert]


def test_bullish_continuation_emits_two_confirmed_ten_point_milestones() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 27, 1, 41, tzinfo=UTC)
    state = _active_trend_state(
        session_id="2026-07-27:gth",
        at=start,
        price=7498.75,
        regime="bullish",
    )
    events: list[dict[str, object]] = []
    observations = (
        (1, 7509.0),
        (2, 7509.25),
        (3, 7519.0),
        (32, 7519.0),
        (33, 7519.25),
        (64, 7529.0),
        (65, 7529.25),
    )
    for minute, price in observations:
        observed_at = start + timedelta(minutes=minute)
        state, event = advance_trend_state(
            state,
            session_id="2026-07-27:gth",
            at=observed_at,
            price=price,
            provider="schwab",
            source_at=observed_at,
            policy=policy,
        )
        if event is not None:
            events.append(event)
            if event.get("signal_stage") == "entry_advisory":
                acknowledge_advisory_delivery(
                    state,
                    event,
                    accepted_at=observed_at,
                )

    assert [event["milestone_index"] for event in events] == [1, 2]
    assert [event["event_id"] for event in events] == [
        "globex-cont:2026-07-27:gth:1:up:m1",
        "globex-cont:2026-07-27:gth:1:up:m2",
    ]
    assert events[0]["signal_stage"] == "entry_advisory"
    assert events[0]["option_right"] == "C"
    assert events[0]["execution_eligible"] is False
    assert events[0]["contract_id"] is None
    assert events[0]["entry_limit"] is None
    assert events[1]["signal_stage"] == "opportunity_management"
    assert events[1]["parent_advisory_id"] == events[0]["advisory_id"]
    assert events[1]["operator_action"] == "conditional_take_profit_or_raise_stop"
    assert all(event["automatic_ordering"] is False for event in events)


def test_continuation_confirmation_survives_provider_switch() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 27, 1, 41, tzinfo=UTC)
    state = _active_trend_state(
        session_id="2026-07-27:gth",
        at=start,
        price=7498.75,
        regime="bullish",
    )
    rows = (
        (1, "schwab"),
        (2, "ibkr"),
        (3, "ibkr"),
        (4, "ibkr"),
    )
    events: list[dict[str, object]] = []
    for minute, provider in rows:
        observed_at = start + timedelta(minutes=minute)
        state, event = advance_trend_state(
            state,
            session_id="2026-07-27:gth",
            at=observed_at,
            price=7509.25,
            provider=provider,
            source_at=observed_at,
            policy=policy,
        )
        if event is not None:
            events.append(event)

    assert len(events) == 1
    assert events[0]["at"] == (start + timedelta(minutes=2)).isoformat()
    assert events[0]["provider"] == "ibkr"


def test_legacy_active_trend_does_not_emit_hindsight_continuation() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 27, 1, 41, tzinfo=UTC)
    state = _active_trend_state(
        session_id="2026-07-27:gth",
        at=start,
        price=7498.75,
        regime="bullish",
    )
    state["version"] = 1
    for key in tuple(state):
        if key.startswith("continuation_") or key == "last_continuation_at":
            state.pop(key)

    state, event = advance_trend_state(
        state,
        session_id="2026-07-27:gth",
        at=start + timedelta(minutes=1),
        price=7524.5,
        provider="schwab",
        source_at=start + timedelta(minutes=1),
        policy=policy,
    )

    assert event is None
    assert state["continuation_suppressed_reason"] == "legacy_active_context_no_hindsight"
    assert state["continuation_events_in_context"] == policy.continuation_session_budget


def test_es_transition_uses_rth_semantics_during_cash_session() -> None:
    event = {
        "event_id": "globex-trend:2026-07-13:1:bullish",
        "at": "2026-07-13T14:00:00+00:00",
        "source_at": "2026-07-13T14:00:00+00:00",
        "from_regime": "bearish",
        "to_regime": "bullish",
        "price": 7577.0,
        "provider": "ibkr",
        "metrics": {},
    }

    alert = alert_from_event(event)

    assert alert.severity == "high"
    assert alert.research_only is True
    assert alert.title == "ES RTH 多头趋势确认"
    assert "ES RTH 趋势确认切换" in alert.detail
    assert "不得按夜盘薄流动性解释" in alert.detail


def test_m1_continuation_alert_is_formal_non_executable_call_advisory() -> None:
    event = {
        "event_type": "continuation",
        "event_id": "globex-cont:2026-07-27:gth:2:up:m1",
        "at": "2026-07-27T05:30:00+00:00",
        "source_at": "2026-07-27T05:30:00+00:00",
        "regime": "bullish",
        "direction": "up",
        "milestone_index": 1,
        "anchor_price": 7498.75,
        "extension_points": 12.5,
        "price": 7511.25,
        "provider": "schwab",
        "metrics": {"return_15m_points": 2.5, "return_60m_points": 9.75},
        "advisory_contract_version": "gth_directional_advisory.v1",
        "advisory_id": "gth-advisory:2026-07-27:gth:2:up",
        "signal_stage": "entry_advisory",
        "option_right": "C",
        "signal_coordinate": {"kind": "future", "instrument_id": "future:ES"},
        "execution_eligible": False,
        "automatic_ordering": False,
    }

    alert = continuation_alert_from_event(event)

    assert alert.kind == "gth_directional_advisory"
    assert alert.event_id == event["event_id"]
    assert "CALL 机会" in alert.title
    assert "评估 CALL 机会" in alert.detail
    assert "EXECUTION_ELIGIBLE=NO" in alert.detail
    assert alert.audit_context is not None
    assert alert.audit_context["contract_id"] is None
    assert alert.audit_context["execution_eligible"] is False


def test_m2_continuation_alert_only_manages_linked_advisory() -> None:
    event = {
        "event_type": "continuation",
        "event_id": "globex-cont:2026-07-27:gth:2:down:m2",
        "at": "2026-07-27T06:30:00+00:00",
        "source_at": "2026-07-27T06:30:00+00:00",
        "regime": "bearish",
        "direction": "down",
        "milestone_index": 2,
        "anchor_price": 7512.0,
        "extension_points": 20.5,
        "price": 7491.5,
        "provider": "schwab",
        "metrics": {},
        "advisory_id": "gth-advisory:2026-07-27:gth:2:down",
        "signal_stage": "opportunity_management",
        "option_right": "P",
        "parent_advisory_id": "gth-advisory:2026-07-27:gth:2:down",
        "execution_eligible": False,
        "automatic_ordering": False,
    }

    alert = continuation_alert_from_event(event)

    assert alert.kind == "gth_advisory_management"
    assert "PUT 机会管理" in alert.title
    assert "禁止把 m2 当作新的 PUT 入场" in alert.detail
    assert alert.audit_context is not None
    assert alert.audit_context["parent_advisory_id"] == event["parent_advisory_id"]


def test_m2_is_suppressed_when_m1_was_blocked() -> None:
    policy = replace(
        GlobexTrendSettings(),
        continuation_cooldown_seconds=1,
    )
    start = datetime(2026, 7, 27, 1, 41, tzinfo=UTC)
    state = _active_trend_state(
        session_id="2026-07-27:gth",
        at=start,
        price=7498.75,
        regime="bullish",
    )
    for minute, price, allowed in (
        (1, 7509.0, False),
        (2, 7509.25, False),
        (3, 7519.0, True),
        (4, 7519.25, True),
    ):
        observed_at = start + timedelta(minutes=minute)
        state, event = advance_trend_state(
            state,
            session_id="2026-07-27:gth",
            at=observed_at,
            price=price,
            provider="schwab",
            source_at=observed_at,
            policy=policy,
            continuation_allowed=allowed,
        )
        assert event is None

    assert state["active_directional_advisory_id"] is None
    assert state["continuation_suppressed_reason"] == "management_without_accepted_entry_advisory"


def test_m2_waits_for_m1_durable_acceptance() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 27, 1, 41, tzinfo=UTC)
    state = _active_trend_state(
        session_id="2026-07-27:gth",
        at=start,
        price=7498.75,
        regime="bullish",
    )
    events: list[dict[str, object]] = []
    for minute, price in (
        (1, 7509.0),
        (2, 7509.25),
        (32, 7519.0),
        (33, 7519.25),
        (34, 7519.5),
    ):
        observed_at = start + timedelta(minutes=minute)
        state, event = advance_trend_state(
            state,
            session_id="2026-07-27:gth",
            at=observed_at,
            price=price,
            provider="schwab",
            source_at=observed_at,
            policy=policy,
        )
        if event is not None:
            events.append(event)

    assert [event["signal_stage"] for event in events] == ["entry_advisory"]
    assert state["active_directional_advisory_id"] is None
    assert state["pending_directional_advisory_id"] == events[0]["advisory_id"]
    assert state["continuation_milestone_index"] == 1
    assert state["continuation_suppressed_reason"] == "management_without_accepted_entry_advisory"


def test_persisted_notification_ack_reconciles_pending_m1_after_restart() -> None:
    accepted_at = datetime(2026, 7, 27, 3, 0, tzinfo=UTC)
    advisory_id = "gth-advisory:2026-07-27:gth:1:up"
    event_id = "globex-cont:2026-07-27:gth:1:up:m1"
    state = initial_state("2026-07-27:gth")
    state.update(
        {
            "pending_directional_advisory_id": advisory_id,
            "pending_event": {
                "event_type": "continuation",
                "event_id": event_id,
                "signal_stage": "entry_advisory",
                "advisory_id": advisory_id,
            },
        }
    )

    assert reconcile_acknowledged_advisory(
        state,
        (event_id,),
        accepted_at=accepted_at,
    )
    assert state["pending_event"] is None
    assert state["pending_directional_advisory_id"] is None
    assert state["active_directional_advisory_id"] == advisory_id
    assert state["active_directional_advisory_accepted_at"] == accepted_at.isoformat()


def test_expired_m1_delivery_clears_pending_parent() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 27, 1, 41, tzinfo=UTC)
    state = _active_trend_state(
        session_id="2026-07-27:gth",
        at=start,
        price=7498.75,
        regime="bullish",
    )
    event = None
    for minute, price in ((1, 7509.0), (2, 7509.25)):
        observed_at = start + timedelta(minutes=minute)
        state, event = advance_trend_state(
            state,
            session_id="2026-07-27:gth",
            at=observed_at,
            price=price,
            provider="schwab",
            source_at=observed_at,
            policy=policy,
        )
    assert event is not None
    assert (
        pending_event(
            state,
            now=datetime.fromisoformat(str(event["at"]))
            + timedelta(seconds=policy.pending_event_ttl_seconds + 1),
            policy=policy,
        )
        is None
    )
    assert state["pending_directional_advisory_id"] is None
    assert state["continuation_suppressed_reason"] == "entry_advisory_delivery_expired"


def test_formal_advisory_only_runs_during_spx_gth() -> None:
    normal = {"mode": "normal", "entry_allowed": True}
    assert not gth_advisory_allowed(
        datetime(2026, 7, 28, 0, 14, tzinfo=UTC),
        normal,
    )
    assert gth_advisory_allowed(
        datetime(2026, 7, 28, 0, 15, tzinfo=UTC),
        normal,
    )
    assert gth_advisory_allowed(
        datetime(2026, 7, 27, 13, 24, tzinfo=UTC),
        normal,
    )
    assert not gth_advisory_allowed(
        datetime(2026, 7, 27, 13, 25, tzinfo=UTC),
        normal,
    )
    assert not gth_advisory_allowed(
        datetime(2026, 7, 27, 5, 30, tzinfo=UTC),
        {"mode": "pre_event", "entry_allowed": False},
    )


def test_trend_reversal_invalidates_accepted_advisory() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 27, 1, 0, tzinfo=UTC)
    state = _active_trend_state(
        session_id="2026-07-27:gth",
        at=start,
        price=7500.0,
        regime="bullish",
    )
    state["active_directional_advisory_id"] = "gth-advisory:test"
    transition = None
    for minute, price in ((15, 7488.0), (16, 7487.5)):
        observed_at = start + timedelta(minutes=minute)
        state, transition = advance_trend_state(
            state,
            session_id="2026-07-27:gth",
            at=observed_at,
            price=price,
            provider="schwab",
            source_at=observed_at,
            policy=policy,
        )

    assert transition is not None
    assert transition["to_regime"] == "bearish"
    assert transition["invalidated_advisory_id"] == "gth-advisory:test"
    assert state["active_directional_advisory_id"] is None
    alert = alert_from_event(transition)
    assert "已失效" in alert.detail
    assert alert.audit_context is not None
    assert alert.audit_context["advisory_lifecycle_action"] == "invalidated"


def test_legacy_budget_is_released_after_next_transition() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 27, 1, 0, tzinfo=UTC)
    state = _active_trend_state(
        session_id="2026-07-27:gth",
        at=start,
        price=7500.0,
        regime="bullish",
    )
    state["version"] = 1
    for key in tuple(state):
        if key.startswith("continuation_") or key in {
            "last_continuation",
            "last_continuation_at",
            "pending_directional_advisory_id",
            "active_directional_advisory_id",
            "active_directional_advisory_accepted_at",
        }:
            state.pop(key)

    state, _ = advance_trend_state(
        state,
        session_id="2026-07-27:gth",
        at=start + timedelta(minutes=1),
        price=7524.5,
        provider="schwab",
        source_at=start + timedelta(minutes=1),
        policy=policy,
    )
    assert state["continuation_migration_budget_consumed"] is True
    transition = None
    for minute, price in ((15, 7511.0), (16, 7510.5), (17, 7510.0)):
        observed_at = start + timedelta(minutes=minute)
        state, transition = advance_trend_state(
            state,
            session_id="2026-07-27:gth",
            at=observed_at,
            price=price,
            provider="schwab",
            source_at=observed_at,
            policy=policy,
        )

    assert transition is not None
    assert state["continuation_migration_budget_consumed"] is False
    assert state["continuation_events_in_context"] == 0


def test_runtime_delivers_confirmed_transition_once(tmp_path, monkeypatch) -> None:
    start = datetime(2026, 7, 14, 0, 15, tzinfo=UTC)
    policy = replace(
        GlobexTrendSettings(),
        confirmation_observations=1,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    delivered: list[str] = []

    class Store:
        def __init__(self, _storage) -> None:
            pass

        def load(self, *, now: datetime) -> LatestState:
            price = 7600.0 if now == start else 7595.0
            quote = _es_quote(Provider.IBKR, price, now, now)
            return LatestState(now, now, (quote,), (quote,))

    def fake_notify(payload, **_kwargs):
        event_id = str(payload["alerts"][0]["event_id"])
        delivered.append(event_id)
        return SimpleNamespace(
            acknowledged_event_ids=(event_id,),
            to_dict=lambda: {"sent_count": 1},
        )

    monkeypatch.setattr(
        globex_service,
        "load_app_settings",
        lambda: SimpleNamespace(globex_trend=policy),
    )
    monkeypatch.setattr(
        globex_service,
        "StorageSettings",
        SimpleNamespace(from_env=lambda: storage),
    )
    monkeypatch.setattr(globex_service, "LatestStateStore", Store)
    monkeypatch.setattr(globex_service, "notify_payload", fake_notify)
    monkeypatch.setattr(
        globex_service,
        "NotificationSettings",
        SimpleNamespace(
            from_env=lambda: SimpleNamespace(state_path=str(tmp_path / "notification-state.json"))
        ),
    )

    assert globex_service.run([], now=start) == 0
    assert globex_service.run([], now=start + timedelta(minutes=15)) == 0
    assert globex_service.run([], now=start + timedelta(minutes=16)) == 0

    assert len(delivered) == 1
    assert delivered[0].endswith(":bearish")


def test_es_transition_keeps_globex_semantics_outside_cash_session() -> None:
    event = {
        "event_id": "globex-trend:2026-07-13:1:bullish",
        "at": "2026-07-13T12:00:00+00:00",
        "source_at": "2026-07-13T12:00:00+00:00",
        "from_regime": "bearish",
        "to_regime": "bullish",
        "price": 7577.0,
        "provider": "ibkr",
        "metrics": {},
    }

    alert = alert_from_event(event)

    assert alert.title == "ES Globex 多头趋势确认"
    assert "现金盘外" in alert.detail


def _minute_path(
    anchors: tuple[tuple[int, float], ...],
) -> list[tuple[int, float]]:
    rows: list[tuple[int, float]] = []
    for (start_minute, start_price), (end_minute, end_price) in zip(anchors, anchors[1:]):
        for minute in range(start_minute, end_minute):
            fraction = (minute - start_minute) / (end_minute - start_minute)
            rows.append((minute, start_price + (end_price - start_price) * fraction))
    rows.append(anchors[-1])
    return rows


def _es_quote(
    provider: Provider,
    price: float,
    source_at: datetime,
    received_at: datetime,
) -> Quote:
    return Quote(
        instrument=InstrumentId.future("ES"),
        provider=provider,
        received_at=received_at,
        last_update_at=received_at,
        quote_time=source_at,
        trade_time=source_at,
        quality=MarketDataQuality.LIVE,
        last=price,
    )


def _active_trend_state(
    *,
    session_id: str,
    at: datetime,
    price: float,
    regime: str,
) -> dict[str, object]:
    state = initial_state(session_id)
    transition = {
        "event_type": "transition",
        "event_id": f"globex-trend:{session_id}:1:{regime}",
        "session_id": session_id,
        "sequence": 1,
        "from_regime": "neutral",
        "to_regime": regime,
        "at": at.isoformat(),
        "source_at": at.isoformat(),
        "price": price,
        "provider": "schwab",
        "metrics": {},
    }
    state.update(
        {
            "regime": regime,
            "transition_sequence": 1,
            "regime_started_at": at.isoformat(),
            "regime_high": price,
            "regime_low": price,
            "samples": [
                {
                    "at": at.isoformat(),
                    "source_at": at.isoformat(),
                    "price": price,
                    "provider": "schwab",
                }
            ],
            "last_transition": transition,
        }
    )
    return state
