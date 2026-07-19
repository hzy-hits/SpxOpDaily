from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.ibkr.quote_demand import (
    build_exact_leg_quote_demand,
    quote_demand_ack_path,
    quote_demand_path,
    write_exact_leg_quote_demand,
    write_quote_demand_tombstone,
)
from spx_spark.ibkr.stream.capacity_tracker import active_market_data_lines
from spx_spark.ibkr.stream.contracts import option_contracts_from_specs
from spx_spark.ibkr.stream.models import OptionSubscriptionPlan
from spx_spark.ibkr.stream.collector import StreamCollector
from spx_spark.ibkr.verifier import VerifyRow
from stream_test_helpers import patch_stream


UTC = timezone.utc


class FakeIB:
    def isConnected(self) -> bool:  # noqa: N802 - mirrors ib_async
        return True

    def sleep(self, _seconds: float) -> None:
        return None


def _subscription(label: str, request_id: int) -> tuple[object, VerifyRow]:
    contract = SimpleNamespace(label=label)
    ticker = SimpleNamespace(contract=contract, ticks=[])
    return ticker, VerifyRow(
        label=label,
        kind="option",
        symbol="SPX",
        subscribed=True,
        request_id=request_id,
    )


def _option_pair(expiry: str, strike: int, request_id: int) -> dict[str, object]:
    return {
        f"option:SPXW:{expiry}:{strike}:C": _subscription(
            f"option:SPXW:{expiry}:{strike}:C", request_id
        ),
        f"option:SPXW:{expiry}:{strike}:P": _subscription(
            f"option:SPXW:{expiry}:{strike}:P", request_id + 1
        ),
    }


def _set_live_nbbo(
    subscription: tuple[object, VerifyRow],
    *,
    at: datetime,
    bid: float,
    ask: float,
) -> None:
    ticker, _row = subscription
    ticker.marketDataType = 1
    ticker.bid = bid
    ticker.ask = ask
    ticker.time = at
    ticker.ticks = [
        SimpleNamespace(time=at, tickType=1),
        SimpleNamespace(time=at, tickType=2),
    ]
    ticker.marketPrice = lambda: (bid + ask) / 2.0


def _collector(tmp_path) -> StreamCollector:
    collector = object.__new__(StreamCollector)
    collector.ib = FakeIB()
    collector.stream_settings = SimpleNamespace(
        exact_leg_pin_enabled=True,
        quote_demand_path="",
        quote_demand_ack_path="",
        market_data_line_capacity=100,
        max_option_lines=84,
    )
    collector.storage_settings = SimpleNamespace(data_root=str(tmp_path))
    collector.skip_options = False
    collector.base_subs = {}
    collector.hot_subs = {}
    collector.rotation_subs = {}
    collector.pinned_subs = {}
    collector.spy_subs = {}
    collector.slow_active_subs = {}
    collector.option_plan = OptionSubscriptionPlan(
        atm_strike=7500,
        expiry="20260720",
        hot=(),
        rotations=(),
    )
    collector.rotation_retry_at = 0.0
    collector.errors = []
    collector.subscription_rejection_sequence = 0
    collector.subscription_rejection_log = []
    collector.subscription_rows_by_req_id = {}
    collector.subscription_lane_by_req_id = {}
    collector.subscription_health_failed = False
    collector.tws_connectivity_lost = False
    collector.subscriptions_lost = False
    collector.tws_connectivity_loss_sequence = 0
    collector.qualified_option_contracts = {}
    collector.option_definition_resolution_sources = {}
    collector.connection_generation = 3
    collector.capacity_tracker = SimpleNamespace(
        effective_capacity=100,
        observe_success=lambda **_kwargs: None,
    )
    collector._initialize_exact_leg_pin()
    return collector


def _demand(now: datetime):
    return build_exact_leg_quote_demand(
        event_id="gth-dip:test",
        status="pending",
        session_date="2026-07-20",
        long_strike=7500,
        short_strike=7550,
        created_at=now,
        updated_at=now,
        valid_until=now + timedelta(seconds=30),
        **_source_fields(now),
    )


def _source_fields(now: datetime) -> dict[str, object]:
    return {
        "source_schema_version": 3,
        "source_policy_version": "gth_dip_reclaim.v4+sha256:test",
        "source_provider": "schwab",
        "coordinate": {
            "kind": "raw_es",
            "instrument_id": "future:ES",
            "observed_value": 7552.0,
            "target_value": 7550.0,
            "spx_observed_value": None,
            "basis_points": 0.0,
            "as_of": now.isoformat(),
            "provider": "schwab",
        },
        "block_reasons": [],
    }


def _patch_transport(monkeypatch, collector: StreamCollector, *, partial_first=False):
    request_id = 10_000
    calls = 0

    def subscribe(_ib, definitions, *, qualify=False):
        nonlocal request_id, calls
        calls += 1
        selected = definitions[:1] if partial_first and calls == 1 else definitions
        result = {}
        for label, kind, contract in selected:
            request_id += 1
            result[label] = (
                SimpleNamespace(contract=contract),
                VerifyRow(
                    label=label,
                    kind=kind,
                    symbol="SPX",
                    subscribed=True,
                    request_id=request_id,
                ),
            )
        return result

    patch_stream(monkeypatch, "qualify_and_subscribe", subscribe)
    patch_stream(monkeypatch, "cancel_subscriptions", lambda *_args: True)
    patch_stream(monkeypatch, "log_event", lambda *_args, **_kwargs: None)


def test_exact_leg_pin_promotes_existing_hot_and_rotation_without_resubscribe(
    tmp_path, monkeypatch
) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    demand = _demand(now)
    long_label, short_label = (leg.label for leg in demand.legs)
    collector.hot_subs[long_label] = _subscription(long_label, 1)
    collector.rotation_subs[short_label] = _subscription(short_label, 2)
    collector._register_subscription_rows(collector.hot_subs, lane="hot")
    collector._register_subscription_rows(collector.rotation_subs, lane="rotation")

    def unexpected_subscribe(*_args, **_kwargs):
        raise AssertionError("existing exact legs must not be subscribed twice")

    patch_stream(monkeypatch, "qualify_and_subscribe", unexpected_subscribe)
    patch_stream(monkeypatch, "log_event", lambda *_args, **_kwargs: None)
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)

    event = collector.reconcile_exact_leg_demand(now=now)

    assert event is not None and event["status"] == "active"
    assert set(collector.pinned_subs) == {long_label, short_label}
    assert long_label not in collector.hot_subs
    assert short_label not in collector.rotation_subs
    assert set(collector.subscription_lane_by_req_id.values()) == {"pinned"}

    refreshed = build_exact_leg_quote_demand(
        event_id=demand.event_id,
        status="pending",
        session_date=demand.session_date,
        long_strike=demand.legs[0].strike,
        short_strike=demand.legs[1].strike,
        created_at=demand.created_at,
        updated_at=now + timedelta(seconds=5),
        valid_until=now + timedelta(seconds=35),
        **_source_fields(now + timedelta(seconds=5)),
    )
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), refreshed)
    second = collector.reconcile_exact_leg_demand(now=now + timedelta(seconds=5))
    assert second is not None and second["reason"] == "lease_refreshed"
    assert set(collector.pinned_subs) == {long_label, short_label}


def test_exact_leg_pin_rejects_unhealthy_existing_rotation_leg(tmp_path) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    demand = _demand(now)
    long_label, short_label = (leg.label for leg in demand.legs)
    collector.hot_subs[long_label] = _subscription(long_label, 1)
    unhealthy = _subscription(short_label, 2)
    unhealthy[1].subscribed = False
    unhealthy[1].error = "IBKR 200: rejected"
    collector.rotation_subs[short_label] = unhealthy
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)

    event = collector.reconcile_exact_leg_demand(now=now)

    assert event is not None and event["status"] == "blocked"
    assert event["reason"] == "existing_rotation_subscription_unhealthy"
    assert collector.pinned_subs == {}


def test_exact_leg_pin_preempts_rotation_pair_and_preserves_six_line_reserve(
    tmp_path, monkeypatch
) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    collector.base_subs = {
        f"base:{index}": _subscription(f"base:{index}", 100 + index)
        for index in range(4)
    }
    for index, strike in enumerate(range(7300, 7440, 5)):
        collector.hot_subs.update(_option_pair("20260720", strike, 1_000 + 2 * index))
    for index, strike in enumerate(range(7600, 7670, 5)):
        collector.rotation_subs.update(
            _option_pair("20260720", strike, 2_000 + 2 * index)
        )
    collector.slow_active_subs = {
        f"slow:{index}": _subscription(f"slow:{index}", 3_000 + index)
        for index in range(6)
    }
    assert active_market_data_lines(collector) == 94
    _patch_transport(monkeypatch, collector)
    demand = _demand(now)
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)

    event = collector.reconcile_exact_leg_demand(now=now)

    assert event is not None and event["status"] == "active"
    assert event["preempted_lines"] == 2
    assert event["subscribed_lines"] == 2
    assert len(collector.rotation_subs) == 26
    assert len(collector.pinned_subs) == 2
    assert active_market_data_lines(collector) == 94


def test_exact_leg_pin_reserves_idle_slow_chunk_before_it_starts(
    tmp_path, monkeypatch
) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    collector.base_subs = {
        f"base:{index}": _subscription(f"base:{index}", 100 + index)
        for index in range(4)
    }
    for index, strike in enumerate(range(7300, 7440, 5)):
        collector.hot_subs.update(_option_pair("20260720", strike, 1_000 + 2 * index))
    for index, strike in enumerate(range(7600, 7670, 5)):
        collector.rotation_subs.update(
            _option_pair("20260720", strike, 2_000 + 2 * index)
        )
    collector.slow_chunks = [[object() for _ in range(6)]]
    assert active_market_data_lines(collector) == 88
    _patch_transport(monkeypatch, collector)
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), _demand(now))

    event = collector.reconcile_exact_leg_demand(now=now)

    assert event is not None and event["status"] == "active"
    assert event["preempted_lines"] == 2
    assert len(collector.rotation_subs) == 26
    collector.slow_active_subs = {
        f"slow:{index}": _subscription(f"slow:{index}", 3_000 + index)
        for index in range(6)
    }
    assert active_market_data_lines(collector) == 94


def test_exact_leg_pin_partial_subscription_restores_preempted_pair(
    tmp_path, monkeypatch
) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    collector.base_subs = {
        f"base:{index}": _subscription(f"base:{index}", 100 + index)
        for index in range(4)
    }
    for index, strike in enumerate(range(7300, 7440, 5)):
        collector.hot_subs.update(_option_pair("20260720", strike, 1_000 + 2 * index))
    for index, strike in enumerate(range(7600, 7670, 5)):
        collector.rotation_subs.update(
            _option_pair("20260720", strike, 2_000 + 2 * index)
        )
    collector.slow_active_subs = {
        f"slow:{index}": _subscription(f"slow:{index}", 3_000 + index)
        for index in range(6)
    }
    _patch_transport(monkeypatch, collector, partial_first=True)
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), _demand(now))

    event = collector.reconcile_exact_leg_demand(now=now)

    assert event is not None and event["status"] == "rejected"
    assert event["reason"] == "exact_leg_subscription_failed"
    assert collector.pinned_subs == {}
    assert len(collector.rotation_subs) == 28
    assert active_market_data_lines(collector) == 94


def test_exact_leg_pin_rolls_back_when_demand_changes_during_subscribe(
    tmp_path, monkeypatch
) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    demand = _demand(now)
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)

    def subscribe(_ib, definitions, *, qualify=False):
        del qualify
        result = {
            label: _subscription(label, 10_000 + index)
            for index, (label, _kind, _contract) in enumerate(definitions)
        }
        write_quote_demand_tombstone(
            quote_demand_path(tmp_path),
            at=now + timedelta(seconds=1),
            reason="superseded_during_subscribe",
            previous_demand_id=demand.demand_id,
            previous_event_id=demand.event_id,
        )
        return result

    patch_stream(monkeypatch, "qualify_and_subscribe", subscribe)
    patch_stream(monkeypatch, "cancel_subscriptions", lambda *_args: True)
    patch_stream(monkeypatch, "log_event", lambda *_args, **_kwargs: None)

    event = collector.reconcile_exact_leg_demand(now=now)

    assert event is not None and event["status"] == "superseded"
    assert event["reason"] == "demand_superseded"
    assert collector.pinned_subs == {}


def test_exact_leg_pin_releases_on_research_expiry_rollover(
    tmp_path, monkeypatch
) -> None:
    before_roll = datetime(2026, 7, 20, 20, 59, 50, tzinfo=UTC)
    collector = _collector(tmp_path)
    _patch_transport(monkeypatch, collector)
    demand = build_exact_leg_quote_demand(
        event_id="gth-dip:rollover",
        status="pending",
        session_date="2026-07-20",
        long_strike=7500,
        short_strike=7550,
        created_at=before_roll,
        updated_at=before_roll,
        valid_until=before_roll + timedelta(seconds=30),
        **_source_fields(before_roll),
    )
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)
    assert collector.reconcile_exact_leg_demand(now=before_roll)["status"] == "active"

    event = collector.reconcile_exact_leg_demand(
        now=before_roll + timedelta(seconds=11)
    )

    assert event is not None and event["status"] == "released"
    assert event["reason"] == "session_expiry_rolled"
    assert collector.pinned_subs == {}


def test_tombstone_releases_pinned_pair(tmp_path, monkeypatch) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    _patch_transport(monkeypatch, collector)
    demand = _demand(now)
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)
    assert collector.reconcile_exact_leg_demand(now=now)["status"] == "active"

    write_quote_demand_tombstone(
        quote_demand_path(tmp_path),
        at=now + timedelta(seconds=1),
        reason="pending_disappeared",
        previous_demand_id=demand.demand_id,
        previous_event_id=demand.event_id,
    )
    event = collector.reconcile_exact_leg_demand(now=now + timedelta(seconds=1))

    assert event is not None and event["status"] == "released"
    assert collector.pinned_subs == {}
    assert collector.exact_leg_pin_demand_id() is None


def test_startup_tombstone_replaces_stale_ack_with_idle(tmp_path) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    write_quote_demand_tombstone(
        quote_demand_path(tmp_path),
        at=now,
        reason="no_exact_leg_quote_demand",
    )

    event = collector.reconcile_exact_leg_demand(now=now)

    assert event is not None and event["status"] == "idle"
    assert event["reason"] == "tombstone"
    assert collector.pinned_subs == {}


def test_exact_leg_pin_rejects_non_current_research_expiry(tmp_path) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    future = build_exact_leg_quote_demand(
        event_id="gth-dip:wrong-session",
        status="pending",
        session_date="2026-07-21",
        long_strike=7500,
        short_strike=7550,
        created_at=now,
        updated_at=now,
        valid_until=now + timedelta(seconds=30),
        **_source_fields(now),
    )
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), future)

    event = collector.reconcile_exact_leg_demand(now=now)

    assert event is not None and event["status"] == "rejected"
    assert event["reason"] == "session_expiry_mismatch"
    assert collector.pinned_subs == {}


def test_exact_leg_pin_ack_tracks_phases_and_only_becomes_ready_after_fresh_nbbo(
    tmp_path, monkeypatch
) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    _patch_transport(monkeypatch, collector)
    demand = _demand(now)
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)

    admitted = collector.reconcile_exact_leg_demand(now=now)

    assert admitted is not None and admitted["status"] == "active"
    assert admitted["quote_state"] == "warming"
    assert admitted["ready_at"] is None
    assert admitted["first_observed_at"] == now.isoformat()
    assert admitted["accepted_at"] == now.isoformat()
    assert admitted["qualification_started_at"] is None
    assert admitted["qualification_finished_at"] is None
    assert admitted["resolution_source"] == "unqualified_passthrough"
    assert admitted["subscription_requested_at"] == now.isoformat()
    assert admitted["submitted_at"] == admitted["subscription_requested_at"]
    assert admitted["automatic_ordering"] is False

    ready_at = now + timedelta(milliseconds=200)
    long_label, short_label = (leg.label for leg in demand.legs)
    _set_live_nbbo(
        collector.pinned_subs[long_label],
        at=ready_at - timedelta(milliseconds=100),
        bid=10.0,
        ask=10.4,
    )
    _set_live_nbbo(
        collector.pinned_subs[short_label],
        at=ready_at - timedelta(milliseconds=150),
        bid=5.0,
        ask=5.4,
    )

    ready = collector.reconcile_exact_leg_demand(now=ready_at)

    assert ready is not None and ready["status"] == "active"
    assert ready["reason"] == "exact_legs_ready"
    assert ready["quote_state"] == "ready"
    assert ready["ready_at"] == ready_at.isoformat()
    assert ready["first_observed_at"] == admitted["first_observed_at"]
    assert ready["accepted_at"] == admitted["accepted_at"]
    assert ready["qualification_started_at"] == admitted["qualification_started_at"]
    assert ready["qualification_finished_at"] == admitted["qualification_finished_at"]
    assert ready["subscription_requested_at"] == admitted["subscription_requested_at"]
    assert ready["nbbo_cross_leg_receipt_skew_seconds"] == 0.05
    assert ready["nbbo_cross_leg_transport_skew_seconds"] == 0.0
    assert ready["nbbo_receipt_time_basis"] == "ib_async_owner_packet_received_at"
    persisted = json.loads(quote_demand_ack_path(tmp_path).read_text())
    assert persisted["kind"] == "ibkr_exact_leg_quote_demand_ack"
    assert all(persisted[key] == value for key, value in ready.items())
    assert collector.reconcile_exact_leg_demand(now=ready_at) is None


def test_exact_leg_pin_ready_rejects_one_sided_nbbo_and_receipt_skew(
    tmp_path, monkeypatch
) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    _patch_transport(monkeypatch, collector)
    demand = _demand(now)
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)
    assert collector.reconcile_exact_leg_demand(now=now)["quote_state"] == "warming"
    long_label, short_label = (leg.label for leg in demand.legs)

    check_at = now + timedelta(seconds=1)
    _set_live_nbbo(
        collector.pinned_subs[long_label],
        at=check_at - timedelta(seconds=5),
        bid=10.0,
        ask=10.4,
    )
    _set_live_nbbo(
        collector.pinned_subs[short_label],
        at=check_at + timedelta(seconds=1),
        bid=5.0,
        ask=5.4,
    )
    assert collector.reconcile_exact_leg_demand(now=check_at) is None
    assert collector._exact_leg_ready_at is None

    corrected_at = check_at + timedelta(milliseconds=100)
    _set_live_nbbo(
        collector.pinned_subs[long_label],
        at=corrected_at,
        bid=10.0,
        ask=10.4,
    )
    _set_live_nbbo(
        collector.pinned_subs[short_label],
        at=corrected_at,
        bid=5.0,
        ask=-1.0,
    )
    assert collector.reconcile_exact_leg_demand(now=corrected_at) is None
    assert collector._exact_leg_ready_at is None

    _set_live_nbbo(
        collector.pinned_subs[short_label],
        at=corrected_at,
        bid=5.0,
        ask=5.4,
    )
    ready = collector.reconcile_exact_leg_demand(now=corrected_at)
    assert ready is not None and ready["quote_state"] == "ready"


def test_exact_leg_pin_reused_quotes_require_new_bbo_and_ignore_oi_only_updates(
    tmp_path, monkeypatch
) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    _patch_transport(monkeypatch, collector)
    demand = _demand(now)
    long_label, short_label = (leg.label for leg in demand.legs)
    collector.hot_subs[long_label] = _subscription(long_label, 1)
    collector.rotation_subs[short_label] = _subscription(short_label, 2)
    _set_live_nbbo(collector.hot_subs[long_label], at=now, bid=10.0, ask=10.4)
    _set_live_nbbo(collector.rotation_subs[short_label], at=now, bid=5.0, ask=5.4)
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)
    admitted = collector.reconcile_exact_leg_demand(now=now)
    assert admitted["quote_state"] == "warming"
    assert admitted["resolution_source"] == "active_subscription"
    assert collector._exact_leg_ready_at is None

    check_at = now + timedelta(seconds=1)
    for ticker, _row in collector.pinned_subs.values():
        ticker.time = check_at
        ticker.callOpenInterest = 1234.0
        ticker.modelGreeks = SimpleNamespace(delta=0.5)
        ticker.ticks = [SimpleNamespace(time=check_at, tickType=27)]

    assert collector.reconcile_exact_leg_demand(now=check_at) is None
    assert collector._exact_leg_ready_at is None

    bbo_at = check_at + timedelta(milliseconds=100)
    long_ticker, _long_row = collector.pinned_subs[long_label]
    long_ticker.bid = 10.1
    long_ticker.ask = 10.5
    long_ticker.ticks = [SimpleNamespace(time=bbo_at, tickType=1)]
    _set_live_nbbo(
        collector.pinned_subs[short_label],
        at=bbo_at,
        bid=5.1,
        ask=5.5,
    )
    assert collector.reconcile_exact_leg_demand(now=bbo_at) is None
    assert collector._exact_leg_ready_at is None

    ask_at = bbo_at + timedelta(milliseconds=10)
    long_ticker.ticks = [SimpleNamespace(time=ask_at, tickType=2)]
    ready = collector.reconcile_exact_leg_demand(now=ask_at)
    assert ready is not None and ready["quote_state"] == "ready"


def test_exact_leg_pin_memory_cache_hit_has_no_qualification_rpc_timestamps(
    tmp_path, monkeypatch
) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    _patch_transport(monkeypatch, collector)
    demand = _demand(now)
    cached = option_contracts_from_specs(demand.specs())
    collector.qualified_option_contracts = {
        label: (label, kind, contract) for label, kind, contract in cached
    }
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)

    event = collector.reconcile_exact_leg_demand(now=now)

    assert event is not None and event["resolution_source"] == "memory_cache"
    assert event["qualification_started_at"] is None
    assert event["qualification_finished_at"] is None


def test_exact_leg_pin_records_qualification_timestamps_only_for_real_rpc(
    tmp_path, monkeypatch
) -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    collector = _collector(tmp_path)
    _patch_transport(monkeypatch, collector)
    qualify_calls: list[int] = []

    def qualify(*contracts):
        qualify_calls.append(len(contracts))
        for index, contract in enumerate(contracts, start=1):
            contract.conId = 900_000 + index
        return list(contracts)

    collector.ib.qualifyContracts = qualify
    demand = _demand(now)
    write_exact_leg_quote_demand(quote_demand_path(tmp_path), demand)

    event = collector.reconcile_exact_leg_demand(now=now)

    assert qualify_calls == [2]
    assert event is not None and event["resolution_source"] == "ibkr_qualification"
    assert event["qualification_started_at"] == now.isoformat()
    assert event["qualification_finished_at"] == now.isoformat()
