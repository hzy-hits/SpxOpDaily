from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.application.shock.service import _persist_gth_quote_demand
from spx_spark.ibkr.quote_demand import (
    QUOTE_DEMAND_POLICY_VERSION,
    QUOTE_DEMAND_SCHEMA_VERSION,
    QUOTE_DEMAND_V1_POLICY_VERSION,
    QUOTE_DEMAND_V1_SCHEMA_VERSION,
    build_exact_leg_quote_demand,
    load_exact_leg_quote_demand,
    parse_exact_leg_quote_demand,
    quote_demand_ack_path,
    quote_demand_path,
    select_gth_quote_demand,
    spxw_call_strike_from_contract_id,
    write_exact_leg_quote_demand,
    write_quote_demand_tombstone,
)
from spx_spark.state_io import atomic_write_json_secure, read_json_object
from spx_spark.strategy_contract import policy_version as strategy_policy_version


NOW = datetime(2026, 7, 15, 3, 0, tzinfo=timezone.utc)
SESSION = "2026-07-15"


def demand(**overrides):
    values = {
        "event_id": "gth-dip:event-1",
        "status": "pending",
        "session_date": SESSION,
        "long_strike": 7505,
        "short_strike": 7545,
        "created_at": NOW,
        "updated_at": NOW,
        "valid_until": NOW + timedelta(seconds=30),
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
            "as_of": NOW.isoformat(),
            "provider": "schwab",
        },
        "block_reasons": [],
    }
    values.update(overrides)
    return build_exact_leg_quote_demand(**values)


def spread() -> dict[str, object]:
    return {
        "right": "C",
        "expiry_date": SESSION,
        "long_strike": 7505,
        "short_strike": 7545,
        "exit_at": (NOW + timedelta(hours=10)).isoformat(),
    }


def source_contract(*, policy: str = "gth_dip_reclaim.v4+sha256:test") -> dict[str, object]:
    return {
        "schema_version": 3,
        "policy_version": policy,
        "valid_until": (NOW + timedelta(minutes=10)).isoformat(),
        "coordinate": {
            "kind": "raw_es",
            "instrument_id": "future:ES",
            "observed_value": 7552.0,
            "target_value": 7550.0,
            "spx_observed_value": None,
            "basis_points": 0.0,
            "as_of": NOW.isoformat(),
            "provider": "schwab",
        },
        "block_reasons": [],
        "automatic_ordering": False,
    }


def test_contract_round_trip_paths_specs_and_labels(tmp_path) -> None:
    path = quote_demand_path(tmp_path)
    assert path == tmp_path / "latest" / "ibkr_exact_leg_quote_demand.json"
    assert quote_demand_ack_path(tmp_path).name == "ibkr_exact_leg_quote_demand_ack.json"

    expected = demand()
    write_exact_leg_quote_demand(path, expected)
    raw = read_json_object(path)
    loaded, issue = load_exact_leg_quote_demand(path, now=NOW)

    assert issue is None
    assert loaded == expected
    assert raw["schema_version"] == QUOTE_DEMAND_SCHEMA_VERSION == 2
    assert raw["policy_version"] == QUOTE_DEMAND_POLICY_VERSION
    assert [leg["right"] for leg in raw["legs"]] == ["C", "C"]
    assert [leg.label for leg in loaded.legs] == [
        "option:SPXW:20260715:7505:C",
        "option:SPXW:20260715:7545:C",
    ]
    assert [(row.expiry, row.strike, row.right, row.lane) for row in loaded.specs()] == [
        ("20260715", 7505, "C", "pinned"),
        ("20260715", 7545, "C", "pinned"),
    ]


@pytest.mark.parametrize("explicit_call_right", (False, True))
def test_reader_accepts_frozen_v1_call_contract(
    tmp_path,
    explicit_call_right: bool,
) -> None:
    payload = demand().to_dict()
    payload["schema_version"] = QUOTE_DEMAND_V1_SCHEMA_VERSION
    payload["policy_version"] = QUOTE_DEMAND_V1_POLICY_VERSION
    if not explicit_call_right:
        for leg in payload["legs"]:
            leg.pop("right")
    atomic_write_json_secure(quote_demand_path(tmp_path), payload)

    loaded, issue = load_exact_leg_quote_demand(
        quote_demand_path(tmp_path),
        now=NOW,
    )

    assert issue is None
    assert loaded is not None
    assert loaded.schema_version == QUOTE_DEMAND_V1_SCHEMA_VERSION
    assert loaded.policy_version == QUOTE_DEMAND_V1_POLICY_VERSION
    assert [leg.right for leg in loaded.legs] == ["C", "C"]
    with pytest.raises(ValueError, match="writer_requires_current_schema_version"):
        write_exact_leg_quote_demand(quote_demand_path(tmp_path), loaded)


def test_v1_policy_hash_is_frozen_and_distinct_from_v2() -> None:
    assert QUOTE_DEMAND_V1_POLICY_VERSION == strategy_policy_version(
        "ibkr_exact_leg_quote_demand.v1",
        {
            "schema_version": 1,
            "lease_seconds": 30,
            "max_lease_seconds": 45,
            "contract": "same-session SPXW call debit spread",
            "quote_provider": "ibkr",
            "automatic_ordering": False,
        },
    )
    assert QUOTE_DEMAND_V1_POLICY_VERSION != QUOTE_DEMAND_POLICY_VERSION


def test_v1_never_reinterprets_put_and_v2_requires_explicit_right() -> None:
    legacy_put = demand(long_strike=7505, short_strike=7465, right="P").to_dict()
    legacy_put["schema_version"] = QUOTE_DEMAND_V1_SCHEMA_VERSION
    legacy_put["policy_version"] = QUOTE_DEMAND_V1_POLICY_VERSION
    parsed, issue = parse_exact_leg_quote_demand(legacy_put, now=NOW)
    assert parsed is None
    assert issue == "malformed"

    v2_without_right = demand().to_dict()
    v2_without_right["legs"][0].pop("right")
    parsed, issue = parse_exact_leg_quote_demand(v2_without_right, now=NOW)
    assert parsed is None
    assert issue == "malformed"


def test_put_debit_spread_contract_round_trip(tmp_path) -> None:
    path = quote_demand_path(tmp_path)
    expected = demand(long_strike=7505, short_strike=7465, right="P")

    write_exact_leg_quote_demand(path, expected)
    loaded, issue = load_exact_leg_quote_demand(path, now=NOW)

    assert issue is None
    assert loaded == expected
    assert [(leg.strike, leg.right) for leg in loaded.legs] == [
        (7505, "P"),
        (7465, "P"),
    ]


def test_valid_until_is_exclusive_and_tombstone_fails_closed(tmp_path) -> None:
    path = quote_demand_path(tmp_path)
    write_exact_leg_quote_demand(path, demand(valid_until=NOW + timedelta(seconds=1)))
    loaded, issue = load_exact_leg_quote_demand(
        path, now=NOW + timedelta(seconds=1)
    )
    assert loaded is None
    assert issue == "expired"

    write_quote_demand_tombstone(path, at=NOW, reason="provider_switched")
    loaded, issue = load_exact_leg_quote_demand(path, now=NOW)
    assert loaded is None
    assert issue == "tombstone"
    assert read_json_object(path)["reason"] == "provider_switched"


def test_parser_rejects_long_or_future_dated_lease() -> None:
    long_lease = demand().to_dict()
    long_lease["valid_until"] = (NOW + timedelta(hours=1)).isoformat()
    parsed, issue = parse_exact_leg_quote_demand(long_lease, now=NOW)
    assert parsed is None
    assert issue == "lease_too_long"

    future = demand(
        created_at=NOW + timedelta(seconds=6),
        updated_at=NOW + timedelta(seconds=6),
        valid_until=NOW + timedelta(seconds=36),
    ).to_dict()
    parsed, issue = parse_exact_leg_quote_demand(future, now=NOW)
    assert parsed is None
    assert issue == "updated_at_in_future"


@pytest.mark.parametrize(
    ("mutation", "issue"),
    (
        (lambda row: row.update(schema_version=99), "schema_version_mismatch"),
        (
            lambda row: row.update(quote_provider="schwab"),
            "quote_provider_mismatch",
        ),
        (lambda row: row.update(session_date="20260715"), "malformed"),
        (lambda row: row["legs"][0].update(right="P"), "malformed"),
        (lambda row: row["legs"][0].update(strike=7501), "malformed"),
        (lambda row: row.update(unrecognized=True), "fields_invalid"),
    ),
)
def test_parser_rejects_malformed_payloads(mutation, issue: str) -> None:
    payload = demand().to_dict()
    mutation(payload)
    parsed, actual_issue = parse_exact_leg_quote_demand(payload, now=NOW)
    assert parsed is None
    assert actual_issue == issue


def test_contract_id_parser_requires_exact_same_session_spxw_call() -> None:
    assert (
        spxw_call_strike_from_contract_id(
            "option:SPX:SPXW:20260715:7505:C", session_date=SESSION
        )
        == 7505
    )
    assert (
        spxw_call_strike_from_contract_id(
            "option:SPX:SPXW:20260715:7505:P", session_date=SESSION
        )
        is None
    )
    assert (
        spxw_call_strike_from_contract_id(
            "option:SPX:SPXW:20260716:7505:C", session_date=SESSION
        )
        is None
    )


def test_pending_demand_has_30_second_lease_capped_by_exit() -> None:
    pending = {
        **source_contract(),
        "event_id": "gth-dip:event-1",
        "session_date": SESSION,
        "provider": "schwab",
        "spread": {**spread(), "exit_at": (NOW + timedelta(seconds=12)).isoformat()},
    }
    selected, reason = select_gth_quote_demand(
        at=NOW,
        session_date=SESSION,
        provider="schwab",
        gth_state={"pending": pending},
        virtual_active=None,
    )
    assert reason == "selected"
    assert selected is not None
    assert selected.status == "pending"
    assert selected.valid_until == NOW + timedelta(seconds=12)


def test_confirmed_demand_renews_short_lease_capped_by_signal_expiry() -> None:
    valid_until = NOW + timedelta(minutes=10)
    selected, reason = select_gth_quote_demand(
        at=NOW,
        session_date=SESSION,
        provider="schwab",
        gth_state={
            "last_signal": {
                **source_contract(),
                "event_id": "gth-dip:confirmed",
                "provider": "schwab",
                "session_date": SESSION,
                "valid_until": valid_until.isoformat(),
                "spread": spread(),
            }
        },
        virtual_active=None,
    )
    assert reason == "selected"
    assert selected is not None
    assert selected.status == "confirmed"
    assert selected.valid_until == NOW + timedelta(seconds=30)


def test_active_exact_spread_has_priority_over_confirmed_signal() -> None:
    selected, reason = select_gth_quote_demand(
        at=NOW,
        session_date=SESSION,
        provider="schwab",
        gth_state={
            "last_signal": {
                "event_id": "signal",
                "provider": "schwab",
                "session_date": SESSION,
                "valid_until": (NOW + timedelta(minutes=10)).isoformat(),
                "spread": spread(),
            }
        },
        virtual_active={
            **source_contract(policy="virtual_strategy_lifecycle.v3+sha256:test"),
            "status": "active",
            "source_kind": "gth_dip_reclaim_call",
            "source_policy_version": "gth_dip_reclaim.v4+sha256:test",
            "session_id": SESSION,
            "position_type": "call_debit_spread",
            "source_signal_id": "active-signal",
            "long_contract_id": "option:SPX:SPXW:20260715:7510:C",
            "short_contract_id": "option:SPX:SPXW:20260715:7550:C",
            "time_stop_at": (NOW + timedelta(minutes=20)).isoformat(),
        },
    )
    assert reason == "selected"
    assert selected is not None
    assert selected.status == "active"
    assert [leg.strike for leg in selected.legs] == [7510, 7550]
    assert selected.valid_until == NOW + timedelta(seconds=30)


@pytest.mark.parametrize(
    ("right", "long_strike", "short_strike"),
    (("C", 7505, 7545), ("P", 7505, 7465)),
)
def test_active_manual_plan_pins_both_legs_when_es_sample_is_unavailable(
    right,
    long_strike,
    short_strike,
) -> None:
    candidate_id = "gth-level-manual:test"
    last_candidate = {
        "schema_version": 1,
        "status": "refresh_pending",
        "candidate_id": candidate_id,
        "policy_version": "gth_level_manual_candidate.v1+sha256:test",
        "expiry": "20260715",
        "valid_until": (NOW + timedelta(minutes=5)).isoformat(),
        "exit_at": (NOW + timedelta(minutes=15)).isoformat(),
        "long_contract_id": (
            f"option:SPX:SPXW:20260715:{long_strike}:{right}"
        ),
        "short_contract_id": (
            f"option:SPX:SPXW:20260715:{short_strike}:{right}"
        ),
        "invalidation_coordinate": {
            **source_contract()["coordinate"],
            "provider": "schwab",
        },
        "block_reasons": ["long_leg_transport_stale"],
        "automatic_ordering": False,
    }
    active_plan = {
        "candidate_id": candidate_id,
        "exit_at": (NOW + timedelta(minutes=15)).isoformat(),
        "long_contract_id": last_candidate["long_contract_id"],
        "short_contract_id": last_candidate["short_contract_id"],
    }

    selected, reason = select_gth_quote_demand(
        at=NOW,
        session_date=SESSION,
        provider="schwab",
        gth_state={},
        virtual_active=None,
        manual_candidate_state={
            "last_candidate": last_candidate,
            "active_manual_plan": active_plan,
        },
        forced_clear_reason="gth_sample_unavailable",
    )

    assert reason == "selected_unified_manual_candidate"
    assert selected is not None
    assert selected.status == "confirmed"
    assert selected.event_id == f"{candidate_id}:ready"
    assert [(leg.strike, leg.right) for leg in selected.legs] == [
        (long_strike, right),
        (short_strike, right),
    ]
    assert selected.valid_until == NOW + timedelta(seconds=30)


def test_cancelled_manual_candidate_keeps_exact_legs_pinned_until_planned_exit() -> None:
    candidate_id = "gth-level-manual:cancelled-but-at-risk"
    exit_at = NOW + timedelta(minutes=15)
    active_plan = {
        "candidate_id": candidate_id,
        "notification_event_id": f"{candidate_id}:ready",
        "policy_version": "gth_level_manual_candidate.v1+sha256:test",
        "expiry": "20260715",
        "exit_at": exit_at.isoformat(),
        "long_contract_id": "option:SPX:SPXW:20260715:7505:P",
        "short_contract_id": "option:SPX:SPXW:20260715:7465:P",
        "invalidation_coordinate": {
            **source_contract()["coordinate"],
            "provider": "ibkr",
        },
        "automatic_ordering": False,
    }

    selected, reason = select_gth_quote_demand(
        at=NOW,
        session_date=SESSION,
        provider="ibkr",
        gth_state={},
        virtual_active=None,
        manual_candidate_state={
            "last_candidate": {
                "candidate_id": candidate_id,
                "status": "blocked",
            },
            "active_manual_plan": None,
            "manual_plan_monitors": [
                {
                    "ready_event_id": f"{candidate_id}:ready",
                    "active_plan": active_plan,
                    "monitor_until": exit_at.isoformat(),
                }
            ],
        },
        forced_clear_reason="gth_sample_unavailable",
    )

    assert reason == "selected_unified_manual_candidate"
    assert selected is not None
    assert selected.event_id == f"{candidate_id}:ready"
    assert [(leg.strike, leg.right) for leg in selected.legs] == [
        (7505, "P"),
        (7465, "P"),
    ]
    assert selected.valid_until == NOW + timedelta(seconds=30)


def test_pending_terminal_receipt_check_renews_exact_leg_pin_until_resolved_or_exit() -> None:
    candidate_id = "gth-level-manual:receipt-unobservable"
    ready_event_id = f"{candidate_id}:ready"
    exit_at = NOW + timedelta(minutes=15)
    active_plan = {
        "candidate_id": candidate_id,
        "ready_event_id": ready_event_id,
        "policy_version": "gth_level_manual_candidate.v1+sha256:test",
        "expiry": "20260715",
        "exit_at": exit_at.isoformat(),
        "long_contract_id": "option:SPX:SPXW:20260715:7505:P",
        "short_contract_id": "option:SPX:SPXW:20260715:7465:P",
        "invalidation_coordinate": {
            **source_contract()["coordinate"],
            "provider": "ibkr",
        },
        "automatic_ordering": False,
    }
    check = {
        "causation_event_id": ready_event_id,
        "occurred_at": NOW.isoformat(),
        "check_until": (NOW + timedelta(minutes=10)).isoformat(),
        "recovery_until": (NOW + timedelta(days=1)).isoformat(),
        "receipt_lookup_status": "degraded_ledger_unavailable",
        "receipt_lookup_degraded": True,
        "receipt_lookup_error": "rust_delivery_receipt_query_failed",
        "active_plan": active_plan,
        "candidate": {"status": "blocked"},
    }
    pending_state = {
        "last_candidate": {"candidate_id": candidate_id, "status": "blocked"},
        "active_manual_plan": {},
        "manual_plan_monitors": [],
        "pending_terminal_receipt_checks": [check],
    }

    first, first_reason = select_gth_quote_demand(
        at=NOW,
        session_date=SESSION,
        provider="ibkr",
        gth_state={},
        virtual_active=None,
        manual_candidate_state=pending_state,
        forced_clear_reason="gth_sample_unavailable",
    )
    after_old_window, later_reason = select_gth_quote_demand(
        at=NOW + timedelta(minutes=11),
        session_date=SESSION,
        provider="ibkr",
        gth_state={},
        virtual_active=None,
        manual_candidate_state=pending_state,
    )
    resolved, resolved_reason = select_gth_quote_demand(
        at=NOW + timedelta(minutes=12),
        session_date=SESSION,
        provider="ibkr",
        gth_state={},
        virtual_active=None,
        manual_candidate_state={
            **pending_state,
            "pending_terminal_receipt_checks": [],
        },
    )
    at_exit, exit_reason = select_gth_quote_demand(
        at=exit_at,
        session_date=SESSION,
        provider="ibkr",
        gth_state={},
        virtual_active=None,
        manual_candidate_state=pending_state,
    )

    assert first_reason == later_reason == "selected_unified_manual_candidate"
    assert first is not None and after_old_window is not None
    assert first.status == after_old_window.status == "pending"
    assert first.demand_id == after_old_window.demand_id
    assert first.event_id == after_old_window.event_id == ready_event_id
    assert [(leg.strike, leg.right) for leg in first.legs] == [
        (7505, "P"),
        (7465, "P"),
    ]
    assert first.valid_until == NOW + timedelta(seconds=30)
    assert after_old_window.valid_until == NOW + timedelta(
        minutes=11,
        seconds=30,
    )
    assert resolved is None
    assert resolved_reason == "no_exact_leg_quote_demand"
    assert at_exit is None
    assert exit_reason == "no_exact_leg_quote_demand"


@pytest.mark.parametrize("status", ("structure_watch", "manual_ready"))
def test_transient_forced_clear_still_blocks_new_manual_entry(status: str) -> None:
    candidate_id = f"gth-level-manual:new-{status}"
    selected, reason = select_gth_quote_demand(
        at=NOW,
        session_date=SESSION,
        provider=None,
        gth_state={},
        virtual_active=None,
        manual_candidate_state={
            "last_candidate": {
                "status": status,
                "candidate_id": candidate_id,
                "policy_version": "gth_level_manual_candidate.v1+sha256:test",
                "expiry": "20260715",
                "valid_until": (NOW + timedelta(minutes=5)).isoformat(),
                "long_contract_id": "option:SPX:SPXW:20260715:7505:P",
                "short_contract_id": "option:SPX:SPXW:20260715:7465:P",
                "invalidation_coordinate": {
                    **source_contract()["coordinate"],
                    "provider": "ibkr",
                },
            },
            "active_manual_plan": {},
            "manual_plan_monitors": [],
            "pending_terminal_receipt_checks": [],
        },
        forced_clear_reason="gth_sample_unavailable",
    )

    assert selected is None
    assert reason == "gth_sample_unavailable"


def test_invalid_unified_manual_plan_does_not_fall_back_to_legacy_gth_dip() -> None:
    candidate_id = "gth-level-manual:invalid"
    last_candidate = {
        "status": "refresh_pending",
        "candidate_id": candidate_id,
        "policy_version": "gth_level_manual_candidate.v1+sha256:test",
        "expiry": "20260715",
        "exit_at": (NOW + timedelta(minutes=15)).isoformat(),
        "long_contract_id": "option:SPX:SPXW:20260715:7505:P",
        "short_contract_id": "option:SPX:SPXW:20260715:7545:C",
        "invalidation_coordinate": source_contract()["coordinate"],
    }
    legacy_signal = {
        **source_contract(),
        "event_id": "gth-dip:legacy",
        "provider": "schwab",
        "session_date": SESSION,
        "valid_until": (NOW + timedelta(minutes=10)).isoformat(),
        "spread": spread(),
    }

    selected, reason = select_gth_quote_demand(
        at=NOW,
        session_date=SESSION,
        provider="schwab",
        gth_state={"last_signal": legacy_signal},
        virtual_active=None,
        manual_candidate_state={
            "last_candidate": last_candidate,
            "active_manual_plan": {
                "candidate_id": candidate_id,
                "exit_at": last_candidate["exit_at"],
                "long_contract_id": last_candidate["long_contract_id"],
                "short_contract_id": last_candidate["short_contract_id"],
            },
        },
    )

    assert selected is None
    assert reason == "manual_candidate_exact_legs_invalid"


def test_unified_manual_plan_expiry_is_exclusive_and_does_not_fall_back() -> None:
    exit_at = NOW + timedelta(seconds=1)
    candidate_id = "gth-level-manual:expiring"
    last_candidate = {
        "status": "refresh_pending",
        "candidate_id": candidate_id,
        "policy_version": "gth_level_manual_candidate.v1+sha256:test",
        "expiry": "20260715",
        "exit_at": exit_at.isoformat(),
        "long_contract_id": "option:SPX:SPXW:20260715:7505:P",
        "short_contract_id": "option:SPX:SPXW:20260715:7465:P",
        "invalidation_coordinate": source_contract()["coordinate"],
    }
    state = {
        "last_candidate": last_candidate,
        "active_manual_plan": {
            "candidate_id": candidate_id,
            "exit_at": last_candidate["exit_at"],
            "long_contract_id": last_candidate["long_contract_id"],
            "short_contract_id": last_candidate["short_contract_id"],
        },
    }
    legacy_signal = {
        **source_contract(),
        "event_id": "gth-dip:legacy",
        "provider": "schwab",
        "session_date": SESSION,
        "valid_until": (NOW + timedelta(minutes=10)).isoformat(),
        "spread": spread(),
    }

    before, before_reason = select_gth_quote_demand(
        at=NOW,
        session_date=SESSION,
        provider="schwab",
        gth_state={"last_signal": legacy_signal},
        virtual_active=None,
        manual_candidate_state=state,
    )
    expired, expired_reason = select_gth_quote_demand(
        at=exit_at,
        session_date=SESSION,
        provider="schwab",
        gth_state={"last_signal": legacy_signal},
        virtual_active=None,
        manual_candidate_state=state,
    )

    assert before is not None
    assert before_reason == "selected_unified_manual_candidate"
    assert before.valid_until == exit_at
    assert expired is None
    assert expired_reason == "manual_candidate_quote_demand_expired"


@pytest.mark.parametrize(
    ("state", "provider", "forced_reason", "reason"),
    (
        (
            {
                "last_signal": {
                    **source_contract(),
                    "event_id": "signal",
                    "provider": "schwab",
                    "session_date": SESSION,
                    "valid_until": (NOW + timedelta(minutes=10)).isoformat(),
                    "spread": spread(),
                }
            },
            "ibkr",
            None,
            "no_exact_leg_quote_demand",
        ),
        (
            {"status": "suppressed_pre_event"},
            "schwab",
            None,
            "gth_entry_suppressed",
        ),
        (
            {"provider_changed": True, "pending": {"event_id": "new-provider"}},
            "ibkr",
            None,
            "gth_provider_switched",
        ),
        ({}, "schwab", None, "no_exact_leg_quote_demand"),
        ({}, None, "missing_es", "missing_es"),
    ),
)
def test_provider_reset_suppression_and_disappearance_clear_demand(
    state, provider, forced_reason, reason
) -> None:
    selected, actual_reason = select_gth_quote_demand(
        at=NOW,
        session_date=SESSION,
        provider=provider,
        gth_state=state,
        virtual_active=None,
        forced_clear_reason=forced_reason,
    )
    assert selected is None
    assert actual_reason == reason


def test_persistence_replaces_stale_demand_with_tombstone(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    initial = {
        "pending": {
            **source_contract(),
            "event_id": "gth-dip:event-1",
            "session_date": SESSION,
            "provider": "schwab",
            "spread": spread(),
        }
    }
    selected = _persist_gth_quote_demand(
        storage,
        at=NOW,
        session_date=SESSION,
        provider="schwab",
        gth_state=initial,
        virtual_active=None,
    )
    assert selected is not None

    cleared = _persist_gth_quote_demand(
        storage,
        at=NOW + timedelta(seconds=5),
        session_date=SESSION,
        provider="schwab",
        gth_state={},
        virtual_active=None,
    )
    assert cleared is None
    raw = read_json_object(quote_demand_path(tmp_path))
    assert raw["schema_version"] == QUOTE_DEMAND_SCHEMA_VERSION
    assert raw["kind"] == "ibkr_exact_leg_quote_demand_tombstone"
    assert raw["previous_demand_id"] == selected.demand_id

    tombstone_mtime = quote_demand_path(tmp_path).stat().st_mtime_ns
    _persist_gth_quote_demand(
        storage,
        at=NOW + timedelta(seconds=10),
        session_date=SESSION,
        provider="schwab",
        gth_state={},
        virtual_active=None,
    )
    assert quote_demand_path(tmp_path).stat().st_mtime_ns == tombstone_mtime


def test_shock_owner_keeps_active_manual_pin_when_sample_is_unavailable(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    candidate_id = "gth-level-manual:pinned"
    last_candidate = {
        "schema_version": 1,
        "status": "refresh_pending",
        "candidate_id": candidate_id,
        "policy_version": "gth_level_manual_candidate.v1+sha256:test",
        "expiry": "20260715",
        "exit_at": (NOW + timedelta(minutes=15)).isoformat(),
        "long_contract_id": "option:SPX:SPXW:20260715:7505:P",
        "short_contract_id": "option:SPX:SPXW:20260715:7465:P",
        "invalidation_coordinate": {
            **source_contract()["coordinate"],
            "provider": "schwab",
        },
        "automatic_ordering": False,
    }
    atomic_write_json_secure(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json",
        {
            "last_candidate": last_candidate,
            "active_manual_plan": {
                "candidate_id": candidate_id,
                "exit_at": last_candidate["exit_at"],
                "long_contract_id": last_candidate["long_contract_id"],
                "short_contract_id": last_candidate["short_contract_id"],
            },
        },
    )

    selected = _persist_gth_quote_demand(
        storage,
        at=NOW,
        session_date=SESSION,
        provider="schwab",
        gth_state={},
        virtual_active=None,
        forced_clear_reason="gth_sample_unavailable",
    )

    assert selected is not None
    assert selected.event_id == f"{candidate_id}:ready"
    assert [(leg.strike, leg.right) for leg in selected.legs] == [
        (7505, "P"),
        (7465, "P"),
    ]
    assert read_json_object(quote_demand_path(tmp_path))["demand_id"] == selected.demand_id
