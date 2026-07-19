from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.application.shock.service import _persist_gth_quote_demand
from spx_spark.ibkr.quote_demand import (
    QUOTE_DEMAND_SCHEMA_VERSION,
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
from spx_spark.state_io import read_json_object


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
    loaded, issue = load_exact_leg_quote_demand(path, now=NOW)

    assert issue is None
    assert loaded == expected
    assert [leg.label for leg in loaded.legs] == [
        "option:SPXW:20260715:7505:C",
        "option:SPXW:20260715:7545:C",
    ]
    assert [(row.expiry, row.strike, row.right, row.lane) for row in loaded.specs()] == [
        ("20260715", 7505, "C", "pinned"),
        ("20260715", 7545, "C", "pinned"),
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
