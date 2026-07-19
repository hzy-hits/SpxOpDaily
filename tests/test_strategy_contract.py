from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.strategy_contract import (
    STRATEGY_EVENT_SCHEMA_VERSION,
    actionable_strategy_contract_issues,
    policy_version,
    strategy_contract_issues,
    strategy_event_fields,
)


NOW = datetime(2026, 7, 18, 3, 0, tzinfo=timezone.utc)


def _coordinate() -> dict[str, object]:
    return {
        "kind": "raw_es",
        "instrument_id": "future:ES",
        "observed_value": 6350.25,
        "target_value": 6344.0,
        "basis_points": 0.0,
        "as_of": NOW.isoformat(),
    }


def test_shared_strategy_fields_are_normalized_and_versioned() -> None:
    fields = strategy_event_fields(
        policy_version_value="gth.v3+sha256:test",
        valid_until=NOW + timedelta(seconds=90),
        coordinate=_coordinate(),
        block_reasons=("quote_stale", "quote_stale", ""),
    )

    assert fields["schema_version"] == STRATEGY_EVENT_SCHEMA_VERSION == 3
    assert fields["valid_until"] == (NOW + timedelta(seconds=90)).isoformat()
    assert fields["coordinate"]["kind"] == "raw_es"
    assert fields["block_reasons"] == ["quote_stale"]
    assert strategy_contract_issues(fields) == ()


@dataclass(frozen=True)
class _Policy:
    ttl: int
    mode: str


def test_policy_version_is_stable_and_changes_with_effective_policy() -> None:
    first = policy_version("example.v3", _Policy(ttl=90, mode="shadow"))
    same = policy_version("example.v3", {"mode": "shadow", "ttl": 90})
    changed = policy_version("example.v3", _Policy(ttl=91, mode="shadow"))

    assert first == same
    assert changed != first


@pytest.mark.parametrize(
    ("offset", "expected"),
    ((-timedelta(microseconds=1), ()), (timedelta(0), ("strategy_event_expired",))),
)
def test_validity_is_half_open(offset: timedelta, expected: tuple[str, ...]) -> None:
    fields = strategy_event_fields(
        policy_version_value="example.v3+sha256:test",
        valid_until=NOW,
        coordinate=_coordinate(),
    )

    assert actionable_strategy_contract_issues(fields, now=NOW + offset) == expected


def test_actionable_contract_rejects_coordinate_instrument_mismatch() -> None:
    fields = strategy_event_fields(
        policy_version_value="example.v3+sha256:test",
        valid_until=NOW + timedelta(seconds=1),
        coordinate={**_coordinate(), "instrument_id": "index:SPX"},
    )

    assert "coordinate_instrument_mismatch" in actionable_strategy_contract_issues(
        fields, now=NOW
    )


def test_naive_validity_is_rejected_at_write_boundary() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        strategy_event_fields(
            policy_version_value="example.v3+sha256:test",
            valid_until=NOW.replace(tzinfo=None),
            coordinate=_coordinate(),
        )
