"""Versioned wire contract for temporary exact-leg IBKR quote demand."""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spx_spark.marketdata import InstrumentId
from spx_spark.sampling import OptionContractSpec
from spx_spark.state_io import atomic_write_json_secure, read_json_object
from spx_spark.strategy_contract import (
    normalize_block_reasons,
    normalize_coordinate,
    policy_version as strategy_policy_version,
    strategy_contract_issues,
)


QUOTE_DEMAND_V1_SCHEMA_VERSION = 1
QUOTE_DEMAND_SCHEMA_VERSION = 2
QUOTE_DEMAND_SUPPORTED_SCHEMA_VERSIONS = frozenset(
    {QUOTE_DEMAND_V1_SCHEMA_VERSION, QUOTE_DEMAND_SCHEMA_VERSION}
)
QUOTE_DEMAND_KIND = "ibkr_exact_leg_quote_demand"
QUOTE_DEMAND_TOMBSTONE_KIND = "ibkr_exact_leg_quote_demand_tombstone"
QUOTE_DEMAND_STATUSES = frozenset({"pending", "confirmed", "active"})
QUOTE_DEMAND_LEASE_SECONDS = 30
QUOTE_DEMAND_MAX_LEASE_SECONDS = 45
QUOTE_DEMAND_MAX_FUTURE_SKEW_SECONDS = 5
QUOTE_DEMAND_V1_POLICY_VERSION = strategy_policy_version(
    "ibkr_exact_leg_quote_demand.v1",
    {
        "schema_version": QUOTE_DEMAND_V1_SCHEMA_VERSION,
        "lease_seconds": QUOTE_DEMAND_LEASE_SECONDS,
        "max_lease_seconds": QUOTE_DEMAND_MAX_LEASE_SECONDS,
        "contract": "same-session SPXW call debit spread",
        "quote_provider": "ibkr",
        "automatic_ordering": False,
    },
)
QUOTE_DEMAND_POLICY_VERSION = strategy_policy_version(
    "ibkr_exact_leg_quote_demand.v2",
    {
        "schema_version": QUOTE_DEMAND_SCHEMA_VERSION,
        "lease_seconds": QUOTE_DEMAND_LEASE_SECONDS,
        "max_lease_seconds": QUOTE_DEMAND_MAX_LEASE_SECONDS,
        "contract": "same-session SPXW call-or-put debit spread",
        "quote_provider": "ibkr",
        "automatic_ordering": False,
    },
)
# Rolling-deploy invariant: restart the dual-read IBKR stream consumer before
# restarting the shock writer that emits v2. A pre-v2 reader rejects v2 rather
# than guessing a leg right, so reversing that order is safe but drops the pin.
_SESSION_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class ExactLegQuoteDemandLeg:
    """One exact SPXW leg, expressed independently of ``ib_async``."""

    role: str
    contract_id: str
    label: str
    expiry: str
    strike: int
    right: str = "C"
    trading_class: str = "SPXW"
    underlier: str = "SPX"
    exchange: str = "SMART"
    currency: str = "USD"
    multiplier: str = "100"

    def spec(self) -> OptionContractSpec:
        return OptionContractSpec(
            expiry=self.expiry.replace("-", ""),
            strike=self.strike,
            right=self.right,
            lane="pinned",
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "role": self.role,
            "contract_id": self.contract_id,
            "label": self.label,
            "underlier": self.underlier,
            "trading_class": self.trading_class,
            "expiry": self.expiry,
            "strike": self.strike,
            "right": self.right,
            "exchange": self.exchange,
            "currency": self.currency,
            "multiplier": self.multiplier,
        }


@dataclass(frozen=True)
class ExactLegQuoteDemand:
    """A short-lived request for the IBKR owner to pin two exact option legs."""

    schema_version: int
    demand_id: str
    event_id: str
    status: str
    created_at: datetime
    updated_at: datetime
    valid_until: datetime
    session_date: str
    policy_version: str
    source_schema_version: int
    source_policy_version: str
    source_provider: str
    quote_provider: str
    coordinate: Mapping[str, object]
    block_reasons: tuple[str, ...]
    automatic_ordering: bool
    legs: tuple[ExactLegQuoteDemandLeg, ExactLegQuoteDemandLeg]

    def specs(self) -> tuple[OptionContractSpec, ...]:
        return tuple(leg.spec() for leg in self.legs)

    def to_dict(self) -> dict[str, object]:
        legs: list[dict[str, object]] = []
        for leg in self.legs:
            serialized = leg.to_dict()
            if self.schema_version == QUOTE_DEMAND_V1_SCHEMA_VERSION:
                # v1 is Call-only; the normalized model carries ``right=C``
                # but the legacy wire shape does not need that discriminator.
                serialized.pop("right", None)
            legs.append(serialized)
        return {
            "schema_version": self.schema_version,
            "kind": QUOTE_DEMAND_KIND,
            "demand_id": self.demand_id,
            "event_id": self.event_id,
            "status": self.status,
            "created_at": canonical_time(self.created_at),
            "updated_at": canonical_time(self.updated_at),
            "valid_until": canonical_time(self.valid_until),
            "session_date": self.session_date,
            "policy_version": self.policy_version,
            "source_schema_version": self.source_schema_version,
            "source_policy_version": self.source_policy_version,
            "source_provider": self.source_provider,
            "quote_provider": self.quote_provider,
            "coordinate": dict(self.coordinate),
            "block_reasons": list(self.block_reasons),
            "automatic_ordering": self.automatic_ordering,
            "legs": legs,
        }


def quote_demand_path(data_root: str | Path) -> Path:
    return Path(data_root) / "latest" / "ibkr_exact_leg_quote_demand.json"


def quote_demand_ack_path(data_root: str | Path) -> Path:
    return Path(data_root) / "latest" / "ibkr_exact_leg_quote_demand_ack.json"


def build_exact_leg_quote_demand(
    *,
    event_id: str,
    status: str,
    session_date: str,
    long_strike: object,
    short_strike: object,
    right: str = "C",
    created_at: datetime,
    updated_at: datetime,
    valid_until: datetime,
    source_schema_version: int,
    source_policy_version: str,
    source_provider: str,
    coordinate: Mapping[str, object],
    block_reasons: object = (),
) -> ExactLegQuoteDemand:
    """Build and validate one current-schema demand."""

    expiry = valid_session_date(session_date)
    long_leg = _build_leg("long", expiry, long_strike, right=right)
    short_leg = _build_leg("short", expiry, short_strike, right=right)
    clean_event_id = required_string(event_id, "event_id")
    clean_source_policy = required_string(
        source_policy_version, "source_policy_version"
    )
    clean_source_provider = required_string(source_provider, "source_provider")
    clean_coordinate = normalize_coordinate(coordinate)
    clean_block_reasons = tuple(normalize_block_reasons(block_reasons))
    token = "|".join(
        (
            clean_event_id,
            expiry,
            clean_source_policy,
            clean_source_provider,
            long_leg.contract_id,
            short_leg.contract_id,
        )
    )
    demand = ExactLegQuoteDemand(
        schema_version=QUOTE_DEMAND_SCHEMA_VERSION,
        demand_id="gth-exact:" + hashlib.sha256(token.encode()).hexdigest()[:24],
        event_id=clean_event_id,
        status=status,
        created_at=aware_utc(created_at, "created_at"),
        updated_at=aware_utc(updated_at, "updated_at"),
        valid_until=aware_utc(valid_until, "valid_until"),
        session_date=expiry,
        policy_version=QUOTE_DEMAND_POLICY_VERSION,
        source_schema_version=required_int(
            source_schema_version, "source_schema_version"
        ),
        source_policy_version=clean_source_policy,
        source_provider=clean_source_provider,
        quote_provider="ibkr",
        coordinate=clean_coordinate,
        block_reasons=clean_block_reasons,
        automatic_ordering=False,
        legs=(long_leg, short_leg),
    )
    issue = _demand_issue(demand)
    if issue is not None:
        raise ValueError(issue)
    return demand


def parse_exact_leg_quote_demand(
    payload: Mapping[str, object],
    *,
    now: datetime,
) -> tuple[ExactLegQuoteDemand | None, str | None]:
    """Parse v1/v2 demand without exceptions; ``valid_until`` is exclusive.

    v1 is a frozen Call-only wire contract. v2 adds a required per-leg right
    and is the only version emitted by current writers.
    """

    try:
        schema_version = payload.get("schema_version")
        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version not in QUOTE_DEMAND_SUPPORTED_SCHEMA_VERSIONS
        ):
            return None, "schema_version_mismatch"
        if payload.get("kind") == QUOTE_DEMAND_TOMBSTONE_KIND:
            return None, "tombstone"
        if payload.get("kind") != QUOTE_DEMAND_KIND:
            return None, "kind_mismatch"
        if set(payload) != {
            "schema_version",
            "kind",
            "demand_id",
            "event_id",
            "status",
            "created_at",
            "updated_at",
            "valid_until",
            "session_date",
            "policy_version",
            "source_schema_version",
            "source_policy_version",
            "source_provider",
            "quote_provider",
            "coordinate",
            "block_reasons",
            "automatic_ordering",
            "legs",
        }:
            return None, "fields_invalid"
        raw_legs = payload.get("legs")
        if not isinstance(raw_legs, list) or len(raw_legs) != 2:
            return None, "legs_invalid"
        legs = tuple(
            _parse_leg(row, schema_version=schema_version) for row in raw_legs
        )
        if len(legs) != 2:  # pragma: no cover - tuple shape is fixed above
            return None, "legs_invalid"
        demand = ExactLegQuoteDemand(
            schema_version=schema_version,
            demand_id=required_string(payload.get("demand_id"), "demand_id"),
            event_id=required_string(payload.get("event_id"), "event_id"),
            status=required_string(payload.get("status"), "status"),
            created_at=parse_time(payload.get("created_at"), "created_at"),
            updated_at=parse_time(payload.get("updated_at"), "updated_at"),
            valid_until=parse_time(payload.get("valid_until"), "valid_until"),
            session_date=valid_session_date(payload.get("session_date")),
            policy_version=required_string(
                payload.get("policy_version"), "policy_version"
            ),
            source_schema_version=required_int(
                payload.get("source_schema_version"), "source_schema_version"
            ),
            source_policy_version=required_string(
                payload.get("source_policy_version"), "source_policy_version"
            ),
            source_provider=required_string(
                payload.get("source_provider"), "source_provider"
            ),
            quote_provider=required_string(
                payload.get("quote_provider"), "quote_provider"
            ),
            coordinate=required_mapping(payload.get("coordinate"), "coordinate"),
            block_reasons=_required_block_reasons(payload.get("block_reasons")),
            automatic_ordering=_required_bool(
                payload.get("automatic_ordering"), "automatic_ordering"
            ),
            legs=(legs[0], legs[1]),
        )
        issue = _demand_issue(demand)
        if issue is not None:
            return None, issue
        current = aware_utc(now, "now")
        if demand.updated_at > current + timedelta(
            seconds=QUOTE_DEMAND_MAX_FUTURE_SKEW_SECONDS
        ):
            return None, "updated_at_in_future"
        if current >= demand.valid_until:
            return None, "expired"
        return demand, None
    except (TypeError, ValueError, OverflowError):
        return None, "malformed"


def load_exact_leg_quote_demand(
    path: Path,
    *,
    now: datetime,
) -> tuple[ExactLegQuoteDemand | None, str | None]:
    payload = read_json_object(path)
    if not payload:
        return None, "missing_or_invalid"
    return parse_exact_leg_quote_demand(payload, now=now)


def write_exact_leg_quote_demand(path: Path, demand: ExactLegQuoteDemand) -> None:
    if demand.schema_version != QUOTE_DEMAND_SCHEMA_VERSION:
        raise ValueError("writer_requires_current_schema_version")
    issue = _demand_issue(demand)
    if issue is not None:
        raise ValueError(issue)
    atomic_write_json_secure(path, demand.to_dict())


def write_quote_demand_tombstone(
    path: Path,
    *,
    at: datetime,
    reason: str,
    previous_demand_id: str | None = None,
    previous_event_id: str | None = None,
) -> None:
    now = aware_utc(at, "at")
    atomic_write_json_secure(
        path,
        {
            "schema_version": QUOTE_DEMAND_SCHEMA_VERSION,
            "kind": QUOTE_DEMAND_TOMBSTONE_KIND,
            "status": "cleared",
            "created_at": canonical_time(now),
            "updated_at": canonical_time(now),
            "valid_until": canonical_time(now),
            "quote_provider": "ibkr",
            "reason": required_string(reason, "reason"),
            "previous_demand_id": _optional_string(previous_demand_id),
            "previous_event_id": _optional_string(previous_event_id),
            "legs": [],
        },
    )


def write_quote_demand_ack(path: Path, payload: Mapping[str, object]) -> None:
    """Atomically persist the collector acknowledgement projection."""

    result = dict(payload)
    schema_version = result.setdefault("schema_version", QUOTE_DEMAND_SCHEMA_VERSION)
    if schema_version != QUOTE_DEMAND_SCHEMA_VERSION:
        raise ValueError("ack schema_version mismatch")
    result.setdefault("kind", "ibkr_exact_leg_quote_demand_ack")
    atomic_write_json_secure(path, result)


def spxw_call_strike_from_contract_id(
    contract_id: object,
    *,
    session_date: str,
) -> int | None:
    """Return the strike only for one exact same-session SPXW Call id."""

    parsed = spxw_leg_from_contract_id(contract_id, session_date=session_date)
    return parsed[0] if parsed is not None and parsed[1] == "C" else None


def spxw_leg_from_contract_id(
    contract_id: object,
    *,
    session_date: str,
) -> tuple[int, str] | None:
    """Return strike/right for one exact same-session SPXW option id."""

    try:
        expiry = valid_session_date(session_date)
        if not isinstance(contract_id, str):
            return None
        parts = contract_id.split(":")
        if len(parts) != 6 or parts[:3] != ["option", "SPX", "SPXW"]:
            return None
        right = parts[5]
        if parts[3] != expiry.replace("-", "") or right not in {"C", "P"}:
            return None
        strike = _valid_strike(float(parts[4]))
        expected = _build_leg("long", expiry, strike, right=right).contract_id
        return (strike, right) if contract_id == expected else None
    except (TypeError, ValueError, OverflowError):
        return None


def valid_session_date(value: object) -> str:
    if not isinstance(value, str) or not _SESSION_DATE_PATTERN.fullmatch(value):
        raise ValueError("session date must use YYYY-MM-DD")
    if date.fromisoformat(value).isoformat() != value:
        raise ValueError("invalid session date")
    return value


def required_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def required_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    return value


def required_mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return {str(key): item for key, item in value.items()}


def parse_time(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO timestamp") from exc
    return aware_utc(parsed, field)


def optional_time(value: object) -> datetime | None:
    try:
        if isinstance(value, datetime):
            return aware_utc(value, "timestamp")
        return parse_time(value, "timestamp")
    except (TypeError, ValueError):
        return None


def aware_utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def canonical_time(value: datetime) -> str:
    return aware_utc(value, "timestamp").isoformat()


def _build_leg(
    role: str,
    expiry: str,
    strike: object,
    *,
    right: str = "C",
) -> ExactLegQuoteDemandLeg:
    parsed_strike = _valid_strike(strike)
    parsed_right = _valid_right(right)
    spec = OptionContractSpec(
        expiry=expiry.replace("-", ""),
        strike=parsed_strike,
        right=parsed_right,
        lane="pinned",
    )
    contract_id = InstrumentId.option(
        "SPX",
        expiry=spec.expiry,
        strike=spec.strike,
        right=parsed_right,
        trading_class="SPXW",
    ).canonical_id
    return ExactLegQuoteDemandLeg(
        role=role,
        contract_id=contract_id,
        label=f"option:SPXW:{spec.expiry}:{spec.strike}:{spec.right}",
        expiry=expiry,
        strike=parsed_strike,
        right=parsed_right,
    )


def _parse_leg(
    value: object,
    *,
    schema_version: int,
) -> ExactLegQuoteDemandLeg:
    if not isinstance(value, Mapping):
        raise ValueError("leg must be an object")
    role = required_string(value.get("role"), "role")
    expiry = valid_session_date(value.get("expiry"))
    if schema_version == QUOTE_DEMAND_V1_SCHEMA_VERSION:
        if value.get("right", "C") != "C":
            raise ValueError("v1 leg right must be implicit Call")
        canonical = _build_leg(role, expiry, value.get("strike"), right="C")
        explicit = canonical.to_dict()
        implicit = {key: item for key, item in explicit.items() if key != "right"}
        expected = explicit if "right" in value else implicit
        if set(value) != set(expected) or any(
            value.get(key) != expected[key] for key in expected
        ):
            raise ValueError("leg contract fields mismatch")
        return canonical
    if schema_version != QUOTE_DEMAND_SCHEMA_VERSION:
        raise ValueError("leg schema version mismatch")
    canonical = _build_leg(
        role,
        expiry,
        value.get("strike"),
        right=required_string(value.get("right"), "right"),
    )
    expected = canonical.to_dict()
    if set(value) != set(expected) or any(
        value.get(key) != expected[key] for key in expected
    ):
        raise ValueError("leg contract fields mismatch")
    return canonical


def _demand_issue(demand: ExactLegQuoteDemand) -> str | None:
    if demand.status not in QUOTE_DEMAND_STATUSES:
        return "status_invalid"
    if demand.schema_version == QUOTE_DEMAND_V1_SCHEMA_VERSION:
        expected_policy_version = QUOTE_DEMAND_V1_POLICY_VERSION
    elif demand.schema_version == QUOTE_DEMAND_SCHEMA_VERSION:
        expected_policy_version = QUOTE_DEMAND_POLICY_VERSION
    else:
        return "schema_version_mismatch"
    if demand.policy_version != expected_policy_version:
        return "policy_version_mismatch"
    if demand.quote_provider != "ibkr":
        return "quote_provider_mismatch"
    source_contract = {
        "schema_version": demand.source_schema_version,
        "policy_version": demand.source_policy_version,
        "valid_until": demand.valid_until,
        "coordinate": demand.coordinate,
        "block_reasons": list(demand.block_reasons),
    }
    if strategy_contract_issues(
        source_contract,
        require_valid_until=True,
        require_actionable_coordinate=True,
    ):
        return "source_contract_invalid"
    if demand.coordinate.get("kind") != "raw_es":
        return "source_coordinate_invalid"
    if demand.status == "active":
        expected_source_prefixes = ("virtual_strategy_lifecycle.v3+sha256:",)
    elif demand.schema_version == QUOTE_DEMAND_V1_SCHEMA_VERSION:
        expected_source_prefixes = ("gth_dip_reclaim.v4+sha256:",)
    else:
        expected_source_prefixes = (
            "gth_dip_reclaim.v4+sha256:",
            "gth_level_manual_candidate.v1+sha256:",
        )
    if not demand.source_policy_version.startswith(expected_source_prefixes):
        return "source_policy_incompatible"
    if demand.coordinate.get("provider") != demand.source_provider:
        return "source_provider_mismatch"
    if demand.block_reasons:
        return "source_blocked"
    if demand.automatic_ordering is not False:
        return "automatic_ordering_enabled"
    try:
        session_date = valid_session_date(demand.session_date)
        created_at = aware_utc(demand.created_at, "created_at")
        updated_at = aware_utc(demand.updated_at, "updated_at")
        valid_until = aware_utc(demand.valid_until, "valid_until")
    except (TypeError, ValueError):
        return "time_or_session_invalid"
    if created_at > updated_at:
        return "created_after_updated"
    if updated_at >= valid_until:
        return "valid_until_not_after_updated"
    if (valid_until - updated_at).total_seconds() > QUOTE_DEMAND_MAX_LEASE_SECONDS:
        return "lease_too_long"
    if tuple(leg.role for leg in demand.legs) != ("long", "short"):
        return "leg_roles_invalid"
    if any(leg.expiry != session_date for leg in demand.legs):
        return "leg_expiry_mismatch"
    if any(
        _parse_leg(leg.to_dict(), schema_version=demand.schema_version) != leg
        for leg in demand.legs
    ):
        return "leg_contract_invalid"
    long_leg, short_leg = demand.legs
    if (
        demand.schema_version == QUOTE_DEMAND_V1_SCHEMA_VERSION
        and (long_leg.right != "C" or short_leg.right != "C")
    ):
        return "v1_leg_right_invalid"
    if long_leg.right != short_leg.right:
        return "leg_right_mismatch"
    if long_leg.contract_id == short_leg.contract_id or long_leg.strike == short_leg.strike:
        return "legs_not_distinct"
    if long_leg.right == "C" and long_leg.strike >= short_leg.strike:
        return "call_spread_order_invalid"
    if long_leg.right == "P" and long_leg.strike <= short_leg.strike:
        return "put_spread_order_invalid"
    token = "|".join(
        (
            demand.event_id,
            session_date,
            demand.source_policy_version,
            demand.source_provider,
            long_leg.contract_id,
            short_leg.contract_id,
        )
    )
    expected_demand_id = "gth-exact:" + hashlib.sha256(token.encode()).hexdigest()[:24]
    return None if demand.demand_id == expected_demand_id else "demand_id_mismatch"


def _valid_strike(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("strike must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0 or not parsed.is_integer():
        raise ValueError("strike must be a positive integer")
    strike = int(parsed)
    if strike % 5:
        raise ValueError("strike must use the SPXW five-point grid")
    return strike


def _valid_right(value: object) -> str:
    if value not in {"C", "P"}:
        raise ValueError("right must be C or P")
    return str(value)


def _required_bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def _required_block_reasons(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError("block_reasons must be a list")
    normalized = tuple(normalize_block_reasons(value))
    if list(normalized) != value:
        raise ValueError("block_reasons must be normalized")
    return normalized


def _optional_string(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


__all__ = [
    "QUOTE_DEMAND_KIND",
    "QUOTE_DEMAND_LEASE_SECONDS",
    "QUOTE_DEMAND_POLICY_VERSION",
    "QUOTE_DEMAND_SCHEMA_VERSION",
    "QUOTE_DEMAND_TOMBSTONE_KIND",
    "QUOTE_DEMAND_V1_POLICY_VERSION",
    "QUOTE_DEMAND_V1_SCHEMA_VERSION",
    "ExactLegQuoteDemand",
    "ExactLegQuoteDemandLeg",
    "aware_utc",
    "build_exact_leg_quote_demand",
    "load_exact_leg_quote_demand",
    "optional_time",
    "parse_exact_leg_quote_demand",
    "quote_demand_ack_path",
    "quote_demand_path",
    "required_int",
    "required_mapping",
    "required_string",
    "spxw_call_strike_from_contract_id",
    "spxw_leg_from_contract_id",
    "valid_session_date",
    "write_exact_leg_quote_demand",
    "write_quote_demand_ack",
    "write_quote_demand_tombstone",
]
