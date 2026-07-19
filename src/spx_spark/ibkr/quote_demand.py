"""Durable, fail-closed contract for temporary exact-leg IBKR quote demand."""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.marketdata import InstrumentId
from spx_spark.sampling import OptionContractSpec
from spx_spark.state_io import atomic_write_json_secure, read_json_object
from spx_spark.strategy_contract import (
    actionable_strategy_contract_issues,
    normalize_block_reasons,
    normalize_coordinate,
    policy_version as strategy_policy_version,
    strategy_contract_issues,
)


QUOTE_DEMAND_SCHEMA_VERSION = 1
QUOTE_DEMAND_KIND = "ibkr_exact_leg_quote_demand"
QUOTE_DEMAND_TOMBSTONE_KIND = "ibkr_exact_leg_quote_demand_tombstone"
QUOTE_DEMAND_STATUSES = frozenset({"pending", "confirmed", "active"})
QUOTE_DEMAND_LEASE_SECONDS = 30
QUOTE_DEMAND_MAX_LEASE_SECONDS = 45
QUOTE_DEMAND_MAX_FUTURE_SKEW_SECONDS = 5
QUOTE_DEMAND_POLICY_VERSION = strategy_policy_version(
    "ibkr_exact_leg_quote_demand.v1",
    {
        "schema_version": QUOTE_DEMAND_SCHEMA_VERSION,
        "lease_seconds": QUOTE_DEMAND_LEASE_SECONDS,
        "max_lease_seconds": QUOTE_DEMAND_MAX_LEASE_SECONDS,
        "contract": "same-session SPXW call debit spread",
        "quote_provider": "ibkr",
        "automatic_ordering": False,
    },
)
_SESSION_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class ExactLegQuoteDemandLeg:
    """One exact SPXW Call leg, expressed independently of ``ib_async``."""

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
        return {
            "schema_version": QUOTE_DEMAND_SCHEMA_VERSION,
            "kind": QUOTE_DEMAND_KIND,
            "demand_id": self.demand_id,
            "event_id": self.event_id,
            "status": self.status,
            "created_at": _canonical_time(self.created_at),
            "updated_at": _canonical_time(self.updated_at),
            "valid_until": _canonical_time(self.valid_until),
            "session_date": self.session_date,
            "policy_version": self.policy_version,
            "source_schema_version": self.source_schema_version,
            "source_policy_version": self.source_policy_version,
            "source_provider": self.source_provider,
            "quote_provider": self.quote_provider,
            "coordinate": dict(self.coordinate),
            "block_reasons": list(self.block_reasons),
            "automatic_ordering": self.automatic_ordering,
            "legs": [leg.to_dict() for leg in self.legs],
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
    created_at: datetime,
    updated_at: datetime,
    valid_until: datetime,
    source_schema_version: int,
    source_policy_version: str,
    source_provider: str,
    coordinate: Mapping[str, object],
    block_reasons: object = (),
) -> ExactLegQuoteDemand:
    """Build and validate one demand; invalid producer input raises ``ValueError``."""

    expiry = _valid_session_date(session_date)
    long_leg = _build_leg("long", expiry, long_strike)
    short_leg = _build_leg("short", expiry, short_strike)
    clean_event_id = _required_string(event_id, "event_id")
    clean_source_policy = _required_string(
        source_policy_version, "source_policy_version"
    )
    clean_source_provider = _required_string(source_provider, "source_provider")
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
        demand_id="gth-exact:" + hashlib.sha256(token.encode()).hexdigest()[:24],
        event_id=clean_event_id,
        status=status,
        created_at=_aware_utc(created_at, "created_at"),
        updated_at=_aware_utc(updated_at, "updated_at"),
        valid_until=_aware_utc(valid_until, "valid_until"),
        session_date=expiry,
        policy_version=QUOTE_DEMAND_POLICY_VERSION,
        source_schema_version=_required_int(
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
    """Parse an untrusted demand without exceptions; ``valid_until`` is exclusive."""

    try:
        if payload.get("schema_version") != QUOTE_DEMAND_SCHEMA_VERSION:
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
        legs = tuple(_parse_leg(row) for row in raw_legs)
        if len(legs) != 2:  # pragma: no cover - tuple shape is fixed above
            return None, "legs_invalid"
        demand = ExactLegQuoteDemand(
            demand_id=_required_string(payload.get("demand_id"), "demand_id"),
            event_id=_required_string(payload.get("event_id"), "event_id"),
            status=_required_string(payload.get("status"), "status"),
            created_at=_parse_time(payload.get("created_at"), "created_at"),
            updated_at=_parse_time(payload.get("updated_at"), "updated_at"),
            valid_until=_parse_time(payload.get("valid_until"), "valid_until"),
            session_date=_valid_session_date(payload.get("session_date")),
            policy_version=_required_string(
                payload.get("policy_version"), "policy_version"
            ),
            source_schema_version=_required_int(
                payload.get("source_schema_version"), "source_schema_version"
            ),
            source_policy_version=_required_string(
                payload.get("source_policy_version"), "source_policy_version"
            ),
            source_provider=_required_string(
                payload.get("source_provider"), "source_provider"
            ),
            quote_provider=_required_string(
                payload.get("quote_provider"), "quote_provider"
            ),
            coordinate=_required_mapping(payload.get("coordinate"), "coordinate"),
            block_reasons=_required_block_reasons(payload.get("block_reasons")),
            automatic_ordering=_required_bool(
                payload.get("automatic_ordering"), "automatic_ordering"
            ),
            legs=(legs[0], legs[1]),
        )
        issue = _demand_issue(demand)
        if issue is not None:
            return None, issue
        current = _aware_utc(now, "now")
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
    now = _aware_utc(at, "at")
    atomic_write_json_secure(
        path,
        {
            "schema_version": QUOTE_DEMAND_SCHEMA_VERSION,
            "kind": QUOTE_DEMAND_TOMBSTONE_KIND,
            "status": "cleared",
            "created_at": _canonical_time(now),
            "updated_at": _canonical_time(now),
            "valid_until": _canonical_time(now),
            "quote_provider": "ibkr",
            "reason": _required_string(reason, "reason"),
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

    try:
        expiry = _valid_session_date(session_date)
        if not isinstance(contract_id, str):
            return None
        parts = contract_id.split(":")
        if len(parts) != 6 or parts[:3] != ["option", "SPX", "SPXW"]:
            return None
        if parts[3] != expiry.replace("-", "") or parts[5] != "C":
            return None
        strike = _valid_strike(float(parts[4]))
        expected = _build_leg("long", expiry, strike).contract_id
        return strike if contract_id == expected else None
    except (TypeError, ValueError, OverflowError):
        return None


def select_gth_quote_demand(
    *,
    at: datetime,
    session_date: str,
    provider: str | None,
    gth_state: Mapping[str, object],
    virtual_active: Mapping[str, object] | None,
    forced_clear_reason: str | None = None,
) -> tuple[ExactLegQuoteDemand | None, str]:
    """Select active, confirmed, or pending demand in fail-closed priority order."""

    try:
        now = _aware_utc(at, "at")
        _valid_session_date(session_date)
    except (TypeError, ValueError):
        return None, "demand_clock_or_session_invalid"
    if virtual_active and virtual_active.get("position_type") == "call_debit_spread":
        source_valid_until = _optional_time(virtual_active.get("time_stop_at"))
        if source_valid_until is None or now >= source_valid_until:
            return None, "active_quote_demand_expired"
        return _demand_from_contracts(
            virtual_active,
            event_id=str(
                virtual_active.get("source_signal_id")
                or virtual_active.get("episode_id")
                or ""
            ),
            valid_until=min(
                now + timedelta(seconds=QUOTE_DEMAND_LEASE_SECONDS),
                source_valid_until,
            ),
            session_date=session_date,
            now=now,
        )
    if forced_clear_reason:
        return None, forced_clear_reason
    if gth_state.get("provider_changed") is True:
        return None, "gth_provider_switched"
    if gth_state.get("status") == "suppressed_pre_event":
        return None, "gth_entry_suppressed"
    last_signal = gth_state.get("last_signal")
    if isinstance(last_signal, Mapping):
        signal_until = _optional_time(last_signal.get("valid_until"))
        if (
            signal_until is not None
            and now < signal_until
            and last_signal.get("provider") == provider
            and last_signal.get("session_date") == session_date
        ):
            return _demand_from_spread(
                last_signal,
                status="confirmed",
                valid_until=min(
                    now + timedelta(seconds=QUOTE_DEMAND_LEASE_SECONDS),
                    signal_until,
                ),
                session_date=session_date,
                now=now,
            )
    pending = gth_state.get("pending")
    if (
        isinstance(pending, Mapping)
        and pending.get("provider") == provider
        and pending.get("event_id")
    ):
        spread = pending.get("spread")
        exit_at = (
            _optional_time(spread.get("exit_at")) if isinstance(spread, Mapping) else None
        )
        if exit_at is not None:
            return _demand_from_spread(
                pending,
                status="pending",
                valid_until=min(
                    now + timedelta(seconds=QUOTE_DEMAND_LEASE_SECONDS),
                    exit_at,
                ),
                session_date=session_date,
                now=now,
            )
    return None, "no_exact_leg_quote_demand"


def _demand_from_spread(
    source: Mapping[str, object],
    *,
    status: str,
    valid_until: datetime,
    session_date: str,
    now: datetime,
) -> tuple[ExactLegQuoteDemand | None, str]:
    source_issue = _source_demand_issue(
        source,
        status=status,
        session_date=session_date,
        now=now,
    )
    if source_issue is not None:
        return None, source_issue
    spread = source.get("spread")
    if (
        not isinstance(spread, Mapping)
        or spread.get("right") != "C"
        or spread.get("expiry_date") != session_date
    ):
        return None, f"{status}_spread_unavailable"
    return _safe_build_demand(
        event_id=str(source.get("event_id") or ""),
        status=status,
        long_strike=spread.get("long_strike"),
        short_strike=spread.get("short_strike"),
        session_date=session_date,
        now=now,
        valid_until=valid_until,
        source=source,
    )


def _demand_from_contracts(
    source: Mapping[str, object],
    *,
    event_id: str,
    valid_until: datetime,
    session_date: str,
    now: datetime,
) -> tuple[ExactLegQuoteDemand | None, str]:
    source_issue = _source_demand_issue(
        source,
        status="active",
        session_date=session_date,
        now=now,
    )
    if source_issue is not None:
        return None, source_issue
    return _safe_build_demand(
        event_id=event_id,
        status="active",
        long_strike=spxw_call_strike_from_contract_id(
            source.get("long_contract_id"), session_date=session_date
        ),
        short_strike=spxw_call_strike_from_contract_id(
            source.get("short_contract_id"), session_date=session_date
        ),
        session_date=session_date,
        now=now,
        valid_until=valid_until,
        source=source,
    )


def _safe_build_demand(
    *,
    event_id: str,
    status: str,
    long_strike: object,
    short_strike: object,
    session_date: str,
    now: datetime,
    valid_until: datetime,
    source: Mapping[str, object],
) -> tuple[ExactLegQuoteDemand | None, str]:
    try:
        coordinate = _required_mapping(source.get("coordinate"), "coordinate")
        source_provider = str(
            source.get("provider") or coordinate.get("provider") or ""
        )
        return (
            build_exact_leg_quote_demand(
                event_id=event_id,
                status=status,
                session_date=session_date,
                long_strike=long_strike,
                short_strike=short_strike,
                created_at=now,
                updated_at=now,
                valid_until=valid_until,
                source_schema_version=_required_int(
                    source.get("schema_version"), "source_schema_version"
                ),
                source_policy_version=_required_string(
                    source.get("policy_version"), "source_policy_version"
                ),
                source_provider=source_provider,
                coordinate=coordinate,
                block_reasons=source.get("block_reasons"),
            ),
            "selected",
        )
    except (TypeError, ValueError):
        return None, f"{status}_quote_demand_invalid"


def _source_demand_issue(
    source: Mapping[str, object],
    *,
    status: str,
    session_date: str,
    now: datetime,
) -> str | None:
    if status == "pending":
        issues = strategy_contract_issues(
            source,
            require_valid_until=False,
            require_actionable_coordinate=True,
        )
    else:
        issues = actionable_strategy_contract_issues(source, now=now)
    if issues:
        return f"{status}_source_contract_invalid"
    coordinate = source.get("coordinate")
    if not isinstance(coordinate, Mapping) or coordinate.get("kind") != "raw_es":
        return f"{status}_source_coordinate_invalid"
    if source.get("automatic_ordering") is not False:
        return f"{status}_automatic_ordering_invalid"
    if status == "active":
        if (
            source.get("status") != "active"
            or source.get("source_kind") != "gth_dip_reclaim_call"
            or source.get("session_id") != session_date
            or not str(source.get("policy_version") or "").startswith(
                "virtual_strategy_lifecycle.v3+sha256:"
            )
            or not str(source.get("source_policy_version") or "").startswith(
                "gth_dip_reclaim.v4+sha256:"
            )
        ):
            return "active_lifecycle_contract_invalid"
    elif (
        source.get("session_date") != session_date
        or not str(source.get("policy_version") or "").startswith(
            "gth_dip_reclaim.v4+sha256:"
        )
        or source.get("provider") != coordinate.get("provider")
    ):
        return f"{status}_source_policy_invalid"
    return None


def _build_leg(role: str, expiry: str, strike: object) -> ExactLegQuoteDemandLeg:
    parsed_strike = _valid_strike(strike)
    spec = OptionContractSpec(
        expiry=expiry.replace("-", ""),
        strike=parsed_strike,
        right="C",
        lane="pinned",
    )
    contract_id = InstrumentId.option(
        "SPX",
        expiry=spec.expiry,
        strike=spec.strike,
        right="C",
        trading_class="SPXW",
    ).canonical_id
    return ExactLegQuoteDemandLeg(
        role=role,
        contract_id=contract_id,
        label=f"option:SPXW:{spec.expiry}:{spec.strike}:{spec.right}",
        expiry=expiry,
        strike=parsed_strike,
    )


def _parse_leg(value: object) -> ExactLegQuoteDemandLeg:
    if not isinstance(value, Mapping):
        raise ValueError("leg must be an object")
    role = _required_string(value.get("role"), "role")
    expiry = _valid_session_date(value.get("expiry"))
    canonical = _build_leg(role, expiry, value.get("strike"))
    expected = canonical.to_dict()
    if set(value) != set(expected) or any(value.get(key) != expected[key] for key in expected):
        raise ValueError("leg contract fields mismatch")
    return canonical


def _demand_issue(demand: ExactLegQuoteDemand) -> str | None:
    if demand.status not in QUOTE_DEMAND_STATUSES:
        return "status_invalid"
    if demand.policy_version != QUOTE_DEMAND_POLICY_VERSION:
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
    expected_source_prefix = (
        "virtual_strategy_lifecycle.v3+sha256:"
        if demand.status == "active"
        else "gth_dip_reclaim.v4+sha256:"
    )
    if not demand.source_policy_version.startswith(expected_source_prefix):
        return "source_policy_incompatible"
    coordinate_provider = demand.coordinate.get("provider")
    if coordinate_provider != demand.source_provider:
        return "source_provider_mismatch"
    if demand.block_reasons:
        return "source_blocked"
    if demand.automatic_ordering is not False:
        return "automatic_ordering_enabled"
    try:
        session_date = _valid_session_date(demand.session_date)
        created_at = _aware_utc(demand.created_at, "created_at")
        updated_at = _aware_utc(demand.updated_at, "updated_at")
        valid_until = _aware_utc(demand.valid_until, "valid_until")
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
    if any(_parse_leg(leg.to_dict()) != leg for leg in demand.legs):
        return "leg_contract_invalid"
    long_leg, short_leg = demand.legs
    if long_leg.contract_id == short_leg.contract_id or long_leg.strike == short_leg.strike:
        return "legs_not_distinct"
    if long_leg.strike >= short_leg.strike:
        return "call_spread_order_invalid"
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
    if demand.demand_id != expected_demand_id:
        return "demand_id_mismatch"
    return None


def _valid_session_date(value: object) -> str:
    if not isinstance(value, str) or not _SESSION_DATE_PATTERN.fullmatch(value):
        raise ValueError("session date must use YYYY-MM-DD")
    if date.fromisoformat(value).isoformat() != value:
        raise ValueError("invalid session date")
    return value


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


def _required_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _required_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    return value


def _required_bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def _required_mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return {str(key): item for key, item in value.items()}


def _required_block_reasons(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError("block_reasons must be a list")
    normalized = tuple(normalize_block_reasons(value))
    if list(normalized) != value:
        raise ValueError("block_reasons must be normalized")
    return normalized


def _optional_string(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _parse_time(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO timestamp") from exc
    return _aware_utc(parsed, field)


def _optional_time(value: object) -> datetime | None:
    try:
        if isinstance(value, datetime):
            return _aware_utc(value, "timestamp")
        return _parse_time(value, "timestamp")
    except (TypeError, ValueError):
        return None


def _aware_utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _canonical_time(value: datetime) -> str:
    return _aware_utc(value, "timestamp").isoformat()
