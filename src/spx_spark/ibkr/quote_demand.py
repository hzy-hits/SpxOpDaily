"""Select durable exact-leg IBKR quote demand from production lifecycle state."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta

from spx_spark.ibkr.quote_demand_wire import (
    QUOTE_DEMAND_KIND,
    QUOTE_DEMAND_LEASE_SECONDS,
    QUOTE_DEMAND_MAX_FUTURE_SKEW_SECONDS,
    QUOTE_DEMAND_MAX_LEASE_SECONDS,
    QUOTE_DEMAND_POLICY_VERSION,
    QUOTE_DEMAND_SCHEMA_VERSION,
    QUOTE_DEMAND_STATUSES,
    QUOTE_DEMAND_SUPPORTED_SCHEMA_VERSIONS,
    QUOTE_DEMAND_TOMBSTONE_KIND,
    QUOTE_DEMAND_V1_POLICY_VERSION,
    QUOTE_DEMAND_V1_SCHEMA_VERSION,
    ExactLegQuoteDemand,
    ExactLegQuoteDemandLeg,
    aware_utc as _aware_utc,
    build_exact_leg_quote_demand,
    load_exact_leg_quote_demand,
    optional_time as _optional_time,
    parse_exact_leg_quote_demand,
    quote_demand_ack_path,
    quote_demand_path,
    required_int as _required_int,
    required_mapping as _required_mapping,
    required_string as _required_string,
    spxw_call_strike_from_contract_id,
    spxw_leg_from_contract_id,
    valid_session_date as _valid_session_date,
    write_exact_leg_quote_demand,
    write_quote_demand_ack,
    write_quote_demand_tombstone,
)
from spx_spark.strategy_contract import (
    STRATEGY_EVENT_SCHEMA_VERSION,
    actionable_strategy_contract_issues,
    strategy_contract_issues,
)


__all__ = [
    "QUOTE_DEMAND_KIND",
    "QUOTE_DEMAND_LEASE_SECONDS",
    "QUOTE_DEMAND_MAX_FUTURE_SKEW_SECONDS",
    "QUOTE_DEMAND_MAX_LEASE_SECONDS",
    "QUOTE_DEMAND_POLICY_VERSION",
    "QUOTE_DEMAND_SCHEMA_VERSION",
    "QUOTE_DEMAND_STATUSES",
    "QUOTE_DEMAND_SUPPORTED_SCHEMA_VERSIONS",
    "QUOTE_DEMAND_TOMBSTONE_KIND",
    "QUOTE_DEMAND_V1_POLICY_VERSION",
    "QUOTE_DEMAND_V1_SCHEMA_VERSION",
    "ExactLegQuoteDemand",
    "ExactLegQuoteDemandLeg",
    "build_exact_leg_quote_demand",
    "load_exact_leg_quote_demand",
    "parse_exact_leg_quote_demand",
    "quote_demand_ack_path",
    "quote_demand_path",
    "select_gth_quote_demand",
    "spxw_call_strike_from_contract_id",
    "spxw_leg_from_contract_id",
    "write_exact_leg_quote_demand",
    "write_quote_demand_ack",
    "write_quote_demand_tombstone",
]


def select_gth_quote_demand(
    *,
    at: datetime,
    session_date: str,
    provider: str | None,
    gth_state: Mapping[str, object],
    virtual_active: Mapping[str, object] | None,
    manual_candidate_state: Mapping[str, object] | None = None,
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
    manual_demand, manual_reason, manual_order_at_risk = _manual_candidate_demand(
        manual_candidate_state,
        session_date=session_date,
        now=now,
    )
    # A transient shock/ES failure blocks new entry demand, but it cannot
    # release exact legs for a card that may already be resting manually. Keep
    # active plans, delivered monitors and pending terminal-receipt checks
    # observable until their own exclusive lifecycle deadline.
    if manual_order_at_risk:
        return manual_demand, manual_reason
    if forced_clear_reason:
        return None, forced_clear_reason
    if manual_demand is not None:
        return manual_demand, manual_reason
    if manual_reason != "no_unified_manual_quote_demand":
        return None, manual_reason
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


def _manual_candidate_demand(
    state: Mapping[str, object] | None,
    *,
    session_date: str,
    now: datetime,
) -> tuple[ExactLegQuoteDemand | None, str, bool]:
    """Pin a delivered/manual-ready unified candidate through its planned exit.

    Notification acceptance is not a broker fill.  It does, however, create an
    order-at-risk observation window during which both exact legs must remain
    subscribed so a later CANCEL/EXIT can be priced and audited.
    """

    if not isinstance(state, Mapping):
        return None, "no_unified_manual_quote_demand", False
    last = state.get("last_candidate")
    last = dict(last) if isinstance(last, Mapping) else {}
    active = state.get("active_manual_plan")
    active = dict(active) if isinstance(active, Mapping) else {}
    monitors = [
        dict(item)
        for item in state.get("manual_plan_monitors") or []
        if isinstance(item, Mapping)
    ]
    terminal_checks = [
        dict(item)
        for item in state.get("pending_terminal_receipt_checks") or []
        if isinstance(item, Mapping)
    ]
    source: dict[str, object] = {}
    order_at_risk = False
    status = "confirmed"
    source_until: datetime | None = None
    monitored_plan: dict[str, object] = {}
    for monitor in reversed(monitors):
        plan = monitor.get("active_plan")
        if not isinstance(plan, Mapping):
            continue
        monitor_until = _optional_time(monitor.get("monitor_until")) or _optional_time(
            plan.get("exit_at")
        )
        if monitor_until is not None and now < monitor_until:
            monitored_plan = dict(plan)
            source_until = monitor_until
            break
    if monitored_plan:
        order_at_risk = True
        candidate_id = str(monitored_plan.get("candidate_id") or "")
        if candidate_id and candidate_id == str(last.get("candidate_id") or ""):
            source = {**last, **monitored_plan}
        else:
            source = monitored_plan
    elif active:
        order_at_risk = True
        candidate_id = str(active.get("candidate_id") or "")
        if candidate_id and candidate_id == str(last.get("candidate_id") or ""):
            source = {**last, **active}
        else:
            source = active
        source_until = _optional_time(source.get("exit_at"))
    else:
        for check in reversed(terminal_checks):
            plan = check.get("active_plan")
            if not isinstance(plan, Mapping):
                continue
            exit_at = _optional_time(plan.get("exit_at"))
            if exit_at is None or now >= exit_at:
                continue
            terminal_candidate = check.get("candidate")
            terminal_candidate = (
                dict(terminal_candidate)
                if isinstance(terminal_candidate, Mapping)
                else {}
            )
            source = {**terminal_candidate, **dict(plan)}
            source_until = exit_at
            status = "pending"
            order_at_risk = True
            break
    if not source and last.get("status") in {"manual_ready", "structure_watch"}:
        source = last
        source_until = _optional_time(source.get("valid_until"))
        status = "pending" if last.get("status") == "structure_watch" else "confirmed"
    if not source:
        return None, "no_unified_manual_quote_demand", False
    if source_until is None:
        return None, "manual_candidate_quote_demand_expiry_invalid", order_at_risk
    if now >= source_until:
        return None, "manual_candidate_quote_demand_expired", order_at_risk
    expiry = str(source.get("expiry") or source.get("session_date") or "")
    if expiry == session_date.replace("-", ""):
        expiry = session_date
    if expiry != session_date:
        return None, "manual_candidate_session_mismatch", order_at_risk
    long_leg = spxw_leg_from_contract_id(
        source.get("long_contract_id"),
        session_date=session_date,
    )
    short_leg = spxw_leg_from_contract_id(
        source.get("short_contract_id"),
        session_date=session_date,
    )
    if long_leg is None or short_leg is None or long_leg[1] != short_leg[1]:
        return None, "manual_candidate_exact_legs_invalid", order_at_risk
    coordinate = source.get("invalidation_coordinate")
    if not isinstance(coordinate, Mapping) or coordinate.get("kind") != "raw_es":
        return None, "manual_candidate_raw_es_coordinate_invalid", order_at_risk
    source_policy = str(source.get("policy_version") or "")
    if not source_policy.startswith("gth_level_manual_candidate.v1+sha256:"):
        return None, "manual_candidate_policy_invalid", order_at_risk
    try:
        return (
            build_exact_leg_quote_demand(
                event_id=str(source.get("notification_event_id") or "")
                or str(source.get("ready_event_id") or "")
                or f"{source.get('candidate_id')}:ready",
                status=status,
                session_date=session_date,
                long_strike=long_leg[0],
                short_strike=short_leg[0],
                right=long_leg[1],
                created_at=now,
                updated_at=now,
                valid_until=min(
                    now + timedelta(seconds=QUOTE_DEMAND_LEASE_SECONDS),
                    source_until,
                ),
                source_schema_version=STRATEGY_EVENT_SCHEMA_VERSION,
                source_policy_version=source_policy,
                source_provider=str(coordinate.get("provider") or "unknown"),
                coordinate=coordinate,
                block_reasons=[],
            ),
            "selected_unified_manual_candidate",
            order_at_risk,
        )
    except (TypeError, ValueError):
        return None, "manual_candidate_quote_demand_invalid", order_at_risk


def _safe_build_demand(
    *,
    event_id: str,
    status: str,
    long_strike: object,
    short_strike: object,
    right: str = "C",
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
                right=right,
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
