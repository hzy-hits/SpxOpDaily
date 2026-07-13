"""Pure Schwab collection profile and cadence planning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil
from typing import Protocol

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.schwab.request_models import CollectionProfile, QuotaMode, SchwabLane


@dataclass(frozen=True)
class CadencePolicy:
    off_hours_quote_seconds: float = 15.0
    off_hours_front_chain_seconds: float = 60.0
    off_hours_next_chain_seconds: float = 300.0
    off_hours_confirmation_chain_seconds: float = 300.0
    gth_quote_seconds: float = 15.0
    gth_front_chain_seconds: float = 15.0
    gth_next_chain_seconds: float = 60.0
    gth_confirmation_chain_seconds: float = 300.0
    normal_quote_seconds: float = 2.0
    normal_front_chain_seconds: float = 3.0
    active_quote_seconds: float = 1.5
    active_front_chain_seconds: float = 2.5
    burst_quote_seconds: float = 1.5
    burst_front_chain_seconds: float = 2.0
    next_chain_seconds: float = 30.0
    spy_xsp_chain_seconds: float = 15.0
    qqq_iwm_chain_seconds: float = 30.0


class CadenceConfig(Protocol):
    off_hours_quote_seconds: float
    off_hours_front_chain_seconds: float
    off_hours_next_chain_seconds: float
    off_hours_confirmation_chain_seconds: float
    gth_quote_seconds: float
    gth_front_chain_seconds: float
    gth_next_chain_seconds: float
    gth_confirmation_chain_seconds: float
    normal_quote_seconds: float
    normal_front_chain_seconds: float
    active_quote_seconds: float
    active_front_chain_seconds: float
    burst_quote_seconds: float
    burst_front_chain_seconds: float
    next_chain_seconds: float
    spy_xsp_chain_seconds: float
    qqq_iwm_chain_seconds: float


def collection_profile(
    now: datetime,
    *,
    burst: bool = False,
    active_window_minutes: int = 30,
) -> CollectionProfile:
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
        return CollectionProfile.GTH
    if not DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        return CollectionProfile.OFF_HOURS
    if burst:
        return CollectionProfile.BURST
    current = now.astimezone(ET)
    session = DEFAULT_MARKET_CALENDAR.session(current.date())
    if session is None:
        return CollectionProfile.OFF_HOURS
    edge = timedelta(minutes=active_window_minutes)
    if current < session.open_at + edge or current >= session.close_at - edge:
        return CollectionProfile.ACTIVE
    return CollectionProfile.NORMAL


def cadence_seconds(
    lane: SchwabLane,
    *,
    profile: CollectionProfile,
    policy: CadenceConfig,
    underlier: str | None = None,
) -> float:
    if lane is SchwabLane.HOT_AND_CONTEXT_QUOTES:
        return {
            CollectionProfile.OFF_HOURS: policy.off_hours_quote_seconds,
            CollectionProfile.GTH: policy.gth_quote_seconds,
            CollectionProfile.NORMAL: policy.normal_quote_seconds,
            CollectionProfile.ACTIVE: policy.active_quote_seconds,
            CollectionProfile.BURST: policy.burst_quote_seconds,
        }[profile]
    if lane is SchwabLane.FRONT_CHAIN and underlier == "SPX":
        return {
            CollectionProfile.OFF_HOURS: policy.off_hours_front_chain_seconds,
            CollectionProfile.GTH: policy.gth_front_chain_seconds,
            CollectionProfile.NORMAL: policy.normal_front_chain_seconds,
            CollectionProfile.ACTIVE: policy.active_front_chain_seconds,
            CollectionProfile.BURST: policy.burst_front_chain_seconds,
        }[profile]
    if lane is SchwabLane.NEXT_CHAIN:
        if profile is CollectionProfile.GTH:
            return policy.gth_next_chain_seconds
        return (
            policy.off_hours_next_chain_seconds
            if profile is CollectionProfile.OFF_HOURS
            else policy.next_chain_seconds
        )
    if underlier in {"SPY", "XSP"}:
        if profile is CollectionProfile.GTH:
            return policy.gth_confirmation_chain_seconds
        return (
            policy.off_hours_confirmation_chain_seconds
            if profile is CollectionProfile.OFF_HOURS
            else policy.spy_xsp_chain_seconds
        )
    if underlier in {"QQQ", "IWM"}:
        if profile is CollectionProfile.GTH:
            return policy.gth_confirmation_chain_seconds
        return (
            policy.off_hours_confirmation_chain_seconds
            if profile is CollectionProfile.OFF_HOURS
            else policy.qqq_iwm_chain_seconds
        )
    if profile is CollectionProfile.GTH:
        return policy.gth_confirmation_chain_seconds
    return (
        policy.off_hours_confirmation_chain_seconds
        if profile is CollectionProfile.OFF_HOURS
        else policy.qqq_iwm_chain_seconds
    )


def request_is_due(last_fetched_at: datetime | None, *, now: datetime, cadence: float) -> bool:
    return last_fetched_at is None or (now - last_fetched_at).total_seconds() >= cadence


def planner_tick_seconds(profile: CollectionProfile, policy: CadenceConfig) -> float:
    """Poll each due lane at least three times inside its shortest cadence."""

    shortest_cadence = min(
        cadence_seconds(
            SchwabLane.HOT_AND_CONTEXT_QUOTES,
            profile=profile,
            policy=policy,
        ),
        cadence_seconds(
            SchwabLane.FRONT_CHAIN,
            profile=profile,
            policy=policy,
            underlier="SPX",
        ),
        cadence_seconds(
            SchwabLane.NEXT_CHAIN,
            profile=profile,
            policy=policy,
            underlier="SPX",
        ),
        cadence_seconds(
            SchwabLane.CONFIRMATION_CHAIN,
            profile=profile,
            policy=policy,
            underlier="SPY",
        ),
    )
    return min(max(shortest_cadence / 3.0, 0.25), 5.0)


def planned_requests_per_minute(
    profile: CollectionProfile,
    policy: CadenceConfig,
) -> int:
    """Return the worst-case scheduled RPM implied by all configured lanes."""

    quote_rpm = ceil(
        60
        / cadence_seconds(
            SchwabLane.HOT_AND_CONTEXT_QUOTES,
            profile=profile,
            policy=policy,
        )
    )
    front_rpm = ceil(
        60
        / cadence_seconds(
            SchwabLane.FRONT_CHAIN,
            profile=profile,
            policy=policy,
            underlier="SPX",
        )
    )
    next_rpm = ceil(
        60
        / cadence_seconds(
            SchwabLane.NEXT_CHAIN,
            profile=profile,
            policy=policy,
            underlier="SPX",
        )
    )
    confirmation_rpm = (
        2
        * ceil(
            60
            / cadence_seconds(
                SchwabLane.CONFIRMATION_CHAIN,
                profile=profile,
                policy=policy,
                underlier="SPY",
            )
        )
        + 2
        * ceil(
            60
            / cadence_seconds(
                SchwabLane.CONFIRMATION_CHAIN,
                profile=profile,
                policy=policy,
                underlier="QQQ",
            )
        )
    )
    return quote_rpm + front_rpm + next_rpm + confirmation_rpm


def effective_profile(profile: CollectionProfile, quota_mode: QuotaMode) -> CollectionProfile:
    if quota_mode in {QuotaMode.THROTTLED, QuotaMode.COOLDOWN}:
        return CollectionProfile.OFF_HOURS
    if quota_mode is QuotaMode.PRESSURE and profile is CollectionProfile.BURST:
        return CollectionProfile.ACTIVE
    return profile
