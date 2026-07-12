"""Provider runtime transitions after flush / connect failures."""

from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.ibkr.farm_health import NON_DEGRADING_ERROR_CODES
from spx_spark.ibkr.stream.models import StreamAction
from spx_spark.ibkr.verifier import IbkrError
from spx_spark.marketdata import Provider, ProviderState, ProviderStatus


def decide_after_flush(
    *,
    connected: bool,
    allowed: bool,
    competing_session: bool,
    gateway_restart: bool = False,
) -> StreamAction:
    if competing_session:
        return StreamAction.CONFLICT_WAIT
    if gateway_restart:
        return StreamAction.GATEWAY_RESTART
    if not connected:
        return StreamAction.RECONNECT
    if not allowed:
        return StreamAction.POLICY_BLOCKED
    return StreamAction.CONTINUE


def provider_error_count(errors: list[IbkrError]) -> int:
    return sum(1 for error in errors if error.error_code not in NON_DEGRADING_ERROR_CODES)


def subscription_outage_reason(
    *,
    tws_connectivity_lost: bool,
    subscriptions_lost: bool,
) -> str | None:
    if subscriptions_lost:
        return "TWS restored without market-data subscriptions; rebuilding"
    if tws_connectivity_lost:
        return "TWS upstream connectivity lost; subscription lifecycle paused"
    return None


def classify_connect_failure(error: BaseException | str) -> str | None:
    text = str(error).lower()
    if "10182" in text:
        return "market_data_rerequest"
    if "326" in text or "client id" in text:
        return "client_id_conflict"
    return None


def connected_state() -> ProviderState:
    return ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.DEGRADED,
        checked_at=datetime.now(tz=timezone.utc),
        reason="connected; awaiting first flush",
        connected=True,
        authenticated=True,
        priority=0,
    )


def account_standby_state() -> ProviderState:
    return ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.DEGRADED,
        checked_at=datetime.now(tz=timezone.utc),
        reason="account standby connected; market data inactive",
        connected=True,
        authenticated=True,
        priority=0,
    )


def unavailable_state(reason: str, *, connected: bool = False) -> ProviderState:
    return ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=datetime.now(tz=timezone.utc),
        reason=reason,
        connected=connected,
        authenticated=True if connected else None,
        priority=0,
    )

