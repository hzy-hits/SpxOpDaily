"""Persistent streaming IBKR collector.

The snapshot collector (`spx_spark.ibkr.collector`) connects, waits ~8 seconds,
snapshots, and disconnects once per service interval, which leaves most of each
minute blind. This module keeps one long-lived, market-data-only connection and
persistent subscriptions instead:

- base contracts (indexes/ETFs/futures/CFDs) stay subscribed permanently;
- SPXW options use the sampling planner: a hot lane near ATM stays subscribed,
  while the remaining line budget rotates through the plan's rolling groups;
- ticker state is flushed to raw storage + latest state every few seconds;
- the ATM window is re-planned when SPX drifts;
- disconnects trigger exponential-backoff reconnects, a competing session
  (IBKR 10197) triggers a non-invasive probe wait, and runtime policy is
  re-checked periodically so `protected` mode still wins.

All connection handling stays read-only and market-data-only, same as the
snapshot collector.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar

from spx_spark.config import (
    IbkrSettings,
    IbkrStreamSettings,
    RuntimePolicySettings,
    SamplingSettings,
    StorageSettings,
    default_spxw_expiry,
)
from spx_spark.ibkr.adapter import snapshot_from_rows
from spx_spark.ibkr.collector import has_competing_session_error
from spx_spark.ibkr.farm_health import (
    FarmHealthTracker,
    NON_DEGRADING_ERROR_CODES,
    probe_data_plane,
    request_gateway_restart,
    runtime_blocks_gateway_restart,
)
from spx_spark.ibkr.gateway import api_port_open
from spx_spark.ibkr.atm_reference import (
    AtmReferenceController,
    ReferenceQuote,
)
from spx_spark.ibkr.option_replan import OptionReplanController
from spx_spark.ibkr.slow_poll import SlowPollAction, SlowPollScheduler
from spx_spark.ibkr.verifier import (
    IbkrError,
    VerifyRow,
    apply_known_index_conid,
    build_base_contracts,
    cancel_subscriptions,
    connect_market_data_only,
    contract_has_con_id,
    first_present,
    midpoint,
    prepare_ib_client,
    qualify_and_subscribe,
    snapshot_rows,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET, MarketCalendar
from spx_spark.marketdata import Provider, ProviderState, ProviderStatus, parse_timestamp
from spx_spark.provider_adapter import ProviderSnapshot, persist_provider_snapshot
from spx_spark.storage import LatestStateStore
from spx_spark.runtime_mode import ibkr_allowed, load_override
from spx_spark.sampling import OptionContractSpec, build_sampling_plan


T = TypeVar("T")

MAX_TRACKED_ERRORS = 200
SUBSCRIPTION_CONFIRM_SECONDS = 0.5
SUBSCRIPTION_REJECTION_CODES = frozenset({100, 101, 354, 420})
OPTION_ROTATION_RETRY_SECONDS = 30.0
QUALIFICATION_TIMEOUT_SECONDS = 5.0
HOT_FLUSH_LIFECYCLE_BUDGET_SECONDS = 6.0
HOT_FLUSH_SLEEP_MAX_SECONDS = 5.0


class StreamAction(str, Enum):
    CONTINUE = "continue"
    RECONNECT = "reconnect"
    CONFLICT_WAIT = "conflict_wait"
    POLICY_BLOCKED = "policy_blocked"
    GATEWAY_RESTART = "gateway_restart"


@dataclass
class ReconnectPolicy:
    min_seconds: float
    max_seconds: float
    attempt: int = 0

    def next_delay(self) -> float:
        delay = min(self.min_seconds * (2**self.attempt), self.max_seconds)
        self.attempt += 1
        return delay

    def reset(self) -> None:
        self.attempt = 0


def lifecycle_has_qualification_budget(
    started_at: float,
    *,
    now_monotonic: float | None = None,
) -> bool:
    """Keep lifecycle work bounded so persisted hot rows stay <=12s apart."""

    now = time.monotonic() if now_monotonic is None else now_monotonic
    remaining = HOT_FLUSH_LIFECYCLE_BUDGET_SECONDS - max(now - started_at, 0.0)
    return remaining >= QUALIFICATION_TIMEOUT_SECONDS + SUBSCRIPTION_CONFIRM_SECONDS


def effective_hot_flush_sleep_seconds(configured_seconds: float) -> float:
    """Honor faster flush settings while enforcing the reliability ceiling."""

    return min(max(float(configured_seconds), 0.0), HOT_FLUSH_SLEEP_MAX_SECONDS)


@dataclass(frozen=True)
class OptionSubscriptionPlan:
    """Line-budgeted view of a sampling plan.

    `hot` stays subscribed for the lifetime of the plan; `rotations` are
    swapped in one slice at a time, each slice fitting the leftover budget.
    """

    atm_strike: int
    expiry: str
    hot: tuple[OptionContractSpec, ...]
    rotations: tuple[tuple[OptionContractSpec, ...], ...]

    @property
    def rotation_count(self) -> int:
        return len(self.rotations)


def contract_pairs_by_atm_distance(
    specs: list[OptionContractSpec],
    atm_strike: int,
) -> list[OptionContractSpec]:
    """Order specs nearest-ATM first, keeping C/P pairs adjacent."""
    pairs: dict[tuple[str, int], list[OptionContractSpec]] = {}
    for spec in specs:
        pairs.setdefault((spec.expiry, spec.strike), []).append(spec)

    ordered: list[OptionContractSpec] = []
    for key in sorted(
        pairs,
        key=lambda item: (abs(item[1] - atm_strike), item[0], item[1]),
    ):
        ordered.extend(sorted(pairs[key], key=lambda spec: spec.right))
    return ordered


def build_option_subscription_plan(
    *,
    atm_reference: float,
    expiry: str,
    next_expiry: str | None,
    mode: str,
    sampling_settings: SamplingSettings,
    max_option_lines: int,
    hot_lane_share: float,
) -> OptionSubscriptionPlan:
    plan = build_sampling_plan(
        underlier_price=atm_reference,
        expiry=expiry,
        next_expiry=next_expiry,
        mode=mode,
        settings=sampling_settings,
    )
    total_budget = max(int(max_option_lines), 0)
    hot_budget = min(max(2, int(total_budget * hot_lane_share)), total_budget)
    hot_budget -= hot_budget % 2  # keep whole C/P pairs
    rotation_budget = max(total_budget - hot_budget, 0)
    rotation_budget -= rotation_budget % 2

    hot = tuple(contract_pairs_by_atm_distance(plan.hot_lane, plan.atm_strike)[:hot_budget])
    hot_keys = {(spec.expiry, spec.strike, spec.right) for spec in hot}

    rotations: list[tuple[OptionContractSpec, ...]] = []
    if rotation_budget >= 2:
        for group in plan.rolling_groups:
            remaining = [
                spec
                for spec in contract_pairs_by_atm_distance(group.contracts, plan.atm_strike)
                if (spec.expiry, spec.strike, spec.right) not in hot_keys
            ]
            for start in range(0, len(remaining), rotation_budget):
                chunk = tuple(remaining[start : start + rotation_budget])
                if chunk:
                    rotations.append(chunk)

    return OptionSubscriptionPlan(
        atm_strike=plan.atm_strike,
        expiry=expiry,
        hot=hot,
        rotations=tuple(rotations),
    )


def should_replan(
    plan: OptionSubscriptionPlan | None,
    atm_reference: float | None,
    *,
    replan_drift_points: float,
    today_expiry: str,
) -> bool:
    if atm_reference is None:
        return False
    if plan is None:
        return True
    if plan.expiry != today_expiry:
        return True
    return abs(atm_reference - plan.atm_strike) >= replan_drift_points


def option_spec_label(spec: OptionContractSpec) -> str:
    return f"option:SPXW:{spec.expiry}:{spec.strike}:{spec.right}"


def option_label_distance(label: str, atm_strike: int) -> float:
    try:
        strike = float(label.rsplit(":", 2)[-2])
    except (IndexError, ValueError):
        return float("inf")
    return abs(strike - atm_strike)


def option_contracts_from_specs(specs: tuple[OptionContractSpec, ...]) -> list[tuple[str, str, Any]]:
    from ib_async import Option

    contracts: list[tuple[str, str, Any]] = []
    for spec in specs:
        contracts.append(
            (
                option_spec_label(spec),
                "option",
                Option(
                    "SPX",
                    spec.expiry,
                    float(spec.strike),
                    spec.right,
                    "SMART",
                    multiplier="100",
                    currency="USD",
                    tradingClass="SPXW",
                ),
            )
        )
    return contracts


def estimate_spy_reference(rows: list[VerifyRow]) -> float | None:
    by_label = {row.label: row for row in rows}
    spy = by_label.get("stock:SPY")
    if spy:
        price = first_present(spy.market_price, spy.last, midpoint(spy.bid, spy.ask), spy.close)
        if price:
            return price
    return None


def reference_quote_from_row(
    row: VerifyRow | None,
    *,
    contract: str | None = None,
    as_of: datetime | None = None,
) -> ReferenceQuote | None:
    if row is None:
        return None
    # Source-time synchronization matters for ES/SPX basis; transport-time
    # last_update_at can make unrelated source ticks look simultaneous.
    observed_at = parse_timestamp(row.ticker_time)
    decision_at = as_of or datetime.now(tz=timezone.utc)
    observed_in_future = bool(
        observed_at is not None
        and (observed_at - decision_at.astimezone(timezone.utc)).total_seconds() > 5.0
    )
    if row.market_data_type in {3, 4}:
        freshness = "delayed"
    elif row.market_data_type == 2:
        freshness = "frozen"
    elif row.market_data_type == 1:
        if observed_in_future:
            freshness = "unknown"
        elif row.stale is False and observed_at is not None:
            freshness = "fresh"
        elif row.stale is True:
            freshness = "stale"
        else:
            freshness = "unknown"
    else:
        freshness = "unknown"
    live_value = first_present(row.last, midpoint(row.bid, row.ask))
    if freshness == "fresh" and live_value is None:
        freshness = "close_only"
    reference_value = (
        first_present(row.close)
        if freshness == "stale"
        else live_value if live_value is not None else first_present(row.close)
    )
    return ReferenceQuote(
        value=reference_value,
        observed_at=observed_at,
        freshness=freshness,
        contract=contract,
    )


def build_spy_option_strikes(spy_price: float, *, lines: int, step: int) -> list[int]:
    n_strikes = max(1, lines // 2)
    atm = round(spy_price / step) * step
    return [atm + step * i for i in range(-(n_strikes // 2), n_strikes - n_strikes // 2)]


def spy_option_spec_label(expiry: str, strike: int, right: str) -> str:
    return f"option:SPY:{expiry}:{strike}:{right}"


def spy_option_contracts(expiry: str, strikes: list[int]) -> list[tuple[str, str, Any]]:
    from ib_async import Option

    contracts: list[tuple[str, str, Any]] = []
    for strike in strikes:
        for right in ("C", "P"):
            contracts.append(
                (
                    spy_option_spec_label(expiry, strike, right),
                    "option",
                    Option(
                        "SPY",
                        expiry,
                        float(strike),
                        right,
                        "SMART",
                        multiplier="100",
                        currency="USD",
                        tradingClass="SPY",
                    ),
                )
            )
    return contracts


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


def persist_state_only(state: ProviderState, storage_settings: StorageSettings) -> None:
    persist_provider_snapshot(
        ProviderSnapshot.from_state(Provider.IBKR, state, received_at=state.checked_at),
        storage_settings,
    )
    if state.status == ProviderStatus.UNAVAILABLE:
        LatestStateStore(storage_settings).purge_provider_quotes(Provider.IBKR)


def sleep_until_reconnect(
    *,
    host: str,
    port: int,
    delay_seconds: float,
    poll_seconds: float = 5.0,
) -> None:
    deadline = time.monotonic() + max(delay_seconds, 0.0)
    # An already-open TCP port says nothing about an application-level IBKR
    # handshake, authentication, or client-id failure.  In that case honor
    # the complete backoff.  A port that was initially down may still wake the
    # loop early when the gateway actually appears.
    port_was_open = api_port_open(host, port)
    if port_was_open:
        time.sleep(max(delay_seconds, 0.0))
        return
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(poll_seconds, remaining))
        if api_port_open(host, port):
            return


def log_event(event: dict[str, object]) -> None:
    event.setdefault("ts", datetime.now(tz=timezone.utc).isoformat())
    print(json.dumps(event, sort_keys=True), flush=True)


def split_base_contracts(
    contracts: list[tuple[str, str, Any]],
    slow_poll_labels: tuple[str, ...],
) -> tuple[list[tuple[str, str, Any]], list[tuple[str, str, Any]]]:
    """Split contracts into persistent vs slow-poll lanes by label."""
    slow_set = set(slow_poll_labels)
    persistent: list[tuple[str, str, Any]] = []
    slow: list[tuple[str, str, Any]] = []
    for contract in contracts:
        label = contract[0]
        if label in slow_set:
            slow.append(contract)
        else:
            persistent.append(contract)
    return persistent, slow


def chunked(items: list[T], size: int) -> list[list[T]]:
    chunk_size = size if size > 0 else 1
    return [items[index : index + chunk_size] for index in range(0, len(items), chunk_size)]


def contract_qualification_key(contract: Any) -> tuple[object, ...]:
    return tuple(
        getattr(contract, field, None)
        for field in (
            "secType",
            "symbol",
            "lastTradeDateOrContractMonth",
            "strike",
            "right",
        )
    )


def merge_slow_rows(
    rows: list[VerifyRow],
    slow_cache: dict[str, VerifyRow],
    subscribed_labels: set[str],
) -> list[VerifyRow]:
    rows.extend(row for label, row in slow_cache.items() if label not in subscribed_labels)
    return rows


OPTION_CACHE_TTL_SECONDS = 900.0


def update_option_cache(
    cache: dict[str, tuple[float, VerifyRow]],
    rows: list[VerifyRow],
    *,
    now_monotonic: float,
    expiry: str | None,
    active_expiries: frozenset[str] | None = None,
    ttl_seconds: float = OPTION_CACHE_TTL_SECONDS,
) -> None:
    """Remember the latest row per rotated option; evict expired/rolled rows."""
    for row in rows:
        if row.kind != "option" or not row.subscribed:
            continue
        cache[row.label] = (now_monotonic, row)
    allowed_expiries = active_expiries or (frozenset({expiry}) if expiry else frozenset())
    expired = [
        label
        for label, (cached_at, row) in cache.items()
        if now_monotonic - cached_at > ttl_seconds
        or (
            allowed_expiries
            and not any(f":{active_expiry}:" in label for active_expiry in allowed_expiries)
        )
    ]
    for label in expired:
        del cache[label]


def merge_cached_option_rows(
    rows: list[VerifyRow],
    cache: dict[str, tuple[float, VerifyRow]],
    subscribed_labels: set[str],
) -> list[VerifyRow]:
    rows.extend(row for label, (_, row) in cache.items() if label not in subscribed_labels)
    return rows


class StreamCollector:
    """Owns the long-lived IB connection and subscription lifecycle."""

    def __init__(
        self,
        ib: Any,
        *,
        ibkr_settings: IbkrSettings,
        stream_settings: IbkrStreamSettings,
        sampling_settings: SamplingSettings,
        storage_settings: StorageSettings,
        runtime_policy: RuntimePolicySettings,
        force: bool = False,
        skip_options: bool = False,
    ) -> None:
        self.ib = ib
        self.ibkr_settings = ibkr_settings
        self.stream_settings = stream_settings
        self.sampling_settings = sampling_settings
        self.storage_settings = storage_settings
        self.runtime_policy = runtime_policy
        self.force = force
        self.skip_options = skip_options or stream_settings.skip_options

        self.base_subs: dict[str, tuple[Any, VerifyRow]] = {}
        self.hot_subs: dict[str, tuple[Any, VerifyRow]] = {}
        self.rotation_subs: dict[str, tuple[Any, VerifyRow]] = {}
        self.spy_subs: dict[str, tuple[Any, VerifyRow]] = {}
        self.spy_plan_key: tuple[str, int] | None = None
        self.spy_retry_at = 0.0
        self.option_plan: OptionSubscriptionPlan | None = None
        self.option_replan_controller = OptionReplanController(
            trigger_points=stream_settings.replan_drift_points,
            rearm_points=min(10.0, stream_settings.replan_drift_points / 2.0),
        )
        atm_state_path = stream_settings.atm_state_path or str(
            Path(storage_settings.data_root) / "state" / "ibkr_atm_reference.json"
        )
        self.atm_reference_controller = AtmReferenceController(Path(atm_state_path))
        self.market_calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR
        self.rotation_index = 0
        self.rotation_retry_at = 0.0
        self.errors: list[IbkrError] = []
        self.subscription_rejection_sequence = 0
        self.subscription_rejection_log: list[tuple[int, IbkrError]] = []
        self.subscription_rows_by_req_id: dict[int, VerifyRow] = {}
        self.subscription_lane_by_req_id: dict[int, str] = {}
        self.subscription_health_failed = False
        self.last_policy_check = 0.0
        self.farm_health = FarmHealthTracker(
            broken_restart_seconds=stream_settings.farm_broken_restart_seconds,
        )
        self.slow_cache: dict[str, VerifyRow] = {}
        self.slow_contracts: list[tuple[str, str, Any]] = []
        self.slow_chunks: list[list[tuple[str, str, Any]]] = []
        self.slow_scheduler: SlowPollScheduler | None = None
        self.slow_active_subs: dict[str, tuple[Any, VerifyRow]] = {}
        self.slow_qualified_contracts: dict[str, tuple[str, str, Any]] = {}
        self.slow_unresolved_contracts: set[str] = set()
        # Rotated option rows linger here between their subscription windows so
        # every flush carries the full chain, not just the current slice.
        # Without this, walls/GEX are computed on a shifting 1/N subset of
        # strikes and jump from push to push.
        self.option_cache: dict[str, tuple[float, VerifyRow]] = {}
        self.qualified_option_contracts: dict[str, tuple[str, str, Any]] = {}

        ib.errorEvent += self._on_error

    def _on_error(self, req_id: int, error_code: int, message: str, contract: Any) -> None:
        error = IbkrError(
            req_id=req_id,
            error_code=error_code,
            message=message,
            contract=str(contract) if contract is not None else None,
            ts=datetime.now(tz=timezone.utc).isoformat(),
        )
        self.errors.append(error)
        del self.errors[:-MAX_TRACKED_ERRORS]

        if error_code in SUBSCRIPTION_REJECTION_CODES:
            self.subscription_rejection_sequence += 1
            self.subscription_rejection_log.append(
                (self.subscription_rejection_sequence, error)
            )
            del self.subscription_rejection_log[:-MAX_TRACKED_ERRORS]
            row = self.subscription_rows_by_req_id.get(req_id)
            if row is not None:
                row.error = f"IBKR {error_code}: {message}"
                row.subscribed = False
                if getattr(self, "subscription_lane_by_req_id", {}).get(req_id) in {
                    "base",
                    "hot",
                }:
                    self.subscription_health_failed = True
            elif req_id < 0:
                self.subscription_health_failed = True

        event = self.farm_health.observe(error_code, message)
        if event is not None:
            log_event(event.to_log_event(task="ibkr_stream"))

    def allowed(self) -> bool:
        if self.force:
            return True
        override = load_override(self.runtime_policy.runtime_mode_path)
        return ibkr_allowed(self.runtime_policy, override=override)

    def open_session(self) -> None:
        connect_market_data_only(
            self.ib,
            replace_client_id(self.ibkr_settings, self.stream_settings.client_id),
        )
        self.ib.reqMarketDataType(self.ibkr_settings.market_data_type)

    def subscribe_base(self) -> None:
        contracts = build_base_contracts(self.ibkr_settings)
        persistent, slow = split_base_contracts(
            contracts,
            self.stream_settings.slow_poll_labels,
        )
        self.slow_contracts = slow
        log_event(
            {
                "task": "ibkr_stream",
                "event": "subscribe_base_start",
                "contracts": len(contracts),
            }
        )

        def on_progress(**payload: object) -> None:
            log_event({"task": "ibkr_stream", "event": "subscribe_progress", **payload})

        rejection_sequence = self.subscription_rejection_sequence
        self.base_subs = qualify_and_subscribe(
            self.ib,
            persistent,
            qualify=self.ibkr_settings.qualify_contracts,
            on_progress=on_progress,
        )
        self._register_subscription_rows(
            {
                label: subscription
                for label, subscription in self.base_subs.items()
                if subscription[1].subscribed and not subscription[1].error
            },
            lane="base",
        )
        if self._apply_subscription_rejections(
            self.base_subs,
            rejection_sequence=rejection_sequence,
        ):
            self.subscription_health_failed = True
        subscribed = sum(1 for _, row in self.base_subs.values() if row.subscribed)
        failed = sum(1 for _, row in self.base_subs.values() if row.error)
        log_event(
            {
                "task": "ibkr_stream",
                "event": "subscribe_base_done",
                "subscribed": subscribed,
                "failed": failed,
                "total": len(contracts),
            }
        )
        if subscribed == 0:
            raise RuntimeError(f"no base contracts subscribed ({failed} failed)")
        self._qualify_slow_contracts()
        self.slow_chunks = chunked(
            self.slow_contracts,
            self.stream_settings.slow_poll_chunk_size,
        )
        self.slow_scheduler = SlowPollScheduler(
            chunk_count=len(self.slow_chunks),
            cycle_seconds=self.stream_settings.slow_poll_interval_seconds,
            hold_seconds=self.stream_settings.slow_poll_hold_seconds,
        )
        self.slow_scheduler.reset(now=time.monotonic())
        self.ib.sleep(self.ibkr_settings.quote_wait_seconds)

    def _qualify_slow_contracts(self) -> None:
        """Batch-resolve slow contracts once, outside the hot flush loop."""

        resolved_by_label: dict[str, tuple[str, str, Any]] = {}
        unresolved: list[tuple[str, str, Any]] = []
        for label, kind, contract in self.slow_contracts:
            known = contract if contract_has_con_id(contract) else apply_known_index_conid(contract)
            if known is not None:
                resolved_by_label[label] = (label, kind, known)
            else:
                unresolved.append((label, kind, contract))

        if unresolved:
            try:
                qualified = self._batch_qualify(
                    [contract for _, _, contract in unresolved]
                )
            except Exception as exc:  # noqa: BLE001
                qualified = []
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "slow_poll_batch_qualification_failed",
                        "contracts": len(unresolved),
                        "error": str(exc),
                    }
                )
            qualified_by_key: dict[tuple[object, ...], list[Any]] = {}
            for contract in qualified:
                qualified_by_key.setdefault(contract_qualification_key(contract), []).append(
                    contract
                )
            for label, kind, contract in unresolved:
                matches = qualified_by_key.get(contract_qualification_key(contract), [])
                if not matches:
                    log_event(
                        {
                            "task": "ibkr_stream",
                            "event": "slow_poll_qualification_failed",
                            "label": label,
                        }
                    )
                    continue
                resolved = matches.pop(0)
                resolved_by_label[label] = (label, kind, resolved)

        self.slow_qualified_contracts = resolved_by_label
        self.slow_unresolved_contracts = {
            label for label, _, _ in self.slow_contracts if label not in resolved_by_label
        }
        for label in self.slow_unresolved_contracts:
            kind = next(kind for item_label, kind, _ in self.slow_contracts if item_label == label)
            self.slow_cache[label] = VerifyRow(
                label=label,
                kind=kind,
                symbol=label.split(":", 1)[-1],
                subscribed=False,
                stale=True,
                error="slow contract qualification pending retry",
            )

    def _batch_qualify(self, contracts: list[Any]) -> list[Any]:
        if not contracts:
            return []
        qualify = getattr(self.ib, "qualifyContracts", None)
        if not callable(qualify):
            return []
        had_timeout = hasattr(self.ib, "RequestTimeout")
        previous_timeout = getattr(self.ib, "RequestTimeout", None)
        try:
            configured_timeout = (
                float(previous_timeout)
                if isinstance(previous_timeout, int | float) and previous_timeout > 0
                else QUALIFICATION_TIMEOUT_SECONDS
            )
            self.ib.RequestTimeout = min(
                configured_timeout,
                QUALIFICATION_TIMEOUT_SECONDS,
            )
            return list(qualify(*contracts))
        finally:
            if had_timeout:
                self.ib.RequestTimeout = previous_timeout
            else:
                delattr(self.ib, "RequestTimeout")

    def _resolve_slow_definitions(
        self,
        chunk: list[tuple[str, str, Any]],
    ) -> list[tuple[str, str, Any]]:
        resolved: dict[str, tuple[str, str, Any]] = {}
        pending: list[tuple[str, str, Any]] = []
        for label, kind, contract in chunk:
            cached = self.slow_qualified_contracts.get(label)
            if cached is not None:
                resolved[label] = cached
            else:
                pending.append((label, kind, contract))
        if pending:
            qualified = self._batch_qualify([contract for _, _, contract in pending])
            qualified_by_key: dict[tuple[object, ...], list[Any]] = {}
            for contract in qualified:
                qualified_by_key.setdefault(contract_qualification_key(contract), []).append(
                    contract
                )
            for label, kind, contract in pending:
                matches = qualified_by_key.get(contract_qualification_key(contract), [])
                if not matches:
                    continue
                definition = (label, kind, matches.pop(0))
                resolved[label] = definition
                self.slow_qualified_contracts[label] = definition
                self.slow_unresolved_contracts.discard(label)
        return [resolved[label] for label, _, _ in chunk if label in resolved]

    def advance_slow_poll(
        self,
        *,
        now_monotonic: float | None = None,
        allow_start: bool = True,
    ) -> None:
        scheduler = self.slow_scheduler
        if scheduler is None or not self.slow_chunks:
            return
        if scheduler.active_chunk_index is None and not allow_start:
            return
        use_live_clock = now_monotonic is None
        now_monotonic = now_monotonic if now_monotonic is not None else time.monotonic()
        step = scheduler.advance(now=now_monotonic)
        if step.action is SlowPollAction.NONE or step.chunk_index is None:
            return
        try:
            if step.action is SlowPollAction.START:
                chunk = self.slow_chunks[step.chunk_index]
                contracts = self._resolve_slow_definitions(chunk)
                unresolved_count = len(chunk) - len(contracts)
                if not contracts:
                    scheduler.abort_active(
                        now=now_monotonic,
                        retry_after_seconds=scheduler.spacing_seconds,
                        retry_same_chunk=False,
                    )
                    log_event(
                        {
                            "task": "ibkr_stream",
                            "event": "slow_poll_qualification_retry",
                            "chunk_index": step.chunk_index,
                            "resolved": 0,
                            "total": len(chunk),
                        }
                    )
                    return
                if unresolved_count:
                    log_event(
                        {
                            "task": "ibkr_stream",
                            "event": "slow_poll_partial_qualification",
                            "chunk_index": step.chunk_index,
                            "resolved": len(contracts),
                            "unresolved": unresolved_count,
                        }
                    )
                rejection_sequence = self.subscription_rejection_sequence
                self.slow_active_subs = qualify_and_subscribe(
                    self.ib,
                    contracts,
                    qualify=False,
                )
                if not self._subscription_batch_succeeded(
                    self.slow_active_subs,
                    expected_count=len(contracts),
                    rejection_sequence=rejection_sequence,
                    confirm_seconds=0.0,
                    lane="slow",
                ):
                    self._cancel_batch(self.slow_active_subs)
                    self.slow_active_subs = {}
                    scheduler.next_chunk_index = step.chunk_index
                    scheduler.abort_active(
                        now=now_monotonic,
                        retry_after_seconds=scheduler.spacing_seconds,
                    )
                    log_event(
                        {
                            "task": "ibkr_stream",
                            "event": "slow_poll_subscription_retry",
                            "chunk_index": step.chunk_index,
                        }
                    )
                    return
                kinds = {label: kind for label, kind, _ in chunk}
                for label, (ticker, _row) in self.slow_active_subs.items():
                    resolved = getattr(ticker, "contract", None) if ticker is not None else None
                    if resolved is not None:
                        self.slow_qualified_contracts[label] = (
                            label,
                            kinds[label],
                            resolved,
                        )
                subscribed_at = time.monotonic() if use_live_clock else now_monotonic
                scheduler.hold_deadline = subscribed_at + max(
                    scheduler.hold_seconds,
                    0.0,
                )
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "slow_poll_start",
                        "chunk_index": step.chunk_index,
                        "contracts": len(chunk),
                    }
                )
                return

            if any(
                not row.subscribed or row.error
                for _, row in self.slow_active_subs.values()
            ):
                self._cancel_batch(self.slow_active_subs)
                self.slow_active_subs = {}
                scheduler.next_chunk_index = step.chunk_index
                scheduler.abort_active(
                    now=now_monotonic,
                    retry_after_seconds=scheduler.spacing_seconds,
                )
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "slow_poll_async_rejection",
                        "chunk_index": step.chunk_index,
                    }
                )
                return
            rows = snapshot_rows(
                self.slow_active_subs,
                self.ibkr_settings.stale_after_seconds,
                slow_index_stale_after_seconds=self.ibkr_settings.slow_index_stale_after_seconds,
                slow_index_labels=frozenset(self.stream_settings.slow_poll_labels)
                | self.ibkr_settings.slow_index_labels,
            )
            for row in rows:
                self.slow_cache[row.label] = row
            if not self._cancel_batch(self.slow_active_subs):
                scheduler.next_chunk_index = step.chunk_index
                scheduler.abort_active(
                    now=now_monotonic,
                    retry_after_seconds=scheduler.spacing_seconds,
                )
                return
            self.slow_active_subs = {}
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "slow_poll_chunk_done",
                    "chunk_index": step.chunk_index,
                    "rows": len(rows),
                }
            )
        except Exception as exc:  # noqa: BLE001
            self._cancel_batch(self.slow_active_subs)
            self.slow_active_subs = {}
            scheduler.next_chunk_index = step.chunk_index
            scheduler.abort_active(
                now=now_monotonic,
                retry_after_seconds=scheduler.spacing_seconds,
            )
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "slow_poll_chunk_error",
                    "chunk_index": step.chunk_index,
                    "error": str(exc),
                }
            )

    def slow_poll_start_due(self, *, now_monotonic: float | None = None) -> bool:
        """Return whether an idle slow lane is ready to start its next chunk."""

        scheduler = self.slow_scheduler
        if (
            scheduler is None
            or not self.slow_chunks
            or scheduler.active_chunk_index is not None
        ):
            return False
        now = time.monotonic() if now_monotonic is None else now_monotonic
        return scheduler.next_start_at is None or now >= scheduler.next_start_at

    def ensure_option_plan(self, rows: list[VerifyRow]) -> None:
        if self.skip_options:
            return
        decision_at = datetime.now(tz=timezone.utc)
        current_expiry, next_expiry = self.market_calendar.research_expiries(decision_at)
        today = current_expiry.strftime("%Y%m%d")
        next_expiry_text = next_expiry.strftime("%Y%m%d")
        by_label = {row.label: row for row in rows}
        es_ticker = self.base_subs.get("future:ES", (None, None))[0]
        es_contract = getattr(
            getattr(es_ticker, "contract", None),
            "lastTradeDateOrContractMonth",
            None,
        )
        basis_state = self.atm_reference_controller.basis_tracker.state
        trading_date = decision_at.astimezone(ET).date()
        basis_age = (
            self.market_calendar.trading_days_elapsed(
                basis_state.trading_date,
                trading_date,
            )
            if basis_state is not None
            else None
        )
        stable = self.atm_reference_controller.stable_atm
        expiry_rollover = bool(
            stable is not None
            and stable.expiry is not None
            and stable.expiry != today
        ) or bool(
            self.option_replan_controller.accepted_expiry is not None
            and self.option_replan_controller.accepted_expiry != today
        )
        atm_result = self.atm_reference_controller.resolve(
            strike_step=max(int(self.sampling_settings.strike_step), 1),
            is_rth=self.market_calendar.is_rth_open(decision_at),
            trading_date=trading_date,
            trading_days_since_basis=basis_age,
            spx=reference_quote_from_row(by_label.get("index:SPX"), as_of=decision_at),
            ibus500=reference_quote_from_row(
                by_label.get("cfd:IBUS500"), as_of=decision_at
            ),
            es=reference_quote_from_row(
                by_label.get("future:ES"),
                contract=str(es_contract) if es_contract else None,
                as_of=decision_at,
            ),
            spy=reference_quote_from_row(by_label.get("stock:SPY"), as_of=decision_at),
            expiry_rollover=expiry_rollover,
        )
        candidate = atm_result.candidate
        decision = self.option_replan_controller.observe(
            atm_strike=candidate.rounded_strike if candidate is not None else None,
            source=candidate.source if candidate is not None else None,
            observed_at=candidate.observed_at if candidate is not None else decision_at,
            expiry=today,
            decision_at=decision_at,
        )
        basis = atm_result.basis
        log_event(
            {
                "task": "ibkr_stream",
                "event": "option_replan_decision",
                "raw_atm": candidate.value if candidate is not None else None,
                "raw_strike": candidate.rounded_strike if candidate is not None else None,
                "raw_source": candidate.source if candidate is not None else None,
                "raw_observed_at": (
                    candidate.observed_at.isoformat() if candidate is not None else None
                ),
                "raw_freshness": candidate.freshness if candidate is not None else None,
                "accepted_atm": self.option_replan_controller.accepted_atm,
                "accepted_source": self.option_replan_controller.accepted_source,
                "accepted_expiry": self.option_replan_controller.accepted_expiry,
                "state": decision.state,
                "reason": decision.reason,
                "confirmations": decision.confirmation_count,
                "basis_value": basis.median if basis is not None else None,
                "basis_as_of": (
                    basis.observed_at.isoformat()
                    if basis is not None and basis.observed_at is not None
                    else None
                ),
                "basis_contract": basis.es_contract if basis is not None else None,
            }
        )
        proposal = decision.proposal
        if proposal is None:
            return

        plan = build_option_subscription_plan(
            atm_reference=float(proposal.atm_strike),
            expiry=proposal.expiry,
            next_expiry=next_expiry_text,
            mode=self.sampling_settings.default_mode,
            sampling_settings=self.sampling_settings,
            max_option_lines=self.stream_settings.max_option_lines,
            hot_lane_share=self.stream_settings.hot_lane_share,
        )
        success = self.reconcile_option_plan(plan)
        completed_at = datetime.now(tz=timezone.utc)
        self.option_replan_controller.record_result(
            proposal,
            success=success,
            applied_at=completed_at,
        )
        if not success:
            return
        if candidate is not None:
            self.atm_reference_controller.record_accepted(
                candidate,
                expiry=proposal.expiry,
            )
        log_event(
            {
                "task": "ibkr_stream",
                "event": "option_replan",
                "atm_strike": plan.atm_strike,
                "expiry": plan.expiry,
                "hot_contracts": len(plan.hot),
                "rotation_slices": plan.rotation_count,
                "reason": proposal.reason,
                "source": proposal.source,
                "confirmations": proposal.confirmation_count,
            }
        )

    def reconcile_option_plan(self, plan: OptionSubscriptionPlan) -> bool:
        desired_contracts = option_contracts_from_specs(plan.hot)
        desired_by_label = {label: (label, kind, contract) for label, kind, contract in desired_contracts}
        retained_labels = set(self.hot_subs) & set(desired_by_label)
        added_labels = set(desired_by_label) - retained_labels
        obsolete_labels = set(self.hot_subs) - retained_labels

        if not self._cancel_batch(self.rotation_subs):
            return False
        self.rotation_subs = {}
        max_lines = getattr(
            getattr(self, "stream_settings", None),
            "max_option_lines",
            len(self.hot_subs) + len(added_labels),
        )
        free_lines = max(int(max_lines) - len(self.hot_subs), 0)
        release_count = max(len(added_labels) - free_lines, 0)
        release_labels = set(
            sorted(
                obsolete_labels,
                key=lambda label: (-option_label_distance(label, plan.atm_strike), label),
            )[:release_count]
        )
        released_subs = {label: self.hot_subs[label] for label in release_labels}
        if released_subs and not self._cancel_batch(released_subs):
            return False
        remaining_hot = {
            label: subscription
            for label, subscription in self.hot_subs.items()
            if label not in release_labels
        }
        rejection_sequence = getattr(self, "subscription_rejection_sequence", 0)
        addition_definitions = self._resolve_option_definitions(
            [desired_by_label[label] for label in sorted(added_labels)]
        )
        additions = qualify_and_subscribe(
            self.ib,
            addition_definitions,
            qualify=False,
        )
        additions_ok = self._subscription_batch_succeeded(
            additions,
            expected_count=len(added_labels),
            rejection_sequence=rejection_sequence,
            lane="hot",
        )
        if not additions_ok:
            self._cancel_batch(additions)
            restored = self._restore_subscriptions(released_subs, lane="hot")
            self.hot_subs = {**remaining_hot, **restored}
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "option_replan_failed",
                    "retained": len(retained_labels),
                    "added": len(added_labels),
                    "removed": len(released_subs) - len(restored),
                    "restored": len(restored),
                }
            )
            return False

        obsolete_subs = {
            label: remaining_hot[label]
            for label in obsolete_labels - release_labels
        }
        if not self._cancel_batch(obsolete_subs):
            self._cancel_batch(additions)
            restored = self._restore_subscriptions(released_subs, lane="hot")
            self.hot_subs = {**remaining_hot, **restored}
            return False
        self.hot_subs = {
            **{
                label: remaining_hot[label]
                for label in retained_labels
            },
            **additions,
        }
        self.option_plan = plan
        self.rotation_index = 0
        return True

    def _resolve_option_definitions(
        self,
        definitions: list[tuple[str, str, Any]],
    ) -> list[tuple[str, str, Any]]:
        """Batch-qualify unseen options and reuse resolved contracts by label."""

        cache = getattr(self, "qualified_option_contracts", None)
        if cache is None:
            # Lightweight unit-test collectors built with object.__new__ do
            # not own a session cache; their mocked transport resolves rows.
            return definitions
        resolved: dict[str, tuple[str, str, Any]] = {}
        pending: list[tuple[str, str, Any]] = []
        for label, kind, contract in definitions:
            cached = cache.get(label)
            if cached is not None:
                resolved[label] = cached
            elif contract_has_con_id(contract):
                resolved[label] = (label, kind, contract)
                cache[label] = resolved[label]
            else:
                pending.append((label, kind, contract))

        qualify = getattr(self.ib, "qualifyContracts", None)
        if pending and callable(qualify):
            try:
                qualified = self._batch_qualify(
                    [contract for _, _, contract in pending]
                )
            except Exception as exc:  # noqa: BLE001
                qualified = []
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "option_batch_qualification_failed",
                        "contracts": len(pending),
                        "error": str(exc),
                    }
                )
            qualified_by_key: dict[tuple[object, ...], list[Any]] = {}
            for contract in qualified:
                qualified_by_key.setdefault(contract_qualification_key(contract), []).append(
                    contract
                )
            for label, kind, contract in pending:
                matches = qualified_by_key.get(contract_qualification_key(contract), [])
                if not matches:
                    continue
                definition = (label, kind, matches.pop(0))
                resolved[label] = definition
                cache[label] = definition
        elif pending:
            # Test doubles without an IB qualification surface still exercise
            # lifecycle logic through the mocked qualify_and_subscribe call.
            for item in pending:
                resolved[item[0]] = item

        return [resolved[label] for label, _, _ in definitions if label in resolved]

    def _subscription_batch_succeeded(
        self,
        subscriptions: dict[str, tuple[Any, VerifyRow]],
        *,
        expected_count: int,
        rejection_sequence: int,
        confirm_seconds: float = SUBSCRIPTION_CONFIRM_SECONDS,
        lane: str = "hot",
    ) -> bool:
        if len(subscriptions) != expected_count or any(
            not row.subscribed or row.error
            for _, row in subscriptions.values()
        ):
            return False
        if subscriptions and confirm_seconds > 0:
            sleep = getattr(self.ib, "sleep", None)
            if callable(sleep):
                sleep(confirm_seconds)
        if self._apply_subscription_rejections(
            subscriptions,
            rejection_sequence=rejection_sequence,
        ):
            return False
        self._register_subscription_rows(subscriptions, lane=lane)
        return True

    def _apply_subscription_rejections(
        self,
        subscriptions: dict[str, tuple[Any, VerifyRow]],
        *,
        rejection_sequence: int,
    ) -> bool:
        rows_by_request_id = {
            row.request_id: row
            for _, row in subscriptions.values()
            if row.request_id is not None
        }
        rejected = False
        for sequence, error in getattr(self, "subscription_rejection_log", []):
            if sequence <= rejection_sequence:
                continue
            row = rows_by_request_id.get(error.req_id)
            if row is None and error.req_id >= 0:
                continue
            rejected = True
            if row is not None:
                row.error = f"IBKR {error.error_code}: {error.message}"
                row.subscribed = False
        return rejected

    def _register_subscription_rows(
        self,
        subscriptions: dict[str, tuple[Any, VerifyRow]],
        *,
        lane: str,
    ) -> None:
        tracked = getattr(self, "subscription_rows_by_req_id", None)
        lanes = getattr(self, "subscription_lane_by_req_id", None)
        contract_cache = getattr(self, "qualified_option_contracts", None)
        if tracked is None:
            return
        for label, (ticker, row) in subscriptions.items():
            if row.request_id is not None:
                tracked[row.request_id] = row
                if lanes is not None:
                    lanes[row.request_id] = lane
            if contract_cache is not None and row.kind == "option":
                contract = getattr(ticker, "contract", None)
                if contract is not None:
                    contract_cache[label] = (label, row.kind, contract)

    def _cancel_batch(self, subscriptions: dict[str, tuple[Any, VerifyRow]]) -> bool:
        result = cancel_subscriptions(self.ib, subscriptions)
        if result is False:
            self.subscription_health_failed = True
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "subscription_cancel_failed",
                    "contracts": len(subscriptions),
                }
            )
            return False
        tracked = getattr(self, "subscription_rows_by_req_id", None)
        lanes = getattr(self, "subscription_lane_by_req_id", None)
        if tracked is not None:
            for _, row in subscriptions.values():
                if row.request_id is not None:
                    tracked.pop(row.request_id, None)
                    if lanes is not None:
                        lanes.pop(row.request_id, None)
        return True

    def _restore_subscriptions(
        self,
        released: dict[str, tuple[Any, VerifyRow]],
        *,
        lane: str,
    ) -> dict[str, tuple[Any, VerifyRow]]:
        definitions: list[tuple[str, str, Any]] = []
        for label, (ticker, row) in released.items():
            contract = getattr(ticker, "contract", None)
            if contract is not None:
                definitions.append((label, row.kind, contract))
        if not definitions:
            return {}
        rejection_sequence = getattr(self, "subscription_rejection_sequence", 0)
        restored = qualify_and_subscribe(self.ib, definitions, qualify=False)
        if not self._subscription_batch_succeeded(
            restored,
            expected_count=len(definitions),
            rejection_sequence=rejection_sequence,
            lane=lane,
        ):
            self._cancel_batch(restored)
            self.subscription_health_failed = True
            return {}
        return {
            label: subscription
            for label, subscription in restored.items()
            if subscription[1].subscribed and not subscription[1].error
        }

    def ensure_spy_option_plan(self, rows: list[VerifyRow], *, expiry: str) -> None:
        if self.skip_options or self.stream_settings.spy_option_lines < 2:
            return
        unhealthy_spy = {
            label: subscription
            for label, subscription in self.spy_subs.items()
            if not subscription[1].subscribed or subscription[1].error
        }
        if unhealthy_spy:
            self._cancel_batch(unhealthy_spy)
            self.spy_subs = {
                label: subscription
                for label, subscription in self.spy_subs.items()
                if label not in unhealthy_spy
            }
            self.spy_plan_key = None
            self.spy_retry_at = time.monotonic() + OPTION_ROTATION_RETRY_SECONDS
            return
        if time.monotonic() < getattr(self, "spy_retry_at", 0.0):
            return
        spy_price = estimate_spy_reference(rows)
        if spy_price is None:
            return
        strike_step = max(self.stream_settings.spy_strike_step, 1)
        strikes = build_spy_option_strikes(
            spy_price,
            lines=self.stream_settings.spy_option_lines,
            step=strike_step,
        )
        if not strikes:
            return
        rounded_atm = round(spy_price / strike_step) * strike_step
        plan_key = (expiry, int(rounded_atm))
        if plan_key == self.spy_plan_key:
            return
        desired_contracts = spy_option_contracts(expiry, strikes)
        desired_by_label = {
            label: (label, kind, contract)
            for label, kind, contract in desired_contracts
        }
        retained_labels = set(self.spy_subs) & set(desired_by_label)
        added_labels = set(desired_by_label) - retained_labels
        obsolete_labels = set(self.spy_subs) - retained_labels
        line_budget = max(int(self.stream_settings.spy_option_lines), len(desired_by_label))
        free_lines = max(line_budget - len(self.spy_subs), 0)
        release_count = max(len(added_labels) - free_lines, 0)
        release_labels = set(sorted(obsolete_labels)[:release_count])
        released_subs = {label: self.spy_subs[label] for label in release_labels}
        if released_subs and not self._cancel_batch(released_subs):
            return
        remaining_spy = {
            label: subscription
            for label, subscription in self.spy_subs.items()
            if label not in release_labels
        }
        rejection_sequence = getattr(self, "subscription_rejection_sequence", 0)
        addition_definitions = self._resolve_option_definitions(
            [desired_by_label[label] for label in sorted(added_labels)]
        )
        additions = qualify_and_subscribe(
            self.ib,
            addition_definitions,
            qualify=False,
        )
        if not self._subscription_batch_succeeded(
            additions,
            expected_count=len(added_labels),
            rejection_sequence=rejection_sequence,
            lane="spy",
        ):
            self._cancel_batch(additions)
            restored = self._restore_subscriptions(released_subs, lane="spy")
            self.spy_subs = {**remaining_spy, **restored}
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "spy_option_replan_failed",
                    "spy_atm": rounded_atm,
                    "added": len(added_labels),
                    "restored": len(restored),
                }
            )
            self.spy_retry_at = time.monotonic() + OPTION_ROTATION_RETRY_SECONDS
            return
        obsolete_subs = {
            label: remaining_spy[label]
            for label in obsolete_labels - release_labels
        }
        if not self._cancel_batch(obsolete_subs):
            self._cancel_batch(additions)
            restored = self._restore_subscriptions(released_subs, lane="spy")
            self.spy_subs = {**remaining_spy, **restored}
            return
        self.spy_subs = {
            **{label: remaining_spy[label] for label in retained_labels},
            **additions,
        }
        self.spy_plan_key = plan_key
        self.spy_retry_at = 0.0
        log_event(
            {
                "task": "ibkr_stream",
                "event": "spy_option_replan",
                "spy_atm": rounded_atm,
                "retained": len(retained_labels),
                "added": len(added_labels),
                "removed": len(obsolete_labels),
            }
        )

    def rotate_options(self) -> None:
        plan = self.option_plan
        if plan is None or not plan.rotations:
            return
        now_monotonic = time.monotonic()
        if now_monotonic < getattr(self, "rotation_retry_at", 0.0):
            return
        if any(
            not row.subscribed or row.error
            for _, row in self.rotation_subs.values()
        ):
            self._cancel_batch(self.rotation_subs)
            self.rotation_subs = {}
            self.rotation_index = max(self.rotation_index - 1, 0)
            self.rotation_retry_at = now_monotonic + OPTION_ROTATION_RETRY_SECONDS
            return
        if not self._cancel_batch(self.rotation_subs):
            return
        self.rotation_subs = {}
        slice_index = self.rotation_index % plan.rotation_count
        slice_specs = plan.rotations[slice_index]
        rejection_sequence = self.subscription_rejection_sequence
        definitions = self._resolve_option_definitions(
            option_contracts_from_specs(slice_specs)
        )
        replacement = qualify_and_subscribe(
            self.ib,
            definitions,
            qualify=False,
        )
        if not self._subscription_batch_succeeded(
            replacement,
            expected_count=len(slice_specs),
            rejection_sequence=rejection_sequence,
            lane="rotation",
        ):
            self._cancel_batch(replacement)
            self.rotation_retry_at = now_monotonic + OPTION_ROTATION_RETRY_SECONDS
            log_event(
                {
                    "task": "ibkr_stream",
                    "event": "option_rotation_failed",
                    "slice_index": slice_index,
                    "contracts": len(slice_specs),
                }
            )
            return
        self.rotation_subs = replacement
        self.rotation_index += 1
        self.rotation_retry_at = 0.0

    def flush(self) -> dict[str, object]:
        received_at = datetime.now(tz=timezone.utc)
        subscriptions = {
            **self.base_subs,
            **self.hot_subs,
            **self.rotation_subs,
            **self.spy_subs,
        }
        rows = snapshot_rows(
            subscriptions,
            self.ibkr_settings.stale_after_seconds,
            slow_index_stale_after_seconds=self.ibkr_settings.slow_index_stale_after_seconds,
            slow_index_labels=self.ibkr_settings.slow_index_labels,
        )
        merge_slow_rows(rows, self.slow_cache, set(subscriptions))
        update_option_cache(
            self.option_cache,
            rows,
            now_monotonic=time.monotonic(),
            expiry=self.option_plan.expiry if self.option_plan is not None else None,
            active_expiries=frozenset(
                spec.expiry
                for spec in (
                    self.option_plan.hot
                    + tuple(
                        item
                        for rotation in self.option_plan.rotations
                        for item in rotation
                    )
                )
            )
            if self.option_plan is not None
            else None,
        )
        merge_cached_option_rows(rows, self.option_cache, set(subscriptions))
        snapshot = snapshot_from_rows(
            rows,
            received_at=received_at,
            stale_after_seconds=self.ibkr_settings.stale_after_seconds,
            connected=self.ib.isConnected(),
            authenticated=True,
            latency_ms=None,
            error_count=provider_error_count(self.errors),
            replace_provider_quotes=True,
        )
        write_result = persist_provider_snapshot(snapshot, self.storage_settings)
        lifecycle_started = time.monotonic()

        self._advance_subscription_lifecycle(
            rows,
            lifecycle_started=lifecycle_started,
        )
        return {
            "task": "ibkr_stream",
            "event": "flush",
            "quotes": snapshot.quote_count,
            "best_quotes": write_result.best_quote_count,
            "provider_status": (
                snapshot.provider_state.status.value if snapshot.provider_state else "unknown"
            ),
            "rotation_index": self.rotation_index,
        }

    def _advance_subscription_lifecycle(
        self,
        rows: list[VerifyRow],
        *,
        lifecycle_started: float,
    ) -> None:
        """Advance one bounded lifecycle slice after hot rows are persisted."""

        # Complete an already-held slow batch promptly, but never start a new
        # qualification before the hot-plan work has had its bounded turn.
        self.advance_slow_poll(allow_start=False)
        self.ensure_option_plan(rows)
        if lifecycle_has_qualification_budget(lifecycle_started):
            self.ensure_spy_option_plan(
                rows,
                expiry=(
                    self.option_plan.expiry
                    if self.option_plan is not None
                    else default_spxw_expiry()
                ),
            )
        if lifecycle_has_qualification_budget(lifecycle_started):
            if self.slow_poll_start_due():
                # A due slow chunk gets one lifecycle slice ahead of rotation;
                # otherwise continuous rotation qualification can starve it.
                self.advance_slow_poll(allow_start=True)
            else:
                self.rotate_options()
        # A zero-hold batch, or one whose hold elapsed during other lifecycle
        # work, can be completed without admitting another qualification.
        self.advance_slow_poll(allow_start=False)

    def teardown(self) -> None:
        cancel_subscriptions(self.ib, self.rotation_subs)
        cancel_subscriptions(self.ib, self.hot_subs)
        cancel_subscriptions(self.ib, self.spy_subs)
        cancel_subscriptions(self.ib, self.slow_active_subs)
        cancel_subscriptions(self.ib, self.base_subs)
        self.base_subs = {}
        self.hot_subs = {}
        self.rotation_subs = {}
        self.spy_subs = {}
        self.spy_plan_key = None
        self.spy_retry_at = 0.0
        self.slow_active_subs = {}
        self.option_plan = None
        self.option_replan_controller = OptionReplanController(
            trigger_points=self.stream_settings.replan_drift_points,
            rearm_points=min(10.0, self.stream_settings.replan_drift_points / 2.0),
        )
        self.rotation_index = 0
        self.rotation_retry_at = 0.0
        self.slow_cache = {}
        self.slow_contracts = []
        self.slow_chunks = []
        self.slow_scheduler = None
        self.slow_qualified_contracts = {}
        self.slow_unresolved_contracts = set()
        self.qualified_option_contracts = {}
        self.subscription_rejection_sequence = 0
        self.subscription_rejection_log = []
        self.errors = []
        self.subscription_rows_by_req_id = {}
        self.subscription_lane_by_req_id = {}
        self.subscription_health_failed = False
        if self.ib.isConnected():
            self.ib.disconnect()

    def drain_new_errors(self) -> list[IbkrError]:
        errors, self.errors = self.errors, []
        return errors


def replace_client_id(settings: IbkrSettings, client_id: int) -> IbkrSettings:
    payload = asdict(settings)
    payload["client_id"] = client_id
    return IbkrSettings(**payload)


@dataclass
class StreamRuntime:
    collector: StreamCollector
    stream_settings: IbkrStreamSettings
    storage_settings: StorageSettings
    runtime_policy: RuntimePolicySettings
    reconnect: ReconnectPolicy = field(init=False)
    deadline: float | None = None
    last_gateway_restart_at: float | None = None
    session_had_healthy_flush: bool = False

    def __post_init__(self) -> None:
        self.reconnect = ReconnectPolicy(
            min_seconds=self.stream_settings.reconnect_min_seconds,
            max_seconds=self.stream_settings.reconnect_max_seconds,
        )

    def expired(self) -> bool:
        return self.deadline is not None and time.monotonic() >= self.deadline

    def run(self) -> int:
        while not self.expired():
            if not self.collector.allowed():
                persist_state_only(
                    unavailable_state("runtime policy blocks IBKR collection"),
                    self.storage_settings,
                )
                log_event({"task": "ibkr_stream", "event": "policy_blocked"})
                self.sleep(self.stream_settings.policy_check_seconds)
                continue

            try:
                self.collector.open_session()
            except Exception as exc:  # noqa: BLE001
                delay = self.reconnect.next_delay()
                persist_state_only(
                    unavailable_state(f"connect failed: {exc}"),
                    self.storage_settings,
                )
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "connect_failed",
                        "error": str(exc),
                        "retry_in_seconds": delay,
                    }
                )
                sleep_until_reconnect(
                    host=self.collector.ibkr_settings.host,
                    port=self.collector.ibkr_settings.port,
                    delay_seconds=delay,
                )
                continue

            log_event({"task": "ibkr_stream", "event": "connected"})
            persist_state_only(connected_state(), self.storage_settings)

            probe = probe_data_plane(self.collector.ib, self.collector.ibkr_settings)
            log_event(probe.to_log_event())
            if not probe.ok:
                event = self.collector.farm_health.mark_probe_failed(probe)
                log_event(event.to_log_event(task="ibkr_stream"))

            needs_reconnect_backoff = False
            self.session_had_healthy_flush = False
            try:
                self.collector.subscribe_base()
                needs_reconnect_backoff = self.session_loop()
            except Exception as exc:  # noqa: BLE001
                needs_reconnect_backoff = True
                persist_state_only(
                    unavailable_state(f"session failed: {exc}", connected=False),
                    self.storage_settings,
                )
                log_event({"task": "ibkr_stream", "event": "session_error", "error": str(exc)})
            finally:
                self.collector.teardown()
            if self.session_had_healthy_flush:
                self.reconnect.reset()
            if needs_reconnect_backoff and not self.expired():
                delay = self.reconnect.next_delay()
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "session_reconnect_backoff",
                        "retry_in_seconds": delay,
                    }
                )
                self.sleep(delay)
        return 0

    def session_loop(self) -> bool:
        while not self.expired():
            self.collector.ib.sleep(
                effective_hot_flush_sleep_seconds(
                    self.stream_settings.flush_interval_seconds
                )
            )
            event = self.collector.flush()
            log_event(event)
            if self.collector.subscription_health_failed:
                persist_state_only(
                    unavailable_state(
                        "IBKR subscription lifecycle failed; reconnecting",
                        connected=self.collector.ib.isConnected(),
                    ),
                    self.storage_settings,
                )
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "subscription_health_reconnect",
                    }
                )
                return True

            new_errors = self.collector.drain_new_errors()
            competing = has_competing_session_error(new_errors)
            gateway_restart = self._should_restart_gateway()
            action = decide_after_flush(
                connected=self.collector.ib.isConnected(),
                allowed=self.collector.allowed(),
                competing_session=competing,
                gateway_restart=gateway_restart,
            )
            if action is StreamAction.CONTINUE:
                self.session_had_healthy_flush = True
                continue

            if action is StreamAction.GATEWAY_RESTART:
                self._restart_gateway_for_farm_outage()
                return False

            if action is StreamAction.CONFLICT_WAIT:
                persist_state_only(
                    unavailable_state(
                        "competing session blocks live market data (IBKR 10197)",
                        connected=self.collector.ib.isConnected(),
                    ),
                    self.storage_settings,
                )
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "competing_session",
                        "probe_in_seconds": self.runtime_policy.ibkr_conflict_probe_seconds,
                    }
                )
                self.collector.teardown()
                self.sleep(self.runtime_policy.ibkr_conflict_probe_seconds)
                return False

            if action is StreamAction.POLICY_BLOCKED:
                log_event({"task": "ibkr_stream", "event": "policy_blocked_mid_session"})
                return False

            # RECONNECT: fall back to the outer loop's backoff.
            log_event({"task": "ibkr_stream", "event": "disconnected"})
            return True
        return False

    def _should_restart_gateway(self) -> bool:
        if not self.stream_settings.auto_restart_gateway_on_farm_broken:
            return False
        if runtime_blocks_gateway_restart(self.runtime_policy, force=self.collector.force):
            return False
        if not self.collector.farm_health.should_restart_gateway():
            return False
        if self.last_gateway_restart_at is not None:
            elapsed = time.monotonic() - self.last_gateway_restart_at
            if elapsed < self.stream_settings.gateway_restart_cooldown_seconds:
                return False
        return True

    def _restart_gateway_for_farm_outage(self) -> None:
        broken_seconds = self.collector.farm_health.broken_duration()
        persist_state_only(
            unavailable_state(
                "IBKR data farms broken; restarting gateway",
                connected=self.collector.ib.isConnected(),
            ),
            self.storage_settings,
        )
        restarted = request_gateway_restart()
        self.last_gateway_restart_at = time.monotonic()
        self.collector.farm_health.reset()
        log_event(
            {
                "task": "ibkr_stream",
                "event": "gateway_restart_requested",
                "restarted": restarted,
                "broken_seconds": round(broken_seconds or 0.0, 1),
                "farm": self.collector.farm_health.last_farm,
                "cooldown_seconds": self.stream_settings.gateway_restart_cooldown_seconds,
            }
        )
        self.collector.teardown()
        self.sleep(self.stream_settings.gateway_restart_cooldown_seconds)

    def sleep(self, seconds: float) -> None:
        remaining = seconds
        while remaining > 0 and not self.expired():
            time.sleep(min(remaining, 1.0))
            remaining -= 1.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the persistent streaming IBKR market-data collector."
    )
    parser.add_argument("--print-config", action="store_true", help="Print settings and exit.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore the runtime mode gate and stream anyway.",
    )
    parser.add_argument(
        "--skip-options",
        action="store_true",
        help="Stream indexes/stocks/futures/CFDs only.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=None,
        help="Exit after this many seconds (smoke tests). Default: run forever.",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ibkr_settings = IbkrSettings.from_env()
    stream_settings = IbkrStreamSettings.from_env()
    sampling_settings = SamplingSettings.from_env()
    storage_settings = StorageSettings.from_env()
    runtime_policy = RuntimePolicySettings.from_env()

    if args.print_config:
        print(
            json.dumps(
                {
                    "ibkr": asdict(ibkr_settings),
                    "stream": asdict(stream_settings),
                    "sampling": asdict(sampling_settings),
                    "storage": asdict(storage_settings),
                },
                indent=2,
                sort_keys=True,
                default=str,
            )
        )
        return 0

    try:
        from ib_async import IB
    except ImportError as exc:
        raise SystemExit("Missing dependency: ib_async. Run `uv sync` first.") from exc

    collector = StreamCollector(
        IB(),
        ibkr_settings=ibkr_settings,
        stream_settings=stream_settings,
        sampling_settings=sampling_settings,
        storage_settings=storage_settings,
        runtime_policy=runtime_policy,
        force=args.force,
        skip_options=args.skip_options,
    )
    prepare_ib_client(collector.ib, request_timeout_seconds=ibkr_settings.request_timeout_seconds)
    runtime = StreamRuntime(
        collector=collector,
        stream_settings=stream_settings,
        storage_settings=storage_settings,
        runtime_policy=runtime_policy,
    )
    if args.duration_seconds is not None:
        runtime.deadline = time.monotonic() + args.duration_seconds
    try:
        return runtime.run()
    finally:
        collector.teardown()


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
