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
from typing import Any

from spx_spark.config import (
    IbkrSettings,
    IbkrStreamSettings,
    RuntimePolicySettings,
    SamplingSettings,
    StorageSettings,
    default_spxw_expiry,
)
from spx_spark.ibkr.adapter import snapshot_from_rows
from spx_spark.ibkr.collector import (
    NON_DEGRADING_ERROR_CODES,
    has_competing_session_error,
)
from spx_spark.ibkr.verifier import (
    IbkrError,
    VerifyRow,
    build_base_contracts,
    cancel_subscriptions,
    connect_market_data_only,
    estimate_atm_reference,
    qualify_and_subscribe,
    snapshot_rows,
)
from spx_spark.marketdata import Provider, ProviderState, ProviderStatus
from spx_spark.provider_adapter import ProviderSnapshot, persist_provider_snapshot
from spx_spark.runtime_mode import ibkr_allowed, load_override
from spx_spark.sampling import OptionContractSpec, build_sampling_plan


MAX_TRACKED_ERRORS = 200


class StreamAction(str, Enum):
    CONTINUE = "continue"
    RECONNECT = "reconnect"
    CONFLICT_WAIT = "conflict_wait"
    POLICY_BLOCKED = "policy_blocked"


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
    return sorted(specs, key=lambda spec: (abs(spec.strike - atm_strike), spec.strike, spec.right))


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
    hot_budget = max(2, int(max_option_lines * hot_lane_share))
    hot_budget -= hot_budget % 2  # keep whole C/P pairs
    rotation_budget = max(max_option_lines - hot_budget, 0)

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


def decide_after_flush(
    *,
    connected: bool,
    allowed: bool,
    competing_session: bool,
) -> StreamAction:
    if competing_session:
        return StreamAction.CONFLICT_WAIT
    if not connected:
        return StreamAction.RECONNECT
    if not allowed:
        return StreamAction.POLICY_BLOCKED
    return StreamAction.CONTINUE


def provider_error_count(errors: list[IbkrError]) -> int:
    return sum(1 for error in errors if error.error_code not in NON_DEGRADING_ERROR_CODES)


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


def log_event(event: dict[str, object]) -> None:
    event.setdefault("ts", datetime.now(tz=timezone.utc).isoformat())
    print(json.dumps(event, sort_keys=True), flush=True)


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
        self.option_plan: OptionSubscriptionPlan | None = None
        self.rotation_index = 0
        self.errors: list[IbkrError] = []
        self.last_policy_check = 0.0

        ib.errorEvent += self._on_error

    def _on_error(self, req_id: int, error_code: int, message: str, contract: Any) -> None:
        self.errors.append(
            IbkrError(
                req_id=req_id,
                error_code=error_code,
                message=message,
                contract=str(contract) if contract is not None else None,
                ts=datetime.now(tz=timezone.utc).isoformat(),
            )
        )
        del self.errors[:-MAX_TRACKED_ERRORS]

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
        self.base_subs = qualify_and_subscribe(
            self.ib,
            build_base_contracts(self.ibkr_settings),
            qualify=self.ibkr_settings.qualify_contracts,
        )

    def ensure_option_plan(self, rows: list[VerifyRow]) -> None:
        if self.skip_options:
            return
        atm_reference, _ = estimate_atm_reference(rows)
        today = default_spxw_expiry()
        if not should_replan(
            self.option_plan,
            atm_reference,
            replan_drift_points=self.stream_settings.replan_drift_points,
            today_expiry=today,
        ):
            return

        plan = build_option_subscription_plan(
            atm_reference=float(atm_reference),
            expiry=today,
            next_expiry=None,
            mode=self.sampling_settings.default_mode,
            sampling_settings=self.sampling_settings,
            max_option_lines=self.stream_settings.max_option_lines,
            hot_lane_share=self.stream_settings.hot_lane_share,
        )
        cancel_subscriptions(self.ib, self.hot_subs)
        cancel_subscriptions(self.ib, self.rotation_subs)
        self.hot_subs = qualify_and_subscribe(
            self.ib,
            option_contracts_from_specs(plan.hot),
            qualify=self.ibkr_settings.qualify_contracts,
        )
        self.rotation_subs = {}
        self.option_plan = plan
        self.rotation_index = 0
        log_event(
            {
                "task": "ibkr_stream",
                "event": "option_replan",
                "atm_strike": plan.atm_strike,
                "expiry": plan.expiry,
                "hot_contracts": len(plan.hot),
                "rotation_slices": plan.rotation_count,
            }
        )

    def rotate_options(self) -> None:
        plan = self.option_plan
        if plan is None or not plan.rotations:
            return
        cancel_subscriptions(self.ib, self.rotation_subs)
        slice_specs = plan.rotations[self.rotation_index % plan.rotation_count]
        self.rotation_index += 1
        self.rotation_subs = qualify_and_subscribe(
            self.ib,
            option_contracts_from_specs(slice_specs),
            qualify=self.ibkr_settings.qualify_contracts,
        )

    def flush(self) -> dict[str, object]:
        received_at = datetime.now(tz=timezone.utc)
        subscriptions = {**self.base_subs, **self.hot_subs, **self.rotation_subs}
        rows = snapshot_rows(subscriptions, self.ibkr_settings.stale_after_seconds)
        snapshot = snapshot_from_rows(
            rows,
            received_at=received_at,
            stale_after_seconds=self.ibkr_settings.stale_after_seconds,
            connected=self.ib.isConnected(),
            authenticated=True,
            latency_ms=None,
            error_count=provider_error_count(self.errors),
        )
        write_result = persist_provider_snapshot(snapshot, self.storage_settings)
        self.ensure_option_plan(rows)
        self.rotate_options()
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

    def teardown(self) -> None:
        cancel_subscriptions(self.ib, self.rotation_subs)
        cancel_subscriptions(self.ib, self.hot_subs)
        cancel_subscriptions(self.ib, self.base_subs)
        self.base_subs = {}
        self.hot_subs = {}
        self.rotation_subs = {}
        self.option_plan = None
        self.rotation_index = 0
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
                self.sleep(delay)
                continue

            self.reconnect.reset()
            log_event({"task": "ibkr_stream", "event": "connected"})
            try:
                self.collector.subscribe_base()
                self.session_loop()
            except Exception as exc:  # noqa: BLE001
                log_event({"task": "ibkr_stream", "event": "session_error", "error": str(exc)})
            finally:
                self.collector.teardown()
        return 0

    def session_loop(self) -> None:
        while not self.expired():
            self.collector.ib.sleep(self.stream_settings.flush_interval_seconds)
            event = self.collector.flush()
            log_event(event)

            new_errors = self.collector.drain_new_errors()
            competing = has_competing_session_error(new_errors)
            action = decide_after_flush(
                connected=self.collector.ib.isConnected(),
                allowed=self.collector.allowed(),
                competing_session=competing,
            )
            if action is StreamAction.CONTINUE:
                continue

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
                return

            if action is StreamAction.POLICY_BLOCKED:
                log_event({"task": "ibkr_stream", "event": "policy_blocked_mid_session"})
                return

            # RECONNECT: fall back to the outer loop's backoff.
            log_event({"task": "ibkr_stream", "event": "disconnected"})
            return

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
