"""IBKR persistent stream runtime package."""

from __future__ import annotations

from spx_spark.ibkr.stream.cache import (
    mark_rows_stale,
    merge_cached_option_rows,
    merge_slow_rows,
    update_option_cache,
)
from spx_spark.ibkr.stream.cli import main, parse_args, run
from spx_spark.ibkr.stream.collector import StreamCollector
from spx_spark.ibkr.stream.contracts import (
    build_option_subscription_plan,
    build_spy_option_strikes,
    chunked,
    contract_pairs_by_atm_distance,
    contract_qualification_key,
    option_contracts_from_specs,
    option_label_distance,
    option_spec_label,
    split_base_contracts,
    spy_option_contracts,
    spy_option_spec_label,
)
from spx_spark.ibkr.stream.flush import persist_account_standby_state, persist_state_only
from spx_spark.ibkr.stream.models import (
    HOT_FLUSH_LIFECYCLE_BUDGET_SECONDS,
    HOT_FLUSH_SLEEP_MAX_SECONDS,
    MAX_TRACKED_ERRORS,
    OPTION_CACHE_TTL_SECONDS,
    OPTION_ROTATION_RETRY_SECONDS,
    QUALIFICATION_TIMEOUT_SECONDS,
    SUBSCRIPTION_CONFIRM_SECONDS,
    SUBSCRIPTION_REJECTION_CODES,
    OptionSubscriptionPlan,
    ReconnectPolicy,
    StreamAction,
    effective_hot_flush_sleep_seconds,
    lifecycle_has_qualification_budget,
    replace_client_id,
)
from spx_spark.ibkr.stream.replan_machine import (
    estimate_spy_reference,
    reference_quote_from_row,
    should_replan,
)
from spx_spark.ibkr.stream.runtime_machine import (
    account_standby_state,
    classify_connect_failure,
    connected_state,
    decide_after_flush,
    provider_error_count,
    subscription_outage_reason,
    unavailable_state,
)
from spx_spark.ibkr.stream.session import log_event, sleep_until_reconnect
from spx_spark.ibkr.stream.supervisor import StreamRuntime

__all__ = [
    "HOT_FLUSH_LIFECYCLE_BUDGET_SECONDS",
    "HOT_FLUSH_SLEEP_MAX_SECONDS",
    "MAX_TRACKED_ERRORS",
    "OPTION_CACHE_TTL_SECONDS",
    "OPTION_ROTATION_RETRY_SECONDS",
    "QUALIFICATION_TIMEOUT_SECONDS",
    "SUBSCRIPTION_CONFIRM_SECONDS",
    "SUBSCRIPTION_REJECTION_CODES",
    "OptionSubscriptionPlan",
    "ReconnectPolicy",
    "StreamAction",
    "StreamCollector",
    "StreamRuntime",
    "account_standby_state",
    "build_option_subscription_plan",
    "build_spy_option_strikes",
    "chunked",
    "classify_connect_failure",
    "connected_state",
    "contract_pairs_by_atm_distance",
    "contract_qualification_key",
    "decide_after_flush",
    "effective_hot_flush_sleep_seconds",
    "estimate_spy_reference",
    "lifecycle_has_qualification_budget",
    "log_event",
    "main",
    "mark_rows_stale",
    "merge_cached_option_rows",
    "merge_slow_rows",
    "option_contracts_from_specs",
    "option_label_distance",
    "option_spec_label",
    "parse_args",
    "persist_account_standby_state",
    "persist_state_only",
    "provider_error_count",
    "reference_quote_from_row",
    "replace_client_id",
    "run",
    "should_replan",
    "sleep_until_reconnect",
    "split_base_contracts",
    "spy_option_contracts",
    "spy_option_spec_label",
    "subscription_outage_reason",
    "unavailable_state",
    "update_option_cache",
]
