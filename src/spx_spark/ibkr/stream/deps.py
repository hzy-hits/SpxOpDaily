"""Patchable stream runtime dependencies.

Tests monkeypatch symbols here (or via the ``stream_collector`` facade re-export).
Implementation modules must import callables from this module so patches apply.
"""

from __future__ import annotations

import time
from dataclasses import asdict

from spx_spark.ibkr.adapter import snapshot_from_rows
from spx_spark.ibkr.collector import has_competing_session_error
from spx_spark.ibkr.farm_health import (
    FarmHealthTracker,
    probe_data_plane,
    request_gateway_restart,
    runtime_blocks_gateway_restart,
)
from spx_spark.ibkr.gateway import api_port_open
from spx_spark.ibkr.position_watcher import (
    connect_broker_readonly_with_positions,
    fetch_positions,
    write_snapshot,
)
from spx_spark.ibkr.stream.cache import (
    mark_rows_stale,
    merge_cached_option_rows,
    merge_slow_rows,
    update_option_cache,
)
from spx_spark.ibkr.stream.contracts import (
    build_option_subscription_plan,
    build_spy_option_strikes,
    option_contracts_from_specs,
    option_label_distance,
    option_spec_label,
    split_base_contracts,
    spy_option_contracts,
)
from spx_spark.ibkr.stream.flush import persist_account_standby_state, persist_state_only
from spx_spark.ibkr.stream.replan_machine import (
    estimate_spy_reference,
    reference_quote_from_row,
    should_replan,
)
from spx_spark.ibkr.stream.runtime_machine import (
    classify_connect_failure,
    connected_state,
    decide_after_flush,
    provider_error_count,
    subscription_outage_reason,
    unavailable_state,
)
from spx_spark.ibkr.stream.session import log_event, sleep_until_reconnect
from spx_spark.ibkr.verifier import (
    apply_known_index_conid,
    build_base_contracts,
    cancel_subscriptions,
    connect_market_data_only,
    contract_has_con_id,
    discard_subscriptions,
    prepare_ib_client,
    qualify_and_subscribe,
    snapshot_rows,
    IbkrError,
)
from spx_spark.provider_adapter import persist_provider_snapshot
from spx_spark.ibkr.stream.models import lifecycle_has_qualification_budget, replace_client_id

# Re-export for tests that patch via deps / facade.
__all__ = [
    "FarmHealthTracker",
    "IbkrError",
    "api_port_open",
    "apply_known_index_conid",
    "asdict",
    "build_base_contracts",
    "build_option_subscription_plan",
    "build_spy_option_strikes",
    "cancel_subscriptions",
    "classify_connect_failure",
    "connect_broker_readonly_with_positions",
    "connect_market_data_only",
    "connected_state",
    "contract_has_con_id",
    "decide_after_flush",
    "discard_subscriptions",
    "estimate_spy_reference",
    "fetch_positions",
    "has_competing_session_error",
    "lifecycle_has_qualification_budget",
    "log_event",
    "mark_rows_stale",
    "merge_cached_option_rows",
    "merge_slow_rows",
    "option_contracts_from_specs",
    "option_label_distance",
    "option_spec_label",
    "persist_account_standby_state",
    "persist_provider_snapshot",
    "persist_state_only",
    "prepare_ib_client",
    "probe_data_plane",
    "provider_error_count",
    "qualify_and_subscribe",
    "reference_quote_from_row",
    "replace_client_id",
    "request_gateway_restart",
    "runtime_blocks_gateway_restart",
    "should_replan",
    "sleep_until_reconnect",
    "snapshot_from_rows",
    "snapshot_rows",
    "split_base_contracts",
    "spy_option_contracts",
    "subscription_outage_reason",
    "time",
    "unavailable_state",
    "update_option_cache",
    "write_snapshot",
]
