"""Slow-poll lane operations for StreamCollector."""

from __future__ import annotations

from typing import Any

from spx_spark.ibkr.stream import deps as stream_deps
from spx_spark.ibkr.stream.contracts import contract_qualification_key
from spx_spark.ibkr.stream.models import (
    QUALIFICATION_TIMEOUT_SECONDS,
)
from spx_spark.ibkr.slow_poll import SlowPollAction
from spx_spark.ibkr.verifier import VerifyRow

apply_known_index_conid = stream_deps.apply_known_index_conid
cancel_subscriptions = stream_deps.cancel_subscriptions
contract_has_con_id = stream_deps.contract_has_con_id
discard_subscriptions = stream_deps.discard_subscriptions
log_event = stream_deps.log_event
qualify_and_subscribe = stream_deps.qualify_and_subscribe
snapshot_rows = stream_deps.snapshot_rows
time = stream_deps.time


class SlowPollOps:
    def _qualify_slow_contracts(self) -> None:
        """Batch-resolve slow contracts once, outside the hot flush loop."""

        resolved_by_label: dict[str, tuple[str, str, Any]] = {}
        unresolved: list[tuple[str, str, Any]] = []
        for label, kind, contract in self.slow_contracts:
            if contract_has_con_id(contract):
                resolved_by_label[label] = (label, kind, contract)
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
                if matches:
                    resolved = matches.pop(0)
                    resolved_by_label[label] = (label, kind, resolved)
                    continue
                # Static conIds are a last-resort path when the sec-def farm
                # times out, not a substitute for a fresh session qualification.
                fallback = apply_known_index_conid(contract)
                if fallback is None:
                    log_event(
                        {
                            "task": "ibkr_stream",
                            "event": "slow_poll_qualification_failed",
                            "label": label,
                        }
                    )
                    continue
                resolved_by_label[label] = (label, kind, fallback)
                log_event(
                    {
                        "task": "ibkr_stream",
                        "event": "slow_poll_qualification_fallback",
                        "label": label,
                    }
                )

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
                connectivity_sequence = getattr(self, "tws_connectivity_loss_sequence", 0)
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
                    connectivity_sequence=connectivity_sequence,
                    confirm_seconds=0.0,
                    lane="slow",
                ):
                    self._invalidate_rejected_slow_definitions(self.slow_active_subs)
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
                self._invalidate_rejected_slow_definitions(self.slow_active_subs)
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

    def _invalidate_rejected_slow_definitions(
        self,
        subscriptions: dict[str, tuple[Any, VerifyRow]],
    ) -> None:
        """Force fresh qualification after IBKR rejects a slow contract."""

        for label, (_ticker, row) in subscriptions.items():
            if not (row.error or "").startswith("IBKR 200:"):
                continue
            self.slow_qualified_contracts.pop(label, None)
            self.slow_unresolved_contracts.add(label)

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
