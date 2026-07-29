"""Bounded reconciliation between outbox receipt intents and receipt SQLite."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
import threading

from spx_spark.config import NotificationSettings
from spx_spark.notifier.delivery_outbox import (
    NotificationDeliveryOutbox,
    TerminalDeliveryReceipt,
)
from spx_spark.notifier.model import SinkResult
from spx_spark.notifier.receipts import (
    ReceiptStoreInspection,
    inspect_delivery_receipt_store,
    prepare_delivery_receipt_store,
)


RECEIPT_FULL_RECONCILE_SECONDS = 60.0


@dataclass(frozen=True)
class ReceiptMirrorSync:
    recorded: int
    pending: int
    repaired: int
    inspection: ReceiptStoreInspection


@dataclass(frozen=True)
class _ReceiptHealthCache:
    checked_at: float
    signature: tuple[int, int, int, int] | None
    inspection: ReceiptStoreInspection


_CACHE: dict[tuple[str, str], _ReceiptHealthCache] = {}
_CACHE_LOCK = threading.Lock()


def sync_terminal_receipts(
    settings: NotificationSettings,
    outbox: NotificationDeliveryOutbox,
    *,
    now: datetime,
    recorder: Callable[..., bool],
) -> ReceiptMirrorSync:
    """Sync new intents on every call and bound historical scans to 60 seconds."""

    key = (str(Path(outbox.path).resolve()), str(Path(settings.delivery_receipt_path)))
    signature = _store_signature(settings.delivery_receipt_path)
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
    now_timestamp = now.timestamp()
    full_reconcile = (
        cached is None
        or now_timestamp < cached.checked_at
        or now_timestamp - cached.checked_at >= RECEIPT_FULL_RECONCILE_SECONDS
        or signature != cached.signature
    )
    pending = outbox.list_terminal_receipts(unrecorded_only=True)

    if cached is not None and not full_reconcile and not pending:
        return ReceiptMirrorSync(
            recorded=0,
            pending=0,
            repaired=0,
            inspection=cached.inspection,
        )
    if cached is not None and not full_reconcile and not _core_store_healthy(cached.inspection):
        return ReceiptMirrorSync(
            recorded=0,
            pending=len(pending),
            repaired=0,
            inspection=cached.inspection,
        )

    if full_reconcile:
        prepare_delivery_receipt_store(settings.delivery_receipt_path)
        scope = outbox.list_terminal_receipts()
    else:
        scope = pending
    required_ids = tuple(receipt.receipt_id for receipt in scope)
    before = inspect_delivery_receipt_store(
        settings.delivery_receipt_path,
        required_mirror_ids=required_ids,
    )
    missing_before = set(before.missing_mirror_ids)
    if full_reconcile and missing_before:
        outbox.mark_terminal_receipts_unrecorded(missing_before)
        pending = outbox.list_terminal_receipts(unrecorded_only=True)
    if not _core_store_healthy(before):
        result = ReceiptMirrorSync(
            recorded=0,
            pending=outbox.count_unrecorded_terminal_receipts(),
            repaired=0,
            inspection=before,
        )
        _cache_result(key, now_timestamp, settings.delivery_receipt_path, before)
        return result

    pending_by_id = {receipt.receipt_id: receipt for receipt in pending}
    if full_reconcile:
        scope_by_id = {receipt.receipt_id: receipt for receipt in scope}
        pending = [
            scope_by_id[receipt_id] for receipt_id in pending_by_id if receipt_id in scope_by_id
        ]
    pending_ids = tuple(receipt.receipt_id for receipt in pending)
    pending_inspection = inspect_delivery_receipt_store(
        settings.delivery_receipt_path,
        required_mirror_ids=pending_ids,
    )
    missing_pending = set(pending_inspection.missing_mirror_ids)
    if not _core_store_healthy(pending_inspection):
        result = ReceiptMirrorSync(
            recorded=0,
            pending=outbox.count_unrecorded_terminal_receipts(),
            repaired=0,
            inspection=pending_inspection,
        )
        _cache_result(
            key,
            now_timestamp,
            settings.delivery_receipt_path,
            pending_inspection,
        )
        return result

    _mirror_missing_groups(
        settings,
        pending,
        missing_ids=missing_pending,
        recorder=recorder,
    )
    verification_scope = scope if full_reconcile else pending
    verification_ids = tuple(receipt.receipt_id for receipt in verification_scope)
    after = inspect_delivery_receipt_store(
        settings.delivery_receipt_path,
        required_mirror_ids=verification_ids,
    )
    missing_after = set(after.missing_mirror_ids)
    recorded = 0
    if _core_store_healthy(after):
        recorded = outbox.mark_terminal_receipts_recorded(
            tuple(receipt_id for receipt_id in verification_ids if receipt_id not in missing_after),
            now=now,
        )
    result = ReceiptMirrorSync(
        recorded=recorded,
        pending=max(
            len(missing_after),
            outbox.count_unrecorded_terminal_receipts(),
        ),
        repaired=len(missing_before - missing_after),
        inspection=after,
    )
    _cache_result(key, now_timestamp, settings.delivery_receipt_path, after)
    return result


def _mirror_missing_groups(
    settings: NotificationSettings,
    receipts: Sequence[TerminalDeliveryReceipt],
    *,
    missing_ids: set[str],
    recorder: Callable[..., bool],
) -> None:
    grouped: dict[
        tuple[str, str, datetime],
        list[TerminalDeliveryReceipt],
    ] = {}
    for receipt in receipts:
        key = (
            receipt.envelope.event_id,
            receipt.outcome,
            receipt.terminal_at,
        )
        grouped.setdefault(key, []).append(receipt)
    for (_, outcome, terminal_at), group in grouped.items():
        receipt_ids = tuple(receipt.receipt_id for receipt in group)
        if not missing_ids.intersection(receipt_ids):
            continue
        first = group[0]
        recorder(
            settings.delivery_receipt_path,
            first.envelope,
            sinks=tuple(
                SinkResult(
                    sink=receipt.sink,
                    attempted=receipt.attempted,
                    ok=receipt.ok,
                    error=(None if receipt.attempted and receipt.ok else receipt.reason),
                    verdict=receipt.outcome,
                )
                for receipt in group
            ),
            outcome=outcome,
            queued_for_recovery=any(receipt.queued_for_recovery for receipt in group),
            attempted_at=terminal_at,
            idempotency_key=(f"terminal:{outcome}:{','.join(sorted(receipt_ids))}"),
            mirror_ids=receipt_ids,
        )


def _core_store_healthy(inspection: ReceiptStoreInspection) -> bool:
    return (
        inspection.exists
        and inspection.quick_check == "ok"
        and inspection.journal_mode == "delete"
        and inspection.synchronous == "full"
        and inspection.schema_present
    )


def _store_signature(path: str | Path) -> tuple[int, int, int, int] | None:
    try:
        stat = os.stat(path)
    except (OSError, TypeError, ValueError):
        return None
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)


def _cache_result(
    key: tuple[str, str],
    checked_at: float,
    path: str | Path,
    inspection: ReceiptStoreInspection,
) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = _ReceiptHealthCache(
            checked_at=checked_at,
            signature=_store_signature(path),
            inspection=inspection,
        )


def clear_receipt_mirror_cache() -> None:
    """Test/process-reload hook; normal workers retain the bounded cache."""

    with _CACHE_LOCK:
        _CACHE.clear()
