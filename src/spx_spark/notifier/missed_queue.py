"""Missed-message queue: park approved IM messages while the channel is
dead, then flush them as a single timeline digest when it recovers."""

from __future__ import annotations

import json
import hashlib
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from spx_spark.config import NotificationSettings
from spx_spark.notifier.model import CommandRunner, SinkResult, default_runner
from spx_spark.notifier.sinks import deliver_trade_push, im_delivery_ok
from spx_spark.state_io import exclusive_state_lock

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

DIGEST_MAX_ENTRIES = 12
DIGEST_MAX_CHARS = 1800


def _entry_id(*, at: str, kind: str, message: str, event_id: str | None = None) -> str:
    if event_id:
        return event_id
    return hashlib.sha256(f"{at}|{kind}|{message}".encode("utf-8")).hexdigest()


def append_missed(
    path: str,
    message: str,
    *,
    kind: str,
    at: datetime,
    event_id: str | None = None,
) -> None:
    if not path:
        return
    at_text = at.astimezone(timezone.utc).isoformat()
    entry = {
        "entry_id": _entry_id(at=at_text, kind=kind, message=message, event_id=event_id),
        "at": at_text,
        "kind": kind,
        "message": message,
    }
    try:
        queue_path = Path(path)
        with exclusive_state_lock(queue_path):
            entries = _load_missed_unlocked(queue_path)
            if any(item.get("entry_id") == entry["entry_id"] for item in entries):
                return
            _write_missed_unlocked(queue_path, [*entries, entry])
    except OSError:
        return


def load_missed(path: str) -> list[dict[str, Any]]:
    if not path:
        return []
    try:
        queue_path = Path(path)
        with exclusive_state_lock(queue_path):
            return _load_missed_unlocked(queue_path)
    except OSError:
        return []


def _load_missed_unlocked(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    entries: list[dict[str, Any]] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            if not payload.get("entry_id"):
                payload["entry_id"] = _entry_id(
                    at=str(payload.get("at") or ""),
                    kind=str(payload.get("kind") or ""),
                    message=str(payload.get("message") or ""),
                )
            entries.append(payload)
    return entries


def _write_missed_unlocked(path: Path, entries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            for entry in entries:
                handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
        os.chmod(path, 0o600)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temp_name)
        except OSError:
            pass


def clear_missed(path: str) -> None:
    if not path:
        return
    try:
        queue_path = Path(path)
        with exclusive_state_lock(queue_path):
            queue_path.unlink(missing_ok=True)
    except OSError:
        return


def _ack_missed(path: str, delivered: list[dict[str, Any]]) -> None:
    delivered_ids = {str(entry.get("entry_id") or "") for entry in delivered}
    queue_path = Path(path)
    with exclusive_state_lock(queue_path):
        remaining = [
            entry
            for entry in _load_missed_unlocked(queue_path)
            if str(entry.get("entry_id") or "") not in delivered_ids
        ]
        if remaining:
            _write_missed_unlocked(queue_path, remaining)
        else:
            queue_path.unlink(missing_ok=True)


def ack_missed_event_ids(path: str, event_ids: set[str] | frozenset[str]) -> None:
    """Remove rollback-shadow entries after the SQLite outbox fully delivers."""

    if not path or not event_ids:
        return
    queue_path = Path(path)
    try:
        with exclusive_state_lock(queue_path):
            remaining = [
                entry
                for entry in _load_missed_unlocked(queue_path)
                if str(entry.get("entry_id") or "") not in event_ids
            ]
            if remaining:
                _write_missed_unlocked(queue_path, remaining)
            else:
                queue_path.unlink(missing_ok=True)
    except OSError:
        return


def build_digest(entries: list[dict[str, Any]], *, now: datetime | None = None) -> str:
    del now
    sorted_entries = sorted(entries, key=lambda entry: str(entry.get("at", "")))
    omitted = 0
    if len(sorted_entries) > DIGEST_MAX_ENTRIES:
        omitted = len(sorted_entries) - DIGEST_MAX_ENTRIES
        display_entries = sorted_entries[-DIGEST_MAX_ENTRIES:]
    else:
        display_entries = sorted_entries

    lines = [f"通道离线期间错过 {len(entries)} 条提醒,时间线如下:"]
    for entry in display_entries:
        at_raw = entry.get("at")
        time_label = "??:??"
        if isinstance(at_raw, str) and at_raw:
            try:
                at_dt = datetime.fromisoformat(at_raw.replace("Z", "+00:00"))
                if at_dt.tzinfo is None:
                    at_dt = at_dt.replace(tzinfo=timezone.utc)
                time_label = at_dt.astimezone(BEIJING_TZ).strftime("%H:%M")
            except ValueError:
                pass
        message = str(entry.get("message", ""))
        first_line = message.split("\n", 1)[0]
        if len(first_line) > 120:
            first_line = first_line[:120] + "…"
        lines.append(f"- {time_label} {first_line}")

    if omitted > 0:
        lines.append(f"(另有 {omitted} 条更早的已省略)")

    text = "\n".join(lines)
    if len(text) > DIGEST_MAX_CHARS:
        text = text[:DIGEST_MAX_CHARS] + "\n..."
    return text


def flush_missed(
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
) -> SinkResult | None:
    entries = load_missed(settings.missed_queue_path)
    if not entries:
        return None
    digest = build_digest(entries)
    sinks = deliver_trade_push(
        settings,
        title="SPX 错过提醒",
        text=digest,
        kind="status",
        lane="trade",
        friend=False,
        runner=runner,
    )
    if im_delivery_ok(sinks):
        _ack_missed(settings.missed_queue_path, entries)
        return SinkResult(sink="missed_digest", attempted=True, ok=True)
    for sink in sinks:
        if sink.attempted and not sink.ok:
            return sink
    return SinkResult(sink="missed_digest", attempted=False, ok=False, error="no delivery channel")


def run() -> int:
    """One-shot recovery entrypoint for the 24h service loop."""

    settings = NotificationSettings.from_env()
    if not settings.enabled:
        print(json.dumps({"ok": True, "skipped": "notification_disabled"}))
        return 0
    if settings.delivery_outbox_enabled:
        # Import lazily to avoid a module cycle: dispatcher keeps the legacy
        # JSONL helpers only for rollback shadowing and disabled-outbox fallback.
        from spx_spark.notifier.dispatcher import recover_pending_notifications

        summary = recover_pending_notifications(settings)
        print(json.dumps(summary, sort_keys=True))
        return 0 if summary.get("ok") else 1
    pending_before = len(load_missed(settings.missed_queue_path))
    result = flush_missed(settings)
    pending_after = len(load_missed(settings.missed_queue_path))
    print(
        json.dumps(
            {
                "ok": result is None or result.ok,
                "pending_before": pending_before,
                "pending_after": pending_after,
                "attempted": bool(result and result.attempted),
            },
            sort_keys=True,
        )
    )
    return 0 if result is None or result.ok or not result.attempted else 1


if __name__ == "__main__":
    raise SystemExit(run())
