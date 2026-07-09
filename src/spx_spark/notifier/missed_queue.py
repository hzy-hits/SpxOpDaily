"""Missed-message queue: park approved IM messages while the channel is
dead, then flush them as a single timeline digest when it recovers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from spx_spark.config import NotificationSettings
from spx_spark.notifier.model import CommandRunner, SinkResult, default_runner
from spx_spark.notifier.sinks import deliver_trade_push, im_delivery_ok

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

DIGEST_MAX_ENTRIES = 12
DIGEST_MAX_CHARS = 1800


def append_missed(path: str, message: str, *, kind: str, at: datetime) -> None:
    if not path:
        return
    entry = {
        "at": at.astimezone(timezone.utc).isoformat(),
        "kind": kind,
        "message": message,
    }
    try:
        from pathlib import Path

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        return


def load_missed(path: str) -> list[dict[str, Any]]:
    if not path:
        return []
    try:
        with open(path, encoding="utf-8") as handle:
            lines = handle.readlines()
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
            entries.append(payload)
    return entries


def clear_missed(path: str) -> None:
    if not path:
        return
    try:
        import os

        os.remove(path)
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
        clear_missed(settings.missed_queue_path)
        return SinkResult(sink="missed_digest", attempted=True, ok=True)
    for sink in sinks:
        if sink.attempted and not sink.ok:
            return sink
    return SinkResult(sink="missed_digest", attempted=False, ok=False, error="no delivery channel")
