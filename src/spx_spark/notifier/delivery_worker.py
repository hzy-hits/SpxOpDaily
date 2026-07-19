"""Independent notification outbox consumer.

Producers only call ``enqueue_notification``.  This process owns all external
delivery I/O, acknowledgements and retry scheduling.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import threading
from types import FrameType

from spx_spark.config import NotificationSettings
from spx_spark.notifier.dispatcher import consume_pending_notifications


DEFAULT_POLL_SECONDS = 0.5


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume durable notification jobs.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Consume one due batch and exit.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help="Idle polling interval for the long-lived worker.",
    )
    return parser.parse_args(argv)


def run(
    argv: list[str] | None = None,
    *,
    stop_event: threading.Event | None = None,
) -> int:
    args = parse_args(argv)
    if args.poll_seconds <= 0:
        raise SystemExit("--poll-seconds must be > 0")
    settings = NotificationSettings.from_env()
    if not settings.enabled:
        print(json.dumps({"ok": True, "skipped": "notification_disabled"}, sort_keys=True))
        return 0
    if not settings.delivery_outbox_enabled or not settings.delivery_outbox_path:
        print(json.dumps({"ok": False, "error": "delivery_outbox_disabled"}, sort_keys=True))
        return 1

    owns_stop_event = stop_event is None
    stop_event = stop_event or threading.Event()
    if not args.once and owns_stop_event:
        install_stop_handlers(stop_event)
    worker_id = f"notification-delivery:{os.getpid()}"
    while not stop_event.is_set():
        # The existing 60-second recovery task owns dead-letter ops alerts.
        # Keeping them out of this sub-second loop prevents a broken ops sink
        # from being hammered on every idle poll.
        summary = consume_pending_notifications(
            settings,
            notify_dead_letters=False,
            worker_id=worker_id,
        )
        if args.once or _has_activity(summary):
            print(json.dumps(summary, sort_keys=True), flush=True)
        if args.once:
            return 0 if summary.get("ok") else 1
        # SIGTERM only sets the event. A currently claimed single-target
        # delivery finishes and settles; no new claim starts afterward.
        if stop_event.is_set():
            break
        if _has_activity(summary):
            continue
        stop_event.wait(args.poll_seconds)
    return 0


def install_stop_handlers(stop_event: threading.Event) -> None:
    def request_stop(signum: int, frame: FrameType | None) -> None:  # noqa: ARG001
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)


def _has_activity(summary: dict[str, object]) -> bool:
    return any(
        int(summary.get(key) or 0) > 0
        for key in (
            "imported_legacy",
            "jobs",
            "attempted_targets",
            "delivered_targets",
            "dead_lettered",
            "dead_letter_notified",
            "pruned_shadow",
        )
    )


if __name__ == "__main__":
    raise SystemExit(run())
