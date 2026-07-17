"""Operator CLI for notification delivery dead letters.

Usage:
    python -m spx_spark.notifier.dead_letters list
    python -m spx_spark.notifier.dead_letters replay <event_id>
    python -m spx_spark.notifier.dead_letters ack <event_id>

``replay`` resets the event's dead-letter targets to pending so the recovery
task redelivers them; ``ack`` marks them reviewed so recovery stops failing.
"""

from __future__ import annotations

import argparse
import json

from spx_spark.config import NotificationSettings
from spx_spark.notifier.dispatcher import _delivery_outbox


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List, replay, or acknowledge notification delivery dead letters."
    )
    parser.add_argument("action", choices=("list", "replay", "ack"))
    parser.add_argument("event_id", nargs="?", default="")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = NotificationSettings.from_env()
    if not settings.delivery_outbox_path:
        print(json.dumps({"ok": False, "error": "delivery outbox path is not configured"}))
        return 1
    outbox = _delivery_outbox(settings)
    if args.action == "list":
        dead_letters = outbox.list_dead_letters()
        print(
            json.dumps(
                {"ok": True, "dead_letters": dead_letters},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    if not args.event_id.strip():
        print(json.dumps({"ok": False, "error": f"{args.action} requires an event id"}))
        return 1
    if args.action == "replay":
        replayed = outbox.replay_dead_letter(args.event_id)
        print(
            json.dumps(
                {"ok": replayed > 0, "event_id": args.event_id, "replayed_targets": replayed}
            )
        )
        return 0 if replayed else 1
    acknowledged = outbox.acknowledge_dead_letter(args.event_id)
    print(
        json.dumps(
            {
                "ok": acknowledged > 0,
                "event_id": args.event_id,
                "acknowledged_targets": acknowledged,
            }
        )
    )
    return 0 if acknowledged else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
