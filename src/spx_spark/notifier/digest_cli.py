"""Flush the missed-message queue as one WeChat digest. Called by the
keepalive timer after it proves the channel is alive."""

from __future__ import annotations

import json

from spx_spark.config import NotificationSettings
from spx_spark.notifier.missed_queue import flush_missed, load_missed


def run(argv: list[str] | None = None) -> int:
    del argv
    settings = NotificationSettings.from_env()
    entries = load_missed(settings.missed_queue_path)
    if not entries:
        print(json.dumps({"flushed": False, "count": 0}))
        return 0
    result = flush_missed(settings)
    print(
        json.dumps(
            {
                "flushed": result is not None and result.ok,
                "count": len(entries),
                "error": result.error if result else None,
            }
        )
    )
    return 0 if (result and result.ok) else 1


def main() -> None:
    raise SystemExit(run())
