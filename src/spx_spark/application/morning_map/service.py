"""Morning-map CLI orchestration."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from spx_spark.application.morning_map.build import _morning_payload_is_thin
from spx_spark.application.morning_map.render import build_map_prompt, render_template
from spx_spark.application.morning_map.state import (
    already_sent,
    default_state_path,
    mark_sent,
    within_send_window,
)
from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.notifier.llm_writer import generate_push_text, record_push


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send SPX Spark pre-market map push.")
    parser.add_argument("--dry-run", action="store_true", help="Print template/agent text only.")
    parser.add_argument(
        "--force", action="store_true", help="Skip time window and idempotency gate."
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    # Resolve through the package facade so tests can monkeypatch
    # ``spx_spark.morning_map.<symbol>`` without chasing submodule bindings.
    from spx_spark import morning_map as mm

    args = parse_args(argv)
    now = now or datetime.now(tz=timezone.utc)
    storage_settings = StorageSettings.from_env()
    state_path = default_state_path(storage_settings)
    trading_date = DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat()

    if not args.force and not args.dry_run:
        if not within_send_window(now):
            print(json.dumps({"skipped": True, "reason": "outside_send_window"}))
            return 0
        if already_sent(state_path, trading_date):
            print(json.dumps({"skipped": True, "reason": "already_sent"}))
            return 0

    payload = mm.build_morning_payload_with_retry(storage_settings, now=now)
    if _morning_payload_is_thin(payload) and not args.force and not args.dry_run:
        print(json.dumps({"skipped": True, "reason": "thin_snapshot_sampling_gap"}))
        return 0
    template = render_template(payload)

    if args.dry_run:
        print(template)
        settings = mm.NotificationSettings.from_env()
        text, writer = generate_push_text(template, build_map_prompt(payload, template), settings)
        if writer != "template":
            print(f"\n--- {writer} ---\n")
            print(text)
        print(json.dumps({"dry_run": True}))
        return 0

    settings = mm.NotificationSettings.from_env()
    result = mm.send_morning_map(
        payload, settings, now=now, previous_push=mm.load_previous_push()
    )
    if (
        result.get("delivered_ok")
        or result["im_ok"]
        or result["bark_ok"]
        or result.get("feishu_ok")
    ):
        mark_sent(state_path, trading_date)
        record_push("morning_map", result["text"], at=now.isoformat())
    print(json.dumps(result, ensure_ascii=False))
    if not (
        result.get("delivered_ok")
        or result["im_ok"]
        or result["bark_ok"]
        or result.get("feishu_ok")
    ):
        return 1
    return 0


def main() -> None:
    raise SystemExit(run())
