from __future__ import annotations

import argparse
import json
from dataclasses import replace

from spx_spark.alert_profile import parse_at
from spx_spark.config import NotificationSettings, StorageSettings, direct_alert_delivery_enabled


def print_alerts(payload: dict[str, object]) -> None:
    window = payload["window"]
    assert isinstance(window, dict)
    print(f"Alert window: {window['name']} priority={window['priority']}")
    print(f"As of: {payload['as_of']}")
    print(f"Alerts: {payload['alert_count']}")
    alerts = payload["alerts"]
    assert isinstance(alerts, list)
    for item in alerts:
        assert isinstance(item, dict)
        print(f"- [{item['severity']}] {item['title']}")
        print(f"  {item['detail']}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate current SPX alert conditions.")
    parser.add_argument(
        "--at", help="ISO timestamp. Naive timestamps are treated as Asia/Shanghai."
    )
    parser.add_argument("--json", action="store_true", help="Print JSON.")
    parser.add_argument("--notify", action="store_true", help="Send configured notifications.")
    parser.add_argument(
        "--no-notify", action="store_true", help="Disable notifications for this run."
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    # Resolve through the package facade so tests can monkeypatch
    # ``spx_spark.alert_engine.<symbol>`` without chasing submodule bindings.
    from spx_spark import alert_engine as ae
    from spx_spark.settings import load_app_settings

    args = parse_args(argv)
    now = parse_at(args.at) if args.at else None
    app_settings = load_app_settings()
    state = ae.LatestStateStore(StorageSettings.from_env()).load(now=now)
    notification_settings = NotificationSettings.from_env()
    if args.notify:
        notification_settings = replace(notification_settings, enabled=True)
    elif args.no_notify:
        notification_settings = replace(notification_settings, enabled=False)
    elif not direct_alert_delivery_enabled():
        # Dual-path cutover: outbox owns live notify unless direct delivery is on.
        notification_settings = replace(notification_settings, enabled=False)
    payload = ae.evaluate_payload(
        state,
        now=now or state.as_of,
        persist_system_events=False,
        persist_movement_state=False,
        persist_gamma_regime=True,
        alert_settings=app_settings.alerts,
    )
    system_event_pending = any(
        isinstance(alert, dict)
        and alert.get("source_gate") in {"ibkr_session_state", "provider_failover_state"}
        for alert in payload.get("alerts", [])
    )
    movement_pending = any(
        isinstance(alert, dict) and alert.get("kind") == "price_move_from_close"
        for alert in payload.get("alerts", [])
    )
    notification_result = None
    if notification_settings.enabled:
        notification_result = ae.notify_payload(payload, settings=notification_settings)
        ae.reconcile_position_event_acknowledgements(notification_result.acknowledged_event_ids)
        payload["notification"] = notification_result.to_dict()
    notified = notification_result is not None and notification_result.sent_count > 0
    settled = not notification_settings.enabled or notified
    if not system_event_pending or settled:
        ae.persist_system_event_state(state)
    if not movement_pending or settled:
        ae.persist_movement_state_snapshot(state)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_alerts(payload)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
