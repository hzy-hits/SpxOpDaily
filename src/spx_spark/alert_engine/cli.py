from __future__ import annotations

import argparse
import json
from dataclasses import replace

from spx_spark.alert_profile import parse_at
from spx_spark.config import NotificationSettings, StorageSettings, direct_alert_delivery_enabled
from spx_spark.provider_failover_controller import ProviderFailoverSettings
from spx_spark.settings import AppSettings, load_app_settings


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


def run(
    argv: list[str] | None = None,
    *,
    app_settings: AppSettings | None = None,
    storage_settings: StorageSettings | None = None,
) -> int:
    # Resolve through the package facade so tests can monkeypatch
    # ``spx_spark.alert_engine.<symbol>`` without chasing submodule bindings.
    from spx_spark import alert_engine as ae
    args = parse_args(argv)
    now = parse_at(args.at) if args.at else None
    app_settings = app_settings or load_app_settings()
    storage_settings = storage_settings or StorageSettings.from_env()
    failover_settings = ProviderFailoverSettings.from_policy(
        app_settings.runtime,
        data_root=storage_settings.data_root,
    )
    state = ae.LatestStateStore(storage_settings).load(now=now)
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
        provider_failover_settings=failover_settings,
        app_settings=app_settings,
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
    settled = not notification_settings.enabled or (
        notification_result is not None
        and notification_result.outcome in {"consumed", "delivered", "queued"}
    )
    if not system_event_pending or settled:
        ae.persist_system_event_state(
            state,
            failover_settings=failover_settings,
            alert_settings=app_settings.alerts,
        )
    if not movement_pending or settled:
        ae.persist_movement_state_snapshot(
            state,
            settings=app_settings.alerts,
        )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_alerts(payload)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
