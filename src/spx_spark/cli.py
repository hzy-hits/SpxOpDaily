"""Single operator command tree for SPX Spark."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import typer

from spx_spark import latest_state
from spx_spark.app_settings import get_settings
from spx_spark.logging_setup import configure_logging

app = typer.Typer(no_args_is_help=True)
core_app = typer.Typer(no_args_is_help=True)
notify_app = typer.Typer(no_args_is_help=True)
app.add_typer(core_app, name="core")
app.add_typer(notify_app, name="notify")


@app.callback()
def _init() -> None:
    configure_logging("spx-cli", get_settings().log_level)


@app.command()
def status(all_providers: bool = False) -> None:
    """Inspect the normalized latest market-data state."""
    argv = ["--all-providers"] if all_providers else []
    raise typer.Exit(latest_state.run(argv))


@core_app.command("run")
def core_run() -> None:
    """Run the consolidated real-time Core process."""
    from spx_spark.core_main import main

    asyncio.run(main())


@notify_app.command("test")
def notify_test() -> None:
    """Queue one real Bark/Feishu notification for delivery verification."""
    from spx_spark.config import NotificationSettings
    from spx_spark.notifier.dispatcher import enqueue_notification
    from spx_spark.notifier.model import NotificationEnvelope

    now = datetime.now(tz=timezone.utc)
    event_id = f"notify-test:{now:%Y%m%dT%H%M%SZ}:{uuid4().hex[:8]}"
    result = enqueue_notification(
        NotificationSettings.from_env(),
        NotificationEnvelope(
            event_id=event_id,
            source="spx_cli",
            kind="notification_test",
            lane="execution_safety",
            occurred_at=now,
            expires_at=now + timedelta(minutes=5),
        ),
        title="SPX notification test",
        text=f"统一通知链路测试 · {now.isoformat(timespec='seconds')}",
        enqueued_at=now,
    )
    typer.echo(
        json.dumps(
            {
                "accepted": result.accepted,
                "event_id": event_id,
                "outcome": result.outcome,
                "targets": result.targets,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    if not result.accepted:
        raise typer.Exit(1)
