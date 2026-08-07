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


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def data(ctx: typer.Context) -> None:
    """Run operational-data status, query, replay, sync or compaction commands."""
    args = list(ctx.args)
    if args[:1] == ["compact"]:
        from spx_spark.data_platform.lake.compact import main as compact

        raise typer.Exit(compact(args[1:]))
    from spx_spark.data_platform.cli import run

    raise typer.Exit(run(args))


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def ops(ctx: typer.Context) -> None:
    """Run a maintenance, profile, runtime-mode, sampling or mock-data tool."""
    name, args = _tool_args(ctx)
    if name == "alert-profile":
        from spx_spark.alert_profile import run
    elif name == "maintenance":
        from spx_spark.maintenance import run
    elif name == "mock-collector":
        from spx_spark.mock_collector import run
    elif name == "runtime-mode":
        from spx_spark.runtime_mode import main as run
    elif name == "sampling-plan":
        from spx_spark.sampling import run
    else:
        raise typer.BadParameter(f"unknown ops tool: {name}")
    _finish(run(args))


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def verify(ctx: typer.Context) -> None:
    """Run an IBKR or Schwab provider verifier."""
    name, args = _tool_args(ctx)
    if name == "ibkr":
        from spx_spark.ibkr.verifier import run
    elif name == "schwab":
        from spx_spark.schwab.verifier import run
    else:
        raise typer.BadParameter(f"unknown verifier: {name}")
    _finish(run(args))


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def report(ctx: typer.Context) -> None:
    """Run an IBKR-hours, Micopedia or options-map report."""
    name, args = _tool_args(ctx)
    if name == "ibkr-hours":
        from spx_spark.ibkr.trading_hours_report import run
    elif name == "micopedia":
        from spx_spark.strategy.micopedia import run
    elif name == "options-map":
        from spx_spark.options_map import run
    else:
        raise typer.BadParameter(f"unknown report: {name}")
    _finish(run(args))


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def replay(ctx: typer.Context) -> None:
    """Run a Steven-strategy or surface-dashboard replay."""
    name, args = _tool_args(ctx)
    if name == "steven":
        from spx_spark.strategy.steven_replay import run
    elif name == "surface":
        from spx_spark.surface_dashboard_replay import run
    else:
        raise typer.BadParameter(f"unknown replay tool: {name}")
    _finish(run(args))


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def ibkr(ctx: typer.Context) -> None:
    """Run an IBKR snapshot, stream, farm probe or position reader."""
    name, args = _tool_args(ctx)
    if name == "collect":
        from spx_spark.ibkr.collector import run
    elif name == "stream":
        from spx_spark.ibkr.stream.cli import run
    elif name == "farm-probe":
        from spx_spark.ibkr.farm_health import run_probe_cli as run
    elif name == "positions":
        from spx_spark.ibkr.position_watcher import run
    else:
        raise typer.BadParameter(f"unknown IBKR command: {name}")
    _finish(run(args))


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def schwab(ctx: typer.Context) -> None:
    """Run a Schwab snapshot, persistent market-data loop or OAuth gateway."""
    name, args = _tool_args(ctx)
    if name == "collect":
        from spx_spark.schwab.collector import run
    elif name == "marketdata":
        if args:
            raise typer.BadParameter("schwab marketdata takes no arguments")
        from spx_spark.schwab.collector import run_loop

        _finish(run_loop())
        return
    elif name == "oauth":
        from spx_spark.schwab.oauth_service import run
    else:
        raise typer.BadParameter(f"unknown Schwab command: {name}")
    _finish(run(args))


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def job(ctx: typer.Context) -> None:
    """Run one scheduled report job directly."""
    name, args = _tool_args(ctx)
    if name == "morning-map":
        from spx_spark.application.morning_map.service import run
    elif name == "order-map":
        from spx_spark.application.order_map.service import run
    elif name == "post-close-review":
        from spx_spark.post_close_review import run
    elif name == "rth-daily-acceptance":
        from spx_spark.application.order_map.rth_daily_acceptance import main as run
    else:
        raise typer.BadParameter(f"unknown job: {name}")
    _finish(run(args))


def _tool_args(ctx: typer.Context) -> tuple[str, list[str]]:
    if not ctx.args:
        raise typer.BadParameter("tool name is required")
    return ctx.args[0], list(ctx.args[1:])


def _finish(result: int | None) -> None:
    if result:
        raise typer.Exit(result)


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
