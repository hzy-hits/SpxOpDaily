"""Single-worker scheduled jobs for the simplified runtime."""

from datetime import datetime, timezone

from huey import SqliteHuey, crontab

from spx_spark.app_settings import get_settings


_settings = get_settings()
huey = SqliteHuey(
    "spx-worker",
    filename=str(_settings.data_root / "huey.sqlite"),
    utc=True,
)


@huey.task(retries=2, retry_delay=5)
def deliver_notification_event(event_id: int) -> None:
    from spx_spark.notifier.unified_delivery import (
        deliver_notification_event as deliver,
    )

    deliver(event_id)


@huey.on_startup()
def recover_notification_delivery_tasks() -> None:
    from spx_spark.notifier.unified_delivery import (
        default_engine,
        recover_notification_tasks,
    )

    recover_notification_tasks(
        default_engine(),
        schedule=deliver_notification_event,
        now=datetime.now(tz=timezone.utc),
    )


@huey.periodic_task(crontab(minute="30", hour="23", strict=True))
def maintenance_daily() -> None:
    from spx_spark.maintenance import run

    if run(["cache-prune", "--execute"]) != 0:
        raise RuntimeError("surface cache maintenance failed")
    if run(["dry-run"]) != 0:
        raise RuntimeError("maintenance daily failed")


@huey.periodic_task(crontab(minute="20", strict=True))
def storage_pressure() -> None:
    from spx_spark.session_finalize import run

    if run(["--date", "auto", "--json", "--pressure-check"]) != 0:
        raise RuntimeError("storage pressure check failed")


@huey.periodic_task(crontab(minute="0", hour="12", day_of_week="0", strict=True))
def schwab_reauth_reminder() -> None:
    from spx_spark.application.schwab_reauth_reminder import run

    if run() != 0:
        raise RuntimeError("Schwab reauthorization reminder failed")


@huey.periodic_task(crontab(minute="*", strict=True))
def hyperliquid_context() -> None:
    from spx_spark.hyperliquid import collector
    from spx_spark.settings import load_app_settings

    if load_app_settings().runtime.hyperliquid_enabled and collector.run(["--json"]) != 0:
        raise RuntimeError("hyperliquid failed")


@huey.periodic_task(crontab(minute="*", strict=True))
def greek_shadow_context() -> None:
    from spx_spark import greek_shadow
    from spx_spark.settings import load_app_settings

    if load_app_settings().runtime.greek_shadow_enabled and greek_shadow.run(["--json"]) != 0:
        raise RuntimeError("greek shadow failed")


@huey.periodic_task(crontab(minute="*/5", strict=True))
def iv_surface_context() -> None:
    from spx_spark import iv_surface
    from spx_spark.settings import load_app_settings

    if load_app_settings().runtime.iv_surface_enabled and iv_surface.run(["--json"]) != 0:
        raise RuntimeError("IV surface failed")


@huey.periodic_task(crontab(minute="0,30", strict=True))
def growth_dislocation_scan() -> None:
    from spx_spark.infrastructure.growth_dislocation import run

    if run() != 0:
        raise RuntimeError("growth dislocation scan failed")
