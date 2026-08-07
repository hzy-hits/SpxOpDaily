"""Single-worker scheduled jobs for the simplified runtime."""

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


@huey.periodic_task(crontab(minute="30", hour="23", strict=True))
def maintenance_daily() -> None:
    from spx_spark.maintenance import run

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
