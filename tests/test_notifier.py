from __future__ import annotations

import subprocess
from datetime import datetime, timedelta, timezone

from spx_spark.config import NotificationSettings
from spx_spark.notifier import notify_payload


def make_settings(
    state_path: str,
    *,
    enabled: bool = True,
    target: str = "user@im.wechat",
    dry_run: bool = True,
) -> NotificationSettings:
    return NotificationSettings(
        enabled=enabled,
        min_severity="high",
        cooldown_seconds=300,
        state_path=state_path,
        openclaw_enabled=True,
        openclaw_command="openclaw",
        openclaw_channel="openclaw-weixin",
        openclaw_account="account-im-bot",
        openclaw_target=target,
        openclaw_dry_run=dry_run,
        openclaw_timeout_seconds=20.0,
        openclaw_agent_enabled=False,
        openclaw_agent_deliver=False,
        openclaw_agent_name="main",
        openclaw_agent_session_key="spx-spark-alerts",
        openclaw_agent_thinking="low",
        openclaw_agent_timeout_seconds=180.0,
    )


def make_payload() -> dict[str, object]:
    return {
        "as_of": "2026-07-07T03:15:00+08:00",
        "window": {"name": "close_one_hour", "priority": "high"},
        "alerts": [
            {
                "severity": "high",
                "kind": "price_move_from_close",
                "instrument_id": "equity:SPY",
                "title": "SPY up 31 bps from close",
                "detail": "SPY moved from close.",
            },
            {
                "severity": "medium",
                "kind": "iv_surface_degraded",
                "instrument_id": "iv_surface:SPXW",
                "title": "surface degraded",
                "detail": "Ignore noisy surface.",
            },
        ],
    }


def test_notifier_sends_openclaw_dry_run_and_marks_cooldown(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = make_settings(str(tmp_path / "notify-state.json"))
    now = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    result = notify_payload(make_payload(), settings=settings, runner=runner, now=now)

    assert result.enabled is True
    assert result.selected_count == 1
    assert result.sent_count == 1
    assert calls
    assert calls[0][:4] == ["openclaw", "message", "send", "--channel"]
    assert "--dry-run" in calls[0]

    second = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=now + timedelta(seconds=60),
    )

    assert second.selected_count == 0
    assert second.skipped_reason == "no_alerts_after_severity_or_cooldown"


def test_notifier_reports_missing_openclaw_target(tmp_path) -> None:
    result = notify_payload(
        make_payload(),
        settings=make_settings(str(tmp_path / "notify-state.json"), target=""),
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 0
    assert result.sinks[0].attempted is False
    assert result.sinks[0].error == "missing openclaw channel or target"
