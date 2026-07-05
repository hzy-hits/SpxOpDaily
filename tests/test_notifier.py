from __future__ import annotations

import subprocess
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from spx_spark.config import NotificationSettings
from spx_spark.notifier import notify_payload, run_codex_exec, run_openclaw_agent


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
        openclaw_agent_model="gpt-5.3-codex-spark",
        openclaw_agent_session_key="spx-spark-alerts",
        openclaw_agent_thinking="high",
        openclaw_agent_timeout_seconds=180.0,
        codex_enabled=False,
        codex_deliver=True,
        codex_command="codex",
        codex_model="gpt-5.3-codex-spark",
        codex_reasoning_effort="high",
        codex_cwd="/home/ubuntu/spx-spark",
        codex_sandbox="read-only",
        codex_timeout_seconds=90.0,
        codex_output_max_chars=1800,
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
        settings=replace(
            make_settings(str(tmp_path / "notify-state.json"), target=""),
            openclaw_channel="telegram",
        ),
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 0
    assert result.sinks[0].attempted is False
    assert result.sinks[0].error == "missing openclaw channel or target"


def test_notifier_auto_resolves_default_weixin_target(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / "openclaw-state"
    account_id = "account-im-bot"
    account_dir = state_dir / "openclaw-weixin" / "accounts"
    account_dir.mkdir(parents=True)
    (state_dir / "openclaw-weixin" / "accounts.json").write_text(
        f'["{account_id}"]',
        encoding="utf-8",
    )
    (account_dir / f"{account_id}.json").write_text(
        '{"userId":"user@im.wechat"}',
        encoding="utf-8",
    )
    monkeypatch.setenv("OPENCLAW_STATE_DIR", str(state_dir))
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = make_settings(str(tmp_path / "notify-state.json"), target="")

    result = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 1
    command = calls[0]
    assert command[command.index("--account") + 1] == account_id
    assert command[command.index("--target") + 1] == "user@im.wechat"


def test_openclaw_agent_uses_configured_model_and_thinking(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = make_settings(str(tmp_path / "notify-state.json"))

    result = run_openclaw_agent(settings, "analyze this alert", runner=runner)

    assert result.ok is True
    command = calls[0]
    assert "--model" in command
    assert command[command.index("--model") + 1] == "gpt-5.3-codex-spark"
    assert "--thinking" in command
    assert command[command.index("--thinking") + 1] == "high"


def test_codex_exec_uses_local_codex_model_and_reasoning(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        output_path = command[command.index("--output-last-message") + 1]
        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write("需要看盘: test confirmation")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    settings = make_settings(str(tmp_path / "notify-state.json"))

    result, message = run_codex_exec(settings, "confirm this alert", runner=runner)

    assert result.ok is True
    assert message == "需要看盘: test confirmation"
    command = calls[0]
    assert command[:3] == ["codex", "exec", "-m"]
    assert command[3] == "gpt-5.3-codex-spark"
    assert 'model_reasoning_effort="high"' in command
    assert "--sandbox" in command
    assert command[command.index("--sandbox") + 1] == "read-only"


def test_notifier_can_use_codex_then_deliver_via_openclaw(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["codex", "exec"]:
            output_path = command[command.index("--output-last-message") + 1]
            with open(output_path, "w", encoding="utf-8") as handle:
                handle.write("需要看盘: SPX alert confirmed")
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        codex_enabled=True,
        openclaw_enabled=False,
    )

    result = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 2
    assert [sink.sink for sink in result.sinks] == ["codex_exec", "openclaw_message"]
    assert calls[0][:2] == ["codex", "exec"]
    assert calls[1][:3] == ["openclaw", "message", "send"]
    assert "需要看盘: SPX alert confirmed" in calls[1]
