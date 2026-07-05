from __future__ import annotations

import subprocess
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from spx_spark.config import NotificationSettings
from spx_spark.notifier import (
    build_codex_prompt,
    codex_message_requests_delivery,
    codex_message_respects_human_scope,
    notify_payload,
    run_codex_exec,
    run_openclaw_agent,
)


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
        codex_require_delivery_cue=True,
    )


def make_payload() -> dict[str, object]:
    return {
        "as_of": "2026-07-07T03:15:00+08:00",
        "window": {"name": "close_one_hour", "priority": "high"},
        "market_context": {
            "quality_summary": {"live_count": 3, "usable_count": 3, "total_count": 25},
            "derived": {"vix1d_vix9d": 0.9, "qqq_spy": 0.97},
            "entries": [
                {"instrument_id": "index:VIX", "quality": "live", "price": 18.0},
                {"instrument_id": "equity:QQQ", "quality": "live", "price": 725.0},
            ],
        },
        "human_focus_context": {
            "visible_scope": ("SPX", "SPXW", "ES"),
            "prices": {
                "spx": {"instrument_id": "index:SPX", "quality": "live", "price": 7500.0},
                "es": {"instrument_id": "future:ES", "quality": "live", "price": 7508.0},
            },
            "spxw_options": {
                "underlier_price": 7500.0,
                "expiries": [
                    {
                        "expiry": "20260707",
                        "put_wall": 7450.0,
                        "call_wall": 7550.0,
                        "zero_gamma": 7505.0,
                        "gamma_state": "positive_gamma_pin",
                    }
                ],
            },
            "spxw_iv_surface": {
                "history_1h": {
                    "snapshot_count": 12,
                    "expiries": [
                        {
                            "expiry": "20260707",
                            "atm_iv_change_1h": 0.04,
                            "put_skew_change_1h": 0.08,
                        }
                    ],
                }
            },
            "micopedia": {
                "regime": "ordinary_rth",
                "confidence": "high_observational",
                "suggested_sampling_mode": "human_alert",
            },
        },
        "alerts": [
            {
                "severity": "high",
                "kind": "price_move_from_close",
                "instrument_id": "index:SPX",
                "title": "SPX up 31 bps from close",
                "detail": "SPX moved from close.",
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

    assert result.sent_count == 1
    assert [sink.sink for sink in result.sinks] == ["codex_exec", "openclaw_message"]
    assert calls[0][:2] == ["codex", "exec"]
    assert calls[1][:3] == ["openclaw", "message", "send"]
    assert "需要看盘: SPX alert confirmed" in calls[1]


def test_codex_delivery_gate_blocks_negative_confirmation(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["codex", "exec"]:
            output_path = command[command.index("--output-last-message") + 1]
            with open(output_path, "w", encoding="utf-8") as handle:
                handle.write("不需要推送: 只是测试链路")
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

    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["codex_exec", "codex_delivery_gate"]
    assert len(calls) == 1


def test_codex_scope_gate_blocks_non_focus_symbols(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["codex", "exec"]:
            output_path = command[command.index("--output-last-message") + 1]
            with open(output_path, "w", encoding="utf-8") as handle:
                handle.write("需要看盘: SPX alert confirmed, but QQQ context also moved")
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

    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["codex_exec", "codex_scope_gate"]
    assert len(calls) == 1


def test_codex_message_requests_delivery_uses_explicit_cues() -> None:
    assert codex_message_requests_delivery("需要看盘: VIX and SPX alert confirmed")
    assert not codex_message_requests_delivery("不需要推送: degraded smoke test")
    assert not codex_message_requests_delivery("结论: critical alert, but no explicit delivery cue")


def test_codex_message_respects_human_scope_blocks_non_focus_context() -> None:
    assert codex_message_respects_human_scope("需要看盘: SPX near SPXW call wall; ES confirms.")
    assert not codex_message_respects_human_scope("需要看盘: SPX setup with VIX context.")
    assert not codex_message_respects_human_scope("需要看盘: SPX setup with Hyperliquid context.")


def test_notifier_filters_non_spx_context_alerts_from_human_push(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "price_move_from_close",
            "instrument_id": "equity:QQQ",
            "title": "QQQ up 40 bps from close",
            "detail": "Hidden algorithm context only.",
        }
    ]

    result = notify_payload(
        payload,
        settings=make_settings(str(tmp_path / "notify-state.json")),
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 0
    assert result.sent_count == 0
    assert calls == []


def test_notifier_filters_research_only_alerts_from_human_push(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "hyperliquid_proxy_quality_gate",
            "instrument_id": "index:SPX",
            "title": "Research-only proxy gate",
            "detail": "Not a human trading alert.",
            "research_only": True,
            "source_gate": "hyperliquid_spx_proxy",
        }
    ]

    result = notify_payload(
        payload,
        settings=make_settings(str(tmp_path / "notify-state.json")),
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 0
    assert result.sent_count == 0
    assert calls == []


def test_notifier_filters_smart_wallet_alerts_from_human_push(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "critical",
            "kind": "smart_wallet_spx_flow",
            "instrument_id": "index:SPX",
            "title": "Smart wallet SPX flow",
            "detail": "Research-only on-chain cohort signal.",
            "source_gate": "onchain_smart_money",
        }
    ]

    result = notify_payload(
        payload,
        settings=make_settings(str(tmp_path / "notify-state.json")),
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 0
    assert result.sent_count == 0
    assert calls == []


def test_notifier_allows_broker_unavailable_proxy_watch(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "broker_unavailable_proxy_watch",
            "instrument_id": "index:SPX",
            "title": "SPX fallback monitor down 35 bps",
            "detail": "Broker feed unavailable; open trading device and verify SPX/SPXW.",
            "quality": "degraded",
            "source_gate": "broker_unavailable_fallback",
        }
    ]

    result = notify_payload(
        payload,
        settings=make_settings(str(tmp_path / "notify-state.json")),
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 1
    assert calls


def test_notifier_allows_ibkr_session_state_events(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "ibkr_session_interrupted",
            "instrument_id": "index:SPX",
            "title": "IBKR market-data session interrupted",
            "detail": "IBKR data session is unavailable because another session owns market data.",
            "quality": "competing_session",
            "source_gate": "ibkr_session_state",
        }
    ]

    result = notify_payload(
        payload,
        settings=make_settings(str(tmp_path / "notify-state.json")),
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 1
    assert calls


def test_notifier_sends_system_events_even_when_raw_openclaw_sink_is_disabled(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "ibkr_session_restored",
            "instrument_id": "index:SPX",
            "title": "IBKR market-data session restored",
            "detail": "IBKR data session is available again.",
            "quality": "available",
            "source_gate": "ibkr_session_state",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        codex_enabled=False,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 1
    assert calls


def test_codex_prompt_hides_non_focus_market_context() -> None:
    prompt = build_codex_prompt(make_payload(), [make_payload()["alerts"][0]])

    assert "human_focus_context" in prompt
    assert "equity:QQQ" not in prompt
    assert "index:VIX" not in prompt
    assert "qqq_spy" not in prompt
    assert "SPXW" in prompt
    assert "future:ES" in prompt
    assert "ibkr_session_state" in prompt
