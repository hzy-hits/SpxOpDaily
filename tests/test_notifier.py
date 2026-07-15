from __future__ import annotations

import json
import sqlite3
import subprocess

import pytest
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from spx_spark.config import NotificationSettings
from spx_spark.data_platform.telemetry import clear_telemetry_cache
from spx_spark.notifier import (
    alert_key,
    build_codex_prompt,
    codex_message_requests_delivery,
    codex_message_respects_human_scope,
    notify_payload,
    openclaw_delivery_error,
    run_codex_exec,
    run_grok_agent,
    run_openclaw_agent,
    select_alerts_for_notification,
    send_bark_message,
    SinkResult,
)
from spx_spark.notifier.state import load_acknowledged_event_ids, mark_alerts_sent


def make_settings(
    state_path: str,
    *,
    enabled: bool = True,
    dry_run: bool = True,
) -> NotificationSettings:
    return NotificationSettings(
        enabled=enabled,
        min_severity="high",
        cooldown_seconds=300,
        state_path=state_path,
        openclaw_enabled=False,
        openclaw_command="openclaw",
        openclaw_channel="",
        openclaw_account="",
        openclaw_target="",
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
        deepseek_enabled=True,
        deepseek_deliver=True,
        feishu_enabled=True,
        feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
        feishu_secret="",
        feishu_timeout_seconds=10.0,
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
                "kind": "required_data_degraded",
                "instrument_id": "index:SPX",
                "title": "SPX required data degraded",
                "detail": "Required context is degraded and retained for audit.",
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


@pytest.fixture(autouse=True)
def _stub_delivery_and_reviewer(monkeypatch):
    """Feishu/Bark delivery + DeepSeek reviewer stubs; OpenClaw Weixin is out of the fan-out."""
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda url, payload, timeout: {"code": 0, "msg": "success"},
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_bark",
        lambda url, payload, timeout: {"code": 200},
    )

    def fake_deepseek(settings, prompt: str):
        return (
            SinkResult(sink="deepseek_reviewer", attempted=True, ok=True),
            "需要看盘: SPX alert confirmed",
        )

    monkeypatch.setattr("spx_spark.notifier.pipeline.run_deepseek_reviewer", fake_deepseek)


def test_notifier_consumes_raw_observation_without_delivery_and_marks_cooldown(tmp_path) -> None:
    settings = make_settings(str(tmp_path / "notify-state.json"))
    now = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    result = notify_payload(make_payload(), settings=settings, now=now)

    assert result.enabled is True
    assert result.selected_count == 1
    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["context_policy"]
    assert result.sinks[0].verdict == "consumed"

    second = notify_payload(
        make_payload(),
        settings=settings,
        now=now + timedelta(seconds=60),
    )

    assert second.selected_count == 0
    assert second.skipped_reason == "no_alerts_after_severity_or_cooldown"


def test_notifier_shadow_records_context_consumption(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "market-data"
    ledger_path = data_root / "runtime" / "research-ledger.sqlite3"
    monkeypatch.setenv("DATA_PLATFORM_ENABLED", "true")
    monkeypatch.setenv("MARKET_DATA_DATA_ROOT", str(data_root))
    monkeypatch.setenv("DATA_PLATFORM_LEDGER_PATH", str(ledger_path))
    clear_telemetry_cache()
    try:
        result = notify_payload(
            make_payload(),
            settings=make_settings(str(tmp_path / "notify-state.json")),
            now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
        )
    finally:
        clear_telemetry_cache()

    assert result.sent_count == 0
    connection = sqlite3.connect(ledger_path)
    try:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM decisions").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM alert_deliveries").fetchone()[0] >= 1
        assert connection.execute(
            "SELECT count(*) FROM alert_deliveries WHERE status='sent'"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT count(*) FROM alert_deliveries WHERE channel='context_policy'"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT status, action, side FROM decisions"
        ).fetchone() == ("context", "observe", "none")
        assert connection.execute(
            "SELECT status FROM alert_deliveries WHERE channel='context_policy'"
        ).fetchone()[0] == "consumed"
    finally:
        connection.close()


def test_context_observation_never_attempts_feishu_and_is_consumed(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda url, payload, timeout: {"code": 19001, "msg": "webhook failed"},
    )
    result = notify_payload(
        make_payload(),
        settings=make_settings(str(tmp_path / "notify-state.json")),
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["context_policy"]
    assert (tmp_path / "notify-state.json").exists()


def test_openclaw_delivery_error_accepts_dry_run_payload() -> None:
    assert (
        openclaw_delivery_error(
            '{"action":"send","channel":"openclaw-weixin","dryRun":true,"handledBy":"core"}'
        )
        is None
    )


def test_send_openclaw_message_auto_resolves_default_weixin_target(tmp_path, monkeypatch) -> None:
    """OpenClaw message helper still resolves Weixin targets; deliver_trade_push no longer calls it."""
    from spx_spark.notifier.sinks import send_openclaw_message

    state_dir = tmp_path / "openclaw-state"
    account_dir = state_dir / "openclaw-weixin" / "accounts"
    account_dir.mkdir(parents=True)
    (state_dir / "openclaw-weixin" / "accounts.json").write_text(
        '["account-im-bot"]',
        encoding="utf-8",
    )
    (account_dir / "account-im-bot.json").write_text(
        '{"userId":"user@im.wechat"}',
        encoding="utf-8",
    )
    monkeypatch.setenv("OPENCLAW_STATE_DIR", str(state_dir))

    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_channel="openclaw-weixin",
        openclaw_account="",
        openclaw_target="",
        openclaw_dry_run=False,
        feishu_enabled=False,
        deepseek_enabled=False,
    )
    result = send_openclaw_message(settings, "hello", runner=runner)
    assert result.ok is True
    command = calls[0]
    assert command[command.index("--target") + 1] == "user@im.wechat"


def test_openclaw_agent_uses_configured_model_and_thinking(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = make_settings(str(tmp_path / "notify-state.json"))

    result, message = run_openclaw_agent(settings, "analyze this alert", runner=runner)

    assert result.ok is True
    assert message == "{}"
    command = calls[0]
    assert "--model" in command
    assert command[command.index("--model") + 1] == "gpt-5.3-codex-spark"
    assert "--thinking" in command
    assert command[command.index("--thinking") + 1] == "high"
    assert "--deliver" not in command


def test_grok_agent_uses_configured_model_and_read_only_mode(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="需要看盘: 突破确认", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        grok_enabled=True,
        grok_model="grok-4.5",
        grok_reasoning_effort="high",
        grok_cwd="/home/ubuntu/spx-spark",
    )

    result, message = run_grok_agent(settings, "analyze this alert", runner=runner)

    assert result.ok is True
    assert message == "需要看盘: 突破确认"
    command = calls[0]
    assert command[command.index("--model") + 1] == "grok-4.5"
    assert command[command.index("--reasoning-effort") + 1] == "high"
    assert command[command.index("--permission-mode") + 1] == "plan"
    assert "--no-subagents" in command
    assert "--disable-web-search" in command
    assert "--verbatim" in command
    assert command[command.index("--max-turns") + 1] == "1"


def test_raw_observation_does_not_run_openclaw_agent(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["openclaw", "agent"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout='{"text":"需要看盘: SPX alert confirmed"}',
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
    )

    result = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["context_policy"]
    assert result.outcome == "consumed"
    assert calls == []


def test_raw_observation_bypasses_all_llm_reviewers(tmp_path, monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_deepseek(settings: NotificationSettings, prompt: str) -> tuple[SinkResult, str]:
        assert "human_focus_context" in prompt
        return (
            SinkResult(sink="deepseek_reviewer", attempted=True, ok=True),
            "需要看盘: SPX alert confirmed",
        )

    monkeypatch.setattr("spx_spark.notifier.pipeline.run_deepseek_reviewer", fake_deepseek)

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        deepseek_enabled=True,
        deepseek_deliver=True,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=True,
    )

    result = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["context_policy"]
    assert calls == []


def test_notifier_consumes_data_quality_alert_without_llm(tmp_path, monkeypatch) -> None:
    def fail_deepseek(settings: NotificationSettings, prompt: str) -> tuple[SinkResult, str]:
        raise AssertionError("weak alert should not enter LLM review")

    monkeypatch.setattr("spx_spark.notifier.pipeline.run_deepseek_reviewer", fail_deepseek)

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "required_data_degraded",
            "instrument_id": "index:SPX",
            "title": "SPX data degraded",
            "detail": "Operational data issue.",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        deepseek_enabled=True,
        openclaw_agent_enabled=False,
        codex_enabled=False,
    )
    now = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    first = notify_payload(payload, settings=settings, now=now)

    assert first.selected_count == 1
    assert first.sent_count == 0
    assert [sink.sink for sink in first.sinks] == ["context_policy"]

    second = notify_payload(
        payload,
        settings=settings,
        now=now + timedelta(seconds=60),
    )
    assert second.selected_count == 0


def test_notifier_keeps_high_iv_pending_when_reviewer_is_rate_limited(
    tmp_path,
    monkeypatch,
) -> None:
    attempts = 0

    def fake_deepseek(settings: NotificationSettings, prompt: str) -> tuple[SinkResult, str]:
        nonlocal attempts
        attempts += 1
        return (
            SinkResult(
                sink="deepseek_reviewer",
                attempted=True,
                ok=False,
                exit_code=429,
                error="429 usage limit reached",
            ),
            "",
        )

    monkeypatch.setattr("spx_spark.notifier.pipeline.run_deepseek_reviewer", fake_deepseek)
    audit_path = tmp_path / "review-audit.jsonl"
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        deepseek_enabled=True,
        openclaw_agent_enabled=False,
        codex_enabled=False,
        review_audit_path=str(audit_path),
    )
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "iv_term_gap",
            "instrument_id": "iv_surface:SPXW",
            "title": "0DTE vs next ATM IV gap 0.051",
            "detail": "Front SPXW ATM IV differs from next expiry.",
            "source_gate": "iv_surface",
        }
    ]
    now = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    first = notify_payload(
        payload,
        settings=settings,
        now=now,
    )

    assert first.selected_count == 1
    assert first.sent_count == 0
    assert [sink.sink for sink in first.sinks] == ["deepseek_reviewer"]

    second = notify_payload(
        payload,
        settings=settings,
        now=now + timedelta(seconds=60),
    )
    assert second.selected_count == 1
    assert second.sent_count == 0
    assert attempts == 2
    entries = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert [entry["outcome"] for entry in entries] == [
        "review_failed_pending",
        "review_failed_pending",
    ]
    assert all(entry["candidates"][0]["kind"] == "iv_term_gap" for entry in entries)


def test_send_bark_message_posts_title_body_and_group(tmp_path) -> None:
    posts: list[tuple[str, dict[str, object], float]] = []

    def poster(url: str, payload: dict[str, object], timeout: float) -> dict[str, object]:
        posts.append((url, payload, timeout))
        return {"code": 200}

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
    )

    result = send_bark_message(settings, "SPX Spark HIGH", "需要看盘: test", poster=poster)

    assert result.ok is True
    url, payload, timeout = posts[0]
    assert url == "https://api.day.app/test-key"
    assert payload["title"] == "SPX Spark HIGH"
    assert payload["body"] == "需要看盘: test"
    assert payload["group"] == "spx-spark"
    assert payload["level"] == "timeSensitive"


def test_send_bark_message_reports_non_200_as_error(tmp_path) -> None:
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
    )

    result = send_bark_message(
        settings,
        "t",
        "b",
        poster=lambda *_: {"code": 400, "message": "device key invalid"},
    )

    assert result.ok is False
    assert "400" in (result.error or "")


def test_raw_observation_never_reaches_bark(tmp_path, monkeypatch) -> None:
    bark_posts: list[dict[str, object]] = []

    def fake_post_bark(url: str, payload: dict[str, object], timeout: float) -> dict[str, object]:
        bark_posts.append(payload)
        return {"code": 200}

    monkeypatch.setattr("spx_spark.notifier.sinks.post_bark", fake_post_bark)

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["openclaw", "agent"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout='{"text":"需要看盘: SPX alert confirmed"}',
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
    )

    result = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert [sink.sink for sink in result.sinks] == ["context_policy"]
    assert result.outcome == "consumed"
    assert result.sent_count == 0
    assert bark_posts == []


def test_alerts_are_market_signals_rejects_mixed_and_empty_batches() -> None:
    from spx_spark.notifier import alerts_are_market_signals

    market = {"kind": "option_gamma_regime"}
    ops = {"kind": "required_data_degraded"}
    assert alerts_are_market_signals([market]) is True
    assert alerts_are_market_signals([market, ops]) is False
    assert alerts_are_market_signals([]) is False


def test_raw_market_signal_never_reaches_friend_bark(tmp_path, monkeypatch) -> None:
    posts: list[tuple[str, dict[str, object]]] = []

    def fake_post_bark(url: str, payload: dict[str, object], timeout: float) -> dict[str, object]:
        posts.append((url, payload))
        return {"code": 200}

    monkeypatch.setattr("spx_spark.notifier.sinks.post_bark", fake_post_bark)

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["openclaw", "agent"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout='{"text":"需要看盘: SPX alert confirmed"}',
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/user-key",
        bark_friend_enabled=True,
        bark_friend_url="https://api.day.app/friend-key",
    )

    result = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert [sink.sink for sink in result.sinks] == ["context_policy"]
    assert posts == []


def test_system_event_alert_never_reaches_friend_bark(tmp_path, monkeypatch) -> None:
    posts: list[tuple[str, dict[str, object]]] = []

    def fake_post_bark(url: str, payload: dict[str, object], timeout: float) -> dict[str, object]:
        posts.append((url, payload))
        return {"code": 200}

    monkeypatch.setattr("spx_spark.notifier.sinks.post_bark", fake_post_bark)

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        bark_enabled=True,
        bark_url="https://api.day.app/user-key",
        bark_friend_enabled=True,
        bark_friend_url="https://api.day.app/friend-key",
    )

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "ibkr_session_interrupted",
            "instrument_id": "index:SPX",
            "title": "IBKR session interrupted",
            "detail": "Session competing login detected.",
        }
    ]

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count >= 1
    assert all(not url.endswith("friend-key") for url, _ in posts)


def test_context_consumption_starts_cooldown_without_bark(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_bark",
        lambda *_: {"code": 200},
    )

    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["openclaw", "agent"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout='{"text":"需要看盘: SPX alert confirmed"}',
                stderr="",
            )
        # Weixin delivery fails (expired contextToken).
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="ret=-2")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
        feishu_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
    )

    first = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )
    assert first.sent_count == 0
    assert [sink.sink for sink in first.sinks] == ["context_policy"]
    assert calls == []

    second = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 2, tzinfo=timezone.utc),
    )
    assert second.selected_count == 0


def test_context_policy_does_not_need_agent_verdict_and_starts_cooldown(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["openclaw", "agent"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout='{"text":"不需要推送: 数据降级，仅记录。"}',
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
        feishu_enabled=False,
    )

    first = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert first.sent_count == 0
    assert [sink.sink for sink in first.sinks] == ["context_policy"]
    assert calls == []

    # Same alerts again shortly after: the rejected bucket is in cooldown, so
    # the agent must not be re-run.
    second = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 2, tzinfo=timezone.utc),
    )

    assert second.selected_count == 0
    assert calls == []


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


def test_raw_observation_does_not_use_codex_or_feishu(tmp_path) -> None:
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
        deepseek_enabled=False,
        codex_enabled=True,
    )

    result = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["context_policy"]
    assert calls == []


def test_context_policy_precedes_codex_delivery_gate(tmp_path) -> None:
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
        deepseek_enabled=False,
        codex_enabled=True,
    )

    result = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["context_policy"]
    gate = result.sinks[-1]
    assert gate.verdict == "consumed"
    assert gate.alert_keys == result.selected_alert_keys
    assert calls == []


def test_context_policy_precedes_codex_scope_gate(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["codex", "exec"]:
            output_path = command[command.index("--output-last-message") + 1]
            with open(output_path, "w", encoding="utf-8") as handle:
                handle.write("需要看盘: SPX alert confirmed, but Hyperliquid proxy also moved")
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        codex_enabled=True,
    )
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "iv_term_gap",
            "instrument_id": "iv_surface:SPXW",
            "title": "0DTE vs next ATM IV gap 0.051",
            "detail": "Front SPXW ATM IV differs from next expiry.",
            "source_gate": "iv_surface",
        }
    ]

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["codex_exec", "codex_scope_gate"]
    assert result.outcome == "pending"
    assert calls and calls[0][:2] == ["codex", "exec"]


def test_codex_message_requests_delivery_uses_explicit_cues() -> None:
    assert codex_message_requests_delivery("需要看盘: VIX and SPX alert confirmed")
    assert codex_message_requests_delivery(
        "需要看盘: SPX 已收复 flip\n若重新跌破，否则不需要推送。"
    )
    assert not codex_message_requests_delivery("不需要推送: degraded smoke test")
    assert not codex_message_requests_delivery("不需要推送: 当前没有增量\n需要看盘时再通知。")
    assert not codex_message_requests_delivery("结论: critical alert, but no explicit delivery cue")


def test_delivery_protocol_cue_is_removed_from_human_message() -> None:
    from spx_spark.notifier.policy import (
        codex_message_respects_desk_style,
        strip_delivery_protocol_cue,
    )

    assert (
        strip_delivery_protocol_cue(
            "需要看盘:\n**SPX 0DTE | TACTICAL UPDATE**\n## Desk View\nCall bias."
        )
        == "**SPX 0DTE | TACTICAL UPDATE**\n## Desk View\nCall bias."
    )
    assert strip_delivery_protocol_cue("需要看盘: SPX reclaimed 7535") == "SPX reclaimed 7535"
    valid = "\n".join(
        (
            "需要看盘:",
            "**SPX 0DTE | TACTICAL UPDATE**",
            "## Desk View",
            "Call bias restored.",
            "## Execution",
            "7533-7535.",
            "## Risk",
            "Below 7530.",
        )
    )
    assert codex_message_respects_desk_style(valid)
    assert not codex_message_respects_desk_style("需要看盘: 半路不追")


def test_review_audit_records_context_consumption_without_calling_reviewer(
    tmp_path, monkeypatch
) -> None:
    raw_reply = "需要看盘: SPX 已收复关键位\n若重新跌破，否则不需要推送。 token=super-secret-value"

    def fake_deepseek(settings: NotificationSettings, prompt: str) -> tuple[SinkResult, str]:
        return SinkResult(sink="deepseek_reviewer", attempted=True, ok=True), raw_reply

    monkeypatch.setattr("spx_spark.notifier.pipeline.run_deepseek_reviewer", fake_deepseek)
    audit_path = tmp_path / "review-audit.jsonl"
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        review_audit_path=str(audit_path),
    )

    result = notify_payload(
        make_payload(),
        settings=settings,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 0
    entries = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["parser_verdict"] == "not_run"
    assert entry["outcome"] == "context_only_consumed"
    assert entry["candidate_count"] == 1
    assert entry["candidates"][0]["kind"] == "required_data_degraded"
    assert entry["raw_reply"] == ""
    assert "super-secret-value" not in audit_path.read_text(encoding="utf-8")


def test_high_iv_alert_remains_pending_after_invalid_reviewer_output(tmp_path, monkeypatch) -> None:
    attempts = 0

    def fake_deepseek(settings: NotificationSettings, prompt: str) -> tuple[SinkResult, str]:
        nonlocal attempts
        attempts += 1
        return (
            SinkResult(sink="deepseek_reviewer", attempted=True, ok=True),
            "结论: IV term gap changed, but the delivery cue is missing.",
        )

    monkeypatch.setattr("spx_spark.notifier.pipeline.run_deepseek_reviewer", fake_deepseek)
    audit_path = tmp_path / "review-audit.jsonl"
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_agent_enabled=False,
        codex_enabled=False,
        review_audit_path=str(audit_path),
    )
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "iv_term_gap",
            "instrument_id": "iv_surface:SPXW",
            "title": "0DTE vs next ATM IV gap 0.051",
            "detail": "Front SPXW ATM IV differs from next expiry.",
            "source_gate": "iv_surface",
        }
    ]
    now = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    first = notify_payload(payload, settings=settings, now=now)
    second = notify_payload(payload, settings=settings, now=now + timedelta(seconds=60))

    assert first.sent_count == 0
    assert second.selected_count == 1
    assert attempts == 2
    entries = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert [entry["parser_verdict"] for entry in entries] == ["invalid", "invalid"]
    assert all(entry["outcome"] == "invalid_parser_pending" for entry in entries)


def test_high_price_alert_is_consumed_before_reviewer_parser(tmp_path, monkeypatch) -> None:
    def fake_deepseek(settings: NotificationSettings, prompt: str) -> tuple[SinkResult, str]:
        return (
            SinkResult(sink="deepseek_reviewer", attempted=True, ok=True),
            "结论: price shock confirmed, but the delivery cue is missing.",
        )

    monkeypatch.setattr("spx_spark.notifier.pipeline.run_deepseek_reviewer", fake_deepseek)
    audit_path = tmp_path / "review-audit.jsonl"
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_agent_enabled=False,
        codex_enabled=False,
        review_audit_path=str(audit_path),
    )
    now = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    result = notify_payload(make_payload(), settings=settings, now=now)

    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["context_policy"]
    assert not any(sink.sink == "feishu" and sink.ok for sink in result.sinks)
    entry = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[-1])
    assert entry["parser_verdict"] == "not_run"
    assert entry["outcome"] == "context_only_consumed"


def test_codex_message_respects_human_scope_blocks_non_focus_context() -> None:
    assert codex_message_respects_human_scope("需要看盘: SPX near SPXW call wall; ES confirms.")
    assert codex_message_respects_human_scope("需要看盘: SPX setup with VIX context.")
    assert codex_message_respects_human_scope(
        "需要看盘: gamma transition, VIX1D 18 -> 21, SKEW rising."
    )
    assert codex_message_respects_human_scope("需要看盘: SPX setup, SPY/QQQ confirm the move.")
    assert not codex_message_respects_human_scope("需要看盘: SPX setup with Hyperliquid context.")
    assert not codex_message_respects_human_scope("需要看盘: Polymarket odds shifted.")


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


def test_notifier_filters_unanchored_proxy_watch(tmp_path) -> None:
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
            "research_only": True,
            "source_gate": "hyperliquid_proxy_unanchored",
        }
    ]

    settings = make_settings(str(tmp_path / "notify-state.json"))
    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 0
    assert result.sent_count == 0
    assert calls == []


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

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        feishu_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
    )
    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 1
    assert any(s.sink == "bark" and s.ok for s in result.sinks)


def test_provider_source_switches_are_direct_state_transition_pushes() -> None:
    from spx_spark.notifier import context_only_alerts, direct_push_alerts

    fallback = {
        "severity": "high",
        "kind": "market_data_ibkr_fallback_activated",
        "instrument_id": "index:SPX",
    }
    restored = {
        "severity": "high",
        "kind": "market_data_schwab_restored",
        "instrument_id": "index:SPX",
    }
    unavailable = {
        "severity": "critical",
        "kind": "market_data_all_providers_unavailable",
        "instrument_id": "index:SPX",
    }

    assert direct_push_alerts([fallback, restored]) == [fallback, restored]
    assert context_only_alerts([fallback, restored]) == []
    assert direct_push_alerts([unavailable]) == [unavailable]


@pytest.mark.parametrize(
    "kind",
    (
        "ibkr_session_login",
        "broker_unavailable_proxy_watch",
        "required_data_missing",
        "required_data_degraded",
        "optional_data_missing",
        "optional_data_degraded",
        "option_quote_freshness_degraded",
        "iv_surface_degraded",
        "iv_surface_stale",
    ),
)
def test_raw_observation_kinds_are_consumed_without_llm_or_delivery(
    kind: str,
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "spx_spark.notifier.pipeline.run_deepseek_reviewer",
        lambda *_: (_ for _ in ()).throw(AssertionError("context must not reach LLM")),
    )
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "critical",
            "kind": kind,
            "instrument_id": "index:SPX",
            "title": f"raw {kind}",
            "detail": "context only",
            "dedup_group": "raw:1",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
    )
    now = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    first = notify_payload(payload, settings=settings, now=now)
    second = notify_payload(payload, settings=settings, now=now + timedelta(seconds=60))

    assert first.sent_count == 0
    assert [sink.sink for sink in first.sinks] == ["context_policy"]
    assert first.sinks[0].verdict == "consumed"
    assert second.selected_count == 0


@pytest.mark.parametrize(
    "kind",
    (
        "intraday_price_shock",
        "intraday_price_reclaim",
        "globex_trend_transition",
        "flip_reclaim_call",
        "call_wall_breakout_call",
        "market_data_ibkr_fallback_activated",
        "market_data_all_providers_unavailable",
        "market_data_schwab_restored",
        "ibkr_session_interrupted",
        "ibkr_session_restored",
    ),
)
def test_deterministic_transition_kinds_deliver_without_reviewer(
    kind: str,
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "spx_spark.notifier.pipeline.run_deepseek_reviewer",
        lambda *_: (_ for _ in ()).throw(AssertionError("direct event must not reach reviewer")),
    )
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "critical",
            "kind": kind,
            "instrument_id": "index:SPX",
            "title": kind,
            "detail": "deterministic transition",
            "dedup_group": f"{kind}:1",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        direct_push_llm_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
    )

    result = notify_payload(
        payload,
        settings=settings,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count >= 1
    assert result.outcome == "delivered"
    assert not any(sink.sink == "deepseek_reviewer" for sink in result.sinks)


@pytest.mark.parametrize(
    "kind",
    (
        "price_move_from_close",
        "option_gamma_regime",
        "option_wall_proximity",
        "iv_term_gap",
        "atm_iv_jump_5m",
        "put_skew_steepening_5m",
        "iv_surface_shift_5m",
        "iv_surface_shift_1h",
        "atm_iv_change_1h",
    ),
)
def test_market_observations_reach_reviewer_lane(kind: str, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "spx_spark.notifier.pipeline.run_deepseek_reviewer",
        lambda *_: (
            SinkResult(sink="deepseek_reviewer", attempted=True, ok=True),
            "需要看盘\n结构变化需要关注。",
        ),
    )
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": kind,
            "instrument_id": "index:SPX",
            "title": kind,
            "detail": "review candidate",
            "dedup_group": f"{kind}:1",
        }
    ]
    result = notify_payload(
        payload,
        settings=make_settings(str(tmp_path / "notify-state.json")),
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count >= 1
    assert result.outcome == "delivered"
    assert any(sink.sink == "deepseek_reviewer" for sink in result.sinks)


def test_notifier_delivers_session_restore_without_reviewer(tmp_path) -> None:
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
        deepseek_enabled=False,
        feishu_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
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
    assert [sink.sink for sink in result.sinks] == ["bark"]


def test_notifier_sends_position_holding_alerts_without_codex(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "spxw_position_book_pnl",
            "instrument_id": "option_map:SPXW",
            "title": "SPXW 浮盈浮亏 $-438 (-11.7%)",
            "detail": "book loss beyond $-400\nSPX 7483",
            "quality": "live",
            "source_gate": "ibkr_positions",
            "event_id": "position-event-1",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
        feishu_enabled=True,
        feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
        codex_enabled=True,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 2  # feishu + bark
    assert any(s.sink == "bark" and s.ok for s in result.sinks)
    assert any(s.sink == "feishu" and s.ok for s in result.sinks)
    assert "bark_friend" not in [s.sink for s in result.sinks]


def test_notifier_skips_near_expiry_position_noise(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "spxw_position_near_expiry",
            "instrument_id": "option:SPX:SPXW:20260706:7480:C",
            "title": "SPXW 20260706 7480C expires in 0d",
            "detail": "Held SPXW expires today; qty=1.",
            "quality": "live",
            "source_gate": "ibkr_positions",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        feishu_enabled=False,
        bark_enabled=False,
        codex_enabled=False,
        openclaw_agent_enabled=False,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 0


def test_notifier_routes_iv_surface_alert_to_reviewer(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "iv_term_gap",
            "instrument_id": "iv_surface:SPXW",
            "title": "0DTE vs next ATM IV gap 0.051",
            "detail": "Front SPXW ATM IV differs from next-expiry ATM IV by 0.051.",
            "quality": "live",
            "source_gate": "iv_surface",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        feishu_enabled=False,
        codex_enabled=True,
        codex_deliver=False,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["codex_exec"]
    assert result.outcome == "pending"
    assert calls and calls[0][:2] == ["codex", "exec"]


def test_offhours_skew_is_captured_below_floor_and_reaches_reviewer(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["window"] = {
        "name": "quiet_futures_context",
        "priority": "high",
        "spxw_sampling_mode": "off",
    }
    payload["alerts"] = [
        {
            "severity": "low",
            "kind": "put_skew_steepening_5m",
            "instrument_id": "iv_surface:SPXW:20260707",
            "title": "SPXW 20260707 put skew steepening 0.031",
            "detail": "Put 25-delta skew widened 0.031 vol points.",
            "quality": "live",
            "source_gate": "iv_surface",
            "dedup_group": "up:1",
        }
    ]
    audit_path = tmp_path / "review-audit.jsonl"
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        codex_enabled=True,
        review_audit_path=str(audit_path),
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["codex_exec"]
    assert calls and calls[0][:2] == ["codex", "exec"]


def test_intraday_shock_delivers_without_reviewer_and_records_ack(tmp_path) -> None:
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "intraday_price_shock",
            "instrument_id": "index:SPX",
            "title": "SPX/ES confirmed 急跌 26.2 bps",
            "detail": "SPX/ES live anchors confirmed the short-window shock.",
            "quality": "live",
            "source_gate": "spx_es_intraday_shock_confirmed",
            "dedup_group": "spx_shock:20260710:down:1432:shock",
            "event_id": "spx_shock:20260710:down:1432",
        }
    ]
    audit_path = tmp_path / "shock-review-audit.jsonl"
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=True,
        direct_push_llm_enabled=False,
        review_audit_path=str(audit_path),
    )

    result = notify_payload(
        payload,
        settings=settings,
        now=datetime(2026, 7, 10, 14, 32, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 1
    assert [sink.sink for sink in result.sinks] == ["feishu"]
    assert result.acknowledged_event_ids == (
        "spx_shock:20260710:down:1432",
        "spx_shock:20260710:down:1432:shock",
    )
    assert load_acknowledged_event_ids(settings.state_path) == result.acknowledged_event_ids
    entries = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert entries[-1]["reviewer"] == "direct_policy"
    assert entries[-1]["parser_verdict"] == "not_run"
    assert entries[-1]["outcome"] == "delivered"


def test_call_path_delivers_without_reviewer_and_records_strategy_ack(tmp_path) -> None:
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "flip_reclaim_call",
            "instrument_id": "index:SPX",
            "title": "SPX 收复 flip 7500，Call 路径确认",
            "detail": "SPX/ES 两组新鲜样本确认，回踩不破才看 call。",
            "quality": "live",
            "source_gate": "spx_es_flip_reclaim_call_confirmed",
            "dedup_group": "spx_call:flip:7500:1432:strategy",
            "event_id": "spx_call:flip:7500:1432",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=True,
        direct_push_llm_enabled=False,
    )

    result = notify_payload(
        payload,
        settings=settings,
        now=datetime(2026, 7, 10, 14, 32, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 1
    assert [sink.sink for sink in result.sinks] == ["feishu"]
    assert not any(sink.sink == "deepseek_reviewer" for sink in result.sinks)
    assert result.acknowledged_event_ids == (
        "spx_call:flip:7500:1432",
        "spx_call:flip:7500:1432:strategy",
    )


def test_recent_shock_suppresses_same_direction_fixed_cycle_price_move(tmp_path) -> None:
    settings = make_settings(str(tmp_path / "notify-state.json"))
    shock_at = datetime(2026, 7, 10, 14, 32, tzinfo=timezone.utc)
    shock = {
        "severity": "high",
        "kind": "intraday_price_shock",
        "instrument_id": "index:SPX",
        "dedup_group": "spx_shock:20260710:down:1432:shock",
    }
    mark_alerts_sent([shock], {}, settings, now=shock_at)
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "price_move_from_close",
            "instrument_id": "index:SPX",
            "dedup_group": "down:3",
        }
    ]

    selected, _ = select_alerts_for_notification(
        payload,
        settings,
        now=shock_at + timedelta(minutes=10),
    )
    assert selected == []

    selected, _ = select_alerts_for_notification(
        payload,
        settings,
        now=shock_at + timedelta(minutes=16),
    )
    assert len(selected) == 1

    payload["alerts"][0]["dedup_group"] = "up:3"  # type: ignore[index]
    selected, _ = select_alerts_for_notification(
        payload,
        settings,
        now=shock_at + timedelta(minutes=10),
    )
    assert len(selected) == 1


def test_reviewed_price_move_is_suppressed_after_realtime_shock(
    tmp_path,
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 10, 14, 32, tzinfo=timezone.utc)
    audit_path = tmp_path / "review-audit.jsonl"
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        review_audit_path=str(audit_path),
    )
    shock = {
        "severity": "high",
        "kind": "intraday_price_shock",
        "instrument_id": "index:SPX",
        "dedup_group": "spx_shock:20260710:down:1432:shock",
    }

    def reviewer(review_settings, prompt):  # noqa: ARG001
        # Simulate the fast path finishing while the fixed-cycle alert waits
        # for its model review.
        mark_alerts_sent([shock], {}, review_settings, now=now)
        return (
            SinkResult(sink="deepseek_reviewer", attempted=True, ok=True),
            "需要看盘\nSPX move confirmed.",
        )

    monkeypatch.setattr("spx_spark.notifier.pipeline.run_deepseek_reviewer", reviewer)
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "price_move_from_close",
            "instrument_id": "index:SPX",
            "title": "SPX down from close",
            "detail": "same move already covered by realtime shock",
            "quality": "live",
            "dedup_group": "down:3",
        }
    ]

    result = notify_payload(payload, settings=settings, now=now)

    assert result.selected_count == 1
    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == [
        "deepseek_reviewer",
        "intraday_shock_correlation_gate",
    ]
    assert not any(sink.sink in {"feishu", "bark"} for sink in result.sinks)
    entries = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert entries[-1]["outcome"] == "correlated_shock_suppressed"


def test_rth_skew_steepening_reaches_reviewer(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["window"] = {
        "name": "close_one_hour",
        "priority": "critical",
        "spxw_sampling_mode": "human_alert",
    }
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "put_skew_steepening_5m",
            "instrument_id": "iv_surface:SPXW:20260707",
            "title": "SPXW 20260707 put skew steepening 0.031",
            "detail": "Put 25-delta skew widened 0.031 vol points.",
            "quality": "live",
            "source_gate": "iv_surface",
            "dedup_group": "up:1",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        feishu_enabled=False,
        codex_enabled=True,
        codex_deliver=False,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 0
    assert [sink.sink for sink in result.sinks] == ["codex_exec"]
    assert calls and calls[0][:2] == ["codex", "exec"]


def _skew_alert(dedup_group: str, severity: str = "high") -> dict[str, object]:
    return {
        "severity": severity,
        "kind": "put_skew_steepening_5m",
        "instrument_id": "iv_surface:SPXW:20260707",
        "title": f"SPXW put skew steepening ({dedup_group})",
        "detail": "Put skew widened.",
        "source_gate": "iv_surface",
        "dedup_group": dedup_group,
    }


def test_kind_rate_limit_caps_bucket_creep_but_allows_jumps_and_flips(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = make_settings(str(tmp_path / "notify-state.json"))
    base = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    def send(dedup_group: str, minutes: int, severity: str = "high") -> int:
        payload = make_payload()
        payload["alerts"] = [_skew_alert(dedup_group, severity)]
        result = notify_payload(
            payload,
            settings=settings,
            runner=runner,
            now=base + timedelta(minutes=minutes),
        )
        return result.selected_count

    assert send("up:1", 0) == 1
    # +10 min, bucket crept one step: rate limited.
    assert send("up:2", 10) == 0
    # +20 min, bucket jumped >= 2 steps from the last sent bucket: allowed.
    assert send("up:3", 20) == 1
    # +30 min, one more creep after the jump: limited again.
    assert send("up:4", 30) == 0


def test_kind_rate_limit_direction_flip_and_expiry(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = make_settings(str(tmp_path / "notify-state.json"))
    base = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    def send(dedup_group: str, minutes: int, severity: str = "high") -> int:
        payload = make_payload()
        payload["alerts"] = [_skew_alert(dedup_group, severity)]
        result = notify_payload(
            payload,
            settings=settings,
            runner=runner,
            now=base + timedelta(minutes=minutes),
        )
        return result.selected_count

    assert send("up:1", 0) == 1
    assert send("down:1", 10) == 1  # direction flip breaks through
    assert send("down:2", 20) == 0  # creep after flip: limited
    assert send("down:3", 75) == 1  # window expired (>1h since last sent)


def test_kind_rate_limit_exempts_critical_and_other_kinds(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = make_settings(str(tmp_path / "notify-state.json"))
    base = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    payload = make_payload()
    payload["alerts"] = [_skew_alert("up:1")]
    assert notify_payload(payload, settings=settings, runner=runner, now=base).selected_count == 1
    # Critical severity bypasses the rate limit even on a one-step creep.
    payload = make_payload()
    payload["alerts"] = [_skew_alert("up:2", severity="critical")]
    assert (
        notify_payload(
            payload, settings=settings, runner=runner, now=base + timedelta(minutes=10)
        ).selected_count
        == 1
    )
    # Non-bucketed kinds (wall proximity) are not touched by the rate limiter.
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "option_wall_proximity",
            "instrument_id": "option_map:SPXW:20260707",
            "title": "SPX near SPXW wall 7450",
            "detail": "wall proximity",
            "dedup_group": "band:7450",
        }
    ]
    assert (
        notify_payload(
            payload, settings=settings, runner=runner, now=base + timedelta(minutes=11)
        ).selected_count
        == 1
    )


def test_direct_push_rewrites_event_with_llm_writer(tmp_path, monkeypatch) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    def fake_writer(
        template: str,
        prompt: str,
        settings: NotificationSettings,
        **kwargs,
    ) -> tuple[str, str]:
        assert "即时事件" in prompt
        assert "持仓事件" in prompt
        return "【持仓事件】开仓 7430C x1，现价贴近 flip zone 下沿。", "grok_cli"

    monkeypatch.setattr("spx_spark.notifier.pipeline.generate_push_text", fake_writer)
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "spxw_position_opened",
            "instrument_id": "option:SPX:SPXW:20260707:7430:C",
            "title": "开仓 SPXW 20260707 7430C",
            "detail": "qty=1 avg=12.3",
            "quality": "live",
            "source_gate": "ibkr_positions",
            "event_id": "position-event-1",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
        direct_push_llm_enabled=True,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 2
    assert any(s.sink == "bark" and s.ok for s in result.sinks)
    assert result.acknowledged_event_ids == ("position-event-1",)
    assert result.to_dict()["acknowledged_event_ids"] == ["position-event-1"]
    assert load_acknowledged_event_ids(settings.state_path) == ("position-event-1",)


def test_position_event_is_not_acknowledged_when_all_human_sinks_fail(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda url, payload, timeout: {"code": 19001, "msg": "webhook failed"},
    )
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "spxw_position_closed",
            "instrument_id": "option:SPX:SPXW:20260707:7430:C",
            "title": "平仓 SPXW 20260707 7430C",
            "detail": "qty 1 -> 0",
            "quality": "live",
            "source_gate": "ibkr_positions",
            "event_id": "position-event-failed",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        bark_enabled=False,
    )

    result = notify_payload(
        payload,
        settings=settings,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 0
    assert result.acknowledged_event_ids == ()
    assert load_acknowledged_event_ids(settings.state_path) == ()


def test_position_event_store_corruption_is_sent_as_direct_ops_alert(tmp_path) -> None:
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "critical",
            "kind": "spxw_position_event_store_corrupt",
            "instrument_id": "option_map:SPXW",
            "title": "SPXW 持仓事件状态损坏",
            "detail": "state unreadable",
            "quality": "error",
            "source_gate": "ibkr_positions",
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
    )

    result = notify_payload(
        payload,
        settings=settings,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 1


def test_only_selected_delivered_position_event_ids_are_acknowledged(tmp_path) -> None:
    payload = make_payload()
    payload["alerts"] = [
        {
            "severity": "high",
            "kind": "spxw_position_opened",
            "instrument_id": "option:SPX:SPXW:20260707:7430:C",
            "title": "开仓 7430C",
            "detail": "qty=1",
            "quality": "live",
            "source_gate": "ibkr_positions",
            "event_id": "selected-event",
        },
        {
            "severity": "medium",
            "kind": "spxw_position_opened",
            "instrument_id": "option:SPX:SPXW:20260707:7440:C",
            "title": "开仓 7440C",
            "detail": "qty=1",
            "quality": "live",
            "source_gate": "ibkr_positions",
            "event_id": "filtered-event",
        },
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
    )

    result = notify_payload(
        payload,
        settings=settings,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.acknowledged_event_ids == ("selected-event",)


def test_session_restore_is_direct_ops_push(tmp_path, monkeypatch) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    monkeypatch.setattr(
        "spx_spark.notifier.pipeline.generate_push_text",
        lambda template, prompt, settings, **kwargs: (template, "template"),
    )
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
        deepseek_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
        direct_push_llm_enabled=True,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 1
    assert [sink.sink for sink in result.sinks] == ["bark"]
    assert calls == []


def test_bark_title_maps_kinds_to_chinese_categories() -> None:
    from spx_spark.notifier.sinks import bark_title_for_alerts

    assert bark_title_for_alerts([{"kind": "spxw_position_opened"}]) == "SPX 持仓事件"
    assert bark_title_for_alerts([{"kind": "ibkr_session_restored"}]) == "SPX 系统事件"
    assert (
        bark_title_for_alerts([{"kind": "put_skew_steepening_5m"}, {"kind": "atm_iv_jump_5m"}])
        == "SPX 波动率信号 +1"
    )
    assert bark_title_for_alerts([{"kind": "price_move_from_close"}]) == "SPX 价格异动"
    assert bark_title_for_alerts([{"kind": "globex_trend_transition"}]) == "SPX 价格异动"
    assert (
        bark_title_for_alerts([{"kind": "gth_dip_reclaim_call"}])
        == "SPX 0DTE | CALL RECLAIM"
    )
    assert bark_title_for_alerts([{"kind": "option_wall_proximity"}]) == "SPX 结构信号"
    assert (
        bark_title_for_alerts([{"kind": "unknown_kind", "severity": "high"}])
        == "SPX Spark HIGH unknown_kind"
    )


def test_codex_prompt_hides_non_focus_market_context() -> None:
    prompt = build_codex_prompt(make_payload(), [make_payload()["alerts"][0]])

    assert "human_focus_context" in prompt
    assert "equity:QQQ" not in prompt
    assert "index:VIX" not in prompt
    assert "qqq_spy" not in prompt
    assert "SPXW" in prompt
    assert "future:ES" in prompt
    assert "ibkr_session_state" in prompt
    assert "负 gamma 不等于看跌" in prompt
    assert "observe_only" in prompt
    assert "regime" in prompt and "trigger" in prompt and "expression" in prompt
    assert "net_dex_proxy" in prompt
    assert "Hyperliquid" in prompt
    assert "09:30-16:00 ET 是 SPX RTH" in prompt
    assert "12:00-13:00 ET" in prompt
    assert "不下单授权" in prompt or "不是下单授权" in prompt
    assert "机构自营台" in prompt
    assert "## Desk View" in prompt
    assert "## Execution" in prompt
    assert "## Risk" in prompt
    assert "第一行必须且只能写 `需要看盘:`" in prompt


def test_direct_push_and_agent_prompts_carry_steven_micopedia_guardrails() -> None:
    from spx_spark.notifier.prompts import (
        build_agent_prompt,
        build_direct_push_prompt,
        direct_push_header,
    )

    payload = make_payload()
    alerts = [payload["alerts"][0]]
    direct = build_direct_push_prompt(payload, alerts)
    agent = build_agent_prompt(payload, alerts)
    for prompt in (direct, agent):
        assert "observe_only" in prompt
        assert "net_dex_proxy" in prompt
        assert "Hyperliquid" in prompt
        assert "不是下单授权" in prompt or "不下单指令" in prompt
    assert direct_push_header([{"kind": "gth_dip_reclaim_call"}]) == (
        "SPX 0DTE | CALL RECLAIM"
    )
    assert direct_push_header([{"kind": "ibkr_session_restored"}]) == (
        "SPX | SYSTEM STATUS"
    )


def test_alert_key_uses_dedup_group_not_title() -> None:
    alert = {
        "kind": "price_move_from_close",
        "instrument_id": "index:SPX",
        "title": "index:SPX up 23.4 bps from close",
        "dedup_group": "up:1",
    }
    assert alert_key(alert) == "price_move_from_close|index:SPX|up:1"
    assert "23.4" not in alert_key(alert)


def test_notifier_cooldown_ignores_title_when_dedup_group_matches(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = make_settings(str(tmp_path / "notify-state.json"))
    now = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)
    base_alert = {
        "severity": "high",
        "kind": "price_move_from_close",
        "instrument_id": "index:SPX",
        "dedup_group": "up:1",
        "detail": "SPX moved from close.",
    }

    first_payload = {
        "as_of": "2026-07-07T03:15:00+08:00",
        "window": {"name": "close_one_hour", "priority": "high"},
        "alerts": [{**base_alert, "title": "index:SPX up 31.0 bps from close"}],
    }
    second_payload = {
        "as_of": "2026-07-07T03:16:00+08:00",
        "window": {"name": "close_one_hour", "priority": "high"},
        "alerts": [{**base_alert, "title": "index:SPX up 31.7 bps from close"}],
    }

    first = notify_payload(first_payload, settings=settings, runner=runner, now=now)
    assert first.selected_count == 1
    assert first.sent_count == 1

    selected, _ = select_alerts_for_notification(
        second_payload,
        settings,
        now=now + timedelta(seconds=60),
    )
    assert selected == []


def _agent_failopen_payload(*, critical_title: str, include_critical: bool) -> dict[str, object]:
    alerts: list[dict[str, object]] = [
        {
            "severity": "high",
            "kind": "price_move_from_close",
            "instrument_id": "index:SPX",
            "title": "SPX up 31 bps from close",
            "detail": "SPX moved from close.",
        }
    ]
    if include_critical:
        alerts.insert(
            0,
            {
                "severity": "critical",
                "kind": "option_gamma_regime",
                "instrument_id": "option_map:SPXW",
                "title": critical_title,
                "detail": "Gamma regime shifted.",
            },
        )
    return {
        "as_of": "2026-07-07T03:15:00+08:00",
        "window": {"name": "close_one_hour", "priority": "high"},
        "human_focus_context": {},
        "alerts": alerts,
    }


def test_notifier_fails_closed_for_critical_market_observation(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["openclaw", "agent"]:
            raise TimeoutError("agent timed out")
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
        min_severity="high",
        review_audit_path=str(tmp_path / "review-audit.jsonl"),
    )
    critical_title = "SPXW gamma flip critical"
    payload = _agent_failopen_payload(
        critical_title=critical_title,
        include_critical=True,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 0
    assert not any(s.sink == "feishu" and s.ok for s in result.sinks)
    assert all(cmd[:3] != ["openclaw", "message", "send"] for cmd in calls)
    entries = [
        json.loads(line)
        for line in (tmp_path / "review-audit.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert entries[-1]["outcome"] == "review_failed_pending"
    assert calls and calls[0][:2] == ["openclaw", "agent"]


def test_notifier_fails_closed_for_high_price_alert_when_agent_times_out(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["openclaw", "agent"]:
            raise TimeoutError("agent timed out")
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        deepseek_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
        review_audit_path=str(tmp_path / "review-audit.jsonl"),
    )
    payload = _agent_failopen_payload(
        critical_title="unused",
        include_critical=False,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert not any(s.sink == "feishu" and s.ok for s in result.sinks)
    assert result.sent_count == 0
    entries = [
        json.loads(line)
        for line in (tmp_path / "review-audit.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert entries[-1]["outcome"] == "review_failed_pending"
    assert calls and calls[0][:2] == ["openclaw", "agent"]


def test_bark_lockscreen_summary_and_feishu_card() -> None:
    from spx_spark.notifier.format_push import (
        bark_lockscreen_summary,
        build_feishu_card,
        push_lane_for_alerts,
        strip_markdown_light,
    )

    text = "## 结论\n**剧本有变**：flip 上移\n\n## 盯\n只盯 `7455`"
    summary = bark_lockscreen_summary(text)
    assert "剧本有变" in summary
    assert "**" not in summary
    assert "`" not in summary
    assert strip_markdown_light("**7450C** 限价 `14.00`") == "7450C 限价 14.00"

    card = build_feishu_card(text, title="市场状态 · 剧本有变", kind="status")
    assert card["header"]["template"] == "orange"
    assert card["body"]["elements"][0]["tag"] == "markdown"
    assert "剧本有变" in card["body"]["elements"][0]["content"]

    assert push_lane_for_alerts([{"kind": "price_move_from_close"}]) == "trade"
    assert push_lane_for_alerts([{"kind": "ibkr_session_interrupted"}]) == "ops"
    assert (
        push_lane_for_alerts(
            [
                {
                    "kind": "spxw_position_opened",
                    "source_gate": "ibkr_positions",
                }
            ]
        )
        == "trade"
    )


def test_feishu_status_card_uses_sections_and_state_color() -> None:
    from spx_spark.notifier.format_push import build_feishu_card

    text = "\n".join(
        (
            "【SPX 15m · 22:39 · 0DTE 07-13 · 美盘上午主战场】",
            "时钟  开盘后 69 分钟　距收官 140 分钟",
            "价格  SPX 7547.7　ES 7592.2　较昨收 -27.7　GTH EM已用 46%",
            "结构  ZeroGamma过渡　Put 7550　Flip 7545–7550　Call 7550",
            "状态  INVALIDATED（已失效）　Call Wall 7550　等待重置",
            "",
            "ES确认  15m 5.2　60m -8.5　量价同向",
            "波动  VIX1D/VIX 0.52　SKEW 144.3",
            "",
            "【条件计划】标的触发后执行",
            "计划1 · 冲墙回落  SPX 7550触发 → SPXW 7550P　触达 95%　参考 10.5–10.8",
            "执行  触位后按实时 mid/IV 重算；当前不可预挂",
            "变化  call wall 7575→7550",
        )
    )

    card = build_feishu_card(text, title="SPX 15分钟市场状态", kind="status")

    assert card["header"]["title"]["content"] == ("SPX 15m · 22:39 · 0DTE 07-13 · 美盘上午主战场")
    assert card["header"]["template"] == "grey"
    elements = card["body"]["elements"]
    assert [element["tag"] for element in elements] == [
        "markdown",
        "hr",
        "markdown",
        "hr",
        "markdown",
    ]
    assert "**价格**" in elements[0]["content"]
    assert "**ES确认**" in elements[2]["content"]
    assert "**条件计划**" in elements[4]["content"]
    assert "- **计划1 · 冲墙回落**" in elements[4]["content"]
    assert "> **执行**" in elements[4]["content"]


def test_feishu_sectioned_card_converts_wall_table_to_compact_layout() -> None:
    from spx_spark.notifier.format_push import build_feishu_card

    text = "\n".join(
        (
            "【SPX 15m · 12:45 · 0DTE 07-15 · 亚盘夜盘】",
            "状态  APPROACHING（接近）",
            "",
            "## 墙位阶梯",
            "| SPX 墙位 | 结构 | 合约 | 当前 mid | BS 触位价 | 触发后参考 |",
            "| ---: | --- | --- | ---: | ---: | ---: |",
            "| 7550 | 主 Put Wall | 7550C | 23.75 | 18.22 | 15.40–18.20 |",
            "| 7535 | 次级支撑 | 7535C | 34.65 | 15.92 | 13.50–15.90 |",
        )
    )

    card = build_feishu_card(text, title="SPX 15分钟市场状态", kind="status")

    elements = card["body"]["elements"]
    table = next(element for element in elements if element["tag"] == "table")
    assert [column["display_name"] for column in table["columns"]] == [
        "结构",
        "合约 / 现价",
        "BS / 触发参考",
    ]
    assert table["rows"][0] == {
        "c0": "7550\n主 Put Wall",
        "c1": "7550C\n现 23.75",
        "c2": "BS 18.22\n参考 15.40–18.20",
    }
    assert card["header"]["template"] == "orange"


def test_feishu_sectioned_card_collapses_secondary_evidence() -> None:
    from spx_spark.notifier.format_push import build_feishu_card

    text = "\n".join(
        (
            "【SPX 15m · 12:45 · 0DTE 07-15 · 亚盘夜盘】",
            "状态  APPROACHING（接近）",
            "",
            "## Greeks 与波动",
            "Gamma 24.6k　VIX 16.1",
            "",
            "## 关键位状态",
            "Flip 7550–7555",
        )
    )

    card = build_feishu_card(text, title="SPX 15分钟市场状态", kind="status")

    panels = [
        element for element in card["body"]["elements"] if element["tag"] == "collapsible_panel"
    ]
    assert len(panels) == 1
    assert panels[0]["expanded"] is False
    assert panels[0]["header"]["title"]["content"] == "Greeks 与波动"
    assert any(
        element["tag"] == "markdown" and "## 关键位状态" in element["content"]
        for element in card["body"]["elements"]
    )


def test_deliver_trade_push_routes_ops_to_bark_ops_group_not_feishu(tmp_path) -> None:
    from spx_spark.notifier.sinks import deliver_trade_push

    bark_posts: list[dict[str, object]] = []
    feishu_posts: list[dict[str, object]] = []

    def bark_poster(url: str, payload: dict[str, object], timeout: float) -> dict[str, object]:
        bark_posts.append(payload)
        return {"code": 200}

    def feishu_poster(url: str, payload: dict[str, object], timeout: float) -> dict[str, object]:
        feishu_posts.append(payload)
        return {"code": 0}

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        openclaw_target="",
        openclaw_channel="",
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
        bark_ops_group="spx-ops",
        bark_markdown_enabled=True,
        feishu_enabled=True,
        feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
    )

    import spx_spark.notifier.sinks as sinks_mod

    original_bark = sinks_mod.post_bark
    original_feishu = sinks_mod.post_feishu
    sinks_mod.post_bark = bark_poster
    sinks_mod.post_feishu = feishu_poster
    try:
        ops_sinks = deliver_trade_push(
            settings,
            title="SPX 系统事件",
            text="IBKR session interrupted",
            kind="direct_event",
            lane="ops",
            friend=False,
        )
        trade_sinks = deliver_trade_push(
            settings,
            title="市场状态",
            text="摘要正文",
            feishu_text="## 完整报告\n**Greeks、墙位和条件计划**",
            kind="status",
            lane="trade",
            friend=False,
        )
    finally:
        sinks_mod.post_bark = original_bark
        sinks_mod.post_feishu = original_feishu

    assert any(s.sink == "bark" and s.ok for s in ops_sinks)
    assert not any(s.sink == "feishu" and s.attempted for s in ops_sinks)
    assert bark_posts[0]["group"] == "spx-ops"
    assert "markdown" not in bark_posts[0]

    assert any(s.sink == "feishu" and s.ok for s in trade_sinks)
    assert any(s.sink == "bark" and s.ok for s in trade_sinks)
    assert bark_posts[1]["group"] == "spx-spark"
    assert "markdown" in bark_posts[1]
    assert bark_posts[1]["markdown"] == "摘要正文"
    assert feishu_posts[0]["msg_type"] == "interactive"
    assert feishu_posts[0]["card"]["header"]["title"]["content"] == "市场状态"
    assert "完整报告" in feishu_posts[0]["card"]["body"]["elements"][0]["content"]


def test_im_delivery_failed_ignores_intentional_ops_only_delivery() -> None:
    from spx_spark.notifier.model import SinkResult
    from spx_spark.notifier.sinks import im_delivery_failed

    assert not im_delivery_failed([SinkResult(sink="bark", attempted=True, ok=True)])
    assert im_delivery_failed(
        [SinkResult(sink="feishu", attempted=True, ok=False, error="timeout")]
    )
    assert not im_delivery_failed([SinkResult(sink="feishu", attempted=True, ok=True)])


def test_send_bark_message_accepts_markdown_and_group_override(tmp_path) -> None:
    posts: list[dict[str, object]] = []

    def poster(url: str, payload: dict[str, object], timeout: float) -> dict[str, object]:
        posts.append(payload)
        return {"code": 200}

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
        bark_markdown_enabled=True,
    )
    result = send_bark_message(
        settings,
        "SPX 系统事件",
        "session down",
        group="spx-ops",
        markdown="**detail**",
        poster=poster,
    )
    assert result.ok is True
    assert posts[0]["group"] == "spx-ops"
    assert posts[0]["markdown"] == "**detail**"
