from __future__ import annotations

import subprocess
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from spx_spark.config import NotificationSettings
from spx_spark.notifier import (
    alert_key,
    build_codex_prompt,
    codex_message_requests_delivery,
    codex_message_respects_human_scope,
    notify_payload,
    openclaw_delivery_error,
    run_codex_exec,
    run_openclaw_agent,
    select_alerts_for_notification,
    send_bark_message,
    SinkResult,
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


def test_notifier_does_not_mark_openclaw_application_error_as_sent(tmp_path) -> None:
    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            0,
            stdout='{"ret":-2,"errMsg":"missing conversation context"}',
            stderr="",
        )

    result = notify_payload(
        make_payload(),
        settings=make_settings(str(tmp_path / "notify-state.json")),
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 0
    assert result.sinks[0].ok is False
    assert result.sinks[0].error == "openclaw returned ret=-2"
    assert not (tmp_path / "notify-state.json").exists()


def test_openclaw_delivery_error_accepts_dry_run_payload() -> None:
    assert (
        openclaw_delivery_error(
            '{"action":"send","channel":"openclaw-weixin","dryRun":true,"handledBy":"core"}'
        )
        is None
    )


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

    result, message = run_openclaw_agent(settings, "analyze this alert", runner=runner)

    assert result.ok is True
    assert message == "{}"
    command = calls[0]
    assert "--model" in command
    assert command[command.index("--model") + 1] == "gpt-5.3-codex-spark"
    assert "--thinking" in command
    assert command[command.index("--thinking") + 1] == "high"
    assert "--deliver" not in command


def test_notifier_uses_openclaw_agent_single_track_for_review_candidates(tmp_path) -> None:
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
        openclaw_enabled=False,
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

    assert result.sent_count == 1
    assert [sink.sink for sink in result.sinks] == ["openclaw_agent", "openclaw_message"]
    assert calls[0][:2] == ["openclaw", "agent"]
    assert "--deliver" not in calls[0]
    assert calls[0][calls[0].index("--session-key") + 1] == "spx-spark-alerts"
    assert calls[1][:3] == ["openclaw", "message", "send"]
    assert calls[1][calls[1].index("--message") + 1] == "需要看盘: SPX alert confirmed"


def test_notifier_prefers_deepseek_before_openclaw_agent_or_codex(tmp_path, monkeypatch) -> None:
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

    assert result.sent_count == 1
    assert [sink.sink for sink in result.sinks] == ["deepseek_reviewer", "openclaw_message"]
    assert calls and calls[0][:3] == ["openclaw", "message", "send"]
    assert all(call[:2] != ["openclaw", "agent"] for call in calls)
    assert all(call[:2] != ["codex", "exec"] for call in calls)


def test_notifier_prefilter_marks_weak_review_alert_without_llm(tmp_path, monkeypatch) -> None:
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
    assert [sink.sink for sink in first.sinks] == ["review_prefilter"]

    second = notify_payload(
        payload,
        settings=settings,
        now=now + timedelta(seconds=60),
    )
    assert second.selected_count == 0


def test_notifier_deepseek_rate_limit_cooldowns_noncritical_without_fallback(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_deepseek(settings: NotificationSettings, prompt: str) -> tuple[SinkResult, str]:
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

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        deepseek_enabled=True,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=True,
    )
    now = datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc)

    first = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=now,
    )

    assert first.selected_count == 1
    assert first.sent_count == 0
    assert [sink.sink for sink in first.sinks] == [
        "deepseek_reviewer",
        "deepseek_reviewer_rate_limit_cooldown",
    ]
    assert calls == []

    second = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=now + timedelta(seconds=60),
    )
    assert second.selected_count == 0


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


def test_notifier_agent_approved_message_also_goes_to_bark(tmp_path, monkeypatch) -> None:
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
        openclaw_enabled=False,
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

    assert [sink.sink for sink in result.sinks] == [
        "openclaw_agent",
        "openclaw_message",
        "bark",
    ]
    assert result.sent_count == 2
    assert bark_posts[0]["body"] == "需要看盘: SPX alert confirmed"
    assert str(bark_posts[0]["title"]).startswith("SPX 价格异动")


def test_alerts_are_market_signals_rejects_mixed_and_empty_batches() -> None:
    from spx_spark.notifier import alerts_are_market_signals

    market = {"kind": "option_gamma_regime"}
    ops = {"kind": "required_data_degraded"}
    assert alerts_are_market_signals([market]) is True
    assert alerts_are_market_signals([market, ops]) is False
    assert alerts_are_market_signals([]) is False


def test_market_signal_agent_message_also_goes_to_friend_bark(tmp_path, monkeypatch) -> None:
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

    assert "bark_friend" in [sink.sink for sink in result.sinks]
    urls = [url for url, _ in posts]
    assert "https://api.day.app/user-key" in urls
    assert "https://api.day.app/friend-key" in urls
    friend_payload = next(payload for url, payload in posts if url.endswith("friend-key"))
    assert friend_payload["body"] == "需要看盘: SPX alert confirmed"


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


def test_notifier_bark_delivery_alone_still_starts_cooldown(tmp_path, monkeypatch) -> None:
    """If the Weixin token is dead but Bark reaches the human, the alert
    counts as delivered and must enter cooldown."""
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
        openclaw_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
    )

    first = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )
    assert first.sent_count == 1

    second = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 2, tzinfo=timezone.utc),
    )
    assert second.selected_count == 0


def test_notifier_agent_no_push_verdict_is_not_delivered_and_starts_cooldown(tmp_path) -> None:
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
        openclaw_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
    )

    first = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert first.sent_count == 0
    assert [sink.sink for sink in first.sinks] == [
        "openclaw_agent",
        "openclaw_agent_delivery_gate",
    ]
    assert all(call[:3] != ["openclaw", "message", "send"] for call in calls)

    # Same alerts again shortly after: the rejected bucket is in cooldown, so
    # the agent must not be re-run.
    second = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 2, tzinfo=timezone.utc),
    )

    assert second.selected_count == 0
    assert len(calls) == 1


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
                handle.write("需要看盘: SPX alert confirmed, but Hyperliquid proxy also moved")
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
    assert codex_message_respects_human_scope("需要看盘: SPX setup with VIX context.")
    assert codex_message_respects_human_scope("需要看盘: gamma transition, VIX1D 18 -> 21, SKEW rising.")
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
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        codex_enabled=True,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 1
    assert len(calls) == 1
    assert calls[0][0:3] == ["openclaw", "message", "send"]
    message_index = calls[0].index("--message") + 1
    assert "浮盈浮亏" in calls[0][message_index]


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
    assert result.sent_count == 0
    assert calls == []


def test_notifier_routes_iv_surface_alerts_through_review(tmp_path) -> None:
    """IV-surface movement alerts must not bypass review with a raw push; they
    go to the codex/agent review path so the human gets gamma/VIX context."""
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
        openclaw_enabled=False,
        codex_enabled=True,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert len(calls) == 1
    assert calls[0][1] == "exec"
    assert "--message" not in calls[0]


def test_offhours_skew_steepening_bypasses_review_and_severity_floor(tmp_path) -> None:
    """Off-hours (spxw_sampling_mode=off) sudden vol repricing alerts push
    directly even when the quiet window stamps them below min_severity."""
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    payload = make_payload()
    payload["window"] = {
        "name": "quiet_futures_context",
        "priority": "low",
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
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        codex_enabled=True,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert result.sent_count == 1
    assert len(calls) == 1
    assert calls[0][0:3] == ["openclaw", "message", "send"]


def test_rth_skew_steepening_still_goes_through_review(tmp_path) -> None:
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
        openclaw_enabled=False,
        codex_enabled=True,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.selected_count == 1
    assert len(calls) == 1
    assert calls[0][1] == "exec"
    assert "--message" not in calls[0]


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
    assert (
        notify_payload(payload, settings=settings, runner=runner, now=base).selected_count == 1
    )
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

    def fake_writer(prompt: str, **kwargs) -> tuple[str | None, str | None]:
        assert "即时事件播报员" in prompt
        assert "持仓事件" in prompt
        return "【持仓事件】开仓 7430C x1，现价贴近 flip zone 下沿。", None

    monkeypatch.setattr("spx_spark.notifier.pipeline.call_llm_writer", fake_writer)
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
        }
    ]
    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        direct_push_llm_enabled=True,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 1
    message_index = calls[0].index("--message") + 1
    assert calls[0][message_index].startswith("【持仓事件】")


def test_direct_push_falls_back_to_template_when_writer_fails(tmp_path, monkeypatch) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    monkeypatch.setattr(
        "spx_spark.notifier.pipeline.call_llm_writer",
        lambda prompt, **kwargs: (None, "http=500: boom"),
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
        openclaw_enabled=False,
        direct_push_llm_enabled=True,
    )

    result = notify_payload(
        payload,
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    assert result.sent_count == 1
    message_index = calls[0].index("--message") + 1
    assert "SPX/SPXW alert" in calls[0][message_index]


def test_bark_title_maps_kinds_to_chinese_categories() -> None:
    from spx_spark.notifier.sinks import bark_title_for_alerts

    assert bark_title_for_alerts([{"kind": "spxw_position_opened"}]) == "SPX 持仓事件"
    assert bark_title_for_alerts([{"kind": "ibkr_session_restored"}]) == "SPX 系统事件"
    assert (
        bark_title_for_alerts([{"kind": "put_skew_steepening_5m"}, {"kind": "atm_iv_jump_5m"}])
        == "SPX 波动率信号 +1"
    )
    assert bark_title_for_alerts([{"kind": "price_move_from_close"}]) == "SPX 价格异动"
    assert bark_title_for_alerts([{"kind": "option_wall_proximity"}]) == "SPX 结构信号"
    assert bark_title_for_alerts(
        [{"kind": "unknown_kind", "severity": "high"}]
    ) == "SPX Spark HIGH unknown_kind"


def test_codex_prompt_hides_non_focus_market_context() -> None:
    prompt = build_codex_prompt(make_payload(), [make_payload()["alerts"][0]])

    assert "human_focus_context" in prompt
    assert "equity:QQQ" not in prompt
    assert "index:VIX" not in prompt
    assert "qqq_spy" not in prompt
    assert "SPXW" in prompt
    assert "future:ES" in prompt
    assert "ibkr_session_state" in prompt


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
    assert len(calls) == 1


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


def test_notifier_failopen_sends_critical_when_agent_fails(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["openclaw", "agent"]:
            raise TimeoutError("agent timed out")
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
        min_severity="high",
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

    message_calls = [cmd for cmd in calls if cmd[:3] == ["openclaw", "message", "send"]]
    assert len(message_calls) == 1
    assert critical_title in message_calls[0][message_calls[0].index("--message") + 1]
    assert result.sent_count == 1


def test_notifier_failopen_skips_when_only_high_alerts_and_agent_fails(tmp_path) -> None:
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["openclaw", "agent"]:
            raise TimeoutError("agent timed out")
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    settings = replace(
        make_settings(str(tmp_path / "notify-state.json")),
        openclaw_enabled=False,
        openclaw_agent_enabled=True,
        openclaw_agent_deliver=True,
        codex_enabled=False,
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

    message_calls = [cmd for cmd in calls if cmd[:3] == ["openclaw", "message", "send"]]
    assert message_calls == []
    assert result.sent_count == 0
