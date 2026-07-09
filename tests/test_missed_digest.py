from __future__ import annotations

import json

import pytest
import subprocess
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.config import NotificationSettings
from spx_spark.notifier import notify_payload
from spx_spark.notifier.digest_cli import run as digest_run
from spx_spark.notifier.missed_queue import (
    append_missed,
    build_digest,
    clear_missed,
    flush_missed,
    load_missed,
)


@pytest.fixture(autouse=True)
def _stub_delivery(monkeypatch):
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda url, payload, timeout: {"code": 0, "msg": "success"},
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_bark",
        lambda url, payload, timeout: {"code": 200},
    )


def make_settings(
    state_path: str,
    *,
    missed_queue_path: str,
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
        bark_enabled=True,
        bark_url="https://api.day.app/test-key",
        bark_group="spx-spark",
        bark_level="",
        bark_timeout_seconds=10.0,
        feishu_enabled=True,
        feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
        feishu_secret="",
        feishu_timeout_seconds=10.0,
        missed_queue_path=missed_queue_path,
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
                "title": "IV surface degraded",
                "detail": "Too few live option quotes.",
            },
        ],
    }


def test_append_load_clear_roundtrip(tmp_path) -> None:
    queue_path = str(tmp_path / "missed.jsonl")
    at = datetime(2026, 7, 7, 1, 0, tzinfo=timezone.utc)

    append_missed(queue_path, "line one\nline two", kind="agent", at=at)
    entries = load_missed(queue_path)

    assert len(entries) == 1
    assert entries[0]["kind"] == "agent"
    assert entries[0]["message"] == "line one\nline two"

    clear_missed(queue_path)
    assert load_missed(queue_path) == []


def test_build_digest_timeline_format() -> None:
    entries = [
        {
            "at": "2026-07-07T02:00:00+00:00",
            "kind": "agent",
            "message": "second line ignored\nextra",
        },
        {
            "at": "2026-07-07T04:30:00+00:00",
            "kind": "direct",
            "message": "latest alert",
        },
        {
            "at": "2026-07-07T01:00:00+00:00",
            "kind": "codex",
            "message": "earliest alert",
        },
    ]

    digest = build_digest(entries)

    assert "3 条" in digest.splitlines()[0]
    body_lines = digest.splitlines()[1:]
    assert body_lines == sorted(body_lines, key=lambda line: line[:5])
    assert "- 09:00 earliest alert" in digest
    assert "- 10:00 second line ignored" in digest
    assert "- 12:30 latest alert" in digest
    assert "extra" not in digest


def test_build_digest_caps_entries() -> None:
    entries = [
        {
            "at": f"2026-07-07T{hour:02d}:00:00+00:00",
            "kind": "direct",
            "message": f"alert {hour}",
        }
        for hour in range(15)
    ]

    digest = build_digest(entries)

    timeline_lines = [line for line in digest.splitlines() if line.startswith("- ")]
    assert len(timeline_lines) == 12
    assert "(另有 3 条更早的已省略)" in digest


def test_pipeline_queues_message_when_feishu_fails_and_bark_ok(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_bark",
        lambda *_: {"code": 200},
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda *_: {"code": 19001, "msg": "fail"},
    )

    queue_path = str(tmp_path / "missed.jsonl")

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
        make_settings(
            str(tmp_path / "notify-state.json"),
            missed_queue_path=queue_path,
        ),
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
    assert first.sent_count == 1  # bark ok

    queued = load_missed(queue_path)
    assert len(queued) == 1
    assert queued[0]["kind"] == "agent"
    assert queued[0]["message"] == "需要看盘: SPX alert confirmed"

    second = notify_payload(
        make_payload(),
        settings=settings,
        runner=runner,
        now=datetime(2026, 7, 7, 0, 2, tzinfo=timezone.utc),
    )
    assert second.selected_count == 0


def test_pipeline_flushes_queue_before_new_send(tmp_path, monkeypatch) -> None:
    queue_path = str(tmp_path / "missed.jsonl")
    append_missed(
        queue_path,
        "queued alert body",
        kind="agent",
        at=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )

    feishu_posts: list[dict] = []

    def feishu_poster(url, payload, timeout):
        feishu_posts.append(payload)
        return {"code": 0, "msg": "success"}

    monkeypatch.setattr("spx_spark.notifier.sinks.post_feishu", feishu_poster)

    settings = replace(
        make_settings(
            str(tmp_path / "notify-state.json"),
            missed_queue_path=queue_path,
        ),
        deepseek_enabled=True,
        deepseek_deliver=True,
    )

    # Stub deepseek so review path delivers
    monkeypatch.setattr(
        "spx_spark.notifier.pipeline.run_deepseek_reviewer",
        lambda settings, prompt: (
            __import__("spx_spark.notifier", fromlist=["SinkResult"]).SinkResult(
                sink="deepseek_reviewer", attempted=True, ok=True
            ),
            "需要看盘: SPX alert confirmed",
        ),
    )

    notify_payload(
        make_payload(),
        settings=settings,
        now=datetime(2026, 7, 7, 1, 0, tzinfo=timezone.utc),
    )

    assert feishu_posts
    digest_bodies = []
    for post in feishu_posts:
        card = post.get("card") or {}
        # flatten card text roughly
        digest_bodies.append(str(card))
    joined = "\n".join(digest_bodies)
    assert "通道离线期间错过" in joined or any("queued alert body" in str(p) for p in feishu_posts)
    assert not Path(queue_path).exists()


def test_digest_cli_returns_zero_on_empty_queue(tmp_path, monkeypatch, capsys) -> None:
    queue_path = str(tmp_path / "missed.jsonl")

    def fake_from_env() -> NotificationSettings:
        return make_settings(
            str(tmp_path / "notify-state.json"),
            missed_queue_path=queue_path,
        )

    monkeypatch.setattr(
        "spx_spark.notifier.digest_cli.NotificationSettings.from_env",
        fake_from_env,
    )

    assert digest_run() == 0
    output = json.loads(capsys.readouterr().out.strip())
    assert output == {"flushed": False, "count": 0}


def test_flush_missed_keeps_queue_on_send_failure(tmp_path, monkeypatch) -> None:
    queue_path = str(tmp_path / "missed.jsonl")
    append_missed(
        queue_path,
        "queued alert body",
        kind="direct",
        at=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda url, payload, timeout: {"code": 19001, "msg": "fail"},
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_bark",
        lambda url, payload, timeout: {"code": 500},
    )

    settings = make_settings(
        str(tmp_path / "notify-state.json"),
        missed_queue_path=queue_path,
    )

    result = flush_missed(settings)

    assert result is not None
    assert result.ok is False
    assert Path(queue_path).exists()
    assert len(load_missed(queue_path)) == 1
