from __future__ import annotations

import subprocess

import pytest

from spx_spark.config import NotificationSettings
from spx_spark.post_close_review import build_push_summary, push_review


@pytest.fixture(autouse=True)
def _stub_feishu(monkeypatch):
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda url, payload, timeout: {"code": 0, "msg": "success"},
    )


def make_settings(
    state_path: str,
    *,
    agent_enabled: bool = False,
) -> NotificationSettings:
    return NotificationSettings(
        enabled=True,
        min_severity="high",
        cooldown_seconds=300,
        state_path=state_path,
        openclaw_enabled=False,
        openclaw_command="openclaw",
        openclaw_channel="",
        openclaw_account="",
        openclaw_target="",
        openclaw_dry_run=True,
        openclaw_timeout_seconds=20.0,
        openclaw_agent_enabled=agent_enabled,
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
        codex_sandbox="read-only",
        codex_cwd="/tmp",
        codex_timeout_seconds=120.0,
        codex_output_max_chars=4000,
        codex_require_delivery_cue=True,
        bark_enabled=False,
        bark_url="",
        bark_group="spx-spark",
        bark_level="",
        bark_timeout_seconds=10.0,
        feishu_enabled=True,
        feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test",
        feishu_secret="",
        feishu_timeout_seconds=10.0,
        missed_queue_path="",
    )


def sample_payload() -> dict:
    return {
        "trading_date": "2026-07-07",
        "spx": {
            "first": 6000.0,
            "last": 6010.0,
            "change_points": 10.0,
            "change_bps": 16.7,
            "range_points": 25.0,
            "low": 5990.0,
            "high": 6015.0,
        },
        "iv_surface": {
            "expiries": [
                {
                    "put_wall_last": 5950.0,
                    "call_wall_last": 6050.0,
                    "zero_gamma_last": 6000.0,
                    "gamma_state_last": "positive_gamma_pin",
                    "atm_iv": {"first": 0.12, "last": 0.10},
                    "put_skew_ratio": {"first": 1.05, "last": 1.02},
                }
            ]
        },
        "verdict": {"status": "complete", "warnings": []},
    }


def test_build_push_summary_format() -> None:
    summary = build_push_summary(sample_payload(), latest_markdown_path="/tmp/review.md")
    first_line = summary.splitlines()[0]
    assert "【盘后复盘 2026-07-07】" in first_line
    assert "5950" in summary
    assert "6050" in summary
    assert "完整报告: /tmp/review.md" in summary


def test_push_review_respects_disabled_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPX_REVIEW_PUSH_ENABLED", "false")
    calls: list[list[str]] = []

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    result = push_review(
        sample_payload(),
        latest_markdown_path="/tmp/review.md",
        runner=runner,
    )
    assert result == {"skipped": True, "reason": "push_disabled"}
    assert calls == []


def test_push_review_agent_fallback(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SPX_REVIEW_PUSH_ENABLED", raising=False)
    payload = sample_payload()
    summary = build_push_summary(payload, latest_markdown_path="/tmp/review.md")
    settings = make_settings(str(tmp_path / "notify-state.json"), agent_enabled=True)

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["openclaw", "agent"]:
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="agent failed")
        return subprocess.CompletedProcess(command, 0, stdout='{"ok":true}', stderr="")

    monkeypatch.setattr(
        "spx_spark.post_close_review.NotificationSettings.from_env",
        lambda: settings,
    )
    result = push_review(payload, latest_markdown_path="/tmp/review.md", runner=runner)
    assert result["used_agent"] is False
    assert result["text"] == summary
    assert result["im_ok"] is True
