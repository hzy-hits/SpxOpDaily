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
                    "expiry": "20260707",
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


def test_post_close_runtime_imports_independently() -> None:
    from spx_spark.post_close_runtime import ReviewLlmSettings

    assert ReviewLlmSettings.__name__ == "ReviewLlmSettings"


def test_build_push_summary_format() -> None:
    summary = build_push_summary(sample_payload(), latest_markdown_path="/tmp/review.md")
    first_line = summary.splitlines()[0]
    assert "【盘后复盘 2026-07-07】" in first_line
    assert "5950" in summary
    assert "6050" in summary
    assert "完整报告已附在飞书卡片下方" in summary


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

    monkeypatch.setattr(
        "spx_spark.post_close_review.NotificationSettings.from_env",
        lambda: settings,
    )
    monkeypatch.setattr(
        "spx_spark.post_close_runtime.generate_push_text",
        lambda template, prompt, settings, **kwargs: (template, "template"),
    )
    result = push_review(payload, latest_markdown_path="/tmp/review.md")
    assert result["used_agent"] is False
    assert result["writer"] == "template"
    assert result["text"] == summary
    assert result["im_ok"] is True


def test_push_review_uses_writer_and_attaches_full_report(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SPX_REVIEW_PUSH_ENABLED", raising=False)
    settings = make_settings(str(tmp_path / "notify-state.json"))
    cards: list[dict[str, object]] = []
    monkeypatch.setattr(
        "spx_spark.post_close_review.NotificationSettings.from_env",
        lambda: settings,
    )
    monkeypatch.setattr(
        "spx_spark.post_close_runtime.generate_push_text",
        lambda template, prompt, settings, **kwargs: (
            "【盘后复盘 2026-07-07】\n\n## 今日结论\n墙位失效，IV 回落。",
            "grok_cli",
        ),
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda url, payload, timeout: cards.append(payload) or {"code": 0, "msg": "success"},
    )

    result = push_review(
        sample_payload(),
        latest_markdown_path="/tmp/review.md",
        full_markdown="# Full Review\n\n## Price Path\n完整价格路径",
    )

    assert result["writer"] == "grok_cli"
    assert result["used_agent"] is True
    card_text = "\n".join(
        str(item.get("content") or "")
        for item in cards[0]["card"]["body"]["elements"]
        if isinstance(item, dict)
    )
    assert "墙位失效" in card_text
    assert "完整价格路径" in card_text
