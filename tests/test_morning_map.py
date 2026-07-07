from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from spx_spark.config import NotificationSettings
from spx_spark.morning_map import (
    already_sent,
    build_morning_payload,
    mark_sent,
    render_template,
    run,
    send_morning_map,
    within_send_window,
)
from spx_spark.storage import LatestState


def make_settings(
    state_path: str,
    *,
    missed_queue_path: str = "",
    agent_enabled: bool = False,
    bark_enabled: bool = False,
) -> NotificationSettings:
    return NotificationSettings(
        enabled=True,
        min_severity="high",
        cooldown_seconds=300,
        state_path=state_path,
        openclaw_enabled=True,
        openclaw_command="openclaw",
        openclaw_channel="openclaw-weixin",
        openclaw_account="account-im-bot",
        openclaw_target="user@im.wechat",
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
        bark_enabled=bark_enabled,
        bark_url="https://example.com/bark" if bark_enabled else "",
        bark_group="spx-spark",
        bark_level="",
        bark_timeout_seconds=10.0,
        missed_queue_path=missed_queue_path,
    )


def sample_payload() -> dict:
    return {
        "kind": "morning_map",
        "as_of": "2026-07-07T13:00:00+00:00",
        "overnight": {
            "es_last": 6010.0,
            "es_prev_close": 6000.0,
            "spx_prev_close": 5995.0,
            "gap_points": 10.0,
            "gap_pct": 10.0 / 6000.0,
        },
        "human_focus_context": {
            "spxw_options": {
                "expiries": [
                    {
                        "call_wall": 6050.0,
                        "put_wall": 5950.0,
                        "level_probabilities": [
                            {
                                "level_name": "put_wall",
                                "level": 5950.0,
                                "prob_touch": 0.24,
                                "prob_close_beyond": 0.12,
                            },
                            {
                                "level_name": "call_wall",
                                "level": 6050.0,
                                "prob_touch": 0.18,
                                "prob_close_beyond": 0.08,
                            },
                        ],
                        "gamma_profile": {
                            "zero_gamma": 6000.0,
                            "flip_zone": [5975.0, 5995.0],
                            "top_strikes": [
                                {"strike": 6050.0, "call_oi": 12000.0, "put_oi": 100.0},
                                {"strike": 5950.0, "call_oi": 80.0, "put_oi": 9000.0},
                            ],
                        },
                    }
                ],
                "wall_confluence": {
                    "spy_put_wall_spx": 5948.0,
                    "spy_call_wall_spx": 6052.0,
                    "put_wall_confluent": True,
                    "call_wall_confluent": False,
                },
            },
            "micopedia": {
                "regime": "pin",
                "vix_ratio": 0.92,
                "dip_context": "expensive_tail_protection",
                "trigger_watchlist": ["watch wall 5950", "watch flip zone", "watch IV crush"],
            },
        },
    }


def test_within_send_window_summer_et() -> None:
    assert within_send_window(datetime(2026, 7, 7, 13, 0, tzinfo=timezone.utc)) is True
    assert within_send_window(datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)) is False
    assert within_send_window(datetime(2026, 7, 11, 13, 0, tzinfo=timezone.utc)) is False


def test_already_sent_roundtrip(tmp_path: Path) -> None:
    state_path = str(tmp_path / "morning_map_state.json")
    assert already_sent(state_path, "2026-07-07") is False
    mark_sent(state_path, "2026-07-07")
    assert already_sent(state_path, "2026-07-07") is True
    assert already_sent(state_path, "2026-07-08") is False


def test_render_template_contains_walls_probs_regime() -> None:
    text = render_template(sample_payload())
    assert "【盘前地图 2026-07-07】" in text
    assert "5950" in text
    assert "6050" in text
    assert "触及" in text
    assert "24%" in text
    assert "flip zone 5975-5995" in text
    assert "dip_context=expensive_tail_protection" in text
    assert "共振" in text
    assert "不共振" in text


def test_send_morning_map_falls_back_to_template_when_agent_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPX_PUSH_LLM_ENABLED", "false")
    payload = sample_payload()
    template = render_template(payload)
    settings = make_settings(str(tmp_path / "notify-state.json"), agent_enabled=True)

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["openclaw", "agent"]:
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="agent failed")
        return subprocess.CompletedProcess(command, 0, stdout='{"ok":true}', stderr="")

    result = send_morning_map(payload, settings, runner=runner)
    assert result["used_agent"] is False
    assert result["text"] == template
    assert result["weixin_ok"] is True


def test_send_morning_map_queues_on_weixin_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPX_PUSH_LLM_ENABLED", "false")
    payload = sample_payload()
    template = render_template(payload)
    missed_path = str(tmp_path / "missed.jsonl")
    settings = make_settings(str(tmp_path / "notify-state.json"), missed_queue_path=missed_path)

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="send failed")

    result = send_morning_map(payload, settings, runner=runner)
    assert result["weixin_ok"] is False
    assert result["text"] == template
    lines = Path(missed_path).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["kind"] == "morning_map"
    assert entry["message"] == template


def test_run_skips_outside_window(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(
        "spx_spark.morning_map.LatestStateStore.load",
        lambda self, **kwargs: LatestState(
            created_at=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
            as_of=datetime(2026, 7, 7, 0, 0, tzinfo=timezone.utc),
            quotes=(),
            best_quotes=(),
        ),
    )
    code = run([], now=datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc))
    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(captured.out.strip()) == {"skipped": True, "reason": "outside_send_window"}


def test_build_morning_payload_shape() -> None:
    state = LatestState(
        created_at=datetime(2026, 7, 7, 13, 0, tzinfo=timezone.utc),
        as_of=datetime(2026, 7, 7, 13, 0, tzinfo=timezone.utc),
        quotes=(),
        best_quotes=(),
    )
    payload = build_morning_payload(state)
    assert payload["kind"] == "morning_map"
    assert "overnight" in payload
    assert "human_focus_context" in payload
