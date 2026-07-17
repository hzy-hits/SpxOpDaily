"""Atomic state and projection IO for unified feature frames."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


STATE_NAME = "market_feature_state.json"
MARKET_FRAME_NAME = "minute_market_frame.json"
OPTION_FRAME_NAME = "option_structure_frame.json"
DECISION_CONTEXT_NAME = "decision_context.json"
SESSION_EPISODE_NAME = "session_episode.json"


def feature_state_path(data_root: str) -> Path:
    return Path(data_root).expanduser() / "latest" / STATE_NAME


def projection_paths(data_root: str) -> dict[str, Path]:
    root = Path(data_root).expanduser() / "latest"
    return {
        "market": root / MARKET_FRAME_NAME,
        "option": root / OPTION_FRAME_NAME,
        "decision": root / DECISION_CONTEXT_NAME,
        "session_episode": root / SESSION_EPISODE_NAME,
    }


def load_json(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    try:
        # Skip the write entirely when content is unchanged: the 5s service
        # loop would otherwise rewrite multi-MB state files all day.
        if target.read_text(encoding="utf-8") == rendered:
            return
    except OSError:
        pass
    temporary = target.with_suffix(f"{target.suffix}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(target)


def append_audit(data_root: str, trading_date: str, payload: dict[str, Any]) -> Path:
    target = (
        Path(data_root).expanduser()
        / "audit"
        / "decision_context"
        / f"date={trading_date}"
        / "events.jsonl"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
    return target
