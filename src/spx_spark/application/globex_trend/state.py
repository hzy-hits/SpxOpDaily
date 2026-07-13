"""Durable state IO for the Globex trend machine."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import fcntl


STATE_FILE_NAME = "globex_trend_state.json"


def trend_state_path(data_root: str) -> Path:
    return Path(data_root).expanduser() / "latest" / STATE_FILE_NAME


def load_trend_state(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_trend_state(path: str | Path, state: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(f"{target.suffix}.tmp")
    temp.write_text(json.dumps(state, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    temp.replace(target)


@contextmanager
def locked_trend_state(path: str | Path) -> Iterator[None]:
    lock_path = Path(path).with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
