from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from spx_spark.config import NotificationSettings
from spx_spark.notifier.model import SinkResult


def read_env_file_value(path: str, key: str) -> str:
    env_path = Path(path).expanduser()
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        name, value = line.split("=", 1)
        if name.strip() == key:
            return value.strip().strip('"').strip("'")
    return ""


def deepseek_api_key(settings: NotificationSettings) -> str:
    return os.getenv("DEEPSEEK_API_KEY", "").strip() or read_env_file_value(
        settings.deepseek_env_file,
        "DEEPSEEK_API_KEY",
    )


def deepseek_usage_limited(error: str | None) -> bool:
    lowered = (error or "").lower()
    return "429" in lowered or "rate limit" in lowered or "usage limit" in lowered


def run_deepseek_reviewer(
    settings: NotificationSettings,
    prompt: str,
) -> tuple[SinkResult, str]:
    api_key = deepseek_api_key(settings)
    if not api_key:
        return (
            SinkResult(
                sink="deepseek_reviewer",
                attempted=False,
                ok=False,
                error="missing DEEPSEEK_API_KEY",
            ),
            "",
        )

    body: dict[str, Any] = {
        "model": settings.deepseek_model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a fast SPX/SPXW alert reviewer. Reply in Chinese. "
                    "The first line must start with 需要看盘: or 不需要推送:."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": settings.deepseek_temperature,
        "max_tokens": settings.deepseek_max_tokens,
        "stream": False,
    }
    request = urllib.request.Request(
        settings.deepseek_url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=settings.deepseek_timeout_seconds) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        return (
            SinkResult(
                sink="deepseek_reviewer",
                attempted=True,
                ok=False,
                exit_code=exc.code,
                error=f"http={exc.code}: {detail}",
            ),
            "",
        )
    except OSError as exc:
        return (
            SinkResult(
                sink="deepseek_reviewer",
                attempted=True,
                ok=False,
                error=str(exc),
            ),
            "",
        )
    try:
        message = json.loads(raw)["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        return (
            SinkResult(
                sink="deepseek_reviewer",
                attempted=True,
                ok=False,
                error=f"bad response shape: {exc}",
            ),
            "",
        )
    if len(message) > settings.deepseek_output_max_chars:
        message = message[: settings.deepseek_output_max_chars].rstrip() + "\n..."
    return (
        SinkResult(
            sink="deepseek_reviewer",
            attempted=True,
            ok=bool(message),
            error=None if message else "empty response",
        ),
        message,
    )
