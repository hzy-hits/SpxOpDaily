"""Generic DeepSeek writer for scheduled report pushes (order map / morning map / status).

Distinct from the OpenClaw agent gate in the alert pipeline: this is a pure
"writer" — the decision to push has already been made, the LLM only turns the
deterministic template + payload facts into trader-voice narration.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from spx_spark.config import NotificationSettings, env_bool, load_dotenv
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.sinks import run_openclaw_agent

DEFAULT_SYSTEM_PROMPT = (
    "你是一名资深的 SPX/SPXW 0DTE 期权量化交易员兼写手。"
    "你只根据用户提供的 JSON 数据与模板事实写作，绝不编造数字、新闻或仓位。"
    "用词专业但口语化，像交易台同事间的简报。"
)


@dataclass(frozen=True)
class LlmWriterSettings:
    enabled: bool
    model: str
    url: str
    env_file: str
    timeout_seconds: float
    max_tokens: int

    @classmethod
    def from_env(cls) -> "LlmWriterSettings":
        load_dotenv()
        return cls(
            enabled=env_bool("SPX_PUSH_LLM_ENABLED", True),
            model=os.getenv("SPX_PUSH_LLM_MODEL", "deepseek-v4-pro").strip(),
            url=os.getenv(
                "SPX_PUSH_LLM_URL",
                "https://api.deepseek.com/v1/chat/completions",
            ).strip(),
            env_file=os.getenv("SPX_PUSH_LLM_ENV_FILE", "/home/ubuntu/.hermes/.env").strip(),
            timeout_seconds=float(os.getenv("SPX_PUSH_LLM_TIMEOUT_SECONDS", "60")),
            max_tokens=int(os.getenv("SPX_PUSH_LLM_MAX_TOKENS", "900")),
        )


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


def api_key(settings: LlmWriterSettings) -> str:
    return os.getenv("DEEPSEEK_API_KEY", "").strip() or read_env_file_value(
        settings.env_file,
        "DEEPSEEK_API_KEY",
    )


def call_llm_writer(
    prompt: str,
    *,
    system: str = DEFAULT_SYSTEM_PROMPT,
    settings: LlmWriterSettings | None = None,
) -> tuple[str | None, str | None]:
    """Return (text, error). Callers fall back to the deterministic template on error."""
    settings = settings or LlmWriterSettings.from_env()
    if not settings.enabled:
        return None, "disabled"
    key = api_key(settings)
    if not key:
        return None, "missing DEEPSEEK_API_KEY"
    body: dict[str, Any] = {
        "model": settings.model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
        "max_tokens": settings.max_tokens,
        "stream": False,
    }
    request = urllib.request.Request(
        settings.url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=settings.timeout_seconds) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        return None, f"http={exc.code}: {detail}"
    except OSError as exc:
        return None, str(exc)
    try:
        content = json.loads(raw)["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        return None, f"bad response shape: {exc}"
    if not content:
        return None, "empty response"
    return content, None


def generate_push_text(
    template: str,
    prompt: str,
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
) -> tuple[str, str]:
    """Return (text, writer). DeepSeek first, OpenClaw agent as fallback, then the template."""
    reply, error = call_llm_writer(prompt)
    if reply:
        return reply, "deepseek"
    if error and error != "disabled":
        print(f"llm_writer: deepseek failed ({error}); falling back", file=sys.stderr)
    if settings.openclaw_agent_enabled:
        sink, agent_reply = run_openclaw_agent(settings, prompt, runner=runner)
        if agent_reply and sink.ok:
            return agent_reply, "openclaw_agent"
    return template, "template"
