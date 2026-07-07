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

DEFAULT_SYSTEM_PROMPT = "\n".join(
    (
        "你是 SPX Spark 的驻场量化交易员，为唯一的一位读者写作。",
        "读者画像：只交易 SPX/SPXW 0DTE/1DTE 期权，只做买 call/put 或垂直价差；"
        "习惯在北京 14:00 到美股开盘之间研究并挂限价单，开盘后前两小时可能盯盘，之后靠挂单睡觉。",
        "写作纪律：",
        "1. 结论先行——第一时间回答读者此刻要做的那个决定，再给证据。",
        "2. 只依据提供的 JSON 与模板事实，绝不编造数字、新闻或仓位；引用关键数字而非全部复述，模板里已有的数字不必逐条重列。",
        "3. 讲赔率不讲指令——把概率、到位价、现价放在一起说这笔单在赌什么、划算不划算，但不下达买卖指令。",
        "4. 用 if/then 说剧本——价格到哪个位置剧本成立/作废，而不是只描述当前状态。",
        "5. 像交易台同事间的口头简报：专业但说人话，不堆术语，不写免责声明套话。",
        "6. 数据 degraded 或缺失时如实说明，并拒绝给方向判断。",
    )
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
            timeout_seconds=float(os.getenv("SPX_PUSH_LLM_TIMEOUT_SECONDS", "90")),
            # deepseek-v4-pro is a reasoning model: the chain-of-thought also
            # consumes completion tokens (observed ~2000 reasoning tokens per
            # report), so leave generous headroom or the visible content comes
            # back empty with finish_reason=length.
            max_tokens=int(os.getenv("SPX_PUSH_LLM_MAX_TOKENS", "12800")),
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


# --- push continuity: remember the last push so the next writer can say
# "剧本维持/剧本有变" instead of starting from amnesia ---

PUSH_CONTEXT_MAX_CHARS = 1600


def default_push_context_path() -> str:
    data_root = os.getenv("MARKET_DATA_DATA_ROOT") or os.getenv("MAINTENANCE_DATA_ROOT") or "data"
    return os.getenv("SPX_PUSH_CONTEXT_PATH") or str(
        Path(data_root) / "latest" / "push_context.json"
    )


def load_previous_push(path: str | None = None) -> dict[str, Any] | None:
    path = path or default_push_context_path()
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def record_push(kind: str, text: str, *, at: str, path: str | None = None) -> None:
    path = path or default_push_context_path()
    target = Path(path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {"kind": kind, "at": at, "text": text[:PUSH_CONTEXT_MAX_CHARS]}
        tmp = target.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp.replace(target)
    except OSError:
        pass


def previous_push_json(previous_push: dict[str, Any] | None) -> str:
    if not previous_push:
        return "null"
    return json.dumps(previous_push, ensure_ascii=False, separators=(",", ":"))


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
