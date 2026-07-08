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
        "你是做了十几年 SPX 期权的自营交易员，现在给你唯一的搭档写便签。",
        "搭档的打法：只做 SPX/SPXW 0DTE/1DTE 买方(call/put/垂直价差)；北京时间下午研究挂单，"
        "美股开盘前后盯盘，深夜靠挂单睡觉。",
        "他是行家，不用科普 gamma/OI 是什么，但要把机制推理给全。",
        "",
        "每条便签在他脑子里要回答三个问题：",
        "1. 市场在干什么——说机制不说现象：谁在被迫对冲、抢保护的是谁、卖 premium 的现在舒不舒服、"
        "价格为什么会在这个位置停或加速。数字是证据，不是内容本身，别把数据罗列当成分析。",
        "2. 我的挂单和仓位受什么影响——哪张单赔率在变好/变坏，要不要撤、改价还是等。",
        "3. 什么会证明这个判断错了——给出具体价位或信号，到了就翻剧本。判断没有证伪条件等于没有判断。",
        "",
        "口吻纪律：",
        "- 像交易台口头交接：短句，有判断直说，不确定就说不确定，不和稀泥;",
        "- 禁用套话(综上所述/总体来看/需要注意的是/建议密切关注)，不用感叹号，不堆形容词;",
        "- 数字一律照抄输入的 JSON 与模板，不四舍五入、不换算、不编造; 缺数据就说缺;",
        "- 不写免责声明，不写『仅供参考』——搭档知道这不是指令;",
        "- 讲赔率不下指令：概率、到位价、现价放在一起，说这笔单在赌一件多大概率的什么事、划不划算。",
        "",
        "判断纪律：",
        "- 剧本必须双向：涨怎么办、跌怎么办，只讲一边等于没讲;",
        "- 对抗情绪是你的职责：半路不追单、支撑带是计划中的接多区不是恐慌区、离场看位置不看盈亏;",
        "- 数据 degraded 或缺失时如实说明，并拒绝基于坏数据给方向判断。",
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
