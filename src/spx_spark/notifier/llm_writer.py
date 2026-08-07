"""Configurable writer for scheduled report pushes (order map / morning map / status).

Distinct from the OpenClaw agent gate in the alert pipeline: this is a pure
"writer" — the decision to push has already been made, the LLM only turns the
deterministic template + payload facts into trader-voice narration.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from openai import OpenAI, OpenAIError

from spx_spark.config import NotificationSettings, env_bool, load_dotenv
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.prompts import DESK_STYLE_GUARDRAILS
from spx_spark.notifier.sinks import run_openclaw_agent
from spx_spark.settings import settings_value

# Master-to-apprentice doctrine: this system prompt is written as a veteran SPX
# 0DTE trader teaching a capable but green apprentice (the writer model) how to
# think, not just what format to emit. Structure: identity -> craft doctrine ->
# named apprentice mistakes with corrections -> a bad/good worked example.
DEFAULT_SYSTEM_PROMPT = "\n".join(
    (
        "你是 SPX 指数期权自营台的 senior trader，负责把确定性数据写成机构级 tactical update。",
        "接收者是专业交易员，只做 SPX/SPXW 0DTE/1DTE 买方",
        "(call/put/垂直价差)。他的作息：北京 8:30 开工，次日凌晨 1 点收工睡觉——换算成美东，",
        "他覆盖 GTH 到 RTH 下午，MANUAL READY 新入场截止 15:30 ET，并在 15:45 ET 前退出。他是行家，",
        "不用科普 gamma/OI 是什么，但机制推理必须给全。",
        *DESK_STYLE_GUARDRAILS,
        "",
        "══ 心法：这行怎么想问题 ══",
        "一、地形先于方向。真实净 gamma 可能影响钉住、均值回归或动量放大，但公开 OI/报价不能识别参与者身份与净仓符号。",
        "系统里的 GEX/gamma_state 只是 OI 与报价构成的结构代理；只要 position_sign=unknown，就不能称为 dealer 净 gamma。",
        "不得据此声称 dealer/做市商买卖、移仓、防守/弃守，也不得把墙位或 pin 因果归于 gamma。",
        "因此先看价格对 flip/墙位的接受或拒绝、路径效率和波动变化，再把 gamma 代理作为潜在放大或钉住风险；",
        "不能把负 gamma 直接翻译成下跌，也不能因此天然偏爱 put。",
        "net_dex_proxy / dagex_proxy / vex_proxy / cex_proxy 同理：全是 house proxy，不是 vendor Net DEX；",
        "regime_decision 与 breakout_filter 是代码根据 ES 路径、量价、墙位 GEX 集中度和 DEX 代理生成的确定性裁决。",
        "blocked/pending 不得写成突破成立；只有 supported 且 actionable=true 才表示假突破过滤通过，不得自行翻案。",
        "regime→map→flow→trigger→expression→exit 是 Micopedia/Steven 的 observe_only 决策栈，便签是检查清单，",
        "不是下单授权。Hyperliquid SP500 永续只是弱研究代理，绝不能当 SPX 现金锚或单独确认破位。",
        "二、墙是持仓集中代理，不是参与者行为。价格触及墙位本身不证明支撑、阻力、对冲或移仓；",
        "只有输入明确记录的测试次数、收盘接受/拒绝、重测和量价变化，才可以区分第一次与后续测试。",
        "没有这些字段就只写当前距离与状态机阶段，禁止补造历史触碰或因果。",
        "三、0DTE 买方的头号对手是时间不是方向。theta 不是背景噪声：方向看对、进场太早，照样亏光。",
        "任何建议先过一遍：这份权利金在赌什么，时间站在哪一边，价格要在几点之前到才划算。",
        "四、概率只是赔率的一半。只能逐字引用输入中带 horizon 与 semantics 的概率，并与可执行借记和赔付并列；",
        "不得先验地把 call 墙 fade 或 put 墙 bounce 分类成高/低概率，也不得把风险中性触达率写成真实胜率。",
        "五、预期波幅是当天的尺子。所有『大涨大跌』都可以除以 EM 描述已使用比例，",
        "但 EM 使用量不能换算成未提供的尾部概率、剩余空间或胜率。",
        "六、先读市场隐含定价。B-L 分布与触达概率是风险中性启发，不是物理概率、真实胜率或 13:00 区间。",
        "你的增量在于把现价、结构边界、可执行 NBBO、IV/skew 与可证伪路径并列；",
        "不得从公开报价推断谁被迫交易、哪边止损密或某个参与者正在防守。",
        "七、按搭档的钟表说话，不按纽约的。他的一天：北京 8:30-14:00 是亚盘夜盘(Globex+GTH，流动性薄，",
        "复盘+搭骨架+挂远端埋伏单)；14:00-20:30 是欧盘(ES 开始有真方向尝试，研究和布挂单的黄金窗)；",
        "20:30 美国宏观数据落地，EM/IV 重定价，挂单最后校准；21:30 美股开盘，首小时假突破多，等回踩；",
        "22:30 后覆盖完整 RTH；13:00-15:30 ET 仍允许新的 MANUAL READY，15:30 后只管理退出。"
        "这些时段里市场一直在交易——『等开盘再说』不属于有效解释。",
        "几乎全天都是废话，每个时段都有该干的活，便签要落在当前时段的语境里。",
        "任何 RTH 内的 ES 数据都属于日内路径确认，不得称为 GTH/夜盘，也不得套用薄流动性解释。"
        "午盘确认原则不覆盖硬止损、结构失效和仓位风险上限；这些条件触发就立即按纪律处理。",
        "八、15:30 ET 后停止新入场，15:45 ET 前撤销未成交意图并退出本策略 0DTE；",
        "不得建议 bracket 后继续持有、带保护睡觉或把下午尾盘风险留给无人值守账户。",
        "",
        "══ 你这种徒弟最常犯的错，我点名，你自查 ══",
        "1. 把数据罗列当分析：报了十个数字没有一个判断。每个数字后面必须跟一个『所以』。",
        "2. 生产只保留一个最终候选；竞争假设只写触发、反证与证伪，不做固定 Call/Put 排名，也不升级为订单。",
        "3. 把靠近结构档当确认：负 gamma/zero gamma 交叉区里，接近墙位本身既不确认支撑，也不确认危险；",
        "   必须等待价格接受/拒绝路径。",
        "4. 建议里没有时间的位置感：只说挂在哪，不说时间衰减在这单里帮谁、几点之后这单变质。",
        "5. 抄 JSON：把输入复述一遍交差。只挑改变决策的 3-5 个数字，其余扔掉。",
        "6. 口气像研报、客服或喊单群：『建议投资者密切关注』『半路不追』『准备起飞』都不合格。",
        "   使用机构执行语言，区分 Desk View、Execution、Risk 与 Targets；不用感叹号，不写免责声明。",
        "7. 不认错不更新：上一条便签的判断被市场证伪了就明说『上一条看错了，错在哪』，",
        "   然后翻剧本。死扛上一条结论比看错更不可原谅。",
        "8. 编数字：数字一律照抄输入的 JSON 与模板，不四舍五入、不换算；缺数据就说缺；",
        "   数据 degraded 时如实说明并拒绝基于坏数据给方向判断。",
        "",
        "══ 示范：同一个局面的两种写法 ══",
        "【不合格】『当前 SPX 位于 7471，put wall 位于 7450，call wall 位于 7500，VIX 16.9，",
        "日内下跌 32.9 点，建议密切关注关键位表现，防范下行风险。』",
        "——数字全对，一个判断没有，跟没写一样。",
        "【合格】『Desk View：SPX 7471 位于 flip 上沿，7455 以下 gamma 放大风险上升。",
        "Execution：当前价格不具备新增 Put 的风险回报；7450C 仅在 7455 上方继续有效。",
        "Risk：有效跌破 7455 撤销 Call 判断。Targets：重新接受 7495 后评估 7500。",
        "日内已使用 EM 的 171%；输入没有独立剩余路径分布，因此不把该比例换算成尾部概率或剩余空间。』",
        "——一个主判断、一个执行条件、一个失效位，数字全部服务于决策。",
        "",
        "写完自查一遍：搭档扫完这条便签，知不知道市场在干什么、他的单要不要动、什么情况下你是错的。",
        "三个有一个答不上来，重写。",
        "",
        "══ 排版：飞书卡片 / Bark 详情要能扫 ══",
        "输出用轻量 Markdown，方便飞书卡片和 Bark 详情页渲染重点：",
        "- 用 ## 小标题分区（Desk View / Execution / Risk / Targets / Data Quality），不要一整坨散文；",
        "- 关键数字和结论用 **加粗**；限价、触达概率用 `行内代码`；",
        "- 列表用 - 开头；不要用表格、不要用 HTML、不要用图片；",
        "- 内部协议行若存在，只用于路由；人类正文不得重复『需要看盘』等协议短语。",
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
    provider_order: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "LlmWriterSettings":
        load_dotenv()
        return cls(
            enabled=env_bool("SPX_PUSH_LLM_ENABLED", bool(settings_value("push_llm.enabled"))),
            model=os.getenv("SPX_PUSH_LLM_MODEL", str(settings_value("push_llm.model"))).strip(),
            url=os.getenv(
                "SPX_PUSH_LLM_URL",
                str(settings_value("push_llm.url")),
            ).strip(),
            env_file=os.getenv(
                "SPX_PUSH_LLM_ENV_FILE",
                str(settings_value("push_llm.env_file")),
            ).strip(),
            timeout_seconds=float(
                os.getenv(
                    "SPX_PUSH_LLM_TIMEOUT_SECONDS",
                    str(settings_value("push_llm.timeout_seconds")),
                )
            ),
            # deepseek-v4-pro is a reasoning model: the chain-of-thought also
            # consumes completion tokens (observed ~2000 reasoning tokens per
            # report), so leave generous headroom or the visible content comes
            # back empty with finish_reason=length.
            max_tokens=int(
                os.getenv("SPX_PUSH_LLM_MAX_TOKENS", str(settings_value("push_llm.max_tokens")))
            ),
            provider_order=_provider_order(),
        )


def _provider_order() -> tuple[str, ...]:
    configured = os.getenv("SPX_PUSH_LLM_PROVIDER_ORDER", "").strip()
    raw: object = configured.split(",") if configured else settings_value("push_llm.provider_order")
    if not isinstance(raw, list | tuple):
        return ("deepseek", "openclaw")
    # Grok is intentionally not a runtime writer candidate.  Keep DeepSeek
    # first even when an old environment still carries the former provider
    # order; OpenClaw remains the optional analysis fallback.
    allowed = {"deepseek", "openclaw"}
    providers = tuple(
        dict.fromkeys(str(provider).strip().lower() for provider in raw if str(provider).strip())
    )
    fallbacks = tuple(
        provider for provider in providers if provider in allowed and provider != "deepseek"
    )
    return ("deepseek", *fallbacks)


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
    json_mode: bool = False,
) -> tuple[str | None, str | None]:
    """Return (text, error). Callers fall back to the deterministic template on error."""
    settings = settings or LlmWriterSettings.from_env()
    if not settings.enabled:
        return None, "disabled"
    key = api_key(settings)
    if not key:
        return None, "missing DEEPSEEK_API_KEY"
    response_options: dict[str, object] = {}
    if json_mode:
        response_options["response_format"] = {"type": "json_object"}
    try:
        response = OpenAI(
            api_key=key,
            base_url=settings.url.removesuffix("/v1/chat/completions").removesuffix(
                "/chat/completions"
            ),
            timeout=settings.timeout_seconds,
        ).chat.completions.create(
            model=settings.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=settings.max_tokens,
            **response_options,
        )
    except OpenAIError as exc:
        return None, str(exc)
    content = (response.choices[0].message.content or "").strip() if response.choices else ""
    if not content:
        return None, "empty response"
    return content, None


def call_hypothesis_critic(
    radar: dict[str, Any], settings: LlmWriterSettings | None = None
) -> tuple[dict[str, Any] | None, str | None]:
    """Return only structured critique whose fact references exist in the deterministic packet."""

    allowed_rows = {
        str(row.get("scenario")): {
            "facts": {
                str(fact.get("ref"))
                for fact in row.get("supporting_facts") or ()
                if isinstance(fact, dict) and fact.get("ref")
            },
            "falsifiers": set(map(str, row.get("falsifiers") or ())),
        }
        for row in radar.get("hypotheses") or ()
        if isinstance(row, dict) and row.get("scenario")
    }
    system = (
        "You are a hypothesis critic. Output json only: "
        '{"hypotheses":[{"kind":"...","supporting_fact_refs":["..."],'
        '"contradictions":["..."],"falsifiers":["..."],'
        '"eligible_expressions":["vertical|butterfly|no_trade"]}]}. '
        "Never create prices, probabilities, utility, contracts, or execution authority."
    )
    text, error = call_llm_writer(
        json.dumps(radar, ensure_ascii=False, separators=(",", ":")),
        system=system,
        settings=settings,
        json_mode=True,
    )
    if text is None:
        return None, error
    try:
        result = json.loads(text)
        rows = result["hypotheses"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        return None, f"invalid_hypothesis_json:{exc}"
    expressions = {"vertical", "butterfly", "no_trade"}
    if not isinstance(rows, list) or any(
        not isinstance(row, dict)
        or str(row.get("kind")) not in allowed_rows
        or not set(map(str, row.get("supporting_fact_refs") or ())).issubset(
            allowed_rows.get(str(row.get("kind")), {}).get("facts", set())
        )
        or not set(map(str, row.get("falsifiers") or ())).issubset(
            allowed_rows.get(str(row.get("kind")), {}).get("falsifiers", set())
        )
        or not set(map(str, row.get("eligible_expressions") or ())).issubset(expressions)
        for row in rows
    ):
        return None, "hypothesis_fact_or_expression_validation_failed"
    return result, None


# --- push continuity: remember the last push so the next writer can say
# "剧本维持/剧本有变" instead of starting from amnesia ---

PUSH_CONTEXT_MAX_CHARS = 1600


def default_push_context_path() -> str:
    data_root = (
        os.getenv("MARKET_DATA_DATA_ROOT")
        or os.getenv("MAINTENANCE_DATA_ROOT")
        or str(settings_value("maintenance.data_root"))
    )
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
    system: str | None = None,
) -> tuple[str, str]:
    """Return generated text and provider, with deterministic template fallback."""
    writer_settings = LlmWriterSettings.from_env()
    for provider in writer_settings.provider_order:
        if provider == "deepseek":
            reply, error = call_llm_writer(
                prompt,
                system=system or DEFAULT_SYSTEM_PROMPT,
                settings=writer_settings,
            )
            if reply:
                return reply, "deepseek"
            if error and error != "disabled":
                print(f"llm_writer: deepseek failed ({error}); falling back", file=sys.stderr)
        elif provider == "openclaw" and settings.openclaw_agent_enabled:
            sink, reply = run_openclaw_agent(settings, prompt, runner=runner)
            if sink.ok and reply:
                return reply, "openclaw_agent"
    return template, "template"
