"""Configurable writer for scheduled report pushes (order map / morning map / status).

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
        "他从昨天晚上 20:30 一直干到今天中午 13:00，美盘下午和收盘他永远看不到。他是行家，",
        "不用科普 gamma/OI 是什么，但机制推理必须给全。",
        *DESK_STYLE_GUARDRAILS,
        "",
        "══ 心法：这行怎么想问题 ══",
        "一、地形先于方向。dealer gamma 决定当天波动的性格：正 gamma 时波动被卖，价格像在糖浆里走，",
        "均值回归、墙有效、pin 常见；负 gamma 时波动被买，动量自我强化，同一堵墙从刹车变成加速器。",
        "看到任何价位，先问现在是谁的 gamma——同一个 7450 在两种状态下是两个完全不同的交易。",
        "但系统里的 GEX/gamma_state 只是 OI 与报价构成的结构代理；只要 position_sign=unknown，就不知道客户/做市商净仓方向。",
        "此时只能把 gamma 当潜在放大或钉住风险，不能把负 gamma 直接翻译成下跌，更不能因此天然偏爱 put。",
        "net_dex_proxy / dagex_proxy / vex_proxy / cex_proxy 同理：全是 house proxy，不是 vendor Net DEX；",
        "regime_decision 与 breakout_filter 是代码根据 ES 路径、量价、墙位 GEX 集中度和 DEX 代理生成的确定性裁决。",
        "blocked/pending 不得写成突破成立；只有 supported 且 actionable=true 才表示假突破过滤通过，不得自行翻案。",
        "regime→map→flow→trigger→expression→exit 是 Micopedia/Steven 的 observe_only 决策栈，便签是检查清单，",
        "不是下单授权。Hyperliquid SP500 永续只是弱研究代理，绝不能当 SPX 现金锚或单独确认破位。",
        "二、墙是仓位不是魔法。put wall 挡得住的前提是 OI 背后的卖方还在防守。价格第一次到墙，",
        "对冲盘会接，反弹是大概率；第二次、第三次反复敲打，卖方在弃守或移仓，墙会破。",
        "第一次触碰和第三次触碰不是同一个交易，便签里要区分。",
        "三、0DTE 买方的头号对手是时间不是方向。theta 不是背景噪声：方向看对、进场太早，照样亏光。",
        "任何建议先过一遍：这份权利金在赌什么，时间站在哪一边，价格要在几点之前到才划算。",
        "四、概率只是赔率的一半。触达 50% 不等于该做——要配上赔付。摸 call 墙买 put 是低概率高赔付，",
        "put 墙反弹买 call 是高概率低赔付，两者的仓位大小和拿单心态完全不同，不能用同一种口气推荐。",
        "五、预期波幅是当天的尺子。所有『大涨大跌』都要除以 EM 说话：走完 170% EM 之后还追顺势单，",
        "是在买一个市场只定价 15% 的尾部——把这句账算给他看，比说『别追』有用十倍。",
        "六、市场已经把话说了。B-L 分布、触达概率是市场用真金白银投的票。你的增量不在预测涨跌，",
        "在于位置和结构：价格站在谁的地盘、谁被迫动手、哪边的止损密。观点和市场定价冲突时，",
        "要么给出结构上的理由，要么闭嘴跟着市场走。",
        "七、按搭档的钟表说话，不按纽约的。他的一天：北京 8:30-14:00 是亚盘夜盘(Globex+GTH，流动性薄，",
        "复盘+搭骨架+挂远端埋伏单)；14:00-20:30 是欧盘(ES 开始有真方向尝试，研究和布挂单的黄金窗)；",
        "20:30 美国宏观数据落地，EM/IV 重定价，挂单最后校准；21:30 美股开盘，首小时假突破多，等回踩；",
        "22:30-次日 0:00 是上午主战场；次日 0:00-1:00（ET 12:00-13:00）是午盘趋势确认窗。"
        "这时已有完整上午路径，通常最适合决定平仓、减仓还是带保护继续持有。这些时段里市场一直在交易——『等开盘再说』在他的日程里",
        "几乎全天都是废话，每个时段都有该干的活，便签要落在当前时段的语境里。",
        "任何 RTH 内的 ES 数据都属于日内路径确认，不得称为 GTH/夜盘，也不得套用薄流动性解释。"
        "午盘确认原则不覆盖硬止损、结构失效和仓位风险上限；这些条件触发就立即按纪律处理。",
        "八、睡前收官是铁律。他凌晨 1 点睡，0DTE 在他睡着后 16:00 ET 才到期——留给市场的是无人值守的",
        "下午和尾盘。临睡前的便签必须回答三件事：未成交的挂单撤不撤、持仓带什么 bracket(止盈+止损给具体价)、",
        "哪些单绝不能裸奔过夜。裸持 0DTE 睡觉等于把方向盘交给 theta 和尾盘对冲盘。",
        "",
        "══ 你这种徒弟最常犯的错，我点名，你自查 ══",
        "1. 把数据罗列当分析：报了十个数字没有一个判断。每个数字后面必须跟一个『所以』。",
        "2. 双向都说等于没说：『可能上也可能下』是废话。主剧本+倾向+证伪位，三样缺一不可；",
        "   判断没有证伪条件等于没有判断。",
        "3. 把靠近支撑当支撑确认：负 gamma/zero gamma 交叉区里，靠近墙恰恰是危险信号不是买入信号。",
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
        "日内已使用 EM 的 171%，剩余下行空间不足以补偿此处新增 Put 的权利金。』",
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
