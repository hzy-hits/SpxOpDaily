"""LLM prompt builders for order-map and status pushes."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from spx_spark.application.order_map.models import PLAY_ORDER, SHANGHAI_TZ
from spx_spark.application.order_map.render import (
    _candidate_by_play,
    _dash,
    _day_move_line,
    _es_volume_line,
    _fmt_prob,
    _greeks_reference_line,
    _hl_volume_line,
    _rn_density_line,
    _wall_ladder_lines,
    render_research_only_template,
)
from spx_spark.application.order_map.state import _phase_clock_text, _session_phase_of
from spx_spark.notifier.llm_writer import previous_push_json


def build_order_prompt(
    payload: dict[str, Any],
    template: str,
    previous_push: dict[str, Any] | None = None,
) -> str:
    writer_payload = {key: value for key, value in payload.items() if not key.startswith("_")}
    if payload.get("research_only") is True:
        return "\n".join(
            (
                "这是 research_only 市场观察。只复述研究参考、闸门原因、结构位距离和原始 bid/ask。",
                "不得给挂单、限价、重定价、触达概率、ETA、买卖建议或可执行措辞。",
                "首行写『研究状态:』，末行明确『不可执行定价』。",
                "JSON:" + json.dumps(writer_payload, ensure_ascii=False, separators=(",", ":")),
                "模板:" + template,
            )
        )
    return "\n".join(
        (
            "这条是当天第一张『挂单地图』。搭档下午刚坐到屏幕前，要拿这张图定今天的埋伏方案：挂什么单、挂什么价、赌的是什么。",
            "动笔前先在心里过一遍(不写出来)：今天的 OI 是怎么摆的——put 侧是密集防线还是孤零零一档？dealer 在现价附近是"
            "正 gamma 压波动还是负 gamma 放大波动？今天的 play 里哪张是真机会、哪张只是模板凑数？想清楚再落笔，观点要有取舍，"
            "所有候选同等推荐等于没推荐。",
            "框架口径：Micopedia/Steven observe_only（regime→map→flow→trigger→expression→exit）；"
            "挂单地图是计划参考不是自动下单；GEX/*_proxy 是结构代理；Hyperliquid 只作弱次级证据，不作 SPX 锚。",
            "",
            "输出中文，最多 18 行。第一行以『挂单参考:』开头，复述模板第一行的日期与时间。",
            "接着给地形定调：pin 还是 transition，为什么(gamma 状态+价格相对 flip 的位置)，今天哪类 play 优先。",
            "墙位讲阶梯不讲孤点(数据在 wall_ladder，OI 定位 + 每档 BS 参考价)：相邻 put 墙 OI 接近(差三成以内)就说成一条支撑带并给出"
            "破了之后的二、三档；第一档独大才说单点硬墙。call 侧同理。"
            "每档 put 墙对应 Call 的到位预估/限价，每档 call 墙对应 Put 的到位预估/限价——写挂单时必须引用这些数字，不要自己估权利金。",
            "rn_density(B-L 风险中性分布)可用时引用：市场把收盘定价在哪个中位、80% 区间在哪；给垂直价差选腿时"
            "买腿放赌的方向内、卖腿放 80% 区间外沿附近最划算；quality 非 ok 时注明并降权。",
            "spxw_0dte_greeks_reference 是严格当日到期、只读的情景参考层，只解释价格/时间/IV 冲击。"
            "position_sign/direction=unknown 时负 gamma 不等于下跌；不得据此改变候选方向、排序、限价或新增下单动作。",
            "conditional_call_bias 只有 status=confirmed 才有效，它来自 5 秒 SPX/ES 价格路径对冻结 flip/旧 call wall 的确认，"
            "不是 Gamma 猜方向；confirmed 时优先讲对应 call 的回踩位与失效线，watch/neutral 不新增动作。",
            "",
            "然后逐条 play(最多 3 条；conditional_call_bias confirmed 时用对应 Call 替换已被证伪的同层 Put；每条 2-3 行)，每条都要把账算给他看：",
            "- 墙位价 vs 先手挡价的取舍：墙位价便宜但常在墙前几点反转吃不到，先手挡成交率高；预估价已含触达前的"
            "时间衰减与 vol 斜率(BS 重定价)，比现价低不是便宜，是时间价值正常流失；",
            "- 赔率账：触达概率、到位预估价、现价放一起，这笔单赌的是一次多大概率的什么事，赔付幅度配不配得上这个概率；",
            "- resting_limit 提醒：0DTE 纯权利金限价可能因时间衰减在指数未到位时提前成交，严格按点位入场改用指数条件单(SPX 触及 XX 时下限价)；",
            "- order_style=stop_trigger 必须提醒：预估价高于现价，预挂被动限价会立即成交，等破位确认后用条件单。",
            "",
            "最后 2-3 行 if/then：开盘前参考价/ES 走到哪些具体位置，哪张单赔率变差该撤或改价，哪个剧本作废——这就是这张图的证伪条件。",
            "es_volume 可用且 label 非 no_baseline/session_reset 时，读量价事件(event_id)而不是只读放量/缩量："
            "字段含 direction(涨跌)、location(贴墙/flip/中间/破位侧)、sequence、break_outcome(holds/reclaimed/pending)、play_hints。"
            "用法按 play 对号入座——put wall 反弹想看 elevated_sell_into_support 后出现 quiet_reclaim_after_sell_test；"
            "flip 破位 put 想看 elevated_break_holds / quiet_breakdown_holds，最怕 break_reclaimed；"
            "call wall fade 想看 elevated_buy_into_resistance 后滞涨，最怕 quiet 站稳在墙上方。"
            "quiet_mid_range / elevated_mid_range 都是半路，不追单。",
            "hl_volume(HL SP500 永续，24/7 薄代理)只当次级证据：与 ES 同向加一分确认，分歧提示 crypto 侧先动或噪声；"
            "aggressor_buy_ratio/book_imbalance 是 ES 没有的方向色彩；ES 停盘/周末时它是唯一量价源，但绝不单独确认破位。",
            "每张单的 touch_eta_minutes 是按布朗缩放估的到位耗时：给出时效纪律——约 2 倍该时间价格还没来，"
            "赔率已被 theta 吃掉，写明大约几点(北京)前不来就撤单。",
            "session_phase 是搭档的时钟：这张图会跨欧盘、美盘数据小时和开盘使用，建议要写清哪些单是欧盘就能成交的埋伏、"
            "哪些要等美盘数据落地校准后才算数；不许把『等开盘』当默认建议。",
            "day_move.em_used_fraction ≥ 0.7 时点明：日内已走完预期波幅的多少，顺方向追单赔率差；挂单纪律是等价格来找你，不去半路追它。",
            "previous_push 是上一条推送正文；关键位相对它有实质变化就在定调处说『剧本有变』并指出哪张单要改，没变化不必提。",
            "previous_push:" + previous_push_json(previous_push),
            "JSON:" + json.dumps(writer_payload, ensure_ascii=False, separators=(",", ":")),
            "模板:" + template,
        )
    )



def _level_probs_line(payload: dict[str, Any]) -> str:
    by_play = _candidate_by_play(payload)
    parts: list[str] = []
    for play in PLAY_ORDER:
        candidate = by_play.get(play)
        if candidate is None:
            continue
        parts.append(
            f"{candidate.get('level_label') or '-'} 触达≈{_fmt_prob(candidate.get('prob_touch'))}"
        )
    return "; ".join(parts) if parts else "-"


def render_status_template(
    payload: dict[str, Any],
    changes: list[str],
    now_utc: datetime,
) -> str:
    if payload.get("research_only") is True:
        return render_research_only_template(payload, title="市场研究状态")
    beijing = now_utc.astimezone(SHANGHAI_TZ)
    phase = _session_phase_of(payload, now_utc)
    open_text = _phase_clock_text(phase)

    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    vol = payload.get("vol_context") if isinstance(payload.get("vol_context"), dict) else {}
    flip_zone = payload.get("flip_zone") if isinstance(payload.get("flip_zone"), list) else None
    flip_lo = _dash(flip_zone[0]) if flip_zone and len(flip_zone) >= 2 else "-"
    flip_hi = _dash(flip_zone[1]) if flip_zone and len(flip_zone) >= 2 else "-"

    lines = [
        f"【市场状态 {beijing.strftime('%H:%M')}】(0DTE={payload.get('expiry') or '-'}, {open_text})",
        f"时段: {phase.get('name_cn')} — {phase.get('traits')}",
        (
            f"参考价: {_dash(underlier.get('price'))}({underlier.get('source') or '-'}); "
            f"ES {_dash(payload.get('es_last'))}; HL perp {_dash(payload.get('hl_sp500_perp'))}"
        ),
        (
            f"gamma: {payload.get('gamma_state') or '-'}, "
            f"zero gamma {_dash(payload.get('zero_gamma'))}, flip zone {flip_lo}-{flip_hi}, "
            f"预期波幅 ±{_dash(payload.get('expected_move_points'))} 点"
        ),
        *([line] if (line := _greeks_reference_line(payload)) else []),
        f"关键位: {_level_probs_line(payload)}",
        *([line] if (line := _day_move_line(payload)) else []),
        *([line] if (line := _es_volume_line(payload)) else []),
        *([line] if (line := _hl_volume_line(payload)) else []),
        *_wall_ladder_lines(payload),
        *([line] if (line := _rn_density_line(payload)) else []),
        (
            f"vol: VIX {_dash(vol.get('vix'))}, VIX1D {_dash(vol.get('vix1d'))}, "
            f"VVIX {_dash(vol.get('vvix'))}, SKEW {_dash(vol.get('skew'))}"
        ),
    ]
    if changes:
        lines.append(f"较上次推送变化: {'; '.join(changes)}")
    else:
        lines.append("较上次推送: 关键位无实质变化")

    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.append(f"数据警告: {'; '.join(str(item) for item in warnings)}")
    return "\n".join(lines)


def build_status_prompt(
    payload: dict[str, Any],
    template: str,
    previous_push: dict[str, Any] | None = None,
) -> str:
    writer_payload = {key: value for key, value in payload.items() if not key.startswith("_")}
    if payload.get("research_only") is True:
        return "\n".join(
            (
                "这是周期性 research_only 市场状态，不是交易提醒。",
                "只说明研究参考、定价闸门、结构位距离和观察到的 bid/ask。",
                "不得产生限价、概率、ETA、模型价格、买卖或下单建议；明确写不可执行定价。",
                "JSON:" + json.dumps(writer_payload, ensure_ascii=False, separators=(",", ":")),
                "模板:" + template,
            )
        )
    return "\n".join(
        (
            "这条是『市场状态+挂单参考』二合一便签，北京 07:30 到次日 01:30 每 15 分钟一条。搭档已经按上一张地图挂好单了，"
            "他扫一眼要能决定：单动不动、价来了接不接。这不是行情播报，是接班交接——上一班的判断在 previous_push 里，"
            "你要么确认它还成立，要么指出哪里被市场证伪了。",
            "",
            "动笔前先在心里过一遍(不要写出来)：现在价格站的这个位置，是谁的地盘？下方 put 墙的 OI 是真金白银的防守还是昨天的尸体？"
            "dealer 在这个价位是被迫买还是被迫卖(gamma 正负)？时间衰减在帮谁？想清楚了再写结论。",
            "框架口径：Micopedia/Steven observe_only；*_proxy 曝露不是 vendor DEX；Hyperliquid 不作 SPX 锚；不下单授权。",
            "",
            "输出中文，14-20 行。第一行以『市场状态:』开头，保留模板第一行的时间与时段信息，紧跟一句定调：",
            "『剧本维持』或『剧本有变: 变在哪』——判断基准是 previous_push 正文和模板『较上次推送』行。",
            "",
            "session_phase 是搭档的时钟：便签必须落在当前时段的语境里(traits 字段是提示)。亚盘/欧盘时段市场在交易",
            "(Globex+GTH)，不许写『等开盘再说』——该说的是这个时段的地形有多可信、埋伏单摆哪里；开盘首小时提防假突破；",
            "主战场时段(北京 22:30-1:00)直接谈执行。minutes_to_bedtime ≤ 60 时这条是【睡前收官】便签：",
            "正文以收官为主——逐张说未成交挂单撤/留、持仓带什么 bracket(止盈止损给具体价)、哪些单绝不能裸奔进无人值守的",
            "美盘下午；证伪条件写给醒着的最后一小时，而不是写给睡着的他。",
            "",
            "正文必须覆盖(顺序自己组织，写成连贯的段落而不是清单)：",
            "- 位置：参考价在 flip zone/zero gamma/两侧墙位阶梯里站在哪，距各关键位几点，这个位置意味着 pin 还是易加速；"
            "相邻 put 墙 OI 接近(差三成以内)就说成支撑带并报出二、三档，别只报一个点；"
            "墙阶梯每一档都带对应期权的 BS 到位预估价与限价(put 墙→Call，call 墙→Put)，写支撑/阻力时顺手带上参考价，别只报 strike；",
            "- 赔率：当前候选的触达概率各多少、相对上一条谁在改善谁在恶化(引用具体百分比变化)，此刻哪张性价比最高、为什么；",
            "- 市场定价对照(rn_density quality=ok 时)：市场把收盘定价在哪个中位、80% 区间在哪，当前价格相对它偏回归还是已到尾部；",
            "- vol：VIX1D/VIX 说明今天的 vol 卖得贵还是便宜，SKEW 异常时说明谁在抢保护；",
            "- 0DTE Greeks：只把 spxw_0dte_greeks_reference 当价格/时间/IV 冲击参考。position_sign/direction=unknown 时"
            "负 gamma 不等于下跌，不得用它改变候选方向、排序或限价；",
            "- conditional_call_bias：只有 confirmed 才用 flip_reclaim_call 替换同层 flip_breakdown_put，或用"
            " call_wall_breakout_call 替换同层 call_wall_fade_put；它来自 5 秒 SPX/ES 对冻结关键位的确认，"
            "不是 Gamma 方向票。watch/neutral 不改变候选；",
            "- 量价事件(es_volume 可用时)：不要只说放量/缩量。读 event_id + direction + location + break_outcome："
            "放量砸支撑(elevated_sell_into_support)是墙测试不是自动破位；缩量收回(quiet_reclaim_after_sell_test)才升温反弹；"
            "破位站稳(holds)才给破位单开灯，破位收回(reclaimed)则假破降权；中间地带(quiet/elevated_mid_range)半路不追。"
            "play_hints 里有现成句子可直接引用。label=no_baseline/session_reset 时不引用；",
            "- hl_volume 是 Hyperliquid SP500 永续的量价(24/7 薄流动性代理)，只当次级证据：与 ES 量价同向可加一分确认，"
            "分歧时提示 crypto 侧资金先动或只是噪声；aggressor_buy_ratio(主动买占比)和 book_imbalance(盘口失衡)是"
            "ES 给不了的方向色彩；周末与 ES 停盘时它是唯一量价源。绝不允许单独用它确认破位；",
            "- 每张挂单的 touch_eta_minutes 是按布朗缩放估的到位耗时：写挂单参考时给出时效纪律——超过约 2 倍该时间"
            "价格还没来，这单的赔率已被 theta 吃掉，写明大约几点(北京时间)前不来就撤；",
            "- 双向 if/then：上行到哪个具体位置、下行到哪个具体位置，分别哪张单该撤/改价、哪个剧本激活——这也是你这个判断的证伪条件；",
            "- 情绪拦截(违反即失职)：价格在中间地带就写明『此处不追单，计划位在 XX/XX』；day_move.em_used_fraction ≥ 0.7 就写明"
            "『日内已走完预期波幅的 X%，顺方向追单赔率差』；价格进 put 墙支撑带就写明『计划中的接多区不是恐慌区，防守只在跌破 XX 后执行』，"
            "进 call 墙带对称处理。哪条情况成立写哪条，都不成立就不硬写。",
            "",
            "倒数第二段固定是『挂单参考』段，3-8 行：从模板的挂单地图部分逐字引用每张单的合约、墙位限价、先手挡价、触达概率，"
            "数字照抄不改写；stop_trigger 的 play 保留『勿预挂限价、用指数条件单』提醒；限价相对上一条有变化就点出方向。",
            "最后 1 行：到下条推送之间最值得盯的一个量，以及它变到什么程度你会改判断；"
            "这个量必须是本系统数据里有的(参考价/触达概率/gamma 状态/墙位 OI/VIX/VIX1D/ES 量能节奏)，"
            "不要让搭档去盯我们不推送的量(如内盘外盘)。",
            "",
            "剧本维持时照样给完整读数，别因为『没变』缩成三行；也别硬编不存在的变化，数字平稳就说平稳。",
            "previous_push:" + previous_push_json(previous_push),
            "JSON:" + json.dumps(writer_payload, ensure_ascii=False, separators=(",", ":")),
            "模板:" + template,
        )
    )
