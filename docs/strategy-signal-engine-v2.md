# SPX Spark 0DTE 算法与策略信号引擎设计 v2

状态：**实施合同（Implementation Contract）**
适用仓库：`hzy-hits/SpxOpDaily`
目标路径：`docs/strategy-signal-engine-v2.md`
自动下单：禁止
生产接入方式：历史因果回放通过后，直接进入人工可见的 Manual Candidate，不再额外等待 20 个交易日 Shadow
最后更新：2026-08-15

> **协同基线**：本文档与架构简化工作的排期、依赖白名单、配置与 Rust 边界裁决见
> `docs/architecture-simplification-execution-plan-v1.md` 第 2 节（协同裁决 11–15）与第 4 节 S-track 任务卡。
> 两份文档冲突时以执行方案为准。

---

## 0. 本文解决什么问题

当前系统已经具备较完整的数据采集、行情质量、SPX/ES 坐标、SPXW 期权链、Gamma/Wall、Greeks、回放、通知和只读安全边界，但策略决策仍存在四个根本问题：

1. 市场状态过早被压缩成 Call/Put 方向。
2. 方向确认几乎等同于入场授权，导致 Vertical 容易追涨杀跌。
3. 策略候选空间过于封闭，缺少 Butterfly、失败突破、Pin/De-pin 和正式 NoTrade。
4. “灵感”只存在于文字解释中，没有成为可验证、可计算、可比较的交易假设。

本文规定一套新的统一算法：

```text
实时事实
  -> 多维市场状态
  -> 竞争性交易假设
  -> Vertical / Butterfly / NoTrade 候选
  -> 可执行价格与净收益分布
  -> 唯一人工决策输出
```

本文不是另一份研究愿望清单。它规定：

- 当前实现如何处理；
- 哪些模块继续复用；
- 哪些模块停止拥有交易权限；
- 新算法具体计算什么；
- 信号何时生成；
- 如何选择合约；
- 如何退出；
- 如何立即使用现有一个月数据；
- GPT-5.6 Sol 实施时允许和禁止做什么。

---

## 1. 已确认的架构决策

以下决策视为默认批准，实施者不得自行改写。

### 1.1 不再等待额外 20 个交易日

仓库已经收集约一个月的行情、期权链和派生数据。正确流程是：

1. 立即用已有数据重建 point-in-time 决策快照；
2. 完成因果回放、费用与滑点测试；
3. 通过本文规定的验收门；
4. 直接进入生产报告和人工候选卡；
5. 后续实时数据继续作为监控和校准，不作为“永远不能上线”的借口。

可以使用以下状态：

```text
model_status = bootstrap_live
automatic_ordering = false
manual_action_only = true
```

“Bootstrap”表示统计置信仍有限，不表示候选必须隐藏。

### 1.2 不新建独立服务

新算法接入现有 Order Map / Market Features 调用链。

禁止因为本设计新增：

- 新 systemd daemon；
- 新 timer；
- 新 Redis；
- 新消息队列；
- 新 operational database；
- 新 Rust service；
- 第二套 notification outbox；
- 第二套 market state writer。

### 1.3 不再让任意单个状态直接生成交易

下列内容只能成为事实或特征，不能单独发出 Trade Ready：

- Zero Gamma；
- Put Wall / Call Wall；
- GEX 符号；
- HMM state；
- VWAP 上下；
- Opening Range 突破；
- 单次接受或拒绝；
- LLM 的自然语言判断；
- 宏观偏多或偏空；
- 单一 Delta、Gamma、Vanna 或 Charm 数值。

### 1.4 策略候选第一版固定为五类

```text
NO_TRADE
CALL_DEBIT_VERTICAL
PUT_DEBIT_VERTICAL
CALL_BUTTERFLY
PUT_BUTTERFLY
```

第一版禁止加入：

- Iron Condor；
- Calendar；
- Diagonal；
- Ratio Spread；
- Broken-Wing Butterfly；
- 裸 Call / 裸 Put；
- Credit Spread；
- 自动滚仓。

这些不是永远禁止，而是不能在核心引擎尚未稳定时扩大搜索空间。

### 1.5 所有判断在 SPX 坐标中完成

策略模型统一使用：

```text
SPX price coordinates
SPXW expiry semantics
ES -> SPX qualified basis
```

XSP、SPY 或 SPX 只是执行载体。

模型不得使用 SPY 价格直接计算 SPX 中轴，也不得用 XSP 稀疏订单簿反推 SPX 终值分布。

---

## 2. 当前算法实现梳理

本节是当前仓库的算法地图。实施者在修改前必须确认这些 owner。

### 2.1 Order Map 总编排

当前 `application/order_map/service.py` 的 `build_order_payload()` 同时负责：

- 构造 Options Map；
- 解析 SPX pricing/research spot；
- 生成旧候选；
- 计算 Greeks reference；
- 调用 Greek decision；
- 生成 skew spread shadow；
- 计算 strike coverage；
- 拼装 Gamma、Walls、RN density、Macro、VIX context；
- 后续继续附加 Spring Gamma、Convexity Radar、通知和 Rust projection。

结果是：事实、策略、展示、通知和研究上下文在一个大型 payload 内逐步拼装。

新设计不要求重写整个 Order Map，但必须增加一个唯一策略出口：

```text
payload["strategy_decision"]
```

人类可见的具体交易候选只能来自该字段。

### 2.2 旧候选生成器

当前 `application/order_map/candidates.py` 主要生成：

- `put_wall_bounce_call`
- `flip_breakdown_put`
- `call_wall_fade_put`
- `flip_reclaim_call`
- `call_wall_breakout_call`
- level-decision 派生 Call/Put

其核心流程是：选择结构位 → 找结构位附近的单只 Call 或 Put → 使用 Delta/Gamma 或 Black-Scholes 投影“标的到达结构位时”的期权价格 → 给出 `prob_touch`、`prob_close_beyond` 和 underlier-triggered limit → 后续由其他层再将部分候选包装成 spread。

它适合做结构位价格观察和单腿重定价参考，不适合继续拥有最终交易选择权。

迁移后：

```text
旧 candidates = legacy_reference_only
new strategy_decision = sole human candidate authority
```

### 2.3 GTH Dip-Reclaim

当前 GTH detector 使用 ES 的 15/60 分钟窗口：检测回撤 → 要求一定下降持续时间 → 要求收复一定比例 → 累积 confirm samples 和 hold seconds → 冻结一个 Call Debit Spread；长腿接近当前隐含 SPX，短腿优先锚定上方 Flip/Call Wall，没有墙位时用 Expected Move 或默认宽度。

问题不是 detector 完全无效，而是它把**形态确认、方向判断、价差构造、入场授权**绑定在同一个事件中。

迁移后：

- GTH Dip-Reclaim 只输出 `hypothesis_evidence`；
- 不再独立生成绿色 Manual Ready；
- 统一策略引擎评估它属于：早期失败下破 / 可接受的回踩入场 / 已经 Late Chase / 宏观事件前 NoTrade。

### 2.4 RTH 5 分钟状态

当前八变量状态包括：price vs VWAP、VWAP slope、Opening Range、market structure、efficiency ratio、VWAP cross count、same-time range ratio、sector breadth proxy。

输出：D（方向分数）、Q（路径质量）、V（波动环境）、`TREND_UP / TREND_DOWN / LOW_VOL_RANGE / HIGH_VOL_CHOP / UNCERTAIN`。

该层继续保留，但职责改为：**提供标准化路径特征，不负责选择交易策略。**

尤其：

- LOW_VOL_RANGE 不等于 Butterfly；
- TREND_UP 不等于立即买 Call；
- TREND_DOWN 不等于立即买 Put。

### 2.5 Convexity Idea Radar

当前 Radar 固定四种边界假设：下方拒绝→Call；下方接受→Put；上方拒绝→Put；上方接受→Call。

它已经意识到：LLM 不能编造数值；风险中性终值分布不等于现实概率；多空假设应同时存在；需要 NoTrade 和反证。

但当前候选空间仍然是封闭的，并且策略窗口在 13:00 ET 结束，无法承载尾盘 Pin Butterfly。

迁移后：

- Radar 不再是独立策略系统；
- 其事实提取、冲突描述和 LLM prompt 可以复用；
- 固定四假设和三条 lane 不再拥有最终优先级；
- LLM 输出进入统一 HypothesisSet。

### 2.6 RN Density

当前 `analytics/options/density.py` 使用：OTM mid、Put-Call Parity 合成 Call curve、非均匀二阶差分、负密度 clipping、重新归一化，输出 P10/P25/Median/P75/P90。

第一版继续将其作为 Q_baseline，但必须新增：

- `mode_strike`；
- 每个 5 点执行价附近的局部概率质量；
- density peak count；
- peak stability；
- clipped mass diagnostics；
- 对候选 Butterfly 盈利帐篷的 Q-mass 积分。

它仍然不能被叫做现实胜率。

### 2.7 Experimental HMM

当前固定三状态在线 Gaussian HMM 可以继续作为一个弱特征：

```text
regime_posterior
normalized_entropy
dwell_observations
```

禁止：将 state label 解释成做市商行为；让 HMM 独立决定 Call/Put；让 HMM 覆盖实时价格反证；为了本次策略重写复杂 HMM 框架。

---

## 3. Edge 的正式定义

系统的 edge 定义为：现实条件分布 P、市场隐含定价 Q、执行成本、尾部风险与模型不确定性之间的差值。

具体到策略 j：

```text
Edge_j = E^P[Π_j | X_t] − Premium_exec_j − Fees_j − Slippage_j
```

其中：

- `X_t`：决策时点可见事实；
- `P`：现实路径或终值条件分布；
- `Q`：期权市场隐含分布；
- `Π_j`：策略收益函数；
- `Premium_exec`：保守可执行价格，不是 mid。

市场隐含分布用于解释 `E^P[Π_j] − E^Q[Π_j]`。**不得在已经减去完整市场权利金后，再重复减去 `E^Q[Π]`。**

我们的主要 edge 假设只有三类：

1. Trend Pullback Continuation
2. Failed Break / Reclaim
3. Stable Pin vs De-pin

其余状态默认 NoTrade。

---

## 4. 统一决策流程

```mermaid
flowchart TD
    A[LatestState + Order Map + 5m Bars] --> B[MarketFactPack]
    B --> C[Regime Assessment]
    B --> D[Deterministic Hypotheses]
    B --> E[Optional LLM Idea/Critic]
    C --> F[Candidate Enumerator]
    D --> F
    E --> F
    F --> G[Exact Quote + Payoff]
    G --> H[P/Q Scenario Evaluation]
    H --> I[Utility + Hard Gates]
    I --> J[NO_TRADE or One Manual Candidate]
```

统一函数概念：

```python
def build_strategy_decision(
    payload: Mapping[str, object],
    latest: LatestState,
    now: datetime,
) -> StrategyDecision:
    facts = build_market_fact_pack(payload, latest, now)
    regime = assess_regime(facts)
    hypotheses = build_competing_hypotheses(facts, regime)
    candidates = enumerate_candidates(facts, regime, hypotheses)
    evaluated = evaluate_candidates(facts, candidates)
    return select_decision(facts, regime, hypotheses, evaluated)
```

任何人类可见交易卡都必须来自这个函数的结果。

---

## 5. 数据合同

### 5.1 MarketFactPack

```json
{
  "schema_version": "market_fact_pack.v1",
  "decision_at": "2026-08-06T17:25:00Z",
  "session_date": "2026-08-06",
  "minutes_to_close": 155,
  "spot": {
    "spx": 7704.3,
    "es": 7729.8,
    "es_spx_basis": 25.5,
    "pricing_source": "official_spx"
  },
  "path": {
    "vwap": 7712.1,
    "distance_to_vwap_atr": -0.62,
    "opening_range_high": 7742.8,
    "opening_range_low": 7709.9,
    "er_15m": 0.18,
    "er_30m": 0.21,
    "er_60m": 0.26,
    "vwap_crosses_30m": 3,
    "impulse_15m_atr": -0.35,
    "last_new_high_minutes_ago": 102,
    "last_new_low_minutes_ago": 48
  },
  "value_center": {
    "spx_equivalent_15m": 7707.8,
    "spx_equivalent_30m": 7708.4,
    "spx_equivalent_60m": 7709.1,
    "drift_15m_points": -0.6,
    "drift_30m_points": -1.3,
    "drift_60m_points": -2.0
  },
  "cross_section": {
    "spy_return_bps": -16,
    "rsp_return_bps": -64,
    "qqq_return_bps": -24,
    "smh_return_bps": 119,
    "dia_return_bps": -63,
    "iwm_return_bps": 18,
    "sector_breadth_above_vwap": 0.36,
    "cap_weight_equal_weight_divergence_bps": 48,
    "cancellation_score": 0.78
  },
  "volatility": {
    "vix": 15.42,
    "vix_change_bps_15m": -8,
    "vix1d": 11.9,
    "atm_straddle": 16.5,
    "straddle_decay_15m_fraction": 0.08,
    "straddle_decay_30m_fraction": 0.14,
    "same_time_range_ratio": 0.72
  },
  "structure": {
    "zero_gamma": 7709.2,
    "flip_low": 7705,
    "flip_high": 7710,
    "put_wall": 7700,
    "call_wall": 7770,
    "q_mode": 7707.0,
    "q_median": 7706.7,
    "q_clipped_mass_fraction": 0.04
  },
  "event": {
    "state": "normal",
    "next_event": null,
    "minutes_to_event": null
  },
  "quality": {
    "status": "ready",
    "reasons": [],
    "quote_age_max_seconds": 2.1
  }
}
```

所有字段必须：

- 标明单位；
- 缺失时为 `null`；
- 不用零代替缺失；
- 有 `source_at` / `available_at` lineage；
- 决策时满足 `available_at <= decision_at`。

### 5.2 RegimeAssessment

不要再用一个互斥标签解释全部市场。使用四个维度：

```json
{
  "path_state": "BALANCED",
  "path_direction": "DOWN",
  "terminal_state": "PIN_STABLE",
  "event_state": "NORMAL",
  "entry_state": "GOOD_LOCATION",
  "confidence": 0.71,
  "reasons": [
    "low_30m_efficiency",
    "stable_value_center",
    "multiple_center_reversions",
    "vix_non_expansion",
    "cross_section_cancellation"
  ],
  "contradictions": [
    "equal_weight_breadth_still_weak"
  ]
}
```

枚举值：

```text
path_state:     TREND | BALANCED | TRANSITION | UNCERTAIN
terminal_state: PIN_STABLE | PIN_MIGRATING | NONE | UNCERTAIN
event_state:    NORMAL | SCHEDULED_EVENT_RISK | POST_EVENT_DISCOVERY
entry_state:    GOOD_LOCATION | LATE_CHASE | POOR_ASYMMETRY | INSUFFICIENT_DATA
```

这种分解避免出现“市场是 TREND_UP，所以只能买 Call”。市场可以同时是：

```text
path_state = TREND
direction = UP
entry_state = LATE_CHASE
decision = NO_TRADE
```

---

## 6. 基础特征定义

### 6.1 路径效率

```text
ER_h = |P_t − P_{t−h}| / Σ_{i=1..n} |P_i − P_{i−1}|
```

计算 15、30、60 分钟。

Bootstrap 解释：

```text
ER >= 0.45     路径较高效
0.25–0.45      混合
ER < 0.25      轮动/震荡
```

### 6.2 Value Center

第一版直接复用 ES 5 分钟 bar 的成交量加权价格：

```text
VC_h^ES = Σ_i (Price_i · Volume_i) / Σ_i Volume_i
VC_h^SPX = VC_h^ES − Basis_{ES−SPX}
```

计算 15m、30m、60m。中心漂移：

```text
drift = VC_15m^SPX − VC_60m^SPX
```

更稳妥的实现可直接使用 bar typical price：`Price_i = (H_i + L_i + C_i) / 3`。

不要新造 tick-level volume profile 引擎。

### 6.3 Cross-Section Cancellation

目标是检测：指数内部存在强烈相反力量，但市值加权 SPX 的净变化接近零。

初始定义从 `Return_SPY − Return_RSP` 出发，再加入 `SMH − RSP`、`QQQ − DIA`、sector breadth 偏离 50%。

归一化 Cancellation Score：

```text
C = clip( 0.35·z(SPY−RSP) + 0.25·z(SMH−RSP) + 0.20·z(QQQ−DIA)
        + 0.20·(1 − |2·Breadth − 1|), 0, 1 )
```

第一版可以使用历史同时间分位数替代 z-score。

### 6.4 Straddle Decay

```text
decay_15m = (Straddle_{t−15m} − Straddle_t) / Straddle_{t−15m}
```

必须使用：同一到期日、同一 ATM 定义、可用的双边 mid、明确 quote age。

若 ATM strike 发生移动，使用固定的 decision-anchor strike 计算一组，另计算 rolling ATM，二者都保存。

### 6.5 Level Penetration 与 Reclaim

对下方结构位 L：

```text
penetration = (L − min(P)) / ATR_5m
Hold = min(Close_{t−1}, Close_t) − L    # reclaim 后保持
```

上方结构反向定义。

---

## 7. Regime Assessment 规则 v1

这些阈值是 versioned bootstrap，不得声称已经证明为长期 Alpha。

### 7.1 TREND

多头趋势要求：

```text
existing D >= +6
ER30 >= 0.45
VWAP crosses 30m <= 2
price is above VWAP
VWAP slope >= +0.05 ATR
sector breadth >= 0.55
```

空头反向。趋势判断只代表路径背景，不代表立即交易。v42 起 RTH 人读 Debit
为 `EVENT_SETTLEMENT_THRESHOLD`、`ES_VOLUME_MOMENTUM`、
`PREAVERAGE15_PULLBACK`、`WALL_BREAKOUT_HAZARD`，以及 v54 明确授权的
`RTH_LEVEL_CONFIRMATION`。
墙位 hazard 是独立左侧 lane：以与冻结模型训练同源的因果 SPX 标准化一分钟路径尺度
归一化 Call/Put Wall、Zero Gamma
与剩余 EM，冻结三分类模型输出未来 15 分钟上破站稳 / 下破站稳 / 未突破概率；仅当
现价直接来自 `latest_state` 中 source/transport 均不超过 15 秒的 Schwab SPX quote，
OI-GEX 可用、同向概率至少 0.17、证据不超过 15 秒、目标结算价值下的保守执行 EV > 0
且 exact BBO、通用几何/借记、PIN 与宏观门全部通过时才可出人工卡。它必须标记
`forward-unvalidated`，不得表述为已证明 edge，也不得继承旧策略方向。
`PREAVERAGE15_PULLBACK` 是用户明确授权的独立原始 SPX 五秒路径 lane：只在
60 秒决策点使用因果 15 秒加权前均值触发，固定选 Schwab 60Δ / 15 点价差，不继承
HMM、GEX 方向、旧 entry-quality 或历史方向 stick 门；它必须标记
`forward-unvalidated`，不得表述为
已证明 edge。失败突破、趋势回踩、突破接受只记结构事实。GTH 宽链/delta 扫描继续
进入 Desk Map 与拒绝漏斗，但不再授权 Trade Ready；GTH 人读 Debit 只保留确认水平 /
回踩收复证据。Desk Map 仍写不做。GTH 同方向锁定 30 分钟；同 setup/direction 卡片
冷却 15 分钟；每个 session mode 每个方向最多接受 2 张卡。
GTH 确认方向翻转另发一张 `GTH Bias UP/DOWN · Observe` 市场警示，
同 session 同方向 30 分钟内去重。该卡不包含期权合约或入场限价，
`action_authority=none` 且 `execution_eligible=false`；必须等关键水平接受/拒绝或突破回踩确认后，
才可由统一策略决策升级为 Trade Ready。

### 7.2 BALANCED

```text
ER30 < 0.30
VWAP crosses 30m >= 2
abs(existing D) <= 3
最近30分钟没有被接受的 OR/结构突破
```

### 7.3 TRANSITION

任一满足：

- D 与 ER 冲突；
- VWAP/Opening Range/market structure 不一致；
- 刚从趋势进入低 ER；
- Value Center 仍明显迁移；
- 突破刚失败但尚未完成 retest；
- 跨市场宽度正在翻转。

Transition 仍不挡量比 setup 的枚举。Failed Break 与量比卡都只记事实，不授权
RTH 人读 Debit。GTH 扫描也只记漏斗事实。本 session 已有反向 RTH 人读卡时，翻向必须等
cash HMM 同向 TREND，不能只靠 5 分钟反弹；该规则约束 RTH 枚举。

### 7.4 PIN_STABLE

必须在 11:00 ET 以后评估。

钉住分两档，不得混用：

```text
LOOK（观察）：RTH 11:00–13:00，未迁移，输入齐，本地 Q，至少 1 次 excursion-return
TRADE（PIN_STABLE）：现有硬栈，进入仍要 2 次 excursion-return
```

LOOK 只发观察卡，不过 `butterfly_requires_pin_stable`，不能成为蝶式交易候选。
TRADE 还要求同一中轴至少 3 个决策快照、持续 10 分钟；同一中轴短暂退回
`NONE + look` 时保留首次观测时间与快照计数，但该拍本身仍不是 `PIN_STABLE`，不得生成交易卡。
`PIN_MIGRATING`、`UNCERTAIN`、`NONE + none`、跨 session 或中轴切换会重置确认。
10 点内的挑战中轴只有在评分领先至少 0.05 时才可替换已有中轴。确认完成前只显示位置，不生成交易卡。

中轴只能从当日期权链的 5 点行权价网格中选择；连续值 Zero Gamma 只是 CenterScore 的结构参考，
不是可直接交易的蝶身。例如 Zero Gamma 为 7666 时，系统比较 7665、7670 等合法行权价的完整得分，
不会生成不存在的 7666 蝶式。

11:00–13:00 TRADE 只枚举已确认的第一中轴，按中轴质量盒子评 10/15/20/50 点蝶；质量已堆在 [K−W, K+W] 内（分数 ≥ 0.50）的梯子档才上架。宽度由统一 selection score 选择，不再强制最窄帐篷。人读蝶式被接受后，中轴、翼宽和 Call/Put 腿组合在现有 15 分钟 winner window 内整体锁定；当 STABLE_PIN 消失时，中性锁自动失效，不阻挡新的独立方向证据。

LOOK 或 TRADE 钉住时，RTH 方向价差（`ES_VOLUME_MOMENTUM` 以及遗留的失败突破 / 趋势回踩 / 突破接受）不得成为人读卡；PIN_MIGRATING 与 UNCERTAIN 不挡价差。夜盘两档都不评蝶。

硬条件（TRADE / PIN_STABLE）：

```text
ER30 < 0.25
abs(value_center_drift_30m) <= 2.5 SPX points
abs(value_center_drift_60m) <= 5.0 SPX points
最近60分钟至少2次 excursion-and-return
VIX 15m 不持续扩张
最近20分钟无持续新高/新低接受
Gamma center、Value Center、Q mode 三者 max-min <= 5 points
PIN 对齐用的 Q mode 是现价 ±30 的本地 5 点质量峰，不是全链密度 argmax。
上一拍峰值若仍在本地质量前两名、且距当前本地峰不超过 5 点，则保持上一拍。
ATM straddle 15m decay > 0
```

加分：Zero Gamma 在最近 5 点 strike 附近；大整数 strike / OI cluster 重合；Cross-section cancellation 高；RSP 与权重科技方向相反；上下突破都失败。

### 7.5 PIN_MIGRATING

任一硬触发：

```text
abs(value_center_drift_30m) > 5 points
或 abs(value_center_drift_60m) > 8 points
或 ER30 从 <0.25 升到 >0.40
或最近15分钟形成连续新高/新低
```

附加确认：breadth 开始同向；VIX 开始确认；Gamma/Q mode 同时迁移；ATM straddle 不再衰减或重新扩张。

**PIN_MIGRATING 禁止新 Butterfly。**

### 7.6 EVENT RISK

由现有 macro event clock 提供。

默认规则：

- 距离 FOMC、CPI、NFP 等事件 30 分钟以内：禁止普通 Pin/Trend 候选；
- 事件公布后 10 分钟内：POST_EVENT_DISCOVERY；
- 只有重新形成价格、宽度、IV 和 Value Center 后才恢复。

FOMC 14:00 ET 特殊处理：13:30 后禁止新 Butterfly；14:00–14:15 不生成普通候选；14:15 后重新计算所有中心和 surface，不继承 14:00 前 Pin。

---

## 8. Entry Quality 与 Anti-Chase

方向和入场必须分开。

### 8.1 通用 Entry Quality

计算：

```text
distance_to_vwap_atr
impulse_15m_atr
distance_to_trigger_atr
target_room_points
stop_distance_points
target_room_ratio = TargetDistance / StopDistance
debit_fraction_of_width = NetDebit / SpreadWidth
iv_jump_5m
minutes_to_close
```

### 8.2 LATE_CHASE 硬条件（v54 及以前）

TREND_PULLBACK / FAILED_BREAK（审计与 GTH 标签）任一满足：

```text
abs(distance_to_vwap_atr) > 1.0 且 abs(impulse_15m_atr) > 1.0
或 target_room_ratio < 1.5
或 vertical debit_fraction > 0.45
或 当前价已完成 trigger->target 路径的 60% 以上（失败突破 50%）
或 Call/Put 长腿 IV 在5分钟内上升 > 2 vol points
或 剩余持有时间不足以覆盖规则的最短观察窗口
```

`ES_VOLUME_MOMENTUM` 不用 VWAP 距离 + 15 分钟冲动判追价（那会把第一脚本身杀掉）。
`PREAVERAGE15_PULLBACK` 使用自己的局部尺度对称目标/失效位，不进入本节的 ATR、
VWAP、路径进度或 20 分钟旧管理门；仍受宏观、PIN、Schwab exact-BBO、单腿相对价差
不高于 5%、最大借记、人工确认和 15:45 ET 硬退出约束。
`WALL_BREAKOUT_HAZARD` 不用 HMM/旧方向作触发，但仍走通用 ATR 几何、目标空间、
最大借记和 exact-BBO 门；额外用 `p_break * target_terminal_value - net_debit > 0`
作为保守执行 EV 硬门。
`RTH_LEVEL_CONFIRMATION` 只接受正式 level state 的 `CONFIRMED + breakout` 价格路径，
不再复用确认前 hazard 概率。它关闭 VWAP/15 分钟 impulse 追价项，目标/止损空间比
最低 1.0、trigger→target 最大进度 80%，仅枚举固定 15 点 Debit Vertical，并保留通用借记、ATR 止损、PIN、宏观、
exact-BBO 与定义风险门。
短周期过晚：`abs(return_5m) / ATR5m > 1.5`，或 trigger→target 路程 ≥ 50%，
或借记/空间门与上表相同。

LATE_CHASE 输出：

```text
方向判断可保留
交易候选 = NO_TRADE
reason = direction_valid_but_entry_too_late
```

禁止通过“提高置信度”绕过 Late Chase。

v55 起，以上复合指标继续写入 `entry_quality` 供执行者判断价格是否昂贵、路径是否已走远，
但不再作为已授权 RTH Directional Debit Vertical 的 hard gate。独立的 ATR 止损距离、
目标/行权价路径、宏观事件、PIN、exact BBO 和 setup 专属证据门仍然有效；
GTH 与历史 replay 合同不因本条自动获得授权。

---

## 9. 策略一：ES Volume Momentum Vertical

v38 起重新授权 RTH 人读卡。`TREND_PULLBACK` 仍只出现在
`rth_setups` 审计里。GTH 仍可把夜盘证据标成该旧名，且 GTH 人读 Debit 不走本 setup。

### 9.1 交易假设

ES 短窗口放量，且 1 分钟与 5 分钟动量同向，这一脚会继续走到墙/目标，
现实延续概率高于当前 Vertical 借记隐含的概率。不要求日间 TREND，不等回踩。

### 9.2 Setup

```text
es_volume.label = elevated
es_volume.direction ∈ {up, down}
sign(return_1m) = sign(return_5m) = volume direction
abs(return_1m) >= 0.35
abs(return_5m) >= 1.0
abs(return_5m) / ATR5m <= 1.5
POST_EVENT_DISCOVERY 不挡量比卡；不改 post_event.entry_allowed
LOOK/TRADE pin 不挡
第一张卡：cash HMM BALANCED / TRANSITION / UNCERTAIN / 不可用 不挡
第一张卡：cash HMM 已是反向 TREND → 挡（es_volume_momentum_hmm_opposes）
同向加仓：必须 cash HMM 同向 TREND，且 |5m|/ATR5m >= 0.50
  （es_volume_momentum_add_needs_new_impulse）
翻向：本 session 已有反向 RTH 人读卡时，必须 cash HMM 同向 TREND
RTH winner stick = 900s；stick 内 rank/delivery 不得改方向
借记管理：50% premium stop + trail + 15:45 ET 硬退出；无 20 分钟 time stop
```

空头：volume down + 1m/5m 为负。不要求 `path_state = TREND`，也不等回踩。
清晰度看的是「这一脚是否同向、HMM 有没有反向 TREND」，不是突破回踩操作。

### 9.3 量价输入

复用已有 `es_volume_signal`（量比、放量/缩量、窗口方向）和 market-frame ES
`return_1m_points` / `return_5m_points`。缺量比或动量则失效关闭。

### 9.4 Trigger

触发位 = 本窗起点（spot − es_volume.price_delta）。
目标/失效仍走现有墙位几何。禁止只因 TREND 或 OR 假突破就发卡。

### 9.5 合约枚举

SPX 坐标中：

- Long strike：接近平值或轻度 ITM/OTM，目标 Delta 0.40–0.60；
- Width：5、10、15 点；
- Short strike：不超过目标位；
- 到期：默认同日 0DTE；
- 若剩余时间太短或价格结构要求跨收盘，不自动退回 1DTE，直接 NoTrade。

对每个候选计算 conservative net ask。

### 9.6 Hard Gates

```text
target_room_ratio >= 1.5
debit_fraction <= 0.45
exact quote ready
max risk within account policy
stop distance >= 0.25 ATR5m
stop distance <= 1.0 ATR5m
```

### 9.7 Invalidation

基于标的，而不是只看期权价格：

- 回踩位被两个 5m close 接受；
- VWAP/OR/Wall 失效；
- market breadth 反向；
- Value Center 快速反向迁移；
- VIX 行为与原趋势冲突。

### 9.8 Exit

默认：

- Target 到达：平仓；
- 失效：平仓；
- 20–30 分钟无 follow-through：time stop；
- 达到最大持有时钟：平仓；
- Vertical mark 达到最大价值 70%–80%：优先兑现。

不得把 GTH 10 分钟信号延长到 RTH，除非 RTH 重新授权。

---

## 10. 策略二：Failed Break / Reclaim Vertical

RTH 不再用本 setup 授权人读卡；`rth_setups` 仍计算供审计。GTH 证据仍可映射此名。

### 10.1 交易假设

市场为突破方向支付了波动和动量溢价，但价格、宽度和波动率没有确认，突破失败后回归的现实概率高于期权定价。

### 10.2 Setup

候选结构位：Put Wall、Flip Low / Flip High、Zero Gamma band、Call Wall、Opening Range、Prior Close、RTH VWAP。

### 10.3 下破失败 → Call

必须满足：

```text
penetration between 0.15 and 0.50 ATR5m
reclaim within 5–15 minutes
至少一个5m close收回结构位
下一根不再跌回结构位下方
VIX未持续扩张
至少一个关键宽度代理未确认下跌
```

推荐再等待一次 retest：收复 → 回踩 → 守住。

### 10.4 上破失败 → Put

完全对称。

LOOK 或 TRADE 钉住与失败突破互斥：中轴观察或稳定钉住时，失败突破价差不得成为人读卡。

### 10.5 Entry Quality

Failed Break 的优势是失效位置近。要求：

```text
target_room_ratio >= 1.8
debit_fraction <= 0.40
结构位到 stop 的距离 <= 0.5 ATR5m
```

### 10.6 禁止情形

- 突破后已经在外侧保持 15 分钟以上；
- breadth 与 VIX 已经确认；
- Value Center 已迁移到外侧；
- Q mode 与 Gamma center 同时迁移；
- 只是因为“跌多了”而猜反弹。

---

## 11. 策略三：Stable Pin Butterfly

### 11.1 交易假设

当前期权市场仍为较宽终值分布定价，但现实路径、成交中心和结构中心已经收敛到稳定中轴，长 Butterfly 的可执行借记低于该现实终值集中度的价值。

### 11.2 评估窗口

正常交易日：

```text
最早 11:00 ET
11:00–13:00：LOOK 观察今日中轴；TRADE（PIN_STABLE）要求同一中轴 3 个快照且持续 10 分钟
仅对已确认第一中轴按质量盒子评 10/15/20/50，过门宽度由统一评分决定
14:50–15:30：TRADE 尾盘窗（5 点 ≤70 分钟）
15:30 后仅接受高置信、低成本候选
```

FOMC 或重大 14:00 事件日使用事件规则，不套用普通窗口。

### 11.3 中轴候选

从当前 SPX 附近的 5 点执行价生成：`K ∈ [Spot−30, Spot+30]`，步长 5 点。

### 11.4 Center Score

对每个中轴 K：

```text
CenterScore(K) = 0.25·G(K) + 0.25·V(K) + 0.20·Q(K)
              + 0.15·R(K) + 0.10·T(K) + 0.05·C(K)
              − 0.25·M(K) − 0.20·D(K)
```

所有分项归一化到 0–1。

- **Gamma Alignment (G)**：取 Zero Gamma、Flip 中心和稳定 Wall 中距离最近者 `K_G`：`G(K) = exp(−|K−K_G|/5)`。若 GEX 质量不可用，则该项缺失，不用零代替；剩余权重重新归一化。
- **Value Center Alignment (V)**：`V(K) = 0.5·exp(−|K−VC_30|/5) + 0.5·exp(−|K−VC_60|/7.5)`。
- **Q Local Mass (Q)**：风险中性密度在 `[K−2.5, K+2.5]` 内的质量，相对其他候选归一化。
- **Reversion Evidence (R)**：过去 60 分钟中，价格离开 K 至少 5 点、随后返回 K±2.5 点，记为一次 excursion-return。`0次→0, 1次→0.4, 2次→0.7, >=3次→1.0`。
- **Theta/Straddle Decay (T)**：根据固定 ATM straddle 15/30 分钟衰减分位数归一化。
- **Cross-Section Cancellation (C)**：使用第 6.3 节定义。
- **Migration Penalty (M)**：由 15/30/60 分钟 Value Center 漂移和 Q mode 漂移组成。
- **De-pin Risk (D)**：见下一节。

### 11.5 De-pin Risk

```text
DePinRisk = 0.25·Drift + 0.20·ERAcceleration + 0.20·BreadthAlignment
          + 0.15·VIXResponse + 0.10·ExtremeRecency + 0.10·StraddleReExpansion
```

解释：

- Drift：中心迁移；
- ERAcceleration：路径从震荡变高效；
- BreadthAlignment：内部冲突消失，开始全面同向；
- VIXResponse：波动率开始确认跌势/涨势；
- ExtremeRecency：刚出现并接受新高/新低；
- StraddleReExpansion：衰减停止并重新变贵。

Bootstrap gate：

```text
DePinRisk >= 0.55  -> 禁止 Butterfly
0.35–0.55          -> 仅观察或大幅缩小风险
< 0.35             -> 允许枚举
```

### 11.6 Butterfly 枚举

对 Top 3 中轴枚举单侧翼宽 `W ∈ {5, 10, 15, 20}`。

Call Butterfly：

```text
+1 Call(K−W)
−2 Call(K)
+1 Call(K+W)
```

Put Butterfly 同到期收益相同；执行时选择合成盘口更优的一侧。

到期收益与净收益：

```text
Payoff(S_T) = max(W − |S_T − K|, 0)
PnL = 100·(Payoff − Debit) − Fees − Slippage
BE_low  = K − W + Debit
BE_high = K + W − Debit
```

### 11.7 Conservative Synthetic BBO

Call Butterfly：

```text
NetAsk = Ask_lower − 2·Bid_body + Ask_upper
NetBid = Bid_lower − 2·Ask_body + Bid_upper
```

所有三腿必须：同 provider、同 expiry、quote age 在阈值内、bid/ask 均存在、跨腿 source skew 在阈值内、body 数量明确为 2。

**不能使用 `lower_mid − 2·body_mid + upper_mid` 作为可执行价格。**

### 11.8 结构选择

选择不是“翼越宽越安全”。每个候选比较：

- Max loss / Max profit；
- 盈利区；
- P(PnL > 0)；
- Expected PnL；
- P10/P50/P90；
- Expected Shortfall；
- 中轴偏移 2.5/5/10 点后的 PnL；
- Liquidity；
- De-pin loss。

30 点宽蝶如果比 10 点宽蝶多支付大量权利金，却只扩展少量 breakeven，必须淘汰。

### 11.9 Invalidation / Exit

新 Butterfly 建仓后，任一触发退出：

- Top center 改变 >= 5 点；
- PIN_STABLE → PIN_MIGRATING；
- DePinRisk >= 0.60；
- 价格在一侧翼外获得两个 5m close 接受；
- VIX 和 breadth 同向确认扩张；
- Q mode 与 Value Center 同时迁移。

利润管理：15:30 前达到最大价值的 60% 以上可兑现；中轴稳定且风险可控可持有至接近结算；不使用无限宽松的期权价格 trailing stop；主要按 SPX 中轴和 De-pin 条件管理。

### 11.10 v44：60 分钟收盘收敛蝶

`CLOSE_CONVERGENCE_60M` 是用户明确授权、但仍标记
`forward_unvalidated_user_override` 的独立 RTH 候选。它只在 15:00:00–15:02:59 ET
运行，并使用当日 15:00 前的原始 Schwab SPX/ES 路径及严格早于当日的完整 session。
三个前向路径专家（季节 Student-t、整段 analog、functional ridge）按先前 session 的
CRPS 在线加权；生产只携带 51 个收盘分位数，不携带完整 Monte Carlo 矩阵。

- 中轴为 online-pool 收盘分布的 5 点 modal bucket，不是现价取整，也不是 dealer/GEX/OI/wall 推断；
- 仅枚举 `W ∈ {10, 15, 20}` 的 Call/Put Butterfly；
- 选择分数是相同 51 个终值路径上的冻结 `risk_adjusted_cvar.v1` 目标；负目标不单独否决，记录为 advisory；
- 只接受 Schwab exact 三腿 BBO，age ≤ 15 秒、source skew ≤ 2 秒、借记/翼宽 ≤ 0.45、最大风险 ≤ $1,000；
- 不继承 `STABLE_PIN`、Q mode、Value Center、GEX、墙位、HMM 或方向门；shock 仍 fail closed；
- 入场保持人工净借记限价，`automatic_ordering=false`；固定以 15:55 ET 新鲜 conservative combo bid 管理退出，不使用盘中 premium stop/trail；
- 生产依据为 14 个 session 的 60 分钟冻结 OOS：13 笔 exact entry/exit BBO，11 胜，总计 +$1,412.72，每个 decision +$100.91，session bootstrap 95% 下界 +$10.54。样本仍小，必须继续前向结算。

### 11.11 v45：0DTE 曲面载荷参与结构排序

v45 删除旧的全局 D3/D4 方向加分，改为对每个已通过 hard gate 的具体
Vertical/Butterfly 以及 Iron Condor Map 变体做 entry-frozen bump-and-revalue：

- 保存当时腿的 strike/log-moneyness，计算组合 Delta、Gamma、Vega、Vanna 和有限差分 Volga；
- 分别把 ATM、put skew、call skew、put curvature、call curvature 抬高 1 vol point 并重估组合；
- 取五个载荷的最大绝对值除以结构最大亏损，形成 `surface_decision_modifier`；
- modifier 只允许在 `[-0.05, 0]`，因此只能降低曲面冲击更敏感的结构排名，不能增加方向置信度；
- 同方向候选可因此更换 strike/width/right；Iron Condor Map 可因此更换 5–20Δ 变体，但 `iron_condor_not_human_authorized` 不变；
- IV 缺失时明确输出 `surface_leg_iv_unavailable` 且 modifier 为 0，不用不完整曲面阻断原有候选；
- `automatic_ordering=false`、所有既有 hard gate、方向 owner 与 manual authority 均不变。

这是当前入场曲面的结构风险决策层，不是 Ravagli premium 已验证为 alpha 的声明。
`joint_spot_surface_management_policy.v1` 另把已完成的同 session-clock 历史路径按
5 分钟重放到冻结入场腿：SPX 路径与 ATM、put/call 25Δ skew、put/call residual
curvature 同步变化，并附带同一 SPX 路径的 sticky-IV 基线。GTH 只使用先前 GTH
同钟点路径，RTH 只使用先前 RTH 同钟点路径；历史点同时要求 `as_of` 和
`created_at` 早于当前决策时刻，路径内部只允许最多 30 分钟的因果向前保持，不做
未来插值。联合历史不足时回退 `physical_path_management_policy.v3`，不得补造曲面。

该联合回放只影响候选/铁鹰 Map 的解释性 PnL、CVaR 和固定通知图。它的
`research_unvalidated`、独立 session 数、曲面降级占比和 sticky-IV 对照必须同时展示；
在独立 walk-forward 通过前仍不得提供新的授权、正向加分、方向或解除 IC gate。

### 11.12 v46：RTH 固定 10 点铁鹰人工候选

v46 根据用户明确授权，从原 `iron_condor_map` 中开放一条独立的
RTH 人工候选，不是由 v45 曲面层解锁：

- 只在 10:00–11:30 ET，每个 RTH session 最多推送一张；
- 固定卖 20Δ 左右短腿，两侧保护腿均为 10 点；
- 四腿必须来自同一 provider，入场 BBO age ≤ 15 秒、source skew ≤ 2 秒；
- 现价必须在两条短腿之间，保守贷记/翼宽为 15%–55%，单组合定义风险不超过 $1,000；
- 用四腿保守回购负债管理：回购价 ≤ 0.5C 止盈，回购价 ≥ 3C 止损
  （相对入场贷记净亏 200%），否则 15:45 ET 平仓；
- `automatic_ordering=false`，只会生成人工净贷记限价卡；GTH 仍为 Desk Map。

生产证据是 2026-08-05–21 的固定合同工程回放。与上述产品合同最接近的
「当日第一个合格 20Δ 候选」在 10 个 session 中为 10 笔、9 胜、平均
+$85.44/笔；开发段 7 笔平均 +$78.01，后续 3 笔平均 +$102.77。样本很小、
一分钟路径可能漏过盘中止损，且不是成交概率证据，因此保持
`forward_unvalidated_user_override` 并继续前向结算。

### 11.13 v47：RTH 铁鹰最低贷记提高至 20%

v47 保持 v46 的 20Δ 短腿、10 点翼、10:00–11:30 ET 每日首张、
0.5C 止盈、3C 止损与 15:45 ET 硬退出，只把保守贷记/翼宽下界从
15% 提高至 20%，上界仍为 55%。自动下单继续关闭，GTH 继续 map-only。

冻结依据为 2026-08-03–21 的 12 个后续可观察 RTH 机会：20% 门留下
9 笔，8 胜，成本和 0.10 点退出滑点后合计 +$664.96、平均 +$73.88/笔；
按全部机会（不做记零）平均 +$55.41，session bootstrap 95% 区间
[+$18.88, +$85.74]。样本仍小，因此继续标记
`forward_unvalidated_user_override`，不声明长期 alpha。

### 11.14 v49：RTH 铁鹰 25% 贷记门与入场曲面风险门

v49 保持固定 20Δ、10 点翼、10:00–11:30 ET 每日最多一张、0.5C
止盈、3C 止损、15:45 ET 硬退出和人工-only，只调整 RTH 人工候选：

- 保守贷记/翼宽范围提高至 25%–55%；
- 入场 ATM IV 必须不高于 0.2374713681；
- `0.5 × (put_skew_25d_0dte + call_skew_25d_0dte)` 必须不高于
  0.0313827831；
- ATM IV 或两侧 25Δ skew 任一缺失时失效关闭；GTH 仍为 map-only。

曲面判定只在当天首个满足 25%–55% 贷记、风险和几何门的候选上执行并冻结：
若通过，当天保留人工候选资格；若拒绝或数据缺失，该 RTH 整日不再开铁鹰。
不允许等待曲面回落后再次放行。该替代语义的分钟回放为 20 笔已解析交易合计
-$371.20，因此没有进入生产。

阈值只用 2026-07-07–31 的分钟候选冻结，8 月没有重拟合。全窗口22个
25%候选机会中，该门挡掉9次，12笔已解析交易全部止盈，1笔路径未解析；
已解析总PnL +$1,348.28，未解析按费用后最大亏损计入后为 +$607.72，
即 +$27.62/机会，bootstrap 95%区间 [-$58.92, +$81.64]。8月门禁只删掉
盈利交易，没有独立证明IV过滤增量，因此继续标记
`forward_unvalidated_user_override`，不声明长期alpha。

### 11.15 v50：RTH 铁鹰逐边 Schwab Delta 与 11:00 截止

v50 不采用 09:45 提前入场。34 个 RTH 的分钟因果回放中，09:45 起扫在
保留 v49 曲面门后为 13 笔已解析交易、平均 +$63.29，低于 10:00 起扫的
12 笔、平均 +$112.36；无曲面门时 09:45 起扫平均为 -$15.56。因此当前合同：

- 只在 10:00–11:00 ET 产生新人工候选；
- Put 与 Call 分别使用最新 Schwab Greeks，选择绝对 Delta 不超过且最接近
  20Δ 的短腿，不使用组合净 Delta 反推行权价；
- Greeks age 与 exact BBO age 均不得超过 15 秒；
- 当天首个满足贷记、风险、几何与曲面门的行权价集合锁定，后续 Delta 迁移
  不得替换交易卡；
- 卡片展示两侧实际 Delta、距 SPX 点数；GTH、管理合同和人工-only 边界不变。

将 11:00–11:30 删除后，历史集合减少一笔 11:30 的止盈交易：11 笔已解析
交易平均 +$112.62，另有一笔路径未解析。样本仍小且历史 lake 没有独立的
`greeks_observed_at` 列；运行时有独立时间戳时严格检查，否则只可使用报价行
时间作为 Greeks 时间，因此继续标记 `forward_unvalidated_user_override`。

### 11.16 v51：RTH 环境只负责过滤与结构选择

v51 把事件、波动率、市场广度和跨资产确认压成一个
`rth_environment`，但明确不给它方向权限：

- `EVENT_RISK`：既有宏观日历在发布前继续禁止普通入场；显式
  `EVENT_SETTLEMENT_THRESHOLD` 仍走自己的事件合同；
- `RISK_EXPANSION`：VIX1D 15 分钟涨幅 ≥2%、ATM IV 5/15 分钟上升
  ≥1/1.5 vol point、ATM 跨式 15 分钟重新扩张 ≥2% 或负 Gamma 中至少
  两项成立；一项成立时还必须有 TREND、单边 breadth 或跨资产压力确认。
  它只允许已有价格触发的 RTH Debit 继续过门，不能决定 Call/Put；
- `VOL_CONTRACTION_BALANCE`：VIX1D、ATM IV 与跨式四项中至少三项确认
  收缩，breadth 位于 35%–65%，同时路径为 BALANCED 或已形成
  PIN_STABLE，且不得处于负 Gamma 加速。它允许评估 RTH 铁鹰和
  STABLE_PIN 蝶式；
- 核心 VIX1D/ATM/跨式/breadth 任一缺失时为 `INSUFFICIENT_DATA`，新结构
  失效关闭；`MIXED_UNCONFIRMED` 也不授权新结构；
- HYG−LQD、SHY/IEF/TLT、UUP、USO 只作 15 分钟压力确认。它们是流动 ETF
  价格代理，不得表述为真实 credit spread、2Y/10Y 基点、DXY 或 WTI；
- `CLOSE_CONVERGENCE_60M` 保留独立的物理收盘分布合同，不继承这个环境门。

全部阈值是 `strategy_policy.bootstrap.v51` 的冻结代码常量。该层不增加服务、
存储、通知通道或自动下单能力；`automatic_ordering=false` 不变。

### 11.17 RTH 铁鹰 Gamma 风险解释层

RTH 铁鹰候选附加 `iron_condor_gamma_risk.v1`，直接使用同一张 Schwab 四腿
快照中的逐腿 Gamma 与逐腿 short Delta，展示组合净 Gamma、10 点 SPX 冲击
对应的 Delta 变化、`GCR10 = 0.5 × |Gamma| × 10² / 入场贷记`，以及两条
short leg 中较大的绝对 Delta。`GCR10` 的 LOW/NORMAL/HOT/HIGH 分界为
10%/20%/30%，只作解释，不改变入场、排序、TP50、3C 或 15:45 ET 退出。

2026-07-07 至 2026-08-24 的 35 个 RTH 日分钟因果回放，严格使用
10:00–11:00 ET 首个 25%–55% 贷记候选并冻结当天首个曲面判定，得到 13 个
曲面通过候选。其入场 `GCR10` 为 4.3%–13.0%，所以 20% 门没有过滤任何交易；
不能据此声明增量 edge。30Δ/35Δ 强平明显过早；40Δ 强平降低单次观察亏损，
但把多笔后来达到 TP50 的路径提前以亏损退出，因此不替代现有 3C。入场冻结
sticky-IV 的 ±10 点四腿完整重估门存在小样本筛选偏差，也不进入授权。
回放仍受一分钟采样、两个 exact-leg 退出路径缺口和缺少独立历史
`greeks_observed_at` 的限制；`automatic_ordering=false` 不变。

### 11.18 v52：RTH 铁鹰曲面降为解释层

历史分钟回放没有证明 v49 的 ATM IV／smile 硬门有增量价值。按当前
10:00–11:00 ET 截止重算，同一 25% 贷记基线中，无曲面门为 20 笔已解析
交易平均 +$35.44，另有 1 笔退出路径未解析；启用曲面门为 19 笔已解析
交易平均 -$25.30，另有 1 笔未解析。用户据此明确取消曲面的拒绝权限。

v52 保留 `human_surface_gate`、entry-frozen 曲面归因及非正向排序降权用于解释，
但 ATM IV、smile 高值或缺失均不得形成 hard gate，也不得把当日 session 标成
surface blocked。当天首个满足 20Δ、10 点翼、25%–55% 贷记、风险和几何门的
候选仍锁定行权价；v51 `rth_environment`、报价新鲜度、单日一张、人工-only、
TP50、3C 与 15:45 ET 退出保持不变。策略版本为
`strategy_policy.bootstrap.v52`，证据仍是用户覆盖且前向未验证，不声明长期 alpha。

### 11.19 v53：RTH 波动扩张失败后的铁鹰转折通道

v53 不在 `RISK_EXPANSION` 中提前卖铁鹰，也不把所有铁鹰贷记门统一下调。
`rth_environment` 因果携带最近一次扩张时间；若其后 20 分钟内 VIX1D、ATM IV、
跨式四项中至少三项转为收缩，breadth 回到 35%–65%，路径为 `BALANCED` 或
`PIN_STABLE`，且 Gamma 不处于负向加速，则状态为
`EXPANSION_TO_CONTRACTION`。

该状态允许一个独立的 RTH 人工铁鹰合同：

- Put/Call 仍分别选择不超过且最接近 20Δ 的短腿，固定 10 点翼；
- 保守贷记占翼宽底线从普通平衡日的 25% 降为 23%，上限仍为 55%；
- Put spread 与 Call spread 较小一侧必须贡献总贷记至少 25%；
- 只在 10:00–11:00 ET、exact BBO/Greeks 新鲜、定义风险不超过 $1,000 时生效；
- 当日首个合格候选继续锁定，TP50、3C、15:45 ET、人工-only 与
  `automatic_ordering=false` 不变；
- 普通 `VOL_CONTRACTION_BALANCE` 仍使用 25% 贷记底线，GTH 不变。

2026-07-07 至 2026-08-25 的 36 个 RTH 日分钟因果工程回放中，取消曲面拒绝后，
“早盘波动释放、15 分钟振幅/效率收缩、ATM IV 5 分钟转负、两侧贷记平衡”的
23% 通道得到 18 笔已解析交易，16 笔盈利，平均约 +$60.00/张；最差一笔
-$605.56。20% 全样本均值为负，22.5% 验证段被一笔止损拖为负，25% 则漏掉
2026-08-25 的 2.30 贷记止盈机会。该窄阈值仍有同样本选择和一分钟止损采样
限制，因此标记 `forward_unvalidated_user_override`，不声明长期 alpha。
策略版本为 `strategy_policy.bootstrap.v53`。

同一版本在现有 `market_feature_state` 内按交易日/到期日持续记录 ready 期权帧的
GTH/RTH ATM 跨式中价与 ATM IV 高低点。Desk Map 固定展示当前跨式、GTH 高低、
当前相对 GTH 高点的收缩比例以及已有的 RTH 高低。该观测只用于判断“GTH 风险预算
是否已释放并开始收敛”的人工上下文，不增加正向分数、不绕过贷记/Delta/BBO/环境门，
也不让 GTH 铁鹰获得人工交易授权；数据陈旧或期权帧降级时停止更新高低点。

### 11.20 v54：墙位确认只保留统一两腿决策

确认前 `approaching`、`break_pending`、`reject_pending` 与确认后的非交易
level-transition 卡不再进入 Bark/飞书；它们继续保留在 projection 与审计中。
人类通道只接受 `build_strategy_decision` 的最终结果。

RTH level state 只有同时满足 `formal_signal=true`、`phase=confirmed`、`thesis=breakout`、
官方质量可用、方向完整且原始 5 分钟有效期未过，才形成
`RTH_LEVEL_CONFIRMATION`。该 setup：

- 方向来自已确认的价格接受/拒绝，不由 Gamma/OI 或环境层决定；
- `rth_environment` 缺失或不在 `RISK_EXPANSION` 只作说明，不再否决该路径；
- 确认前 wall-hazard EV 不跨阶段复用；确认后重新使用当前目标、失效位和 exact 两腿报价；
- 目标/止损空间比最低 1.0，路径进度必须小于 80%，借记不超过翼宽 45%；
- 只允许固定 15 点宽的 Debit Vertical，不从 5/10/15/20 点中择优；
- Schwab exact BBO age ≤15 秒、source skew ≤2 秒，定义风险 ≤$1,000；
- 宏观事件、PIN 冲突、报价、几何与 ATR 失效门保持 fail-closed；
- 一旦统一候选生成，给人工完整 5 分钟限价窗口，`automatic_ordering=false`。

`fade` 确认继续进入 level audit/outcome 研究，但不得生成方向价差候选，也不得获得
人工交易权限。

该路径为用户明确授权的 `forward_unvalidated_user_override`，不是已验证 alpha。
策略版本为 `strategy_policy.bootstrap.v54`。不新增服务、定时器、数据库或通知 owner。

### 11.21 v55：RTH 方向确认不再二次等待波动扩张

价格证据已经形成且属于已授权 RTH Directional Debit Vertical 时：

- `rth_environment` 保留结构选择和风险说明职责，但 `MIXED_UNCONFIRMED`、
  `VOL_CONTRACTION_BALANCE` 或 `EXPANSION_TO_CONTRACTION` 不再以
  `rth_directional_environment_not_expanding` 否决方向价差；
- `direction_valid_but_entry_too_late` 从 hard gate 降为 `entry_quality` 数值说明；
- 核心环境输入缺失仍以 `rth_environment_inputs_unavailable` 失效关闭；
- 独立 ATR 止损门和每组最大定义风险 `$1,000` 保留；宏观事件、PIN、目标/行权价路径、
  exact BBO、报价新鲜度、`WALL_BREAKOUT_HAZARD` 的正执行 EV 等门保持不变；
- Iron Condor 与 Butterfly 的环境合同不变，自动下单仍关闭。

该放宽是用户明确授权的 `forward_unvalidated_user_override`，不等同于已验证 alpha。
策略版本为 `strategy_policy.bootstrap.v55`；不新增服务、定时器、数据库或通知 owner。

### 11.22 v56：RTH 方向价差不设绝对美元风险 hard gate

已授权 RTH Directional Debit Vertical 继续要求合法的定义风险结构，并在候选卡显示
`max_loss_points` 与美元最大亏损；但不再以 `$1,000` 或其他绝对美元金额拒绝候选。
张数和账户风险预算由人工执行者决定，`automatic_ordering=false` 不变。

本条不改变 Iron Condor、Butterfly 和 GTH 人工候选各自已有的风险合同，也不绕过
宏观、PIN、ATR、目标/行权价路径、exact BBO、报价新鲜度或 setup 专属证据门。
策略版本为 `strategy_policy.bootstrap.v56`。

### 11.23 v57：动量缺失模型回退与持续突破确认

`ES_VOLUME_MOMENTUM` 已通过因果 ES 放量、1m/5m 同向、PIN、ATR、目标空间、
行权价路径及 exact BBO hard gate 后，若生产目录仅缺少可选的
`strategy_edge_model.v1.json`，不得再以该文件缺失作为唯一否决原因；候选进入
5 分钟人工窗口并明确标记 `forward_unvalidated_user_override`。若模型文件存在，
其 promoted、domain 与正 edge 结果仍拥有模型门禁；无效、陈旧或未推广模型不走回退。

正式 RTH breakout 仍必须先证明 inside-to-outside crossing、至少 20 秒方向接受和
ES 同向。接受后允许两条因果确认路径：

- 回踩冻结水平并再次同向离开，持续至少 10 秒；
- 不回踩但突破扩展持续至少 10 秒。

两者均进入原有 `RTH_LEVEL_CONFIRMATION`，固定 15 点 Debit Vertical、5 分钟人工
窗口，且继续要求宏观、PIN、ATR、目标空间、借记、exact BBO 与报价新鲜度门。
确认到期且价格完整离开原水平后，现有 rearm generation 允许下一次完整回踩、
重新穿越和确认形成新的机会；不得从单个陈旧事件重复发卡。`fade` 仍只作研究，
`automatic_ordering=false` 不变。

策略版本为 `strategy_policy.bootstrap.v57`；不新增服务、定时器、数据库、队列或通知 owner。

### 11.24 v58：ICT 流动性事件仅作 RTH 方向价差过滤

RTH 复用现有 ES 采样，在完整一分钟结束后因果识别 ONH/ONL 与 OR15H/OR15L 的
Sweep/Reclaim、五根 K 结构突破代理（MSS）及同向 Displacement。阈值冻结为：最小
穿越 `max(0.5pt, 0.1×ATR1m)`、最大延伸 `1.0×ATR1m`、三分钟内收回、五分钟内 MSS、
MSS buffer `0.25pt`、位移实体 `≥0.8×ATR1m`；15:00 ET 后不再形成新事件。

该事件不新增 setup、不产生或翻转方向、不绕过 hard gate，也不单独创建 Trade Ready。
只有 `MSS_DISPLACEMENT_CONFIRMED` 才参与现有 RTH Debit Vertical 排序：同向修正为零，
反向最多降权 `0.05`；缺失、过期或只有 Sweep/MSS 时对旧决策零影响。FVG 不进入生产
门禁。候选卡只增加一行简短 ICT 过滤说明，全部 exact BBO、宏观、PIN、ATR、几何、
人工窗口与 `automatic_ordering=false` 合同保持不变。

本路径来自 2026-08-31 的 32-session 因果研究，仍标记为过滤用途，不声明独立 alpha。
策略版本为 `strategy_policy.bootstrap.v58`；不新增服务、定时器、数据库、队列或通知 owner。

### 11.25 v59：市场广度缺失降级为方向候选提示

RTH 已授权 Directional Debit Vertical 在 VIX1D、ATM IV 与 ATM 跨式输入均可用、仅
`breadth_above_vwap` 缺失时，不再由 `rth_environment_inputs_unavailable` 拒绝。环境状态
保留为 degraded，并在 Desk Map 与人工卡明确提示“市场广度缺失”；该提示没有方向权限，
也不会提高候选分数。

此例外只适用于已有独立因果触发的 RTH 方向 Debit Vertical。铁鹰、STABLE_PIN 蝶式以及
任何其他核心环境输入缺失继续失效关闭；宏观、PIN、ATR、目标空间、setup 证据、exact BBO
和人工-only 合同不变。策略版本为 `strategy_policy.bootstrap.v59`。

### 11.26 v60：短周期动量冲突、路径尾部与 Pin 通知收敛

`ES_VOLUME_MOMENTUM` 继续要求 ES 放量及 1m/5m 同向，但不再把陈旧破位上下文当成
当前方向确认。若本周期刚发生 `break_reclaim`，该脚动量只保留观察；若当前方向与
15m 冲动相反且反向幅度达到 `0.5×ATR5m`，必须存在 15 分钟内、同方向的新破位才能
进入人工候选。90 分钟 break watch 仍可服务结构观察，但不再提供交易授权。

赢家附加历史路径后，仅对 `ES_VOLUME_MOMENTUM` 增加一个窄的 fail-closed 尾部门：
至少 20 个独立 session，risk objective 选择 `NO_TRADE`、亏损概率不低于 75%，且
P90 净 PnL 仍不高于零时，拒绝人工方向卡。其他 setup 和样本不足/不可用的路径结果
不受此门影响。

Pin Desk View 仍可展示形成中的中心，但主动 `pin_stable_watch` 通知只允许
`PIN_STABLE + center_confirmation_ready`，即同一中轴至少 3 个决策快照并持续 10 分钟。
人工交易卡末尾固定展示 ICT 流动性参考；ICT 继续只有非正向过滤权，不改变授权。
策略版本为 `strategy_policy.bootstrap.v61`；不新增服务、定时器、数据库、队列或通知 owner。

---

## 12. 正式 NoTrade

NoTrade 必须是候选集合中的真实策略：`Score_NoTrade = 0`。

以下均应输出 NoTrade：

- 未获授权的 GTH/研究路径出现 Late Chase；
- Regime 为 Transition；
- Event Risk；
- Exact quotes 不完整；
- Model uncertainty 太大；
- Butterfly center 分歧过大；
- Pin 正在迁移；
- Vertical 目标空间不足；
- 借记已经过贵；
- P 与 Q 差异不足覆盖成本；
- 当前候选最大风险超过账户上限。

NoTrade 输出必须说明：

```text
最接近的候选是什么
为什么没有通过
什么新事实会重新授权
```

---

## 13. P 与 Q 的第一版实现

### 13.1 Q：市场隐含分布

第一版：复用当前 RN density；新增 mode、local mass 和 candidate payoff integration；保存 clipping 和 strike coverage；density 质量差时 fail closed。

推荐第三方能力：numpy（数组）、scipy.interpolate（平滑曲线）、scipy.optimize（简单约束拟合）；后续如确需严格 bid-ask convex fit，再引入 cvxpy。

**禁止第一版直接手写大型 surface optimizer。**

### 13.2 P：现实条件分布

现有样本约一个月，不适合立刻训练复杂深度模型。第一版采用：**分层经验分布 + 相似历史场景 + 向 Q 收缩。**

场景时间桶：

```text
09:45–11:00
11:00–12:30
12:30–14:00
14:00–15:00
15:00–15:45
```

条件维度：path state、direction、ER bucket、breadth bucket、VIX response、event state、distance to VWAP/level、Pin center drift、time-to-close。

相似度：使用标准化特征距离，推荐 scikit-learn 的 `StandardScaler` + `NearestNeighbors`。不要手写 KD-tree。

收缩：设历史相似场景分布为 `P_emp`，风险中性分布为 `Q`：

```text
w = n_eff / (n_eff + k)
P_v1 = w·P_emp + (1−w)·Q
k = 20（初始）
```

含义：相似历史少时，避免小样本过度自信；数据增加后，现实分布逐渐获得更高权重。

每个决策必须输出：

```text
n_raw
n_effective
shrinkage_weight
historical_sessions
```

### 13.3 Vertical 的 P

Vertical 关心路径，不只是收盘。

历史标签：

```text
target_first
stop_first
neither
time_exit
```

对每个历史相似场景使用实际后续 SPX 路径。

经济结果优先使用历史当时的真实双腿报价；缺失时：可保留方向 outcome；不伪造净 PnL；该样本不能进入经济评分。

Live 候选的场景重定价：复用现有 Black-Scholes projection；对两腿同时重定价；使用历史条件 IV shift；保存 model uncertainty；**不使用单腿 Delta/Gamma Taylor 直接代表 Vertical**。

### 13.4 Butterfly 的 P

Butterfly 持有到结算时，PnL 只需要终值 SPX 和入场借记，因此最容易可靠计算。

历史相似场景直接使用正式结算值：

```text
PnL_i = 100·(max(W − |S_{T,i} − K|, 0) − Debit) − Costs
```

这应成为第一版最可信的经济模型。

---

## 14. Candidate Utility

先应用 Hard Gates，再计算 Utility。归一化到最大亏损：

```text
Utility_j = EV_j/MaxLoss_j − 0.75·ES10_j/MaxLoss_j
          − 0.25·LiquidityPenalty_j − 0.25·ModelUncertainty_j
          − 0.50·MigrationRisk_j − 0.50·LateChasePenalty_j
```

其中 ES10 使用正数形式的左尾损失；Liquidity、Uncertainty、Migration、LateChase 均在 0–1。

候选授权：

```text
Utility > 0
且 conservative lower bound > 0
且所有 hard gate 通过
```

若多个候选通过：先按 Utility；再按较小 MaxLoss；再按较低 execution friction。最终只输出一个 Primary Candidate；最多输出一个 Alternative Observation，**不得同时发多个绿色交易卡**。

---

## 15. LLM Idea / Critic 的正确职责

LLM 不负责：计算价格、计算中轴、计算概率、计算 payoff、选择执行价、覆盖 hard gate、直接创建 Trade Ready。

LLM 接收确定性的 MarketFactPack，输出结构化竞争假设：

```json
{
  "hypotheses": [
    {
      "kind": "stable_pin",
      "thesis": "权重科技与普通股票卖压相互抵消，终值中心可能稳定在7710附近",
      "why_now": [
        "zero_gamma与value_center接近",
        "两次下破失败",
        "VIX未扩张"
      ],
      "contradictions": [
        "等权宽度继续恶化"
      ],
      "falsifiers": [
        "value_center下移超过5点",
        "VIX持续转涨"
      ],
      "eligible_expressions": [
        "butterfly",
        "no_trade"
      ]
    }
  ]
}
```

系统随后验证：每一条 supporting fact 是否真的存在；falsifier 是否可计算；expression 是否属于允许集合。

LLM 不可用时，确定性假设生成器仍能工作。

LLM 只在以下时点调用：Regime 冲突；Pin/De-pin 接近阈值；两种策略 Utility 接近；固定报告时点；重大事件后重新定价。

**禁止每 5 秒调用 LLM。**

---

## 16. Execution Instrument Router

Alpha 和执行载体分开。

### 16.1 Canonical Strategy

统一输出 SPX 坐标，例如 `SPXW 7700/7710/7720 Butterfly`。

### 16.2 执行选择

GTH：`XSP or SPX`；SPY 期权不作为 GTH 执行。

RTH：比较 SPX、XSP、SPY 映射，但必须保持同一经济观点。

选择依据：

```text
max risk
combo spread
quote depth
mapping error
settlement/assignment preference
```

账户风险较小时优先 XSP；SPY 只在 RTH 且映射和流动性明显更优时使用。

---

## 17. 人类输出卡

```text
SPX STRATEGY DECISION · MANUAL CANDIDATE

Desk View
状态      BALANCED + PIN_STABLE
方向      无单边方向
核心判断  终值中心稳定在7710；下破失败，宽度弱但VIX未扩张
反证      RSP仍弱；尾盘若中心下移则De-pin

Why Now
Zero Gamma      7709.2
ES Value Center 7708.4
Q Mode          7707.0
Center Drift    -1.3 pts / 30m
De-pin Risk     0.24
Straddle Decay  14% / 30m

Execution
结构      SPXW 7700/7710/7720 Call Butterfly
自然组合  1.30 / 1.65
人工限价  <= 1.45
替代载体  XSP 770/771/772
有效期    120 秒，提交前重报
禁止市价

Economics
最大亏损  $145
最大收益  $855
盈亏平衡  7701.45 / 7718.55
P盈利     42%（P-bootstrap）
Q盈利质量 36%
P10/P50/P90 -145 / +190 / +790
Utility   +0.18
样本      16 sessions, n_eff=9.4, shrinkage=0.32

Invalidation
中心迁移 >= 5点
De-pin Risk >= 0.60
SPX在7700下方获得两个5m收盘接受
VIX与breadth共同确认下行

Data Quality
三腿报价新鲜，同源，最大age 1.2s，跨腿skew 0.8s
automatic_ordering=false
```

NoTrade 卡也必须使用同样清晰度。

---

## 18. 现有代码的具体迁移方案

### 18.1 新增文件上限

第一阶段最多新增以下 5 个生产文件：

```text
src/spx_spark/application/order_map/strategy_facts.py
src/spx_spark/application/order_map/strategy_regime.py
src/spx_spark/application/order_map/strategy_select.py
src/spx_spark/analytics/options/strategy_payoff.py
src/spx_spark/data_platform/research/strategy_decision_replay.py
```

不新增新的顶层 Python package。

### 18.2 文件职责

**strategy_facts.py**：只负责从现有 payload 和 LatestState 组成 MarketFactPack。必须复用 market_state_5m、options_map、RN density、wall/flip、ES bars、cross-index/breadth、macro event、quote quality。**禁止再次计算一套平行 VWAP/Wall/Gamma。**

**strategy_regime.py**：纯函数——path state、Pin state、event state、entry state、competing hypotheses 的确定性版本。无 I/O，无通知，无 broker import。

**strategy_payoff.py**：纯数学——Vertical payoff、Butterfly payoff、breakeven、conservative synthetic BBO、scenario PnL、quantiles、Expected Shortfall。推荐使用 NumPy；不手写 DataFrame 框架。

**strategy_select.py**：唯一策略 selector——枚举允许候选、调用 quote evaluator、调用 P/Q evaluator、hard gates、utility、输出 StrategyDecision。

**strategy_decision_replay.py**：从现有 Parquet/DuckDB/SQLite lineage 重建 fact pack、candidate、conservative execution、outcome、legacy comparison。**禁止新建第二个历史存储系统。**

### 18.3 修改现有文件

**application/order_map/service.py**：在所有事实附加完成后：

```python
payload["strategy_decision"] = build_strategy_decision(...)
```

通知和 desk projection 只读这个结果。

**application/order_map/candidates.py**：保留为 legacy candidate reference 和 level repricing helper，去掉其人类最终授权。

**application/market_features/gth_manual_candidate.py**：GTH 绿色候选改为调用统一 selector。原 GTH signal 仍可作为 evidence，不再自己决定 spread。

**application/order_map/convexity_idea_radar.py**：保留 fact summary、LLM hypothesis prompt、contradictions；删除或停止使用固定三 lane 的最终排名、13:00 以后完全关闭所有策略思想、将 Call/Put 作为唯一表达。

**application/market_features/market_state_5m.py**：继续输出 D/Q/V 和原始分项。不要在这里直接启用 LOW_VOL_PIN Trade Ready；Pin 由统一引擎计算。

**analytics/options/density.py**：新增 mode/local mass/payoff mass，不改变原 percentile API 的兼容读取。

### 18.4 旧 owner 删除规则

新 strategy_decision 在生产报告中稳定工作后，同一个迁移 PR 或下一紧邻 PR 必须删除/降级：

- fixed Call/Put/Vol opportunity board 的交易优先级；
- GTH direct spread green-card authority；
- legacy candidate 直接进入人类 Trade Ready 的路径；
- 重复的策略选择字段。

**禁止永久双写两套“最终候选”。**

---

## 19. 现有一个月数据的立即回放

### 19.1 决策时点

每个交易日固定重建：

```text
10:00  11:00  12:30  13:30  14:30  15:15 ET
```

并额外加入：wall/flip first touch、reclaim confirmation、breakout confirmation、Pin center material change、De-pin trigger。

### 19.2 因果约束

每个输入必须满足 `available_at <= decision_at`。

禁止：

- 使用盘后 OI 反向填入盘中；
- 使用最终收盘判断当时 Pin；
- 用未来 quote 修复当时缺腿；
- 用当前算法派生标签冒充原始事实。

### 19.3 对照组

每个时点同时计算：

```text
Legacy candidate
New Vertical candidate
New Butterfly candidate
NoTrade
```

### 19.4 成本

至少测试：

```text
entry at conservative synthetic ask
exit at conservative synthetic bid
fees
0 / 0.05 / 0.10 / 0.20 points per leg per side slippage
```

### 19.5 Bootstrap 上线门

无需再等待新 20 天，但历史回放必须满足：

1. 无 lookahead violation；
2. 至少覆盖已有数据中的 15 个完整 session；不足时如实显示；
3. 新策略相对 legacy 在保守成本下改善以下至少两项：net PnL、Expected Shortfall、late-chase loss、false-pin loss；
4. 8 月 5 日 / 8 月 6 日两个冻结案例通过；
5. Manual Candidate 卡可完整生成；
6. automatic ordering 仍关闭。

通过后直接上线人工候选。

---

## 20. 冻结验收案例

### 20.1 2026-08-05

已知结构：高开后趋势下跌；午后约 7740 暂时平衡；最后半小时中轴下移；正式收盘约 7723.55。

预期：

```text
path_state = TRANSITION or TREND
terminal_state = PIN_MIGRATING
DePinRisk 中高
7740 Butterfly 不得成为高置信 Manual Candidate
```

若系统在 15:00 仍把 7740 标为稳定中轴，验收失败。

### 20.2 2026-08-06

已知结构：Zero Gamma 约 7709；ES Value Center 约 7707–7709；下破 7700 未持续；VIX 未扩张；权重科技与等权弱势抵消；正式收盘约 7709.96。

预期：

```text
13:00后 terminal_state 逐步转 PIN_STABLE
7710 进入 Top 3
合理时点 7710 成为 Top 1
7700/7710/7720 被枚举
宽蝶因资金效率差被降级
```

注意：不要求模型预测 7709.96 的最后小数；要求识别稳定中心区。

### 20.3 连续上涨趋势日

预期：

- GTH 不明确不能否决 RTH；
- RTH TREND 可成立；
- 只在第一/第二次受控回踩生成 Vertical；
- 突破后远离 VWAP 时输出 Late Chase；
- 不因宏观故事直接阻断已确认价格趋势。

---

## 21. 测试政策

GPT-5.6 Sol 不得把本设计再次变成测试工程项目。

### 21.1 必须测试

**数学不变量**：Vertical max loss/profit；Butterfly max loss/profit；breakeven；Call/Put Butterfly 到期收益等价；conservative BBO 方向；PnL 分位数和 ES。推荐使用 Hypothesis 处理数学性质。

**因果**：future data 被拒绝；stale quote fail closed；session/expiry 一致；缺腿不生成候选；decision snapshot 可重放。

**策略案例**：8 月 5 日；8 月 6 日；一个趋势回踩；一个 Late Chase；一个 Failed Break；一个 NoTrade。

### 21.2 不要测试

- 每个内部 helper；
- 每一句 LLM 文案；
- 临时权重的所有排列；
- JSON 中每个非关键展示字段；
- 与策略价值无关的 mock 层；
- 只是为了提高 coverage 的分支。

### 21.3 代码预算

第一阶段：

```text
新增生产代码 <= 1,200 行
新增测试     <= 600 行
新增配置项   <= 20
新增服务     = 0
新增数据库   = 0
新增 Rust    = 0
```

超过必须先回到设计审查，不得自行扩张。

---

## 22. 第三方包使用

### 22.1 立即建议

```text
scipy
scikit-learn
hypothesis (dev)
```

用途：SciPy——插值、简单优化、统计；scikit-learn——StandardScaler、NearestNeighbors、Logistic/Quantile baseline；Hypothesis——收益数学性质测试。

### 22.2 暂不立即引入

```text
cvxpy
LightGBM
CatBoost
PyTorch
```

只有以下条件出现才引入：current Q density clipping 明显破坏 mode；简单经验/nearest-neighbor P 无法校准；有足够 session 做 walk-forward；有明确 benchmark 表明新包改善经济指标。

**禁止为了“显得量化”而引入复杂模型。**

---

## 23. GPT-5.6 Sol 实施合同

每个实现 PR 前，必须先输出以下 Change Brief：

```text
Goal
Current owners reused
Files modified
Files added
Files deleted or demoted
End-to-end user-visible behavior
Algorithm formulas changed
Data lineage
Third-party package decision
Tests
Non-goals
Complexity delta
```

硬约束：

1. 先完成一个端到端人工候选，再补外围能力。
2. 不创建第二套 scheduler、outbox、state store 或 report pipeline。
3. 不新增 Rust。
4. 不把内部 Python 函数改成 subprocess/JSON IPC。
5. 不复制已有 VWAP、Gamma、Wall、calendar 或 quote-quality 算法。
6. 新增抽象必须至少有两个真实调用者，否则使用普通函数。
7. 不为每个阶段创建新的 enum/state machine。
8. 同一事实只能有一个 owner。
9. 被替代代码必须删除或降级，不能永久并存。
10. 测试优先保护数学、因果、钱和外部边界，不测试实现细节。

实现结束必须报告：行数增减、模块增减、配置增减、服务增减、数据库增减、删除了哪些旧逻辑。

**不得以“继续 Shadow 20 天”代替完成当前工程接入。**

---

## 24. 实施顺序

### PR 1：统一事实与决策出口

完成：MarketFactPack、RegimeAssessment、StrategyDecision、NO_TRADE、将 legacy candidates 作为对照输入、接入 Order Map 和人类报告。

不完成：Butterfly、新模型、LLM。

### PR 2：Vertical Anti-Chase

完成：Trend Pullback、Failed Break、Entry Quality、Late Chase、两腿 conservative BBO、Legacy vs New replay。

通过回放后，直接替换旧 Vertical 人工权限。

### PR 3：Stable Pin Butterfly

完成：Value Center、Center Score、De-pin Risk、三腿 BBO、Butterfly payoff、Top 3 centers、8 月 5/6 验收。

通过回放后，直接进入人工候选卡。

### PR 4：P/Q 与 Bootstrap Utility

完成：Q local mass、nearest-neighbor empirical P、shrinkage、net PnL quantiles、ES、Utility、NoTrade competition。

### PR 5：LLM Idea/Critic 与旧逻辑删除

完成：结构化假设、contradiction/falsifier、删除 fixed lane authority、删除旧 GTH direct candidate authority、收敛文档和配置。

---

## 25. 完成定义

算法 v2 完成不是“文件都写了”或“测试全绿”。必须同时满足：

1. 一个 Order Map 周期只产生一个 `strategy_decision`；
2. 可以明确输出 NoTrade；
3. 趋势正确但位置过晚时能够拒绝交易；
4. Failed Break 可以独立生成 Vertical；
5. Stable Pin 可以生成 Top 3 中轴；
6. Butterfly 使用三腿真实 conservative BBO；
7. 8 月 5 日识别 De-pin 风险；
8. 8 月 6 日把 7710 列入高质量中轴；
9. 历史回放使用当时可见数据；
10. 人工候选进入现有生产报告；
11. 自动下单仍关闭；
12. 旧最终候选 owner 已删除或降级；
13. 没有新增服务、数据库或 Rust；
14. 代码和测试未超过预算，或有明确批准。

---

## 26. 最终原则

系统以后不再问：

> “现在看多还是看空，所以买什么？”

而是依次问：

1. 当前事实是什么？
2. 当前路径、终值、事件和入场位置分别处于什么状态？
3. 哪些竞争假设仍然成立？
4. 当前最匹配的收益函数是 Vertical、Butterfly 还是 NoTrade？
5. 可执行价格是否仍有优势？
6. 什么事实会立即推翻这笔交易？

最终决策原则：

```text
市场状态不是交易。
方向正确不代表入场正确。
结构位不是做市商承诺。
LLM 灵感必须可证伪。
期权结构必须匹配分布形状。
NoTrade 必须能赢。
所有 Alpha 在 SPX 坐标中形成。
所有价格与风险由确定性代码计算。
```

---

## 27. v48 GTH 分钟级人工门禁

GTH 方向价差仅接受现有的已确认水平或 dip-reclaim 因果证据。它们不再因缺少
first-touch/time-stop 推广模型而永久停在观察层；生产决策改由以下分钟门授权：

- 当前 1 分钟路径收益必须与候选方向同向；
- 5 分钟路径允许回收前的残余反向，但不得超过 `0.5 × ATR5m`；
- 上游证据仍须在有效期内，双腿 conservative exact BBO 仍须通过；
- 借记不超过翼宽 45%，单张定义风险不超过 $1,000；
- 仅人工候选，自动下单保持关闭，原方向锁、冷却与 session 限额不变。

`GTH_WIDTH_SCAN`、`GTH_DELTA_SCAN` 与纯 trend-transition 背景不进入该门。该门标记
`forward_unvalidated_user_override`，不会伪装成已经验证的统计 edge。

2026-09-01 的增量结构源只在 Europe segment 读取已经冻结的 Asia High/Low：必须先有
至少两个位于区间外的已完成 ES 分钟观测（相邻可用观测间隔不超过 3 分钟），随后在
10 分钟内从区间外回测至边界 1.5 点内且不能收回区间，再在 5 分钟内重新向突破方向
延续至少 2 点。事件从最后一次延续观测可用时开始计算 5 分钟 TTL，并继续经过上述
1m/5m、IBKR exact BBO、借记、风险、方向锁和通知门。首次刺穿、没有回踩、回踩收回
Asia range 或未来分钟尚未 available 的路径只作观察；该源仍是人工-only，不能自动下单。

---

## 28. v61 Europe 已确认趋势切换

Europe segment 的已确认 ES trend-transition 可进入现有 GTH 分钟级人工门，不再无条件
归类为不可交易的趋势背景。授权从趋势状态机完成两次因果确认时开始，不把事后识别的
趋势腿高低点冒充为实时信号：

- 仅接受 `source_kind=gth_es_trend_transition` 且 `source_segment=europe`；
- Asia、US premarket 和无分段来源的纯 trend-transition 继续不可授权；
- 仍要求 5 分钟 TTL、当前 1m 同向、5m 反向幅度不超过 `0.5×ATR5m`；
- 仍要求 IBKR exact BBO、借记/翼宽不超过 45%、定义风险不超过 $1,000；
- 使用 `EUROPE_TREND_TRANSITION` setup，标记 `forward_unvalidated_user_override`；
- 只生成现有统一 `strategy_decision` 人工卡，`automatic_ordering=false`。

Asia High/Low 的后续突破—回踩失败—延续仍是独立结构确认，可用于后续再评估，不能
反向改写 Europe trend-transition 的确认时间。

---

## 29. v62 前向未验证路径否决与动量防追价

生产不再把成熟的负路径证据仅作为卡片说明。候选已经通过原 hard gate、人工授权和
排序后，先为唯一赢家计算同钟历史路径；仅当候选标记
`forward_unvalidated_user_override`、risk objective 可用且至少覆盖 20 个独立 session
时，以下任一条件直接改为 `NO_TRADE`：

- P90 净 PnL 不高于零；
- risk objective 低于零且路径亏损概率不低于 70%。

样本不足或路径不可用不会伪装成负证据；promoted model 候选继续服从其自身模型门。
该门适用于前向未验证的 Debit Vertical 与 Butterfly 赢家，不再只覆盖
`ES_VOLUME_MOMENTUM`。

`ES_VOLUME_MOMENTUM` 另增加窄的末端防追价条件：候选方向上的 VWAP 距离和 15 分钟
冲动同时超过 `2×ATR5m` 时，必须存在 15 分钟内同方向的新破位；否则只保留观察，
不生成手工交易卡。低于该双阈值的第一脚仍按原 1m/5m、放量和几何合同评估；正式
回踩结构继续由各自 setup 负责。

人工候选卡固定展示当时可见的 OI-GEX Gamma 代理：代理正 Gamma 提醒趋势可能被
压回并建议等待新破位或重新接受；代理负 Gamma 只说明已确认方向可能被放大；过渡
区只说明反馈可能切换。该说明不获得方向、确认、否决或正向加分权限，Gamma 缺失
也不得阻断候选。`dealer_position_sign=unknown` 不变，不把代理表述为真实 dealer
持仓。

策略版本为 `strategy_policy.bootstrap.v62`。自动下单保持关闭；不新增服务、定时器、
数据库、队列或通知 owner。

---

## 30. GEX × VWAP × Price Action 盘型投影（不改变授权）

参考《期权墙 · 买方篇》的 GVP 思路，把已有因果事实压成一个人读盘型，而不是再造一套
方向模型。投影只使用当前 `market_fact_pack`、正式 level state、RTH setup、VWAP、
`rth_environment` 与 OI-GEX 代理，并随 `strategy_decision` 一起审计：

- `TRUE_BREAK`：仅来自现有 `RTH_LEVEL_CONFIRMATION` 的正式 breakout；继续沿原有固定
  15 点 Directional Vertical 门禁，不新增权限；
- `FAILED_BREAK`：来自已存在的 OR/session reclaim；只提示退出或禁止追单，不自动反手；
- `TREND_PULLBACK`：来自已存在的 VWAP/已接受 OR 回踩拒绝；只作确认参考，RTH 不因该
  标签单独获得交易授权；
- `RANGE_EDGE_REJECTION`：来自正式 confirmed fade；只作减仓或区间结构参考，不生成
  反向 Directional Vertical；
- `COMPRESSION`：来自已有波动收缩平衡或 `PIN_STABLE`。正 Gamma 代理只允许继续筛选
  区间结构；负 Gamma/过渡区只提示等待放量选边。压缩本身不能授权卖波动。

Desk View 与人工候选卡各展示一行盘型；Gamma、墙位和 ICT 的既有边界不变。OI 墙仍是
结构代理，当前逐 strike 资金流不冒充书中的 Volume GEX，也不推断真实 dealer 持仓。
该投影没有新增阈值、setup 权限、服务、存储或通知 lane，因此策略版本仍为
`strategy_policy.bootstrap.v62`，`automatic_ordering=false` 不变。

同一版本另在既有 `iron_condor_map` 中前向记录一个完全去权的研究观测：Put/Call 分别
选择不超过且最接近 17.5Δ 的短腿，固定 10 点翼；当保守贷记占翼宽为 20%–23%、较小
一侧贷记至少占总贷记 25% 时标记 `qualified`。该记录不进入候选枚举、当日锁定、排序、
通知或管理，不获得人工/自动交易权限；它只用于与正式 20Δ/10 点翼合同积累去重的前向
证据。历史扫描存在同样本选择与假想持仓腿估值间隔，因此不得据此宣称 alpha 或改变
现行 23%/25% 生产门槛。

---

## 31. v63 GTH 简报与扩张转收缩铁鹰

GTH 固定 Desk Map 改为只保留一个结论、一个位置、一个结构、一个触发、一个执行和一个
数据状态。重复的“无目标/无失效位/仅人工候选”不再逐段复述；`10197` 等同一故障码只在
数据状态中出现一次。完整方向候选和铁鹰候选仍使用各自独立人工卡，不塞回固定简报。

研究结果只形成一行决策参考：ICT 的 `MSS_DISPLACEMENT_CONFIRMED`、Spring ready 方向与
仍在有效期内的 0DTE 资金流背离，被归纳为“确认 / 冲突 / 无有效确认”。同向只能解释为
确认参考，反向提示少追价；缺失不得阻断，任何研究项都不能单独创建、翻转或授权交易。

GTH 铁鹰新增一条用户明确授权、仍属前向未验证的人工候选合同。这里的“Gamma 扩张转
收缩”不推断 dealer 仓位，实际使用因果可观测的 ATM 跨式与 ATM IV 作为 short-gamma
压力代理：

- GTH 跨式至少 30 个有效观测，先从会话低点扩张至少 10%，且低点时间早于峰值；
- 峰值已形成 5–120 分钟，当前跨式较峰值收缩至少 8%，15 分钟衰减至少 3%；
- ATM IV 的 5/15 分钟变化均不为正，最近 15 分钟价格位移不超过 `1.25×ATR5m`；
- Put 与 Call 分别使用 IBKR 最新 Greeks，选择绝对 Delta 不超过且最接近 20Δ 的短腿；
- 两侧固定 10 点保护翼，四腿必须全为不超过 15 秒、源偏斜不超过 2 秒的 IBKR exact BBO；
- 保守贷记占翼宽 25%–55%，较小一侧至少贡献总贷记 25%，定义风险不超过 $1,000；
- 四腿组合 `GCR10 = 0.5×|net gamma|×10²/credit` 不超过 20%；Gamma 缺失时失效关闭；
- 每个 GTH session 最多一张，只允许人工限价，`automatic_ordering=false`；
- 回购价不高于 `0.5C` 止盈，不低于 `3C` 止损，未触发则次日 12:30 ET 清仓。

普通横盘、单纯 IV 低、只有一次跨式尖峰、仍在扩张、趋势位移过大、Schwab 冻结报价或
IBKR `10197` 冲突都不能生成该人工卡。该合同标记
`forward_unvalidated_user_override`，不得宣称已经证明长期 alpha。

策略版本为 `strategy_policy.bootstrap.v63`。不新增服务、定时器、数据库、队列或 Rust
合同；自动下单继续关闭。

---

## 32. v64 GTH 铁鹰报价新鲜度修正

GTH 扩张转收缩铁鹰的四腿 IBKR exact BBO 与各腿 Greeks 最大年龄由 15 秒放宽为
30 秒。四腿 BBO 的源时间偏斜放宽为 10 秒，避免 IBKR 未同步更新的安静腿造成运营误拒，
同时防止把跨度过大的市场时刻拼成虚假贷记；
Greeks 只要求每条腿各自在 30 秒内，不要求四腿 Greeks 同时更新。31 秒及以上的报价或
Greeks 继续失效关闭，Schwab frozen quote 与 IBKR `10197` 仍不得生成候选。

其他 GTH 铁鹰合同保持不变：20Δ 短腿、10 点翼、扩张转收缩、25%–55% 贷记、两侧
平衡、`GCR10≤20%`、定义风险不超过 $1,000、每 GTH session 最多一张、人工限价且
`automatic_ordering=false`。策略版本为 `strategy_policy.bootstrap.v64`；不新增服务、
定时器、数据库、队列或 Rust 合同。

---

## 33. v65 GTH 铁鹰局部扩张周期

GTH 铁鹰不再使用整段夜盘唯一的 ATM 跨式最高点作为扩张峰值。系统在既有
`atm_straddle_session` 状态内额外保存当前因果局部周期的基准低点与峰值：尚未扩张
10% 时允许更低的后续观测重置基准；达到 10% 后冻结基准并追踪更高峰值；基准或峰值
超过 120 分钟后，以当时新鲜观测开始下一周期。整段 GTH 的绝对高低仍保留用于
Desk Map，不再阻止后续较低但独立的扩张转收缩周期。

人工候选仍要求局部峰值形成 5–120 分钟、从局部峰值收缩至少 8%、15 分钟跨式衰减
至少 3%、ATM IV 5/15 分钟不扩张、15 分钟价格位移不超过 `1.25×ATR5m`。部署中途
缺少局部周期状态时从部署后的第一条新鲜观测开始，不利用已发生路径反推峰谷。

其余 GTH 铁鹰合同保持不变：20Δ 短腿、10 点翼、IBKR 四腿 BBO/Greeks ≤30 秒、
报价源偏斜 ≤10 秒、25%–55% 贷记、两侧平衡、`GCR10≤20%`、定义风险不超过
$1,000、每 GTH session 最多一张、人工限价且 `automatic_ordering=false`。策略版本为
`strategy_policy.bootstrap.v65`；不新增服务、定时器、数据库、队列或 Rust 合同。

GTH Desk Map 即使尚未达到人工授权，也展示当前 20Δ/10 宽四腿、保守贷记、
贷记/翼宽比及首要待触发条件；文案明确标注“仅观察”，不因展示完整结构而获得
人工入场或自动下单权限。
