# SPX Spark 策略信号引擎 v4：命题、赔率、路径与波动率优势

状态：**设计草案，等待批准；本文只定义账户无关的策略引擎，不授权实现或部署。**  
适用仓库：`hzy-hits/SpxOpDaily`  
目标路径：`docs/strategy-signal-engine-v4.md`  
基线提交：`e279ba6029ccba2dbbe4d2b98ceb0f688f43b487`  
自动下单：**继续禁止**

本文是 `docs/strategy-signal-engine-v2.md`、`docs/strategy-signal-engine-v3.md` 和
`docs/strategy-signal-engine-v3-p1p2-design.md` 之上的增量设计。除本文明确修订的部分外，既有合同继续有效。

v4 的边界经过一次明确收缩：

> **策略引擎只判断“市场上是否存在一笔值得考虑的、单位化的 SPXW 交易”。**
>
> 它不读取账户净值、购买力、持仓、日内盈亏或资金规模；不决定买几组；不做账户级风险锁；不因为用户已有其他仓位而改变市场判断。

仓位大小、组合风险、账户相关性和是否实际执行，属于策略引擎之外的执行层或人工决策。

---

## 0. Change Brief

### 0.1 用户可见目标

让系统从目前的：

```text
市场状态 → Call / Put / Butterfly / NoTrade
```

升级为：

```text
市场事实
  → 明确、可证伪的交易命题
  → 市场给出的可执行赔率
  → 现实概率或路径风险估计
  → 候选结构的单位化净收益分布
  → MANUAL_CANDIDATE / RESEARCH_CANDIDATE / NO_TRADE
```

系统应能主动回答：

- 这笔交易究竟押注什么；
- 到期、路径、触碰还是钉住，哪个事件决定收益；
- 市场要求多高概率才能保本；
- 我们是否有独立证据认为实际概率优于市场赔率；
- 候选是否因报价、路径、事件、尾部或成本而不成立；
- 该候选是已验证、待验证，还是仅有合理叙事。

典型输出从：

```text
CALL_DEBIT_VERTICAL · 看涨
```

升级为：

```text
命题：SPX 到期结算高于 7732.50
结构：5 点 Call Debit Vertical
可执行 Debit：2.40
Debit / Width：48.0%
手续费后近似保本概率：50.2%
现实概率：55.0%，保守下界 46.0%（未校准）
单位最大损失：$240 + 成本
结论：RESEARCH_CANDIDATE
```

### 0.2 明确删除的账户职责

v4 策略引擎不包含：

- Account Risk Gate；
- 账户净值或 Buying Power；
- 单笔占账户百分比；
- 日、周、月亏损锁；
- 当前持仓去重；
- SPX、SPY、TLT 跨资产因子暴露；
- 根据账户大小选择 1 组或 5 组；
- 根据用户过往盈亏调整候选权限。

策略引擎只输出**每一策略单位**的：

- 最大收益；
- 最大损失；
- 盈亏平衡点；
- 可执行报价；
- 费用压力；
- 路径与终值风险；
- 研究状态。

### 0.3 现有 owner 与文件

继续复用当前策略链，不新建第二套系统：

```text
src/spx_spark/macro_event_clock.py
src/spx_spark/application/market_features/service.py
src/spx_spark/application/market_features/strategy_distribution_forecast.py
src/spx_spark/application/market_features/physical_followthrough.py
src/spx_spark/application/order_map/strategy_facts.py
src/spx_spark/application/order_map/strategy_regime.py
src/spx_spark/application/order_map/candidate_factory.py
src/spx_spark/application/order_map/strategy_ranker.py
src/spx_spark/application/order_map/strategy_select.py
src/spx_spark/application/order_map/strategy_outcomes.py
src/spx_spark/application/order_map/delivery.py
src/spx_spark/analytics/options/strategy_payoff.py
src/spx_spark/infrastructure/operational_db.py
```

第一版最多新增一个生产文件：

```text
src/spx_spark/application/market_features/strategy_propositions.py
```

该文件只负责从 point-in-time 事实生成命题；候选枚举、门控、排序与输出继续由现有 owner 负责。

### 0.4 新依赖、进程与持久化

- 新依赖：0；
- 新 service/timer/queue：0；
- 新数据库/表：0；
- 新 Rust contract/consumer：0；
- 自动下单：仍为 false；
- 新研究产物继续使用既有 `features/`、`research/` 和 outcomes 管道。

### 0.5 最小端到端路径

```text
MarketFactPack
  → StrategyPropositionSet
  → PathRegime + PricingEdge
  → Candidate Factory
  → Structure Hard Gates
  → Ranker
  → strategy_decision
  → MANUAL_CANDIDATE / RESEARCH_CANDIDATE / NO_TRADE
  → 多 horizon 与路径标签
```

---

## 1. 当前系统还差什么

当前系统已经具有较强的数据质量、SPX/ES 坐标、RTH 路径状态、精确多腿报价、借方价差、Butterfly、near-miss 和因果 outcome 能力。

真正缺少的不是更多指标，而是以下四个决策环节。

### 1.1 缺少“命题”这一层

系统目前通常从已经出现的价格确认开始：

- Failed Break；
- Trend Pullback；
- Confirmed Level；
- Stable Pin。

但很多事件交易首先是一个终值命题：

```text
今天是否收在昨日收盘上方？
CPI 后是否收在事件前价格上方？
到期是否越过某个结构阈值？
到期是否仍在一个区间内？
```

例如：

```text
+7730C -7735C，Debit < 2.50
```

本质不是“买一个 Call Spread”，而是：

```text
H: SPX settlement > approximately 7732.5
```

没有命题层，系统只能在别人给出 strike 后解释，不能主动扫描。

### 1.2 缺少赔率层

对于宽度为 `W`、净 Debit 为 `D` 的窄幅 Call Vertical：

```text
D / W
```

可以作为局部风险中性概率价格的近似，但不是现实胜率。

当前代码已经计算 `debit_fraction_of_width`，但主要将其用于 late-chase 与结构门控，没有把它提升为候选卡的核心赔率字段。

### 1.3 路径状态与期权贵贱尚未分开

`TREND`、`BALANCED`、`PIN_STABLE` 回答市场怎么走；它们不能回答期权是否值得买或卖。

横盘时：

- 期权可能仍然昂贵，适合卖方研究；
- 也可能已经过度塌缩，卖方赔率很差。

趋势时：

- Debit Vertical 可能仍便宜；
- 也可能已追价到没有剩余收益空间。

因此需要独立的 `PricingEdge`，而不是继续把方向状态当成交易优势。

### 1.4 卖方结构缺少正确的现金流和路径语义

现有系统以 Debit 为中心：

- 开仓使用 combo ask；
- 退出使用 combo bid；
- ManagementPolicy 适用于长期权；
- Outcome 计算以 entry debit 为基准。

直接加入 Credit Spread 或 Iron Condor 会把开仓 Credit、Buyback Ask、止盈、止损和最大风险全部混淆。

卖方还必须研究 first-touch，而不能只看最终是否 ITM。

---

## 2. 目标与非目标

### 2.1 v4 目标

1. 建立 `StrategyProposition`，将宏观或盘面观点转换为明确的终值或路径命题。
2. 建立 Debit/Credit 通用、单位化的策略经济学。
3. 建立路径状态与 `PricingEdge` 的正交判断。
4. 增加事件结算阈值 Vertical，主动扫描类似“昨日收盘附近的 5 点价差”。
5. 增加单侧定义风险 Credit Vertical。
6. 在两侧单独合格后，才允许组合 Iron Condor。
7. 为卖方建立 first-touch、恢复、继续突破和 long-wing reach 标签。
8. 增加 `RESEARCH_CANDIDATE`，使结构完整但优势未验证的候选可见。
9. 继续保持 `strategy_decision` 为唯一人工候选权限出口。
10. 用前瞻、样本外、费用后的结果决定策略是否升为 Manual。

### 2.2 v4 非目标

第一版不做：

- 账户净值、Buying Power 或仓位大小；
- 账户级风险限制；
- 跨资产持仓和相关性；
- 裸 Call、裸 Put、未定义风险 Ratio；
- Calendar、Diagonal、Jade Lizard、Broken-Wing Butterfly；
- SPY、TLT 或其他非 SPXW 执行候选；
- 自动下单、自动滚仓或自动加仓；
- 将 Twitter、LLM 或自然语言共识直接转换成概率；
- 用 Delta 直接称为胜率；
- 用少量样本宣称稳定 Alpha。

---

## 3. 简化后的权威链

```text
Provider-normalized data
  → MarketFactPack
  → StrategyPropositionSet
  → PathRegime
  → PricingEdge
  → Candidate Factory
  → Structure-specific Hard Gates
  → Ranker
  → strategy_decision
```

只保留三个核心研究对象：

```yaml
path_regime:
  state: TREND | CONVERGENCE | PIN | SHOCK | TRANSITION | UNCERTAIN
  direction: UP | DOWN | NONE

proposition:
  kind: TERMINAL_ABOVE | TERMINAL_BELOW | TERMINAL_BETWEEN |
        FIRST_TOUCH_UPPER | FIRST_TOUCH_LOWER
  levels: [...]
  target_at: ...

pricing_edge:
  state: RICH | FAIR | CHEAP | UNAVAILABLE
  basis: TERMINAL_PNL | MANAGEMENT_PNL | FIRST_TOUCH
```

这三层互不替代：

- `TREND` 不自动等于可以买；
- `CONVERGENCE` 不自动等于可以卖；
- `PIN` 不自动等于 Butterfly 便宜；
- 一个合理命题不自动等于市场价格有优势；
- 一个便宜结构不自动等于现实概率支持它。

---

## 4. Strategy Proposition

### 4.1 Python-only schema

第一版命题只存在于 Python `MarketFactPack` 与 `strategy_decision`，不进入 Rust 或 golden contract。

```yaml
schema_version: strategy_proposition.v1
proposition_id: proposition:<stable-hash>
source_kind: PRIOR_CLOSE | EVENT_PRE_CLOSE | OVERNIGHT_SYNTHETIC |
             Q_MEDIAN | Q_MODE | ZERO_GAMMA | FLIP_ZONE |
             PUT_WALL | CALL_WALL | CONFIRMED_LEVEL
session: GTH | RTH
expiry: YYYYMMDD
target_at: iso8601
kind: TERMINAL_ABOVE | TERMINAL_BELOW | TERMINAL_BETWEEN |
      FIRST_TOUCH_UPPER | FIRST_TOUCH_LOWER
lower_level: float | null
upper_level: float | null
reference_level: float | null
thesis_direction: UP | DOWN | NEUTRAL
macro_event_id: string | null
spans_event: bool
available_at: iso8601
valid_until: iso8601
provenance:
  coordinate_kind: OFFICIAL_SPX | CHAIN_IMPLIED_SPX | ES_BASIS_ADJUSTED
  source_fields: []
  source_times: []
research_status: UNVALIDATED | CALIBRATING | VALIDATED
```

### 4.2 命题不是策略

同一个命题可以有多种表达：

```text
H: SPX settlement > 7732.5
```

可能对应：

- 7730/7735 Call Debit Vertical；
- 7725/7735 Call Debit Vertical；
- 更宽的 Call Debit Vertical；
- 不交易。

命题层不得直接选择 strike 或创建人工候选。

### 4.3 初始命题来源

第一版只允许确定性来源：

1. prior close；
2. 当前事件前最后一个官方 SPX close；
3. 合格的 overnight synthetic SPX；
4. Q median；
5. Q mode；
6. Zero Gamma / Flip；
7. Put Wall / Call Wall；
8. 已确认的 level decision。

禁止直接从：

- Twitter 文本；
- LLM 观点；
- 新闻标题；
- “连续两天下跌所以该涨”；

生成概率或人工候选。

这些内容以后可以作为研究 feature，但不能成为数值权威。

### 4.4 去重与数量限制

同一 expiry、kind 和相近阈值的命题合并：

```text
abs(level_a - level_b) <= 2.5 points
```

每个 cycle 最多保留：

- 3 个 TERMINAL_ABOVE；
- 3 个 TERMINAL_BELOW；
- 2 个 TERMINAL_BETWEEN；
- 2 个 first-touch 命题。

排序优先级：

1. 明确事件或前收阈值；
2. 当前结构位；
3. Q median/mode；
4. 其他研究阈值。

---

## 5. PathRegime

保留并整理现有状态：

```text
TREND
CONVERGENCE
PIN
SHOCK
TRANSITION
UNCERTAIN
```

### 5.1 TREND

需要：

- 方向分数；
- 路径效率；
- VWAP 斜率；
- VWAP 穿越次数；
- Breadth；
- 价格与 VWAP 方向一致。

用途：生成方向性 Debit Vertical 研究候选。

### 5.2 CONVERGENCE

需要：

- 低路径效率；
- 多次 VWAP 穿越；
- 区间收缩；
- Breadth 接近中性；
- 没有 active shock；
- 没有一侧形成持续接受。

用途：允许评估 Credit Vertical / Iron Condor，但仍必须通过 PricingEdge。

### 5.3 PIN

继续使用现有：

- Value Center；
- Q mode；
- local mass；
- 多次离开后返回；
- de-pin risk；
- VIX/Breadth；
- recent extreme；
- shock veto。

用途：Butterfly。

### 5.4 SHOCK

以下任一成立：

- 现有 intraday shock ACTIVE；
- post-event discovery 未完成；
- 价格或 IV 在短窗口异常扩张；
- Value Center 尚未重新形成。

用途：禁止新 Short Premium 与 Pin Butterfly；方向 Debit 仅在后续确认后重新竞争。

---

## 6. PricingEdge

### 6.1 定义

```yaml
schema_version: pricing_edge.v1
state: RICH | FAIR | CHEAP | UNAVAILABLE
basis: TERMINAL_PNL | MANAGEMENT_PNL | FIRST_TOUCH
q_probability: float | null
p_probability: float | null
p_interval_low: float | null
required_probability: float | null
expected_pnl_points: float | null
conservative_pnl_points: float | null
expected_shortfall_points: float | null
sample_count: int
session_count: int
model_status: UNAVAILABLE | UNCALIBRATED | CALIBRATING | VALIDATED
reason_codes: []
```

### 6.2 基本原则

市场 Edge 不是：

```text
看涨概率高
```

而是：

```text
现实测度下的预期净收益
>
可执行权利金 + 费用 + 滑点 + 模型误差缓冲
```

即：

```text
EV_net = E_P[payoff or management PnL]
         - executable premium
         - fees
         - slippage
```

### 6.3 状态判定

第一版：

```text
RICH:
  对卖方而言，保守 P 模型下净期望为正；
  对买方而言，表示结构价格相对 P 过高，不宜买。

CHEAP:
  对买方而言，保守 P 模型下净期望为正；
  对卖方而言，表示卖出补偿不足。

FAIR:
  点估计接近零，或优势不足以覆盖误差缓冲。

UNAVAILABLE:
  P/Q、报价、费用或路径数据不足。
```

为避免混淆，候选卡应同时展示：

```text
pricing_edge_for_long
pricing_edge_for_short
```

或直接展示该具体候选的 `expected_pnl`，不只给一个抽象形容词。

### 6.4 未验证阶段

在达到 promotion 门槛前：

- PricingEdge 只能排序和展示；
- 不能单独产生 Manual Candidate；
- 结构完整但 Edge 未验证时输出 `RESEARCH_CANDIDATE`；
- 不能因为模型说有 Edge 而绕过报价、路径和事件硬门。

---

## 7. 通用 Debit/Credit 经济学

### 7.1 有符号现金流

统一约定：

```text
账户收到现金：正
账户支付现金：负
```

```yaml
entry_cashflow:
  kind: DEBIT | CREDIT
  points: float
  executable_basis: CONSERVATIVE_BBO

exit_cashflow:
  kind: CREDIT | DEBIT
  points: float
  executable_basis: CONSERVATIVE_BBO
```

单位化净收益：

```text
PnL_points = entry_cashflow + exit_cashflow - fees_points - slippage_points
```

示例：

- Debit Vertical：开仓 `-2.40`，平仓 `+4.50`；
- Credit Spread：开仓 `+1.20`，平仓 `-0.50`。

### 7.2 StrategyUnitEconomics

```yaml
schema_version: strategy_unit_economics.v1
quantity_basis: ONE_STRATEGY_UNIT
entry_kind: DEBIT | CREDIT
entry_price_points: float
width_points: float | null
max_gain_points: float
max_loss_points: float
breakeven_levels: []
fees_points_round_trip: float
slippage_stress_points: float
max_gain_usd_per_unit: float
max_loss_usd_per_unit: float
```

这里的 USD 仅表示 SPXW 每一策略单位、乘数 100 的金额，不包含账户规模或仓位建议。

### 7.3 Conservative BBO

Debit：

```text
entry = buy legs at ask + sell legs at bid
exit  = sell long legs at bid + buy short legs at ask
```

Credit：

```text
entry = sell short legs at bid - buy long legs at ask
exit  = buy short legs at ask - sell long legs at bid
```

禁止用多腿 mid 代替可执行价格。

### 7.4 费用

候选经济学必须同时输出：

- gross payoff；
- commission estimate；
- one-tick adverse slippage；
- two-tick stress；
- net payoff。

费用不是事后报告字段，而是候选筛选的一部分。

---

## 8. 策略候选空间

v4 候选固定为：

```text
NO_TRADE
CALL_DEBIT_VERTICAL
PUT_DEBIT_VERTICAL
CALL_BUTTERFLY
PUT_BUTTERFLY
CALL_CREDIT_VERTICAL
PUT_CREDIT_VERTICAL
IRON_CONDOR
```

`EVENT_SETTLEMENT_VERTICAL` 不新增 payoff 类型，而是 Debit Vertical 的 `setup_kind`。

---

## 9. Directional Debit Vertical

继续复用现有：

```text
FAILED_BREAK_RECLAIM
TREND_PULLBACK
BREAKOUT_ACCEPTANCE
```

### 9.1 生成条件

- PathRegime = TREND，或有效失败突破；
- 方向、VWAP、Breadth 不冲突；
- target / invalidation 几何完整；
- exact two-leg quote ready；
- 候选没有 late chase；
- debit/width、target room 和 stop ATR 通过现有硬门。

### 9.2 赔率展示

候选卡增加：

```text
Debit / Width
Max Gain / Max Loss
Breakeven
Required terminal probability proxy
Management-policy EV
```

不得只展示最大利润。

---

## 10. Event Settlement Vertical

这是 v4 的第一个新增能力，用于识别类似 `7730/7735 < 2.50` 的交易。

### 10.1 命题

```text
H: SPX settlement > L
```

或：

```text
H: SPX settlement < L
```

### 10.2 候选构造

对每个阈值 `L`，枚举：

- 5 点宽；
- 10 点宽；
- 阈值位于两个 strike 之间或靠近价差中部；
- front expiry 与命题 target_at 一致；
- exact quote ready。

Call 示例：

```text
K1 < L < K2
long K1 Call
short K2 Call
```

Put 对称。

### 10.3 关键字段

```yaml
setup_kind: EVENT_SETTLEMENT_THRESHOLD
threshold_level: L
threshold_source: PRIOR_CLOSE | EVENT_PRE_CLOSE | ...
width: W
executable_debit: D
odds_proxy: D / W
breakeven: K1 + D   # Call
required_probability_after_costs: (D + costs) / W
```

### 10.4 重要语义

```text
D / W = 48%
```

不等于：

```text
现实胜率 = 52%
```

它只能解释为局部风险中性赔率代理。

只有：

```text
P_conservative > required_probability + model_buffer
```

时，才能说存在候选 Edge。

### 10.5 事件时钟

现有 `pre_event → entry_allowed=false` 改成策略家族权限：

```yaml
pre_event:
  EVENT_SETTLEMENT_VERTICAL: RESEARCH_ALLOWED
  DIRECTIONAL_DEBIT_VERTICAL: BLOCKED
  BUTTERFLY: BLOCKED
  CREDIT_VERTICAL: BLOCKED
  IRON_CONDOR: BLOCKED

post_event_discovery:
  EVENT_SETTLEMENT_VERTICAL: MANAGE_EXISTING_ONLY
  DIRECTIONAL_DEBIT_VERTICAL: WAIT_CONFIRMATION
  BUTTERFLY: BLOCKED
  CREDIT_VERTICAL: BLOCKED
  IRON_CONDOR: BLOCKED

normal:
  all_supported_families: EVALUATE_NORMALLY
```

第一版 Event Settlement 候选只输出 `RESEARCH_CANDIDATE`；升为 Manual 需独立验证。

---

## 11. Credit Vertical

先做单侧，再做 Iron Condor。

### 11.1 Put Credit Vertical

命题：

```text
SPX 在管理窗口内不会有效跌破下方边界
```

候选：

```text
short Put at K_short
long Put at K_short - width
```

### 11.2 Call Credit Vertical

命题：

```text
SPX 在管理窗口内不会有效突破上方边界
```

候选对称。

### 11.3 生成条件

第一版固定：

- 仅 RTH；
- 仅 0DTE；
- 5 点宽；
- short strike 的绝对 Delta 只作为定位工具，目标 0.10–0.18；
- short strike 必须位于 Opening Range 与结构失效位之外；
- no active shock；
- exact two-leg credit BBO ready；
- Credit 必须显著高于费用与滑点；
- PricingEdge 对 short side 为 RICH 或至少不是 CHEAP。

### 11.4 Delta 语义

禁止：

```text
15 Delta = 15% 亏损概率
```

候选卡必须同时展示：

- terminal ITM probability；
- first-touch probability；
- short strike touch 后恢复率；
- long wing reach probability。

### 11.5 初始权限

Credit Vertical 第一阶段只进入 Shadow / Research Candidate，不直接升为 Manual。

---

## 12. Iron Condor

### 12.1 生成顺序

禁止直接使用“两侧各 12 Delta”生成 Iron Condor。

必须先有：

```text
通过硬门的 Put Credit Vertical
AND
通过硬门的 Call Credit Vertical
```

然后再组合成四腿候选。

### 12.2 必要条件

- PathRegime = CONVERGENCE；
- no active shock；
- 两侧 short strike 均在 OR / invalidation 外；
- 两侧独立 first-touch 风险均可接受；
- 四腿同 provider、同 expiry、时间偏差合格；
- combined Credit 大于单侧费用之和；
- combined expected PnL 在保守路径模型下为正；
- 任一侧只有“Delta 很远”但没有 P 模型时，不生成。

### 12.3 单位经济学

```text
max_gain = total_credit
max_loss = wing_width - total_credit
```

若两侧宽度不同，使用较大单侧损失明确计算，不能用对称公式偷换。

### 12.4 路径风险

必须输出：

```text
P(touch short put)
P(touch short call)
P(touch either side)
P(recover after touch)
P(reach long wing)
P(finish between breakevens)
```

仅报告 POP 不合格。

### 12.5 初始权限

Iron Condor 在完成前瞻验证前始终为：

```text
RESEARCH_CANDIDATE
```

---

## 13. Butterfly

Butterfly 延续现有严格条件：

- `PIN_STABLE`；
- body 对齐 Value Center 与 Q mode；
- body 距 spot 不过远；
- no active shock；
- recent extreme 为 false；
- de-pin risk 低；
- debit/width 合格；
- 三腿 BBO ready。

v4 只增加赔率解释：

```text
Q mass under tent
P terminal mass under tent
Debit / Width
Management-policy EV
```

不放宽 Pin Gate。

---

## 14. First-Touch 与路径标签

### 14.1 为什么需要

到期 OTM 不代表持有路径安全。

一张 15 Delta Put 可能：

- 盘中触碰 short strike；
- 组合价格触发止损；
- 最终又收回 OTM。

只看结算会把真实亏损错误标成盈利。

### 14.2 标签

对每个卖方候选记录：

```yaml
short_put_touched: bool | null
short_call_touched: bool | null
first_touch_at: iso8601 | null
first_touch_side: PUT | CALL | null
touch_recovered: bool | null
accepted_beyond_short: bool | null
long_wing_reached: bool | null
finish_between_breakevens: bool | null
max_adverse_excursion_points: float | null
max_favorable_excursion_points: float | null
```

### 14.3 恢复与继续

第一版透明规则：

```text
TOUCH:
  minute low/high crosses short strike

RECOVERED:
  touch 后 10 分钟内重新进入 short strike 内侧，并连续 2 根 5m 收盘保持

ACCEPTED_BEYOND:
  touch 后连续 2 根 5m 收盘仍在 short strike 外侧

LONG_WING_REACHED:
  minute high/low reaches protective wing
```

阈值是 bootstrap 定义，后续改动必须版本递增并做 replay 对照。

### 14.4 模型

第一版不引入复杂机器学习。

使用：

- 同时钟历史样本；
- session-level weighting；
- event / non-event 分桶；
- PathRegime 分桶；
- Beta-Binomial shrinkage；
- Day-block bootstrap 置信区间。

只有样本充分后再考虑 challenger 模型。

---

## 15. ManagementPolicy

三类策略使用不同 policy。

### 15.1 Long Premium

继续使用现有、版本化的：

```text
entry = conservative ask
valuation = conservative bid
profit arm
trail
premium stop
time stop
hard close
```

### 15.2 Event Settlement Debit

事件前普通 Stop 不能限制跳空损失。

因此：

- 硬最大损失 = 全部 Debit + 费用；
- 不允许把 Stop 价格写成“最大风险”；
- outcome 同时保存 event move、IV crush 和 settlement payoff；
- 第一版使用持有到结算或明确的事件后时间退出，两者分别统计，不混合。

### 15.3 Short Premium

```yaml
policy_version: management_policy.short_premium.v1
entry_basis: conservative_combo_bid
valuation_basis: conservative_buyback_ask
profit_take_fraction_of_credit: 0.50
premium_stop_multiple: 2.00
path_invalidation_enabled: true
hard_exit_et: "15:15"
hold_to_settlement: false
```

这些值是冻结研究参数，不代表已证明最优。

### 15.4 策略样本不得混合

以下必须分开统计：

- 事件前持有到结算；
- 事件后主动退出；
- 0DTE Short Premium；
- 1DTE 当天平仓；
- GTH 入场；
- RTH 入场。

---

## 16. Hard Gates

所有候选先过确定性硬门，再谈 Edge。

### 16.1 通用 Gate

- session legal；
- coordinate ready；
- proposition 未过期；
- expiry 与 target_at 一致；
- exact legs complete；
- same provider；
- quote fresh；
- cross-leg time skew 合格；
- executable BBO 有效；
- fees/slippage 后 payoff 合法；
- event permission 允许该策略家族；
- `automatic_ordering=false`；
- `manual_action_only=true`。

### 16.2 Debit Gate

- target / stop geometry；
- target room；
- debit/width；
- no late chase；
- proposition 与方向一致；
- breakeven 没有明显越过命题合理阈值。

### 16.3 Credit Gate

- defined risk；
- short strike 在 invalidation 外；
- Credit 大于费用压力；
- buyback BBO 可计算；
- no shock；
- first-touch evidence available；
- long-wing risk 可估计。

### 16.4 Iron Condor Gate

- 两侧单独 Credit Vertical 均通过；
- four-leg BBO ready；
- no leg cancellation anomaly；
- combined path model available；
- PathRegime = CONVERGENCE；
- event lane 未禁止卖方。

---

## 17. 排序

### 17.1 排序对象

`NO_TRADE` 与所有通过硬门的候选一起比较。

### 17.2 初始分数

```text
Score = conservative_expected_pnl / max_loss
        - tail_loss_penalty
        - liquidity_penalty
        - model_uncertainty_penalty
```

其中：

- `max_loss` 是每一策略单位的定义风险；
- `tail_loss` 使用该策略对应的 PnL 分布；
- `liquidity` 使用可执行 bid/ask；
- `uncertainty` 来自样本和区间宽度。

### 17.3 权限边界

在校准不足时：

- Score 只用于排序；
- 不因 Score 为正自动升为 Manual；
- 硬门全过但 edge 未验证 → `RESEARCH_CANDIDATE`；
- 已验证策略族且 promotion 条件通过 → `MANUAL_CANDIDATE`；
- 无候选通过 → `NO_TRADE`。

---

## 18. strategy_decision v4

仍为 Python-only，不进入 Rust 投影。

```yaml
schema_version: strategy_decision.v4
decision_id: ...
decision_type: MANUAL_CANDIDATE | RESEARCH_CANDIDATE | NO_TRADE
policy_version: strategy_policy.v4
runtime_git_sha: ...
decision_at: ...
available_at: ...

path_regime: {...}
proposition: {...}
pricing_edge: {...}

candidate:
  candidate_id: ...
  strategy_type: ...
  setup_kind: ...
  expiry: ...
  legs: [...]
  quote: {...}
  economics: {...}
  management_policy_version: ...

unit_risk:
  quantity_basis: ONE_STRATEGY_UNIT
  max_loss_points: ...
  max_loss_usd: ...
  max_gain_points: ...
  max_gain_usd: ...
  breakevens: [...]

why_not:
  reasons: [...]
  nearest_candidates: [...]
  reauthorize_on: ...

automatic_ordering: false
manual_action_only: true
```

明确不出现：

```text
account_nlv
buying_power
recommended_contracts
risk_fraction_of_account
daily_loss_limit
```

---

## 19. 人类候选卡

### 19.1 Event Debit 示例

```text
RESEARCH CANDIDATE · Event Settlement Vertical

命题
SPX 结算高于 7732.50
来源 prior_close

结构
5-point Call Debit Vertical
可执行 Debit 2.40
Debit/Width 48.0%
盈亏平衡 7732.40

赔率
手续费后近似保本概率 50.2%
P 点估计 55.0%
P 保守下界 46.0%
状态 UNCALIBRATED

单位经济学
最大损失 $240 + 费用
最大收益 $260 - 费用

结论
结构完整，赔率有吸引力，但现实概率下界尚未超过保本门槛；研究候选。
```

### 19.2 Iron Condor 示例

```text
RESEARCH CANDIDATE · Iron Condor

路径
CONVERGENCE

定价
总 Credit 1.40
翼宽 5
最大收益 $140 - 费用
最大损失 $360 + 费用

路径风险
P(touch put) 18%
P(touch call) 16%
P(touch either) 29%
P(long wing reached) 7%
P(finish between breakevens) 73%

Edge
Expected PnL +0.12 points
保守下界 -0.08 points
状态 CALIBRATING

结论
未达到 Manual promotion；仅研究候选。
```

---

## 20. Outcomes 与统计单位

### 20.1 样本单位

一个完整策略仓位 = 一个样本。

不是：

- 每条腿；
- 每个 fill；
- 同一 opportunity 每个 tick；
- 同一天重复生成的同一候选。

继续使用 `opportunity_id` 去重。

### 20.2 必须保存

```text
entry executable BBO
fees/slippage assumptions
multi-horizon marks
management-policy exit
terminal payoff
first-touch labels
invalidation breach
censor kind
path regime
proposition
pricing edge
policy versions
```

### 20.3 未成交问题

v4 核心策略引擎不要求读取真实账户或订单。

第一版使用：

- conservative executable BBO；
- quote-reached；
- one/two-tick adverse slippage stress。

不得把 `quote_reached` 称为真实成交概率。

真实 fill 分析可以作为独立研究输入，不能成为策略引擎的账户依赖。

---

## 21. Promotion 门槛

每个策略家族单独验证，不允许用其他策略的盈利为其背书。

### 21.1 数据划分

- 训练/开发：时间顺序前 60%；
- 验证：中间 20%；
- OOS：最后 20%；
- 同一交易日不得跨集合；
- 重叠 horizon purge；
- 置信区间按交易日 block bootstrap。

### 21.2 最低样本

升为 Manual 前：

- 总样本 ≥100；
- 独立交易日 ≥40；
- OOS 样本 ≥30；
- 每个核心 regime 桶有效样本 ≥20，否则回退到上层桶并显式标注。

### 21.3 通用门槛

- 扣费后 OOS 平均 PnL > 0；
- OOS Profit Factor ≥1.10；
- Full Sample Profit Factor ≥1.20；
- 95% Day-block bootstrap 均值下界 > 0，或保持 Research；
- 双倍费用与滑点下期望不显著为负；
- 最大回撤与 ES10 在设计限制内；
- 参数在 OOS 前冻结。

### 21.4 Event Settlement Vertical

额外要求：

- `required_probability` 校准可靠；
- Brier Score 优于无条件基线；
- 连续两日涨跌等特征必须通过样本外验证；
- 删除最好三笔后，整体期望仍不为负；
- 事件类型分开：CPI、FOMC、NFP 不混桶。

### 21.5 Credit Vertical / Iron Condor

额外要求：

- first-touch 校准通过；
- touch 后恢复/继续模型有稳定区分；
- 人为每 25 笔插入一次 max-loss 后仍不显著为负；
- 结算回测与真实 ManagementPolicy 结果必须分别报告；
- 不得只用胜率或 POP promotion。

---

## 22. 实施顺序

### Phase A：通用经济学

修改：

```text
analytics/options/strategy_payoff.py
application/order_map/strategy_outcomes.py
application/order_map/strategy_select.py
application/order_map/strategy_ranker.py
```

完成：

- Debit/Credit signed cashflow；
- unit economics；
- Conservative Credit BBO；
- Long/Short Premium ManagementPolicy；
- outcome 支持 Credit 与 Buyback Ask。

不得在 Phase A 同时加入 Iron Condor。

### Phase B：Event Proposition

新增最多一个文件：

```text
application/market_features/strategy_propositions.py
```

修改：

```text
macro_event_clock.py
market_features/service.py
order_map/strategy_facts.py
order_map/candidate_factory.py
order_map/delivery.py
```

完成：

- prior close / event pre-close / synthetic / structure propositions；
- 5/10 点 Event Settlement Vertical；
- Debit/Width 与 required probability 展示；
- `RESEARCH_CANDIDATE`。

### Phase C：First-Touch

修改：

```text
physical_followthrough.py
strategy_outcomes.py
strategy_ranker.py
```

完成：

- short strike touch；
- recovered / accepted beyond；
- long wing reached；
- session-weighted probability；
- block bootstrap interval。

### Phase D：Credit Vertical Shadow

修改：

```text
candidate_factory.py
strategy_ranker.py
strategy_select.py
delivery.py
```

完成：

- Call/Put Credit Vertical；
- 仅 Research；
- 单侧路径与定价 Edge；
- 对应 outcome。

### Phase E：Iron Condor Shadow

只有 Phase D 两侧候选稳定后：

- 从两侧 passed Credit Vertical 组合；
- 四腿 BBO；
- combined touch model；
- Research Candidate；
- 不直接 Manual。

### Phase F：Promotion

按策略家族分别生成报告，满足 §21 后再通过新 policy version 升门。

---

## 23. 测试

### 23.1 数学不变量

使用 Hypothesis：

- Debit/Credit payoff 上下界；
- max gain/max loss；
- breakeven；
- signed cashflow PnL；
- Credit buyback 方向；
- Iron Condor 非对称翼；
- fees 增加不能改善净 PnL。

### 23.2 因果不变量

- `available_at <= decision_at`；
- proposition source time 不得晚于 decision；
- P 模型只读取当前交易日前完成会话；
- event result 不得泄漏到 pre-event feature；
- outcome 样本按 opportunity 去重。

### 23.3 Gate 测试

- Event Debit 在 pre-event 可研究，Credit/IC 被阻止；
- shock 阻止 Short Premium；
- 12 Delta 不能绕过 first-touch unavailable；
- 两侧单侧候选任一失败时，不生成 IC；
- stale 或 provider mismatch 时 fail closed；
- PricingEdge 未校准只能 Research。

### 23.4 冻结回放

保留现有 8/5–8/8 案例，并增加：

1. 昨收阈值附近窄 Vertical 被主动枚举；
2. Debit/Width 低于 0.5，但 P 下界不足 → Research；
3. 横盘但 Credit 太低 → NoTrade；
4. 两侧 12 Delta 但 touch 风险不可用 → 不生成 IC；
5. 收敛 + Rich + 两侧通过 → IC Research Candidate。

---

## 24. 复杂度预算

整个 v4：

| 项 | 上限 |
|---|---:|
| 新生产文件 | 1 |
| 新研究脚本 | 0，优先扩展现有 backfill/replay |
| 新依赖 | 0 |
| 新 service/timer/queue | 0 |
| 新数据库/表 | 0 |
| 新 Rust contract | 0 |
| 新配置键 | 0，策略阈值继续版本化代码常量 |
| 自动下单变化 | 0 |
| 账户读取 | 0 |

每个 Phase 独立 PR、独立验收，不允许一次性把全部策略族塞进生产。

---

## 25. 删除与迁移

实现时同步删除或替换：

- Debit-only 的 `NET_DEBIT_LIMIT` 硬编码；
- outcomes 中只接受 entry debit 的私有路径；
- 对 Credit 错用 long-premium ManagementPolicy 的任何临时代码；
- 全局 `macro entry_allowed` 对所有策略一票否决的语义；
- 使用 Delta 作为概率展示的旧文案。

兼容字段最多保留一个发布周期，并必须写明删除 commit。

---

## 26. 设计验收标准

v4 设计通过后，系统最终应能对以下问题给出不同答案：

### 26.1 好命题、坏赔率

```text
今天可能收涨，但 5 点价差 Debit 已到 3.20；
市场要求概率过高 → NO_TRADE。
```

### 26.2 好赔率、证据不足

```text
Debit/Width 48%，但现实概率下界仍低于保本概率；
→ RESEARCH_CANDIDATE。
```

### 26.3 横盘，但卖方不划算

```text
Path=CONVERGENCE，Credit 过低、费用高、MaxLoss/MaxGain 差；
→ NO_TRADE。
```

### 26.4 两侧都合格

```text
CONVERGENCE + Short-Premium Rich + 两侧 touch 风险可接受；
→ Iron Condor RESEARCH_CANDIDATE。
```

### 26.5 趋势确认但已经追价

```text
Path=TREND，方向正确，但 Debit/Width 和目标空间失败；
→ NO_TRADE，reason=direction_valid_but_entry_too_late。
```

---

## 27. 需要用户批准的事项

批准本文不等于一次性批准全部实现。

首先需要确认：

1. 策略引擎保持完全账户无关；
2. 新增 `RESEARCH_CANDIDATE`；
3. Event Settlement Vertical 先行，Credit/IC 后置；
4. Credit Vertical 与 Iron Condor 初期仅 Shadow/Research；
5. 新增最多一个生产文件；
6. `strategy_decision.v4` 仍为 Python-only；
7. 自动下单继续禁止。

批准后，按 Phase A → B → C → D → E 的顺序逐阶段实施。