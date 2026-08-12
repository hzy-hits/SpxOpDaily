# SPX Spark 策略信号引擎 v4：命题、赔率、波动率优势与账户风险闭环

状态：**设计草案，等待批准；本提交只新增文档，不授权实现或部署。**  
适用仓库：`hzy-hits/SpxOpDaily`  
目标路径：`docs/strategy-signal-engine-v4.md`  
基线提交：`e279ba6029ccba2dbbe4d2b98ceb0f688f43b487`  
自动下单：**继续禁止**  
真实账户读取：**默认禁止；Paper 数据不得冒充真实账户风险**

本文是 `docs/strategy-signal-engine-v2.md`、`docs/strategy-signal-engine-v3.md` 和
`docs/strategy-signal-engine-v3-p1p2-design.md` 之上的增量设计。除本文明确提出并在后续获得批准的修订外，既有合同继续有效。

本文特别覆盖四个此前未完整拥有的职责：

1. 将宏观或盘面观点转换为可证伪的**结算命题**；
2. 将候选结构转换为**市场赔率、现实概率和保守净期望**；
3. 增加定义风险的**单侧信用价差与 Iron Condor**，但不允许裸卖；
4. 在最终人工候选前增加**账户风险与重复因子暴露 Gate**。

---

## 0. Change Brief

### 0.1 用户可见目标

让系统不再只回答“趋势还是震荡、买 Call 还是 Put”，而能够回答：

- 当前交易命题到底是什么；
- 市场按什么概率和赔率为该命题定价；
- 我们的现实概率估计是否高于市场要求；
- 这笔交易对路径、触碰、跳跃和结算位置的依赖是什么；
- 即使市场候选合格，当前账户是否仍有资格承担该风险；
- 该候选是已验证、仅有点估计优势，还是纯粹研究想法。

典型输出应从：

```text
CALL_DEBIT_VERTICAL · 看涨
```

升级为：

```text
命题：到期结算高于某一阈值
结构：5 点 Call Debit Vertical
可执行 Debit/Width：48.0%
手续费后近似保本概率：50.2%
现实概率：未校准 / 点估计 / 保守下界
账户风险：0.50% risk capital
重复事件因子：无 / 已存在
结论：MANUAL_CANDIDATE / RESEARCH_CANDIDATE / NO_TRADE
```

### 0.2 现有 owner 与相关文件

继续复用现有 owner，不新建第二套策略系统：

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
src/spx_spark/ibkr/position_watcher.py
src/spx_spark/infrastructure/operational_db.py
```

### 0.3 可复用能力

- 已有 SPX/ES 统一坐标与 GTH parity/futures fallback；
- 已有 RTH 路径状态、VWAP、Opening Range、Breadth、Shock；
- 已有 SPXW wide chain、exact-leg fresh BBO 与跨腿时间偏差检查；
- 已有 Q/P advisory、RN density、同钟点物理分布 bootstrap；
- 已有候选 factory、逐候选 hard gates、near-miss、rejection funnel；
- 已有多 horizon outcome、结构失效、删失与 shadow candidate 标记；
- 已有 SPXW position snapshot 与事件状态，但当前不含真实账户 NLV；
- 已有 Python-only `strategy_decision` 唯一人工候选权限出口。

### 0.4 新依赖

**无。** 第一版只使用 stdlib、NumPy、SciPy、scikit-learn 和现有项目依赖。

### 0.5 本设计阶段删除项

无代码删除。后续实现每个阶段必须同时删除被替代的 Debit-only 分支或兼容字段；兼容双写最多保留一个发布周期。

### 0.6 持久化、进程与通知影响

本设计要求：

- 不新增 service、timer、queue、数据库、表或 Rust consumer；
- 不改变 `automatic_ordering=false`；
- 新研究数据继续落现有 `features/` / `research/` 数据湖；
- 人类可见候选仍只能来自 `payload["strategy_decision"]`；
- 新增的 `RESEARCH_CANDIDATE` 也必须经过同一出口，不允许旁路绿色卡片。

### 0.7 最小端到端验收路径

```text
宏观事件时钟
  → 结算命题枚举
  → SPXW 窄 Vertical 可执行 BBO
  → 市场赔率与现实概率对照
  → 账户风险 Gate
  → strategy_decision
  → 人工候选卡 / NO_TRADE
  → shadow outcome / actual-fill 离线对账
```

---

## 1. 问题定义

当前系统已经能较严格地识别路径、结构和报价，但仍存在五个决策缺口。

### 1.1 观点没有被转换为明确命题

“CPI 偏低”“今天可能收涨”“市场可能回到前收上方”目前只能成为自然语言背景。系统不能自动转换为：

```text
H: SPX settlement > L
```

也不能自动扫描覆盖该阈值的窄幅 Vertical，导致一些交易只有在外部交易员给出具体 strike 后才显得“明显”。

### 1.2 结构价格没有被直观转换为赔率

对于窄 Call Spread：

```text
long K1 Call
short K2 Call
width = K2 - K1
net debit = D
```

`D / width` 近似是区间 `[K1, K2]` 上风险中性生存概率的平均值；价差越窄，越接近 midpoint 附近的数字期权价格。它不是精确现实胜率，但应当成为候选卡的第一层赔率信息。

当前系统虽有 `debit_fraction_of_width`，但主要把它用于 late-chase Gate，没有把它作为“市场要求什么概率才保本”的核心展示和命题扫描入口。

### 1.3 路径状态与波动率相对价值尚未正交

`TREND`、`BALANCED`、`PIN_STABLE` 回答市场正在怎么走；它们不回答期权是否昂贵。缺少独立的：

```text
VolEdge = RICH | FAIR | CHEAP | UNAVAILABLE
```

因此当前系统仍偏向路径/方向系统，而不是完整的波动率相对价值系统。

### 1.4 卖方策略缺少底层会计和路径模型

现有报价、ManagementPolicy、outcomes 与人工订单语义均以净 Debit 为中心。直接加入 Iron Condor 会产生以下错误：

- 开仓 Credit 与平仓 Buyback Ask 混用；
- 长 Premium 的 +50% arm / -50% stop 被错误套用到短 Premium；
- 只看到期 ITM，而忽略 short strike first-touch；
- 用 Delta 代替真实触碰和尾部概率；
- 不能正确计算手续费后的最大风险与平均损失。

### 1.5 市场候选与账户是否应交易仍未分离

系统当前主要判断候选本身是否合格，但不知道：

- 用户批准的 risk capital；
- 当前开放 SPXW 最大损失；
- 日、周、月风险锁；
- 是否已经有同一宏观因子的仓位；
- 真实账户快照是否缺失、陈旧或来自 Paper。

市场候选好，不代表账户应继续加仓。

---

## 2. 目标与非目标

### 2.1 目标

1. 建立版本化的 `StrategyProposition`，将交易观点转换成终值、区间或路径命题。
2. 支持 Debit 与 Credit 的统一有符号现金流、保守 BBO、Payoff 和 Outcome。
3. 建立事件阶段 × 策略家族的权限矩阵，取代单一 `entry_allowed`。
4. 增加 `VolEdgeAssessment`，分离路径状态、Q 定价和 P 风险。
5. 建立 first-touch、touch-recovery、touch-continuation 和 long-wing reach 标签。
6. 先增加单侧定义风险 Credit Vertical，再允许 Iron Condor 组合。
7. 增加账户风险 Gate，明确禁止使用显示 Buying Power 作为风险资本。
8. 增加 `RESEARCH_CANDIDATE`，让未验证但结构完整的机会可见，同时不伪装成已验证 Edge。
9. 将真实成交、手续费、滑点和人工修改导入研究闭环。
10. 所有输出继续可因果回放、可解释、可审计。

### 2.2 非目标

第一版明确不做：

- 裸 Call、裸 Put、未定义风险 Ratio；
- Calendar、Diagonal、Jade Lizard、Broken-Wing Butterfly；
- 自动下单、自动滚仓、自动加仓；
- 将 Twitter、LLM 或自然语言共识直接转换为概率；
- 在本仓库生成 SPY、TLT 等非 SPXW 执行合约；
- 用 Paper 持仓或 Paper NLV 代表用户真实账户；
- 用一个月样本宣称稳定 Alpha；
- 为本设计新增服务、队列、数据库或 Rust 合约；
- 用 Delta 直接称为“胜率”。

---

## 3. 权威链与核心架构

### 3.1 唯一权限链

```text
Provider-normalized market data
  → MarketFactPack
  → PathRegime
  → EventPermission
  → StrategyPropositionSet
  → VolEdgeAssessment
  → Candidate Factory
  → Structure-specific Hard Gates
  → Account Risk Gate
  → Ranker
  → strategy_decision
  → MANUAL_CANDIDATE / RESEARCH_CANDIDATE / NO_TRADE
```

任何 Wall、GEX、Delta、LLM、宏观叙事或单一概率均不得旁路上述链路。

### 3.2 五个正交状态

```yaml
path_regime:
  state: TREND | CONVERGENCE | PIN | SHOCK | TRANSITION | UNCERTAIN
  direction: UP | DOWN | NONE

vol_edge:
  state: RICH | FAIR | CHEAP | UNAVAILABLE
  measure: MANAGEMENT_PNL | TERMINAL_RANGE | FIRST_TOUCH

macro_permission:
  phase: NORMAL | PREPOSITION | EVENT_FREEZE | POST_EVENT_DISCOVERY |
         POST_EVENT_TREND | POST_EVENT_CONVERGENCE

execution_state:
  status: READY | WIDE | STALE | INCOMPLETE | UNAVAILABLE

account_risk_state:
  status: ACTIVE | REDUCED | DAILY_LOCK | WEEKLY_LOCK | MONTHLY_LOCK |
          SNAPSHOT_UNAVAILABLE
```

这五层不得相互替代：

- `CONVERGENCE` 不自动等于 Vol Rich；
- `VIX 高` 不自动等于可卖；
- `CPI 偏多` 不自动等于事件候选；
- 候选 Edge 为正不自动等于账户有资格做；
- 账户风险合格不代表市场候选有优势。

---

## 4. Strategy Proposition 合同

### 4.1 数据结构

建议新增纯 Python 结构 `strategy_proposition.v1`：

```yaml
schema_version: strategy_proposition.v1
proposition_id: proposition:<stable-hash>
source_kind: PRIOR_CLOSE | EVENT_PRE_CLOSE | OVERNIGHT_SYNTHETIC |
             Q_MEDIAN | Q_MODE | ZERO_GAMMA | FLIP_ZONE |
             PUT_WALL | CALL_WALL | EXPECTED_MOVE | CONFIRMED_LEVEL
market_session: GTH | RTH
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
  source_fields: []
  source_times: []
  coordinate_kind: OFFICIAL_SPX | CHAIN_IMPLIED_SPX | ES_BASIS_ADJUSTED
research_status: UNVALIDATED | CALIBRATING | VALIDATED
```

### 4.2 命题不是策略

命题只说明要预测的事件。例如：

```text
SPX 结算高于前一日收盘
```

它不规定：

- 使用单 Call 还是 Vertical；
- 使用 5 点还是 10 点宽；
- 支付多少 Debit；
- 账户是否可以承担风险。

同一命题可以映射到多个候选，再由经济学和风险 Gate 排序。

### 4.3 第一版命题来源

第一版只枚举以下 anchor，避免搜索空间失控：

1. 前一 RTH 官方收盘；
2. 事件前最后一个 RTH 官方收盘；
3. GTH 当前 qualified synthetic SPX；
4. 当前到期 Q median；
5. 当前到期 Q mode；
6. Zero Gamma；
7. Flip Zone 上下边界；
8. Put Wall / Call Wall；
9. 已确认 Level Decision；
10. Expected Move 上下界。

相距小于一个最小 strike interval 的 anchor 必须去重，并保留全部 provenance。

### 4.4 Event Settlement Vertical

事件型命题继续使用现有结构类型：

```text
CALL_DEBIT_VERTICAL
PUT_DEBIT_VERTICAL
```

但必须增加：

```text
strategy_family = EVENT_SETTLEMENT_DEBIT
setup_kind = EVENT_SETTLEMENT_THRESHOLD
management_policy = event_settlement.v1
proposition_id = ...
```

这样不新增重复 Payoff 类型，但不会把事件结算下注与盘中趋势价差混在同一统计样本。

### 4.5 窄 Vertical 枚举

第一版固定 5 点宽，仅在 proposition anchor 附近枚举有限数量的 Vertical：

```text
anchor ± 10 points
width = 5
same expiry that spans the event
```

候选必须报告：

- anchor 与 spread midpoint 距离；
- exact breakeven；
- executable Debit；
- Debit/Width；
- 手续费后的 required probability proxy；
- 完整 P-payoff EV，而不是只用二元近似。

不得因为目标价差未出现而扩大到 5 组、移动到更远阈值或临时改变宽度。

---

## 5. 市场赔率的精确定义

### 5.1 Narrow Call Spread

对 Call Spread：

\[
\Pi(S_T)=(S_T-K_1)^+-(S_T-K_2)^+ - D,
\qquad W=K_2-K_1
\]

无套利市场价格满足：

\[
D \approx e^{-rT}\int_{K_1}^{K_2}Q(S_T>K)\,dK
\]

因此：

\[
q_{avg}=\frac{D}{e^{-rT}W}
\]

是 `[K1,K2]` 上风险中性生存概率的平均值。只有当价差足够窄时，它才近似 midpoint 附近的数字概率。

候选卡可以显示：

```text
market_probability_proxy = debit / discounted_width
```

但必须附注：

```text
average risk-neutral probability across strikes; not physical win rate
```

### 5.2 近似保本概率

若把窄 Vertical 简化为宽度为 `W` 的二元结果，手续费和滑点折合为 `C` 点，则：

\[
p_{BE,proxy}=\frac{D+C}{W}
\]

该值仅用于解释赔率。最终授权必须使用完整 Payoff：

\[
EV_P=E^P[(S_T-K_1)^+-(S_T-K_2)^+] - D - C
\]

### 5.3 禁止的表述

以下表述禁止出现在人工候选卡：

- “2.40/5，所以胜率 48%”；
- “15 Delta，所以只有 15% 风险”；
- “连续两日下跌，所以今天高胜率收涨”；
- “Twitter 共识看多，所以模型概率提高”。

允许表述：

```text
市场赔率代理约 48%；现实概率尚未校准。
```

或：

```text
模型点估计高于赔率要求，但保守下界仍低于保本门槛，Edge 未证明。
```

---

## 6. 宏观事件权限矩阵

### 6.1 当前问题

现有 `macro_event_clock` 将 `pre_event` 直接映射为 `entry_allowed=false`，适合普通策略防错，但会把合法的事件结算研究候选一并关闭。

v4 不删除安全门，而是将单一布尔值升级为按策略家族的权限矩阵。

### 6.2 事件阶段

```text
NORMAL
PREPOSITION
EVENT_FREEZE
POST_EVENT_DISCOVERY
POST_EVENT_TREND
POST_EVENT_CONVERGENCE
```

建议由事件时钟继续提供 `minutes_to_release`，策略层按冻结规则映射：

- `PREPOSITION`：事件前预设窗口；
- `EVENT_FREEZE`：发布前最后一段时间，不建立新仓；
- `POST_EVENT_DISCOVERY`：发布后价格和收益率仍在重新定价；
- `POST_EVENT_TREND`：方向与跨市场反应确认；
- `POST_EVENT_CONVERGENCE`：冲击后波动和路径均稳定。

具体分钟阈值必须随 `macro_permission_policy_version` 冻结并回放，不允许运行时临时修改。

### 6.3 权限矩阵

| 阶段 | Event Settlement Debit | Intraday Directional Debit | Butterfly | Credit Vertical | Iron Condor |
|---|---:|---:|---:|---:|---:|
| NORMAL | 不适用 | 允许 | 允许 | 研究/允许 | 研究/允许 |
| PREPOSITION | 研究或小风险人工 | 禁止 | 禁止 | 禁止 | 禁止 |
| EVENT_FREEZE | 禁止新仓 | 禁止 | 禁止 | 禁止 | 禁止 |
| POST_EVENT_DISCOVERY | 管理已有仓 | 等待 | 禁止 | 禁止 | 禁止 |
| POST_EVENT_TREND | 不再追原命题 | 允许 | 通常禁止 | 单侧研究 | 禁止 |
| POST_EVENT_CONVERGENCE | 不适用 | 视路径 | 可评估 | 可评估 | 可评估 |

第一版仅对 CPI 做冻结验收；FOMC、非农、Core PCE 在相同框架下逐类增加，不允许将所有事件混为一个样本桶。

---

## 7. Debit/Credit 统一现金流与 BBO

### 7.1 Canonical leg quantity

统一规定：

```text
quantity > 0  = long / buy-to-open
quantity < 0  = short / sell-to-open
```

### 7.2 开仓保守现金流

对每条腿：

- Long 按 Ask 买入；
- Short 按 Bid 卖出。

定义开仓现金流，收到现金为正、支付现金为负：

\[
CF_{open}=-\sum_i q_i\cdot
\begin{cases}
Ask_i,&q_i>0\\
Bid_i,&q_i<0
\end{cases}
\]

因此：

```text
CF_open < 0 → DEBIT
CF_open > 0 → CREDIT
```

### 7.3 平仓保守现金流

平仓使用反向数量 `-q_i`，同样按主动成交侧计算：

\[
CF_{close}=-\sum_i (-q_i)\cdot
\begin{cases}
Ask_i,&-q_i>0\\
Bid_i,&-q_i<0
\end{cases}
\]

保守毛 P&L：

\[
PnL_{gross}=CF_{open}+CF_{close}
\]

净 P&L：

\[
PnL_{net}=PnL_{gross}-Fees-Slippage
\]

该有符号定义必须成为唯一 owner，替代当前 `_entry_debit` / `exit_combo_bid` 的 Debit-only 语义。

### 7.4 新 Quote/Economics 字段

```yaml
entry_cashflow:
  kind: DEBIT | CREDIT
  signed_points: float
  price_points: abs(signed_points)
  executable_basis: conservative_leg_bbo

flatten_quote:
  signed_cashflow_points: float
  buyback_cost_points: float
  exit_credit_points: float

economics:
  width_points: float | null
  put_width_points: float | null
  call_width_points: float | null
  max_gain_points: float
  max_loss_points: float
  breakevens: [float]
  defined_risk: true
```

### 7.5 不变量

所有结构必须满足：

```text
defined_risk == true
max_loss_points > 0
max_gain_points > 0
entry quote uses one provider and bounded source skew
available_at <= decision_at
```

数学测试至少覆盖：

- leg 顺序不影响现金流；
- Call/Put 对称结构 Payoff 正确；
- Credit Vertical 最大亏损 = width - credit；
- Iron Condor 最大亏损 = max(put_width, call_width) - credit；
- 任意结算价 Payoff 位于 `[-max_loss, max_gain]`；
- 手续费只能降低净收益，不能提高。

---

## 8. ManagementPolicy 分离

### 8.1 Long Premium Intraday

继续沿用当前逻辑，但升级版本并明确适用范围：

```yaml
policy_version: management_policy.long_premium.v2
entry_basis: conservative_open_debit
valuation_basis: conservative_flatten_credit
profit_arm_return_on_debit: 0.50
trail_after_arm_fraction: 0.75
premium_stop_fraction: 0.50
time_stop_minutes: 20
hard_exit_et: "15:45"
```

### 8.2 Event Settlement Debit

事件结算交易不能假装普通止损能跨越数据跳空，因此：

```yaml
policy_version: management_policy.event_settlement.v1
holding_objective: SETTLEMENT
pre_event_stop_assumption: NONE
max_loss_basis: FULL_DEBIT_PLUS_FEES
post_event_management: RESEARCH_ONLY
hard_exit: settlement_or_manual_close
```

Outcome 必须同时保存：

- settlement P&L；
- 数据后 1/5/15/30 分钟可执行 P&L；
- 最大有利/不利变动；
- IV crush 后的实际价差表现。

候选卡必须明确：

```text
普通 Stop 不能保证跨事件控制损失；硬风险为全部 Debit。
```

### 8.3 Short Premium Intraday

```yaml
policy_version: management_policy.short_premium.v1
entry_basis: conservative_open_credit
valuation_basis: conservative_buyback_ask
profit_take_buyback_fraction: 0.50
premium_stop_buyback_multiple: 2.00
hard_exit_et: "15:15"
hold_to_settlement: false
roll_allowed: false
average_down_allowed: false
```

路径退出优先于价格止损：

- short strike 外出现有效接受；
- touch 后 Breadth、VWAP 和 Shock 同方向恶化；
- long wing 被触及；
- 报价或数据质量失效。

上述阈值在 first-touch 数据校准前只用于 shadow，不直接宣称最优。

---

## 9. VolEdgeAssessment

### 9.1 目标

回答：

> 当前到期的隐含剩余风险，相对于条件化的未来实际终值和路径风险，是昂贵、合理还是便宜？

### 9.2 合同

```yaml
schema_version: vol_edge_assessment.v1
expiry: YYYYMMDD
as_of: iso8601
path_state: TREND | CONVERGENCE | PIN | SHOCK | TRANSITION | UNCERTAIN
state: RICH | FAIR | CHEAP | UNAVAILABLE
q_method: string
p_method: string
implied_remaining_move_points: float | null
physical_terminal_move_p50: float | null
physical_terminal_move_p90: float | null
terminal_between_probability: float | null
short_put_touch_probability: float | null
short_call_touch_probability: float | null
either_touch_probability: float | null
touch_recovery_probability: float | null
long_wing_probability: float | null
management_ev_points: float | null
management_ev_interval_low: float | null
n_raw: int
n_effective: float
session_count: int
quality: READY | DEGRADED | UNAVAILABLE
reason_codes: []
research_status: UNVALIDATED | CALIBRATING | VALIDATED
```

### 9.3 Q 层

第一阶段可以继续使用现有 surface/density 作为 advisory，但必须清楚标注：

- synthetic mid + clipping 不是最终可执行 Q；
- 每个 ultra-short expiry 独立处理；
- event-spanning 与 non-event expiry 不混合；
- Debit/Width 只是 narrow vertical 的局部 Q proxy；
- Delta 仅作 strike locator。

Credit/IC 升为人工候选前，需要 bid-ask 约束、静态无套利的 surface 诊断达到 READY。

### 9.4 P 层

第一版使用透明的 session-clustered bootstrap / nearest-neighbour：

- 每个交易日总权重不超过 1；
- 同日分钟样本不得冒充独立 session；
- 事件日按 CPI/FOMC/NFP 分桶；
- 特征至少包括：时间、OR、效率、VWAP crosses、Breadth、VIX1D 变化、Shock、event phase；
- 输出 Beta/Bootstrap 置信区间与 `n_effective`；
- 当前 session 永不进入训练集。

### 9.5 RICH 判定

在未验证阶段，`RICH` 只能是研究标签。候选升门需要：

```text
management_ev_interval_low > 0
AND sufficient n_effective
AND cost-doubled stress remains non-negative
AND tail stress remains within risk policy
```

不得仅使用：

```text
VIX > 某值
ATM IV 高
10–15 Delta 看起来很远
```

---

## 10. First-Touch 与 Touch 后状态转移

### 10.1 为什么必须建模

到期 ITM 概率不等于盘中触碰概率。0DTE short option 在到期 OTM 前也可能经历：

```text
5Δ → 15Δ → 30Δ → 50Δ
```

如果真实管理会在 touch、acceptance 或 premium multiple 时退出，只看结算价会严重高估卖方收益。

### 10.2 标签

每个卖方候选必须标记：

```yaml
short_put_touched: bool
short_call_touched: bool
either_short_touched: bool
both_shorts_touched: bool
first_touch_side: PUT | CALL | NONE
first_touch_at: iso8601 | null
touch_recovered: bool | null
touch_accepted: bool | null
long_put_wing_reached: bool
long_call_wing_reached: bool
max_buyback_ask_points: float | null
settlement_inside_shorts: bool
settlement_inside_breakevens: bool
```

### 10.3 Touch Recovery

第一版定义建议：

```text
TOUCH_RECOVERED:
short strike 被触及后，价格重新进入 short strikes 内部，
且随后 10 分钟没有再次在同侧形成 5 分钟接受。

TOUCH_ACCEPTED:
至少两个连续 5 分钟收盘位于 short strike 外，
且 Breadth 或 VWAP 方向一致。
```

这些定义是待校准初始值，必须版本化，不允许在看到结果后随意更改。

### 10.4 Path-aware Outcome

卖方 Outcome 必须同时给出：

- 持有到结算 P&L；
- 固定 50% take-profit / 2× stop P&L；
- structural acceptance exit P&L；
- touch 即退出 P&L；
- 无管理 max-loss P&L。

这样才能判断“touch 就砍”“接受后砍”或“持有到结算”哪一种是真实可行策略，而不是用事后最优规则。

---

## 11. Credit Vertical 与 Iron Condor

### 11.1 扩展候选空间

后续获得批准后，新增：

```text
PUT_CREDIT_VERTICAL
CALL_CREDIT_VERTICAL
IRON_CONDOR
```

继续禁止裸卖。

### 11.2 先单侧，后双侧

生成顺序固定：

```text
Put-side hypothesis
  → Put Credit Vertical candidate

Call-side hypothesis
  → Call Credit Vertical candidate

两侧均独立通过
  → Combined Iron Condor candidate
```

禁止直接用“Put 12Δ + Call 12Δ”生成 Iron Condor。

### 11.3 单侧 Credit Gate

单侧候选至少要求：

- `VolEdge` 对该侧风险不是 CHEAP；
- short strike 位于结构失效位之外；
- first-touch / continuation 模型可用；
- executable Credit 足以覆盖成本和最大损失；
- defined-risk long wing 完整；
- Quote READY；
- Account Risk Gate 通过。

### 11.4 Iron Condor Gate

组合候选还要求：

- 两侧 expiry 相同；
- 两侧同 provider、source skew 合格；
- short strikes 顺序正确；
- 两侧宽度在第一版固定 5 点；
- combined Credit 达到风险预算要求；
- 任一侧 first-touch 过高时不生成；
- `SHOCK`、`POST_EVENT_DISCOVERY`、`TREND` 状态禁用；
- 不与方向性 SPXW 仓位并存；
- 不持有到结算。

### 11.5 Delta 的角色

Delta 只用于在满足结构和概率条件后定位候选 strike：

```text
target_abs_delta = 0.125
allowed = [0.10, 0.15]
```

系统输出必须显示：

```text
Delta locator: 12.5
Terminal probability: separately estimated
First-touch probability: separately estimated
```

---

## 12. Account Risk Gate

### 12.1 风险资本

系统禁止使用：

- Buying Power；
- Excess Liquidity；
- Paper NLV；
- 未经来源验证的账户截图；

作为实盘风险资本。

第一版采用用户明确批准的：

```text
risk_capital_usd
```

它存放在本机 gitignored 配置或受保护的 read-only snapshot 中，不提交仓库。

如同时存在可信 NLV，则：

\[
EffectiveRiskCapital=\min(risk\_capital\_usd, NLV)
\]

### 12.2 数据来源模式

```yaml
source_mode: STATIC_APPROVED_CAPITAL | READ_ONLY_ACCOUNT_IMPORT | PAPER_ONLY
```

- `STATIC_APPROVED_CAPITAL`：用户明确设置；
- `READ_ONLY_ACCOUNT_IMPORT`：离线导入的真实账户摘要，必须带时间和来源；
- `PAPER_ONLY`：不能授权真实账户风险，只能研究。

### 12.3 风险状态

```yaml
schema_version: account_risk_state.v1
as_of: iso8601
source_mode: ...
risk_capital_usd: float | null
nlv_usd: float | null
open_spxw_max_loss_usd: float
realized_pnl_today_usd: float | null
weekly_strategy_pnl_usd: float | null
monthly_strategy_drawdown_usd: float | null
open_factor_risk: {}
open_strategy_count: int
status: ACTIVE | REDUCED | DAILY_LOCK | WEEKLY_LOCK | MONTHLY_LOCK |
        SNAPSHOT_UNAVAILABLE
reason_codes: []
```

### 12.4 Bootstrap 风险预算

以下为建议初始冻结值，不代表已有 Edge：

```yaml
policy_version: account_risk_policy.bootstrap.v1
max_long_premium_loss_fraction: 0.005
max_event_debit_loss_fraction: 0.005
max_short_premium_loss_fraction: 0.0075
max_total_open_spxw_loss_fraction: 0.0075
daily_loss_lock_fraction: 0.010
weekly_loss_lock_fraction: 0.020
monthly_loss_lock_fraction: 0.040
max_open_spxw_strategies: 1
max_same_factor_loss_fraction: 0.010
```

单笔风险使用结构硬最大损失，不使用“计划止损”替代。

### 12.5 因子标签

每个候选必须带：

```yaml
factor_tags:
  - SPX_UP | SPX_DOWN | RANGE
  - VOL_LONG | VOL_SHORT
  - RATES_DOWN | RATES_UP
  - CPI_COOL | CPI_HOT | FOMC_DOVISH | FOMC_HAWKISH
  - PIN_CENTER:<level>
```

系统不执行 SPY/TLT，但可识别多个仓位是否在重复押注同一宏观因子。

### 12.6 Fail-closed

以下情况不得成为 `MANUAL_CANDIDATE`：

- risk capital 不可用；
- snapshot 超时；
- source_mode=PAPER_ONLY；
- 新仓后超过单笔、总开放或同因子风险；
- 已进入日/周/月锁；
- 当前已有一个 SPXW 策略且 policy 限制为一笔；
- 无法正确计算候选最大损失。

可降级为 `RESEARCH_CANDIDATE`，但必须清晰显示账户风险未知或不合格。

---

## 13. 决策等级与 Edge 状态

### 13.1 决策等级

建议 `strategy_decision.v3` 支持：

```text
MANUAL_CANDIDATE
RESEARCH_CANDIDATE
NO_TRADE
```

含义：

- `MANUAL_CANDIDATE`：确定性 Gate、账户 Gate 和当前策略家族的 promotion policy 均通过；仍需人工决定；
- `RESEARCH_CANDIDATE`：结构完整、报价可执行，但 Edge 或账户授权尚未达到人工候选标准；
- `NO_TRADE`：没有完整候选，或存在明确硬拒绝。

### 13.2 Edge 状态

```text
PRICED_ONLY
MODEL_UNAVAILABLE
POINT_ESTIMATE_POSITIVE
CONSERVATIVE_EDGE_POSITIVE
VALIDATED_EDGE
NO_EDGE
```

规则示例：

```text
q 可用，p 不可用 → PRICED_ONLY
p_hat > required_p，但 p_low <= required_p → POINT_ESTIMATE_POSITIVE
p_low > required_p + safety_margin → CONSERVATIVE_EDGE_POSITIVE
满足 OOS promotion → VALIDATED_EDGE
```

“高胜率”只能在经过校准、样本外验证并给出置信区间时使用；默认卡片不使用该词。

---

## 14. 候选排序

### 14.1 Hard Gate 先于评分

排序不得救活以下候选：

- stale/incomplete quote；
- 未定义风险；
- event permission 不允许；
- account risk 不允许；
- unsupported strategy；
- expiry 不跨目标事件；
- source timestamp 因果违规；
- GTH 使用 stale cash SPX；
- 卖方缺 first-touch/VolEdge required capability。

### 14.2 评分

研究期建议：

\[
Score=\frac{EV_{net}}{MaxLoss}
-0.50\frac{ES_{10}}{MaxLoss}
-0.25 LiquidityPenalty
-0.25 ModelUncertainty
-0.25 AccountConcentrationPenalty
\]

但未校准的 Score 只能排序，不得单独授权。

### 14.3 NoTrade

`NO_TRADE` 继续是正式候选：

\[
Score_{NO\_TRADE}=0
\]

只有非交易候选在保守、扣成本后优于 0，且全部硬门通过，才可晋级。

---

## 15. Outcome 与真实执行闭环

### 15.1 Shadow Outcome 不等于实际成交

当前 multi-horizon conservative ask/bid 标记继续保留，但必须继续标注：

```text
label_basis = decision_quote_shadow_not_fill
```

不得用它宣称真实 Fill Probability 或实盘 P&L。

### 15.2 Actual Execution Import

不新增实时订单服务。通过离线导入 IBKR Activity/Flex/CSV 建立：

```yaml
schema_version: strategy_execution_record.v1
execution_id: string
candidate_id: string | null
opportunity_id: string | null
match_status: EXACT | AMBIGUOUS | UNMATCHED
submitted_at: iso8601 | null
entry_filled_at: iso8601
entry_fill_points: float
exit_filled_at: iso8601 | null
exit_fill_points: float | null
contracts: int
commission_usd: float
realized_pnl_usd: float | null
manual_adjustment: bool
roll_or_add: bool
source_file_hash: string
```

匹配优先级：

1. order reference 中的 candidate_id；
2. exact expiry/rights/strikes/quantities + 时间窗口；
3. 人工确认；
4. ambiguous/unmatched 不进入 Edge 统计。

### 15.3 样本单位

- 一次完整策略仓位算一笔；
- 四条腿不是四笔；
- 同一机会多次报价不是多个独立样本；
- 同一交易日多笔高度相关，bootstrap 按 session block；
- 加仓、滚仓和结构转换单独标记，不与冻结 V1 混合。

---

## 16. Promotion 门槛

### 16.1 总则

100 笔只是一轮筛查，不足以证明微小 Edge。每个策略家族分别验证，不允许买方盈利为卖方扩仓背书。

### 16.2 Directional Debit

建议最低：

- 总样本 ≥50；
- 时间顺序 OOS ≥20；
- 扣费后平均期望 ≥0.12R；
- Full-sample PF ≥1.30；
- OOS PF ≥1.10；
- day-block bootstrap 平均值下界 >0；
- 删除最佳三笔后总 P&L 不为负；
- 双倍成本压力后 PF ≥1.05。

### 16.3 Event Settlement Debit

按事件类型分别：

- 同一 event class 总样本 ≥30；
- OOS ≥12；
- 不将 CPI 与 FOMC 直接合并；
- 赔率 proxy 校准误差可审计；
- p_hat 与 realized frequency 的 Brier/Log Loss 优于市场 proxy；
- 事件 gap 下全部 Debit 风险压力可接受；
- 不依赖单一极端大涨样本。

### 16.4 Credit Vertical

- 总样本 ≥60；
- OOS ≥25；
- 扣费后平均期望 ≥0.08R；
- PF ≥1.20，OOS PF ≥1.10；
- actual win-rate 的 Wilson 下界高于真实盈亏平衡胜率；
- 每 25 笔插入一次 max-loss 后期望不为负；
- 双倍成本后期望不为负；
- first-touch 与 stop execution 偏差受控。

### 16.5 Iron Condor

- 总样本 ≥75；
- OOS ≥30；
- 两侧单独模型均通过；
- `either_touch_probability` 校准通过；
- 99% block-bootstrap max drawdown ≤10R；
- 每 20 笔插入一次 max-loss 后总期望仍不为负；
- 最差 5% 平均损失未明显超出模型；
- 不能只靠高胜率通过。

### 16.6 扩仓

从一组合约升到两组必须同时满足：

- 对应策略家族通过 promotion；
- 至少 60 个交易日无越权加仓/滚仓；
- 最近三个月回撤小于 4%；
- 两组合计最大损失仍不超过 `max_total_open_spxw_loss_fraction`；
- risk capital 上升，而不是 Buying Power 上升；
- 账户同因子暴露仍合格。

---

## 17. 人工候选卡

### 17.1 Event Threshold 示例

```text
Decision · RESEARCH CANDIDATE

命题
SPX 结算高于 prior-close anchor

结构
5-point CALL_DEBIT_VERTICAL
管理
EVENT_SETTLEMENT · 全部 Debit 视为硬风险

市场赔率
Executable debit / width = 48.0%
Fees-adjusted required probability proxy = 50.2%
Q semantics = average risk-neutral survival across strikes

现实概率
p_hat = n/a
p_low = n/a
Edge = PRICED_ONLY · 不称为高胜率

事件
CPI PREPOSITION · expiry spans event
普通方向/卖方策略仍被阻止

账户
Risk fraction = 0.50%
Same-factor risk after trade = 0.50%
Account gate = PASS / FAIL / UNKNOWN

反证
事件结果、收益率与价格反应不支持命题时，不得加仓解释。
```

### 17.2 Iron Condor 示例

```text
Decision · RESEARCH CANDIDATE

路径
CONVERGENCE
VolEdge
RICH(point estimate) · conservative lower bound not promoted

结构
5-point IRON_CONDOR
Short-leg delta locator = 12.5 / 11.8
Delta is not win probability

路径风险
P(either short touched) = ...
P(touch recovered) = ...
P(long wing reached) = ...

经济性
Credit = ...
Max loss = ...
50% buyback target net P&L = ...
2x credit stop stress = ...

账户
Open SPXW strategy count = 0
Risk after trade = ...
```

### 17.3 NO_TRADE 示例

```text
Decision · NO TRADE
Primary blocker: duplicate_event_factor_exposure
Nearest candidate: EVENT_SETTLEMENT CALL VERTICAL
Market candidate: structurally valid
Account candidate: rejected
Reauthorize when: existing event exposure is closed or risk capital increases under policy
```

---

## 18. 实施阶段与文件范围

### V4-0：设计合同

仅本文档。无生产改动。

### V4-1：Signed Economics 与 ManagementPolicy

修改：

```text
analytics/options/strategy_payoff.py
application/order_map/strategy_outcomes.py
application/order_map/strategy_select.py
application/order_map/strategy_ranker.py
```

目标：Debit/Credit 统一现金流、long/event/short policy 分离、Outcome 支持 signed P&L。

### V4-2：Event Proposition Scanner

建议新增一个生产 owner：

```text
application/market_features/strategy_propositions.py
```

真实调用者至少两个：

- `market_features/service.py` 生产 proposition set；
- `candidate_factory.py` 与 Desk/Decision 消费。

修改：

```text
macro_event_clock.py
strategy_facts.py
candidate_factory.py
strategy_ranker.py
strategy_select.py
delivery.py
```

第一版只生成 Event Settlement Debit，Credit 保持禁止。

### V4-3：Account Risk Gate

建议新增：

```text
application/order_map/account_risk.py
```

调用者：

- `strategy_select.py` 最终授权；
- `delivery.py` 展示；
- `strategy_ranker.py` 可附着 concentration penalty，但不得绕过 hard gate。

复用现有 SPXW PositionSnapshot；真实 risk capital 只从显式本地输入或 read-only import 获取。

### V4-4：VolEdge 与 First-Touch

优先扩展现有：

```text
application/market_features/physical_followthrough.py
application/market_features/strategy_distribution_forecast.py
application/order_map/strategy_outcomes.py
```

如单文件职责超出预算，再单独申请新模块，不预先创建抽象。

### V4-5：Credit Vertical Shadow

修改 candidate factory、payoff、ranker、outcomes、delivery。仅输出 `RESEARCH_CANDIDATE` / shadow；不升人工权限。

### V4-6：Iron Condor Shadow

只有两侧单侧候选均通过时组合。仍 research-only，直至 promotion 门槛达成并再次批准。

### V4-7：Actual Execution 与 Promotion

优先扩展现有 `strategy_policy_backfill.py` 或新增一个 research-only importer；不得新增实时订单进程。

---

## 19. 冻结验收案例

### Case A：事件前窄 Vertical 被识别

输入：

- scheduled CPI；
- expiry spans event；
- prior close anchor；
- anchor 附近 5 点 Call Spread；
- conservative executable Debit < width/2。

期望：

- 生成 `EVENT_SETTLEMENT_THRESHOLD` proposition；
- 显示 Debit/Width 和 required probability proxy；
- 不称为高胜率；
- 无 P 或账户数据时为 `RESEARCH_CANDIDATE`，不是静默丢失。

### Case B：事件前普通策略仍被阻止

同一时点：

- Event Settlement Debit 可研究；
- Butterfly、Credit Vertical、Iron Condor 必须拒绝；
- 不允许宏观 pre-event permission 全局打开。

### Case C：价格变便宜但标的同步恶化

若 spread 从 2.60 降到 2.40，同时 synthetic SPX 明显下跌：

- 不得标记为“错价”；
- required probability 降低与 conditional P 同时更新；
- 只有 Edge margin 改善才升级。

### Case D：重复事件暴露

已有同因子事件仓位时，新候选即使市场 Gate 通过，也必须：

```text
NO_TRADE or RESEARCH_CANDIDATE
reason = duplicate_event_factor_exposure
```

### Case E：GTH 坐标

GTH official cash SPX stale 时：

- 使用 qualified chain-implied SPX；
- 再不行使用 qualified ES-basis；
- 不得用 stale close 计算 threshold distance。

### Case F：Credit 会计

对合成 5 点 Credit Vertical：

- Credit 1.50；
- max gain 1.50；
- max loss 3.50；
- 平仓 Ask 0.75 时毛 P&L 0.75；
- 双边费用后净 P&L 更低。

### Case G：Touch 与 settlement 分离

路径先穿 short strike 后恢复，最终 OTM：

- settlement label 为 win；
- `short_touched=true`；
- touch-exit policy 可以为 loss；
- 不允许只展示 settlement win。

### Case H：Iron Condor 不由 Delta 单独生成

两侧 12 Delta，但 VolEdge 不可用或账户快照缺失：

- 不生成 Manual Candidate；
- 最多 research/shadow；
- why-not 明确给出缺失能力。

### Case I：现有 v3 回归

V4-1/V4-2 合入后，非事件普通 RTH 的现有 Debit Vertical / Butterfly 冻结案例在无新 proposition 时必须保持相同决策，或给出逐字段书面差异说明。

---

## 20. 测试策略

### 20.1 数学纯函数

使用 Hypothesis 覆盖：

- signed cashflow；
- Debit/Credit 对称；
- payoff 上下界；
- first-touch 时间单调；
- account risk 百分比和最大损失；
- probability proxy 不越过 `[0,1]` 的输入质量 Gate；
- `available_at <= decision_at`。

### 20.2 决策矩阵

参数化测试：

```text
macro phase × strategy family × path state × vol edge × account state
```

验证权限和拒绝原因，不测试私有 helper 调用顺序。

### 20.3 Frozen Replay

继续保留既有 2026-08-05/06/07/08；新增：

- 一个 CPI preposition fixture；
- 一个 post-event trend fixture；
- 一个 convergence + touch/recovery fixture；
- 一个 account duplicate-factor fixture。

### 20.4 成本压力

所有候选回放同时报告：

- conservative BBO；
- 1 tick adverse per leg；
- 2× commission/slippage；
- no-fill；
- delayed fill adverse selection。

---

## 21. 复杂度预算

本设计分两个批准批次，避免一次性扩张。

### Batch A：Event Debit + Account Risk

| 维度 | 上限 |
|---|---:|
| 新增生产文件 | 2 |
| 修改生产文件 | ≤10 |
| 净生产 LOC | ≤900 |
| 测试 LOC | ≤700 |
| 新依赖 | 0 |
| 新 service/timer/queue | 0 |
| 新数据库/表 | 0 |
| Rust / golden contract | 0 |
| local-only config 输入 | ≤2 |

### Batch B：VolEdge + Credit + Iron Condor

| 维度 | 上限 |
|---|---:|
| 新增生产文件 | 0，除非另行批准 |
| 新增 research-only 文件 | ≤1 |
| 净生产 LOC | ≤900 |
| 测试 LOC | ≤800 |
| 新依赖 | 0 |
| 新 process/store | 0 |

若任何阶段需要增加进程、数据库、Rust 消费路径或真实订单权限，必须停止并重新批准。

---

## 22. 迁移与删除计划

后续实现必须在同一阶段处理：

1. `entry_combo_ask` / `_entry_debit` 迁移到 signed cashflow；
2. `NET_DEBIT_LIMIT` 硬编码改为按 `entry_cashflow.kind`；
3. Debit-only outcome 字段双写最多一个发布周期；
4. 旧 `macro entry_allowed` 保留为兼容摘要，但策略 Gate 改用 permission matrix；
5. 旧字段删除需单独列出版本和日期，不允许无限兼容；
6. 不保留第二套 final candidate authority。

---

## 23. 需要再次明确批准的事项

本文合并只代表设计文档可进入仓库，不代表以下实现已获批准：

1. 将候选空间扩大到 `PUT_CREDIT_VERTICAL`、`CALL_CREDIT_VERTICAL`、`IRON_CONDOR`；
2. 新增 `RESEARCH_CANDIDATE` 决策等级；
3. `strategy_decision.v3`、signed outcome 与 account-risk schema bump；
4. 新增两个生产文件的复杂度预算；
5. Event Settlement Debit 在 pre-event 阶段是否允许成为小风险 Manual Candidate；
6. account risk bootstrap 百分比；
7. risk capital 的本地输入方式；
8. first-touch 初始定义；
9. short-premium ManagementPolicy 初始参数；
10. Credit/IC 从 shadow 升为人工候选的 promotion 门槛。

批准顺序建议：

```text
先批准 Batch A
→ 完成 Event Debit + Account Risk
→ 验收
→ 再批准 Batch B
→ Credit Vertical shadow
→ Iron Condor shadow
→ 数据达标后单独批准 promotion
```

---

## 24. 最终产品定义

v4 完成后，SPX Spark 不应只是“根据状态推荐一个结构”，而应成为：

```text
观点 → 命题 → 市场赔率 → 现实概率 → 路径风险
→ 可执行结构 → 账户适配 → 人工权限 → 真实结果 → 样本外验证
```

系统的正确目标不是每天找到交易，而是做到：

1. 能在外部交易员给出 strike 之前主动发现同类命题；
2. 能解释该价格为什么是好赔率或坏赔率；
3. 能明确区分赔率好、胜率高和 Edge 已证明；
4. 能识别卖方策略的 first-touch 与尾部风险；
5. 能阻止市场上“好交易”在错误账户状态下被重复放大；
6. 能用真实成交和前瞻样本推翻自己的假设。

`NO_TRADE` 继续是正式、可回测的输出；`RESEARCH_CANDIDATE` 让新想法可见；只有通过结构、概率、账户和验证四层门槛的候选，才成为 `MANUAL_CANDIDATE`。
