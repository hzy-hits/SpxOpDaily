# SPX Spark 策略信号引擎 v4：账户无关的命题与赔率引擎

状态：**设计草案，等待批准；本文不授权实现或部署。**  
适用仓库：`hzy-hits/SpxOpDaily`  
基线：`e279ba6029ccba2dbbe4d2b98ceb0f688f43b487`  
自动下单：**继续禁止**

本文是 v2/v3 策略合同的增量修订。v4 只解决一个问题：

> 给定当前 SPX/SPXW 市场事实，是否存在一笔单位化、可解释、可证伪的交易候选？

v4 **完全账户无关**。它不读取账户净值、Buying Power、持仓、盈亏或资金规模；不决定买几组；不设置日、周、月亏损锁。它只输出每一策略单位的市场逻辑、可执行赔率、最大收益、最大损失和证据状态。

---

## 0. Change Brief

### 用户可见目标

将现有流程：

```text
市场状态 → Call / Put / Butterfly / NoTrade
```

改为：

```text
市场事实
  → 可证伪命题
  → 可执行赔率
  → 路径/终值证据
  → 候选结构
  → MANUAL_CANDIDATE / RESEARCH_CANDIDATE / NO_TRADE
```

系统必须回答：

1. 这笔交易押注的事件是什么；
2. 市场要求多高概率才能保本；
3. 我们是否有独立证据优于市场赔率；
4. 结构是否因报价、路径、事件或成本而失效；
5. 该候选是已验证还是仅供研究。

### 明确不做

v4 不包含：

- Account Risk Gate；
- 账户净值或 Buying Power；
- 推荐合约数量；
- 当前持仓与重复因子暴露；
- SPY、TLT 等非 SPXW 执行候选；
- 裸 Call、裸 Put；
- 自动下单、自动滚仓、自动加仓。

### 现有 owner

继续复用：

```text
macro_event_clock.py
application/market_features/strategy_distribution_forecast.py
application/market_features/physical_followthrough.py
application/order_map/strategy_facts.py
application/order_map/strategy_regime.py
application/order_map/candidate_factory.py
application/order_map/strategy_ranker.py
application/order_map/strategy_select.py
application/order_map/strategy_outcomes.py
application/order_map/delivery.py
analytics/options/strategy_payoff.py
infrastructure/operational_db.py
```

最多新增一个生产文件：

```text
application/market_features/strategy_propositions.py
```

### 复杂度边界

- 新依赖：0；
- 新 service/timer/queue：0；
- 新数据库/表：0；
- 新 Rust contract：0；
- 新账户读取：0；
- `strategy_decision` 仍是唯一人工候选出口；
- `automatic_ordering=false` 不变。

---

## 1. 当前缺口

### 1.1 缺少命题层

系统目前从 Failed Break、Trend Pullback、Confirmed Level、Stable Pin 等路径事件开始。

但很多交易首先是一个终值命题，例如：

```text
SPX 今日是否收在昨日收盘上方？
CPI 后是否结算在事件前价格上方？
到期是否越过某个结构阈值？
到期是否留在某个区间？
```

`7730/7735 Call Spread < 2.50` 的本质不是“买一个价差”，而是：

```text
H: SPX settlement > approximately 7732.5
```

没有命题层，系统只能在外部给出 strike 后解释，不能主动发现。

### 1.2 缺少赔率层

窄幅 Debit Vertical 的：

```text
Debit / Width
```

可以近似表示市场对局部终值事件的风险中性定价，但不是现实胜率。

当前代码已计算 `debit_fraction_of_width`，却没有把它作为核心赔率字段展示和扫描。

### 1.3 路径状态不等于价格优势

- `TREND` 不代表 Debit 仍便宜；
- `CONVERGENCE` 不代表卖方仍有足够 Credit；
- `PIN_STABLE` 不代表 Butterfly 定价合理。

需要把“市场如何走”与“期权是否贵/便宜”分开。

### 1.4 卖方缺少正确语义

现有系统以 Debit 为中心。直接加入 Credit Spread 或 Iron Condor 会错误处理：

- 开仓 Credit；
- Buyback Ask；
- Short Premium 止盈止损；
- first-touch；
- 最大风险；
- 费用后的真实期望。

---

## 2. 最小架构

v4 只增加三层，不再扩张为账户或组合管理系统。

```text
MarketFactPack
  → StrategyPropositionSet
  → PathRegime + PricingEdge
  → Candidate Factory
  → Hard Gates + Ranker
  → strategy_decision
```

### 2.1 PathRegime

```text
TREND
CONVERGENCE
PIN
SHOCK
TRANSITION
UNCERTAIN
```

回答：市场正在怎么走。

### 2.2 StrategyProposition

```text
TERMINAL_ABOVE
TERMINAL_BELOW
TERMINAL_BETWEEN
FIRST_TOUCH_UPPER
FIRST_TOUCH_LOWER
```

回答：候选押注什么事件。

### 2.3 PricingEdge

```text
RICH
FAIR
CHEAP
UNAVAILABLE
```

回答：可执行价格是否相对现实风险有优势。

三层不得互相替代：

- 横盘但 Credit 太少 → 不卖；
- 趋势正确但 Debit 已追贵 → 不买；
- 命题合理但概率证据不足 → 研究候选；
- Delta 很远但 first-touch 风险未知 → 不生成卖方候选。

---

## 3. Strategy Proposition

### 3.1 Schema

Python-only，不进入 Rust：

```yaml
schema_version: strategy_proposition.v1
proposition_id: proposition:<stable-hash>
source_kind: PRIOR_CLOSE | EVENT_PRE_CLOSE | OVERNIGHT_SYNTHETIC |
             Q_MEDIAN | Q_MODE | ZERO_GAMMA | FLIP_ZONE |
             PUT_WALL | CALL_WALL | CONFIRMED_LEVEL
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
research_status: UNVALIDATED | CALIBRATING | VALIDATED
```

### 3.2 初始来源

第一版只允许确定性来源：

1. prior close；
2. 事件前最后一个官方 SPX close；
3. 合格的 overnight synthetic SPX；
4. Q median / mode；
5. Zero Gamma / Flip；
6. Put Wall / Call Wall；
7. confirmed level。

Twitter、新闻和 LLM 可以提供解释，但不得直接提供阈值、概率或候选权限。

### 3.3 命题不是策略

同一命题可以由多个结构表达。

例如：

```text
H: settlement > 7732.5
```

可以比较：

- 7730/7735 Call Debit Vertical；
- 7725/7735 Call Debit Vertical；
- 更宽 Vertical；
- NoTrade。

命题层不选 strike，不创建 Trade Ready。

### 3.4 去重

同一 expiry、kind 且阈值相距不超过 2.5 点的命题合并。

每轮最多保留：

- 3 个上方终值命题；
- 3 个下方终值命题；
- 2 个区间命题；
- 2 个 first-touch 命题。

---

## 4. PathRegime

### TREND

使用现有方向分数、效率、VWAP 斜率、穿越次数、Breadth 与价格位置。

用途：方向性 Debit Vertical。

### CONVERGENCE

要求：

- 低路径效率；
- 多次 VWAP 穿越；
- 区间收缩；
- Breadth 接近中性；
- 没有 active shock；
- 没有持续的一侧接受。

用途：允许评估 Credit Vertical / Iron Condor；不自动授权。

### PIN

继续使用 Value Center、Q mode、local mass、return-to-center、de-pin risk、VIX/Breadth、recent extreme 与 shock veto。

用途：Butterfly。

### SHOCK

现有 intraday shock ACTIVE、事件后发现期或异常价格/IV 扩张均属于 SHOCK。

用途：禁止新 Short Premium 与 Pin Butterfly。

---

## 5. PricingEdge

### 5.1 Schema

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

### 5.2 定义

对具体候选：

```text
EV_net = E_P[payoff or management PnL]
         - executable premium
         - fees
         - slippage
```

不是只比较“上涨概率”。

### 5.3 状态

- `CHEAP`：对买方有利；
- `RICH`：对卖方有利；
- `FAIR`：优势不足以覆盖成本和误差；
- `UNAVAILABLE`：报价或概率数据不足。

### 5.4 权限

在达到统计 promotion 前，PricingEdge 只排序和展示：

- 硬门通过但 Edge 未验证 → `RESEARCH_CANDIDATE`；
- 已验证策略族才可输出 `MANUAL_CANDIDATE`；
- 模型不能绕过报价、事件或路径硬门。

---

## 6. Debit/Credit 通用经济学

### 6.1 有符号现金流

统一约定：

```text
收到现金为正；支付现金为负。
```

```text
PnL_points = entry_cashflow + exit_cashflow - fees - slippage
```

示例：

- Debit Vertical：开仓 `-2.40`，平仓 `+4.50`；
- Credit Spread：开仓 `+1.20`，平仓 `-0.50`。

### 6.2 Unit Economics

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

USD 只表示 SPXW 乘数 100 下的一组结构，不表示推荐仓位。

### 6.3 Conservative BBO

Debit 开仓：买腿 Ask、卖腿 Bid。  
Credit 开仓：卖腿 Bid、买腿 Ask。  
平仓方向相反。

禁止使用多腿 Mid 冒充可执行价格。

### 6.4 费用压力

每个候选必须报告：

- gross payoff；
- round-trip commission；
- one-tick adverse slippage；
- two-tick stress；
- net payoff。

---

## 7. 策略候选

v4 固定为：

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

`EVENT_SETTLEMENT_THRESHOLD` 是 Debit Vertical 的 setup kind，不是新的 payoff 类型。

---

## 8. Directional Debit Vertical

继续使用现有：

```text
FAILED_BREAK_RECLAIM
TREND_PULLBACK
BREAKOUT_ACCEPTANCE
```

硬门保持：

- 方向与路径一致；
- target / invalidation 完整；
- target room 足够；
- stop ATR 合理；
- no late chase；
- Debit/Width 合格；
- exact quote ready。

候选卡增加：

```text
Debit / Width
Max Gain / Max Loss
Breakeven
Required probability proxy
Management-policy EV
```

---

## 9. Event Settlement Vertical

这是 v4 第一优先级，用于主动发现类似 `7730/7735 < 2.50` 的机会。

### 9.1 命题

```text
H: settlement > L
```

或：

```text
H: settlement < L
```

### 9.2 枚举

对每个阈值 `L`：

- 枚举 5 点和 10 点宽；
- 使 `L` 落在两个 strike 之间或靠近价差中部；
- expiry 与 target_at 一致；
- exact two-leg quote ready。

### 9.3 字段

```yaml
setup_kind: EVENT_SETTLEMENT_THRESHOLD
threshold_level: L
threshold_source: PRIOR_CLOSE | EVENT_PRE_CLOSE | ...
width: W
executable_debit: D
odds_proxy: D / W
breakeven: ...
required_probability_after_costs: (D + costs) / W
```

### 9.4 语义

```text
D / W = 48%
```

只能称为市场赔率代理，不能称为 52% 现实胜率。

只有：

```text
P_conservative > required_probability + model_buffer
```

才可能存在正优势。

### 9.5 宏观事件权限

将全局 `entry_allowed` 改成策略家族权限：

```yaml
pre_event:
  EVENT_SETTLEMENT_VERTICAL: RESEARCH_ALLOWED
  DIRECTIONAL_DEBIT: BLOCKED
  BUTTERFLY: BLOCKED
  CREDIT_VERTICAL: BLOCKED
  IRON_CONDOR: BLOCKED

post_event_discovery:
  EVENT_SETTLEMENT_VERTICAL: MANAGE_EXISTING_ONLY
  DIRECTIONAL_DEBIT: WAIT_CONFIRMATION
  BUTTERFLY: BLOCKED
  CREDIT_VERTICAL: BLOCKED
  IRON_CONDOR: BLOCKED

normal:
  supported_families: EVALUATE
```

Event Settlement 第一阶段只输出 Research Candidate。

---

## 10. Credit Vertical

先单侧，后 Iron Condor。

### Put Credit

命题：管理窗口内不会有效跌破下方边界。

```text
short Put K
long Put K-width
```

### Call Credit

命题对称。

### 初始条件

- 仅 RTH；
- 仅 0DTE；
- 固定 5 点宽；
- short Delta 目标 0.10–0.18，仅用于 strike 定位；
- short strike 在 Opening Range 和 invalidation 外；
- no active shock；
- exact credit BBO ready；
- Credit 足以覆盖费用和滑点；
- first-touch evidence available；
- PricingEdge 对卖方不是 CHEAP。

第一阶段仅 Shadow / Research Candidate。

---

## 11. Iron Condor

### 11.1 生成规则

禁止直接用“两侧各 12 Delta”构造。

只有：

```text
passed Put Credit Vertical
AND
passed Call Credit Vertical
```

才组合 Iron Condor。

### 11.2 必要条件

- PathRegime = CONVERGENCE；
- no active shock；
- 两侧 short strike 均在结构边界外；
- 两侧 first-touch 风险可估计；
- 四腿同 provider、同 expiry、报价新鲜；
- combined Credit 覆盖四腿费用；
- conservative expected PnL 为正或至少进入研究阈值。

### 11.3 输出

```text
P(touch short put)
P(touch short call)
P(touch either)
P(recover after touch)
P(reach long wing)
P(finish between breakevens)
```

POP 单独出现不合格。

第一阶段始终为 Research Candidate。

---

## 12. Butterfly

保持现有严格 Pin Gate：

- PIN_STABLE；
- body 对齐 Value Center 与 Q mode；
- body 距 spot 合理；
- no active shock；
- recent extreme=false；
- de-pin risk 低；
- Debit/Width 合格；
- 三腿 BBO ready。

v4 只增加：

```text
Q mass under tent
P terminal mass under tent
Management-policy EV
```

不放宽生成条件。

---

## 13. First-Touch 标签

卖方不能只看到期 ITM。

对每个 Credit/IC 候选记录：

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
```

Bootstrap 定义：

```text
TOUCH:
  minute high/low crosses short strike

RECOVERED:
  10 分钟内回到 short strike 内侧，并连续 2 根 5m 收盘保持

ACCEPTED_BEYOND:
  touch 后连续 2 根 5m 收盘仍在外侧

LONG_WING_REACHED:
  minute high/low reaches protective wing
```

第一版模型保持透明：

- same-clock samples；
- session weighting；
- event/non-event 分桶；
- PathRegime 分桶；
- Beta shrinkage；
- day-block bootstrap。

不先引入复杂 ML。

---

## 14. ManagementPolicy

### Long Premium

继续使用现有版本化规则：

```text
conservative ask entry
conservative bid valuation
profit arm
trail
premium stop
time stop
hard close
```

### Event Debit

事件前普通 Stop 无法约束跳空：

- 最大风险始终是全部 Debit + 费用；
- 不把 Stop 价格表述为最大损失；
- 持有到结算与事件后主动退出分别统计。

### Short Premium

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

这些是冻结研究参数，不是已验证最优值。

---

## 15. Hard Gates 与权限

### 通用 Gate

- session legal；
- SPX coordinate ready；
- proposition 未过期；
- expiry/target_at 一致；
- exact legs complete；
- same provider；
- quote fresh；
- cross-leg skew 合格；
- executable BBO 有效；
- fees 后 payoff 合法；
- event permission 允许；
- automatic ordering=false。

### Debit Gate

- target/stop geometry；
- target room；
- no late chase；
- Debit/Width；
- proposition 与方向一致。

### Credit Gate

- defined risk；
- short strike 在 invalidation 外；
- Credit 显著高于成本；
- buyback ask 可计算；
- no shock；
- first-touch evidence available。

### 权限

```text
MANUAL_CANDIDATE:
  结构族已经通过独立 promotion，且本次硬门通过。

RESEARCH_CANDIDATE:
  结构与报价完整，但 Edge 或策略族尚未验证。

NO_TRADE:
  无完整候选，或硬门失败。
```

---

## 16. Ranker

`NO_TRADE` 与候选一起比较。

```text
Score = conservative_expected_pnl / max_loss
        - tail_loss_penalty
        - liquidity_penalty
        - model_uncertainty_penalty
```

校准不足时：

- Score 只排序；
- 不能单独升门；
- Edge 未验证的通过候选仍是 Research；
- NoTrade 必须保留为正式候选。

---

## 17. strategy_decision.v4

Python-only：

```yaml
schema_version: strategy_decision.v4
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

明确禁止字段：

```text
account_nlv
buying_power
recommended_contracts
risk_fraction_of_account
daily_loss_limit
```

---

## 18. Outcome 与验证

### 18.1 样本单位

一个完整策略机会 = 一个样本。

不是：

- 每条腿；
- 每个 tick；
- 同一 opportunity 的重复 decision；
- 同日多次重建的相同候选。

继续使用 `opportunity_id` 去重。

### 18.2 保存字段

```text
proposition
path regime
pricing edge
entry executable BBO
fees/slippage assumptions
multi-horizon marks
management-policy exit
terminal payoff
first-touch labels
invalidation breach
censor kind
policy versions
```

### 18.3 成交语义

核心引擎只使用市场数据：

- conservative BBO；
- quote reached；
- one/two-tick slippage stress。

不得把 `quote_reached` 称为真实 fill probability。

---

## 19. Promotion

每个策略家族独立验证。

最低门槛：

- 总样本 ≥100；
- 独立交易日 ≥40；
- OOS 样本 ≥30；
- OOS 平均净 PnL > 0；
- OOS Profit Factor ≥1.10；
- Full Sample PF ≥1.20；
- 双倍成本下不显著为负；
- 参数在 OOS 前冻结；
- day-block bootstrap 置信区间明确报告。

Event Debit 额外要求：

- required probability 校准；
- Brier Score 优于无条件基线；
- CPI/FOMC/NFP 分开；
- 删除最好三笔后期望仍不为负。

Credit/IC 额外要求：

- first-touch 校准；
- touch 后恢复/继续有区分能力；
- 每 25 笔插入一次 max-loss 的压力测试；
- settlement 与真实 ManagementPolicy 分开报告；
- 不以胜率或 POP 单独 promotion。

---

## 20. 实施顺序

### Phase A：通用 Debit/Credit 经济学

修改现有 payoff、outcome、select、ranker。

完成：

- signed cashflow；
- unit economics；
- Credit BBO；
- Short Premium policy；
- Credit outcome。

不增加策略类型。

### Phase B：Event Proposition

新增最多一个文件 `strategy_propositions.py`。

完成：

- prior close / event pre-close / synthetic / structure propositions；
- 5/10 点 Event Settlement Vertical；
- Debit/Width 与 required probability；
- Research Candidate。

### Phase C：First-Touch

扩展现有 physical/outcome 管道，完成 touch/recovery/wing 标签。

### Phase D：Credit Vertical Shadow

增加 Call/Put Credit Vertical，只输出 Research。

### Phase E：Iron Condor Shadow

仅从两侧 passed Credit Vertical 组合，只输出 Research。

### Phase F：Promotion

按 §19 分策略族升门。

---

## 21. 测试与验收

### 数学不变量

- Debit/Credit payoff 上下界；
- max gain/max loss；
- breakeven；
- signed cashflow；
- Credit buyback 方向；
- IC 非对称翼；
- 费用增加不能改善净 PnL。

### 因果不变量

- `available_at <= decision_at`；
- proposition source 不晚于 decision；
- P 模型不读取当前会话未来；
- event 结果不泄漏到 pre-event；
- opportunity 去重。

### 冻结案例

1. 昨收附近窄 Vertical 被主动枚举；
2. Debit/Width < 0.5，但 P 下界不足 → Research；
3. 横盘但 Credit 太低 → NoTrade；
4. 两侧 12 Delta、touch unavailable → 不生成 IC；
5. CONVERGENCE + Rich + 两侧通过 → IC Research；
6. 趋势正确但 late chase → NoTrade。

---

## 22. 复杂度预算

| 项 | 上限 |
|---|---:|
| 新生产文件 | 1 |
| 新研究脚本 | 0，扩展现有 replay/backfill |
| 新依赖 | 0 |
| 新 service/timer/queue | 0 |
| 新数据库/表 | 0 |
| 新 Rust contract | 0 |
| 账户读取 | 0 |
| 自动下单变化 | 0 |

每个 Phase 独立 PR，不一次性实现全部策略族。

---

## 23. 需要批准的决策

1. v4 保持完全账户无关；
2. 新增 `RESEARCH_CANDIDATE`；
3. Event Settlement Vertical 优先；
4. Credit Vertical 与 Iron Condor 初期仅 Research；
5. 最多新增一个生产文件；
6. `strategy_decision.v4` 继续 Python-only；
7. 自动下单继续禁止。

批准后按 Phase A → B → C → D → E 实施。