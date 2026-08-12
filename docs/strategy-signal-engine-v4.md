# SPX Spark 策略信号引擎 v4：复用优先的事件结算价差扩展

状态：**设计草案，等待批准；本文不授权实现或部署。**  
适用仓库：`hzy-hits/SpxOpDaily`  
基线：`e279ba6029ccba2dbbe4d2b98ceb0f688f43b487`  
自动下单：**继续禁止**

本文是 v2/v3 策略合同的最小增量修订。它不重建一套新的“命题引擎”、
“赔率引擎”或“账户引擎”，只补上当前系统已经暴露出的一个具体能力缺口：

> 系统能够解释别人给出的 `7730/7735 Call Debit Spread`，但不能在事件前主动从
> 昨日收盘等现有参考位中枚举出这种窄幅结算价差，并展示其可执行赔率。

v4.0 只解决这一件事：

```text
现有市场事实
  → 现有参考位
  → 现有 Debit Vertical 枚举器
  → 事件结算候选
  → 现有 Ranker / NO_TRADE / nearest-candidate
  → 现有持久化与回填
```

第一版**不增加 Credit Vertical、Iron Condor、First-Touch 模型、账户风控、
新决策状态或新生产模块**。这些能力只有在当前垂直切片验证完成、且已有 owner
确实无法承载时，才另行提交 Change Brief。

---

## 0. Change Brief

### 0.1 用户可见目标

在 CPI、FOMC、NFP 等已登记高影响事件前，系统应能主动发现并展示类似：

```text
参考位：昨日 SPX 收盘 7728.20
候选：Aug-12 7730/7735 Call Debit Vertical
可执行 Debit：2.40
宽度：5.00
Debit / Width：48.0%
到期盈亏平衡：7732.40
含开仓费用的近似保本概率：约49%–50%
证据状态：research_unvalidated
权限：NO_TRADE，最近研究候选可见
```

系统必须明确区分：

- `Debit / Width` 是市场赔率代理，不是现实胜率；
- 候选被发现，不等于已经证明有 Edge；
- 研究候选可见，不等于自动升为人工交易授权。

### 0.2 明确不做

v4.0 不包含：

- Account Risk Gate、账户净值、Buying Power、仓位数量；
- `RESEARCH_CANDIDATE` 新决策状态；
- `strategy_decision.v4` 新 schema；
- Credit Vertical、Iron Condor、Short Premium ManagementPolicy；
- 新的 `PricingEdge`、`PathRegime` 或 `StrategyProposition` 类型层；
- 新生产文件；
- 新 service、timer、queue、数据库、表或 Rust contract；
- SPY、TLT 等非 SPXW 合约；
- 自动下单、自动滚仓或自动加仓；
- Twitter、LLM 或自然语言直接提供数值概率。

### 0.3 复杂度边界

```text
新生产文件          0
新研究脚本          0
新依赖              0
新配置系统          0
新 service/timer     0
新数据库/表          0
新 Rust contract     0
新 decision schema   0
账户读取              0
自动下单变化          0
```

实现只扩展现有 owner。任何阶段若发现必须新增文件，先停止实现并提交新的
Change Brief；不得以“未来还要做 Iron Condor”为理由预先搭建抽象。

---

## 1. 复用审计

本节是 v4 的核心。实现前必须逐项证明现有能力不足；没有证明不足，就不得新增。

### 1.1 参考位已经存在：不新增行情或参考位服务

`application/order_map/service.py` 已经在 Order Map payload 中保存：

```text
day_move.prior_close
macro_event.active_event
macro_event.next_event
trigger_coordinate
underlier / spot
rn_density
zero_gamma
flip_zone
wall_ladder
front expiry
```

v4.0 只使用其中的：

```text
day_move.prior_close
macro_event.next_event / active_event
trigger_coordinate
front expiry
```

第一版不把 Q mode、Wall、Flip 等全部引入事件候选，避免候选膨胀。它们继续服务
现有路径策略；是否加入事件阈值必须以后用真实样本证明增量价值。

结论：**不新增 reference-level collector 或 proposition service。**

### 1.2 宏观事件时钟已经存在：不新增 Permission Matrix

`macro_event_clock.py` 已经返回：

```text
mode = normal | pre_event | post_event
entry_allowed
active_event
next_event
release_at
minutes_to_release
impact
```

v4.0 直接复用：

- `normal` 且下一高影响事件在冻结窗口内：可以枚举研究候选；
- `pre_event`：沿用现有全局冻结，不生成新的人工建议；
- `post_event`：事件前结算候选过期，不再追原命题。

不新增策略家族权限矩阵。若以后加入卖方策略，届时再评估单一布尔权限是否不足。

结论：**`macro_event_clock.py` 第一阶段无需修改。**

### 1.3 事件类型已经存在：不新增 StrategyProposition schema

`domain/strategy_distribution_forecast.py` 已有：

```text
ProbabilityEventDefinition
ProbabilityEventKind.TERMINAL_ABOVE
ProbabilityEventKind.TERMINAL_BELOW
ProbabilityEventKind.TERMINAL_BETWEEN
ProbabilityEventKind.UPPER_FIRST_TOUCH
ProbabilityEventKind.LOWER_FIRST_TOUCH
```

v4.0 不再定义 `strategy_proposition.v1`。候选内部只附着现有语义兼容的普通字典：

```yaml
probability_event:
  event_id: event-threshold:<stable-hash>
  kind: terminal_above | terminal_below
  target_at: expiry settlement time
  lower_level: ...
  upper_level: ...
```

不修改 Rust 消费的 forecast contract，也不新增 Python schema。

结论：**复用现有 ProbabilityEventDefinition 语义，不创建新 domain 类型。**

### 1.4 Vertical 枚举器已经存在：不新增候选工厂

`candidate_factory.py` 已经具备：

- 5/10/15/20 点 Debit Vertical 枚举；
- 稳定 `candidate_id` / `opportunity_id`；
- exact-leg 查找；
- Schwab 优先、IBKR fallback；
- 同 provider、quote freshness、cross-leg skew；
- conservative synthetic BBO；
- `vertical_economics`；
- 候选去重与排序入口。

v4.0 只新增一种 evidence：

```text
setup_kind = EVENT_SETTLEMENT_THRESHOLD
```

并把昨日收盘附近的两个相邻 5 点价差交给现有 Vertical 构造函数。

当前 `_rth_option_legs` 实际已具备 provider fallback 和新鲜度检查。若 GTH 复用时
名称不准确，可以在同一文件内重命名为 `_exact_option_legs`，并同步现有调用；不另写
第二套 GTH exact-leg 逻辑。

结论：**不新增 `strategy_propositions.py` 或第二个 candidate factory。**

### 1.5 Payoff 与赔率已经存在：不新增 Unit Economics schema

`analytics/options/strategy_payoff.py` 已有：

```text
conservative_vertical_bbo
vertical_economics
vertical_payoff
```

`vertical_economics` 已输出：

```text
width_points
max_loss_points
max_gain_points
breakeven_spx
debit_fraction_of_width
```

这已经足够表达 5 点窄幅价差。v4.0 不新增 `strategy_unit_economics.v1`，也不先做
Debit/Credit 有符号现金流重构。

事件候选只额外计算两个展示字段：

```text
market_odds_proxy = executable_debit / width
required_probability_after_open_cost
```

其中：

```text
required_probability_after_open_cost
  = (executable_debit + opening_fees_points) / width
```

第一版明确是“持有至结算”的概率门槛；主动提前退出属于不同 ManagementPolicy，
不得混在同一个数字里。

结论：**复用现有 Vertical economics，只增加字段，不新增经济学对象。**

### 1.6 Ranker 已有概率、Edge 与研究状态：不新增 PricingEdge

`strategy_ranker.py` 已经支持：

- 通用 quote/TTL hard gates；
- 结构专属 hard gates；
- `probability_evidence`；
- `required_p_breakeven`；
- `model_p`；
- `edge_status=research_unvalidated`；
- `advisories`；
- `policy_ev`；
- passed / near-miss / gate audit。

v4.0 不新增：

```text
PricingEdge schema
RICH / FAIR / CHEAP 状态机
新的 Ranker
```

事件候选增加一个专属 hard-gate 分支，避免错误套用盘中方向价差的 ATR、target-room
和 late-chase 规则。其 Edge 字段继续使用现有 `candidate.edge`。

结论：**扩展 `_vertical_hard_gates` 与 `_score_candidate`，不新增层。**

### 1.7 研究候选已经有表达方式：不新增 RESEARCH_CANDIDATE

现有系统已有：

```text
manual_authority_eligible = false
research_alternative_only
NO_TRADE + why_not.nearest_candidate
shadow candidate / nearest candidate persistence
```

v4.0 的事件候选固定：

```text
manual_authority_eligible = false
```

因此第一阶段仍输出：

```text
Decision: NO_TRADE
Nearest candidate: EVENT_SETTLEMENT_THRESHOLD ...
Primary blocker: research_alternative_only
```

Desk View 只需要把该 blocker 人类化为：

```text
研究候选已发现，但事件结算优势尚未完成前向验证
```

不创建第三种决策状态，不修改 `strategy_decision.v2`。

结论：**复用 NO_TRADE + nearest candidate。**

### 1.8 持久化与回填已经覆盖 nearest candidate：不新增表

`operational_db._decision_rows()` 在没有正式 candidate 时，会冻结
`why_not.nearest_candidate` 的腿；`strategy_policy_backfill.py` 也会读取：

```text
decision.candidate
or
why_not.nearest_candidate
```

因此研究候选已经能够使用现有 `decisions`、`decision_legs`、`outcomes` 和 quote lake。

v4.0 只在现有 backfill 中增加事件候选的终值标签，不新增表或脚本。

结论：**复用现有 immutable decision、legs、outcome 与 backfill。**

---

## 2. v4.0 唯一新增能力

### 2.1 Setup Kind

```text
EVENT_SETTLEMENT_THRESHOLD
```

它仍然生成现有策略类型：

```text
CALL_DEBIT_VERTICAL
PUT_DEBIT_VERTICAL
```

不新增 payoff 类型。

### 2.2 命题

第一版只使用一个阈值来源：

```text
PRIOR_CLOSE
```

理由：

- 它已经存在于 payload；
- 语义清楚；
- 与“今天是否收涨/收跌”直接对应；
- 能覆盖 `7730/7735` 这一真实缺口；
- 不会一次引入 Wall、Q mode、Flip、Expected Move 等大量相关候选。

候选附着：

```yaml
threshold:
  source: PRIOR_CLOSE
  level: 7728.20

probability_event:
  kind: terminal_above
  target_at: 2026-08-12T16:00:00-04:00
  lower_level: 7732.40
```

注意：真正的事件阈值是候选的到期盈亏平衡点，而 `prior_close` 是候选生成参考位。
两者不得混写。

### 2.3 双向扫描

系统不能因为外部叙事偏多就只生成 Call。

对同一个 prior close，同时扫描：

```text
CALL_DEBIT_VERTICAL：押注结算高于某个 breakeven
PUT_DEBIT_VERTICAL：押注结算低于某个 breakeven
```

后续由市场赔率和物理证据分别评估。Twitter 或 LLM 不得直接决定方向。

---

## 3. 候选生成

### 3.1 事件定位窗口

完全复用 `macro_event.next_event` 与现有 `pre_event` 冻结。

冻结研究常量放入现有 `StrategyPolicy`：

```python
event_positioning_max_minutes = 18 * 60
event_positioning_min_minutes = 30
event_settlement_widths = (5.0,)
```

只有同时满足才枚举：

1. 当前处于 SPXW 合法 GTH 或 RTH；
2. `next_event.impact == "high"`；
3. `30 < minutes_to_release <= 1080`；
4. front expiry 日期等于事件发布日期；
5. `day_move.prior_close` 可用；
6. 当前宏观模式不是 `pre_event` 或 `post_event`；
7. exact two-leg quote 能从现有 LatestState 找到。

这里没有新增 event state machine。30 分钟内继续由现有 `pre_event` 全局 Gate 关闭。

### 3.2 Strike 枚举

令：

```text
L = prior_close
K_floor = floor_to_5(L)
K_ceil  = ceil_to_5(L)
```

Call 只枚举两个相邻候选：

```text
K_floor / (K_floor + 5)
K_ceil  / (K_ceil  + 5)
```

Put 对称枚举：

```text
K_ceil  / (K_ceil  - 5)
K_floor / (K_floor - 5)
```

若 floor 与 ceil 相同，去重。候选仍通过现有稳定 `candidate_id` 去重。

以 `prior_close=7728.20` 为例，Call 候选会包含：

```text
7725/7730
7730/7735
```

因此系统能够主动看到 `7730/7735`，但不会预设它一定优于 `7725/7730`。

### 3.3 Exact Quote

复用现有 exact-leg 路径：

```text
same provider
fresh bid/ask
cross-leg source skew within policy
Schwab first during RTH
IBKR fallback / GTH fresh quote
```

不为事件候选新增订阅服务，不突破当前 ticker-line 预算。没有现成 fresh quote 就输出
明确的 near-miss，而不是主动扩张数据服务。

### 3.4 候选字段

复用现有 CandidateRow，只增加：

```yaml
setup_kind: EVENT_SETTLEMENT_THRESHOLD
setup_variant: PRIOR_CLOSE
manual_authority_eligible: false
threshold_source: PRIOR_CLOSE
threshold_level: 7728.20
probability_event:
  event_id: event-threshold:<hash>
  kind: terminal_above | terminal_below
  target_at: ...
  lower_level: ...
  upper_level: ...
market_odds_proxy: 0.48
required_probability_after_open_cost: 0.49
```

不新增 proposition_id、pricing_edge schema 或 decision schema。

---

## 4. 赔率语义

### 4.1 市场赔率代理

对于 5 点 Call Debit Vertical：

```text
market_odds_proxy = executable_debit / 5
```

例如：

```text
Debit = 2.40
Width = 5.00
market_odds_proxy = 48.0%
```

它只能解释为窄幅价差在风险中性定价下的局部概率代理，不是现实世界胜率。

### 4.2 保本概率

持有至结算、忽略折现：

```text
required_p_before_cost = debit / width
required_p_after_open_cost = (debit + opening_fees_points) / width
```

保守开仓价已经使用：

```text
long ask - short bid
```

因此不再重复加入 bid/ask spread；费用单独报告。

### 4.3 不复用错误公式

当前 Ranker 针对盘中 ManagementPolicy 的 `required_p_breakeven` 语义不能直接套到
事件结算候选。事件候选必须分支计算上述 settlement 公式，并在字段中写明：

```text
basis = hold_to_settlement_binary_approximation
```

不修改现有方向性候选的公式，避免语义回归。

### 4.4 研究状态

v4.0 不构建新的事件 Physical 模型。第一阶段固定：

```text
edge_status = research_unvalidated
model_p = null
p_interval_low = null
manual_authority_eligible = false
```

理由：现有 `physical_followthrough` 面向已确认价格路径和短 horizon；直接拿它给 CPI
隔夜结算命题赋概率会制造错误精度。

系统第一步先把命题、赔率、报价和终值结果完整记录下来。达到足够样本后，才在现有
`physical_followthrough.py` 内增加事件专属估计；没有证据前不新增模型。

---

## 5. Hard Gates

事件候选不使用盘中方向价差的：

```text
ATR stop band
target room
VWAP distance
15m impulse
late chase
```

因为这些变量不对应“事件后到结算是否超过 breakeven”的命题。

事件候选使用独立、最小的确定性 Gate：

1. event window 合法；
2. front expiry 与事件日期一致；
3. prior close 来源可用；
4. probability event target_at 晚于 decision_at；
5. exact quote ready；
6. quote TTL 与 source skew 合格；
7. `0 < executable_debit < width`；
8. `market_odds_proxy` 可计算；
9. macro mode 不是 `pre_event` / `post_event`；
10. `automatic_ordering=false`；
11. `manual_authority_eligible=false`。

最后一项不是市场质量失败，而是第一阶段的研究权限边界。Ranker 应把它放入
near-miss，保留完整候选和赔率字段。

---

## 6. strategy_decision 与展示

### 6.1 不升级 schema

继续使用：

```text
strategy_decision.v2
```

事件候选第一阶段的输出：

```yaml
decision_type: NO_TRADE
candidate: null
action_authority: none
why_not:
  nearest_candidate:
    strategy_type: CALL_DEBIT_VERTICAL
    setup_kind: EVENT_SETTLEMENT_THRESHOLD
    ...
  reasons:
    - research_alternative_only
```

### 6.2 Desk View

复用 `desk_strategy_view.py` 的 nearest-candidate 路径，只增加人类化文案和赔率摘要：

```text
结论  不做
主因  研究候选已发现，但事件结算优势尚未完成前向验证
最近候选  Call 价差 7730/7735 · Debit/Width=48% · BE=7732.4
下一步  保留研究样本，不把外部叙事当作已验证胜率
```

不新增通知 lane，不发送 Trade Ready 卡。

### 6.3 为什么不用 RESEARCH_CANDIDATE

现有 NO_TRADE + nearest candidate 已经同时满足：

- 研究机会可见；
- 不产生人工下单权限；
- 候选腿可持久化；
- 能进入现有回填；
- 不扩大 decision state machine。

因此新增 `RESEARCH_CANDIDATE` 没有必要。

---

## 7. 持久化与结果标签

### 7.1 在线持久化无需改表

现有 `_decision_rows()` 已会在 NO_TRADE 时冻结 nearest candidate 的腿。

事件候选继续写入：

```text
decisions
decision_legs
```

现有 1/2/3/4/5/7/10/15/20 分钟 outcomes 仍可作为事件前短路径诊断，但不得冒充
最终结算标签。

### 7.2 终值标签复用现有 backfill

扩展现有：

```text
src/spx_spark/data_platform/research/strategy_policy_backfill.py
```

当：

```text
setup_kind == EVENT_SETTLEMENT_THRESHOLD
```

时，不调用现有 20 分钟 Long Premium ManagementPolicy 作为官方标签，而是：

1. 从候选 `probability_event.target_at` 读取结算目标时点；
2. 从现有标准化 SPX session 数据取得当日最后合格价格；
3. 使用现有 `vertical_payoff()` 计算 terminal payoff；
4. 扣除记录的开仓费用；
5. 输出：

```text
terminal_event_success
terminal_spx
terminal_payoff_points
terminal_net_pnl_points
market_odds_proxy
required_probability_after_open_cost
censor_kind
```

若目标时点或最终 SPX 缺失，显式删失，不补陈旧值。

不新增表；输出继续进入现有 `strategy_policy_labels` 数据集。

### 7.3 样本单位

一个稳定 `opportunity_id` = 一个事件候选样本。

同一结构跨 tick 重复出现时，继续使用现有 opportunity 去重；不能把每个 tick 当作
独立预测。

---

## 8. 实现文件与修改范围

第一阶段只允许修改以下现有文件：

```text
src/spx_spark/application/order_map/strategy_facts.py
src/spx_spark/application/order_map/candidate_factory.py
src/spx_spark/application/order_map/strategy_ranker.py
src/spx_spark/application/order_map/desk_strategy_view.py
src/spx_spark/data_platform/research/strategy_policy_backfill.py
相关现有测试文件
```

### 8.1 `strategy_facts.py`

从已经存在的 payload 复制：

```text
references.prior_close
```

`event.next_event` 已经存在，不重复存储第二份。

### 8.2 `candidate_factory.py`

- 新增 `EVENT_SETTLEMENT_THRESHOLD` evidence；
- 复用 Vertical candidate constructor；
- 将 `_rth_option_legs` 在需要时重命名为通用 exact-leg helper；
- 只枚举 5 点相邻价差；
- 固定 research-only。

### 8.3 `strategy_ranker.py`

- 对 event setup 使用独立最小 Gate；
- 计算 odds proxy 与 settlement required probability；
- 不调用 ATR/target-room/late-chase；
- 保持 `research_alternative_only`。

### 8.4 `desk_strategy_view.py`

- humanize `event_threshold_research_only`；
- nearest candidate 行展示 Debit/Width 与 breakeven。

### 8.5 `strategy_policy_backfill.py`

- 增加 terminal-event label 分支；
- 复用现有 standardized SPX、quote lake、vertical payoff 与输出数据集。

### 8.6 明确不修改

第一阶段不修改：

```text
macro_event_clock.py
strategy_distribution_forecast.py
physical_followthrough.py
strategy_select.py
strategy_outcomes.py
operational_db.py
delivery.py
Rust workspace
```

如果实现过程中发现必须修改这些文件，先停止并更新设计，不得扩大范围后再补文档。

---

## 9. 冻结常量

只在现有 `StrategyPolicy` 中增加：

```python
event_positioning_max_minutes: float = 1080.0
event_positioning_min_minutes: float = 30.0
event_settlement_widths: tuple[float, ...] = (5.0,)
```

候选数量由确定性几何自然限制，不增加新的 runtime config。

修改这些值必须：

- 提升 `policy_version`；
- 运行冻结回放；
- 说明为什么不是对已有少量样本过拟合。

---

## 10. 验收案例

### 10.1 主案例：主动发现 7730/7735

输入：

```text
prior_close = 7728.20
next_event = high-impact CPI, next morning 08:30 ET
front_expiry = CPI date
session = GTH
7730C / 7735C fresh IBKR BBO available
combo executable ask = 2.40
```

要求：

- 枚举 7725/7730 与 7730/7735 Call Vertical；
- `7730/7735` 显示 `market_odds_proxy=0.48`；
- breakeven 为 7732.40；
- 决策仍为 NO_TRADE；
- nearest candidate 带完整两腿；
- blocker 为 research-only，而不是“没有候选”。

### 10.2 双向性

同一输入必须同时有 Put 方向研究候选；不得因外部宏观叙事只扫描 Call。

### 10.3 事件缺失

`next_event` 不存在、不是 high impact 或时间超出窗口：不得生成 event candidate。

### 10.4 Expiry 不匹配

front expiry 不等于事件日期：不得用错误到期日表达事件命题。

### 10.5 冻结窗口

进入现有 `pre_event` 后：不得再产生新的事件定位建议。

### 10.6 数据后

`post_event`：原事件候选过期，不得继续以事件前赔率追价。

### 10.7 报价失败

任一腿陈旧、provider 不一致或 cross-leg skew 超限：候选只能作为明确报价 near-miss，
不能使用 Mid 补齐。

### 10.8 因果性

- prior close 的 source time 必须早于 decision；
- next event 信息必须在 decision 时已可用；
- 不读取 CPI 实际值；
- 不读取当前事件日未来价格；
- `available_at <= decision_at`。

### 10.9 非事件回归

既有 2026-08-05、08-06、08-07、08-08 冻结回放在无匹配 next event 时，现有
Directional Vertical / Butterfly / NO_TRADE 决策必须不变。

### 10.10 结果回填

盘后 backfill 必须为 event candidate 生成 terminal label；缺价格时显式 censored，
不能被静默排除。

---

## 11. 统计与升门

v4.0 只收集研究样本，不设日历等待承诺，也不因少数成功案例升门。

事件候选转为 `manual_authority_eligible=true` 至少需要：

1. 总机会样本 ≥100；
2. 独立事件日 ≥40；
3. 时间顺序 OOS 样本 ≥30；
4. OOS 平均 terminal net PnL > 0；
5. OOS Profit Factor ≥1.10；
6. 双倍费用压力下不显著为负；
7. required probability 与实际频率校准误差明确报告；
8. CPI、FOMC、NFP 分开报告，不用混合结果掩盖单类无效；
9. 删除最好三笔后，总期望仍不为负；
10. 参数在 OOS 开始前冻结。

未达到时，系统继续输出 NO_TRADE + nearest research candidate。

---

## 12. 实施顺序

### Phase A：候选发现

修改 facts、factory、ranker、desk view：

```text
prior close
→ 相邻 5 点 Vertical
→ exact quote
→ odds proxy
→ NO_TRADE nearest research candidate
```

这是第一个完整垂直切片。

### Phase B：终值回填

扩展现有 backfill：

```text
research candidate
→ terminal SPX
→ vertical payoff
→ terminal net PnL
→ censored audit
```

### Phase C：研究报告

复用现有 policy/replay 报告路径，按 event type、direction 和 price bucket 输出结果。

只有 Phase A/B 验收通过后，才讨论 Physical 模型。

---

## 13. 后续能力的边界

### 13.1 Physical Event Model

不在 v4.0 内实现。

未来若样本足够，优先在现有 `physical_followthrough.py` 中复用 session-weighted、
Beta shrinkage 和 causal cutoff，再增加事件专属 terminal-above/below 估计。只有现有
owner 无法保持清晰时，才讨论新文件。

### 13.2 Credit Vertical 与 Iron Condor

不在 v4.0 内设计或实现。

原因不是永远不做，而是当前系统仍是 Debit 开仓、Debit outcome 和 Long Premium
ManagementPolicy。为了事件阈值发现而提前重构全套 Credit 会扩大风险和验证面。

卖方策略需要单独回答：

- conservative entry credit；
- conservative buyback ask；
- first-touch；
- touch recovery / continuation；
- short-premium management；
- settlement 与真实管理规则差异。

等 v4.0 证明“命题发现 → 报价 → 持久化 → 回填”链路可靠后，再提交独立 Change
Brief。届时仍先扩展现有 payoff、ranker、outcome 与 backfill；不会默认新增新模块。

---

## 14. 复杂度预算

| 项 | 上限 |
|---|---:|
| 新生产文件 | 0 |
| 修改生产文件 | 4 |
| 修改研究文件 | 1 |
| 新研究脚本 | 0 |
| 新依赖 | 0 |
| 新配置键 | 0（冻结代码常量） |
| 新 service/timer/queue | 0 |
| 新数据库/表 | 0 |
| 新 Rust contract | 0 |
| 新 decision schema | 0 |
| 预计净生产 LOC | ≤250 |
| 预计测试 LOC | ≤250 |

超过预算必须重新评审，不允许“顺手”扩张。

---

## 15. 需要批准的事项

1. v4.0 只增加 `EVENT_SETTLEMENT_THRESHOLD`；
2. 第一版阈值来源只用 `PRIOR_CLOSE`；
3. 只枚举相邻 5 点 Debit Vertical；
4. 双向扫描，不接受外部叙事直接选方向；
5. 固定 `manual_authority_eligible=false`；
6. 继续使用 NO_TRADE + nearest candidate，不新增决策状态；
7. 0 新生产文件、0 新 schema、0 新存储；
8. Credit Vertical 与 Iron Condor 延后到独立 Change Brief；
9. 自动下单继续禁止。

批准后只实施 Phase A，再提交验收结果；Phase B 不与 Phase A 混在同一提交。