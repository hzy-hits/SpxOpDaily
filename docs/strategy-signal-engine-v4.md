# SPX Spark 策略信号引擎 v4.0：观点到事件结算价差

状态：**已实现于 Draft PR，尚未合并或部署。**  
适用仓库：`hzy-hits/SpxOpDaily`  
基线：`e279ba6029ccba2dbbe4d2b98ceb0f688f43b487`  
自动下单：**继续禁止**

v4.0 不重建一套新的策略架构。它只补上当前系统已经暴露出的一个具体缺口：

> 系统可以解释别人给出的 `7730/7735 Call Debit Spread`，但此前不能主动把
> “事件日收在昨日收盘上方/下方”这种观点，转换为可验证的终值命题，再用现有
> SPXW Vertical 工厂寻找可执行表达。

本阶段只实现：

```text
现有 prior close
  → CLOSE_ABOVE / CLOSE_BELOW 两个竞争观点
  → terminal_above / terminal_below 命题
  → 昨收附近相邻 5 点 Call / Put Debit Vertical
  → 现有 BBO、Payoff、Ranker 与 Manual Candidate
```

它是账户无关的市场策略能力：不读取净值、Buying Power、持仓或盈亏，不决定数量。

---

## 1. 用户可见目标

在已登记的高影响事件发布前，系统主动生成并比较类似：

```text
观点：SPX 今日结算高于昨日收盘 7728.20
命题：terminal_above(7728.20, today_close)
表达：7730/7735 Call Debit Vertical
可执行 Debit：2.50
宽度：5.00
Debit / Width：50.0%
盈亏平衡：7732.50
相对观点阈值：还需额外上涨 4.30 点
证据：thesis_driven_unvalidated
```

候选可以成为 `MANUAL_CANDIDATE`，但必须明确：

- `Debit / Width` 是市场赔率代理，不是现实胜率；
- 当前尚未估计 CPI/FOMC/NFP 条件下的现实概率；
- `automatic_ordering=false`；
- 事件跳空时普通止损不保证成交；
- 事件公布后，原事件前观点立即过期。

---

## 2. Change Brief

### 2.1 用户可见变化

新增一种 setup：

```text
EVENT_SETTLEMENT_THRESHOLD
```

它仍然使用已有结构类型：

```text
CALL_DEBIT_VERTICAL
PUT_DEBIT_VERTICAL
```

没有增加新的 Payoff 类型、决策状态或账户层。

### 2.2 复用的现有 owner

```text
application/order_map/service.py
  已提供 day_move.prior_close、macro_event、front expiry、统一 SPX 坐标

macro_event_clock.py
  已提供 active_event / next_event / release_at / impact / pre_event

application/order_map/candidate_factory.py
  已提供 exact-leg 查询、provider fallback、报价新鲜度、跨腿 skew、候选 ID

analytics/options/strategy_payoff.py
  已提供 conservative_vertical_bbo、vertical_economics、vertical_payoff

application/order_map/strategy_ranker.py
  已提供 quote/TTL hard gates、near-miss、advisory edge、排序

application/order_map/strategy_select.py
  已提供唯一 strategy_decision 权限出口

application/order_map/delivery.py
  已提供现有 Manual Candidate 通知通道

infrastructure/operational_db.py
  已提供 decision / frozen legs / outcome 持久化
```

### 2.3 不新增

```text
生产文件             0
Schema               0
服务 / timer / queue  0
数据库 / 表           0
依赖                  0
Rust contract         0
账户读取              0
自动下单              0
```

### 2.4 明确延期

- Credit Vertical；
- Iron Condor；
- First-Touch 模型；
- Short Premium ManagementPolicy；
- CPI 专属现实概率模型；
- 自由文本或持久化的人工观点输入；
- SPY、TLT 等非 SPXW 执行候选。

这些能力不得因“以后可能需要”而预先搭建抽象。

---

## 3. 观点、命题与结构

### 3.1 观点

v4.0 内置两个竞争观点模板：

```text
CLOSE_ABOVE_PRIOR_CLOSE
CLOSE_BELOW_PRIOR_CLOSE
```

它们回答的是：

```text
今天到期结算相对昨日收盘在哪一侧？
```

v4.0 不声称这两个观点中任意一个更可能发生。它先把双方都表达出来，再由现有
结构分数比较可执行赔率与摩擦。

### 3.2 命题

复用现有 `ProbabilityEventDefinition` 语义：

```yaml
kind: terminal_above | terminal_below
target_at: 当前 front expiry 的 SPX session close
lower_level: prior_close   # terminal_above
upper_level: prior_close   # terminal_below
```

每个命题都带：

- 稳定 event ID；
- 宏观事件 ID、名称和发布时间；
- 到期结算时点；
- 昨日收盘阈值；
- `thesis_driven_unvalidated` 证据状态。

不新增 `StrategyProposition` schema。

### 3.3 结构表达

若昨日收盘为 7728.20，最近 5 点 strike 为 7730，则枚举：

```text
Call 7725/7730
Call 7730/7735
Put  7735/7730
Put  7730/7725
```

四组均为定义风险 Debit Vertical。系统不枚举单 Call、单 Put、Credit 或 Iron Condor。

---

## 4. 事件适用条件

候选仅在以下条件同时满足时生成：

1. `day_move.prior_close` 存在且为正；
2. front expiry 存在；
3. `active_event` 或 `next_event` 存在；
4. 事件 `impact ∈ {high, critical}`；
5. 发布时间晚于当前时刻；
6. 发布时间不晚于该 expiry 的 SPX session close；
7. 两腿 exact quote 可用且满足现有 provider/freshness/skew 规则。

事件发布后，候选不再生成。

### 4.1 Macro Gate 的最小调整

此前 `macro_entry_not_authorized` 在候选枚举前全局终止策略链，导致专门跨越事件的
候选永远无法出现。

v4.0 将该原因从全局 Gate 下移到候选级 Gate：

```text
EVENT_SETTLEMENT_THRESHOLD + event_spans_release=true
  → 可以跨事件评估

普通 Trend / Pullback / Butterfly
  → 继续受原 macro entry gate 阻止
```

没有增加新的权限状态机。

---

## 5. 报价和经济学

### 5.1 Exact-leg 报价

继续复用现有 `_rth_option_legs()` 的实际行为：

- Schwab 优先；
- IBKR fallback；
- 同一 provider；
- 每腿 bid/ask 必须完整；
- 报价年龄不超过 `quote_max_age_seconds`；
- 跨腿时间差不超过 `quote_max_skew_seconds`。

尽管函数名含 `rth`，它的报价校验本身可在 GTH 使用 IBKR。若后续确认需要重命名，
应原地改名，不复制第二套逻辑。

### 5.2 Conservative BBO

继续使用：

```text
买入腿按 Ask
卖出腿按 Bid
```

禁止用多腿 Mid 冒充可执行 Debit。

### 5.3 已有经济学字段

`vertical_economics()` 已计算：

```text
width_points
max_loss_points
max_gain_points
breakeven_spx
debit_fraction_of_width
```

v4.0 只增加展示字段：

```text
market_odds_proxy = debit_fraction_of_width
breakeven_gap_points = breakeven 与 prior-close 命题阈值之间的额外距离
```

其中：

```text
Debit / Width = 50%
```

只能解释为窄 Vertical 的市场赔率代理，不能写成“50% 现实胜率”。

---

## 6. Gate 与权限

### 6.1 Event Vertical 专属 Hard Gates

它不套用盘中 Trend Vertical 的 ATR、target room、stop distance 和 late-chase Gate。
原因是事件结算观点没有同一套盘中 target/stop 几何。

专属硬门只检查：

- 两腿完整；
- BBO ready；
- TTL 未过期；
- Vertical payoff 合法；
- `0 < Debit / Width < 1`；
- 命题方向、阈值和 target time 一致；
- 候选明确跨越对应事件；
- `automatic_ordering=false`。

### 6.2 人工权限

满足确定性硬门后，候选可以成为：

```text
MANUAL_CANDIDATE
```

同时必须携带：

```text
edge_status = thesis_driven_unvalidated
model_p = null
advisory = physical_probability_not_estimated
```

“未校准”不再是禁止展示人工候选的理由；它是风险披露和排序信息。

### 6.3 排序

继续复用现有 `selection_score`：

```text
max_gain / max_loss
- BBO friction penalty
```

这意味着 v4.0 当前选的是“赔率与摩擦最优的观点表达”，不是“统计上最可能发生的
方向”。这是第一版的已知限制，卡片必须明确 P 未估计。

---

## 7. 输出字段

候选沿用现有 Candidate 字段，并增加：

```yaml
setup_kind: EVENT_SETTLEMENT_THRESHOLD
setup_variant: CLOSE_ABOVE_PRIOR_CLOSE | CLOSE_BELOW_PRIOR_CLOSE
manual_authority_eligible: true
event_spans_release: true

probability_event:
  event_id: ...
  kind: terminal_above | terminal_below
  target_at: ...
  lower_level: ...
  upper_level: ...

view:
  source: PRIOR_CLOSE
  statement: ...
  threshold_level: ...
  target_at: ...
  macro_event_id: ...
  macro_event_name: ...
  release_at: ...
  market_odds_proxy: ...
  breakeven_gap_points: ...
  evidence_status: thesis_driven_unvalidated
```

不修改 `strategy_decision.v2` schema。

---

## 8. 人类候选卡

Event Settlement 卡片单独显示：

```text
观点
命题及结算时点
具体两腿合约与 conservative BBO
Debit / Width
盈亏平衡点
相对观点阈值仍需移动多少点
最大亏损
事件跳空风险
事件发布后的过期语义
P 未估计和 edge 未验证状态
```

不沿用普通 Trend 卡片中的 `SPX invalidation` 文案，因为事件交易没有可靠的盘前
价格止损上限。

---

## 9. 测试与验收

新增测试覆盖：

1. 昨收 7728.20 时枚举四个相邻 5 点 Call/Put Vertical；
2. 7730/7735 Call 的可执行 Debit=2.50 时，赔率代理为 50%；
3. Event candidate 在 `pre_event` 下不被全局 macro gate 误杀；
4. 普通 Trend candidate 在相同 pre-event 环境仍被阻止；
5. Event candidate 不要求 ATR、target room 或 stop geometry；
6. 概率不可用只进入 advisory，不否决人工候选；
7. 事件发布后候选不再生成；
8. Ruff 和 Python 全量测试通过。

Rust CI 当前存在与本 PR 无关的既有 Clippy 错误时，应单独记录，不得为了本功能修改
Rust workspace。

---

## 10. 已知限制

### 10.1 尚无显式人工观点输入

v4.0 生成的是两个竞争观点模板，而不是读取用户的一条自由观点。因此它能证明：

```text
view template → terminal proposition → executable spread
```

但尚未实现：

```text
用户自由文本 → 结构化 view
```

后续如有需要，应优先复用现有 `ConvexityIdeaRadar` / `DecisionGuidance` 或一个已有
payload 字段，不应先创建新服务。

### 10.2 尚无现实概率

系统不会因为“连续两日收跌”或“Twitter 共识”自动把上涨概率改成 60%。当前只显示
市场赔率和 `P=未估计`。

### 10.3 两个方向都可能有候选

最终第一名由现有确定性结构分数选出。因此第一名表示“当前价格表达更有利”，不等于
系统证明了该方向更可能发生。

### 10.4 没有账户适配

每张卡只报告一组 SPXW 的最大收益/最大亏损。数量和账户风险仍由外部决定。

---

## 11. 复杂度结果

```text
新增生产文件          0
修改生产文件          candidate_factory.py
                      strategy_ranker.py
                      strategy_select.py
                      delivery.py
新增测试文件          test_event_settlement_vertical.py
新增文档              strategy-signal-engine-v4.md
新依赖                0
新配置键              0
新 service/timer      0
新数据库/表            0
Rust 改动              0
自动下单变化            0
```

---

## 12. 后续是否扩张的判据

在本垂直切片完成实际 GTH/RTH 验收前，不增加 Credit、Iron Condor 或新概率模型。

下一阶段只能根据具体证据选择：

1. 若两个竞争观点无法表达用户真实判断，再补一个最小结构化 view 输入；
2. 若赔率候选经常出现但没有方向区分力，再复用历史终值数据估计条件概率；
3. 若 short-premium 确实需要独立语义，再单独写 Credit/IC Change Brief。

不以“未来可能需要”为理由预建抽象。
