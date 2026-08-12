# SPX Spark 策略信号引擎 v4.0：观点到事件结算价差

状态：**已实现于 Draft PR，尚未合并或部署。**  
适用仓库：`hzy-hits/SpxOpDaily`  
基线：`e279ba6029ccba2dbbe4d2b98ceb0f688f43b487`  
自动下单：**继续禁止**

v4.0 不重建新的命题引擎、概率引擎或账户系统。它只补一个已经观察到的能力缺口：

> 系统能够解释外部给出的 `7730/7735 Call Debit Spread`，但此前不能主动把
> “事件日收在昨日收盘上方/下方”转换为终值命题，再使用现有 SPXW Vertical
> 能力寻找可执行表达。

当前端到端链路是：

```text
现有 day_move.prior_close
  → CLOSE_ABOVE / CLOSE_BELOW 两个竞争观点
  → terminal_above / terminal_below 命题
  → 昨收附近相邻 5 点 Call / Put Debit Vertical
  → 现有 exact-leg BBO、Payoff、Ranker、strategy_decision
  → MANUAL_CANDIDATE 或 NO_TRADE
```

该能力完全账户无关：不读取净值、Buying Power、持仓或盈亏，不决定合约数量。

---

## 1. 用户可见结果

在已登记的高影响事件发布前，系统可以主动生成并比较：

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

候选满足确定性门控后可以成为 `MANUAL_CANDIDATE`，但卡片必须明确：

- `Debit / Width` 是市场赔率代理，不是现实胜率；
- CPI/FOMC/NFP 条件下的现实概率尚未估计；
- `automatic_ordering=false`；
- 事件跳空时普通止损不保证成交；
- 事件发布后，原事件前观点立即过期。

---

## 2. Change Brief

### 2.1 新增的业务语义

只新增一个 setup：

```text
EVENT_SETTLEMENT_THRESHOLD
```

它仍复用已有 Payoff 类型：

```text
CALL_DEBIT_VERTICAL
PUT_DEBIT_VERTICAL
```

没有新增决策状态、账户层或订单状态机。

### 2.2 现有能力复用

| 现有 owner | 复用内容 |
|---|---|
| `application/order_map/service.py` | `day_move.prior_close`、macro event、front expiry、统一 SPX 坐标 |
| `macro_event_clock.py` | `active_event`、`next_event`、`release_at`、`impact`、`pre_event` |
| `candidate_factory.py` | exact-leg 选择、Schwab→IBKR fallback、freshness、cross-leg skew、Vertical 构造、ID |
| `analytics/options/strategy_payoff.py` | conservative BBO、`vertical_economics`、`vertical_payoff` |
| `strategy_ranker.py` | quote/TTL hard gates、near-miss、Edge advisory、排序 |
| `strategy_select.py` | 唯一 `strategy_decision` 权限出口 |
| `delivery.py` | 现有 Manual Candidate 通知 lane |
| `operational_db.py` | decision、frozen legs、outcomes 持久化 |

### 2.3 为什么新增一个生产文件

最初将事件组合逻辑直接放入 `candidate_factory.py`，使该文件超过仓库强制的
1,000 行生产模块预算。继续压入同一文件会把“通用候选工厂”和“事件观点组合”混成
一个 owner。

因此新增同层的小模块：

```text
application/order_map/event_settlement_vertical.py
```

它只负责：

```text
prior-close + macro event
  → event view
  → 调用 candidate_factory 现有私有核构造 Vertical
```

它不复制报价、Payoff、provider fallback 或 ID 基础能力。`candidate_factory.py` 已恢复
为 master 版本和原有行数。

### 2.4 复杂度边界

```text
新增生产文件          1（聚焦的组合模块）
新 Schema             0
新 service/timer      0
新 queue              0
新数据库/表            0
新依赖                0
新 Rust contract       0
账户读取               0
自动下单变化            0
```

### 2.5 明确延期

- 自由文本或持久化的人工观点输入；
- CPI/FOMC/NFP 专属现实概率模型；
- Credit Vertical、Iron Condor、First-Touch；
- Short Premium ManagementPolicy；
- SPY、TLT 等非 SPXW 执行候选；
- 账户和组合风险。

---

## 3. 观点、命题与结构

### 3.1 观点模板

v4.0 内置两个竞争观点：

```text
CLOSE_ABOVE_PRIOR_CLOSE
CLOSE_BELOW_PRIOR_CLOSE
```

它们回答：

```text
当前 front expiry 的 SPX 结算相对昨日收盘在哪一侧？
```

系统不声称其中任意方向更可能发生。第一版先比较双方可执行结构的赔率与摩擦。

### 3.2 命题

复用现有 `ProbabilityEventDefinition` 语义，不新增 Proposition schema：

```yaml
kind: terminal_above | terminal_below
target_at: 当前 front expiry 的 SPX session close
lower_level: prior_close   # terminal_above
upper_level: prior_close   # terminal_below
```

每个候选附着：

- 稳定 event ID；
- 宏观事件 ID、名称、发布时间；
- 到期结算时点；
- 昨日收盘阈值；
- `thesis_driven_unvalidated` 证据状态。

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
7. 两腿 exact quote 满足现有 provider/freshness/skew 规则。

事件发布后，候选不再生成。

### 4.1 Macro Gate

此前 `macro_entry_not_authorized` 在候选枚举前全局终止策略链，使专门跨越事件的候选
无法出现。

v4.0 仅将这一原因下移到候选级：

```text
EVENT_SETTLEMENT_THRESHOLD + event_spans_release=true
  → 允许跨事件评估

普通 Trend / Pullback / Butterfly
  → 在显式 pre_event 下继续被阻止

facts 未携带 event
  → 保留旧候选行为，不新增隐式阻断
```

没有新增 Permission Matrix 或事件状态机。

---

## 5. 报价与经济学

### 5.1 Exact-leg

事件模块调用 `candidate_factory.py` 已有内核：

- Schwab 优先；
- IBKR fallback；
- 同一 provider；
- 每腿 bid/ask 完整；
- 报价年龄不超过 `quote_max_age_seconds`；
- 跨腿时间差不超过 `quote_max_skew_seconds`。

虽然现有 helper 名称包含 `rth`，其校验逻辑可在 GTH 使用 IBKR。第一版不复制第二套
GTH exact-leg 实现。

### 5.2 Conservative BBO

继续使用：

```text
买入腿按 Ask
卖出腿按 Bid
```

禁止用多腿 Mid 冒充可执行 Debit。

### 5.3 赔率

现有 `vertical_economics()` 已输出：

```text
width_points
max_loss_points
max_gain_points
breakeven_spx
debit_fraction_of_width
```

事件候选只增加：

```text
market_odds_proxy = debit_fraction_of_width
breakeven_gap_points = breakeven 相对 prior-close 命题阈值的额外距离
```

`Debit / Width = 50%` 只能称为市场赔率代理。

---

## 6. Gate 与人工权限

### 6.1 Event Vertical 专属硬门

事件价差不套用盘中 Trend Vertical 的 ATR、target room、stop distance 和 late-chase
规则，因为结算观点不存在同一套盘中 target/stop 几何。

专属硬门检查：

- 两腿完整；
- BBO ready；
- quote/opportunity TTL 未过期；
- Vertical payoff 合法；
- `0 < Debit / Width <= 0.50`；
- 命题方向、阈值和 target time 一致；
- 候选明确跨越对应事件；
- `automatic_ordering=false`。

高于 50% 翼宽的候选仍保留为 near-miss，并明确显示：

```text
event_settlement_debit_fraction_exceeded
```

### 6.2 人工权限

确定性硬门通过后，候选可以成为：

```text
MANUAL_CANDIDATE
```

并必须携带：

```text
edge_status = thesis_driven_unvalidated
model_p = null
advisory = physical_probability_not_estimated
```

未校准不是隐藏候选的理由；它是风险披露。

### 6.3 排序语义

继续复用现有 `selection_score`：

```text
max_gain / max_loss
- BBO friction penalty
```

因此第一名表示“当前价格下赔率与摩擦更有利的表达”，不表示系统证明该方向更可能。

---

## 7. 输出与候选卡

候选增加：

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

Event Candidate 卡片单独显示：

- 观点与 prior-close 阈值；
- terminal 命题和结算时点；
- 两腿合约与 conservative BBO；
- Debit/Width；
- 盈亏平衡点；
- 相对观点阈值仍需移动多少点；
- 最大亏损；
- 事件跳空与过期语义；
- P 未估计、Edge 未验证。

---

## 8. 测试与验收

新增测试覆盖：

1. 昨收 7728.20 时枚举四个相邻 5 点 Call/Put Vertical；
2. 7730/7735 Call 的可执行 Debit=2.50 时，赔率代理为 50%；
3. Event candidate 在显式 `pre_event` 下不被 macro gate 误杀；
4. 普通 Trend candidate 在相同 pre-event 环境仍被阻止；
5. facts 没有 event 字段时，原有 Ranker 候选行为不变；
6. Event candidate 不要求 ATR、target room 或 stop geometry；
7. 概率不可用只进入 advisory，不否决人工候选；
8. 事件发布后候选不再生成；
9. Ruff 与 Python 全量测试。

Rust CI 中若仍存在与本 PR 无关的既有 Clippy 错误，应单独记录，不为本功能修改 Rust。

---

## 9. 已知限制

### 9.1 尚无显式人工观点输入

当前生成两个竞争模板，而不是读取自由文本：

```text
view template → terminal proposition → executable spread
```

尚未实现：

```text
用户自由文本 → 结构化 view
```

下一阶段应优先复用 `ConvexityIdeaRadar`、`DecisionGuidance` 或已有 payload 字段，而不是
新建服务。

### 9.2 尚无现实概率

系统不会因为“两日连跌”或 Twitter 共识自动把上涨概率改成 60%。当前只显示市场赔率
和 `P=未估计`。

### 9.3 两个方向均被枚举

最终第一名由结构赔率与摩擦决定，不等于方向预测。

### 9.4 无账户适配

每张卡只报告一组 SPXW 的最大收益/最大亏损。数量和账户风险由外部决定。

---

## 10. 复杂度结果

```text
新增生产文件          event_settlement_vertical.py（聚焦事件组合）
恢复原文件            candidate_factory.py 回到 master 实现和原有行数
修改生产文件          strategy_ranker.py
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

## 11. 后续扩张判据

在该垂直切片完成实际 GTH/RTH 验收前，不增加 Credit、Iron Condor 或新概率模型。

后续只能根据证据选择：

1. 若两个竞争模板不能表达用户真实判断，补一个最小结构化 view 输入；
2. 若赔率候选经常出现但没有方向区分力，复用历史终值数据估计条件概率；
3. 若 short-premium 确实需要独立语义，再单独提交 Credit/IC Change Brief。

不以“未来可能需要”为理由预建抽象。
