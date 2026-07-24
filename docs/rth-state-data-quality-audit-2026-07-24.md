# RTH 状态机与数据链路质量审计（2026-07-24）

> 状态：本文是修复前的生产证据快照。文中确认的 lifecycle bug 已由
> `b7e1901` 修复；本次改动又补上独立 ES 5m bar、RTH 八变量 extractor、
> 同刻 range 基线回填入口、严格 NBBO coverage 和状态/期权 overlay 解耦。
> 当前合同见 `docs/rth-five-minute-market-state-v1.md`，历史 replay 结果以
> 本次新生成的报告为准。

## 结论

最近周一至周三（2026-07-20～2026-07-22）RTH “完全没有信号”的主因不是 ES/SPY 断流，也不是通知发送失败，而是一个已经确认并于周四开盘前修复的状态机生命周期缺陷：

- 当新的 OI/GEX 结构进入两帧确认期时，旧实现把 `structure_change_pending` 当成坏数据，并立即把正在运行的 level path 置为 `INVALIDATED`。
- 这三天每个 RTH 约 6,200 个状态采样中，94.77%～99.52% 都停留在 `invalidated`，所以无法形成 `CONFIRMED`。
- `b7e1901`（2026-07-23 20:19 北京时间，周四 RTH 开盘前）修复为：候选结构只阻止新的 arm，已经基于冻结稳定结构启动的生命周期继续运行。
- 修复后的 2026-07-23 RTH 立即从 0 个恢复为 7 个唯一 `CONFIRMED` formal signals；7 个都进入下游 gate，但全部被风险/方向/空间 gate 阻止，因此仍然没有可执行 trade intent。

所以：

1. 7 月 20～22 日的原始行情仍可使用，但这些日期已有的派生 signal/phase 日志不能直接拿来调阈值。
2. 必须用当前修复后的状态机从原始 quote lake 重放这些日期，再评估参数。
3. 2026-07-23 是当前唯一完整经过生命周期修复的已结束 RTH，但样本只有一天，且 15:30～16:00 ET 的历史 0DTE 链存在采集缺口，仍不足以直接定参。
4. 截至本审计 2026-07-24 19:15 北京时间（07:15 ET），当天 RTH 尚未开始；此时不能据此判断“今天 RTH 没信号”。

## 审计范围与证据源

本次只读检查了：

- 原始 quote lake：`/srv/data/spx-spark/data/lake/quotes/schema=v1/`
- level health：`/srv/data/spx-spark/data/features/level_decision_health/`
- level transitions：`/srv/data/spx-spark/data/features/level_decision_audit/`
- confirmed gate：`/srv/data/spx-spark/data/features/confirmed_gate_results/`
- trade intents：`/srv/data/spx-spark/data/features/trade_intents/`
- IV surface：`/srv/data/spx-spark/data/features/iv_surface/`
- 15 分钟报告审计：`/srv/data/spx-spark/data/audit/order_map_pricing/`
- durable notification outbox：`/srv/data/spx-spark/data/ledger/notification_delivery_outbox.sqlite`
- 当前 `latest` 文件、systemd user services 和 journal
- 当前 `master` 代码及修复提交历史

审计没有修改生产集成代码、运行时配置或历史数据。

## 逐日 RTH 证据

### 状态机结果

下表时间范围均为 09:30～16:00 ET。`formal` 是唯一 transition 数量；health 中同一个 confirmed 状态会每约 5 秒重复采样，不能当成独立信号。

| 日期 | health 样本 | quality_ok | `structure_change_pending` | `invalidated` 样本 | 唯一 formal transitions | confirmed gate |
|---|---:|---:|---:|---:|---:|---|
| 2026-07-20 | 6,221 | 2,081 | 4,140 | 6,191 | 0 | 无事件 |
| 2026-07-21 | 6,215 | 3,919 | 2,296 | 5,955 | 0 | 0 |
| 2026-07-22 | 6,209 | 4,809 | 1,399 | 5,884 | 0 | 0 |
| 2026-07-23 | 6,210 | 6,210 | 0 | 9 | 7 | 7/7 blocked |

7 月 20～22 日的 transition 审计进一步显示：

- 2026-07-20：两个事件进入 `invalidated`，原因均为 `structure_change_pending`。
- 2026-07-21：三个事件进入 `invalidated`，原因均为 `structure_change_pending`。
- 2026-07-22：四个事件进入 `invalidated`，原因均为 `structure_change_pending`。
- 这些并不是报价解析失败；当时稳定墙位仍为 `stable_15m_oi_gex`，只是结构候选的确认状态被错误地当成了 lifecycle 失效条件。

### 已修复旧 bug 的代码证据

提交 `b7e1901fa3868aa95b5dc2b312e6a5dedc3e8d63` 的标题就是 `fix: restore RTH signal lifecycle`。其关键变化是：

- 删除 `structure_change_pending` 对活动 path 的立即 `INVALIDATED`。
- 增加 `arm_allowed`/`arm_block_reason`。
- 结构候选待确认时，仅在 phase 为 `FAR`、`INVALIDATED` 或 `EXPIRED` 时阻止新 arm。
- 已在 `APPROACHING`、`TESTING`、`BREAK_PENDING`、`RETEST` 等阶段的事件继续沿冻结的 stable structure 运行。

当前相关实现位于：

- `src/spx_spark/application/order_map/level_decision_shadow.py`
- `src/spx_spark/application/order_map/level_decision_machine.py`

针对 level decision 与 market calendar 的定向测试结果为：

```text
56 passed
```

### 修复后 2026-07-23 的 7 个 RTH formal signals

| ET | 方向/类型 | level | 下游结果 |
|---|---|---:|---|
| 09:46:17 | down breakout / flip_low | 7445 | blocked |
| 12:16:23 | down breakout / flip_low | 7415 | blocked |
| 12:19:13 | down breakout / flip_low | 7415 | blocked |
| 13:15:10 | down breakout / flip_low | 7415 | blocked |
| 15:00:16 | up breakout / call_wall | 7390 | blocked |
| 15:17:51 | up breakout / call_wall | 7390 | blocked |
| 15:35:06 | down fade / call_wall | 7400 | blocked |

7 个 confirmed gate 的主要阻止原因：

| 原因 | 事件数 |
|---|---:|
| `remaining_target_room_insufficient` | 5 |
| `breakout_filter_not_supported` | 4 |
| `es_return_1m_points_opposes_direction` | 4 |
| `es_return_5m_points_opposes_direction` | 3 |
| `es_spy_confirmation_opposes_direction` | 3 |
| `price_volume_not_directionally_aligned` | 3 |
| `remaining_reward_risk_insufficient` | 3 |
| `regime_direction_conflict` | 2 |
| `trigger_structure_drift` | 2 |

最后一个 15:35 ET 信号还被以下数据条件阻止：

- `option_structure_not_ready`
- `option_l1_not_ready`
- ES anchor stale
- `expected_move_unavailable`

因此 2026-07-23 的正确表述是：“有 7 个 level-path 市场信号，但没有一个通过 executable trade gate”，不是“RTH 完全没有信号”。

RTH 共评估 4,657 条 trade intent：

- `observing`：4,545
- `blocked`：112
- `ready/actionable`：0

## 上游采集与解析：不是周一至周三零信号的主因

### ES/SPY/RSP RTH 覆盖

Schwab 在四个已审计 RTH 都完整覆盖 ES、SPY 和 RSP：

| 日期 | Schwab ES 分钟 | Schwab SPY 分钟 | Schwab RSP 分钟 | IBKR ES 分钟 |
|---|---:|---:|---:|---:|
| 2026-07-20 | 390/390 | 390/390 | 390/390 | 160/390 |
| 2026-07-21 | 390/390 | 390/390 | 390/390 | 290/390 |
| 2026-07-22 | 390/390 | 390/390 | 390/390 | 353/390 |
| 2026-07-23 | 390/390 | 390/390 | 390/390 | 340/390 |

Schwab ES/SPY 的行级 `quality=live` 为 100%。source age p95：

| 日期 | ES p95 | SPY p95 |
|---|---:|---:|
| 2026-07-20 | 0.203s | 0.398s |
| 2026-07-21 | 0.316s | 0.581s |
| 2026-07-22 | 0.301s | 0.507s |
| 2026-07-23 | 0.194s | 0.362s |

IBKR 确实存在断续，但 Schwab 冗余完整覆盖了 ES/SPY。因此把周一至周三的零 formal signal 归因为 ES/SPY 上游缺数不成立。

### Sector breadth

11 个 sector ETF 的 Schwab RTH 分钟覆盖：

| 日期 | 每个 sector ETF 的分钟数 |
|---|---:|
| 2026-07-20 | 277 |
| 2026-07-21 | 385 |
| 2026-07-22 | 389 |
| 2026-07-23 | 389 |

但当前 breadth 存在两个结构性问题：

1. `spx_sector_breadth()` 计算的是“相对昨收上涨/下跌的 sector 数量”，不是新 5m scorer 所要求的 `% above VWAP`。
2. breadth 只进入 `market_context`/human focus，没有进入 `minute_market_frame`、`decision_context` 或 15 分钟报告。

2026-07-23 的 389 个可观测分钟全部被现有公式分类为 `mixed_tactical`。这不是 sector quote 缺失，而是指标定义和接线尚未完成；不能假设 breadth 已参与当前 gate。

### ES/SPY 5m bar、RTH VWAP、开盘区间和同时间 range

当前生产 `market.py` 每分钟只保留一个 normalized quote sample，并从采样点计算 return；它没有构建 ES/SPY 的真实 1m/5m OHLCV bar。因此以下 RTH 特征还没有完整生产来源：

- RTH opening range high/low
- 连续两个 5m close 的 ORH/ORL 确认
- 同时刻 range 与过去 20 个 session 中位数之比
- 5m market structure（HH/HL、LH/LL）
- `% breadth above VWAP`

`market_state_5m.py` 当前是纯 scorer，要求八个已经派生好的输入，但生产代码没有调用点。

现有报告中的 `VWAP` 也不是 RTH VWAP：

- 它使用整个 Globex `session_id` 的 ES 累计 volume delta，从隔夜开始累计。
- 它是每分钟 quote price × 累计 volume delta 的近似值，不是 provider 的 trade/OHLCV VWAP。
- 2026-07-23 09:30 ET 开盘报告显示 ES 距 “VWAP -45.4 点”；真正的 RTH VWAP 在开盘第一分钟应接近开盘成交区间，这直接证明当前字段是全 Globex VWAP。
- 该字段又参与 trend score、mean-reversion score 和 breakout impulse，所以在 RTH 报告/模型中必须明确拆成 `globex_vwap` 与 `rth_vwap`，不能继续共用一个模糊的 `VWAP`。

同时间 volume pace baseline 当前只有 8 个历史 session，而配置要求 20 个，因此 percentile 正确地保持 `null`。该 percentile 目前没有进入 trade gate；这不是零信号主因，但在完成 20-session backfill 前不能拿它调参。

### SPX bar builder 的静默失效

Steven bar 路径当前也没有产生 bar：

- `latest/spx_bars_1m.json`：0 bars
- `latest/spx_bars_5m.json`：0 bars
- 2026-07-23 lake 的 1m/5m JSONL：均为 0 行

原因是 `evaluate_steven_cycle()` 在没有外部 `bar_builder` 时每轮新建 `SpxBarBuilder`，只恢复 closed bars，然后只 ingest 当前一个 sample。open bar 没有持久化到下一轮，所以永远没有下一分钟 sample 来关闭它。任务日志仍显示 `ok=true`，属于静默数据质量故障。

这不是 level-path 7 月 20～22 日零信号的直接原因，但任何依赖 Steven bars 的回测都应在修复/重建 bars 前视为不可用。

## SPXW NBBO、IV 和墙位

### RTH 原始链

2026-07-23 09:30～16:00 ET：

| Provider / 到期日 | contracts | 最后收到时间 | 有效 NBBO | IV 非空 | OI 非空 |
|---|---:|---|---:|---:|---:|
| IBKR / 20260723 | 188 | 15:30:00 ET | 86.75% | 99.74% | 79.59% |
| Schwab / 20260723 | 208 | 15:30:05 ET | 100.00% | 75.71% | 100.00% |
| IBKR / 20260724 | 170 | 16:00:08 ET | 98.54% | 98.39% | 94.18% |
| Schwab / 20260724 | 170 | 16:00:09 ET | 100.00% | 84.69% | 100.00% |

同日 78 个 RTH IV-surface 5m snapshots 中：

- 72 个有 ATM IV，但全部标记为 `wide_quote_degraded`。
- 6 个从 15:32 ET 起为 `missing_atm_iv`。
- 75/78 同时有 put wall 与 call wall；最后 15 分钟两侧墙位都消失。

`wide_quote_degraded` 的 250 bps 阈值使用整条链平均 spread，0DTE 远翼自然会拉高平均值。它目前只降级 IV movement alerts，不直接阻断所有交易，但建议改为 ATM/25Δ 子网格或稳健分位数，逐腿执行仍以真实 NBBO gate 为准。

### 15:30～16:00 ET 0DTE 历史缺口

7 月 21、22、23 日两个 provider 的同日 0DTE 都在 15:30 ET 停止，而 next-expiry 继续到 16:00。这是旧 `option_collection_expiry()` 在收盘前 30 分钟提前滚到下一到期日造成的采集策略缺口，不是交易所没有 0DTE 报价。

当前 `master` 的 `a35a4bf` 已改为：

- 同日 0DTE 保持采集到 RTH close。
- 15:30 ET 只开启 next-expiry prefetch，不再丢弃 front expiry。
- 到 16:00 ET 后再正式滚动。

IBKR、Schwab marketdata 和 feature services 已在 2026-07-24 14:01 北京时间使用该代码启动，calendar 定向测试通过。但截至审计时当天 RTH 尚未开始，所以这个修复仍需今天 15:30～16:00 ET 的 live smoke 才能确认生产闭环。

历史 7 月 20～23 日最后 30 分钟没有可恢复的同日 0DTE 原始 NBBO，回放时必须标为 missing；不能插值或用 next-expiry 伪装成 0DTE 成交价。

### 61 档 price coverage 的严格性缺陷

`_strike_price_coverage()` 当前只检查 bid/ask 是否为有限数，没有检查：

- `bid >= 0`
- `ask >= bid`
- `mid` 是否有效

IBKR 的 `-1/-1` placeholder 因而可能被标记为 `usable=true`、`complete_pair=true`。在 2026-07-23 的 68 份报告中：

- 48 份至少包含一个假 complete pair。
- 最坏一份声称 61/61，严格 NBBO 复算只有 22/61。
- RTH 因 Schwab 核心链更完整，影响较小：首份为 61→59，另一份为 61→60，其余已审计 RTH 报告为 61→61。

这主要污染“密度/覆盖可信度”展示；execution quote gate 更严格，所以没有直接生成虚构限价。但修复前不应把 GTH 的 61/61 展示当作真实双边 NBBO 覆盖。

## 当前 2026-07-24 GTH 中断：上游 session，而非 RTH/渲染

2026-07-24 18:42:02～18:56:13 北京时间，IBKR journal 连续出现：

- `competing_session`
- IBKR error 10197
- `subscription_rejected`
- `subscription_setup_interrupted`

18:45 北京时间的 15 分钟报告正好落在该中断内，所以忠实显示：

- SPXW `0/61`
- L1 unavailable
- missing SPXW option quotes
- pricing blocked

该报告 `delivered_ok=true`，说明生成与投递没有失败。18:56 后 subscription 恢复；截至 19:15：

- option frame `quality=ready`
- IBKR L1 84 contracts
- two-sided ratio 1.0
- 61 档严格 NBBO 复算 61/61

这次是 GTH 的账户级 market-data session 抢占，不是当天 RTH 没信号，也不是 parser 或报告渲染丢失。

## 15 分钟报告层的问题

报告任务并没有覆盖完整 RTH：

- `state.py` 的 status window 截止到北京次日 01:30。
- 夏令时对应 13:30 ET。
- systemd timer 也只排到北京 01:30。
- `alert_profile.py` 却另外定义了 13:30～15:00 ET unattended afternoon 和 15:00～16:00 ET close window。

因此 15 分钟状态报告固定缺少最后 2.5 小时 RTH。实际 RTH 报告：

| 日期 | 实际 status 报告数 | 最后一份 ET | 已生成报告 delivery |
|---|---:|---|---|
| 2026-07-20 | 10 | 13:15 | 10/10 ok |
| 2026-07-21 | 16 | 13:15 | 16/16 ok |
| 2026-07-22 | 15 | 13:15 | 15/15 ok |
| 2026-07-23 | 14 | 13:00 | 14/14 ok |

13:30 ET 前也不是绝对每 15 分钟一份：RTH 状态在 `no_material_changes` 时会跳过；只有 GTH 有 quarter-hour heartbeat。代码注释和 CLI help 所说的 “fixed cadence” 与实际行为不一致。

2026-07-23 的 7 个 formal signals 中，15:00、15:17、15:35 ET 三个发生在 status window 之后，所以不会出现在 15 分钟报告中。不过 7 个 `level_path_confirmed` 都已进入 durable outbox，并分别投递到 Bark、Bark friend 和 Feishu，延迟约 2～7 秒。这里是“15 分钟摘要覆盖不足”，不是 signal 通知 worker 丢消息。

## 哪些日期可以用于什么

| 日期/区间 | 原始行情 | 已有派生 signal 日志 | 可否直接调参 | 说明 |
|---|---|---|---|---|
| 2026-07-20～22 RTH | ES/SPY 可用；options 有局部缺口 | 被旧 lifecycle bug 污染 | 否 | 只可把原始行情作为修复后 replay 输入 |
| 2026-07-23 09:30～15:30 ET | 基本可用 | lifecycle 已修复 | 仅诊断，不足以定参 | 只有一个 session；7 个 formal 全被 gate block |
| 2026-07-23 15:30～16:00 ET | futures/ETF 可用；同日 0DTE 缺失 | 最后一个信号受 options/anchor stale 影响 | 否 | 不能回填或插值 0DTE NBBO |
| 2026-07-24 RTH | 审计时尚未发生 | 尚无 | 否 | 需完成当日 live smoke |
| 当前 8-session volume baseline | 不足 20-session 合约 | percentile 为 null | 否 | 先 backfill，再验证 |
| Steven SPX 1m/5m bars | 0 bars | 不可用 | 否 | 先修持久 builder 并重建 |

更早日期如果运行的是 `b7e1901` 之前的派生逻辑，也不能直接使用已有 formal-signal count 做参数优化；应统一从原始 lake 用当前代码重放，避免混用不同生命周期语义。

## 修复后 replay 建议

### 1. 重放输入与状态隔离

每个交易日使用独立临时 data root 和全新状态文件，输入窗口从前一交易日 20:15 ET 开始，到当日 16:00 ET 结束，以便：

- 正确 warm up GTH stable structure、ES-SPX basis 和 wall persistence。
- 在 09:30 ET 切换并单独构建 RTH VWAP、opening range 和 RTH bars。
- 避免前一回放日期的 terminal state、candidate structure 或 notification ledger 泄漏到下一日。

### 2. 固定当前语义

至少固定以下版本：

- lifecycle：`b7e1901` 或更高
- same-day option collection：`a35a4bf` 或更高
- 配置文件 hash
- 数据质量和 execution quote policy version

先做“当前参数基线 replay”，再单独变更一组参数。不能一边修数据、一边同时放宽 signal gate，否则无法归因。

### 3. 不生成不存在的行情

- NBBO 不插值。
- 7 月 20～23 日 15:30～16:00 ET 缺失的 0DTE 保持 missing。
- 只允许对 IV surface/结构函数做平滑，并保留输入覆盖、quote age 和置信区间。
- IBKR `-1` placeholder 必须在进入 coverage、IV 或 execution 计算前剔除。

### 4. 输出两层结果

将结果明确分开：

1. `market signal`：level path 到达 `CONFIRMED`。
2. `executable trade signal`：通过方向、剩余空间、reward/risk、真实双腿 NBBO、expiry 和 freshness gate。

每个 session 至少记录：

- data-ready 占比及每层 drop reason
- 唯一 formal transition 数
- gate block reason 频次与共现
- 1/3/5/15/30 分钟 MFE、MAE、终值
- 实际 NBBO 下的 spread/slippage/可成交性
- weekday 与 open/midday/close 分层
- options 缺失时的 censored 样本标记

### 5. 调参顺序

建议按以下顺序，避免把数据缺陷拟合成参数：

1. 先修/接通真实 ES/SPY 1m/5m bars、RTH VWAP、opening range 和 same-time range。
2. 修严格 NBBO coverage 与 Steven bar persistence。
3. 完成至少 20 个 session 的 same-clock baseline。
4. 用当前 lifecycle 重放所有原始日期。
5. 只在修复后 replay 结果上审查 `remaining_target_room`、breakout support、1m/5m 同向和 reward/risk 阈值。
6. 用 walk-forward 划分训练/验证，不能用同一批周一至周三既选参数又汇报结果。

## 最小修复优先级

### P0

1. 用当前 lifecycle 重放 2026-07-20～22，替换“零信号”这一失真结论。
2. 把 15 分钟 status window 延伸至 16:00 ET，并明确决定是否真要每 15 分钟 heartbeat。
3. 建立真实 ES/SPY 1m/5m OHLCV 派生层，拆分 `globex_vwap` 与 `rth_vwap`。
4. 修复 SPX bar builder 的跨 cycle open-bar 持久化。
5. 将 `bid >= 0 && ask >= bid && mid valid` 作为 price coverage 的完整 pair 合约。

### P1

1. 将定义一致的 5m breadth 接入 `minute_market_frame`/`decision_context`。
2. backfill 20-session same-time range/volume baseline。
3. 将 IV surface 的 wide-spread 判定改为 ATM/25Δ 核心区稳健统计。
4. 在 2026-07-24 15:30～16:00 ET 对同日 0DTE 保留采集做 live smoke。

## 最终判断

周一至周三的“RTH 无信号”已经找到确定根因：旧状态机在 OI/GEX 结构候选确认期间错误地杀死活动 lifecycle。这个问题周四开盘前已经修复，并由周四 7 个 formal signals 的恢复得到生产数据佐证。

现在不能靠简单放宽阈值补救，因为：

- 旧日期的派生标签本身已失真；
- RTH 关键八变量的数据层还不完整；
- 报告只覆盖到 13:30 ET；
- 历史最后 30 分钟缺少真实 0DTE；
- 修复后有效 RTH 样本目前只有一天。

正确下一步是先做修复后 deterministic replay，再根据真实的 gate attribution 调参，而不是把旧 bug 造成的零信号当成策略过严。
