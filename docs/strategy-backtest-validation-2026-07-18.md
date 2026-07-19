# SPX 0DTE：GTH、Put/Down 与入场质量修复验证

生成日期：2026-07-18
验证状态：**v3 工程契约已落地；forward readiness 0/20，策略保持 collecting/shadow，禁止自动下单**

## Technical Summary

旧报告里“GTH 12 笔全亏、S2 Put 8 笔 -$1,730”的数字不能再当成生产策略表现。
审计发现，S2 只是 `first_touch + 15s` 的观察性代理，12 笔旧回测交易中 **0 笔**曾进入
production `trade_ready`；同时 `chain_implied_spx` 被错误映射为 `future:ES - 45`，并有重复
semantic opportunities。这个坐标错误与 GTH/Put 高度混杂，制造了主要负值。

本轮已把回测集合改为持久化的真实 `trade_ready`，严格使用记录的 provider、合约、限价、
有效窗口、目标和失效位。截止 2026-07-17，474 条 intent evaluation 中有 452 条
`observing`、20 条 `blocked`、仅 2 条 `trade_ready`。严格重放后：1 条 Call 成交并模拟
+$800；1 条 Put 在限价满足前已经触及目标，按 `target_before_entry` 跳过、PnL 不记 0。
**n=2 只能证明回测语义修正，不能证明策略有 edge。**

GTH 还发现两个生产生命周期缺陷：历史 6 次 virtual open 中有 2 次在确认后 826 秒和
5,261 秒才开仓，超过 600 秒信号 TTL；macro/active suppression 期间又会继续积累确认。
现在整条决策链统一保存 v3 五字段契约，virtual entry 对缺失、过期和超前时间戳 fail closed；suppression
和 provider switch 都重置确认；exact spread 还必须满足双腿新鲜同步 NBBO、报价有序、
`0 < executable ask < width`。趋势一致性只新增为预注册 shadow 特征，不因 6 个历史样本
事后直接上线。

新的参数裁决门是 forward-only：旧 v1/v2 不能回填成 v3。本次 cutoff 观察到 5 个 feature
partition，但只有 2026-07-14..17 四个 session 通过数据健康门；2026-07-13 的 GTH 分钟覆盖
只有 85.19%，低于 90% 门槛。readiness 另排除了 43 条 legacy material decision，474 次重复
evaluation 和 6 个 legacy GTH 也不能冒充独立 forward 样本。当前 contract-consistent session、
GTH exact entry、Put exact entry、exact-spread complete exit 均为 **0/20**。在各自门槛满足前，
GTH、Put 和 sat85/trail33/clock 退出规则只能 collecting/shadow，不得晋级；达到门槛也只触发
人工 review，不会自动 promotion。

> **研究声明：** 本报告是内部工程与策略研究快照，不构成投资建议。仅覆盖 5 个 observed
> feature partitions、其中 4 个 health-complete session，以及 4 日 production intent
> telemetry；这些均早于 forward-v3 cohort，不能回填进入 readiness。
> `automatic_ordering=False` 保持不变。

## Key Findings

1. **Put 方向没有写反。** 267/267 个历史 pricing-outcome 的 play、option right 和 direction
   一致。问题是错误 cohort、错误坐标和重复，不是 C/P 映射。
2. **不能禁掉全部 Put。** 旧亏损中 5 个 GTH/down 与坐标错误混杂；修正后的观察性 S2
   down 只有 3 笔、合计 +$120。真实 production Put 只有 1 条，因目标先于限价触发而不入场。
3. **真实 production 入场样本极少。** 474 条 evaluation 不是 474 个可交易机会；452 条
   `observing` 既不是 pass 也不是 block。2 条 trade-ready 中只有 1 条严格成交。
4. **GTH 的直接缺陷已封住。** 2/6 历史 virtual opens 超过 TTL；新代码阻止陈旧信号、
   suppression 攒确认和不可执行 debit spread。
5. **旧 S2 盈亏已降级为观察性结果。** 原始 267 行按 semantic key 在 5 个 observed
   partitions 中去重为 233 个机会；排除不完整的 07-13 后，health-complete cohort 为 217 个，
   baseline proxy 6 笔、+$100。它不等于 production gate，不计入策略总收益。
6. **账户 replay 三行仍只是报价覆盖敏感性。** 30s/5s、90s/15s、300s/60s 改变了
   common cohort，不能解释为策略由亏转盈。
7. **20 session 不是 20 次 evaluation。** 只有跨日分钟覆盖完整、v3 契约一致、无重复
   semantic sample 的独立 session 才计数；5 个 observed partitions 中只有 4 个通过健康门，
   当前 forward-v3 合格进度仍为 0/20。

## Scope, Data, and Metrics

### 范围与截止

| 数据 | 截止/规模 | 本轮用途 |
|---|---:|---|
| Feature store | 2026-07-13..17，5 个 observed partitions；4 个 health-complete session（07-14..17） | S1/S2/S3 control/proxy |
| `features/trade_intents` | 2026-07-14..17，474 evaluations | production entry gate 与真实 cohort |
| GTH signal + virtual audit | 6 confirmations / 6 historical opens | 信号年龄和生命周期审计 |
| Quote lake | 截止 2026-07-17 | point-in-time limit、underlier、bid/ask 路径 |
| IBKR statement replay | 核心 GTH Call debit 7 rounds | 报价覆盖敏感性，不归因 K3 入场 |

主回测采用 `--as-of 2026-07-17`，exclusive cutoff 为 2026-07-18 00:00 UTC；另用
`--as-of 2026-07-16` 做累计 cutoff 对照。2026-07-18 的部分日数据不进入结果。

### 指标语义

- `observing`：非终态 telemetry；不计 pass、不计 blocked、不计 PnL。
- `trade_ready`：生产已经持久化的完整入场决定；按 unique `intent_id` 去重。
- production executable：entry window 内首次观察到记录 provider 的 `ask <= entry_limit`，
  且此前未触及记录 target/invalidation。
- S2 proxy：定价结果 touched 后的 follow-through-only 反事实；只用于诊断采集和坐标质量。
- 未成交/数据缺失：保持 skip/unavailable，绝不填成收益 0。
- forward-v3 complete session：同一 research expiry 的 GTH、RTH 分钟覆盖均达到 90%，且
  该 session 内参与裁决的事件 100% 满足冻结契约、没有重复 semantic sample。
- readiness：达到门槛仅代表 `ready for review`；`automatic_promotion` 永远为 false。

## Methodology

### Production `trade_ready` 重放

1. 只读取 `status=trade_ready` 的持久化行，要求 direction/right、expiry、provider、contract、
   evaluated time、limit、expiry window、spot、target 和 invalidation 全部有效。
2. 使用记录的 provider 和 exact contract，不用未来报价覆盖率重选 provider。
3. 在 `[evaluated_at, expires_at)` 内扫描 ask；只在 `ask <= limit` 时以观察 ask 模拟成交。
4. 成交前的 SPX 路径若先碰 target 或 invalidation，分别记 `target_before_entry` 或
   `invalidation_before_entry`，不制造交易。
5. `Signal.horizons` 不进入 production gate；未来 outcome 标签在 loader 中被清空。
6. trade-ready 只运行 naked exact contract；不事后构造 5/10/wall spread。

### S2 坐标与去重修正

- `official_spx` 使用 `index:SPX`。
- `chain_implied_spx` 只使用记录的 `synthetic:SPXW_PARITY`；quote lake 无原生路径时
  fail closed，禁止偷换 official SPX 或 ES。
- `es_equivalent` 使用记录的 raw `trigger_target` 和 `future:ES`；禁止固定减 45。
- 按生产 semantic key 保留最早 touch，将 267 条重复 snapshot 降为 233 个机会。
- S2 名称和报告统一标为 observational proxy，不进入 production total。

### v3 五字段契约与 GTH 生命周期修正

- GTH signal、RTH trade intent、confirmed gate 和 virtual lifecycle 统一写入
  `schema_version=3`、`policy_version`、`valid_until`、`coordinate`、`block_reasons`。
- policy version 绑定有效规则/config；coordinate 冻结 point-in-time 原生坐标，禁止把 official
  SPX、chain parity、ES-equivalent 与 raw ES 互换。
- `valid_until` 使用 exclusive boundary；schema v3 缺任一强制字段都 fail closed。
- virtual open 必须同时满足：时间可解析、未过 `valid_until`、未来时钟偏差不超过 5 秒、
  same session/expiry、exact Call legs、两腿 age/skew 门、两边 NBBO 有序、debit ask 小于 width。
- suppression 保留 raw samples 供研究，但清空 eligible confirmation；恢复后必须重新满足
  fresh count/hold。provider switch 同样重置 pending。
- 趋势假设 `gth_trend_alignment_shadow_v1` 只记录 fresh same-session Globex regime；
  shadow verdict 不改变告警，不开启自动执行。

### Forward-only readiness 与样本定义

旧 v1/v2 原样保留用于历史错误审计，绝不补字段、改版本或计入 forward 分母。冻结的裁决门为：

| 独立裁决单元 | 当前 | 目标 | 达标前状态 |
|---|---:|---:|---|
| contract-consistent complete session | 0 | 20 | collecting |
| GTH exact two-leg entry | 0 | 20 | GTH 仅 shadow |
| Put point-in-time exact entry | 0 | 20 | Put 仅 shadow |
| exact-spread complete entry→exit | 0 | 20 | 退出规则仅 shadow |

截止 2026-07-17 共观察到 5 个 partition。2026-07-14..17 四个 session 通过健康完整门；
2026-07-13 的 GTH 覆盖为 673/790 分钟，即 **85.19%**，因低于预注册的 90% 门槛被排除。
这 4 个健康完整 session 仍不在 forward policy window，契约一致进度因此是 0/20。readiness
同时排除 43 条 legacy material decision：confirmed gate 1、GTH signal 6、trade intent 22、
virtual strategy 14；它们只保留作审计证据。

每个 signal/intent/episode 按稳定 semantic key 最多计一次；服务轮询产生的 observing/blocked
evaluation 不增加样本数。任何 forward contract anomaly、policy 漂移或重复记录都会阻止该
session 进入合格集合。四项门槛全部满足后也只是进入固定 cutoff 的人工 review。

## Results

### 真实 production entry gate

| 状态 | evaluation records | distinct events | 解释 |
|---|---:|---:|---|
| observing | 452 | 279 | 非终态，不属于 pass/block |
| blocked | 20 | 5 | 生产门明确阻断 |
| trade_ready | 2 | 2 | 可进入 strict entry replay |

| trade-ready intent | 方向 | 入场结果 | 严格时序 | baseline PnL |
|---|---|---|---|---:|
| 2026-07-14 7530C breakout | up/call | 19.8 ask 满足 19.9 limit | 14:33:53.651Z 成交 | +$800 |
| 2026-07-15 7560P breakout | down/put | `target_before_entry` | 15:51:04.021Z 先到目标；15:51:25.513Z 才满足限价 | 不产生交易 |

因此，当前 production headline 是 **2 个决定、1 个模拟成交、+$800**，不是旧 proxy 的
23 笔/-$2,155。这个单笔正值没有统计意义，也不能用来放宽限价或启用自动执行。

2026-07-16 与 2026-07-17 两个 cutoff 的 production cohort 和结果完全相同：后一天没有新增
trade-ready。这个稳定性只说明 cutoff 实现一致，不是新的 holdout 证据。

### Put/Down 诊断

| 口径 | down/put n | 总盈亏 | 是否可作策略证据 |
|---|---:|---:|---|
| 旧 S2 baseline | 8 | -$1,730 | 否：混入 GTH 坐标错误和重复 |
| 旧 S2 中 GTH/down | 5 | -$1,770 | 否：错误 coordinate cohort |
| 旧 S2 中 RTH/down | 3 | +$40 | 否：旧 proxy、小样本 |
| 修正后 S2 observational proxy | 3 | +$120 | 否：只用于采集诊断 |
| production trade-ready Put | 1 decision / 0 fills | 无交易 | 样本不足 |

结论不是“Put 已经变好”，而是“旧数据不能证明 Put 很差”。运营规则应保持：RTH Put 继续
collecting/shadow、方向作为固定切片；GTH/coordinate-unavailable fail closed；只有同时满足
20 个 forward 完整 session 和 20 个 Put exact entries 后才进入预注册 walk-forward 裁决。

### GTH 生命周期审计

| confirmation → historical virtual open | 结果 |
|---|---:|
| 826.5 秒 | 超过 600s TTL，现已阻断 |
| -1.5 秒 | provider/clock 偏差，在 5s tolerance 内 |
| 5,261.5 秒 | 超过 600s TTL，现已阻断 |
| 3.4 / 4.6 / 0.7 秒 | 在 TTL 内 |

历史陈旧率为 **2/6**。修复只能证明这两类不合法开仓以后不会发生，不能反事实宣称它们的
全部亏损都会被“挽回”。6 个历史 GTH 信号重放时均落在 bearish regime，但这是同窗口、
事后看到的 n=6；因此只把 bullish alignment 预登记为 shadow hypothesis，不直接 enforce。

### S2 observational proxy

| 指标 | 旧实现 | 修正后 |
|---|---:|---:|
| touched rows / semantic opportunities | 267 rows | 233 observed unique；217 health-complete |
| baseline executable | 12 | 6 |
| total PnL | -$1,560 | +$100 |
| production trade-ready overlap | 0/12 | 不作为 production cohort |

从负到正主要说明坐标、重复和 session 健康筛选会扭曲结果，**不说明 follow-through 突然变成有效策略**。

### Account replay coverage sensitivity

| Age / skew gate | Common | Rounds | Actual net | sat85 est. net | trail33 est. net | clock est. net |
|---|---:|---:|---:|---:|---:|---:|
| 30s / 5s | 1 | 7 | -$1,411 | -$2,686 | -$2,686 | -$2,686 |
| 90s / 15s | 3 | 7 | +$3,052 | +$1,407 | -$1,713 | +$1,407 |
| 300s / 60s | 5 | 7 | +$9,226 | +$7,041 | +$1,531 | +$7,041 |

三行的 common cohort 分别不同。30s/5s 恰好只留下核心 7 个实际回合中唯一的亏损回合；
放宽门后加入的是盈利回合。它反映 quote-lake 覆盖选择偏差，不是 age 参数带来策略收益。

## Limitations and Robustness

- production 只有 2 个 intent、1 个模拟成交，无法估计胜率、expectancy 或方向差异。
- historical intent 保存的 `expires_at` 按当时运行时状态重放；当前代码的 entry window 已固定
  为 20 秒，历史 runtime drift 必须在更多新样本中单独报告。
- S2 的 233 个机会仍是 pricing-outcome 观察数据；seed 时冻结的 session/trend/vol 在实际
  touch 时可能陈旧，不能用来样本内调阈值。
- GTH detector 参数本轮没有用 raw ES samples 重新检测；只验证现有确认事件后的生命周期。
- forward-v3 exact-spread S3 仍为 0/20；legacy/derived spread 不得回填，无法验证 sat85 的经济效果。
- top-of-book 模拟没有队列、滑点、部分成交、人工通知反应延迟和真实佣金。
- account replay 缺失非随机，且没有 event_id 将账单回合归因到 K3 入场。
- 2026-07-17 只是累计 cutoff，不是预先冻结的独立 OOS。

## Recommended Next Steps

1. 合并并部署确定性的 TTL、suppression reset、provider reset 和 exact-NBBO 工程门；部署前后
   保持 `automatic_ordering=False`。
2. GTH trend alignment 保持 `collecting/shadow`；同时满足 20 个 forward complete session
   和 20 个 exact GTH entries 前不得晋级。
3. RTH Put 不硬禁；按 `RTH/GTH × up/down × breakout/fade` 固定分层，收满 20 个
   point-in-time exact Put entries 前不得晋级。
4. 继续保存 v3 policy/config fingerprint、真实 entry window、两腿或单腿 fill-time
   NBBO/source timestamp 和 terminal block reasons。
5. 收满 20 个 exact-spread 完整 entry→exit 后，才在固定 cutoff 比较 sat85、trail33、clock；
   不再用 legacy derived spread 代替生产组合。
6. 每周自动生成 readiness 快照，但仅在冻结门槛全部满足后做一次人工参数裁决，避免反复看
   样本调门；任何流程都不得自动 promotion。

## Further Questions

- 20 个 forward session 与 20 个 exact GTH entries 后，bullish-alignment shadow 的 pass
  coverage 和同 cohort outcome 如何？
- `target_before_entry` 在更多 Put/Call 中的比例是多少，是否说明 limit 太慢或信号到达太晚？
- blocked reasons 中哪些属于真实策略过滤，哪些只是采集/结构缺失？
- `synthetic:SPXW_PARITY` 是否应进入 quote lake，还是 chain-implied proxy 永久只做 unavailable？
- 真实 fill-time NBBO 补齐后，严格 account replay 的非随机缺失能否显著下降？

## Reproducibility

- 主回测：`/srv/data/spx-spark/data/reports/odte_level_backtest/validation=2026-07-18-contract-v3/cutoff=2026-07-17/`
- 对照 cutoff：`/srv/data/spx-spark/data/reports/odte_level_backtest/validation=2026-07-18-contract-v3/cutoff=2026-07-16/`
- 可复现诊断 notebook：`docs/notebooks/entry-quality-gates-2026-07-18.ipynb`
- 入口：`scripts/backtest-0dte-levels.py`
- 关键实现：`odte_level_signals.py`、`odte_level_backtest.py`、`gth_dip.py`、`virtual_strategy.py`
- 验证：focused 147 tests passed；完整 repository `1541 passed`（1 条第三方 deprecation warning）。
