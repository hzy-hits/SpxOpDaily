# SPXW raw → merge → feature 时钟契约

## 目标

期权执行报价与结构分析使用不同的新鲜度权限，避免把外围轮转的正常年龄误判为整条链不可用。

- `as_of`：本次 feature 计算的统一 UTC 截止时刻。
- `source_at`：交易所/供应商记录的最后价格变化时刻。
- `observed_at`：本进程最近一次收到或重新确认该字段的时刻。
- pricing、Greeks、Open Interest 各自保留 `observed_at`；较新的 OI 不能刷新较旧 Greeks 的年龄。

任何字段的 `observed_at > as_of + 5s` 都拒绝。回放只读取 `observed_at <= as_of` 的记录，不允许未来数据。

## Freshness lane

| lane | 分析最大年龄 | 用途 |
| --- | ---: | --- |
| core | 15 秒 | Schwab 实时核心、IBKR core |
| rotation | 90 秒 | IBKR 外围快速轮转缓存 |

轮转窗口只授予分析权限。执行定价仍走原有严格 gate；分析 clone 带有 `analytical_only=true`，不能据此生成可执行 NBBO。

## 合并规则

1. 每个合约先按供应商保留 pricing、Greeks、OI 的独立时钟。
2. RTH pricing 仍优先 Schwab；GTH 仍只允许 IBKR。
3. Greeks 可从另一个仍在 freshness lane 内的供应商补全，但 bid/ask 不随之替换。
4. 每条腿输出 pricing/Greeks provider、lane、source age、observed age、structure age 和拒绝原因。
5. NBBO 不插值；`nbbo_interpolated=false` 是持久化契约。
6. Delta/Gamma 可以由已观察到的 IV 用现有 Black–Scholes 实现推导；这属于结构函数平滑，不会生成不存在的成交价格。

IBKR stream 的 flush 时刻不是 Greeks/OI 的观察时刻。只有字段首次出现或
实际变化时才推进各自 `observed_at`；安静但仍有 ticker heartbeat 的报价只
推进 pricing 时钟。当前 stream 行若缺少独立字段时钟会按 stale fail-closed，
不能借用本轮 flush 的 `received_at`。

## Spring Gamma coverage

结构质量固定评估离 spot 最近的 61 个 strike，再划分 ATM core 与左右翼。订阅更远的稀疏外围不会降低核心覆盖率。原始总 strike 数仍以 `available_strike_count` 保留。

生产推断至少要求 13 个每腿均通过 analytical gate 的完整 C/P 对（5 点档时对应
ATM ±30 点核心），不再允许 3 档被称作稠密结构。覆盖诊断同时发布
`density_state`、`density_target_pair_count=61` 与相对 61 档的完成率：
13–48 对只表示 `core_covered`，49–60 对才是 `dense`，61 对为 `full_61`。
61 是稠密度目标而不是硬推断门槛，避免外围暂缺使全天方向状态归零。

`snapshot_age_seconds` 表示 core Greeks 的最坏年龄；rotation 年龄单独记录。
Greeks/IV 与 OI 各自发布 provider、lane 和 observed age。过龄或未通过
analytical gate 的单腿 IV/Delta/Gamma/Vanna/Charm 会被清空；OI/volume
仍保留作来源披露，但不能让该腿计入完整 C/P 对。过龄轮转腿不会通过缩短
整体链或伪造价格绕过 gate。

距离到期不足 5 分钟的 Greek reference 风控保持不变。

## 2026-07-24 历史证据边界

持久化的 7 月 24 日生产档案包含 390 个 RTH v2 分钟点，其中 Option overlay
为 `Ready` 的是 `125/390`（32.05%）。对应的每日验收文件记录了
`spring_rth_minute_coverage=390/390` 和
`spring_option_overlay_ready_ratio=125/390`。旧 coverage 使用无界 strike
计数，125 个 Ready 点的 `paired_strikes` 为 75–84；这个范围不能与现在的
最近 61 档口径直接比较。

曾在字段独立时钟、90 秒 rotation lane、跨源 Greeks 和最近 61 档改造后做过
一次中间版本重放，得到 `269/390`（68.97%）。这个数字产生在最终的
per-leg `analytical_allowed` fail-closed 和生产 `min_paired_strikes=13`
生效之前；重放输出及输入指纹没有作为版本化 artifact 持久化。7 月 24 日的
生产 prediction 也没有当前的逐腿 `leg_metadata`，而兼容读取会有意采用旧
schema 语义。因此 `269/390` 只能证明中间改造扩大了可分析覆盖，不能作为
当前代码的最终历史 Ready 率、75% 验收结果或参数放宽依据。

当前契约不声称一个事后“最终 7 月 24 日百分比”。若要发布历史最终数字，必须
新增可重复执行的 raw → merge → exposure → Spring 全链重放，固定代码 commit、
runtime 配置、每分钟 input fingerprint 和逐腿拒绝原因，并把输出 artifact
持久化后再审阅；只重数旧 prediction 会错误绕过新 fail-closed gate。

## 下一次真实 RTH 验收

下一次完整交易时段是 2026-07-27 09:30–16:00 ET。17:30 ET 的
`spx-spark-rth-daily-acceptance.timer` 必须基于新采集的逐腿 provenance
验收，而不是沿用 7 月 24 日中间重放：

- 目标为 390 个有效 Spring RTH 分钟点，覆盖率至少 95%；
- Option overlay Ready 率至少 75%；若正好 390 个有效点，至少需要 293 个；
- 每个 Ready 点至少有 13 个双腿均通过 analytical gate 的完整 C/P 对；
- coverage 必须披露相对 61 对目标的 `density_state` 和完成率；
- 26 个 RTH heartbeat 报告及其投递必须完整，NBBO 仍不得插值。

验收结果写入
`reports/rth_daily_acceptance/date=2026-07-27/acceptance.json` 和
`latest/rth_daily_acceptance.json`。任何一项未通过都保持 degraded，
不把服务进程 active 等同于数据面可用。
