# Operations Schedule

<!-- documentation-status: 2026-10-18 -->
> **文档定位：现行运行与参考。** 现行说明；历史段落保留原适用日期。运行状态以实际服务和源字段时钟为准。
> [全仓文档、当前运行状态与合同优先级](README.md)（目录核对：2026-10-18）。

核对日期：2026-10-18。实际执行入口以已安装 unit、`systemctl list-timers` 和
`src/spx_spark/infrastructure/jobs.py` 为准。下面区分 user units、system units
与 Huey，不能把旧 24h loop 的任务表当成当前调度。unit 级清单见
[服务与定时任务清单](service-inventory.md)。

## 常驻 owner

| Owner | systemd 范围 | 当前职责 |
| --- | --- | --- |
| `spx-core.service` | user | SPX/ES 采样、特征与最终策略、独立 shock runner、必要实时任务 |
| `spx-worker.service` | user | Huey 通知任务、宏观刷新、维护、链路故障监测及启用的慢任务 |
| `spx-spark-ibkr-stream.service` | user | GTH/备用 broker 连接、配额、hot/rotation 与已授权 exact-leg 订阅 |
| `spx-spark-schwab-marketdata.service` | user | RTH 主链与 ES/上下文采集 |
| `spx-spark-schwab-oauth.service` | user | 既有 OAuth callback 与本机行情入口 |
| `ibc-gateway.service` | user | IB Gateway；API 仅 loopback，不因普通缺字段反复重启 |
| `spx-rust-core-shadow.service`、`spx-rust-normalized-bridge.service` | system | 既有 typed ingress、frames、projection 与桥接 |
| `spx-rust-report.service`、`spx-rust-delivery.service` | system | 桌图编排、Rust ledger 与每目标投递 |

`shadow` 是保留的部署名称，不能据此断言当前 report/delivery 未投入使用。
旧 `spx-spark-24h`、独立 hot-worker 和旧 notification-delivery owner 已收敛；
不要重新 enable 旧 unit。`spx-spark-post-close-review.timer` 与
`spx-spark-backtest-weekly.timer` 默认禁用，分别由 session finalizer 与家庭研究节点
取代。图片容器及可选 VNC 有独立用途，见专题部署说明。

## 定时工作

| 任务 | 时钟 / 计划 | 边界 |
| --- | --- | --- |
| Desk Map 源快照 | `spx-spark-order-map-status.timer`，合法 GTH/RTH 每 15 分钟 | `16:00 ET` 为收盘快照；源快照保存不等于推送 |
| 常规 Desk Map | Rust report，合法时段 `:00/:30` ET | Python 只准备投影，受 owner fence 约束 |
| 盘前地图 | `spx-spark-morning-map.timer`，周一至五 `08:35`、`09:15 ET` | 盘前摘要；不建立新仓 |
| Order Map 定时推送 | `spx-spark-order-map.timer`，周一至五 `14:00`、`16:30 Asia/Shanghai` | Rust owner 开启时只准备投影，不再外发 |
| 数据恢复更新 | Core 新鲜决策识别缺失能力恢复 | 不等下一半小时；重新核对当前时间，按投影去重；无恢复不伪造更新 |
| 宏观日历 | Huey 每 5 分钟检查 TTL | Worker 联网刷新，Core 只读；保留来源覆盖与 last-good |
| 桌图链路监测 | Huey 每分钟，priority 20 | 直接飞书故障/恢复告警，不能掩盖 Worker/主机整体失效 |
| 机会台账 | Huey 每分钟 | 读 `latest/strategy_decision.json`；按 decision_id/state 去重追加 `opportunities/date=*.jsonl` |
| 存储压力检查 | Huey 每小时 UTC `:20` | 替代退役的 storage-pressure timer，复用 session finalizer |
| 维护审计 | Huey 每日 `23:30 UTC`；user weekly timer 周日 `13:00 Asia/Shanghai` | 默认审计，不因定时到达获得任意删除权限 |
| 原始数据压缩 | user timer 每小时 `:08`，随机延迟最多 120 秒 | copy-only，保存 manifest；不能自动删除未验证原始数据 |
| 周末补压缩 | 周六/日 `08:30 Asia/Shanghai`，随机延迟最多 300 秒 | 同样复用原压缩合同 |
| Session finalizer | 每日 `18:00 America/New_York` | 自动选择完成的交易日；修复重跑幂等 |
| RTH daily acceptance | 工作日 `19:00 America/New_York` | 行情/决策/投递分开验收 |
| 每周 backtest job | 周一 `09:17`，随机延迟最多 300 秒 | **安装脚本默认禁用**，已移到家庭研究节点；本机不跑 |
| IBKR verifier | `spx-ibkr-verifier.timer`，每 15 分钟 | 只读核验，不改变采集 owner |
| IBC watchdog | `ibc-watchdog.timer`，每 2 分钟 | 外部 Gateway 守护，不归应用调度 |
| Rust frame retention | system timer 每日 `00:15 UTC` | 只处理已完成 UTC 日，保护当前日与写入 reserve |

Huey 使用 UTC。交易时钟由 `America/New_York` 与交易日历解释，不能全年固定
减 12 小时。所有 timer 是否已启用、漏跑补跑及实际耗时都要现场核对。
宏观、Hyperliquid、Greek shadow、IV surface 和 growth-dislocation 的开关/任务
由现有配置与 `jobs.py` 决定，不从本文复制新的 cron。

## Fallen Angel / Growth Dislocation 数据与日报

Scanner 继续由现有 Worker 运行。2026-11-10 用户在 v13 分数排名之后，授权
**中期修复和收盘确认先于评分**（policy v14），并要求 PR、合并部署。
下述规则不提供收益保证；得分、WATCH/ARMED/TRIGGER 均不是买入指令。

- 仅对本轮细查的股票及其行业基准重新获取券商日线，计入原有 Schwab
  请求预算。按已有交易日历验证连续性和最后已收盘交易日；旧缓存的条数足够
  不能替代校验。缺口、超时或预算不足时不使用历史缓存生成当日评分。
- 日报和 RTH 均使用至少 61 个连续、已完成交易日的券商收盘价计算信号。
  RTH 报价只可否决已有确认，不能追加成收盘价制造确认；不造假日或未来日线。
- 2026-11-10 盘后取数故障修复：Schwab 日线请求必须显式传入扫描参考时刻的
  `endDate`（带时区时间转换为 Unix 毫秒），不依赖 provider 默认截止日期。
  原请求在 11 月 9 日 20:00 ET 只返回到 11 月 6 日；ORCL/XLK 的只读对照中，
  补上截止时间即返回 11 月 9 日且通过原连续性校验。仍拒绝真实缺口和未完成日线，
  不用盘后现价替代收盘，也不修改评分、请求预算、调度或 Bark。
- 日线失败进入 partial 并沿用原 Core Pool 冻结机制。IVP 的单股票缺失仍按原有
  规则排除该股，不改变其他成员的生命周期；报告单独列出刷新、有效缓存和缺失
  数量，`scan_complete` 只表示扫描执行完成，不证明每个标的的数据完整或刚刷新。
- 飞书正文顺序为：当日扫描、淘汰/数据缺口、当轮确认机会、WATCH/ARMED、Core 跟踪。
  候选、Core 和飞书统一按综合分降序，保留行业去拥挤；不要求先成为旧 Core 成员。
  暂停成员后置，明确最后合格快照日期；未取得本轮 IVP 时留空，不混入旧 IVP，
  不展示历史评分和历史合约为当日机会。
- 历史快照仍保留用于审计，通过 `snapshot_status`、`snapshot_as_of_et` 和独立的
  `current_observation` 区分本轮观察。Bark 日报路由/传输、调度和存储
  owner 不变；修复后的底层有效候选可能与修复前不同。

v14 确认与评分合同（初始、前向未验证；不新增最低总分门）：

- WATCH：中期结构未修复。修复要求收盘价 > MA50、MA20 比五个交易日前上升、
  MA50 比五个交易日前不下降；满足后为 ARMED，不要求 RSI 曾跌破 30.00。
- 在修复条件下，收盘突破**此前 20 个交易日最高收盘价**（不含突破当日），
  下一个已完成交易日仍收在固定突破位或以上，且修复条件持续成立，才为 TRIGGER。
  突破位和确认日冻结，不随滚动高点上移，不用未来收盘提前确认。
  必须在可用日线中先看到未修复/失守，再观察修复与突破；不能把历史窗口首端
  已在上涨的状态冒充新突破。原起点退出可用窗口时失去证据，不重画一个起点。
- 后续任何收盘跌破固定突破位或修复条件失效，原确认失效；需重新突破及次日守住。
  未失效的确认可持续保留，显示原确认日/年龄；不是每日新信号。盘中价跌破突破位
  或不再高于当前已完成日线 MA50，取消当轮机会资格，不能创造新确认。
- 仅有效 TRIGGER 且行情、基准和目标合约齐全时评分：趋势结构 35.00%、
  行业/市场相对强度 20.00%、入场位置/路径平稳度 20.00%、期权成本 15.00%、
  市值/流动性 10.00%。52W 位置、股息、RSI、13W/26W IVP 仅为背景，不贡献分数。
  13W/26W IVP 缺失显示为空，不阻断；52W IVP 等实际评分输入缺失不造分数。
- 趋势分 = 50.00 + 25.00 × clip(MA20 五日变动 / 3.00%)
  + 25.00 × clip(MA50 五日变动 / 2.00%)，clip 范围 [0.00, 1.00]。
  RS = clip(50.00 + 250.00 × excess, 0.00, 100.00)；行业 ETF 和 SPY 各半，
  各自 20 日/60 日超额收益权重 60.00%/40.00%。均线涨幅饱和，不奖励无限追高。
- 支撑参考是固定突破位与 MA50 的较高值，不是保证成交的止损价。
  入场分 = 60.00/(1.00 + 距支撑百分比/10.00%)
  + 40.00/(1.00 + RV20/40.00%)。RV20 是收盘对数收益年化标准差；没有 OHLCV，
  不冒充 ATR、成交量确认、机构吸筹或跳空损失模型。
- 期权成本分 = 50.00% 的 52W IVP 分 + 25.00% 绝对 LEAPS IV 分
  + 25.00% 原 IV/RV 分。IVP/绝对 IV 保留 `100/(1+value/anchor)`；IV/RV
  保留 0.80 到 2.00 的线性降分。底层 IVP 与所选 LEAPS IV 期限不同，不能等同。
- 市值/流动性分各半：市值 `100*cap/(cap+min_market_cap)`；流动性
  `100/(1+spread_mid/4.00%)`。不存在跌得越多或更超卖就能抵消未确认的路径。
  删除旧低位/股息/短 IVP/RSI 评分与八个无效配置键；保留有效锚点和合约限制。
- 仍保留数据有效性和合约要求：LEAPS 365–730 天（恢复用户原始一年下限，优先
  540 天以上）、Delta 0.68–0.80、
  Spread/Mid ≤8.00%、OI ≥100、时间价值/股价 ≤20.00%；报价缺失、延迟、陈旧、
  未来时间或不合法两边报价均不可成为正式候选。报价发生在请求过程中时，
  以取数结束时间判断，而不是错误地相对扫描起始时间判为“未来报价”。
- 2026-11-05 只读链核对中，RBLX 在该查询范围只返回剩余 407／442 天的合约；
  旧 450 天下限会全部挡掉。恢复一年下限不等于合约已通过 Delta、价差或其他门。

预算与动态进出（沿用原 schema、状态文件、Worker 和通知 owner）：

- 全 Universe 获取轻量报价，按市值偏好交错轮转；在原 RTH 30 次／日报 90 次
  Schwab 预算内**先检查日线**。仅价格确认者再取 IVP/LEAPS，未确认者列入观察名单，
  不将“未请求 IVP/链”误报为 warming。每只预留日线/链以及去重后的行业/SPY日线成本；
  预留未花完不代表全市场完成，不扩张 IBKR 同时请求数。
- 细查顺序交错为旧成员刷新、新领先股、新领先股、轮转股；轮转来自粗排头部
  以外。旧成员按上次实际细查时间优先，未细查不更新该时间，不给留存加分。
- `scan_scope=budgeted_ranked_shortlist`；`shortlisted` 和 `not_evaluated` 明示范围。
  complete 表示本轮计划完成，不等于全市场 IVP 和合约全部完成；未细查不是淘汰。
- 完成本轮后，当前评分 Top N（现有 top_count，默认 10，沿用行业上限）入池；
  已刷新旧成员跌出排名即 `RANKED_OUT`，恢复排名可以再次进入。没有两日入池／
  三日退出等待，也没有旧成员豁免。未细查或缺数据的旧成员保留 STALE，不以旧分
  与新股竞争，因此记录总数可能超过 N，但当前 Active 不超过 N。
- 目标 LEAPS 明确违反保留的合约门槛且返回数据有效时退出。空响应、缺字段、报价
  延迟/过期不能证明无可交易合约，保留成员并标为 STALE；混合的缺失/失败合约
  也不作为已确认淘汰。缺失的 OI/optionable 不再用零/false 伪装成真实失败。
- 日线已就绪而回到 WATCH/ARMED 的旧成员以 `trend_watch`/`trend_armed` 明确退出；
  未完成数据不伪装成趋势失败。旧 v13 快照不能授权 v14 机会。
- 全局 partial 仍冻结成员；单只数据未知不影响其他已确认成员的进出。
  STALE 不是 Active，不进入当轮严格机会榜；历史快照保留用于审计。
- 旧的 entry/exit streak 被移除。研究否决仍有效。
  退出时间、原因和本轮观察写回既有 archive；不新增数据库或状态机。
- 飞书前部展示当轮新增/退出及原因，Core 跟踪记录放后面。不再展示两日入池
  等待提示。盘中新增和退出发送飞书，未发生名单变化不因分数波动重复推送；
  不新增 Bark 盘中投递，既有日报设置保持不变。

历史说明：此前 v12 对 `2026-11-05T01:00:50Z` 的只读演算会让六个旧成员按
IVP／股息门退出；该演算没有写生产，且已被用户随后授权的 v13 评分合同替代。
不能把历史演算当作 v14 当前榜单或已部署结果。v14 不新增历史深跌硬门；
52W 位置仅辅助人工挑选 Fallen Angel 背景，趋势确认本身不证明公司曾为成长明星。

这些是既有 scanner 内的数据修复与用户授权的评分调整，不涉及 Phase 6 / P6-1～P6-4 的
控制面迁移，也不恢复延期的架构工作。PR 验证使用隔离工作树与模拟券商边界；
合并、生产部署及真实通知投递需另行验收。

v14 上线前只读核对：八只标的的券商日线均连续并截至 2026-11-09。
APP/RBLX 为 ARMED，LMND/VST/SOFI/ORCL/GOOGL/AVGO 为 WATCH；这只是价格状态，
不是合约已验证的推荐。2026-11-10 美东凌晨的完整试跑将前一日股票报价标为 frozen，
没有伪装实时行情或修改生产状态/推送。合成路径回归覆盖次日确认、固定突破位、
失效/再入、历史窗口边界、盘中不能确认、基准缺口和 partial 不伪造机会；
没有历史可执行 LEAPS 报价的收益回测，不能据此声称胜率或 alpha。

### 冬夏令时核对（2026-11-02）

2026-11-01 纽约转为 EST（UTC−05:00）。交易时钟继续使用
`America/New_York`，无需手工平移 timer；北京比纽约晚 13 小时，夏令时则晚 12 小时。
普通交易日的现有运行计划对应如下，休市及提前收盘仍服从交易日历：

| 任务 / 时段 | 美东时间 | 北京夏令时对应 | 北京标准时对应 |
| --- | --- | --- | --- |
| SPXW GTH | 前一日 20:15–当日 09:25 | 08:15–21:25 | 09:15–22:25 |
| 首张常规 GTH Desk Map | 前一日 20:30 | 08:30 | 09:30 |
| 盘前地图 | 08:35 / 09:15 | 20:35 / 21:15 | 21:35 / 22:15 |
| RTH | 09:30–16:00 | 21:30–次日 04:00 | 22:30–次日 05:00 |
| Session finalizer | 18:00 | 次日 06:00 | 次日 07:00 |
| RTH daily acceptance | 19:00 | 次日 07:00 | 次日 08:00 |

常规 Desk Map 仍为美东 `:00/:30`，不因时差增加发送次数；`20:15 ET` 是源快照，
首张常规 GTH 消息是 `20:30 ET`。北京时间固定的 `14:00/16:30` 准备任务不移动。
11 月 2 日只读核对中，已安装盘前 timer 的两次时点为北京 `21:35/22:15`；
`systemd-analyze calendar` 也确认 `09:15 ET` 从 10 月 30 日北京 `21:15`
自动变为 11 月 2 日 `22:15`。这证明调度换季正确，不等于行情或手机投递验收。

本次修复沿用 S1 的时间事实与现有报告 owner：写作提示不再固定宣称北京
`21:30` 开盘；开盘倒计时先转 UTC 再相减，避免跨换季周末少算或多算一小时。
回归覆盖春秋切换、秋季重复的 `01:30 ET`、GTH/RTH 边界、源投影 slot 与北京时间标题。
无新增依赖、配置、服务或存储，也不改变交易权限及通知路由。此处记录 PR 改动，
合并部署状态另以发布记录为准。

## 健康检查与数据缺口

```bash
systemctl --user list-units --type=service --state=running 'spx*' 'ibc*'
systemctl --user list-timers --all 'spx*'
systemctl list-units --type=service --state=running 'spx*'
systemctl list-timers --all 'spx-rust*'
```

按顺序核对：进程、provider/token、源时间、BBO、Greeks/OI 独立时间、
`strategy_decision.decision_at`、桌图 projection、桥接接受时间、报告保存、回执。
当前期权没有 Delta 时，不能把不断变化的 BBO 或 collector 心跳写成选腿恢复。

Core 的特征调度通常每 5 秒一轮；周期耗时应从日志测量。2026-10-05 验收时
约 1.3–2.0 秒的构建耗时不等于每 2 秒调度，也不是全天延迟保证。
历史准备共用现有有界后台线程，仍可能与实时工作争用进程资源。

## 数据与容量

IBKR 100 是全部 concurrent ticker lines。常规 RTH validation 目标为
44 hot + 20 rotation；GTH/fallback 目标为 46 hot + 38 rotation，上限 84 option
lines，实际分配还扣除 base、temporary 与 reserve。这不是全链覆盖承诺。

回放使用原始 broker 数据，按当时可用时间重建，不用推送记录挑样本。
Python warning/critical 为低于 10 GiB，Rust 不接受写后低于 10 GiB 的 append；
28 GiB action threshold 只用于提前维护。具体清理权与证据见
[存储说明](storage-plan.md)和[运行配置](runtime-configuration.md)。

## Research context archive (2026-10-24)

The existing macro-calendar refresh task emits a `macro_calendar_snapshot` structured
log after a persisted refresh and when reusing a fresh overlay. The snapshot retains
the complete allowlisted events, per-source coverage, refresh clocks, and partial
failures. It does not change calendar TTLs, entry permissions, strategy parameters,
or notification delivery. Existing provider-control snapshots already go to the
Core journal; no new service, timer, database, or capture process is required.

The home archive exports only these operational inputs from the Core/Worker journal.
The journal receipt time is the earliest time a replay may consume an exported record;
source timestamps and production freshness checks remain authoritative. Historical
missing records must remain explicit gaps. A current calendar snapshot must never be
backdated to authorize a historical trade. Runtime release acceptance is separate
from complete historical-context coverage and from strategy profitability.

### NAS cold archive recovery (2026-11-07)

The existing `spx-nas-archive.timer` remains daily at 08:45 Asia/Shanghai (up to
five minutes jitter). Failed transfers retry in the same service after 15 minutes;
each attempt is bounded to 30 minutes and resumes rsync partial files. The
versioned owner is `scripts/archive-nas.py` in the current runtime bundle. Remote
mount/hash verification precedes local retirement, including newly retained
operational history. See [storage contract](storage-plan.md#repeated-historical-alert-and-nas-recovery-2026-11-07-follow-up).
Historical expired Desk Maps are recorded once by the Worker monitor; they are
not recurring active failures and are never resent as fresh trading information.
