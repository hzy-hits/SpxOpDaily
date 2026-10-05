# SPX Spark 文档目录与当前状态

本目录覆盖仓库全部 Markdown：根目录、`docs/`、`rust/`、`contracts/`、`site/`、
`tests/fixtures/`。2026-10-05 对 146 份原文档完成定位与导航整理，加本目录共 147 份。
当前运行文档按代码和已安装服务修订；历史报告保留原时点、样本、结果与限制。
这次文档整理没有重新执行所有历史实验，也没有把旧收益升级成生产 edge。

## 阅读顺序与优先级

1. [根 README](../README.md)：当前产品、架构图、入口与限制。
2. [AGENTS.md](../AGENTS.md)：协作、安全边界及用户授权的策略变更。
3. [架构执行基线](architecture-simplification-execution-plan-v1.md)与
   [模块架构](../module-architecture.md)：现有 owner、冻结边界与延期阶段。
4. 下方策略合同索引与相应后续修复，再阅读旧版本设计和历史验收。

目录提供导航，不新建另一套策略授权。冲突时依据用户已明确授权、现行代码合同
及对应后续版本；旧文档中的“当前”“已部署”“通过”只属于其原始记录时点。
发现代码和授权不一致应修正或报告，不能因一篇旧研究有盈利就放宽生产权限。

<a id="current-state"></a>
## 当前运行状态与数据边界

- 生产主机的 Python `spx-core` / `spx-worker` 和两套券商采集继续运行；
  Rust core/bridge/report/delivery 仍承担冻结职责。Phase 6 Rust 退出和 Phase 7
  数据平台重写延期；旧 24h/hot-worker 独立服务不是当前启动入口。
- [部署手册](headless-deployment.md)、[运行时刻表](operations-schedule.md)、
  [通知架构](notification-architecture.md)与 [Rust 运维](../rust/docs/OPERATIONS.md)
  分别说明 user/system services 与 Huey 任务。正常文档发布不重启采集或 Gateway。
- 2026-10-05 数据恢复更新和诊断代码已上线：`710ad982`、`79473823`、`1db03acb`；
  现场验收见[事故记录](desk-data-recovery-2026-10-05.md)。恢复摘要用当前时间重算，
  不必等下一张半小时卡；单项恢复不等于全部字段恢复。
- 同日 12:07–12:15 UTC 检查确认部分 IBKR 合约 **BBO 新鲜但 native Delta 缺失**，
  重连未补齐。此时无法选齐目标 20Δ 短腿，概率计算尚未开始；不是历史样本量问题。
  `data_plane_healthy`、订阅确认、报价、Greeks、OI、策略、投递必须分别核查。
- 随后 12:24 UTC 两条恢复更新均取得三个既有目标成功回执；12:30 定时报告也送达。
  12:45 复核两侧 Delta 已返回，两种翼宽报价就绪，桌图均显示 42 个历史交易日模拟。
  这是新的现场验收，不证明之前重连修复了券商内部原因。行情改变导致换腿时，新结构
  仍需独立准备历史路径，不能无条件沿用另一组概率。12:49 又出现 Delta 过期、
  Put 侧无合格短腿，因此尚未证明数据持续稳定。
- GTH 执行报价仍限 IBKR；不能拿 frozen Schwab、旧 Delta 或未授权本地反算填补。
  字段、报价与概率分别按当前诊断报告，不以文档日期替代实际新鲜度。
- IBKR 总配额包含全部 ticker lines。GTH/fallback/prefetch 目标 46 hot + 38 rotation，
  RTH validation 44 + 20；实际容量和临时 exact-leg 仍受原配额 owner 约束。
- Python 空间 warning/critical 与生产 Rust 写入 reserve 为 10 GiB；28 GiB 是维护
  触发线。去重、清理需原有 manifest 验证，不删除未经验证的原始券商行情。

<a id="strategy-contracts"></a>
## 当前策略合同索引

全局策略版本 `strategy_policy.bootstrap.v69`。最终人工候选仅来自
`build_strategy_decision`；`automatic_ordering=false`，Bark 保持不动。

| 范围 | 现行要点 | 权威版本/后续修复 |
| --- | --- | --- |
| 统一授权 | 候选自身否决继续检查下一名；当前报价和实际动作时钟；研究路径不另建策略出口 | [v2 基线](strategy-signal-engine-v2.md)、[v4 事件观点](strategy-signal-engine-v4.md)、[因果与经济修复](strategy-push-data-audit-2026-09-05.md) |
| RTH 铁鹰 | 09:30≤ET<15:45 当前 20Δ/10 点翼，取消每日一次与 10:00–11:00 限定；冷却、环境与经济门保留 | [v69 入场合同](rth-entry-contract-v69.md) |
| GTH 铁鹰 | 20Δ/10 点翼，扩张回落或独立授权平缓收敛；IBKR 四腿报价/Greeks≤30 秒、BBO skew≤10 秒；保留会话和管理门 | [逐合约历史 v68](gth-entry-recovery-v68.md)、[平缓收敛授权](gth-smooth-entry-authorization-2026-09-15.md) |
| 方向 Spread | 仅已授权价格/量价 setup；通用管理无固定 20 分钟退出；旧 20 分钟训练标签不能证明新持有政策 | [协作合同后续版本](../AGENTS.md)、[数据与策略审计](strategy-push-data-audit-2026-09-05.md) |
| 滚动蝶式 | RTH 11:00 起预测未来 60 分钟，退出绑定冻结 target_at，不固定 15:55；独立于 Stable Pin | [滚动蝶式与 5% 资金流合同](rolling-butterfly-flow-v69.md) |
| 铁鹰概率展示 | 10/20 点翼分别扫描、估计与诊断；20 点翼比较不继承人工入场权限；有效小样本显示频率并标未校准 | [结果与展示合同](condor-results-and-disk-threshold-2026-09-23.md)、[缺失诊断与恢复](desk-data-recovery-2026-10-05.md) |

每套期权都须区分当前价格、历史模拟频率、原始四腿 NBBO 回测与真实成交。
回测、回放及成败归因从原始 IBKR/Schwab 数据按当时可用时间重建；策略卡、
NO_TRADE 或推送记录不能用于挑选样本或生成收益标签，Bark 不参与本次归因。
OI/Gamma、L1 资金流仍是代理，不是真实参与者持仓/完整买卖开平仓。

<a id="document-catalog"></a>
## 全仓 Markdown 清单

每份原文档都链接回本目录并标明定位。定位为“现行”的文件也可能保留明确标注的
历史段落；研究和验收数字均保留其原始分母、管理合同与执行时点。

### 现行运行与参考（30 份）

| 文档 | 路径 |
| --- | --- |
| [SPX Spark 项目协作说明](../AGENTS.md) | `AGENTS.md` |
| [SPX Spark](../README.md) | `README.md` |
| [Shared wire-contract registry](../contracts/README.md) | `contracts/README.md` |
| [SPX Spark 架构简化与第三方能力替代总方案 v1](architecture-simplification-blueprint-v1.md) | `docs/architecture-simplification-blueprint-v1.md` |
| [SPX Spark 架构简化执行方案 v1（面向执行 Agent 的施工图与硬约束）](architecture-simplification-execution-plan-v1.md) | `docs/architecture-simplification-execution-plan-v1.md` |
| [SPX Spark 希腊字母与曝露代理指标定义（exposure_map 规范）](greeks-definitions.md) | `docs/greeks-definitions.md` |
| [Headless Deployment Notes](headless-deployment.md) | `docs/headless-deployment.md` |
| [Market-data capability matrix](market-data-capability-matrix.md) | `docs/market-data-capability-matrix.md` |
| [Market Data Normalization Model](market-data-model.md) | `docs/market-data-model.md` |
| [Unified Market Feature Frames](market-feature-frames.md) | `docs/market-feature-frames.md` |
| [SPX Spark monorepo contract](monorepo-layout.md) | `docs/monorepo-layout.md` |
| [Notification architecture](notification-architecture.md) | `docs/notification-architecture.md` |
| [Operations Schedule](operations-schedule.md) | `docs/operations-schedule.md` |
| [RTH report clock contract](rth-report-clock.md) | `docs/rth-report-clock.md` |
| [RTH runtime clock and end-to-end acceptance](rth-runtime-clock-and-acceptance.md) | `docs/rth-runtime-clock-and-acceptance.md` |
| [Runtime configuration](runtime-configuration.md) | `docs/runtime-configuration.md` |
| [Schwab OAuth through Cloudflare Tunnel](schwab-cloudflare-oauth.md) | `docs/schwab-cloudflare-oauth.md` |
| [Schwab primary and IBKR fallback decision](schwab-primary-ibkr-fallback.md) | `docs/schwab-primary-ibkr-fallback.md` |
| [SPXW raw → merge → feature 时钟契约](spxw-option-clock-contract.md) | `docs/spxw-option-clock-contract.md` |
| [Storage Plan](storage-plan.md) | `docs/storage-plan.md` |
| [SPXW 0DTE Greeks Reference](zero-dte-greeks-reference.md) | `docs/zero-dte-greeks-reference.md` |
| [SPX Spark 模块架构与分层协议](../module-architecture.md) | `module-architecture.md` |
| [SPX Spark Core collaboration guide](../rust/AGENTS.md) | `rust/AGENTS.md` |
| [SPX Spark Core](../rust/README.md) | `rust/README.md` |
| [SPX Spark Core architecture](../rust/docs/ARCHITECTURE.md) | `rust/docs/ARCHITECTURE.md` |
| [Operations guide](../rust/docs/OPERATIONS.md) | `rust/docs/OPERATIONS.md` |
| [Research boundary](../rust/docs/RESEARCH_BOUNDARY.md) | `rust/docs/RESEARCH_BOUNDARY.md` |
| [State-machine contract](../rust/docs/STATE_MACHINES.md) | `rust/docs/STATE_MACHINES.md` |
| [SPXW Notification Image Entry](../site/spxw-surface/README.md) | `site/spxw-surface/README.md` |
| [Frozen runtime defaults for unit/architecture tests.](../tests/fixtures/README.md) | `tests/fixtures/README.md` |

### 版本策略与功能合同（20 份）

| 文档 | 路径 |
| --- | --- |
| [v67 门禁与缺失数据修复（S1/S3）](gate-signal-recovery-v67.md) | `docs/gate-signal-recovery-v67.md` |
| [GTH 因果路径 Rank](gth-causal-path-ranks.md) | `docs/gth-causal-path-ranks.md` |
| [GTH 入场证据修复与平缓收缩检验（v68）](gth-entry-recovery-v68.md) | `docs/gth-entry-recovery-v68.md` |
| [User-authorized GTH smooth-convergence iron condor](gth-smooth-entry-authorization-2026-09-15.md) | `docs/gth-smooth-entry-authorization-2026-09-15.md` |
| [GTH 已确认水平优先级修复与铁鹰长持有复核](gth-source-priority-recovery.md) | `docs/gth-source-priority-recovery.md` |
| [Wall/Flip Level-Decision Shadow](level-decision-shadow.md) | `docs/level-decision-shadow.md` |
| [Wall/Flip Realtime Repricing](level-trigger-pricing.md) | `docs/level-trigger-pricing.md` |
| [v69：未来60分钟蝶式与5%资金流覆盖](rolling-butterfly-flow-v69.md) | `docs/rolling-butterfly-flow-v69.md` |
| [RTH 入场合同与缺失观察诊断（v69）](rth-entry-contract-v69.md) | `docs/rth-entry-contract-v69.md` |
| [RTH 5 分钟市场状态与 Spring Gamma v3 Shadow](rth-five-minute-market-state-v1.md) | `docs/rth-five-minute-market-state-v1.md` |
| [RTH Signal Unstarve v1](rth-signal-unstarve-v1.md) | `docs/rth-signal-unstarve-v1.md` |
| [SPXW 执行价差分特征与决策上下文接入设计 v1](spxw-strike-differential-decision-context-v1.md) | `docs/spxw-strike-differential-decision-context-v1.md` |
| [SPXW 执行价算子扩展与模型研究附录 v1](spxw-strike-operator-extension-addendum-v1.md) | `docs/spxw-strike-operator-extension-addendum-v1.md` |
| [Strategy Edge Model v1](strategy-edge-model-v1.md) | `docs/strategy-edge-model-v1.md` |
| [SPX Spark 0DTE 算法与策略信号引擎设计 v2](strategy-signal-engine-v2.md) | `docs/strategy-signal-engine-v2.md` |
| [SPX Spark 0DTE 策略信号引擎 v3 P1/P2 设计合同](strategy-signal-engine-v3-p1p2-design.md) | `docs/strategy-signal-engine-v3-p1p2-design.md` |
| [SPX Spark 0DTE 策略信号引擎 v3 P1/P2 执行方案](strategy-signal-engine-v3-p1p2-execution.md) | `docs/strategy-signal-engine-v3-p1p2-execution.md` |
| [SPX Spark 0DTE 策略信号引擎实施合同 v3](strategy-signal-engine-v3.md) | `docs/strategy-signal-engine-v3.md` |
| [SPX Spark 策略信号引擎 v4.0：观点到事件结算价差](strategy-signal-engine-v4.md) | `docs/strategy-signal-engine-v4.md` |
| [SPX structure-first signal system](structure-signal-vnext.md) | `docs/structure-signal-vnext.md` |

### 事故修复与时点验收（15 份）

| 文档 | 路径 |
| --- | --- |
| [ATM 历史字段丢失导致策略输入永久缺失](atm-history-field-incident-2026-09-08.md) | `docs/atm-history-field-incident-2026-09-08.md` |
| [2026-09-25 桥接磁盘故障恢复](bridge-disk-recovery-2026-09-25.md) | `docs/bridge-disk-recovery-2026-09-25.md` |
| [铁鹰结果、样本漏斗与 10 GiB 空间线](condor-results-and-disk-threshold-2026-09-23.md) | `docs/condor-results-and-disk-threshold-2026-09-23.md` |
| [铁鹰概率摘要与冗余研究副本回收](desk-condor-probabilities-and-storage-2026-09-23.md) | `docs/desk-condor-probabilities-and-storage-2026-09-23.md` |
| [Desk Map 数据恢复即时更新（2026-10-05）](desk-data-recovery-2026-10-05.md) | `docs/desk-data-recovery-2026-10-05.md` |
| [Desk Map decision clock recovery — 2026-09-17](desk-decision-clock-recovery-2026-09-17.md) | `docs/desk-decision-clock-recovery-2026-09-17.md` |
| [Desk Map 模型身份失败回退与空ATM崩溃修复](desk-map-runtime-recovery-2026-09-10.md) | `docs/desk-map-runtime-recovery-2026-09-10.md` |
| [Desk pipeline outage visibility and bounded restart recovery](desk-pipeline-monitor-2026-09-21.md) | `docs/desk-pipeline-monitor-2026-09-21.md` |
| [ES live rollover recovery — 2026-09-15](es-live-rollover-2026-09-15.md) | `docs/es-live-rollover-2026-09-15.md` |
| [资金流漏识别恢复（S1/S3、生产故障修复）](flow-exhaustion-recovery-2026-09-14.md) | `docs/flow-exhaustion-recovery-2026-09-14.md` |
| [Session queries, historical preparation and liquidation cash flows](hot-path-data-contracts-2026-09-15.md) | `docs/hot-path-data-contracts-2026-09-15.md` |
| [2026-09-08 宏观日历路径错配](macro-calendar-path-incident-2026-09-08.md) | `docs/macro-calendar-path-incident-2026-09-08.md` |
| [SPX Spark Operations Acceptance - 2026-07-12](operations-acceptance-2026-07-12.md) | `docs/operations-acceptance-2026-07-12.md` |
| [RTH rollover and latency recurrence — 2026-09-15](rth-rollover-latency-repair-2026-09-15.md) | `docs/rth-rollover-latency-repair-2026-09-15.md` |
| [策略查询阻塞与桌图时间一致性修复（2026-09-14）](strategy-query-latency-recovery-2026-09-14.md) | `docs/strategy-query-latency-recovery-2026-09-14.md` |

### 历史研究与实验（49 份）

| 文档 | 路径 |
| --- | --- |
| [告警优化建议（修正版，2026-07-18）](alert-optimization-from-backtest-2026-07-18.md) | `docs/alert-optimization-from-backtest-2026-07-18.md` |
| [Alert Reasoning and SPXW Strategy Model Review](alert-reasoning-and-strategy-review.md) | `docs/alert-reasoning-and-strategy-review.md` |
| [Call / Put Skew Spread Shadow](call-skew-spread-shadow.md) | `docs/call-skew-spread-shadow.md` |
| [入场前预测条件收益：路径匹配与 IV 外推的反证](conditional-option-value-exploration-2026-09-06.md) | `docs/conditional-option-value-exploration-2026-09-06.md` |
| [0DTE Convexity Idea Radar](convexity-idea-radar.md) | `docs/convexity-idea-radar.md` |
| [原始行情正收益候选：向上开盘区间接受＋20 分钟 Call Spread](directional-edge-signal-validation-2026-09-05.md) | `docs/directional-edge-signal-validation-2026-09-05.md` |
| [SPXW Exposure Cockpit 数据与表现复核](exposure-cockpit-data-and-ui-audit-2026-07-20.md) | `docs/exposure-cockpit-data-and-ui-audit-2026-07-20.md` |
| [SPXW Exposure Trading Cockpit 验收报告](exposure-cockpit-validation-2026-07-19.md) | `docs/exposure-cockpit-validation-2026-07-19.md` |
| [从箱体出发：Gamma聚集与价格反应的第一层验收](gamma-box-mechanism-audit-2026-09-07.md) | `docs/gamma-box-mechanism-audit-2026-09-07.md` |
| [GTH convergence visibility and gate diagnosis](gth-convergence-diagnostics-2026-09-15.md) | `docs/gth-convergence-diagnostics-2026-09-15.md` |
| [IBKR API Research Notes](ibkr-api-research.md) | `docs/ibkr-api-research.md` |
| [铁鹰贷记与短腿位置联合选择：探索结果与接入方案](ic-credit-placement-exploration-2026-09-10.md) | `docs/ic-credit-placement-exploration-2026-09-10.md` |
| [ICT、Gamma 位置与三类期权结构：原始行情回放](ict-gamma-position-replay-2026-09-05.md) | `docs/ict-gamma-position-replay-2026-09-05.md` |
| [7640/7650/7730/7740贷记2.15核查](iron-condor-credit-audit-2026-09-08.md) | `docs/iron-condor-credit-audit-2026-09-08.md` |
| [SPX 短到期 Jade Lizard：结构选择与证据边界](jade-lizard-research-2026-09-08.md) | `docs/jade-lizard-research-2026-09-08.md` |
| [MA50/MA200 日内四态审计（2026-07-25）](ma50-ma200-regime-audit-2026-07-25.md) | `docs/ma50-ma200-regime-audit-2026-07-25.md` |
| [MrMicopedia Agent Guidance](micopedia-agent-guidance.md) | `docs/micopedia-agent-guidance.md` |
| [MrMicopedia Background Knowledge](micopedia-background-knowledge.md) | `docs/micopedia-background-knowledge.md` |
| [铁鹰与蝶式：因子库、冻结假设与原始行情验算](option-factor-hypotheses-2026-09-06.md) | `docs/option-factor-hypotheses-2026-09-06.md` |
| [用新的交易机制检验铁鹰与蝶式，而非继续叠加因子](option-mechanism-experiments-2026-09-06.md) | `docs/option-mechanism-experiments-2026-09-06.md` |
| [Order-map pricing backtest (2026-07-13)](order-map-pricing-backtest-2026-07-13.md) | `docs/order-map-pricing-backtest-2026-07-13.md` |
| [原始券商行情探索：开盘向上接受与价格失效退出](price-driven-signal-discovery-2026-09-05.md) | `docs/price-driven-signal-discovery-2026-09-05.md` |
| [SPX 0DTE 概率模型：从市场隐含分布到可执行净收益](probability-model-p-vs-q-execution-design.md) | `docs/probability-model-p-vs-q-execution-design.md` |
| [从原始 IBKR / Schwab 行情重建策略，2026-09-05](raw-broker-strategy-audit-2026-09-05.md) | `docs/raw-broker-strategy-audit-2026-09-05.md` |
| [Regime 切换是否使策略状态失效：因果识别与退出对照](regime-transition-attribution-2026-09-06.md) | `docs/regime-transition-attribution-2026-09-06.md` |
| [Confirmed Breakout 执行回放（2026-08-08）](research/confirmed-breakout-execution-replay-2026-08-08.md) | `docs/research/confirmed-breakout-execution-replay-2026-08-08.md` |
| [ES ICT/SMC 因果事件研究 · 2026-08-31](research/ict-liquidity-event-study-2026-08-31.md) | `docs/research/ict-liquidity-event-study-2026-08-31.md` |
| [Spring Gamma v3 forward audit · 2026-08-28](research/spring-gamma-v3-forward-audit-2026-08-28.md) | `docs/research/spring-gamma-v3-forward-audit-2026-08-28.md` |
| [Strategy Decision v2 · S2/S3 Replay · 2026-08-07](research/strategy-decision-vertical-replay-2026-08-07.md) | `docs/research/strategy-decision-vertical-replay-2026-08-07.md` |
| [Strategy Engine v3 · Pass-B 回填与冻结验收 · 2026-08-05..08](research/strategy-engine-v3-freeze-acceptance-2026-08-09.md) | `docs/research/strategy-engine-v3-freeze-acceptance-2026-08-09.md` |
| [Strike Differential Context v1 · SDCTX-1–5 验收 · 2026-08-10](research/strike-differential-context-acceptance-2026-08-10.md) | `docs/research/strike-differential-context-acceptance-2026-08-10.md` |
| [回放正确性与测试简化审查（2026-09-05）](review-replay-correctness-and-simplification-2026-09-05.md) | `docs/review-replay-correctness-and-simplification-2026-09-05.md` |
| [RTH 状态机与数据链路质量审计（2026-07-24）](rth-state-data-quality-audit-2026-07-24.md) | `docs/rth-state-data-quality-audit-2026-07-24.md` |
| [Schwab GTH market-data capability audit](schwab-gth-streaming-capability.md) | `docs/schwab-gth-streaming-capability.md` |
| [SPX 3–5DTE 海鸥与玉蜥蜴：原始入场、报价退出与官方结算](seagull-jade-3-5dte-research-2026-09-08.md) | `docs/seagull-jade-3-5dte-research-2026-09-08.md` |
| [Spring Gamma v3 RTH 八变量接入审计](spring-gamma-v3-rth-eight-feature-integration-audit-2026-07-24.md) | `docs/spring-gamma-v3-rth-eight-feature-integration-audit-2026-07-24.md` |
| [SPX 0DTE 全信号回测与 RTH 漏斗复核](spx-0dte-all-signal-backtest-2026-07-23.md) | `docs/spx-0dte-all-signal-backtest-2026-07-23.md` |
| [Steven SPX Options Framework 集成规格（exposure_map + strategy/steven）](steven-framework-integration.md) | `docs/steven-framework-integration.md` |
| [到达后停留、分阶段铁鹰与因果结构选择](strategy-adaptations-replay-2026-09-06.md) | `docs/strategy-adaptations-replay-2026-09-06.md` |
| [原始券商快照去重与策略收益归因](strategy-attribution-and-dedup-2026-09-05.md) | `docs/strategy-attribution-and-dedup-2026-09-05.md` |
| [SPX 0DTE：GTH、Put/Down 与入场质量修复验证](strategy-backtest-validation-2026-07-18.md) | `docs/strategy-backtest-validation-2026-07-18.md` |
| [逐日宏观环境、结构价格与退出归因](strategy-environment-attribution-2026-09-05.md) | `docs/strategy-environment-attribution-2026-09-05.md` |
| [策略推送、真实报价与代码收口验收](strategy-push-data-audit-2026-09-05.md) | `docs/strategy-push-data-audit-2026-09-05.md` |
| [取消固定持有要求与铁鹰 Delta／翼宽／波动率验收](strategy-relaxed-exit-condor-volatility-2026-09-05.md) | `docs/strategy-relaxed-exit-condor-volatility-2026-09-05.md` |
| [策略、K3 改动与缺口复盘（第三次修正版，2026-07-18）](strategy-review-and-gaps-2026-07-18.md) | `docs/strategy-review-and-gaps-2026-07-18.md` |
| [SPX 0DTE：每日 1–2 笔目标的门禁与实时性诊断](strategy-throughput-and-latency-2026-07-18.md) | `docs/strategy-throughput-and-latency-2026-07-18.md` |
| [改变结构与触发机制：宽蝶、双向失败铁鹰](structure-and-auction-experiments-2026-09-06.md) | `docs/structure-and-auction-experiments-2026-09-06.md` |
| [Trend Spread Framework](trend-spread-framework.md) | `docs/trend-spread-framework.md` |
| [量化概率层与 SPY 墙位对照设计(实施规格)](../quant-probability-design.md) | `quant-probability-design.md` |

### 历史设计与迁移基线（29 份）

| 文档 | 路径 |
| --- | --- |
| [0DTE strategy lifecycle](0dte-strategy-lifecycle.md) | `docs/0dte-strategy-lifecycle.md` |
| [ADR-0001: SPX 0DTE 市场数据采用 Oracle 单机优先存储](adr/0001-oracle-first-market-data-storage.md) | `docs/adr/0001-oracle-first-market-data-storage.md` |
| [SPX Spark Architecture Plan](architecture-plan.md) | `docs/architecture-plan.md` |
| [SPX Spark 架构逻辑梳理与问题指正](architecture-review-kangyu.md) | `docs/architecture-review-kangyu.md` |
| [Change Brief — P4-1 `spx-worker`](change-brief-p4-1-worker.md) | `docs/change-brief-p4-1-worker.md` |
| [Change Brief — P4-2 notification cutover](change-brief-p4-2-notification-cutover.md) | `docs/change-brief-p4-2-notification-cutover.md` |
| [Change Brief — P5-1 / S6 operational DB and strategy-decision owner](change-brief-p5-1-s6-operational-db.md) | `docs/change-brief-p5-1-s6-operational-db.md` |
| [Change Brief — P5-2 CLI consolidation and transitional TOML cutover](change-brief-p5-2-cli-data.md) | `docs/change-brief-p5-2-cli-data.md` |
| [SPX Spark Data Platform](data-platform-design.md) | `docs/data-platform-design.md` |
| [Data Source And Runtime Decision Memo](data-source-decision.md) | `docs/data-source-decision.md` |
| [设计缺陷 Review 与改进计划](design-review.md) | `docs/design-review.md` |
| [挂单地图(Order Map)实施规格](order-map-design.md) | `docs/order-map-design.md` |
| [SPX Spark 首个 RTH 前重构实施计划](pre-rth-refactor-implementation-plan.md) | `docs/pre-rth-refactor-implementation-plan.md` |
| [SPX Spark 工程化重构与验收规格](refactor-architecture-acceptance-plan.md) | `docs/refactor-architecture-acceptance-plan.md` |
| [kangyu 评审修复·批次 1:正确性修复(实施规格)](review-fix-batch1-design.md) | `docs/review-fix-batch1-design.md` |
| [评审修复·批次 2:收盘换月 + 量化修正(S1/S2/S4/S5/S8)(实施规格)](review-fix-batch2-design.md) | `docs/review-fix-batch2-design.md` |
| [Sampling Engine Design](sampling-engine-design.md) | `docs/sampling-engine-design.md` |
| [Schwab 宽链与 Option Hot Lane 设计](schwab-wide-chain-hot-lane-design.md) | `docs/schwab-wide-chain-hot-lane-design.md` |
| [simplification-owner-inventory](simplification-owner-inventory.md) | `docs/simplification-owner-inventory.md` |
| [SPX Spark reliability hardening checkpoint](superpowers/checkpoints/2026-07-10-spx-spark-reliability-planning.md) | `docs/superpowers/checkpoints/2026-07-10-spx-spark-reliability-planning.md` |
| [SPX Spark reliability hardening design](superpowers/specs/2026-07-10-spx-spark-reliability-hardening-design.md) | `docs/superpowers/specs/2026-07-10-spx-spark-reliability-hardening-design.md` |
| [SPX Spark 可靠性加固设计](superpowers/specs/2026-07-10-spx-spark-reliability-hardening-design.zh.md) | `docs/superpowers/specs/2026-07-10-spx-spark-reliability-hardening-design.zh.md` |
| [IBKR 会话抢占（Session Preemption）数据一致性加固设计](superpowers/specs/2026-07-11-ibkr-session-preemption-hardening.zh.md) | `docs/superpowers/specs/2026-07-11-ibkr-session-preemption-hardening.zh.md` |
| [SPX Spark 规划：数据预算分配 + Steven 框架 / 高阶希腊落地](superpowers/specs/2026-07-11-steven-framework-data-budget-plan.zh.md) | `docs/superpowers/specs/2026-07-11-steven-framework-data-budget-plan.zh.md` |
| [Steven 框架 Phase 2/3/4 测试用例矩阵与验收标准](superpowers/specs/2026-07-11-steven-test-acceptance-matrix.zh.md) | `docs/superpowers/specs/2026-07-11-steven-test-acceptance-matrix.zh.md` |
| [盘前地图推送 + 盘后复盘推送 + agent 解读风格(实施规格)](../morning-map-review-push-design.md) | `morning-map-review-push-design.md` |
| [Migration plan](../rust/docs/MIGRATION.md) | `rust/docs/MIGRATION.md` |
| [慢速轮询 lane:VIX 族与上下文 ETF 改为轮询,腾行情线给期权链(实施规格)](../slow-poll-lane-design.md) | `slow-poll-lane-design.md` |
| [SPY 期权直采(IBKR)与微信离线消息时间线汇总(实施规格)](../spy-lane-missed-digest-design.md) | `spy-lane-missed-digest-design.md` |

### 退役界面与固定历史页面（3 份）

| 文档 | 路径 |
| --- | --- |
| [SPXW Live Session Surface（历史记录）](spxw-live-session-surface-2026-07-19.md) | `docs/spxw-live-session-surface-2026-07-19.md` |
| [SPXW 0DTE Decision Surface（历史记录）](spxw-surface-dashboard.md) | `docs/spxw-surface-dashboard.md` |
| [SPX Strategy Review Site](../site/strategy-review/README.md) | `site/strategy-review/README.md` |

本目录自身：`docs/README.md`。后续增删 Markdown 时同步本清单；更新历史文档时保留原结果和时点，避免把目录维护当成重新验收。
