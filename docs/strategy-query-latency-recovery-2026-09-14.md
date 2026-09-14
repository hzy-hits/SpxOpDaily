# 策略查询阻塞与桌图时间一致性修复（2026-09-14）

对应架构执行计划 S1/S3（Phase 8 策略出口）；属于已有路径的生产故障修复，不改变策略授权合同。

## 已确认事实

- 生产基线 `314515ca`，Core 存活、Schwab SPX/ES 采样正常，但 market_features 周期的 strategy_build 分别耗时约 647、654、1395 秒。
- 线程栈停在 `recent_selected_strategy_cards` 的 SQL 执行，调用来自 `_rth_committed_direction`。这不是滚动蝶式模型计算；观察时尚未到 11:00 ET。
- 13 GB 的 operational SQLite 存在交易日索引，但查询计划选用 `ix_decisions_strategy_decision_at (strategy_name=?)`，扫描同策略全部历史后才筛选 session。
- 相同本日查询指定既有 `ix_decisions_session` 后，计数为零，首次约 1.47 秒，应用函数热缓存约 0.005 秒。仅用于运行故障诊断，不作回测样本或收益证据；未读取 Bark 数据。
- 10:00 ET 桌图的行情时间更新，但策略仍是 09:35 ET。旧 `committed_strategy_decision` 只拒绝未来时间与内容哈希错配，没有最大年龄。

## 修复

1. 原会话查询用绑定参数，并明确使用已有交易日索引。过滤条件与返回含义不变；无迁移、无新索引，不执行 ANALYZE 或重写历史数据。
2. 冻结决策沿用现有 `opportunity_ttl_seconds=300` 的上限，从 `decision_at` 计算；更新 `available_at` 不能续命。此规则不是报价有效期，原 exact-BBO 新鲜度规则保持。
3. 报告继续复用 Core 导出；不可用时保留引用 ID/时间作诊断，但清空策略内容。桌图输出 PAUSED/DEGRADED、无执行方向/目标，并明确不能沿用旧结论。行情结构仍可作观察。
4. 不修改 Bark、IBKR 冲突处理、阈值、TP/SL、策略候选范围或 Rust wire。10197 时继续退让用户交易会话。

## 验收与限制

- 数据库测试加入 3000 条其他 session 的大 payload，在 SQLite VM 工作量上限内验证目标 session 查询，防止退回全历史扫描；不依赖墙钟计时。
- 决策测试覆盖 299/300/1800 秒、新发布时间不续期、已有哈希校验；报告集成验证新行情不会恢复过期决策。
- RTH/GTH 桌图测试验证缺失有效策略时降级，并保留独立持仓管理提示。
- 修复吞吐不等于验证交易 edge；积累所需同合约历史、报价与全部原入场条件仍必要。运行恢复证据以部署后的日志/源时间为准。

复杂度：生产文件新增/删除 0/0，修改 5 个，净增加 7 行；依赖、配置键、服务/timer、数据库/表均 0 增减。移除跨全部历史的会话扫描及无限期信任旧策略的路径，没有新增兼容分支。

发布验证：针对性 254 项通过；全量 Python 3439 项通过（两条已有弃用警告）；报告集成参数扩展后 4 项通过。Ruff、Import Linter、Rust fmt/clippy/workspace tests 与 diff 检查通过。
