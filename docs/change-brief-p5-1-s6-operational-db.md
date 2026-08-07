# Change Brief — P5-1 / S6 operational DB and strategy-decision owner

## Goal

- `spx.sqlite` 成为操作事实的唯一目标数据库，先承接实时 `StrategyDecision` 与两腿/三腿执行快照。
- JSON 继续作为 dashboard/人工检查的只读导出，不再授权 READY。
- 只有决策与合约腿在同一事务中写入成功，策略 READY 才可进入通知队列。

## Current owner

- `application/market_features/service.py` 把最新策略决策写到
  `latest/strategy_decision.json`，随后直接尝试入通知队列。
- Order Map 把 `strategy_decision` 嵌在 payload/audit artifact 中，没有统一 SQL owner。
- 可选 data-platform 仍默认使用 `runtime/research-ledger.sqlite3`，并由
  `data_platform/adapters/sqlite_ledger.py` 自行执行 SQL migration；该路径当前默认关闭。

## Reuse

- 复用 P4 的 `spx.sqlite`、Alembic、SQLAlchemy Core 与数据库 engine 创建函数。
- 复用现有 `strategy_decision.v1`、稳定 `decision_id`、`policy_version`、候选合约
  `source_at` 和 manual-only 权限。
- 表列沿用既有 data-platform record 的时间语义，保留 `available_at <= decision_at`
  与 `quote_source_at <= quote_available_at` 约束。

## Replace/Delete

- 本增量把 live StrategyDecision owner 从 JSON 切到 `decisions`/`decision_legs`；
  `strategy_decision.json` 仅在 SQL commit 后作为 projection 导出。
- P5 后续 owner cutover 会把旧 research ledger 的 event/outcome/manifest 写入迁到同一
  Alembic schema，随后删除其 `schema_migrations`、SQL migration 文件和手写 DDL。
- 不保留 StrategyDecision 的 DB/JSON 双写权限：SQL 失败时 JSON 可诊断但 READY 不投递。

## Dependency decision

- 不新增依赖。只使用已批准的 Alembic 与 SQLAlchemy Core。
- 不建 ORM、relationship、session factory、repository 或第二套 migrator。

## Boundaries

- 进程：不增加进程、worker 或 timer。
- DB：只扩展现有 `spx.sqlite`；`huey.sqlite` 不变。
- 文件：`latest/strategy_decision.json` 降级为只读 export。
- 通知：候选必须先成功持久化，随后才可调用现有 `enqueue_strategy_decision`。
- 自动交易权限：保持关闭；数据库记录和通知成功都不构成下单授权。

## Minimal path

`fresh market/options facts → StrategyDecision → one SQL transaction
 (decision + legs) → JSON export → existing unified notification queue → Bark/Feishu`。

NO_TRADE 同样落一行 decision，但没有 decision legs，也不会进入 READY lane。

## Migration and cutover

1. Alembic `0002` 新建 `sessions`、`events`、`decisions`、`decision_legs`、`outcomes`、
   `provider_incidents`、`compaction_manifests`。
2. 在隔离 data root 执行 fresh upgrade 与 `0001 → 0002` upgrade，校验约束和幂等写入。
3. Oracle 周末窗口先备份/查询旧 owner，执行 `alembic upgrade head`，再启新 Core/Worker。
4. 核对 DB 决策时间、JSON export、通知 event/attempt 与 Bark/飞书外部 receipt 是同一机会。
5. 回滚点为 cutover 前 tag 与 `spx.sqlite` 备份；回滚代码时保留新增表，不做 destructive downgrade。

## Tests

- fresh Alembic upgrade 只产生计划内九张业务表（通知两表 + 七张 operational 表）
  和 Alembic 自管的 `alembic_version`。
- NO_TRADE 和两腿 candidate 的事务、幂等、冲突、时间约束测试。
- DB 失败时 READY 不入队；DB 成功后才导出并入队。
- Oracle 实际验证 unit/restart、数据新鲜度、decision/legs、notification attempts 和
  Bark/飞书 receipt。

## Non-goals

- 本增量不训练模型、不修改策略阈值、不改变 READY 选择规则或消息文案。
- 不在 RTH/GTH 运行中切 owner；不迁移 Rust ledger/report owner。
- 不为 provider incidents 或 compaction 增加新状态机；后续只迁现有 owner。
