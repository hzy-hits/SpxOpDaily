# Change Brief — P4-2 notification cutover

## Goal

- 所有实时告警、READY 生命周期消息和周期报告进入同一个 `spx.sqlite` 通知队列。
- 用户只在数据库存在真实 Bark/飞书成功 attempt 时看到 `delivered` 证据。

## Current owner

- `notifier/delivery_outbox*.py` 负责最终模板的逐 sink claim/ack/retry。
- `infrastructure/ledger/outbox.py` 与 `application/notifications/outbox_consumer.py`
  负责 `ALERT_CANDIDATE` 的前置 domain-event 队列。
- `spx-spark-notification-delivery.service` 轮询旧最终模板 outbox；Rust ingress 仍可能是
  READY 的最终 fan-out owner。

## Reuse

- 保留 `NotificationEnvelope` 的逻辑事件身份、quiet-window policy、`deliver_trade_push`、
  Rust ingress、消息格式化/LLM review 和 position acknowledgement 行为。
- 复用 P4-1 的 Huey、SQLAlchemy/Alembic 与单线程 worker，不新增依赖或进程。

## Replace/Delete

- 用 `notification_events` 每渠道一行和 `notification_attempts` 取代两套旧 outbox。
- 排空旧队列并完成周末 cutover 后，删除执行计划 P4-2 点名的 claim/receipt/worker 文件
  和旧 `spx-spark-notification-delivery.service`。

## Dependency decision

- 不新增包。SQLAlchemy Core 只执行 `insert/select/update`，Huey 只承担持久任务调度。

## Boundaries

- 进程：仍只有 `spx-worker`，单 worker、单线程、单 periodic scheduler。
- DB：仍只有 P4-1 的两张业务表；一个逻辑消息按冻结 target 拆成多行，
  `idempotency_key = logical_event_id + target`。
- 文件：旧 JSONL missed queue 只在 cutover 排空期读取，不再成为事实源。
- 通知：Bark/飞书逐 target 独立结算；Rust-owned READY 只写一个 `rust_ingress` target，
  不顺带转移 Rust delivery owner。
- 自动交易权限：关闭；通知成功不构成下单授权。

## Preserved safety semantics

- 相同 idempotency key + 相同 payload 是成功重放；不同 payload 是硬碰撞。
- 取消只允许在 transport 开始前原子生效；取消后 late enqueue 不得复活。
- `processing` 表示 transport 已可能开始；进程异常遗留的 `processing` 转
  `uncertain`，不得自动重发。
- 明确可重试错误最多三次；永久错误为 `failed`；只有真实 target success 为
  `delivered`。
- 生命周期 linked message 继承 cause 已冻结的 target，不读取后来修改的渠道配置。
- 外部 receipt 时间来自 `notification_attempts.finished_at`；Rust-owned 事件继续从
  Rust ledger 读取真实 Bark/飞书 receipt，直到 Phase 6 独立切换。

## Minimal path

`producer → event rows + Huey task ids（同一事务边界） → 单 target transport → attempt →
delivered / failed / uncertain`。

`ALERT_CANDIDATE → review task → final template event rows`，不再经过第二个自研 domain
outbox。

## Tests

- duplicate/collision、逐 target 部分成功、三次 retry、timeout→uncertain、取消竞态。
- READY enqueue → worker → Bark/飞书假 transport receipt 的端到端测试。
- 旧 outbox 排空只读查询与新表 row count；cutover 后无双写、无双投递。

## Non-goals

- 不改消息文案、策略 READY 门、LLM provider 或 Rust report owner。
- 不在工作日启停 owner；不保留第二个长期 fallback outbox。
