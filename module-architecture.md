# SPX Spark 模块架构与分层协议

状态: 2026-07-12 与 pre-RTH 实施计划对齐。本文档是模块划分的权威来源；新增模块或
import 前先对照本文分层规则与 `tests/architecture/test_module_registry.py`。
违反分层规则或遗漏模块登记会直接挂测试。

配套:
- 守护测试: `tests/architecture/test_module_registry.py`（兼容入口
  `tests/test_architecture.py`）
- 验收计划: `docs/refactor-architecture-acceptance-plan.md`
- 首个 RTH 前实施计划: `docs/pre-rth-refactor-implementation-plan.md`
- Schwab 宽链与 hot lane: `docs/schwab-wide-chain-hot-lane-design.md`
- 进度清单: `artifacts/refactor-acceptance/inventory/report.json`

## 1. 分层总览（低层在下，依赖只允许指向同层或更低层）

```
L5 orchestration   application/*（realtime / order_map / shock / morning_map /
                   notifications / runtime）, service_loop, maintenance,
                   post_close_review, latest_state,
                   morning_map / order_map / intraday_shock 兼容门面
L4 alerting        alert_engine/*, notifier/*, position_alerts, alert_profile,
                   data_platform, greek_shadow, intraday_event_outcomes,
                   infrastructure/*
L3 analytics       analytics/*（options / greeks）, options_map/*, features,
                   greek_reference, iv_surface, market_context, human_focus,
                   strategy/*, intraday_strategy, steven_validation
L2 providers       ibkr/*（含 ibkr/stream/*）, schwab/*, hyperliquid/*,
                   polymarket/*, mock_collector,
                   provider_failover_controller（temporary）
L1 infrastructure  config, storage, state_io, sampling, runtime_mode,
                   provider_adapter, provider_failover, position_events
L0 foundation      marketdata, market_calendar, alert_model, runtime_config,
                   domain/*, settings/*
```

目标依赖方向见验收计划。`tests/architecture/test_module_registry.py` 的
`LAYERS` 表必须与上表同步；**未登记生产模块必须失败**。

分层规则（守护测试强制执行）:

1. 任何模块只能 import 同层或更低层的模块。
2. L0 模块不得 import 任何 spx_spark 内部模块（彼此之间也不行）。
   `config.py` 位于 L1，可从 `market_calendar.py` 重导兼容日历入口。
3. provider 包（L2）之间不得互相 import。
4. 只有 L5 orchestration 可以跨层拼装（import 任意层）。
5. 非 provider / 非 L5 模块不得 import provider 包；例外白名单:
   `position_alerts` → `ibkr.position_watcher`。

## 2. 各层职责与当前包布局

### L0 foundation
- `domain/` — 有界上下文枚举（SignalMode / DeliveryMode / ReplanMode 等）
- `settings/` — composition-root 类型化设置（stdlib/YAML only）
- `marketdata.py`、`market_calendar.py`、`alert_model.py`、`runtime_config.py`

### L1 infrastructure
- `config.py` — Settings + env helpers + notification/outbox/shock delivery flags
- `storage.py`、`state_io.py`、`sampling.py`、`runtime_mode.py`
- `provider_adapter.py`、`provider_failover.py`、`position_events.py`

### L2 providers
- `ibkr/` — collector / gateway / adapter；流式实现在 `ibkr/stream/*`
  （models / runtime_machine / replan_machine / subscriptions / cache /
  flush / session / supervisor / cli）
- `schwab/`、`hyperliquid/`、`polymarket/`、`mock_collector`
- `provider_failover_controller.py` — temporary control-document consumer

### L3 analytics
- `analytics/options/` — models（含 `DensityQuality`）、chain、quality、pricing、
  probability、density、exposure、levels、service
- `analytics/greeks/` — black_scholes / higher_order 等纯核
- `options_map/` — LatestState orchestration + CLI；`__init__.py` 为兼容门面（≤150 行）
- `features/`、`greek_reference.py`、`iv_surface.py`、`market_context.py`、
  `human_focus.py`、`strategy/*`、`intraday_strategy.py`、`steven_validation.py`

### L4 alerting / platform
- `alert_engine/` — constants / rules_* / evaluator / cli（候选评估；投递分离）
- `notifier/` — model / policy / state / prompts / sinks / pipeline
- `position_alerts.py`、`alert_profile.py`
- `data_platform/`、`greek_shadow.py`、`intraday_event_outcomes.py`
- `infrastructure/` — ledger / outbox / projection adapters

### L5 orchestration / application
- `application/realtime/` — RealtimeEngine、`OptionsAnalyticsKernel`、composition、
  alert evaluator、health（STARTING/WARMING/READY fail-closed）
- `application/notifications/` — outbox consumer、deliver、settlement
- `application/order_map/` — models / pricing / spot / candidates / machines /
  render / delivery / service；`order_map.py` 为门面
- `application/shock/` — models / machine / evaluator / delivery / service；
  `intraday_shock.py` 为门面（保留 `shock_direct_delivery_enabled` 快路径）
- `application/morning_map/` — build / render / delivery / state / service；
  `morning_map.py` 为门面
- `application/runtime/` — service_loop settings / registry / runner / scheduler
- `service_loop.py`、`maintenance.py`、`post_close_review.py`、`latest_state.py`

## 3. 兼容门面预算

| 门面 | 预算（非空行） | 实现包 |
| --- | --- | --- |
| `options_map/__init__.py` | ≤150 | `analytics.options` + `options_map.*` |
| `order_map.py` | ≤100 | `application.order_map` |
| `intraday_shock.py` | ≤50 | `application.shock` |
| `morning_map.py` | ≤50 | `application.morning_map` |
| `ibkr/stream_collector.py` | ≤100 | `ibkr.stream` |
| `service_loop.py` | 调度门面 | `application.runtime` |

门面只允许 re-export、参数转换与 deprecation；不得保留第二套业务逻辑。
守护: `tests/architecture/test_facade_size_budget.py`。

## 4. Phase 闸门（摘要）

| Phase | 状态 | 说明 |
| --- | --- | --- |
| 0–4 | PARTIAL | 主要门面已拆，但 typed settings 热路径与大文件债务未完成 |
| 5 realtime | GO (P1) | 生产默认 `OptionsAnalyticsKernel`；`analytics_ok` 要求显式 SUCCESS；STARTING/WARMING + fail-closed |
| 6 outbox | GO-CANDIDATE | outbox/幂等消费已实现，仍需与真实 analytics 做 RTH 集成验证 |
| 7 UDS | NO-GO | 缺 §10.2 连续 RTH 指标证据 |
| 8 Rust | NO-GO | 缺 §11 生产级 benchmark / profiler 证据 |
| §9.2 RTH density golden | NO-GO | 缺实盘 session shadow 语料 |
| P1-C settings | PARTIAL | import-time `runtime_value`=0；残留按文件递减预算（见 architecture test） |

权威进度见 `artifacts/refactor-acceptance/inventory/report.json`。

## 5. 日常约定

- 新模块先在 §1 / `LAYERS` 定层，再写代码；守护测试挂了不许改白名单蒙混。
- provider 字段名知识只允许出现在对应 `*/adapter.py`。
- 跨层共享数据结构下沉到 L0（参考 `alert_model.py` / `domain.state_machines`）。
- analytics 纯核禁止 import `storage` / `config` / `notifier` /
  `alert_engine` / `service_loop`（见 `tests/architecture/test_pure_boundaries.py`）。
- `runtime_value()` 新调用点禁止扩张；允许名单见
  `tests/architecture/test_runtime_value_allowlist.py`。

## 6. Pre-RTH 新模块归属

下列模块是首个 RTH 前计划新增的边界。文件落地时必须同步
`tests/architecture/test_module_registry.py`，不得通过扩大例外白名单绕过依赖规则。

| 模块 | 层 | 职责 | 禁止依赖 |
| --- | --- | --- | --- |
| `analytics/options/snapshot.py` | L3 | 从已归一化 snapshot 构造 chain coverage/readiness | storage/config/provider/notifier |
| `application/realtime/analytics_kernel.py` | L5 | 把 chain 与纯 analytics 拼成 `AnalyticsResult` | provider 原始字段、notification I/O |
| `domain/shadow.py` | L0 | shadow schema/value objects | 所有非 stdlib 模块 |
| `application/realtime/shadow.py` | L5 | 从 tick/analytics 构造 shadow record | notifier sinks |
| `infrastructure/analytics_shadow.py` | L4 | append-only writer/reader | alert evaluator、human delivery |

Schwab 扩展仍全部位于 L2 provider 包：`request_models.py`、`quota_machine.py`、
`market_data_plan.py`、`chain_discovery.py`、`hot_lane.py` 和
`observation_assembler.py`。其中 planner/selector/transition 必须保持无 I/O；跨 provider
比较与 IBKR adaptive validation 位于 L5 application，Schwab/IBKR 包不得互引。

首个 RTH 前的 canonical 流程必须是：

```text
provider adapters -> LatestMarketProjection -> MarketSnapshot
  -> OptionChainSnapshot -> OptionsAnalyticsKernel -> AnalyticsResult
  -> AlertEvaluator -> SQLite outbox
                         \
                          -> analytics shadow writer (no human delivery)
```

`PassthroughAnalytics` 只允许显式测试注入，不得作为生产 composition 默认值。
