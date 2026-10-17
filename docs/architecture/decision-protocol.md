# 决策协议、时间契约与实时分层

<!-- documentation-status: 2026-10-18 -->
> **文档定位：现行设计合同（待实施部分已标注）。** 本文定义跨进程消息、时间语义、
> 存储 owner、策略接入口和决策分层；现状与目标分开写，不把设计当成已上线。
> [返回架构指南](README.md) · [模块架构](../../module-architecture.md) ·
> [合同注册表](../../contracts/README.md)

## 1. 为什么需要一份协议

当前跨模块传递主要是 `dict[str, object]`：`TradeIntent`、`StrategyDecision`、
`desk_map_projection`、`research_context` 各自约定键名，靠 golden JSON 对拍。
这带来三个可观测的问题：

- 至少 6 套并行 schema：`marketdata`、`notifier.model`、`strategy_contract`、
  `data_platform.contracts`、desk projection、research context，另有 Rust `spx-domain`；
- `contracts/golden/` 只有 JSON 样例，没有 JSON Schema；Python 是 dict，Rust 是 serde；
- Python 的 `strategy_decision`（完整决策）与 Rust 的 `strategy_decision.v1`
  （只有 `NO_TRADE` / `MANUAL_CANDIDATE`）同名不同物。

**目标**：一个信封、一类一 schema、一个 correlation_id；不引入消息总线或框架。

## 2. 统一信封（目标）

```python
@dataclass(frozen=True)
class Envelope:
    message_id: str        # ULID，一条消息一个
    type: str              # trade_intent | strategy_decision | notification_intent | desk_projection
    version: int           # payload schema version
    produced_at: datetime  # UTC，写入时刻
    as_of: datetime        # 事实截止/决策时钟
    source: str            # core | worker | rust-report | research
    correlation_id: str    # 贯穿 collector → decision → delivery
    payload: Mapping[str, object]
```

规则：

1. `correlation_id` 从采集快照开始生成，写入 `latest` 投影、decision、notification、
   receipt 和日志；任何一段都可以用它反查整条链路。现状（2026-10-18）：决策出口与
   人工交易通知已带 correlation_id；`notification_events` 表已加
   `correlation_id` 列（alembic `0005_notification_correlation`），Python 回执与机会
   台账可按列 join。Rust 账本 schema 仍在冻结期，Rust receipt 通过入队时写的
   `audit/notification_correlations.jsonl`（event_id → correlation_id）关联，不改
   Rust 表结构。
2. `type + version` 唯一确定 payload schema；未知 type/version 必须 fail closed。
   现状（2026-10-18）：`domain/payload_schemas.py` 注册了 strategy_decision v1/v2、
   trade_intent v1、notification_intent v1；`persist_strategy_decision` 写库前校验
   strategy_decision，`Envelope.validated_payload()` 在消费端校验；未知版本抛
   `UnknownPayloadVersion`。
3. schema 由 pydantic 生成（`model_json_schema()`，入口
   `payload_json_schema(type, version)`），Rust 用 `schemars` 对齐；
   golden fixture 只保留跨语言必需的用例。Rust 侧对齐仍是目标。
4. 解冻这三个类型时，优先把 Rust 的 `strategy_decision.v1` 改名为 `manual_advisory.v1`，消除同名。
5. `contracts/README.md` 的冻结决定继续有效；解冻只针对上述三个类型。

## 3. 时间契约

现状：`datetime.now(` 约 194 处，时间字段名 9 种（`as_of` / `source_at` /
`received_at` / `available_at` / `observed_at` / `created_at` / `generated_at` /
`recorded_at` / `decision_at`），没有统一 Clock 端口。

目标元组（不是每个消息都要全带，但语义固定）：

| 字段 | 含义 | 用途 |
| --- | --- | --- |
| `as_of` | 事实/决策截止 | 决策时钟、展示 |
| `source_at` | provider 源时间 | 新鲜度、因果回放 |
| `received_at` | 本地接收 | 传输延迟 |
| `available_at` | 可被消费的时间 | 回放过滤 |
| `created_at` | 记录写入 | 审计 |
| `expires_at` | TTL | 拒绝过期消息 |

规则：

1. 跨边界一律 aware UTC；ET 只出现在交易日历和展示层。
2. 新增 `Clock` Protocol（`now()` / `monotonic()`，`spx_spark/clock.py` 的
   `SystemClock` / `FixedClock`），`envelope_for` 已支持注入；`domain/` 与
   `analytics/` 的 `datetime.now()` 已清零，由
   `tests/architecture/test_no_wall_clock.py` 守住。Core/Worker 全链路启动注入仍是
   目标（当前入口仍各自取时间）。
3. 回放只按 `available_at` 过滤，禁止用 `created_at` 或消息发送时间代替。

## 4. 存储 owner

| Store | Owner | 内容 | 约束 |
| --- | --- | --- | --- |
| `spx.sqlite` | Python | operational、notification、data_platform ledger | 单一写者；Alembic 管理 |
| `huey.sqlite` | Python Worker | 任务队列 | 不作为业务事实来源 |
| Rust `operations.sqlite` | Rust | frames、report outbox、delivery receipts | 独立 owner fence |
| `latest/*.json` | 各 producer | 投影/健康/控制文件 | 带 schema + TTL，不是数据库 |
| Parquet lake | Python data platform | 原始报价、manifest | 只追加；compaction 有 manifest |
| Prefect PostgreSQL | Pi | 调度元数据 | 不含行情/策略事实 |

规则：一个事实只有一个写者；跨库一致性靠 correlation_id + 幂等，不做分布式事务。

## 5. 策略接入口（目标）

`build_strategy_decision` 仍是唯一人工候选授权点。为了研究能接入而不改核心，
新增一个最小 Protocol（不是框架）：

```python
class Strategy(Protocol):
    strategy_id: str
    version: str
    def evaluate(self, facts: FactPack) -> list[Candidate]: ...
    def gates(self, candidate: Candidate, facts: FactPack) -> list[GateResult]: ...
    def manage(self, candidate: Candidate) -> ManagementPolicy: ...
    def explain(self, candidate: Candidate) -> NarrativeFacts: ...
```

- 注册表由配置启用/禁用；每个策略必须支持 `shadow`（只记录、不授权）。
- 研究侧用同一个 Protocol 做 replay；回放输出 `Candidate` 和 `GateResult`，
  不允许直接产出 `READY`。
- 新增策略不改 `strategy_select` 的 if/else；先在 shadow 跑，再切 owner。
  现状（2026-10-18）：`domain/strategy.py` 定义 Protocol，`application/order_map/
  strategy_plugins.py` 提供注册表、`SPX_STRATEGY_PLUGINS` 插件加载与 shadow runner；
  插件只写 `latest/strategy_shadow.json`（`authority: none`），不参与授权。
  `scripts/strategy-shadow-replay.py` 可对保存的 facts 做 replay。新增策略只需新增
  模块并在 `SPX_STRATEGY_PLUGINS` 注册，不改 Core 文件。

## 6. 决策分层与延迟预算

现状：决策 5s 一轮、+1–2s 计算；realtime 15s；alerts 30s；分钟样本 60s。
瓶颈不是 5s 轮询，而是每轮同步的路径回放与全量链重建。

| Lane | 触发 | 内容 | 输出 | 预算 |
| --- | --- | --- | --- | --- |
| Fast | 1s / quote tick | 价格触发、level 状态机 | `watch` / `armed` | ≤1s |
| Slow | 5–15s | 链分析、概率、排序、gates | `ready` / `no_trade` | ≤2–3s |
| Async | 分钟级 | LLM、Desk Map、研究 | 报告/卡片 | 不阻塞决策 |
| Fast lane | 1s | 已提交 trigger/level 的 watch/armed 投影 | `latest/fast_lane.json` | ≤1s |

规则：

1. `armed` 可以早发，但必须带 `evidence_age` 与 `valid_until`；`ready` 才代表证据齐全。
2. 链分析按 snapshot 内容指纹在 Core 内共享 OptionsMap/曝光快照（market features /
   shock / alert 只构建一次）；路径回放已批量化（4,000 路径、5 分钟粒度约 20–40 ms，
   1 分钟粒度约 50 ms）。debit 分布复用 30s 内完成的快照，10s 后在后台刷新；冷 miss
   时同步回退（约 20–40 ms），避免授权等待或使用过期分布。
3. 消息同时带 `decision_at` 与 `quote_as_of`，接收方能看到判断的时效。
4. 现状（2026-10-18）：Core 已启动独立 1s fast lane（`application/fast_lane.py`），
   只发布 armed/watch 投影、`authority: none`；ready 仍由 5s 慢 cycle 授权。
   fast lane 输出 `latency_ms` / `evidence_age_ms` / `ready_budget_ms=3000`；慢 cycle
   输出 `decision_latency_ms`（cycle 开始→decision 可发布）。P95 聚合与告警是运行侧
   follow-up，不在本次代码内。

## 7. 观测接入

五个金信号与最小 ops snapshot 投影见 [运行观测与决策延迟指标](../observability.md)。
correlation_id 是日志、DB、消息共用的检索键；每个 owner 每 60s 写一个
`latest/ops_<service>.json`，Worker 读取并生成 ops 消息，不引入新常驻服务。

## 8. 实施顺序与验收

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| P0 | correlation_id + Envelope + Rust 改名 | 一条 decision 能反查到 quote/receipt |
| P1 | 三个 payload 的 pydantic schema | schema 生成、未知版本 fail closed（Python 侧已做；Rust 对齐待做） |
| P2 | Clock 端口 + 时间字段检查 | domain/analytics 无 `datetime.now()`（已做；Core/Worker 全链路注入待做） |
| P3 | Strategy Protocol + shadow + replay | 新策略不改核心文件即可 shadow（已做：插件模块 + env） |
| P4 | Fast/Slow 分层 | `armed` ≤1s、`ready` ≤3s（fast lane 已做；ready P95/告警待接） |

## 9. 非目标

- 不引入 Kafka/消息总线、不新增常驻服务、不做跨库分布式事务。
- 不改变 `automatic_ordering=false`、不把研究结果升级为授权。
- 不在本文件定义策略阈值；阈值仍由策略合同和 typed config 拥有。
