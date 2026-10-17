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
   receipt 和日志；任何一段都可以用它反查整条链路。
2. `type + version` 唯一确定 payload schema；未知 type/version 必须 fail closed。
3. schema 由 pydantic 生成（`model_json_schema()`），Rust 用 `schemars` 对齐；
   golden fixture 只保留跨语言必需的用例。
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
2. 新增 `Clock` Protocol（`now()` / `monotonic()`），Core/Worker 在启动时注入；
   `domain/` 与 `analytics/` 禁止 `datetime.now()`，用 AST 测试守住（已有
   `tests/architecture/test_pure_boundaries.py` 可扩展）。
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

## 6. 决策分层与延迟预算

现状：决策 5s 一轮、+1–2s 计算；realtime 15s；alerts 30s；分钟样本 60s。
瓶颈不是 5s 轮询，而是每轮同步的路径回放与全量链重建。

| Lane | 触发 | 内容 | 输出 | 预算 |
| --- | --- | --- | --- | --- |
| Fast | 1s / quote tick | 价格触发、level 状态机 | `watch` / `armed` | ≤1s |
| Slow | 5–15s | 链分析、概率、排序、gates | `ready` / `no_trade` | ≤2–3s |
| Async | 分钟级 | LLM、Desk Map、研究 | 报告/卡片 | 不阻塞决策 |

规则：

1. `armed` 可以早发，但必须带 `evidence_age` 与 `valid_until`；`ready` 才代表证据齐全。
2. 链分析增量更新 + 按 snapshot 版本缓存；路径回放向量化或非阻塞。
3. 消息同时带 `decision_at` 与 `quote_as_of`，接收方能看到判断的时效。

## 7. 观测接入

五个金信号与最小 ops snapshot 投影见 [运行观测与决策延迟指标](../observability.md)。
correlation_id 是日志、DB、消息共用的检索键；每个 owner 每 60s 写一个
`latest/ops_<service>.json`，Worker 读取并生成 ops 消息，不引入新常驻服务。

## 8. 实施顺序与验收

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| P0 | correlation_id + Envelope + Rust 改名 | 一条 decision 能反查到 quote/receipt |
| P1 | 三个 payload 的 pydantic schema | schema 生成、未知版本 fail closed |
| P2 | Clock 端口 + 时间字段检查 | domain/analytics 无 `datetime.now()` |
| P3 | Strategy Protocol + shadow + replay | 新策略不改核心文件即可 shadow |
| P4 | Fast/Slow 分层 | `armed` ≤1s、`ready` ≤3s |

## 9. 非目标

- 不引入 Kafka/消息总线、不新增常驻服务、不做跨库分布式事务。
- 不改变 `automatic_ordering=false`、不把研究结果升级为授权。
- 不在本文件定义策略阈值；阈值仍由策略合同和 typed config 拥有。
