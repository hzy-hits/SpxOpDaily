# SPX Spark 工程化重构与验收规格

- 日期: 2026-07-12
- 状态: In progress - pre-RTH remediation（Phase 1/2/5 NO-GO）
- 范围: `src/spx_spark/`、`tests/`、`config/`、`systemd/`
- 目标: 在不改变 SPX/SPXW 0DTE 业务行为的前提下，把当前功能型原型重构为可长期运行、可验证、可演进的模块化系统。

### 实施检查点（2026-07-12）

2026-07-12 代码审计修正了先前过早的 Phase GO 判断：

- 生产 realtime composition 仍注入 `PassthroughAnalytics`，真实 options analytics
  尚未成为 engine kernel；
- runtime health 对缺失 realtime factors 默认按健康处理，启动阶段可能误报 READY；
- `AppSettings/load_settings()` 尚无生产调用者，`runtime_value()` AST 实际为 456
  次调用（414 是包含调用的文本行数）；
- `OptionChainSnapshot` 尚无生产构造/消费路径，density diagnostics 和发布门槛不完整；
- 五个完整 RTH session 的 shadow recorder、日报和差异报告尚未落地。

因此 Phase 1/2/5 当前为 NO-GO，Phase 6 仅为 GO-CANDIDATE。首个 RTH 前的
实施顺序、接口、排期和停止条件以
`docs/pre-rth-refactor-implementation-plan.md` 为准；本文件继续作为完整长期验收规格。

此前已完成的门面拆分、架构 registry、outbox 和部分状态机测试仍保留，不因本次
状态修正而回退。

| 阶段 | 状态 | 已落地 | 未满足的退出条件 |
| --- | --- | --- | --- |
| Phase 0 | 基线稳定 | 测试 defaults 隔离、6 个配置失败修复、options/exposure golden、全量回归恢复 | 脱敏 RTH fixture 与性能基线仍需补齐 |
| Phase 1 | 基础设施已落地，迁移未完 | 穷尽式模块登记、分层测试、typed settings、严格 deployment overlay、环境隔离 | composition roots 尚未全部改为显式注入；旧 `runtime_value()` 调用尚未清零 |
| Phase 2 | 进行中 | `domain` contracts、统一 Black-Scholes kernel、`analytics/options` 分包、旧 API 兼容门面 | analytics 仍接收部分 legacy `Quote/LatestState`；snapshot 边界、density diagnostics 和 differential runner 待完成 |
| Phase 3-4 | 部分完成 | order-map/IBKR stream 等门面与状态机已拆分 | 大文件和完整 contract/differential gates 未完成 |
| Phase 5 | 未通过 | RealtimeEngine/health 框架存在 | 默认 passthrough kernel 与启动乐观 readiness 必须修复 |
| Phase 6 | GO-CANDIDATE | SQLite outbox、幂等消费、latest projection 已实现 | 与真实 analytics 的 RTH restart/replay 尚未验收 |
| Phase 7-8 | NO-GO | 仅完成决策门设计 | 缺 IPC/Rust 所需生产指标 |

2026-07-12 operations 验证基线为 `1032 passed`、Ruff 与 build 全绿；本次审计的
realtime/options/runtime focused suites 为 `55 passed`。这些结果证明当前行为稳定，
但没有覆盖真实 kernel、完整 chain publishability 和五日 RTH shadow，因此不能据此
将 Phase 1/2/5 标为完成。

## 1. 决策摘要

SPX Spark 当前应定义为“功能闭环已成立的 operational prototype”，不是一次性脚本，
但也尚未达到 production-ready。核心数据接入、标准化、特征、策略、告警、通知、
历史落盘和复盘路径都已存在；主要技术债是职责混合、全局配置、隐式状态、超大模块、
健康语义不足，以及快照文件承担了过多跨进程协调职责。

本次重构采用以下决策:

1. Python 继续作为 provider、业务规则、应用编排、研究与存储的主语言。
2. 实时引擎先以 Python 实现，定义可替换的 `AnalyticsKernel` 接口，不在本轮引入 Rust。
3. Rust 只在性能验收证明 NumPy/算法优化仍无法满足 deadline 后，作为局部数值内核引入。
4. TypeScript 只用于未来 dashboard/Web UI，不进入行情、策略或账本核心。
5. SQLite outbox 存可靠低频领域事件；JSONL/Parquet 存历史行情；latest JSON 只做兼容投影。
6. 第一阶段不强制增加消息队列或 socket。IPC 必须通过量化门槛后再启用 Unix stream socket。
7. 状态机按 bounded context 分开定义，不创建一个承载所有业务的通用 `StateMachine` 基类。
8. 迁移必须支持 shadow/differential verification，不允许一次性大爆炸重写。

## 2. 当前事实基线

### 2.1 代码与验证基线

截至本文编写时:

- 全量测试: `922 passed, 6 failed`。
- Ruff: 全绿。
- 6 个失败来自工作区部署配置与测试默认假设漂移，涉及 IBKR 账户可见性、
  Schwab REST fast lane 和 Steven 默认启用状态。
- `order_map.py` 超过 3400 行。
- `ibkr/stream_collector.py` 约 2400 行。
- `post_close_review.py`、`config.py`、`options_map.py`、`alert_engine.py` 等同时承担多个职责。
- `tests/test_architecture.py` 遇到未登记模块会跳过，因此不是完整的架构防线。

### 2.2 Schwab SPXW 链与风险中性密度基线

当前配置中 `$SPX`:

- `option_chain_strike_count = 40`
- `chain_interval_seconds = 5`

2026-07-11 latest-state 只读统计:

| Provider | Expiry | Legs | Distinct strikes | 双边 mid strikes | IV legs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Schwab | 20260713 | 80 | 40 | 40 | 80 |
| Schwab | 20260714 | 80 | 40 | 40 | 80 |

数量上足以执行现有 Breeden-Litzenberger 非均匀二阶差分。当天是周六，报价已 stale，
`option_mid()` 的质量门会拒绝这些点，因此密度结果为 `insufficient_strikes`。
这不表示 Schwab 只提供了不足 6 个 strike，而表示“当前没有足够的可用实时 strike”。

40 个 strike 也不能自动等价于高质量密度。生产验收必须同时满足:

- 报价新鲜；
- strike 连续；
- ATM 两侧覆盖足够；
- 双边报价率足够；
- 合成 call 曲线基本单调且凸；
- 负密度裁剪占比可接受；
- 覆盖至少延伸到目标概率分位所需的尾部。

### 2.3 当前计算模型基线

系统已存在 Black-Scholes 计算:

- `greek_reference.py`: price、delta、gamma、vega、情景重估和高阶有限差分参考；
- `features/exposure_map.py`: 解析 vanna、charm；
- `options_map.py`: BS gamma spot scan、zero gamma、风险中性密度；
- 实时 exposure 主路径优先使用 vendor delta/gamma/IV，自算 vanna/charm 和场景 gamma。

风险中性密度来自市场 call mid 对 strike 的二阶导数，不是直接输出 BS 对数正态密度。
当前生产实现没有使用 NumPy 向量化；以单标的、约 80-200 legs、秒级重算的规模，
标量 Python 仍应先作为正确性基线。

## 3. 问题定义

### 3.1 P0: 重构前必须解决

1. 测试结果必须与机器部署配置解耦。
2. 所有生产模块必须纳入架构分层；未知模块必须让架构测试失败。
3. 建立 before/after golden，防止重构改变交易相关数值和通知行为。
4. 明确 canonical event、latest projection、historical fact 三种不同数据职责。

### 3.2 P1: 本轮核心技术债

1. 超大文件混合领域计算、I/O、状态和 CLI。
2. `runtime_value()` 在 import 和业务函数中广泛调用，依赖隐式全局配置。
3. 大量 `status: str`、`state: str`、`mode: str` 缺少枚举和合法迁移约束。
4. `service_loop` 的 heartbeat 只表示主循环活着，不表示关键任务或行情健康。
5. latest-state 文件仍同时承担兼容缓存和跨进程交接。
6. 业务规则中的硬编码阈值、字符串状态和 provider 特例分散。

### 3.3 非目标

- 不增加自动下单。
- 不改变既有 provider 优先级、数据质量门或 human-visible scope。
- 不立即引入 Kubernetes、Kafka、RabbitMQ、Redis 或分布式数据库。
- 不在没有 benchmark 的情况下重写 Rust。
- 不为了减少文件行数创建没有业务含义的 wrapper、manager 或 helper。

## 4. 目标上下文与依赖方向

目标依赖方向由上到下:

```text
entrypoints/services
        |
application
        |
analytics      providers
        \        /
          domain
        /        \
infrastructure  settings
```

约束:

1. `domain` 只能依赖标准库。
2. `analytics` 只能依赖 `domain` 和同一 analytics 子包的低层模块。
3. `providers` 只能把 provider payload 转成 domain contract，不允许 import analytics/alerts。
4. `application` 编排 ports，不直接解析 provider payload，不直接拼 SQL 或文件路径。
5. `infrastructure` 实现 ports，不包含交易/告警判断。
6. `entrypoints` 只解析参数、构造依赖并映射退出码。
7. `settings` 可被 composition root 使用；纯 domain/analytics 函数不得自行读取环境变量。

## 5. 目标目录结构

迁移期间保留旧模块作为 compatibility facade。目标结构如下:

```text
src/spx_spark/
  domain/
    market.py
    snapshots.py
    analytics.py
    events.py
    health.py
    state_machines.py

  settings/
    loader.py
    schema.py
    market_data.py
    ibkr.py
    schwab.py
    analytics.py
    alerts.py
    runtime.py
    storage.py

  analytics/
    greeks/
      models.py
      black_scholes.py
      higher_order.py
      vendor_reference.py
      kernel.py
    options/
      models.py
      chain.py
      quality.py
      pricing.py
      probability.py
      density.py
      exposure.py
      levels.py
      service.py

  providers/
    common/
      contracts.py
      health.py
    # 现有 ibkr/schwab/... 可分阶段移动，不要求第一批改路径

  application/
    realtime/
      contracts.py
      engine.py
      health.py
      pipeline.py
    alerts/
      evaluator.py
      rules_data.py
      rules_price.py
      rules_options.py
      rules_system.py
      service.py
    order_map/
      models.py
      pricing.py
      spot.py
      candidates.py
      bias_machine.py
      volume_machine.py
      state.py
      render.py
      delivery.py
      service.py
    runtime/
      tasks.py
      scheduler.py
      health.py
      supervisor.py

  infrastructure/
    market_data/
      raw_jsonl.py
      latest_projection.py
      snapshot_source.py
    ledger/
      sqlite.py
      outbox.py
    ipc/
      contracts.py
      latest_polling.py
      unix_stream.py       # 达到启用门槛后才实现
    metrics/
      jsonl.py

  entrypoints/
    cli/
    services/
```

### 文件规模约束

- 新生产文件 soft limit 400 行，hard limit 600 行。
- 超过 600 行必须在 ADR 中说明原因；生成代码和 migration SQL 例外。
- 新函数 soft limit 40 行，hard limit 60 行。
- 分支复杂度目标 `C901 <= 10`，`PLR0912 <= 10`，`PLR0915 <= 60`。
- `if/for/try/with` 组合嵌套目标不超过 3 层。
- 不允许仅为满足行数创建 `utils.py`、`helpers.py`、`common.py` 垃圾桶模块。

## 6. 核心领域契约

以下接口名称和字段是迁移目标。实现可增加向后兼容字段，但不得弱化约束。

### 6.1 MarketSnapshot

```python
@dataclass(frozen=True)
class MarketSnapshot:
    schema_version: int
    snapshot_id: str
    as_of: datetime
    received_at: datetime
    quotes: tuple[Quote, ...]
    provider_states: tuple[ProviderState, ...]
    source_batch_ids: tuple[str, ...]

    def quotes_for(self, instrument_id: str) -> tuple[Quote, ...]: ...
    def options(self, underlier: str, expiry: str | None = None) -> tuple[Quote, ...]: ...
    def validate(self) -> None: ...
```

契约:

- 时间统一为 aware UTC。
- `snapshot_id` 对输入事实确定，不能包含随机数。
- quotes 按 `(canonical_id, provider, received_at)` 去重。
- snapshot 不执行 provider fallback；选择结果属于 projection/chain builder。
- 不携带 provider raw payload。

### 6.2 OptionChainSnapshot

```python
class ChainReadiness(str, Enum):
    READY = "ready"
    DEGRADED = "degraded"
    BLOCKED = "blocked"

class ChainIssue(str, Enum):
    STALE = "stale"
    INSUFFICIENT_STRIKES = "insufficient_strikes"
    INSUFFICIENT_WIDTH = "insufficient_width"
    GAPPED_GRID = "gapped_grid"
    INSUFFICIENT_TWO_SIDED = "insufficient_two_sided"
    MISSING_IV = "missing_iv"
    MISSING_OI = "missing_oi"
    WIDE_MARKET = "wide_market"
    NON_MONOTONE_CALL_CURVE = "non_monotone_call_curve"
    NON_CONVEX_CALL_CURVE = "non_convex_call_curve"

@dataclass(frozen=True)
class ChainCoverage:
    total_legs: int
    distinct_strikes: int
    usable_strikes: int
    two_sided_strikes: int
    iv_legs: int
    oi_legs: int
    min_strike: float | None
    max_strike: float | None
    median_step: float | None
    max_gap: float | None
    lower_width_points: float | None
    upper_width_points: float | None
    max_quote_age_seconds: float | None
    max_cross_row_skew_seconds: float | None

@dataclass(frozen=True)
class OptionChainSnapshot:
    snapshot_id: str
    underlier: str
    trading_class: str
    expiry: str
    as_of: datetime
    spot: float | None
    quotes: tuple[Quote, ...]
    coverage: ChainCoverage
    readiness: ChainReadiness
    issues: tuple[ChainIssue, ...]
```

构造入口:

```python
def build_option_chain(
    snapshot: MarketSnapshot,
    *,
    underlier: str,
    trading_class: str,
    expiry: str,
    policy: ChainQualityPolicy,
) -> OptionChainSnapshot: ...
```

### 6.3 Analytics 输入与输出

```python
@dataclass(frozen=True)
class AnalyticsDiagnostics:
    input_legs: int
    usable_legs: int
    duration_ms: float
    warnings: tuple[str, ...]
    model_versions: Mapping[str, str]

@dataclass(frozen=True)
class ExpiryAnalytics:
    chain: OptionChainSnapshot
    density: RiskNeutralDensityResult
    exposure: ExpiryExposure
    levels: OptionLevels

@dataclass(frozen=True)
class AnalyticsResult:
    schema_version: int
    result_id: str
    input_snapshot_id: str
    computed_at: datetime
    underlier: UnderlierReference
    expiries: tuple[ExpiryAnalytics, ...]
    diagnostics: AnalyticsDiagnostics
```

核心 port:

```python
class AnalyticsKernel(Protocol):
    def calculate(
        self,
        snapshot: MarketSnapshot,
        request: AnalyticsRequest,
    ) -> AnalyticsResult: ...
```

Python 第一版:

```python
class PythonAnalyticsKernel(AnalyticsKernel): ...
```

未来 Rust 版本若启用:

```python
class RustAnalyticsKernel(AnalyticsKernel): ...
```

两者必须通过同一 contract/golden/differential tests。

### 6.4 实时引擎契约

```python
class EngineMode(str, Enum):
    STARTING = "starting"
    WARMING = "warming"
    READY = "ready"
    DEGRADED = "degraded"
    BLOCKED = "blocked"
    STOPPING = "stopping"
    FAILED = "failed"

@dataclass(frozen=True)
class EngineTick:
    tick_id: str
    started_at: datetime
    source_snapshot_id: str
    analytics: AnalyticsResult | None
    events: tuple[DomainEvent, ...]
    health: EngineHealth
    duration_ms: float

class SnapshotSource(Protocol):
    def read(self) -> MarketSnapshot: ...

class ProjectionStore(Protocol):
    def publish(self, tick: EngineTick) -> None: ...

class EventOutbox(Protocol):
    def append(self, events: Sequence[DomainEvent]) -> AppendResult: ...

class RealtimeEngine:
    def tick(self, *, now: datetime) -> EngineTick: ...
```

`RealtimeEngine.tick()` 只允许做:

1. 读取 snapshot；
2. 验证数据质量；
3. 调用 analytics kernel；
4. 调用 alert/signal evaluator；
5. 原子发布 projection；
6. 把可靠领域事件写入 outbox；
7. 记录 metrics。

不得直接发送微信、调用 LLM、刷新 OAuth 或操作 IB Gateway。

### 6.5 领域事件

```python
class EventKind(str, Enum):
    PROVIDER_TRANSITION = "provider_transition"
    DATA_QUALITY_TRANSITION = "data_quality_transition"
    PRICE_SHOCK = "price_shock"
    OPTION_STRUCTURE_TRANSITION = "option_structure_transition"
    POSITION_TRANSITION = "position_transition"
    ALERT_CANDIDATE = "alert_candidate"
    DELIVERY_RESULT = "delivery_result"

@dataclass(frozen=True)
class DomainEvent:
    schema_version: int
    event_id: str
    kind: EventKind
    source_at: datetime
    available_at: datetime
    aggregate_id: str
    sequence: int
    payload: Mapping[str, JsonValue]
```

要求:

- `event_id` 确定性生成；
- payload 必须 JSON serializable；
- 写入至少一次，消费方按 `event_id` 幂等；
- neutral evaluation 不写 outbox；只写状态转换和需要审计的事实。

## 7. 状态机规格

有限集合首先应使用 Enum，但只有包含时间演进和合法迁移的对象才是状态机。例如 provider
status、quality、severity 是分类枚举；provider runtime、delivery、replan、signal lifecycle
才需要 transition function。不得为了“状态机化”给静态分类增加虚假的 previous/next state。

### 7.1 Provider runtime

```python
class ProviderRuntimeMode(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    LIVE = "live"
    DEGRADED = "degraded"
    BACKOFF = "backoff"
    POLICY_BLOCKED = "policy_blocked"
    CONFLICT_WAIT = "conflict_wait"
    STOPPING = "stopping"
    FAILED = "failed"
```

合法迁移:

| From | Event | To |
| --- | --- | --- |
| STOPPED | start_requested | STARTING |
| STARTING | connected_and_subscribed | LIVE |
| STARTING | policy_denied | POLICY_BLOCKED |
| STARTING | competing_session | CONFLICT_WAIT |
| STARTING | transient_failure | BACKOFF |
| LIVE | partial_data | DEGRADED |
| LIVE | disconnected | BACKOFF |
| LIVE | stop_requested | STOPPING |
| DEGRADED | healthy_observation | LIVE |
| DEGRADED | disconnected | BACKOFF |
| BACKOFF | retry_due | STARTING |
| POLICY_BLOCKED | policy_allowed | STARTING |
| CONFLICT_WAIT | probe_due | STARTING |
| 任意非终态 | unrecoverable_error | FAILED |
| STOPPING | stopped | STOPPED |

状态转换函数必须是纯函数:

```python
def advance_provider_runtime(
    state: ProviderRuntimeState,
    event: ProviderRuntimeEvent,
    policy: ProviderRuntimePolicy,
) -> ProviderRuntimeDecision: ...
```

`StreamAction` 变为 transition effect，而不是主状态本身。

### 7.2 Option replan

```python
class ReplanMode(str, Enum):
    STEADY = "steady"
    CANDIDATE_PENDING = "candidate_pending"
    APPLYING = "applying"
    COOLDOWN = "cooldown"
    FAILURE_BACKOFF = "failure_backoff"
```

禁止继续返回任意字符串 `state/reason`。`reason` 使用独立 `ReplanReason` enum，
外部展示时再映射为文本。

### 7.3 Engine health

`READY` 必须同时满足:

- 至少一个直接 TradFi SPX anchor 可用于 pricing；
- front SPXW chain 未超 freshness deadline；
- 最近一次 analytics 成功；
- outbox 可写；
- 关键任务未超过连续失败阈值。

`DEGRADED` 表示仍可输出研究结果但至少一个非关键能力失效；
`BLOCKED` 表示不能产生 executable/pricing output；`FAILED` 表示引擎自身不可恢复。

### 7.4 Delivery

```python
class DeliveryMode(str, Enum):
    PENDING = "pending"
    REVIEWING = "reviewing"
    APPROVED = "approved"
    REJECTED = "rejected"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    RETRY_WAIT = "retry_wait"
    DEAD_LETTER = "dead_letter"
```

所有 notification if-chain 最终应映射到显式 transition。`REJECTED` 是成功终态，
不是失败；`DEAD_LETTER` 只能由超出 retry policy 或不可重试错误进入。

### 7.5 Order-map signal lifecycle

不同 play 共用生命周期枚举，但 transition policy 按 play 实现:

```python
class SignalMode(str, Enum):
    OBSERVING = "observing"
    ARMED = "armed"
    TRIGGERED = "triggered"
    CONFIRMED = "confirmed"
    INVALIDATED = "invalidated"
    EXPIRED = "expired"
```

不得用一个巨型函数同时处理 volume break、flip reclaim、call-wall breakout、发送状态。
每个 machine 接收 typed observation，返回 typed transition/effects。

## 8. 模块级拆分计划

### 8.1 `options_map.py`

| 目标模块 | 迁移内容 |
| --- | --- |
| `analytics/options/models.py` | `OptionsMap`、`ExpiryOptionsMap`、density/coverage/level value objects |
| `analytics/options/chain.py` | expiry 分组、pair-by-strike、ATM reference、chain implied spot |
| `analytics/options/quality.py` | quote/structure/coverage/readiness policy |
| `analytics/options/pricing.py` | mid/IV/delta/gamma accessors、time-to-expiry |
| `analytics/options/probability.py` | level close/touch probability |
| `analytics/options/density.py` | synthetic call curve、convexity checks、density、quantiles |
| `analytics/options/exposure.py` | GEX/DEX/VEX/CEX、zero-gamma scan |
| `analytics/options/levels.py` | walls、gamma regime、SPY confluence |
| `analytics/options/service.py` | `calculate_expiry()` 与 `calculate_options()` composition |

旧 `options_map.py` 暂时 re-export 公共 API 和保留 CLI；迁移完成后只允许少于 150 行。

### 8.2 `features/exposure_map.py` 与 `greek_reference.py`

消除 `exposure_map -> options_map` 的函数内延迟 import。共同公式下沉到:

- `analytics/greeks/black_scholes.py`
- `analytics/greeks/higher_order.py`
- `analytics/options/exposure.py`

`black_scholes.py` 必须只含纯函数和数组/标量 kernel，不 import storage/config。
vendor normalization 与 model comparison 放 `vendor_reference.py`，不能混入公式模块。

### 8.3 `order_map.py`

| 目标模块 | 职责 |
| --- | --- |
| `application/order_map/models.py` | candidate、spot resolution、payload、状态对象、enums |
| `pricing.py` | tick、Taylor/BS repricing、ETA，纯函数 |
| `spot.py` | TradFi/chain/HL research 与 pricing gate |
| `candidates.py` | 从 AnalyticsResult 生成 typed candidates |
| `bias_machine.py` | flip reclaim / call-wall breakout 状态机 |
| `volume_machine.py` | ES volume break 状态机 |
| `state.py` | 状态序列化、版本迁移、repository port |
| `render.py` | 确定性模板和 prompt 数据，不发送 |
| `delivery.py` | outbox event 创建，不直接混入计算 |
| `service.py` | orchestration，目标少于 250 行 |
| `entrypoints/cli/order_map.py` | CLI 和退出码 |

原 `order_map.py` 最终为兼容 facade，不超过 100 行。

### 8.4 `ibkr/stream_collector.py`

| 目标模块 | 职责 |
| --- | --- |
| `ibkr/stream/models.py` | plan、cache、runtime typed models |
| `ibkr/stream/runtime_machine.py` | provider runtime transition |
| `ibkr/stream/replan_machine.py` | ATM replan transition |
| `ibkr/stream/contracts.py` | qualification key、contract factories |
| `ibkr/stream/subscriptions.py` | subscribe/cancel/confirm/rotation |
| `ibkr/stream/cache.py` | base/hot/slow rows、freshness、merge |
| `ibkr/stream/flush.py` | ProviderSnapshot 构造和 persistence port |
| `ibkr/stream/session.py` | ib_async connection adapter |
| `ibkr/stream/supervisor.py` | effects 执行和生命周期编排 |
| `ibkr/stream/cli.py` | 参数和 composition root |

要求同一时间仍只有一个 IBKR market-data session owner。拆模块不等于拆成多个 IBKR 进程。

### 8.5 `config.py` 与 `runtime_config.py`

目标:

- YAML 只加载一次；
- schema validation 在启动时完成；
- 业务函数接收 typed settings；
- 测试不读取工作区 deployment config；
- secrets 只来自 environment/secret files。

接口:

```python
@dataclass(frozen=True)
class AppSettings:
    market_data: MarketDataSettings
    ibkr: IbkrSettings
    schwab: SchwabSettings
    analytics: AnalyticsSettings
    alerts: AlertSettings
    runtime: RuntimeSettings
    storage: StorageSettings

def load_settings(
    *,
    defaults_path: Path,
    deployment_path: Path | None,
    environ: Mapping[str, str],
) -> AppSettings: ...
```

优先级固定为 `defaults < deployment < environment`。每个 override 必须记录来源，
但日志中不得包含 secret value。

### 8.6 `alert_engine.py`

拆为 typed rule evaluator:

```python
class AlertRule(Protocol):
    name: str
    def evaluate(self, context: AlertContext) -> tuple[AlertCandidate, ...]: ...
```

规则包按 data health、price、option structure、provider/system、position 分开。
rule 只能产生 candidate，不发送、不写 cooldown。选择、去重、审阅、投递分别属于不同阶段。

### 8.7 `service_loop.py`

拆为:

- task registry；
- scheduler；
- subprocess runner；
- health aggregator；
- supervisor/CLI。

`TaskSpec`:

```python
class TaskCriticality(str, Enum):
    CRITICAL = "critical"
    IMPORTANT = "important"
    OPTIONAL = "optional"

class TaskMode(str, Enum):
    DISABLED = "disabled"
    IDLE = "idle"
    RUNNING = "running"
    BACKOFF = "backoff"
    UNHEALTHY = "unhealthy"

@dataclass(frozen=True)
class TaskSpec:
    name: str
    command: tuple[str, ...]
    interval_seconds: float
    timeout_seconds: float
    criticality: TaskCriticality
    max_consecutive_failures: int
```

heartbeat 必须输出每个任务的 `last_success_at`、连续失败、in-flight age 和 overall readiness。

### 8.8 数据平台与 storage

- `LatestStateStore` 改名为 `LatestMarketProjectionStore`，明确它是可重建 projection。
- raw normalized JSONL/Parquet 是行情历史事实。
- SQLite ledger/outbox 是低频可靠领域事实。
- analytics/latest JSON 是展示和兼容 projection，不是 event queue。
- 所有 canonical latest 写入必须通过 port，禁止散落 `write_text/replace`。

### 8.9 `notifier/`

保留现有 package 方向，但进一步分离四个阶段:

```text
candidate selection -> review -> delivery gate -> sink delivery
```

目标模块:

| 模块 | 职责 |
| --- | --- |
| `application/notifications/models.py` | typed candidate/review/delivery result |
| `review_service.py` | LLM reviewer port 和 deterministic fallback |
| `delivery_machine.py` | DeliveryMode transition/retry/dead-letter |
| `policy.py` | human scope、severity、cooldown、dedup policy |
| `infrastructure/notifications/openclaw.py` | OpenClaw adapter |
| `infrastructure/notifications/bark.py` | Bark adapter |
| `infrastructure/notifications/feishu.py` | Feishu adapter |
| `application/notifications/service.py` | outbox claim -> review -> deliver -> ack |

通知 sink 返回 typed error category:

```python
class DeliveryErrorKind(str, Enum):
    TRANSIENT_NETWORK = "transient_network"
    RATE_LIMITED = "rate_limited"
    AUTHENTICATION = "authentication"
    INVALID_PAYLOAD = "invalid_payload"
    POLICY_REJECTED = "policy_rejected"
    UNKNOWN = "unknown"
```

不得从 error message substring 决定是否重试；adapter 负责分类。

### 8.10 `post_close_review.py`

拆为:

| 目标模块 | 职责 |
| --- | --- |
| `application/review/models.py` | session review input/output |
| `application/review/datasets.py` | 选择冻结的数据集版本 |
| `application/review/statistics.py` | 确定性统计和归因 |
| `application/review/render.py` | Markdown/JSON render |
| `application/review/llm.py` | LLM enrichment port |
| `application/review/service.py` | orchestration |
| `entrypoints/cli/post_close_review.py` | CLI |

复盘必须从 `ResearchReader` 读取确定版本数据，不得读取 outcome 后再回填同一 decision 输入。
相同 session + strategy version + dataset version 必须产生相同 deterministic section；LLM 文本单独标记。

### 8.11 Schwab、Hyperliquid、Polymarket 与 mock providers

每个 provider 都遵守相同边界:

```python
class ProviderClient(Protocol):
    def fetch(self, request: ProviderRequest) -> ProviderRawBatch: ...

class ProviderNormalizer(Protocol):
    provider: Provider
    def normalize(self, raw: ProviderRawBatch) -> ProviderSnapshot: ...
```

- client 负责 transport/auth/rate limit/retry；
- normalizer 是纯转换，不能发请求或写存储；
- collector service 组合 client、normalizer、landing writer；
- provider-specific units 必须在 normalizer 明确转换，禁止用值域猜单位；
- Schwab OAuth/gateway 继续是唯一 refresh-token owner；
- Hyperliquid/Polymarket 只能作为 context，不得被 adapter 冒充直接 SPX pricing source；
- mock provider 必须实现相同 contract，用于 E2E 和 failure injection。

### 8.12 Strategy、intraday shock 与 position

Strategy 接口:

```python
class StrategyEvaluator(Protocol):
    strategy_id: str
    version: str
    def evaluate(self, context: StrategyContext) -> StrategyDecision: ...
```

要求:

- Micopedia、Steven、intraday strategy 只消费 frozen `StrategyContext`；
- evaluator 不读取 latest file、不发送通知、不修改全局状态；
- stateful watch/reclaim/shock 逻辑使用独立 typed machine；
- position observation、position event、position alert 是三个不同模型；
- read-only broker position input 不得进入 provider market-data domain model；
- strategy decision 必须记录 strategy/model/config version 和 input snapshot ID。

建议拆分:

| 当前模块 | 目标 |
| --- | --- |
| `intraday_shock.py` | `application/shock/models.py`, `machine.py`, `evaluator.py`, `service.py` |
| `strategy/steven.py` | `strategy/steven/models.py`, `regime.py`, `machine.py`, `evaluator.py`, `episodes.py` |
| `strategy/micopedia.py` | `strategy/micopedia/models.py`, `regime.py`, `guidance.py` |
| `position_events.py` | `domain/positions.py`, `application/positions/machine.py`, repository adapter |
| `position_alerts.py` | position alert rule package |

### 8.13 Data platform

现有 hexagonal ports/adapters 方向保留。调整重点:

- `contracts.py` 的有限 status/phase 改为 enum；
- `facade.py` 继续保证 realtime import 不加载 DuckDB；
- `SQLiteDecisionLedger` 不承担高频 quote bus；
- outbox migration 与 decision ledger 可同库，但表和 repository port 分离；
- Parquet compaction、manifest、research catalog 继续在 batch process；
- schema version、writer version、source hash 和 available_at contract 不得在重构中丢失。

### 8.14 兼容层和删除策略

以下旧入口在迁移期保持:

- console script 名；
- systemd `ExecStart`；
- documented public import；
- latest JSON schema，除非提供版本迁移；
- raw JSONL schema。

compatibility facade 只允许 re-export、参数转换和 deprecation warning，不允许保留第二套业务逻辑。
每个 facade 必须有删除条件和 `rg` 零调用证明。迁移完成前不得同时维护 old/new 两套状态写入者。

## 9. 风险中性密度质量契约

### 9.1 新接口

```python
class DensityQuality(str, Enum):
    READY = "ready"
    DEGRADED_NOISY = "degraded_noisy"
    BLOCKED_STALE = "blocked_stale"
    BLOCKED_COVERAGE = "blocked_coverage"
    BLOCKED_ARBITRAGE = "blocked_arbitrage"

@dataclass(frozen=True)
class DensityDiagnostics:
    usable_strikes: int
    lower_width_points: float
    upper_width_points: float
    two_sided_ratio: float
    max_gap_multiple: float
    monotonic_violation_fraction: float
    negative_mass_fraction: float
    normalized_mass: float

@dataclass(frozen=True)
class RiskNeutralDensityResult:
    quality: DensityQuality
    diagnostics: DensityDiagnostics
    p10: float | None
    median: float | None
    p90: float | None
    prob_below_put_wall: float | None
    prob_above_call_wall: float | None
```

### 9.2 RTH 初始验收阈值

这些是首版工程阈值，必须通过 5 个完整 RTH session 的 shadow 数据校准后再固化:

- usable distinct strikes `>= 21`；
- two-sided mid ratio `>= 0.80`；
- ATM 上下各至少 8 个 usable strikes；
- lower/upper coverage 各至少 `max(1.25 * expected_move, 50 points)`；
- max gap `<= 2 * median strike step`；
- max quote age `<= 15s`；
- cross-row time skew p95 `<= 10s`；
- monotonic call-curve violation fraction `<= 0.10`；
- negative/clipped mass fraction `<= 0.15` 为 READY，`<= 0.40` 为 DEGRADED，超过则 BLOCKED；
- normalized mass 必须在 `[0.95, 1.05]`，否则不得发布概率。

Schwab 的 40-strike 链在数量上可满足此标准，但必须在 RTH 实测宽度和质量；如果 spot
快速移动导致 40 strikes 偏向一侧，collector 必须重取 ATM chain，而不是让 analytics 猜尾部。

### 9.3 数值方法演进

第一版保持当前二阶差分作为 regression baseline。后续按顺序评估:

1. 清理 crossed/wide/stale quotes；
2. OTM 合成 call 曲线；
3. 单调/凸性诊断；
4. 可选 constrained convex projection；
5. 再进行二阶导数和归一化。

不得用平滑后的漂亮曲线掩盖输入质量问题；raw 与 repaired diagnostics 必须同时保存。

## 10. IPC 与消息边界

### 10.1 第一阶段

保留 provider -> raw/latest projection -> realtime engine 的读路径，但通过 ports 隔离。
此时不新增 socket，先测量:

- latest file size；
- lock wait；
- parse duration；
- provider received_at 到 engine available_at 的延迟；
- snapshot 重复率。

### 10.2 启用 Unix stream socket 的门槛

满足任意一项，并连续 5 个 RTH session 出现，才实施 `unix_stream.py`:

- provider-to-engine p95 延迟超过 750ms，且主要来自 polling；
- latest projection 超过 5 MiB；
- projection parse p95 超过 100ms；
- file-lock wait p95 超过 100ms；
- 需要稳定支持 1 秒以下 engine tick。

### 10.3 QuoteBatchEnvelope

```python
@dataclass(frozen=True)
class QuoteBatchEnvelope:
    schema_version: int
    batch_id: str
    provider: Provider
    sequence: int
    source_at: datetime | None
    received_at: datetime
    quotes: tuple[Quote, ...]
    provider_state: ProviderState
```

若启用 UDS:

- Unix `SOCK_STREAM`；
- 4-byte big-endian length prefix；
- MessagePack 或 canonical JSON payload；
- engine 为 socket server；provider 为 reconnecting client；
- batch 先写 raw durable log，再 publish；
- engine 按 `(provider, sequence, batch_id)` 去重；
- ack 只表示 engine 已接收，不表示 analytics 已完成；
- 断线后 engine 从 latest projection 恢复，不要求 socket 自身持久化；
- 必须有最大 frame、write timeout 和背压策略。

Socket 是低延迟快路径，不能替代 raw history 或 SQLite outbox。

## 11. Rust/NumPy 决策门

### 11.1 Python baseline

先实现纯 Python kernel，并建立 benchmark。随后只对矩阵型路径使用 NumPy:

- `spot_grid x contracts` gamma scan；
- 批量 BS price/Greeks；
- density grid 数值运算；
- 大规模 replay feature calculation。

NumPy 应成为 direct dependency 后才能在生产代码 import，不能依赖传递依赖碰巧存在。

### 11.2 Rust 进入条件

同时满足以下条件才创建 Rust crate:

1. 正确性和模块边界重构完成；
2. 有生产输入规模 benchmark；
3. 已完成算法和 NumPy 优化；
4. analytics p99 仍超过 engine deadline 的 30%；
5. profiler 显示瓶颈位于纯数值 kernel，而不是 I/O/序列化；
6. 能为 Python/Rust 建立相同 golden 和误差容限。

首选 crate 边界:

```text
rust/analytics_core
  black_scholes_batch
  higher_order_greeks_batch
  gamma_spot_scan
  density_second_derivative
```

Rust 不读取 YAML、不连接券商、不写 SQLite、不发送通知。

## 12. 分阶段实施计划

### Phase 0: Baseline freeze

交付:

- 固定测试 defaults fixture；
- 修复当前 6 个失败；
- 采集至少一个脱敏 RTH SPXW fixture；
- 保存 options/exposure/order-map/alert golden；
- 记录现有性能基线。

退出条件:

- 全量测试 100% 通过；
- Ruff 通过；
- fixtures 不引用 `/srv`、`.env` 或实盘 token；
- golden 包含数据质量降级场景。

### Phase 1: Architecture guard and settings

交付:

- 未登记模块导致架构测试失败；
- `AppSettings` 和分域 settings；
- defaults/deployment/environment 分层；
- composition root 注入配置；
- 禁止新增业务代码直接调用 `runtime_value()`。

退出条件:

- 配置测试在任意 CWD 下结果一致；
- deployment config 变化不影响 unit tests；
- 缺字段、错类型、未知 enum 在启动时失败。

### Phase 2: Domain and analytics extraction

交付:

- MarketSnapshot、OptionChainSnapshot、AnalyticsResult；
- greeks/options 子包；
- density quality diagnostics；
- old/new differential runner。

退出条件:

- captured fixtures 上旧/新关键数值在约定容差内一致；
- analytics 无文件、环境变量、network import；
- 单测可直接构造 snapshot，不需要 tmp repo。

### Phase 3: Order-map decomposition

交付:

- models/pricing/spot/candidates/state-machines/render/service；
- 原 CLI 和 import path 兼容；
- 所有 play 使用 typed lifecycle。

退出条件:

- `order_map.py <= 100` 行；
- 目标模块均 `< 600` 行；
- 现有 order-map golden 不变；
- illegal transition 明确拒绝。

### Phase 4: Provider runtime decomposition

交付:

- IBKR stream runtime/replan 状态机；
- session/subscription/cache/flush/supervisor；
- 单 session owner contract test；
- Schwab gateway 保持单 token owner。

退出条件:

- `stream_collector.py <= 100` 行 compatibility facade；
- reconnect/conflict/policy/backoff 全部 table-driven 测试；
- 模拟 100 次断连重连无订阅泄漏或重复 owner。

### Phase 5: Realtime engine and health

交付:

- RealtimeEngine；
- task registry/scheduler/health/supervisor；
- readiness projection；
- critical failure 触发进程退出策略。

退出条件:

- heartbeat 不再无条件 `ok=true`；
- stale provider、analytics failure、outbox failure 均产生正确 overall mode；
- systemd 可基于退出码重启不可恢复故障。

### Phase 6: Outbox and projection boundary

交付:

- domain event outbox；
- notifier 幂等消费；
- latest projection repository；
- direct canonical latest writes 架构测试。

退出条件:

- kill/restart notifier 不丢 alert candidate；
- 重复消费不重复发送；
- neutral tick 不增长 outbox；
- outbox 可 replay/dead-letter。

### Phase 7: IPC decision

根据第 10 节指标决定 `NO-GO` 或实现 UDS。没有达到门槛时，正确结果是明确记录
“暂不实施”，而不是为了架构图增加 socket。

### Phase 8: Rust decision

根据第 11 节指标决定 `NO-GO`、NumPy 或 Rust。Rust 不是重构完成标准。

## 13. 验收测试矩阵

### 13.1 Architecture tests

新增 `tests/architecture/`:

1. `test_all_production_modules_are_classified`
   - 扫描全部 `src/spx_spark/**/*.py`；任何未登记模块失败。
2. `test_dependency_direction`
   - AST 检查 domain/analytics/providers/application/infrastructure/entrypoints 方向。
3. `test_domain_has_stdlib_only`
4. `test_analytics_has_no_io_or_environment_access`
   - 禁止 `open`、`Path.read_*`、`os.environ`、socket、subprocess。
5. `test_provider_packages_do_not_import_analytics_or_alerts`
6. `test_entrypoints_contain_no_business_rules`
7. `test_no_direct_canonical_projection_writes`
8. `test_module_and_function_size_budget`
9. `test_no_new_untyped_status_state_mode_fields`
   - 新 dataclass 中这些字段必须是 Enum/Literal；遗留白名单逐 phase 归零。

### 13.2 Contract tests

1. aware UTC enforcement；
2. deterministic IDs；
3. duplicate quote resolution；
4. JSON round-trip；
5. schema version rejection；
6. provider consistency；
7. expiry/trading-class isolation；
8. no raw provider payload leakage；
9. event idempotency；
10. illegal state transition rejection。

### 13.3 Greeks tests

1. BS price/delta/gamma/vega 对固定 golden；
2. put-call parity；
3. gamma call/put equality；
4. vanna/charm analytical vs central difference；
5. speed/color/vomma/zomma step-halving stability；
6. `tau -> 0` floor behavior；
7. invalid spot/strike/IV；
8. vendor percent-vs-decimal normalization by provider contract；
9. scalar vs NumPy kernel differential，若启用 NumPy；
10. Python vs Rust differential，若启用 Rust。

误差要求:

- 一阶解析 Greeks `rtol <= 1e-10`；
- 高阶有限差分 `rtol <= 1e-4` 或使用按量纲定义的 absolute tolerance；
- Python/Rust batch 输出在相同输入下不得出现 NaN/None 语义差异。

### 13.4 Density tests

1. BS synthetic chain 恢复已知近似分位；
2. 40-strike Schwab fixture 为 READY 或给出明确质量原因；
3. 5 strikes -> BLOCKED_COVERAGE；
4. stale 40 strikes -> BLOCKED_STALE，而不是误报数量不足；
5. 单侧缺失 -> INSUFFICIENT_TWO_SIDED；
6. 中间 strike 缺口 -> GAPPED_GRID；
7. crossed/wide quote 被过滤；
8. 非单调 call curve；
9. 非凸 curve 和负质量裁剪；
10. 概率总质量归一化；
11. p10 <= median <= p90；
12. wall probability 在 `[0, 1]`；
13. 输入顺序变化不改变结果；
14. 相同 snapshot 重放字节级稳定。

### 13.5 State-machine tests

每个状态机必须有:

- transition table 全覆盖；
- 每个非法迁移；
- retry/backoff 边界；
- clock rollback/future timestamp；
- restart serialization round-trip；
- duplicate observation 幂等；
- terminal state 行为；
- effects 与 state 分离断言。

Provider runtime 额外覆盖:

- policy block -> allow -> start；
- competing session -> probe -> recover；
- partial subscription -> degraded；
- disconnect storm；
- unrecoverable contract error。

### 13.6 Storage/outbox tests

1. multiprocess latest projection update 不丢数据；
2. atomic replace crash injection；
3. SQLite busy/locked retry；
4. outbox append + claim + ack；
5. consumer crash before ack；
6. duplicate event append；
7. retry exhaustion -> dead letter；
8. raw write succeeds而 projection 失败时可恢复；
9. projection 可从 raw/latest facts 重建；
10. 文件权限保持 `0600`。

### 13.7 End-to-end tests

1. Schwab normalized batch -> snapshot -> analytics -> candidate -> outbox；
2. Schwab unavailable -> failover transition -> IBKR fallback；
3. both unavailable -> engine BLOCKED，无 executable output；
4. stale chain -> research-only/no pricing；
5. valid 0DTE chain -> density/exposure/order map；
6. notification review rejected -> 不投递但成功终态；
7. delivery transient failure -> retry -> delivered；
8. process restart -> no duplicate human notification；
9. replay同一 session -> deterministic decisions；
10. live path import 不加载 DuckDB。

## 14. 性能与长期运行验收

在 Oracle 生产同规格主机，以脱敏 fixture 和 shadow live 数据测量。

### 14.1 Analytics

输入上限基线: 500 option legs、2 expiries、101 spot scan grid points。

- Python analytics p95 `<= 100ms`；
- Python analytics p99 `<= 250ms`；
- 单 tick 不发生未解释的 `> 1s` pause；
- 输出与 scalar golden 一致。

这些不是对交易所行情延迟的要求，只测纯 analytics。

### 14.2 Engine

- 不含 LLM/notification 的 engine tick p95 `<= 300ms`；
- provider data available 到 projection publish p95 `<= 1s`；
- 关键任务 deadline miss 率 `< 0.1%`；
- task timeout 不阻塞其他 critical task；
- 连续 6 小时 RSS 增长 `< 5%` 或 `< 100MiB`，取较大值；
- latest projection 和 cache 有明确上限，不随到期日无限增长。

### 14.3 数据质量 shadow acceptance

至少连续 5 个完整 RTH session:

- Schwab SPXW front expiry density READY 比例 `>= 95%`；
- BLOCKED 的每次原因可归类；
- 双边率、宽度、gap、age 有分布报告；
- new/old analytics 关键输出差异有报告；
- 不因 shadow 路径增加 human notification。

## 15. Definition of Done

整个重构完成必须同时满足:

1. 全量 unit/integration/architecture tests 通过。
2. Ruff 和复杂度/规模 guard 通过。
3. 当前 6 个配置相关失败归零。
4. 未登记生产模块归零。
5. `order_map.py`、`stream_collector.py` 只剩 compatibility facade。
6. 纯 analytics 不访问 env/files/network。
7. 所有关键状态使用 Enum + 纯 transition function。
8. latest JSON 明确为 projection，不再承载可靠事件。
9. alert candidate 通过 SQLite outbox 可恢复、幂等消费。
10. heartbeat/readiness 能反映行情、analytics、outbox 和任务真实健康。
11. 5 个 RTH session shadow acceptance 通过。
12. 性能目标通过，或记录有证据的优化/Rust ADR。
13. systemd 服务经过 restart、timeout、SIGTERM 和磁盘只读故障演练。
14. README、module architecture、runtime configuration 文档同步。

## 16. 实施纪律

- 每个 phase 单独 PR/commit 边界，禁止把行为变更混入纯移动重构。
- 先 characterization tests，后移动，最后删 compatibility shim。
- 移动阶段 old/new 双跑并比较，不直接切换人类通知路径。
- 新 enum 必须定义 unknown/blocked 语义，禁止 `except ValueError: use default` 静默吞错。
- hard-coded 数值必须进入 typed policy/settings，公式常数除外。
- 状态文本、展示文本和机器状态分离；机器逻辑不匹配自然语言字符串。
- 删除旧路径前用 `rg` 和 architecture tests 证明无生产调用方。
- 任何 Rust/IPC/Kubernetes 提议必须附生产指标和 ADR；技术偏好不是实施依据。

## 17. 测试文件与验收命令

### 17.1 计划新增测试文件

```text
tests/
  architecture/
    test_module_registry.py
    test_dependency_rules.py
    test_pure_boundaries.py
    test_code_size_and_complexity.py
  contracts/
    test_market_snapshot_contract.py
    test_option_chain_contract.py
    test_analytics_result_contract.py
    test_domain_event_contract.py
    test_provider_contracts.py
  analytics/
    test_black_scholes.py
    test_higher_order_greeks.py
    test_chain_quality.py
    test_risk_neutral_density.py
    test_exposure.py
    test_analytics_differential.py
  state_machines/
    test_provider_runtime_machine.py
    test_replan_machine.py
    test_signal_machine.py
    test_delivery_machine.py
    test_engine_health_machine.py
  infrastructure/
    test_latest_projection_concurrency.py
    test_outbox_repository.py
    test_projection_recovery.py
    test_unix_stream_contract.py       # 仅 Phase 7 GO 时添加
  application/
    test_realtime_engine.py
    test_order_map_service.py
    test_alert_service.py
    test_notification_service.py
  e2e/
    test_provider_to_outbox.py
    test_failover_recovery.py
    test_restart_idempotency.py
    test_session_replay_determinism.py
  performance/
    test_analytics_benchmark.py
    test_engine_benchmark.py
```

### 17.2 每个 phase 的强制命令

```bash
uv run pytest -q
uv run ruff check src tests
uv run pytest -q tests/architecture
uv run pytest -q tests/contracts tests/state_machines
```

Phase 2 后增加:

```bash
uv run pytest -q tests/analytics tests/application/test_realtime_engine.py
uv run python scripts/benchmark-analytics.py --fixture tests/golden/spxw-rth.json
```

Phase 4 后增加:

```bash
uv run pytest -q tests/state_machines/test_provider_runtime_machine.py
uv run pytest -q tests/e2e/test_failover_recovery.py
```

Phase 6 后增加:

```bash
uv run pytest -q tests/infrastructure/test_outbox_repository.py
uv run pytest -q tests/e2e/test_restart_idempotency.py
```

### 17.3 验收报告

每个 phase 必须生成 `artifacts/refactor-acceptance/<phase>/report.json`，至少包含:

- git commit；
- Python/package version；
- 测试通过/失败/跳过数；
- Ruff 结果；
- module size violations；
- old/new differential summary；
- performance p50/p95/p99；
- fixture hash；
- schema/model/config versions；
- GO/NO-GO 结论和未解决风险。

`artifacts/` 可不提交大型运行输出，但 report schema、生成脚本和一个脱敏样例必须入库。

## 18. Provider quota utilization design

Schwab 的可实施详细设计已经独立为
`docs/schwab-wide-chain-hot-lane-design.md`；本节保留跨 provider 的长期 quota contract
与决策原则。两者冲突时，接口和 rollout 以详细设计为准，长期验收门槛以本文件为准。

### 18.1 当前能力边界

Schwab 的 Market Data REST endpoints 和 Trader API streamer login 均已验证。
当前部署运行 WebSocket live stream，ES/MES 已收到连续 Level-One 消息；SPXW
订阅虽然被接受，但 RTH/GTH option message 仍为零，因此单独标记为期权覆盖缺口，
不得扩大解释成 OAuth、gateway 或整个 Schwab provider 故障。

项目目前把 Schwab operational ceiling 配置为:

- 120 outbound requests/minute；
- quote endpoint 每个 request 最多 500 symbols；
- `$SPX` chain 每 5 秒一次、`strikeCount=40`；
- context quote symbols 合并为 500-symbol batches；
- theoretical normal load 约 40 requests/minute。

这些是 deployment policy，不得当作永远不变的 provider protocol constant。gateway 必须根据
实际 response、429、`Retry-After` 和 operator override 降低 effective capacity。

IBKR 官方默认提供至少 100 条 concurrent market-data lines；实际账户上限可能因账户条件、
佣金或 quote booster 改变，并且 TWS 和 API clients 共享。实现以 100 为 configured ceiling，
并通过成功订阅 high-watermark 与明确 ticker-limit rejection 维护持久化 effective estimate。
当前配置的 worst-case 约为:

```text
68 SPXW option lines
+ 16 SPY option lines
+ 2 direct anchors
+ 6 slow-poll peak
= 92 concurrent lines
```

这低于名义 100，但只剩约 8 条余量，对 TWS UI、replan overlap、取消延迟和其他 API client
不够稳健。目标 allocator 默认保留至少 10%-15% discovered capacity。

### 18.2 三种 quota 不得混淆

```python
class QuotaDimension(str, Enum):
    REQUESTS_PER_WINDOW = "requests_per_window"
    SYMBOLS_PER_REQUEST = "symbols_per_request"
    CONCURRENT_LINES = "concurrent_lines"

class DemandClass(str, Enum):
    P0_DIRECT_ANCHOR = "p0_direct_anchor"
    P1_HOT_0DTE = "p1_hot_0dte"
    P2_STRUCTURE = "p2_structure"
    P3_CONFIRMATION = "p3_confirmation"
    P4_CONTEXT = "p4_context"
    P5_RESEARCH = "p5_research"
```

- Schwab REST 主要受 requests/window 和 symbols/request 约束；
- `strikeCount` 增加通常增加 payload/latency，不一定增加 request count；
- IBKR streaming 主要受 simultaneous lines 约束；
- IBKR 轮换 subscription 可扩大时间覆盖，但不能增加同一时刻的 line capacity；
- provider entitlement、响应大小、pacing、session conflict 是独立限制，不能只看数字配额。

### 18.3 Quota contracts

```python
@dataclass(frozen=True)
class DataDemand:
    demand_id: str
    provider: Provider
    instrument_ids: tuple[str, ...]
    demand_class: DemandClass
    minimum_cadence_seconds: float
    desired_cadence_seconds: float
    max_staleness_seconds: float
    line_cost: int
    request_cost: int
    pair_key: str | None
    expires_at: datetime | None

@dataclass(frozen=True)
class QuotaCapacity:
    provider: Provider
    requests_per_minute: int | None
    symbols_per_request: int | None
    concurrent_lines: int | None
    reserved_fraction: float
    learned_at: datetime

@dataclass(frozen=True)
class QuotaLease:
    lease_id: str
    demand_id: str
    granted_at: datetime
    expires_at: datetime
    allocated_lines: int
    scheduled_at: datetime | None

class QuotaAllocator(Protocol):
    def plan(
        self,
        demands: Sequence[DataDemand],
        capacity: QuotaCapacity,
        usage: QuotaUsage,
        now: datetime,
    ) -> QuotaPlan: ...
```

Allocator 是纯函数。provider supervisor 执行 lease effects，回报 accepted/rejected/cancelled；
allocator 不直接发 HTTP 或 `reqMktData`。

### 18.4 Schwab allocation policy

Schwab 的优势是“一个 request 带大量 symbols 或一段完整 chain”。优先做 batch amplification，
而不是用满 120 次请求。

固定原则:

1. 所有普通 underlying/context quotes 合并成尽可能少的 quote batches。
2. 500 是 payload ceiling，不是“必须请求 500 个 symbols”的目标；只请求能进入 feature/health 的 universe。
3. SPX chain request 优先于次要 ETF chain。
4. 先增加同一次 SPX chain 的 strike width，再考虑增加相同 chain 的请求频率。
5. scheduler ceiling 使用 nominal limit 的 70%，即配置 120 时最多规划 84/min。
6. 固定剩余 30% 给 retry、OAuth/session maintenance、operator probes；不再额外保留
   未登记的隐性冗余。
7. 429 立即进入 THROTTLED，服从 `Retry-After`，成功窗口逐步恢复，不瞬间回到 nominal rate。

建议 cadence profile:

| Lane | Midday | Open/close or high-vol | Request cost |
| --- | ---: | ---: | ---: |
| SPX/SPXW chain | 5s | 2-3s after measured acceptance | 1 each |
| all direct/context quotes | 5s | 2-3s | 1 batch each |
| XSP/SPY chains | 15s | 10-15s | 1 per underlier |
| QQQ/IWM confirmation chains | 15-30s | 15s | 1 per underlier |
| slow context | included in quote batch | included | no extra request if same batch |

现有约 40/min 负载已经有大量 headroom。headroom 的第一用途应是:

- RTH 将 SPX `strikeCount` 从 40 shadow-test 到 80/100/120，扩大 density tails；
- 开盘、急速行情和尾盘临时提高 SPX chain/anchor cadence；
- 对失败 batch 做有限 retry；
- 保留 provider safety margin。

不建议为了“用满 quota”添加与 SPX 0DTE 无关的 400 个 symbols。无消费方的数据只增加解析、
存储、质量告警和研究偏差。

### 18.5 IBKR allocation policy

IBKR 的优势是持续 streaming、source timestamps、hot option updates 和独立 provider validation。
其 line budget 应按 mode 动态分配，而不是一个静态 `max_option_lines`。

建议 nominal 100-line 账户的 allocation:

| Lane | Schwab healthy shadow | IBKR fallback | Notes |
| --- | ---: | ---: | --- |
| direct anchors | 4 | 4 | SPX/ES and required liveness anchors |
| SPXW hot C/P | 32 | 48 | whole call/put pairs nearest ATM |
| SPXW structure rotation | 12 | 22 | walls, density width, next groups |
| SPY option validation | 0-4 | 8 | Schwab already supplies broad SPY chain |
| slow context temporary lease | 6 | 6 | rotate, then cancel |
| unallocated safety reserve | >=42 | >=12 | TWS/shared clients/replan/cancel lag |

规则:

1. C/P pair 是原子 allocation；不得只因最后一条 line 空余订阅单腿。
2. ATM hot lane 持续订阅；outer structure 按 TTL rotation。
3. Schwab healthy 时，IBKR 用于 hot validation 和 failover readiness，不重复完整 Schwab chain。
4. Schwab degraded/failover 时，扩张 IBKR hot/structure leases。
5. SPY option lines 从 persistent 16 降为 0-8；Schwab 已提供 SPY chain，释放的 lines 优先给
   SPXW tail coverage 或 safety reserve。
6. slow indexes/ETFs 使用短 lease，收到足够 sample 或 hold timeout 后立即 cancel。
7. replan 优先 make-before-break；只有 reserve 足够覆盖 overlap 才允许。否则先取消最远 P4/P5 leases。
8. 收到 max-ticker/pacing rejection 后，立即冻结新 lease，reconcile active subscriptions，
   再从最低 priority 回收。
9. account/position connection 与 market-data leases 分离，不能为了释放 quote line 失去 position visibility。

### 18.6 Adaptive quota state

```python
class QuotaMode(str, Enum):
    NORMAL = "normal"
    PRESSURE = "pressure"
    THROTTLED = "throttled"
    COOLDOWN = "cooldown"
    RECOVERING = "recovering"
```

Transition examples:

| From | Observation | To | Effect |
| --- | --- | --- | --- |
| NORMAL | usage > 80% | PRESSURE | stop P5, slow P4 |
| PRESSURE | rejection/429 | THROTTLED | freeze grants, honor retry delay |
| THROTTLED | retry window elapsed | COOLDOWN | allow P0/P1 only |
| COOLDOWN | consecutive success | RECOVERING | increase capacity gradually |
| RECOVERING | stable success window | NORMAL | restore configured ceiling |
| any | new rejection | THROTTLED | reduce learned capacity |

### 18.7 Quota observability

每 15 秒输出 provider quota snapshot:

- nominal/effective/reserved capacity；
- requests last 10s/60s；
- retries和429；
- IBKR active/requested/cancel-pending lines；
- usage by demand class；
- oldest waiting demand；
- dropped/deferred demands and reason；
- quote freshness by lane；
- useful-update ratio: batches containing at least one changed normalized quote；
- payload bytes、parse duration、persist duration。

Schwab collector 当前的 `requests_last_minute` 只反映 collector 记录的 logical calls；最终指标必须由
gateway 统计每一次 actual HTTP attempt，包括 retry。IBKR active line count 必须由 subscription registry
计算，不能只从配置推断。

### 18.8 Quota acceptance tests

新增:

```text
tests/quota/
  test_allocator_priority.py
  test_schwab_request_budget.py
  test_schwab_batch_amplification.py
  test_ibkr_line_budget.py
  test_quota_state_machine.py
  test_replan_overlap_budget.py
  test_quota_fairness.py
```

必须覆盖:

1. 任何随机 demand 集合都不超过 effective capacity；
2. reserve 不被普通 P2-P5 消耗；
3. C/P pair 原子 grant/revoke；
4. P0/P1 永不被 P3-P5 饿死；
5. P4/P5 在健康 quota 下有 bounded wait，防止永远不轮转；
6. Schwab 500 symbols 被正确拆 batch；
7. retry 计入 actual request budget；
8. 429 + Retry-After transition；
9. IBKR cancellation pending 仍计 active line；
10. replan overlap 不越线；
11. Schwab healthy -> IBKR shadow 和 failover 两套 plan 差异正确；
12. 动态 capacity 从 100 降至 80 时按 priority 回收；
13. 相同 input/capacity 产生 deterministic plan；
14. 5 个 RTH session 中 quota rejection 为 0，且 safety reserve 从不低于策略值。
