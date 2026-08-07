# SPX Spark 架构简化与第三方能力替代总方案 v1

状态：**仓库级设计基线，代码修改前必须先确认本方案**
适用仓库：`hzy-hits/SpxOpDaily`
目标读者：项目维护者、GPT-5.6 Sol、Codex 及其他代码 Agent
核心目标：**把 SPX Spark 从“功能很多、边界很多、每层都手撸”的系统，收敛为一个可理解、可删除、可持续演进的单机模块化应用。**

配套执行文档：`docs/architecture-simplification-execution-plan-v1.md`（对本方案的事实核实、偏差澄清与逐阶段任务卡）。两份文档冲突时，以执行文档为准，因为它锚定了仓库真实文件。

---

## 0. 本文档如何使用

本文档不是建议清单，也不是让 Agent 自由发挥的灵感来源。它是后续重构工作的上位约束。

任何 Agent 在修改本仓库前，必须按以下顺序工作：

1. 阅读本文档、`AGENTS.md`、与任务直接相关的源文件。
2. 先提交一份 **Change Brief**，说明要解决的用户问题、准备复用的现有能力、准备引入的第三方包、将删除的旧实现、涉及的服务和存储边界。
3. 用户确认 Change Brief 后才允许修改代码。
4. 修改必须完成一个最小端到端结果；不得先搭框架、状态机、兼容层和测试矩阵，再回头实现用户要的功能。
5. 每次交付必须报告 **复杂度变化**：新增/删除文件、代码行、配置项、systemd unit、数据库表、后台进程和依赖。

未经明确授权，Agent 不得：

- 新建 systemd service 或 timer；
- 新增 Rust crate 或跨语言 wire contract；
- 新建数据库、队列、事件总线或通用状态机框架；
- 为一个调用点创建 interface / port / adapter / repository 抽象；
- 以“以后可能需要”为理由保留新旧两套实现；
- 用“还需要再 Shadow N 天”代替工程实现；
- 因为测试容易编写而扩大任务范围。

---

## 1. 执行摘要

### 1.1 当前项目的问题不是代码质量差，而是职责边界过多

当前仓库已经具备较强的数据质量意识、安全边界和审计能力，但这些能力被实现成了过多的自研基础设施：

- 43 个 Python console entrypoint；
- 自研线程池调度器、子进程 runner、心跳和 stdout JSON 协议；
- 大量 systemd service / timer；
- 多个手写 HTTP server；
- 手写配置合并、环境变量解析和 schema 校验；
- 手写 SQLite outbox、claim、retry、receipt、dead-letter 和 migration；
- JSON 文件充当进程间总线；
- Python 与 Rust 同时拥有 domain contract、ledger、report、notification 和 lifecycle；
- 自研 AST 架构检查器维护完整模块登记表；
- 策略、通知、回放和交付分别存在多套状态机。

这导致项目出现一种典型失衡：

> **业务决策仍然相对机械，但支撑这些决策的工程复杂度已经接近多团队系统。**

### 1.2 总体决策

本方案采用以下技术方向：

1. **以 Python 3.12 模块化单体为主运行时。** Broker collector 因 session ownership 保持独立进程，其余 feature、strategy、API、report、notification 尽量收敛。
2. **冻结并逐步退出 Rust 生产控制面。** Rust 不再承接新功能；只有经过 profiling 证明的 CPU 热点才允许保留为窄接口数值组件。
3. **购买通用基础设施，保留交易领域逻辑。** HTTP、配置、数据库迁移、任务调度、重试、日志、CLI、架构检查、交易日历使用成熟包；SPX/ES basis、Gamma、wall、pin、期权收益结构和数据 entitlement 继续自研。
4. **停止把 JSON 文件当作内部模块总线。** 同一进程内直接函数调用；跨 collector 边界只保留少数完整 provider snapshot 或 UDS API。
5. **统一运行职责。** 最终只保留 4 个应用服务：IBKR、Schwab、Core、Worker；IB Gateway/IBC 作为外部依赖单独存在。
6. **统一存储职责。** 一个 operational SQLite 数据库、一个轻量 job queue store、Parquet 历史湖和 DuckDB 研究查询；不再为每条链路各自维护状态数据库和 receipt 文件。
7. **测试公共行为和风险不变量，不测试实现形状。** 架构规则交给 Import Linter，数学边界交给 Hypothesis，少量冻结 session 验证策略行为。
8. **迁移必须删除旧实现。** 每一阶段只允许一个 owner；兼容入口最多保留一个发布周期，不能无限期双写、双读和双投递。

### 1.3 目标不是“引入更多第三方包”

第三方包只在以下条件满足时引入：

- 能删除一块明显的基础设施代码；
- 能替代至少一个常驻服务、一个复杂状态机或约 300–500 行通用代码；
- 有持续维护、清晰文档、Python 3.12 支持和可接受许可证；
- 项目中不存在另一套解决同一职责的包；
- 引入后系统的 owner 数量减少，而不是增加一层 wrapper。

核心原则：

> **Buy infrastructure, write domain.**
> 通用基础设施用成熟实现；真正形成交易差异的领域判断自己写。

---

## 2. 当前架构梳理（As-Is）

### 2.1 运行时拓扑

当前系统大致由以下层次组成：

```text
IB Gateway / IBC
      │
      ├─ IBKR snapshot collector / persistent stream / verifier / farm probe
      ├─ Schwab OAuth / gateway / collector / market-data loop
      ├─ Hyperliquid / Polymarket collectors
      │
      ▼
raw JSONL + latest/state.json + 多个 latest/*.json projection
      │
      ├─ options map / IV surface / Greeks / market features
      ├─ shock / morning map / order map / regime signal
      ├─ strategy / virtual strategy / replay / post-close
      │
      ▼
Python notification pipeline + SQLite outbox + receipts
      │
      ▼
Rust bridge / core / ledger / report / delivery
      │
      ▼
OpenClaw / Bark / Feishu / human-facing reports
```

与此同时，项目还有：

- 自研 `service_loop` 调度一批 CLI 子进程；
- 独立 hot worker 和 sampler；
- 大量 systemd unit 分别运行相邻职责；
- HTTP replay、live surface、Schwab OAuth/gateway 各有独立 server；
- Python 与 Rust 之间使用版本化 JSON/golden contract 对齐。

### 2.2 Python CLI 已经成为内部 RPC

`pyproject.toml` 当前注册 43 个 `spx-spark-*` console script。很多模块不是给人直接操作的 CLI，而是被另一个 Python 调度器作为子进程调用。

现有 `scheduler.py` 使用 `ThreadPoolExecutor` 判断任务到期、维持 in-flight 集合、聚合健康状态和发送 heartbeat；`runner.py` 再启动进程组、管理 SIGTERM/SIGKILL、捕获 stdout/stderr，并根据 task name 手工解析不同 JSON 输出。

这相当于自行实现了一个小型 job supervisor 和 RPC protocol，但内部任务仍在同一仓库、同一主机和同一 Python 环境中。

### 2.3 文件系统承担了过多运行时协调

项目中的 atomic JSON 与 file lock 本身并不坏。问题是它们被广泛用作：

- provider latest state；
- feature projection；
- strategy state；
- notification state；
- lifecycle coordination；
- replay source；
- cross-runtime transport；
- health lease。

`state_io.py` 的 atomic write 和 advisory lock 是一块小而清晰的 Linux 工具，应保留。但 `storage.py` 和周围 projection 体系使“写文件、再由下一个进程读文件”成为默认架构，造成：

- schema 到处复制；
- freshness 与 identity gate 到处重复；
- 读取和写入时钟难以统一；
- 每增加一个 feature 就增加一个文件 owner 和 service；
- 测试需要构造大量 JSON fixture。

### 2.4 配置体系同时承担默认值、部署、策略和兼容

当前配置由：

- `config.py` 的 env helper 和 dotenv parser；
- `settings/loader.py` 的 YAML deep merge、环境覆盖、provenance；
- `settings/schema.py` 的 dataclass aggregate；
- 约 108 KB 的 `config/runtime.yaml`；
- 分散在代码里的 `runtime_value()` / env helper；

共同组成。

问题不只是手写 loader。更大的问题是几乎所有策略阈值、运行参数、兼容开关、服务 ownership 和实验选项都可配置，导致：

- 有效配置难以重建；
- Agent 倾向于“再加一个开关”而不删除旧路径；
- policy version hash 覆盖大量不稳定字段；
- 一个简单需求可能触及 settings、YAML、env、tests 和部署 overlay 五处。

### 2.5 HTTP 层重复实现协议细节

仓库至少存在两套明显的手写 HTTP transport：

- Schwab OAuth callback 与本地 market-data gateway；
- surface replay HTTP/Unix socket API。

这些模块自行处理：

- 路由；
- query parsing；
- Content-Length；
- security headers；
- health endpoint；
- ETag；
- GET/HEAD；
- TCP/Unix socket server；
- shutdown 和 request thread。

其中安全处理有价值，但不应由业务仓库长期维护一个 web framework。

### 2.6 通知系统存在多重 owner 与重复耐久化

当前通知链包括：

- Python reviewer pipeline；
- 自研 SQLite delivery outbox；
- target claims、retry schedule、stale claims、receipts、cancellation fence；
- missed queue 与 dead letter；
- Rust ingress、Rust ledger 和 Rust delivery；
- 多种 delivery receipt mirror。

这套设计解决了许多理论故障，但对一个单用户、单主机、低吞吐的交易辅助系统而言，状态空间过大。多数日常修改都需要理解“candidate → Python outbox → Rust staging → target delivery → receipt mirror → source acknowledgement”的完整链。

### 2.7 Python/Rust 双控制面是最大的维护放大器

当前文档明确把以下职责分给 Rust：

- domain invariants；
- market frame bridge；
- SQLite ledger；
- report scheduling；
- delivery coordination；

Python 同时仍拥有：

- provider、normalization、strategy；
- notification pipeline；
- outbox producer；
- report projection；
- compatibility and replay。

因此每次修改都可能需要：

- Python schema；
- Rust struct；
- golden fixture；
- conversion；
- ledger migration；
- report rendering；
- delivery compatibility；
- 双语言测试。

Rust 在这里主要承担“可靠控制面”，而不是性能瓶颈。对于当前规模，跨语言一致性的成本大于收益。

### 2.8 手写基础设施的清单

| 领域 | 当前自研实现 | 问题 |
|---|---|---|
| 配置 | env parser、dotenv、YAML merge、provenance、dataclass validation | 重复、配置面过宽 |
| HTTP | BaseHTTPRequestHandler、手工 routing/headers/UDS | 协议细节进入业务代码 |
| 调度 | ThreadPoolExecutor scheduler、subprocess runner、heartbeat | 内部 CLI 被当作 RPC |
| 重试 | 各 provider、delivery、collector 自己维护 backoff | 行为不一致 |
| 日志 | `print(json.dumps(...))` + stdout parser | 日志与进程协议耦合 |
| DB | sqlite3 DDL、ALTER 检查、transaction、migration | schema 演进分散 |
| 通知 | outbox、claim、retry、receipt、dead letter、mirror | 状态机过大 |
| 架构检查 | AST import parser + 手写模块登记表 | 每新增模块都修改测试代码 |
| 日历 | 美股假日、复活节、early close、GTH/Globex | 交易所规则长期维护风险 |
| IPC | latest JSON、lease JSON、projection JSON | schema/freshness 重复 |
| 跨语言 | Python/Rust contracts/golden fixtures | 修改成本翻倍 |

---

## 3. 复杂度根因

### 3.1 把单机应用按分布式系统方式设计

当前项目只有一台 Oracle 主机、一个主要操作者和低吞吐通知，但采用了：

- 多进程 pipeline；
- 文件消息总线；
- 多 owner delivery；
- 跨语言 contract；
- durable receipt mirror；
- 多级 fallback spool；
- 多套 state machine。

这些模式本身并非错误，但它们通常用于：

- 多主机；
- 多团队；
- 高吞吐；
- 独立部署；
- 强监管审计；
- 无人值守自动执行。

SPX Spark 当前不具备这些约束，因此大部分属于 accidental complexity。

### 3.2 把“可测试”误当成“应该存在”

GPT-5.6 Sol 很容易看到一个失败场景，就新增：

```text
新字段
→ 新 enum
→ 新 schema version
→ 新 lifecycle
→ 新兼容路径
→ 新 fixture
→ 新架构登记
→ 新文档
```

每一步都有测试，因此看起来很严谨；但系统并没有减少用户操作，也没有提高交易判断。测试只是把新增复杂度固定下来。

### 3.3 兼容路径没有明确期限

仓库存在 facade、legacy writer、Python/Rust owner 切换、old schema migration 和 dual path。若每次迁移只“增加新路径但不删除旧路径”，项目必然单调变复杂。

新的硬规则是：

> 每次迁移必须写明旧实现的删除提交；兼容层最多存在一个发布周期。

### 3.4 过度配置化掩盖了缺乏架构决策

当不确定某条路径是否保留时，当前倾向是加一个 enable flag。久而久之，配置文件成为历史分支的墓地。

新原则：

- 主机、凭据、端口和资源上限是配置；
- 经过确认的业务规则是代码；
- 未确认的研究参数属于研究 notebook / replay spec；
- 旧实现不通过 feature flag 永久保留。

---

## 4. 目标架构（To-Be）

### 4.1 总体形态：单机模块化单体 + 必要的 provider 隔离

```text
                     ┌──────────────────────┐
                     │ IB Gateway / IBC     │
                     └──────────┬───────────┘
                                │
               ┌────────────────┴────────────────┐
               │                                 │
      ┌────────▼────────┐               ┌────────▼────────┐
      │ spx-ibkr        │               │ spx-schwab      │
      │ persistent feed │               │ session/feed/API│
      └────────┬────────┘               └────────┬────────┘
               │ normalized snapshot / UDS       │
               └────────────────┬────────────────┘
                                │
                      ┌─────────▼─────────┐
                      │ spx-core          │
                      │ quote book        │
                      │ features          │
                      │ strategies        │
                      │ decisions         │
                      │ FastAPI local API │
                      └─────────┬─────────┘
                                │
                         operational SQLite
                                │
                      ┌─────────▼─────────┐
                      │ spx-worker        │
                      │ Huey jobs         │
                      │ delivery/report   │
                      │ compaction        │
                      └─────────┬─────────┘
                                │
                   Parquet + DuckDB / human sinks
```

### 4.2 最终常驻服务上限

应用层只保留：

1. `spx-ibkr.service`
2. `spx-schwab.service`
3. `spx-core.service`
4. `spx-worker.service`

外部依赖：

5. `ibc-gateway.service`

不得为一个 feature、report、sampler、surface 或 strategy 新建 service。它们分别成为 Core 的内部任务或 Worker 的 job。

### 4.3 各服务职责

#### `spx-ibkr`

保留独立进程，因为 IBKR session、client id、market-data line budget、10197 冲突和 entitlement 具有独立生命周期。

只负责：

- 连接和恢复；
- contract qualification；
- line budget / hot lane / exact-leg subscription；
- raw provider payload；
- normalized provider snapshot；
- provider health。

禁止负责：

- 策略；
- 通知；
- human rendering；
- report；
- cross-index regime。

#### `spx-schwab`

只负责：

- OAuth/token owner；
- Schwab REST/stream session；
- chain discovery / rate limit；
- normalized provider snapshot；
- OAuth callback 和本地 provider health API。

#### `spx-core`

一个 Python asyncio 进程，使用 `TaskGroup` 管理连续任务。负责：

- 内存 quote book；
- provider selection / freshness；
- 5 秒、1 分钟、5 分钟 bar；
- option analytics；
- market state；
- strategy idea/candidate；
- decision persistence；
- local FastAPI health、dashboard、replay query endpoint；
- 将需要异步处理的 job 投递给 Huey。

同一进程中的 feature 之间直接调用函数，不再通过 `latest/*.json` 互相通信。

#### `spx-worker`

一个 Huey consumer，使用 SQLite backend。负责：

- 通知发送和有限重试；
- post-close report；
- session finalization；
- closed-hour compaction；
- periodic maintenance；
- Hyperliquid / Polymarket 等低优先级采集；
- 慢速 LLM review。

Worker 不参与 5 秒热路径，不得阻塞 Core。

---

## 5. 数据与状态设计

### 5.1 状态角色固定为四类

| 角色 | 技术 | 用途 |
|---|---|---|
| 热状态 | Core 内存 | quote book、bars、active lifecycle |
| 操作事实 | SQLite + SQLAlchemy | decisions、events、jobs、deliveries、outcomes、manifests |
| 历史行情 | ZSTD Parquet | raw/normalized quote history、features、bars |
| 研究查询 | DuckDB | replay、research views、统计 |

Huey 自己使用一个独立 `huey.sqlite` queue store。整个项目允许的 mutable database 文件上限为两个：

```text
spx.sqlite
huey.sqlite
```

### 5.2 JSON 文件的新定位

允许保留：

- IBKR/Schwab collector 的 last-known complete snapshot，供 Core 重启恢复；
- dashboard/export 的只读 projection；
- immutable replay artifact；
- 调试和人工检查输出。

禁止继续作为：

- 同一进程内部 feature IPC；
- notification acknowledgement；
- lifecycle owner 协调；
- scheduler lease；
- Python/Rust wire；
- 数据库 migration 替代品。

### 5.3 Provider 到 Core 的边界

迁移初期可继续读取两个 atomic snapshot 文件。目标边界使用本机 Unix Domain Socket：

```text
POST /internal/provider-snapshots/ibkr
POST /internal/provider-snapshots/schwab
```

- FastAPI/Pydantic 做请求验证；
- HTTPX 使用 UDS transport；
- 每个 payload 是完整 snapshot，而不是逐 tick event；
- 包含 provider sequence、source_at、received_at 和 snapshot hash；
- Core 按 provider+sequence 幂等替换；
- engine 不可用时 collector 继续写 raw history，并重试最新完整 snapshot。

这比引入 Kafka/NATS/Redis Streams 更符合当前规模。

### 5.4 Operational DB

使用 SQLAlchemy Core，不要求 ORM entity graph。Alembic 负责 forward-only migration。

建议保留的核心表：

```text
sessions
events
decisions
decision_legs
outcomes
notification_events
notification_attempts
provider_incidents
compaction_manifests
schema_migrations
```

删除以下模式：

- 每个子系统自己检查 `PRAGMA table_info` 再 `ALTER TABLE`；
- receipt DB、outbox DB、strategy DB、Rust ledger 分别迁移；
- JSON state 和 SQLite row 双 owner；
- 同一事实被 mirror 到多处作为权威来源。

---

## 6. 第三方能力替代矩阵

### 6.1 立即采用

| 职责 | 当前实现 | 目标依赖 | 决策 |
|---|---|---|---|
| 配置与输入模型 | 手写 env/YAML/dataclass/validation | `pydantic` + `pydantic-settings` | 替换 |
| HTTP/API | `BaseHTTPRequestHandler` / `ThreadingHTTPServer` | `FastAPI` + `Uvicorn` + `HTTPX` | 替换 |
| CLI | 43 个独立 entrypoint、各自 argparse | `Typer` | 合并为一个 `spx` 命令树 |
| 周期任务与重试 job | 自研 scheduler、runner、systemd timer 组合 | `Huey` / `SqliteHuey` | 替换非热路径 |
| 网络重试 | 各模块手写 backoff | `Tenacity` | 替换 |
| SQLite access | 直接 sqlite3 + 手写 DDL/migration | `SQLAlchemy Core` + `Alembic` | 替换 |
| 结构化日志 | `print(json.dumps)` + stdout parser | `structlog` + stdlib logging/journald | 替换 |
| 架构规则 | 自研 AST parser + LAYERS registry | `Import Linter` | 替换 |
| 交易日历 | 手写 NYSE holidays/early close | `exchange_calendars` + 小型 SPX GTH overlay | 部分替换 |
| 数学性质测试 | 大量示例测试 | `Hypothesis` | 用于 payoff、serialization、time invariants |

### 6.2 研究层可选采用

| 用途 | 依赖 | 边界 |
|---|---|---|
| IV/root/curve fitting | `scipy.optimize` | 研究与纯 analytics；不进入 orchestration |
| 无套利曲面约束 | `CVXPY` 或 SciPy constrained optimizer | 仅当密度 v2 开始实现时引入 |
| 大型列式研究 | DuckDB 已足够 | 暂不引入 Polars/Pandas 新依赖 |
| HMM 训练 | `hmmlearn` / pomegranate | 当前固定 3-state online filter 不需要；需要训练时再评估 |

### 6.3 明确不引入

当前规模禁止引入：

- Kubernetes；
- Kafka；
- RabbitMQ；
- NATS；
- Temporal；
- Airflow；
- Dagster；
- Prefect；
- Spark；
- Flink；
- Elasticsearch；
- 多节点 Redis cluster；
- LangChain / LangGraph；
- 通用 event-sourcing framework；
- 通用 finite-state-machine package；
- service mesh。

这些技术不会解决当前问题，反而会把单机维护负担进一步放大。

---

## 7. 每项替代的具体边界

### 7.1 Pydantic Settings：替换配置加载，不搬运配置垃圾

迁移目标不是把 108 KB YAML 原样变成 108 KB Pydantic model。

配置只保留：

```text
broker credentials / account mode
network host / port / paths
provider capacity and rate limits
service enablement
storage paths and retention
notification destinations
risk hard caps
```

以下内容不再做部署配置：

- 每个临时策略分支的阈值；
- 每个 lifecycle 的实验性 timeout；
- legacy compatibility flags；
- 为测试方便暴露的内部常量；
- 没有第二个合法值的选项。

目标文件：

```text
config/defaults.toml
config/production.toml
.env / secrets_dir
```

优先级：

```text
code defaults < defaults.toml < production.toml < environment < init override
```

迁移完成后删除：

- 自研 dotenv parser；
- `env_bool/env_int/env_float/env_csv`；
- YAML deep-merge/provenance loader；
- PyYAML 生产依赖；
- runtime_value allowlist tests。

### 7.2 FastAPI/Uvicorn：统一 HTTP，但不把业务写进 route

统一两类 API：

```text
spx-schwab FastAPI
  /oauth/callback
  /healthz
  /provider/*

spx-core FastAPI
  /healthz
  /api/replay/*
  /api/live/*
  /internal/provider-snapshots/*
```

Uvicorn 支持 TCP 和 UDS；不再维护自定义 TCP/Unix server 类。

Route 只允许：

- Pydantic input/output；
- authentication/loopback check；
- 调用 application function；
- 映射 domain error 到 HTTP status。

禁止在 route 中写：

- replay 算法；
- provider selection；
- OAuth token business logic；
- strategy；
- file I/O state machine。

### 7.3 Typer：把 CLI 还原为人类入口

最终只保留一个 console script：

```text
spx
```

命令树示例：

```text
spx status
spx marketdata ibkr run
spx marketdata schwab run
spx replay session 2026-08-06
spx strategy evaluate
spx report post-close
spx data compact
spx notify test
spx ops runtime-mode
```

内部模块之间不得通过 `spx` CLI 通信。CLI 只是 composition root。

原有 `spx-spark-*` alias 可作为薄 wrapper 暂留一个发布周期，然后删除。

### 7.4 Huey：替换 job orchestration，不接管实时行情

Huey 负责：

- 定时 report；
- 通知 delivery；
- retryable provider side job；
- compaction；
- maintenance；
- slow LLM review；
- outcome sampling。

Huey 不负责：

- IBKR/Schwab persistent stream；
- 5 秒 market state；
- hot exact-leg subscription；
- 内存 quote book；
- latency-sensitive shock path。

实时任务在 `spx-core` 中使用 `asyncio.TaskGroup`；进程 crash/restart 交给 systemd。

通知语义简化为：

```text
notification_events: pending / processing / delivered / failed / uncertain
Huey task: execute one event
explicit retryable error: Huey retry
unknown delivery outcome: uncertain，不自动重复发送
```

不再维护独立 target claim、receipt mirror、missed queue 和 Rust ingress 多层状态。

### 7.5 SQLAlchemy/Alembic：统一 DB owner

优先使用 SQLAlchemy Core 的 table/statement/transaction，不需要为所有表建立 ORM relationship。

所有 schema 演进通过 Alembic revision；禁止运行时 `PRAGMA table_info` 后临时 ALTER。

每次 migration 必须：

- forward-only；
- 有唯一 revision；
- 能在生产 DB copy 上运行；
- 不在 application startup 隐式改 schema；
- 迁移与代码同一提交。

### 7.6 structlog：日志不再充当 API

日志统一包含：

```text
event
service
session_date
provider
instrument
source_at
received_at
decision_id
opportunity_id
error_code
duration_ms
```

stdout 只输出日志，不再要求 scheduler 按 task name 解析不同 JSON schema。

机器状态通过：

- health endpoint；
- operational DB；
- systemd exit code；

获得，而不是从 subprocess stdout 猜测。

### 7.7 Import Linter：删除自研架构测试

目标只保留四层：

```text
web/cli
application
infrastructure/providers
analytics/domain
```

Import Linter 定义：

- layers contract；
- provider independence；
- domain forbidden imports；
- protected infrastructure modules。

删除：

- 手写 AST import parser；
- `LAYERS` 全模块登记字典；
- 每增加一个顶层模块都修改测试的机制。

### 7.8 exchange_calendars：只保留产品特有 overlay

第三方日历负责：

- XNYS session；
- holiday；
- early close；
- previous/next session；
- session minutes。

本项目只保留：

- SPX GTH 20:15–09:25 overlay；
- RTH 与 GTH 的 session-date 映射；
- CME maintenance 与 product-specific exception；
- `review_ready_at` 这类项目业务时钟。

不得再手写复活节和联邦/交易所假日算法。

### 7.9 Hypothesis：减少测试数量，提高不变量覆盖

适用：

- Call/Put/vertical/butterfly payoff；
- breakeven 与 max loss；
- timestamp normalization；
- provider sequence 幂等；
- config precedence；
- serialization round-trip；
- no-lookahead 条件。

不适用：

- prompt 逐字输出；
- 巨型 JSON snapshot；
- 内部 helper 调用顺序；
- 未校准策略阈值。

---

## 8. 明确保留的自研能力

第三方包不能替代项目真正的领域价值。以下内容继续自研，并要求保持纯函数、小接口和明确单位。

### 8.1 Provider-specific adapters

- IBKR contract qualification；
- market-data entitlement；
- line-budget planner；
- 10197 conflict/backoff；
- Schwab chain/rate-limit semantics；
- provider timestamp mapping。

Broker SDK 只能提供 API，不会替项目处理这些业务约束。

### 8.2 Market-data truth model

必须保留：

- source_at / received_at / available_at；
- live/delayed/frozen/stale/missing；
- same-provider / same-expiry quote coherence；
- SPX/ES coordinate separation；
- fail-closed eligibility。

可以用 Pydantic 表达模型，但语义仍属于项目。

### 8.3 SPX 领域分析

保留：

- SPX/ES basis；
- expected move；
- wall / flip / zero gamma；
- 0DTE Greeks 与情景；
- IV/skew/density；
- pin/de-pin；
- market breadth 与权重抵消；
- strategy payoff/economics。

### 8.4 小型数学算法

当前固定参数、三状态 online Gaussian filter 本身很小，不应为了“使用第三方”换成大型 HMM framework。若未来进入训练、EM、模型选择和 walk-forward calibration，再采用成熟包。

### 8.5 atomic snapshot helper

`state_io.py` 的 atomic replace、fsync 和 file lock 仍适合：

- provider last-known snapshot；
- immutable export；
- local control file。

应限制使用范围，而不是删除一个正确的小工具后引入新的 wrapper。

---

## 9. Python/Rust 收敛决策

### 9.1 推荐决策：退出 Rust 生产控制面

原因：

- 当前瓶颈不在 CPU；
- provider SDK 和研究生态主要在 Python；
- Rust 负责的是 ledger/report/delivery，而这些可由成熟 Python 基础设施承担；
- 跨语言 contract、golden fixtures 和 owner fence 是显著维护成本；
- 一个用户的 manual-only 系统不需要双语言控制面。

### 9.2 Rust 冻结规则

从本文档确认起：

- Rust 不接受新策略、新通知、新报表和新 lifecycle；
- 只修复生产故障和安全问题；
- 不新增 crate、wire version 或 golden fixture；
- 不把新的 Python feature 同步翻译成 Rust。

### 9.3 迁移顺序

1. Python Operational DB 接管 ledger schema；
2. Python Worker 接管 report schedule；
3. Python Worker 接管 notification delivery；
4. 删除 Rust ingress 与 receipt mirror；
5. 删除 cross-runtime golden contracts；
6. 停止 Rust systemd units；
7. 打 tag 后从 active tree 删除 `rust/`，历史由 Git 保存。

每步都是 owner cutover，不长期双写。

### 9.4 允许保留 Rust 的唯一条件

只有满足以下条件才保留窄 Rust 组件：

- profiler 证明 Python hot path 不满足明确 latency/CPU 预算；
- Rust 组件是纯函数/批处理 kernel；
- 通过 PyO3 或 Arrow/Parquet 等成熟边界调用；
- 不拥有数据库、通知、调度、配置或 lifecycle；
- 删除的 Python 热点代码多于新增的跨语言 glue。

---

## 10. 策略与 LLM 的目标形态

架构简化以后，策略不再各自拥有 service、state file、notification lifecycle 和 contract。

### 10.1 统一策略接口

```text
MarketSnapshot
    -> generate_ideas(snapshot)
    -> price_expressions(idea, chain)
    -> rank(candidates + NoTrade)
    -> Decision
```

每条 Idea 只需要：

```text
thesis
supporting facts
contradicting facts
trigger
invalidation
horizon
candidate expressions
NoTrade reason
```

Vertical、Butterfly 是 expression，不是独立子系统。

### 10.2 LLM 不是 orchestrator

LLM 只负责：

- 从确定性 fact pack 中发现冲突；
- 生成/批判候选假设；
- 写简洁的 desk explanation。

LLM 不负责：

- 调度；
- DB；
- 报价；
- 概率和盈亏数学；
- lifecycle；
- 下单；
- inventing facts。

使用 provider 官方 SDK 或现有 HTTP client，加 Pydantic structured output。**不引入 LangChain/LangGraph。**

### 10.3 策略实现预算

一个新策略首版：

- 最多一个 generator 文件；
- 最多一个 economics 文件；
- 最多一个 integration touchpoint；
- 不新增 service；
- 不新增 DB；
- 不新增通用状态机；
- 不新增跨语言 contract；
- 公共行为测试不超过一个文件。

若超过，需要用户明确批准。

---

## 11. 目标代码组织

不进行一次性大搬家。逻辑目标如下，迁移时在现有目录内逐步对齐：

```text
src/spx_spark/
  domain/              # 纯类型、交易数学、时间语义
  analytics/           # 纯计算，输入 snapshot，输出 value objects
  providers/           # ibkr/schwab/external adapters
  application/         # quote book、features、strategy、decision
  infrastructure/      # SQLAlchemy、Huey、notification、Parquet
  web/                 # FastAPI routes 和 dependencies
  cli.py               # Typer composition root
```

依赖方向：

```text
web / cli
    ↓
application
    ↓
analytics / domain

infrastructure 和 providers 通过 application composition 注入；
domain/analytics 不 import web、DB、broker、notification。
```

避免为了“整齐”把当前几百个文件全量移动。每次只在替换职责时调整路径，并在同一阶段删除旧路径。

---

## 12. 分阶段迁移方案

迁移不按时间承诺，而按可验收结果推进。

### Phase 0：冻结与盘点

**目标**：停止继续增加新架构分支。

动作：

- 将本文档加入仓库并从 `AGENTS.md` 链接；
- 标记 Rust、legacy service loop、legacy notification 为 frozen；
- 生成 service / timer / CLI / SQLite / latest JSON owner 清单；
- 标出每个 owner 的唯一消费者；
- 禁止新 feature flag 和新 systemd unit。

验收：

- 所有生产职责都有一个目标 owner；
- 有明确删除清单；
- 后续 PR 能引用 phase 和 cutover 项。

### Phase 1：开发基础设施统一

**目标**：先统一 Agent 工作方式和新代码基础，不改变业务行为。

采用：

- Pydantic / pydantic-settings；
- structlog；
- Typer；
- Import Linter；
- Tenacity；
- Hypothesis。

动作：

- 建立一个最小 `AppSettings`；
- 新日志入口；
- 新 `spx` CLI；
- import-linter contract；
- 替换一个代表性 provider retry；
- 替换 payoff invariants 测试。

删除：

- 相应手写 helper 和重复测试；
- 不允许只加新库而保留旧入口永久并行。

### Phase 2：HTTP 统一

**目标**：删除自研 web transport。

顺序：

1. surface replay → FastAPI route；
2. surface live → 同一 Core app；
3. Schwab OAuth/gateway → Schwab FastAPI app；
4. Uvicorn UDS / loopback binding；
5. 删除 BaseHTTPRequestHandler server classes。

验收：

- API response contract 保持；
- OAuth secret 不进入日志；
- UDS/loopback 安全边界保持；
- 旧 HTTP 实现删除。

### Phase 3：Core 进程与内部直接调用

**目标**：删除内部 CLI RPC 和 service loop。

动作：

- 在 `spx-core` 中使用 `asyncio.TaskGroup` 运行 bars、features、strategy 和 health；
- 直接调用 Python function；
- 将 stdout JSON summary 替换为 structlog + health state；
- systemd 只监督 Core process。

删除：

- `application/runtime/scheduler.py`；
- `application/runtime/runner.py`；
- 大部分 registry task wrapper；
- `spx-spark-service-loop`；
- 仅供内部调度的 console scripts；
- 对 task-specific stdout 的解析逻辑。

### Phase 4：Job/通知统一

**目标**：一个 Worker、一条 delivery 路径。

动作：

- 引入 SqliteHuey；
- post-close、compaction、maintenance、notification 迁移为 tasks；
- SQLAlchemy 建立简化 notification event/attempt 表；
- explicit retryable failure 才自动重试；unknown outcome 标 uncertain。

删除：

- 自研 scheduler 的慢任务；
- missed queue；
- Python custom outbox claim 状态机；
- receipt mirror；
- Rust ingress；
- 重复 recovery timer。

### Phase 5：Operational DB 统一

**目标**：一个事实 owner。

动作：

- SQLAlchemy Core table definitions；
- Alembic migration；
- decisions/events/outcomes/delivery/manifests 迁移到 `spx.sqlite`；
- JSON projection 由 DB/内存单向导出。

删除：

- runtime startup DDL；
- 每模块 sqlite3 schema；
- 多 DB mirror；
- state file 与 DB 双写 owner。

### Phase 6：退出 Rust 控制面

按第 9 节 owner cutover 执行。每完成一项就删除对应 Python/Rust bridge，不等待一次性大爆炸。

### Phase 7：数据平台简化

**目标**：保留历史可靠性，删除自动保留状态机。

保留：

- hourly JSONL landing；
- DuckDB normalization；
- ZSTD Parquet；
- manifest 中的 source hash/row count/time range。

简化：

- compaction task 由 Huey 执行；
- manifest 进入 operational DB；
- raw deletion 默认禁用；
- 需要清理时使用“verified + age”单一规则或 systemd-tmpfiles；
- 删除 raw-delete multi-phase machine、quarantine evidence 和重复 audit file。

### Phase 8：策略能力继续演进

只有基础设施 owner 收敛后，再按统一策略接口引入 Vertical Late-Chase、Butterfly、Pin 和 Idea Engine。不得为每种策略重新建立服务、队列、DB 和通知链。

---

## 13. 删除清单

以下是目标删除项，不是长期保留的“legacy support”：

### Runtime

- `spx-spark-service-loop`
- 自研 scheduler / subprocess runner
- task-specific stdout JSON parsers
- 内部功能专属 console scripts
- 绝大多数 feature-specific systemd services/timers

### HTTP

- Schwab BaseHTTPRequestHandler 实现
- surface replay custom HTTP/Unix server
- live surface custom HTTP server

### Config

- env helper family
- custom dotenv loader
- YAML deep merge/provenance loader
- 巨型 runtime YAML 的 legacy/experimental 部分
- runtime_value allowlist machinery

### Notification

- custom delivery outbox claims/retry implementation
- missed queue / receipt mirror
- Rust ingress/delivery/report ownership
- duplicate delivery receipts

### Architecture

- AST module registry test
- manual top-level LAYERS dictionary
- facade size tests用于维持临时结构的部分

### Cross-language

- active Rust control-plane crates
- Python/Rust golden contract fixtures
- dual report/notification owner fences

### Data platform

- raw deletion multi-phase state machine
- duplicate JSON audit/mirror state
- runtime schema ALTER logic

---

## 14. 明确保留清单

- `ib-async`、`schwab-py`、DuckDB、NumPy；
- provider entitlement/freshness；
- IBKR line-budget 和 exact-leg planner；
- raw history与Parquet；
- SPX/ES、options、Greeks、Gamma、wall、pin 领域算法；
- manual-only / read-only / automatic_ordering=false；
- atomic snapshot helper 的受限使用；
- deterministic replay 与 no-lookahead；
- systemd 作为进程 supervisor；
- fail-closed 原则。

---

## 15. GPT-5.6 Sol 工作合同

以下内容应追加到 `AGENTS.md`，并视为高于普通“多写测试、全量验证”的默认偏好。

```markdown
## Architecture Simplification Contract

### Before coding

For every non-trivial task, first output a Change Brief:

- User-visible goal
- Existing owner and files
- Existing dependency that can solve it
- New dependency, if any, and why stdlib/existing packages are insufficient
- Old files/services/config/tests that will be deleted
- Persistence, process and notification impact
- Minimal end-to-end acceptance path

Do not edit code until this brief is accepted when the user requested design-first work.

### Implementation rules

1. Prefer extending the existing owner. Do not create a new module unless no current module owns the responsibility.
2. A new abstraction needs at least two real callers in the current change. Hypothetical future callers do not count.
3. Do not create a service, timer, database, queue, state machine, schema version or Rust contract without explicit user approval.
4. Use the blessed dependency for generic infrastructure. Do not hand-write another config loader, HTTP server, scheduler, retry loop, logger, database migrator, CLI parser or architecture linter.
5. Do not add a second package for a responsibility already covered by the blessed stack.
6. Build the smallest working vertical slice first. Tests and refactoring follow after the user-visible path works.
7. Delete the replaced implementation in the same phase. Compatibility wrappers require an explicit deletion issue and may live for at most one release cycle.
8. Do not use feature flags to retain rejected architecture indefinitely.
9. Do not turn an internal Python call into a CLI subprocess or JSON-file IPC.
10. Do not use LLM frameworks for a single structured model call. Use the provider client and typed output directly.

### Test rules

1. Test public behavior, money/time/data invariants and external boundaries.
2. Do not test helper call order, private dictionaries, exact prose or transient weights.
3. Prefer Hypothesis for mathematical invariants and timestamps.
4. Use a small number of frozen replay sessions for strategy semantics.
5. Run tests proportional to the change. Full Python + Rust validation is a release/cutover gate, not the default for an unrelated small edit.
6. A test is not a reason to keep an unnecessary abstraction.

### Complexity budget

Every final report must show:

- production files added/deleted
- net production LOC
- dependencies added/removed
- config keys added/removed
- services/timers added/removed
- databases/tables added/removed
- legacy paths removed

A change that increases process count, active languages, mutable stores or ownership paths requires explicit user approval.
```

### 15.1 Change Brief 模板

```text
Change Brief

Goal
- 用户最终能看到/完成什么？

Current owner
- 当前哪个模块和服务负责？

Reuse
- 将直接复用哪些类、函数、表、任务和第三方包？

Replace/Delete
- 这次会删除什么旧实现？

Dependency decision
- 是否使用 blessed dependency？
- 若新增包，它替代多少手写代码/服务？

Boundaries
- 进程：
- DB：
- 文件：
- 通知：
- 自动交易权限：

Minimal path
- 输入 → 核心行为 → 输出

Tests
- 只列不变量与端到端验证

Non-goals
- 明确本次不做什么
```

### 15.2 完成报告模板

```text
Delivered
- 用户需求的端到端结果

Deleted
- 旧代码/配置/服务/测试

Validation
- 相关测试与实际运行检查

Complexity delta
- files: +N / -N
- production LOC: +N / -N
- dependencies: +N / -N
- systemd units: +N / -N
- config keys: +N / -N
- DB tables/files: +N / -N

Remaining risks
- 只列真实、可验证的风险
```

---

## 16. 复杂度预算与验收指标

### 16.1 架构目标

| 指标 | 当前 | 目标 |
|---|---:|---:|
| Python console scripts | 43 | 1 个 `spx` 命令树 |
| 应用常驻 service | 大量 feature-specific units | 4 |
| 活跃应用语言 | Python + Rust | Python |
| HTTP framework | 多套手写 server | FastAPI/Uvicorn 一套 |
| scheduler/job 系统 | systemd + service loop + Rust schedule + custom retries | systemd supervisor + Huey |
| notification owner | Python + Rust + mirrors | Worker 单 owner |
| operational DB owner | 多 SQLite/JSON/Rust ledger | `spx.sqlite` |
| queue store | custom outbox/missed queue | `huey.sqlite` |
| 内部 IPC | 大量 latest JSON | direct calls + 2 provider boundaries |
| 架构检查 | custom AST registry | Import Linter |
| 配置 | ~108 KB runtime YAML + env helpers | 精简 typed settings |

### 16.2 PR 预算

默认单次重构 PR：

- 新生产依赖 ≤ 1；
- 新生产文件 ≤ 3；
- 新 systemd unit = 0；
- 新 DB file = 0；
- 新 schema owner = 0；
- 新通用 abstraction = 0；
- 生产代码净增加不超过 200 行，除非同一 PR 明确删除后续 phase 的旧实现；
- tests 不得明显多于被保护的生产行为。

迁移基础 PR 可以超出净行数限制，但必须列出紧随其后的删除提交，不能只搭新骨架。

### 16.3 架构完成标准

重构不是“新路径跑通”就完成。必须同时满足：

- 旧路径删除；
- owner 唯一；
- 无双写；
- 无长期 compatibility mode；
- service 数减少；
- 操作手册缩短；
- Agent 能从一个 composition root 追到业务输出；
- 用户需求不需要跨 Python/Rust/JSON/outbox 多层追踪。

---

## 17. 依赖白名单

### 核心运行依赖

```text
pydantic
pydantic-settings
fastapi
uvicorn
httpx
typer
huey
sqlalchemy
alembic
structlog
tenacity
exchange-calendars
hypothesis (dev)
```

继续保留：

```text
duckdb
ib-async
schwab-py
numpy
```

研究可选：

```text
scipy
cvxpy
```

任何新依赖必须解释为何上述栈不能解决，并说明它将删除的现有实现。

---

## 18. 关键 ADR 决策

应把以下决策分别记录为简短 ADR，避免 Agent 每次重新讨论：

1. `ADR: Python modular monolith as primary runtime`
2. `ADR: Four-service single-host topology`
3. `ADR: Pydantic settings and TOML configuration`
4. `ADR: FastAPI/Uvicorn for all local HTTP`
5. `ADR: Huey for durable background jobs`
6. `ADR: SQLAlchemy Core and Alembic for operational SQLite`
7. `ADR: Parquet/DuckDB for historical research`
8. `ADR: Rust control-plane retirement`
9. `ADR: JSON as export, not internal bus`
10. `ADR: Import Linter replaces custom architecture registry`
11. `ADR: LLM as bounded analyst, not orchestrator`
12. `ADR: No new service per strategy or feature`

ADR 每份控制在一页，写明：背景、决定、替代方案、后果、删除项。

---

## 19. 风险与取舍

### 19.1 Python 单体故障域增大

对策：

- provider collectors 仍独立；
- Core 使用 structured concurrency；
- systemd restart；
- 热路径和 Worker 分离；
- operational state 落 SQLite；
- provider snapshot 可恢复。

这比当前“每项功能独立进程，但通过文件和 schema 紧耦合”更容易理解和恢复。

### 19.2 移除 Rust 可能降低某些状态机的形式严格度

对策：

- 保留领域不变量测试；
- Pydantic/SQL constraints；
- Hypothesis；
- idempotency key；
- failure injection；
- 单 owner。

形式严格度不能只看语言；跨语言重复实现本身也是正确性风险。

### 19.3 第三方依赖带来升级风险

对策：

- `uv.lock` 固定版本；
- 依赖白名单；
- 每个职责只选一个包；
- release 时升级，不追逐最新版本；
- package adapter 只在 composition root，避免业务代码被框架侵入。

### 19.4 大规模重构可能影响交易时段

对策：

- 按 owner cutover，而不是一次性重写；
- 先 replay 和故障注入；
- 每个 cutover 有明确旧路径回滚点；
- 不在 RTH 中做部署迁移；
- 不长期双跑。

---

## 20. 最终结论

SPX Spark 的核心资产不是自研 scheduler、HTTP server、配置 loader、SQLite outbox 或跨语言 contract。核心资产是：

- 对 SPX/SPXW 数据真实性的严格理解；
- Broker 与交易时段的特殊约束；
- 0DTE 结构、Gamma、IV、Pin 与执行经济性；
- 因果 replay；
- 人工决策安全边界。

今后的架构必须围绕这些资产收敛：

```text
成熟包负责通用工程
项目代码负责交易领域
一个 owner 负责一个事实
一个进程内直接调用
迁移完成就删除旧路径
测试保护风险，不保护复杂度
```

对 GPT-5.6 Sol 最重要的约束不是“写得更严谨”，而是：

> **先理解最终用户要得到什么；先找仓库里和成熟生态中已经存在的解决方案；只实现最短端到端路径；用删除证明重构完成。**
