# SPX Spark 首个 RTH 前重构实施计划

状态：实施中  
制定日期：2026-07-12（Asia/Shanghai）  
目标窗口：首个周一 RTH 行情进入系统前完成代码、离线测试、部署和 shadow
采集准备；五个完整 RTH session 之后才能完成数据质量验收。

本文是本轮执行顺序、模块边界和验收命令的权威来源。长期目标与完整验收规格见
`docs/refactor-architecture-acceptance-plan.md`，稳定分层规则见
`module-architecture.md`。

Schwab 宽链、500-symbol option hot lane、分到期 cadence、field-group merge 与 IBKR
line allocation 的详细设计见 `docs/schwab-wide-chain-hot-lane-design.md`。

## 1. 决策与范围

### 1.1 周一前必须完成

1. 生产 `RealtimeEngine` 使用真实 options analytics kernel，不再使用
   `PassthroughAnalytics` 作为默认实现。
2. 从 `MarketSnapshot` 构造 front-expiry `OptionChainSnapshot`，并计算完整
   `ChainCoverage`、`ChainIssue` 和 readiness。
3. density 发布前执行 strikes、双边率、宽度、gap、age/skew、单调性、凸性、
   clipped mass 和 normalized mass 门槛。
4. runtime 启动阶段 fail closed：critical task 未成功、realtime health 未产生、
   chain 不完整时不得 READY。
5. 建立 append-only analytics shadow recorder、日报和 new/legacy differential；
   shadow 路径不能触达人类通知。
6. 把上述 policy 接入 typed `AnalyticsSettings`，entrypoint 只解析一次配置。
7. 添加针对这些边界的 contract、architecture、application 和 analytics 测试。
8. 完成离线 fixture 回放、全量测试、Ruff、构建和 systemd restart dry run。

### 1.2 周一前不应强行完成

- 不在首个 RTH 前一次性迁移全部 456 个 `runtime_value()` 调用。
- 不在首个 RTH 前大规模移动 Steven、post-close、notifier 和 compactor 代码。
- 不引入 Rust、Unix socket、消息队列、Kubernetes 或新数据库。
- 不切换 human notification canonical path。
- 不把“shadow recorder 已部署”声明为“五个 RTH session 已验收”。

这些工作会扩大回归面，却不能提高首个 RTH 样本的可信度。周一前只允许修改
analytics、realtime composition、health、typed policy、shadow observability 及其直接测试。

### 1.3 工期判断

单人顺序实施的关键路径约 10–12 小时，属于紧窗口，没有同时完成全量配置迁移或
大文件拆分的余量。若距 `T0` 少于 8 小时，必须把真实 kernel 先作为 shadow 双跑，
不切换 canonical alert input；仍需完成 chain quality、fail-closed health、recorder 和
离线 replay。多人实施时也按 I1 契约冻结后再分工，禁止各自定义不同 schema。

## 2. 完成定义

### 2.1 PRE-RTH-CODE-READY

首个实时样本到达前必须同时满足：

- `build_realtime_runtime()` 默认注入 `OptionsAnalyticsKernel`；
- passthrough 只能由测试或显式 `analytics.mode=passthrough` 选择；
- 一个 option leg、陈旧链或未完成任务均不能产生 READY；
- density 只有 `READY` 才发布 percentile/probability；
- shadow 输出可由 fixture 回放生成，并能生成 session report；
- shadow recorder 不 import notifier/delivery 模块；
- 测试、Ruff、build 和 acceptance report 全绿；
- 服务配置可回滚到旧 kernel，但默认保持 shadow-only notification 行为。

### 2.2 RTH-SHADOW-ACCEPTED

这一定义不可能在行情进入前完成。必须收集连续五个完整 RTH session，并满足
本文件第 10 节和总验收规格 §14.3 后才能标记完成。

## 3. 目标数据流

```text
Schwab REST chain (recentered SPXW front-expiry window)
                \
                 -> LatestMarketProjection -> MarketSnapshot
IBKR anchors ----/                         |
                                             v
                               OptionChainSnapshotBuilder
                                             |
                               ChainQualityEvaluator
                                             |
                                   OptionsAnalyticsKernel
                                    /                 \
                          AnalyticsResult        ShadowRecord
                                |                     |
                         AlertEvaluator       append-only JSONL/Parquet
                                |                     |
                         SQLite outbox          SessionReport
```

约束：provider adapter 负责字段归一化；analytics 不读取环境、文件或网络；application
负责拼装；infrastructure 负责 shadow 持久化。IBKR 不重复订阅整条 SPXW chain。

## 4. 依赖顺序

```text
I0 baseline freeze
  -> I1 contracts + quality policy
      -> I2 real analytics kernel
          -> I3 readiness fail-closed
              -> I4 shadow recorder/report/differential
                  -> I5 integration, deployment and freeze
```

I1 至 I4 不允许并行修改同一契约。每个迭代先写 characterization/contract tests，
再实现，再运行本迭代 gate。

## 5. 具体迭代

### I0：冻结基线

交付：

- 保存当前全量测试、Ruff、build 输出和 git diff 摘要；
- 记录 `runtime_value()` AST 总数和各文件预算；
- 固定现有 options-map golden 与至少一份脱敏 synthetic SPXW fixture；
- 记录当前 passthrough heartbeat，作为修复前证据。

退出条件：基线 report 写入
`artifacts/refactor-acceptance/pre-rth-baseline/report.json`，fixture 不包含 token、
账户号或 `/srv` 路径。

### I1：chain contract 与质量门槛

计划文件：

```text
src/spx_spark/settings/analytics.py
src/spx_spark/domain/snapshots.py
src/spx_spark/analytics/options/snapshot.py       # 新建
src/spx_spark/analytics/options/quality.py
src/spx_spark/analytics/options/density.py
tests/contracts/test_option_chain_contract.py     # 新建
tests/analytics/test_chain_quality.py              # 新建
tests/analytics/test_risk_neutral_density.py       # 新建/迁移
```

必须提供的接口：

```python
@dataclass(frozen=True)
class ChainQualityPolicy:
    min_usable_strikes: int
    min_two_sided_ratio: float
    min_strikes_each_side: int
    min_width_expected_move_multiple: float
    min_width_points: float
    max_gap_multiple: float
    max_quote_age_seconds: float
    max_cross_row_skew_seconds: float
    max_monotonic_violation_fraction: float
    ready_max_clipped_mass_fraction: float
    degraded_max_clipped_mass_fraction: float
    normalized_mass_min: float
    normalized_mass_max: float

def build_option_chain_snapshot(
    snapshot: MarketSnapshot,
    *,
    underlier: str,
    expiry: str,
    spot: float | None,
    policy: ChainQualityPolicy,
) -> OptionChainSnapshot: ...

def evaluate_chain_quality(
    coverage: ChainCoverage,
    *,
    expected_move_points: float | None,
    policy: ChainQualityPolicy,
) -> tuple[ChainReadiness, tuple[ChainIssue, ...]]: ...
```

行为要求：

- coverage 统计 distinct/usable/two-sided strikes，而不是 legs；
- quote age 使用 source timestamp；缺少 timestamp 按 unknown/stale 处理；
- front expiry 必须由 market calendar 和实际可用 expiry 共同解析；
- issues 可多选并保持确定性顺序；
- BLOCKED 链不得发布 density probability；DEGRADED 可保留诊断但默认不供告警使用。

退出测试：覆盖空链、单 leg、单边、宽度不足、gap、stale、time skew、正常链和
提前收市日。

### I2：真实 analytics kernel

计划文件：

```text
src/spx_spark/application/realtime/analytics_kernel.py  # 新建
src/spx_spark/application/realtime/composition.py
src/spx_spark/domain/analytics.py
tests/application/test_realtime_composition.py
tests/analytics/test_analytics_differential.py           # 新建
```

接口：

```python
@dataclass(frozen=True)
class OptionsAnalyticsKernel:
    policy: AnalyticsPolicy

    def compute(
        self,
        snapshot: MarketSnapshot,
        *,
        now: datetime,
    ) -> AnalyticsResult: ...
```

kernel 必须：

1. 选择 live SPX anchor 和 SPXW front expiry；
2. 构造 `OptionChainSnapshot`；
3. 在纯 analytics 内计算 expected move、IV/skew、Greeks/GEX/walls 和 density；
4. 把 chain readiness、issues、density diagnostics、模型版本和耗时写入
   `AnalyticsResult`；
5. 不执行 alert、LLM、notification、文件或网络 I/O；
6. 对无 anchor、无 front expiry、blocked chain 返回结构化结果，而不是伪成功空结果；
7. 只有未预期异常才令 tick FAILED。

composition 规则：真实 kernel 是默认值；passthrough 需显式注入。不得用环境变量在
业务函数内部临时选择实现。

退出条件：同一 fixture 上 new/legacy 的 ATM、expected move、GEX、walls、density
percentiles 在既定容差内；差异必须有字段级报告，不能只比较 JSON 是否相等。

### I3：readiness fail closed

计划文件：

```text
src/spx_spark/application/realtime/engine.py
src/spx_spark/application/realtime/health.py
src/spx_spark/application/runtime/health.py
src/spx_spark/application/runtime/scheduler.py
src/spx_spark/domain/health.py
tests/application/test_realtime_engine.py
tests/test_runtime_health.py
tests/state_machines/test_engine_health_machine.py
```

状态要求：

```text
STARTING -> WARMING -> READY
                     -> DEGRADED
                     -> BLOCKED
any state -> FAILED
```

- STARTING：进程已启动但任务未调度；
- WARMING：critical tasks 尚未全部至少成功一次，或 realtime health 尚不存在；
- READY：anchor、front chain、real analytics、outbox、critical tasks 全部健康；
- DEGRADED：有结果但质量不足，且策略明确允许研究输出；
- BLOCKED：缺少交易所行情或 chain 不可发布；
- FAILED：代码、持久化或不可恢复任务故障。

删除“缺失 factor 默认 True”的行为。heartbeat 必须携带 `first_success_at`、
`last_success_at`、`last_engine_health` 和明确的 blocking reasons。

退出测试至少包括：刚启动、task in-flight、一个 option leg、stale chain、blocked
density、outbox readonly、analytics exception、全部健康八种场景。

### I4：shadow 与 differential

计划文件：

```text
src/spx_spark/domain/shadow.py                         # 新建，stdlib-only schema
src/spx_spark/application/realtime/shadow.py           # 新建，构造 record
src/spx_spark/infrastructure/analytics_shadow.py        # 新建，append-only writer
scripts/replay-analytics-shadow.py                      # 新建
scripts/report-analytics-shadow.py                      # 新建
tests/contracts/test_analytics_shadow_contract.py       # 新建
tests/infrastructure/test_analytics_shadow_writer.py    # 新建
tests/architecture/test_shadow_boundaries.py            # 新建
```

每条 `AnalyticsShadowRecord` 至少包含：

```text
schema_version, session_date, observed_at, snapshot_id, provider,
underlier, expiry, spot, total_legs, distinct_strikes, usable_strikes,
two_sided_ratio, lower_width_points, upper_width_points, median_step,
max_gap_multiple, max_quote_age_seconds, cross_row_skew_seconds,
chain_readiness, chain_issues, density_quality, clipped_mass_fraction,
monotonic_violation_fraction, negative_mass_fraction, normalized_mass,
analytics_duration_ms, legacy_result, new_result, differential,
schwab_request_budget, ibkr_active_lines, model_versions
```

持久化规则：

- runtime 先落 JSONL，按 `date=YYYY-MM-DD/hour=HH` 分区；
- 单行写入使用 lock + append + flush；进程崩溃最多损失当前未完成行；
- compaction 可异步转 Parquet，不阻塞 realtime tick；
- recorder 错误进入 telemetry，但不得令交易告警路径失败；
- record 不包含 token、账户、完整原始 provider payload；
- writer 不依赖 notifier、alert evaluator 或 outbox。

日报脚本必须输出样本/bucket 覆盖率、READY/DEGRADED/BLOCKED 比例、所有 issue
计数、质量指标 p05/p50/p95/p99、计算耗时和 differential。缺少完整 session 时退出非零。

### I5：集成、部署和冻结

执行顺序：

1. fixture replay 生成 shadow report；
2. 全量 pytest、Ruff、build；
3. `systemd-analyze verify` 检查 units；
4. notification 保持 shadow/no-human-delivery；
5. 重启服务并验证第一个 heartbeat 为 WARMING/BLOCKED，不得误报 READY；
6. 验证 append-only 路径可写、磁盘空间和 retention；
7. 生成 `pre-rth-code-ready/report.json`；
8. 冻结代码，首个 RTH 只允许修复阻断采集/记录的缺陷。

回滚单位是 kernel composition 和 shadow recorder 两个独立开关。readiness fail-closed
不得回滚成乐观 READY。

## 6. 周一前排期

以首个计划实时采样时间为 `T0`：

| 时间窗 | 工作 | 硬退出条件 |
| --- | --- | --- |
| T0-12h 至 T0-10h | I0 基线 | report/fixture/hash 完整 |
| T0-10h 至 T0-7h | I1 contract/quality | chain/density 测试全绿 |
| T0-7h 至 T0-5h | I2 kernel | composition 默认真实 kernel，differential 通过 |
| T0-5h 至 T0-3h | I3 health | 启动不得 READY，故障矩阵通过 |
| T0-3h 至 T0-1.5h | I4 shadow | replay/report/边界测试通过 |
| T0-1.5h 至 T0-0.5h | I5 集成部署 | 全量 gate、restart、磁盘检查通过 |
| T0-0.5h 至 T0 | 冻结和观察 | 不再做结构移动 |

如果任一硬退出条件未满足，不压缩下一阶段测试时间。降级顺序是：保留旧 alert
canonical path、部署 recorder、将真实 kernel 保持 shadow-only；绝不能伪造 READY。

## 7. 配置债务的本轮边界

本轮把 typed settings 从“无人使用的脚手架”变成 composition root 的真实输入，但不
承诺一次清零所有 legacy 调用。

周一前必须：

- 完整实现 `AnalyticsSettings`/`ChainQualityPolicy`/shadow settings；
- realtime entrypoint 调用 `load_settings()` 并显式注入；
- 删除 analytics/realtime 新代码中的所有 `runtime_value()`；
- 将 architecture guard 改为逐文件精确预算，任何文件调用数增加即失败；
- import-time 调用单列预算，并设为优先清零队列。

周一后按顺序迁移：shock、alert rules、provider policy、Steven/review、legacy config。
每个迭代只降低预算，不允许重置更高 baseline。

## 8. 大文件拆分队列

首个 RTH 期间不拆这些文件。五日 shadow 运行期间按不改变行为的顺序执行：

| 顺序 | 当前模块 | 目标包 | 第一刀 |
| --- | --- | --- | --- |
| 1 | `intraday_strategy.py` | `strategy/intraday/` | models/policy/transition/repository/service |
| 2 | `notifier/pipeline.py` | `notifier/` | eligibility/review/delivery/audit |
| 3 | `post_close_review.py` | `application/post_close/` | metrics/completeness/render/llm/service |
| 4 | `strategy/steven.py` | `strategy/steven/` | models/policy/trigger/transition/repository/service |
| 5 | `data_platform/lake/compact.py` | `data_platform/lake/compaction/` | discovery/validation/writer/manifest/retention |
| 6 | `greek_reference.py` | `analytics/greeks/reference/` | contracts/calculation/aggregation/reporting |
| 7 | `config.py` | `settings/` + compatibility facade | 先迁调用方，最后缩门面 |

每次拆分必须先 golden/characterization，再纯移动，再删除旧实现。业务函数软上限 80
行；超过必须在 review 中说明算法不可再分的原因。

## 9. Provider quota 策略

本节只给执行摘要；接口、状态机、预算公式、测试和 rollout 以
`docs/schwab-wide-chain-hot-lane-design.md` 为准。

- Schwab：负责 SPX anchor、front-expiry 自适应请求窗口（目标 80/100/120 strikes）
  和必要时 ATM recenter；计划请求最多使用 70% nominal capacity，固定保留 30%
  authentication/recovery reserve，不能为了提高采样频率耗尽 120-order operational
  ceiling。
- IBKR：永久订阅 SPX/ES/SPY 等 anchors；剩余额度分配 ATM hot lane 和轮换验证样本；
  不复制 Schwab 的整条链。
- 同一 canonical instrument 同时有两源时，shadow 保存两源质量差异；canonical
  projection 仍按 provider policy 选择，不在 analytics 内做 provider fallback。
- quota exhaustion 必须产生结构化状态和 telemetry，不能静默缩小 strike window。

## 10. 五个 RTH session 运行与验收

每日流程：

1. 开盘前验证服务、磁盘、Schwab token、IBKR session owner 和 notification shadow；
2. RTH 内持续 append，禁止手工改写当日分区；
3. 收盘后等待最后 buffer flush，再执行 session report；
4. report 不完整则保留原因并将该日标记 invalid，不计入连续五日；
5. 第五个有效 session 后生成 aggregate report 和 GO/NO-GO。

最终门槛：

- 每日预期时间 bucket 覆盖率 `>= 95%`；
- Schwab SPXW front-expiry density READY `>= 95%`；
- BLOCKED 样本 `100%` 有结构化原因；
- usable strikes `>= 21`、two-sided ratio `>= 0.80`；
- ATM 上下各至少 8 strikes；
- 双侧宽度各 `>= max(1.25 * expected_move, 50 points)`；
- max gap `<= 2 * median strike step`；
- max quote age `<= 15s`，cross-row skew p95 `<= 10s`；
- clipped mass READY `<= 0.15`，normalized mass 在 `[0.95, 1.05]`；
- engine tick p95 `<= 300ms`，deadline miss `< 0.1%`；
- shadow human notification 次数为 0；
- new/legacy 所有超容差差异均已解释或修复。

## 11. 强制验收命令

```bash
uv run pytest -q tests/contracts tests/analytics tests/application \
  tests/state_machines tests/infrastructure tests/architecture
uv run pytest -q
uv run ruff check src tests scripts
uv build
uv run python scripts/replay-analytics-shadow.py \
  --fixture tests/golden/spxw-rth-synthetic.json \
  --output artifacts/analytics-shadow/replay
uv run python scripts/report-analytics-shadow.py \
  --input artifacts/analytics-shadow/replay \
  --allow-synthetic-session
uv run python scripts/refactor-acceptance-report.py --phase pre-rth-code-ready --go
```

尚不存在的脚本或 fixture 是实施项，不得跳过后仍声明 PRE-RTH-CODE-READY。

## 12. Commit/Review 边界

建议保持以下独立变更单元：

1. `contracts: add chain quality policy and snapshot builder`
2. `analytics: enforce density publishability gates`
3. `realtime: wire production options analytics kernel`
4. `runtime: fail readiness closed during startup and degraded data`
5. `observability: add append-only analytics shadow and reports`
6. `acceptance: add pre-RTH replay and deployment evidence`

每个单元可独立 review 和回滚；禁止把大文件移动、格式化全仓或无关配置迁移混入。

## 13. 停止条件与风险

- fixture differential 无法解释时停止 canonical kernel 切换，保留 shadow 双跑。
- shadow 写入影响 tick p95 时移出 tick 同步路径，使用有界队列；队列满时丢 shadow
  并报警，绝不阻塞实时 engine。
- Schwab 链宽度不足时触发 recenter，不降低质量门槛来制造 READY。
- IBKR session 冲突时保持单 owner，不为补 shadow 数据启动第二 session。
- 五日结果不足只代表 NO-GO，不构成采用 Rust、IPC 或 K8s 的证据。
