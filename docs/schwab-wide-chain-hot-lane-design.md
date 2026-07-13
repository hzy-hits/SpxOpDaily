# Schwab 宽链与 Option Hot Lane 设计

状态：核心实现与非 RTH smoke acceptance 完成；等待五个 RTH session
日期：2026-07-12  
范围：Schwab Market Data REST、SPX/SPXW 0DTE analytics、IBKR validation  
非目标：交易下单、L2/order book、用满配额、在无证据时切换人类告警

## 1. 决策摘要

采用“双层采集 + 双源验证”：

1. Schwab `/chains` 负责合约发现、宽 strike coverage、OI/Greeks 和低频结构刷新。
2. Schwab `/quotes` 负责最多 500 symbols 的 SPXW hot option + context 合并刷新。
3. Schwab front expiry 与 next expiry 分开调度，不再用同一 cadence 更新。
4. SPX `strikeCount` 默认从 80 起，根据实际 coverage 在 80/100/120 中选择最小充分值。
5. IBKR 保留连续 tick、source timestamp、ATM/hot pairs 和外翼轮换验证，不复制整链。
6. pricing 与 structure 字段分组投影，禁止 hot quote 整行覆盖 chain 的 OI/Greeks。
7. 所有 HTTP attempt（包括 retry/429）由 gateway 统一计量；collector 不再用成功数近似真实消耗。

本设计的目标是提高“进入 analytics 的有效信息量”，不是机械请求 500 symbols 或跑满
120 requests/minute。

## 2. 改造前事实与问题

当前 deployment policy：

- gateway nominal ceiling：120 HTTP attempts/minute；
- `/quotes`：单 request 最多 500 symbols；
- `$SPX` chain：每 5 秒，`strikeCount=40`；
- chain 查询范围同时包含 current 与 next research expiry；
- 正常理论负载约 40 requests/minute；
- Schwab WebSocket 当前为 off，因此本设计只依赖已验证的 REST endpoints。

现有问题：

1. `strikeCount=40` 对 density tails、快速行情 recenter 和 wall 外侧覆盖偏窄。
2. current/next expiry 共用一次请求和 cadence，无法把新鲜度预算集中到 0DTE。
3. `/quotes` 的 500-symbol batch 主要用于 underlying/context，没有充分用于已发现的
   SPXW option contracts。
4. collector 只在成功返回后增加 `request_count`，retry 和失败 attempt 不进入其窗口统计。
5. 如果 chain 与 quote snapshots 按整条 `Quote` 后写覆盖，较新的 price update 可能丢失
   chain 才有的 OI、IV 或 Greeks。
6. 当前配额是静态配置，没有把市场阶段、波动、429 和 payload latency 纳入状态机。

## 3. 目标数据流

```text
                         +-----------------------+
Schwab /chains --------->| ChainDiscoveryStore   |
  front/next separately  | structure fields      |
                         +-----------+-----------+
                                     |
                                     v
                         HotLaneSelector + RecenterPolicy
                                     |
context symbols ---------------------+----> <=500 symbol batch
                                          Schwab /quotes
                                                |
                                                v
                         +-----------------------+------------------+
                         | OptionObservationAssembler               |
                         | pricing clock + structure clock          |
                         +-----------------------+------------------+
                                                 |
                                  normalized MarketSnapshot
                                                 |
                          ChainQuality -> OptionsAnalyticsKernel
                                                 |
                                         analytics shadow

IBKR stream -> anchors + selected hot pairs + rotating outer pairs
            -> provider differential / failover readiness
```

## 4. 模块边界

计划新增或调整：

```text
src/spx_spark/settings/schwab.py
  SchwabCapacitySettings
  SchwabCadenceSettings
  SchwabWideChainSettings
  SchwabHotLaneSettings

src/spx_spark/schwab/request_models.py       # request/observation value objects
src/spx_spark/schwab/quota_machine.py        # pure quota transitions
src/spx_spark/schwab/market_data_plan.py     # pure due-request planner
src/spx_spark/schwab/chain_discovery.py      # front/next request construction
src/spx_spark/schwab/hot_lane.py             # option selector + hysteresis
src/spx_spark/schwab/observation_assembler.py# pricing/structure field merge
src/spx_spark/schwab/collector.py            # orchestration only
src/spx_spark/schwab/gateway.py              # actual-attempt telemetry/rate limit

src/spx_spark/ibkr/stream/quota_plan.py       # pure line allocator
```

依赖规则：

- planner、selector、quota transition 不发 HTTP、不读 env、不写文件；
- gateway 是唯一 Schwab HTTP attempt owner；
- collector 只执行 plan、归一化和持久化；
- analytics 不知道 Schwab endpoint 或 provider symbol；
- IBKR 与 Schwab 包不得互相 import，共同比较在 application 层完成。

## 5. Typed settings

```python
@dataclass(frozen=True)
class SchwabCapacitySettings:
    nominal_requests_per_minute: int = 120
    planned_requests_per_minute: int = 84
    max_symbols_per_quote_request: int = 500

@dataclass(frozen=True)
class SchwabCadenceSettings:
    off_hours_quote_seconds: float = 15.0
    off_hours_front_chain_seconds: float = 60.0
    off_hours_next_chain_seconds: float = 300.0
    off_hours_confirmation_chain_seconds: float = 300.0
    normal_front_chain_seconds: float = 3.0
    active_front_chain_seconds: float = 2.5
    burst_front_chain_seconds: float = 2.0
    next_chain_seconds: float = 30.0
    normal_quote_seconds: float = 2.0
    active_quote_seconds: float = 1.5
    burst_quote_seconds: float = 1.5
    spy_xsp_chain_seconds: float = 15.0
    qqq_iwm_chain_seconds: float = 30.0

@dataclass(frozen=True)
class SchwabWideChainSettings:
    strike_count_candidates: tuple[int, ...] = (80, 100, 120)
    expected_move_multiple: float = 2.5
    min_width_points: float = 150.0
    min_usable_strikes: int = 40
    min_two_sided_ratio: float = 0.80
    max_gap_multiple: float = 2.0

@dataclass(frozen=True)
class SchwabHotLaneSettings:
    minimum_dynamic_symbol_reserve: int = 10
    max_plan_age_seconds: float = 30.0
    recenter_drift_points: float = 10.0
```

option symbol budget 每次按
`500 - len(direct_context_symbols) - minimum_dynamic_symbol_reserve` 动态计算，充分使用
剩余 batch 容量，同时确保最终 batch 永远不超过 500。所有配置由 composition root
注入，不在 planner 内调用 `runtime_value()`。

## 6. 请求与配额契约

```python
class SchwabLane(str, Enum):
    HOT_AND_CONTEXT_QUOTES = "hot_and_context_quotes"
    FRONT_CHAIN = "front_chain"
    NEXT_CHAIN = "next_chain"
    SPY_XSP_CHAIN = "spy_xsp_chain"
    QQQ_IWM_CHAIN = "qqq_iwm_chain"
    RECOVERY_PROBE = "recovery_probe"

class QuotaMode(str, Enum):
    NORMAL = "normal"
    PRESSURE = "pressure"
    THROTTLED = "throttled"
    COOLDOWN = "cooldown"
    RECOVERING = "recovering"

@dataclass(frozen=True)
class SchwabRequestSpec:
    request_id: str
    lane: SchwabLane
    path: str
    params: tuple[tuple[str, str], ...]
    symbol_count: int
    priority: int
    due_at: datetime
    deadline_at: datetime

@dataclass(frozen=True)
class SchwabRequestObservation:
    path: str
    attempted_at_epoch: float
    completed_at_epoch: float
    retry_index: int
    status_code: int | None
    response_bytes: int
    latency_ms: float
    retry_after_seconds: float | None
    outcome: str
```

每一次 `session.get()` 都产生一条 observation，网络异常也必须留下记录。rolling usage
以 attempt observation 计数，而不是以成功 snapshot 计数。

### 6.1 状态机

| From | 条件 | To | 行为 |
| --- | --- | --- | --- |
| NORMAL | planned usage >= 70% nominal | PRESSURE | 停 QQQ/IWM 临时刷新，保持 front/hot |
| NORMAL/PRESSURE | 429 或明确 pacing response | THROTTLED | 停新 grant，服从 Retry-After |
| PRESSURE | usage < 50% 且无 429 60s | NORMAL | 恢复正常 cadence |
| THROTTLED | Retry-After 到期 | COOLDOWN | 只开放 front、hot/context、recovery |
| COOLDOWN | 连续 10 成功且无 429 | RECOVERING | 每 30s 恢复一条低优先 lane |
| RECOVERING | 连续稳定 5min | NORMAL | 恢复完整 plan |
| RECOVERING | 任意 429 | THROTTLED | 重新冷却 |

priority 固定为：front chain > hot/context quotes > next chain > SPY/XSP > QQQ/IWM。

## 7. 分到期 chain 设计

当前 `fetch_chain()` 把 current/next expiry 放在同一 `fromDate/toDate`。改为显式请求：

```python
def build_chain_request(
    *,
    underlier: str,
    expiry: date,
    strike_count: int,
    lane: SchwabLane,
) -> SchwabRequestSpec:
    # fromDate == toDate == expiry
    ...
```

调度：

| Chain | NORMAL | OPEN/CLOSE/VOLATILE | 初始 strikeCount |
| --- | ---: | ---: | ---: |
| SPXW front | 3s | 2–2.5s | adaptive 80/100/120 |
| SPXW next | 30s | 30s | 40–60 |
| SPY/XSP | 15s | 15s | 20–40 |
| QQQ/IWM | 30s | 30s | 20–40 |

### 7.1 自适应宽度

每个完整 front response 后计算：

```text
required_width = max(2.5 * expected_move_points, 150.0)
actual_lower_width
actual_upper_width
usable_strikes
two_sided_ratio
max_gap_multiple
```

选择算法：

1. 首次使用 80。
2. 任一双侧宽度不足、usable `<40` 或 recenter 后仍偏向一侧，则下一次尝试 100。
3. 100 仍不足且 response latency/payload 未超门槛，尝试 120。
4. 充分时保持当前 candidate，避免 80/100 抖动；当前实现只自动扩宽，不自动缩窄。
5. 120 仍不满足时保持上限并由 coverage 诊断表达降级，不继续无界扩大。

建议保护门槛初值：单 response `<8 MiB`、HTTP p95 `<2s`、normalize p95 `<150ms`。
这些是 shadow 初值，必须由 RTH 数据校准。

## 8. Option hot lane 选择

输入是最近一次有效 front chain、spot、expected move、walls、density boundaries 和
上一次 plan。输出 concrete Schwab option symbols。

```python
@dataclass(frozen=True)
class HotLanePlan:
    plan_id: str
    source_chain_snapshot_id: str
    expiry: str
    reference_spot: float
    symbols: tuple[str, ...]
    pair_keys: tuple[str, ...]
    reasons: Mapping[str, tuple[str, ...]]
    created_at: datetime
    expires_at: datetime
```

选择顺序：

1. ATM 最近 32 个完整 C/P pairs。
2. expected-move 上下边界各 ±2 pairs。
3. put/call wall 各 ±2 pairs。
4. density p10/p90 各 ±2 pairs；density 未 READY 时不使用该来源。
5. 最大 gap 两侧的 pairs，用于判断缺口来自行情还是 selection。
6. 剩余额度按 `abs(strike-spot)` 扩展，直到 `max_option_symbols`。

规则：

- C/P pair 是原子单位，绝不只加入一腿；
- symbol 必须来自最近有效 chain，不手工拼 provider OCC symbol；
- crossed、无 bid/ask 或 stale contract 可进入诊断，但不进入 usable analytics；
- reasons 用于解释每个 symbol 为什么占用预算；
- 去重后与 direct/context symbols 合并成一个 `/quotes` batch。

### 8.1 Hysteresis/recenter

仅在以下事件重建：

- expiry rollover；
- spot 相对 plan reference 漂移 `>=10` points；
- call/put wall 漂移 `>=10` points；
- plan age `>=30s`；
- selected symbols missing ratio `>10%`；
- chain width candidate 发生变化。

普通 tick 不重排 symbol 列表，避免 plan churn 和不可比较的 shadow coverage。

## 9. Pricing/structure 分组投影

不能直接用最新 `/quotes` 结果整行覆盖 `/chains` 结果。定义：

```python
@dataclass(frozen=True)
class OptionPricingFields:
    bid: float | None
    ask: float | None
    last: float | None
    mark: float | None
    bid_size: float | None
    ask_size: float | None
    observed_at: datetime

@dataclass(frozen=True)
class OptionStructureFields:
    open_interest: float | None
    volume: float | None
    implied_volatility: float | None
    delta: float | None
    gamma: float | None
    theta: float | None
    vega: float | None
    observed_at: datetime

@dataclass(frozen=True)
class AssembledOptionObservation:
    instrument_id: str
    provider: Provider
    pricing: OptionPricingFields
    structure: OptionStructureFields | None
```

合并规则：

- pricing 只按 pricing clock 取最新；
- structure 只按 structure clock 取最新非空组；
- 新 pricing 不得把旧 structure 置空；
- structure 超过 policy TTL 后保留值但标 stale，不可假装与 pricing 同时；
- analytics diagnostics 分别输出 pricing_age 和 structure_age；
- OI 的 session 语义独立标注，不能按 5 秒 freshness 解读。

如果暂时不能修改公共 `Quote`，assembler 可以在 projection 层维护 sidecar structure
state，再构造 analytics input；不得退回整行覆盖。

## 10. 激进请求预算（固定保留 30%）

context 与 hot options 合并为同一个 quote batch：

| Lane | NORMAL | ACTIVE | BURST | Requests/min |
| --- | ---: | ---: | ---: | ---: |
| hot options + context | 2s | 1.5s | 1.5s | 30 / 40 / 40 |
| front SPXW chain | 3s | 2.5s | 2s | 20 / 24 / 30 |
| next SPXW chain | 30s | 30s | 30s | 2 |
| SPY + XSP chains | 15s | 15s | 15s | 8 |
| QQQ + IWM chains | 30s | 30s | 30s | 4 |
| **合计** | | | | **64 / 78 / 84** |

planned ceiling 固定为 84/min，即使用 nominal 120 的 70%，严格保留 36 attempts/min
（30%）供 retry、OAuth、operator probes 和短时调度碰撞。BURST 可以用满 planned
capacity，gateway 以 120/min 做最终串行限速。

profile 切换：

- NORMAL：普通 RTH；
- ACTIVE：开盘后 30 分钟、收盘前 30 分钟，或 realized-vol/spot drift 超阈值；
- BURST：shock、快速 recenter、chain freshness 恶化时，最长持续 60 秒；
- BURST 每 15 秒重新评估，条件消失立即降回 ACTIVE/NORMAL；
- quota mode 进入 PRESSURE 时禁止 BURST，THROTTLED 时只保留 P0/P1 lanes。

如果 hot+context 超过 500：

1. 保留 anchors/context P0；
2. 保留所有明确 reason 的 hot pairs；
3. 删除最远、无双边、无结构用途的扩展 pairs；
4. 仍超限才拆第二 batch，并由 quota planner 判断是否可发；
5. 不允许静默截断。

## 11. IBKR allocation

IBKR nominal 100 lines 使用两个 mode：

| Lane | Schwab healthy | Schwab fallback |
| --- | ---: | ---: |
| SPX/ES/SPY/VIX1D anchors | 4–8 | 4–8 |
| ATM/hot SPXW C/P | 44 | 54 |
| outer rotation SPXW C/P | 20 | 24 |
| temporary context | 4 | 4 |
| safety reserve | 24–28 | 10–14 |

allocator 从 Schwab hot plan 的 pair keys 选择 IBKR 可映射合约，但比较逻辑位于
application 层。规则：

- Schwab healthy：IBKR 验证 ATM、walls 和 boundaries，outer pairs 轮换；
- Schwab DEGRADED/BLOCKED：扩大 persistent hot pairs；
- C/P pair 原子分配；
- reserve 不足时先取消 temporary/context，再取消最远 outer rotation；
- replan overlap 只有 reserve 足够时 make-before-break；
- IBKR failure 不降低 Schwab chain quality threshold。

## 12. Analytics 使用规则

更多数据进入系统后仍必须分层使用：

- density：只使用 front expiry、双边 mid、质量门槛内的 strike grid；
- Greeks/GEX：允许使用 structure fields，但必须输出 structure age/OI session；
- wall：优先 OI-weighted structure，不用快速 quote volume 让墙追着价格跑；
- skew：使用可比较 delta/moneyness bucket，不把 symbol 数量变化当 skew 变化；
- next expiry：用于 term structure/稳定性比较，不混入 0DTE density；
- Schwab/IBKR differential：用于质量和 failover，不把两源价格简单平均。

## 13. Telemetry

每个 request observation 和每个 analytics shadow record至少产生：

```text
attempts_60s, successes_60s, retries_60s, status_429_60s,
lane, symbol_count, response_bytes, http_latency_ms, normalize_duration_ms,
requested_strike_count, returned_legs, distinct_strikes, usable_strikes,
two_sided_ratio, lower_width, upper_width, max_gap_multiple,
hot_plan_symbol_count, hot_missing_ratio, pricing_age_p95,
structure_age_p95, quota_mode, planned_requests_60s
```

必须能回答：增加 40→60→80 后，多得到多少 usable strikes，付出了多少 bytes、latency
和 CPU，而不是只知道返回 JSON 更大。

## 14. 测试设计

新增：

```text
tests/schwab/test_market_data_plan.py
tests/schwab/test_quota_machine.py
tests/schwab/test_chain_discovery.py
tests/schwab/test_hot_lane.py
tests/schwab/test_observation_assembler.py
tests/schwab/test_request_telemetry.py
tests/application/test_provider_differential.py
tests/e2e/test_schwab_wide_chain_replay.py
```

必须覆盖：

1. front/next 生成独立同日 `fromDate=toDate` 请求。
2. 80 不满足 coverage 后升 100，稳定满足后不继续升 120。
3. 100/80 threshold 附近不会每 tick 抖动。
4. hot selector 保证 C/P pair 原子性、去重、reason 和 500 ceiling。
5. density 未 READY 时不选择 p10/p90 reasons。
6. 新 quote price 不清空 chain OI/Greeks。
7. structure stale 与 pricing fresh 可同时表达。
8. 网络异常和每个 retry 都计入 attempt window。
9. 429 进入 THROTTLED 并服从 Retry-After。
10. NORMAL/ACTIVE/BURST 预算分别不超过 64/78/84 nominal plan。
11. 超 500 时按 priority 缩减或显式拆 batch，不静默截断。
12. Schwab degraded 时 IBKR allocator 扩容但保留最小 reserve。
13. replay 相同输入产生确定性 plan 和 analytics 输出。

## 15. 分阶段上线

### Stage A：真实 attempt telemetry

先修 gateway 计量，保持采集行为不变。至少运行一次离线 retry/429 模拟。

### Stage B：宽链 shadow

RTH 顺序测试 80、100、120；保存 coverage、bytes、latency 和 normalize time。不改变
canonical analytics input。

### Stage C：front/next 分离

front 5s、next 30s。验证 expiry rollover、提前收市和 request budget。

### Stage D：hot quote lane shadow

生成 concrete option batch 并写独立 shadow projection；比较 chain pricing 与 hot pricing
freshness，不进入 human alerts。

### Stage E：field-group assembler

analytics shadow 改用 assembled observation，执行 legacy/new differential。确认 OI/Greeks
没有因快速报价刷新丢失。

### Stage F：canonical analytics + IBKR adaptive validation

五个 RTH session 达标后切换 analytics input；human notification 另行经过 outbox shadow
验收，不与数据切换同日进行。

## 16. 验收标准

代码级：

- 所有 planner/selector/state machine 为纯函数；
- collector 不再包含 cadence/priority 大型 if-chain；
- gateway attempt telemetry 覆盖 retry/error；
- `/quotes` batch 永不超过 500；
- analytics/provider 分层测试通过；
- 全量 pytest、Ruff、build 通过。

五个完整 RTH session：

- front expiry bucket coverage `>=95%`；
- density READY `>=95%`；
- usable strikes p05 `>=40`；
- two-sided ratio p05 `>=0.80`；
- 双侧 acquisition coverage p05 `>=max(2.5*EM, 150 points)`；density 发布底线仍按
  `max(2*EM, 100 points)`；
- hot pricing age p95 `<=5s`，volatile profile 目标 `<=3s`；
- max gap p95 `<=2*median step`；
- gateway 429 为 0；如发生必须证明状态机正确降速且无数据链路失控；
- attempt p99 不超过 84/min planned ceiling；
- HTTP p95 `<2s`、normalize p95 `<150ms`，或有实测后修订阈值；
- shadow human notification 为 0；
- IBKR active lines 不超过 discovered capacity，reserve 不低于 mode 下限。

## 17. 为什么保留保护边界

限制不是拒绝扩大数据，而是保证扩大的数据可被证明有用：

- 500 symbols 是 transport ceiling，不是信息质量指标；
- 超远无双边 strike 会放大 density 二阶差分噪声；
- retry 也消耗 provider capacity；
- hot quote 与 chain 字段语义不同，错误覆盖会让“更多数据”变成字段丢失；
- IBKR lines 与 Schwab HTTP requests 是不同配额，不能相加；
- 没有 telemetry 时把 cadence 调到极限，只会在首个波动时段暴露 429 和 stale chain。

本设计将 NORMAL/ACTIVE/BURST 计划提高到约 64/78/84 attempts/min，BURST 用满 84 的
70% planned ceiling，同时把 SPXW acquisition target 提高到 80/100/120 strikes 和
2.5 expected moves。固定剩余 30% 是 retry/failure capacity，不再额外保留隐性冗余。
OFF_HOURS 单独降到约 10 attempts/min，并由最短 cadence 推导 5 秒 planner tick；静态期权
数据不消耗 RTH 的激进预算。

## 18. 2026-07-12 实施证据

已落地：

- dedicated `spx-spark-schwab-marketdata.service`，24h loop 内嵌 collector 已关闭；
- gateway rolling attempt/retry/429/bytes telemetry；
- quota mode 驱动 lane admission；
- front/next 单 expiry 请求；
- adaptive 80/100/120 strike planning；
- concrete front SPXW option hot quote batch；
- pricing/structure 分组 merge；
- IBKR validation/fallback 64/78 option-line allocation。

周日非 RTH smoke evidence：

```text
SPX front requested strikes: 80
distinct/usable strikes:     80 / 80
two-sided ratio:             1.00
lower/upper width:           195.39 / 204.61 points
median step / max gap:       5 / 10 points
hot option symbols:          160
hot+context response rows:   198
gateway rolling attempts:    31/min
retry / 429:                 0 / 0
Schwab option rows:          480
rows retaining OI/Greeks:    480 / 480
```

这是 connectivity、字段合并和预算 smoke evidence，不是 RTH density acceptance。RTH
仍需验证 source age、spread、expected-move acquisition coverage、NORMAL/ACTIVE/BURST
cadence 和五日 density READY 比例。
