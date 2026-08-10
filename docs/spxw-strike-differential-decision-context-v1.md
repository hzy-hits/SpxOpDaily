# SPXW 执行价差分特征与决策上下文接入设计 v1

状态：**已实现（SDCTX-1–5；SDCTX-desk-v1.1 已允许 Desk 解释与硬门后 soft rank）**
适用仓库：`hzy-hits/SpxOpDaily`  
适用分支基线：`master`  
设计边界：**不改变候选生成、硬门、人工权限或自动下单边界；仅允许 Desk 解释与硬门后封顶 soft rank**
上位合同：`docs/strategy-signal-engine-v2.md`、`docs/strategy-signal-engine-v3.md`、`module-architecture.md`

> 用户已确认的本期目标：把 Butterfly 对应的二阶执行价差分，以及三阶、四阶和多尺度派生特征，作为期权曲面事实加入统一决策上下文。模型如何解释、是否具备预测价值、是否进入候选排序，全部留到后续独立阶段。

---

## 0. 决策摘要

本设计新增一个版本化、只读的：

```text
market_facts.structure.strike_differential_context
```

其数据来自与 RN density 相同的 SPXW 同到期 OTM synthetic-call curve，计算：

```text
D2：Call 价格关于 strike 的二阶差分
D3：局部风险中性密度沿 strike 的斜率代理
D4：局部风险中性密度曲率代理
Multi-scale：在 5 / 10 / 15 / 20 点尺度上的同类测量
Noise bound / SNR：相应差分组合对 bid-ask 噪声的敏感度
```

该字段随 `MarketFactPack` 进入 `strategy_decision.v2.market_facts`。现有 `call_strategy_idea_memo()` 会序列化整个 decision，因此模型能够看到该上下文；本期不修改 prompt，不规定模型必须如何解释，也不允许这些特征影响交易授权。

本期明确不做：

- 不新增策略类型；
- 不直接交易三阶、四阶组合；
- 不改变 `manual_authority_eligible`；
- 不修改 utility 或 ManagementPolicy EV；`selection_score` 仅允许 v1.1 定义的硬门后 soft prior；
- 不新增 Transformer、分类器或训练任务；
- 不新增服务、timer、数据库、表、队列、Rust contract 或通知路径；
- 不增加通知 lane；现有 Desk View 可显示一行曲面形状摘要；
- 不把 D2/D3/D4 称为现实概率、Alpha、Pin 信号或做市商仓位。

---

## 1. Change Brief

### 1.1 User-visible goal

在每次统一策略决策中保留一份紧凑、可审计的 SPXW 执行价曲面局部形状上下文，使后续模型、研究回放和人类诊断能够看到：

- 某个结构位附近的局部二阶凸性；
- 局部概率质量是向高 strike 还是低 strike 增加；
- 中央局部峰相对两侧肩部的弯曲程度；
- 不同 strike 尺度是否给出一致或冲突的曲面形状；
- 这些高阶量是否小于其 bid-ask 噪声上界。

本期用户可见交易卡、候选数量和授权结果必须保持不变。

### 1.2 Existing owner and files

现有职责已经有明确 owner：

| 责任 | 当前 owner |
|---|---|
| OTM synthetic Call curve、RN density、非均匀二阶差分 | `src/spx_spark/analytics/options/density.py` |
| Options analytics value objects | `src/spx_spark/analytics/options/models.py` |
| 每到期日 options analytics 编排、walls / zero gamma / density | `src/spx_spark/analytics/options/service.py` |
| 决策时点 `MarketFactPack` | `src/spx_spark/application/order_map/strategy_facts.py` |
| 唯一 `strategy_decision` 出口 | `src/spx_spark/application/order_map/strategy_select.py` |
| GPT strategy memo，读取整个 decision | `src/spx_spark/notifier/llm_writer.py` |

因此本期不新建生产模块。差分数学继续由 `analytics/options/density.py` 持有。

### 1.3 Existing dependency

仅使用当前 stdlib、dataclass 和已有 options analytics。第一版不需要新增 NumPy、SciPy、cvxpy 或深度学习依赖。

### 1.4 New dependency

无。

### 1.5 Old files/services/config/tests deleted

无删除；无兼容层；无新 feature flag。

### 1.6 Persistence, process and notification impact

- 进程数：不变；
- mutable store：不变；
- 数据库/表：不变；
- systemd：不变；
- 配置键：不变；
- 通知文案：不变；
- 决策记录：通过现有 `strategy_decision.market_facts` 自然持久化新增上下文；
- 历史记录：不回写旧 decision，缺失字段按 `unavailable` 处理，不用 0 填充。

### 1.7 Minimal end-to-end acceptance path

```text
SPXW quotes
  -> build_expiry_map()
  -> synthetic Call curve
  -> strike differential context
  -> RnDensity.to_dict()
  -> option_structure_frame.density
  -> build_market_fact_pack()
  -> strategy_decision.market_facts.structure.strike_differential_context
```

验收时同一冻结输入在“有上下文”和“移除上下文”两种情况下，以下结果必须完全一致：

```text
decision_type
candidate_id / opportunity_id
action_authority
execution action / limit
rank order
hard-gate failures
```

---

## 2. 为什么放在 Decision Context，而不是直接创建策略

当前统一策略链为：

```text
MarketFactPack
  -> RegimeAssessment
  -> Candidate Factory
  -> Hard Gates / Ranker
  -> StrategyDecision
```

D2/D3/D4 首先是**市场曲面事实**，并非已经验证的交易规则：

- D2 与风险中性密度有关，但不是现实概率 P；
- D3 是局部密度斜率，不等于全分布 skewness；
- D4 是局部密度曲率，不等于全分布 kurtosis；
- 高阶差分会放大报价误差；
- 单帧曲面形状不证明之后的 SPX 路径或某只 Butterfly 会达到 `+50%`。

因此最合理的第一步是：

```text
计算 -> 保存 -> 可见 -> 回放
```

而不是：

```text
计算 -> 解释成 Pin -> 放行 Butterfly
```

将字段放入 `MarketFactPack` 后，现有 deterministic regime、candidate factory、ranker 和未来模型都可以读取，但 v1 没有任何消费者被允许根据它改变结果。

---

## 3. 数学语义

### 3.1 Synthetic Call curve

对同一 SPXW 到期日，使用当前已有的 OTM side 规则构造 Call 曲线：

```text
K < Spot：使用 OTM Put，经 Put-Call Parity 合成 Call
K >= Spot：优先使用 OTM Call
```

在 0DTE 时间尺度下沿用当前实现的近似：

\[
C(K) \approx P(K) + S - K
\]

phase 1 必须把当前 private tuple curve 提升为两个真实调用者共用的 typed value：

```python
@dataclass(frozen=True)
class SyntheticCallPoint:
    strike: float
    mid: float
    bid: float | None
    ask: float | None
    source_right: str
    source_at: datetime | None
```

调用者：

1. 现有 RN density；
2. 新增 strike differential context。

这满足“新抽象至少两个真实调用者”的仓库约束。

### 3.2 二阶中央差分 D2

对中心执行价 K 和尺度 h：

\[
D_2(K,h)=\frac{C(K-h)-2C(K)+C(K+h)}{h^2}
\]

未除以 \(h^2\) 的组合价格：

\[
B(K,h)=C(K-h)-2C(K)+C(K+h)
\]

就是等宽 Butterfly 的 synthetic mid。经济语义：

```text
fly_mid_points = B(K,h)
d2 = B(K,h) / h²
```

`d2` 近似风险中性密度在 K 附近的局部水平，但不能直接称为现实落点概率。

### 3.3 三阶中央差分 D3

使用对称五点 stencil，使数值仍以 K 为中心：

\[
D_3(K,h)=
\frac{-C(K-2h)+2C(K-h)-2C(K+h)+C(K+2h)}{2h^3}
\]

其连续极限近似：

\[
\frac{\partial^3 C}{\partial K^3}
=\frac{\partial f_Q}{\partial K}
\]

符号约定：

```text
D3 > 0：局部风险中性密度随 strike 上升
D3 < 0：局部风险中性密度随 strike 下降
```

这只是局部 slope，不得输出为“市场看涨/看跌”。

### 3.4 四阶中央差分 D4

\[
D_4(K,h)=
\frac{C(K-2h)-4C(K-h)+6C(K)-4C(K+h)+C(K+2h)}{h^4}
\]

其连续极限近似：

\[
\frac{\partial^4 C}{\partial K^4}
=\frac{\partial^2 f_Q}{\partial K^2}
\]

D4 表示局部密度曲率。为避免模型误读符号，context 同时给出一个更直观、但仍中性的 derived field：

\[
PeakVsShoulders(K,h)
=
D_2(K,h)-\frac{D_2(K-h,h)+D_2(K+h,h)}{2}
\]

```text
PeakVsShoulders > 0：中心 D2 高于左右肩部
PeakVsShoulders < 0：中心 D2 低于左右肩部
```

字段名不能叫 `pin_score`，因为它没有路径、时间稳定性或现实概率语义。

### 3.5 多尺度

phase 1 复用当前 candidate factory 已采用的宽度集合，避免第二套宽度 taxonomy：

```python
STRIKE_DIFFERENTIAL_SCALES_POINTS = (5.0, 10.0, 15.0, 20.0)
```

该常量属于版本化 feature semantics，不进入 runtime config。修改尺度集合必须将：

```text
feature_version = strike_differential_context.v1
```

升级为新版本，并保留回放兼容。

### 3.6 不能混淆的 Greeks

这里所有导数均相对于执行价 K：

\[
\partial^n C / \partial K^n
\]

不是相对于标的 S 的：

```text
Delta / Gamma / Speed
```

决策上下文的字段必须使用 `strike_d2`、`strike_d3`、`strike_d4`，禁止简称 `gamma`、`speed`。

---

## 4. Bid-Ask 噪声上界

高阶差分最大的现实问题是：权重绝对值快速增加，报价噪声会被放大。v1 不只输出估计值，还必须输出由各腿半点差推导的线性误差上界。

设每个 synthetic Call 点的半点差为：

\[
e_i=(Ask_i-Bid_i)/2
\]

对于线性组合：

\[
Y=\sum_i w_iC_i
\]

保守报价噪声上界：

\[
NoiseBound(Y)=\sum_i |w_i|e_i
\]

分别使用 D2、D3、D4 stencil 的归一化权重计算：

```text
d2_noise_bound
d3_noise_bound
d4_noise_bound
```

以及：

\[
SNR_n=\frac{|D_n|}{\max(NoiseBound(D_n),\epsilon)}
\]

输出：

```text
d2_snr
d3_snr
d4_snr
```

SNR 仅描述“估计量相对于当前报价宽度有多大”，不表示预测质量。v1 不将 SNR 放入 hard gate 或 ranker。

若任何所需点缺少双边 bid/ask：

- 对应差分值仍可在 mid 可用时计算；
- noise bound 与 SNR 必须为 `null`；
- point quality 标为 `degraded_missing_bbo`；
- 不得用 0 代替缺失 spread。

---

## 5. Reference Centers 与 Context 尺寸

为了防止完整 strike grid 膨胀 `strategy_decision` 和 GPT prompt，decision context 只保留有限 reference centers。

候选中心来源：

```text
ATM / nearest-to-spot strike
Q mode
Zero Gamma
Flip midpoint
Put Wall
Call Wall
```

规则：

1. 所有中心先 round 到真实 5 点 strike；
2. 相同数值去重；
3. 一个中心可以带多个 labels，例如 `['atm', 'q_mode', 'zero_gamma']`；
4. 仅保留 chain 中存在的中心；
5. 上限 6 个中心；
6. 不为满足上限制造 fallback 数值；
7. 本期不加入 candidate target，因为 Candidate Factory 在 MarketFactPack 之后运行。

每个中心最多输出 4 个 scale，总点数上限：

```text
6 centers x 4 scales = 24 observations
```

完整 strike grid 不进入 decision。未来若 dashboard 或训练需要全网格，应从 quote lake 用相同纯函数重建，或另行批准独立 research artifact；不得先把全链塞进人类决策对象。

---

## 6. 数据合同

### 6.1 RnDensity 增量字段

`RnDensity` 新增 optional：

```python
strike_differential_context: dict[str, Any] | None = None
```

`to_dict()` 在值为 `None` 时删除该 key，保持旧冻结 payload 兼容。phase 1 不升级 `strategy_decision.v2`，因为现有 decision 已把 `market_facts` 作为可扩展 mapping；若实现中发现 Rust 或 golden contract 对嵌套字段严格拒绝，必须停止并另行批准 schema bump，不得静默改 contract。

### 6.2 MarketFactPack 字段

目标路径：

```text
market_facts.structure.strike_differential_context
```

`strategy_facts.py` 只复制 analytics 结果，不重新计算差分，不解释符号，不生成结论。

### 6.3 JSON 示例

```json
{
  "feature_version": "strike_differential_context.v1",
  "authority": "context_only",
  "semantics": "risk_neutral_strike_shape",
  "expiry": "20260810",
  "as_of": "2026-08-10T17:35:02.144000+00:00",
  "status": "ready",
  "source_curve": "otm_synthetic_call_bbo",
  "scales_points": [5.0, 10.0, 15.0, 20.0],
  "references": [
    {
      "center": 7775.0,
      "labels": ["atm", "q_mode", "zero_gamma"],
      "observations": [
        {
          "scale_points": 5.0,
          "quality": "ready",
          "fly_mid_points": 0.85,
          "strike_d2": 0.034,
          "strike_d3": -0.0012,
          "strike_d4": -0.00008,
          "peak_vs_shoulders": 0.0065,
          "d2_noise_bound": 0.009,
          "d3_noise_bound": 0.0008,
          "d4_noise_bound": 0.00007,
          "d2_snr": 3.7778,
          "d3_snr": 1.5,
          "d4_snr": 1.1429,
          "required_strikes": [7765.0, 7770.0, 7775.0, 7780.0, 7785.0],
          "reasons": []
        }
      ]
    }
  ],
  "diagnostics": {
    "reference_count": 4,
    "observation_count": 16,
    "ready_count": 10,
    "degraded_count": 4,
    "unavailable_count": 2,
    "missing_strikes": [7815.0],
    "local_monotonic_violations": 0,
    "local_convexity_violations": 1
  }
}
```

示例数字仅说明 wire shape，不是阈值或预期行情。

### 6.4 Status 枚举

顶层：

```text
ready       至少一个 reference/scale 可用，且没有全局阻断
partial     部分 reference/scale 可用
unavailable 没有任何可用 observation
```

单 observation：

```text
ready
degraded_missing_bbo
degraded_low_snr
unavailable_missing_strikes
blocked_monotonicity_violation
blocked_convexity_violation
```

`degraded_low_snr` 的 bootstrap 定义为相应高阶 SNR < 1；它仅影响 quality 标签，不影响策略。阈值若后续要参与 ranking，必须由新的版本化设计和回放证据批准。

---

## 7. 计算流程

### 7.1 `analytics/options/density.py`

将当前 `_synthetic_call_curve()` 改造成 typed、可复用函数：

```python
def synthetic_call_curve(
    pairs: dict[float, dict[OptionRight, Quote]],
    underlier: float,
) -> tuple[SyntheticCallPoint, ...]:
    ...
```

现有 global RN density 和新 local differential context 都读取同一份 curve，禁止各自实现一次 OTM/parity 规则。

新增纯函数：

```python
def build_strike_differential_context(
    curve: Sequence[SyntheticCallPoint],
    *,
    expiry: str,
    as_of: datetime,
    reference_levels: Mapping[str, float | None],
    scales: Sequence[float] = STRIKE_DIFFERENTIAL_SCALES_POINTS,
) -> dict[str, Any]:
    ...
```

内部步骤：

```text
1. curve 按 strike 排序并构建 exact-strike map
2. reference level round 到真实 strike，并去重 labels
3. 对每个 center / scale 请求 K±h、K±2h
4. 缺点则 observation unavailable，不插值
5. 检查本地 Call monotonicity
6. 检查组成 D2 的本地 convexity
7. 计算 fly_mid、D2、D3、D4、PeakVsShoulders
8. 若双边 BBO 完整，计算 noise bound 和 SNR
9. 汇总 diagnostics
```

### 7.2 不从 clipped density 再求导

当前 RN density 会对负质量 clipping 后重新归一化。D3/D4 不能在 clipping 后的 density 上继续求导，因为 clipping 会人为制造 kink。

正确顺序：

```text
raw synthetic Call curve
  -> local static-arbitrage checks
  -> D2 / D3 / D4
```

与：

```text
raw synthetic Call curve
  -> global D2 cells
  -> clipping / normalization
  -> RN percentiles
```

是两个并行输出。

### 7.3 Local monotonicity

同到期 Call 应随 strike 非增。五点窗口若存在：

\[
C(K_{i+1}) > C(K_i) + tolerance
\]

则该 observation：

```text
quality = blocked_monotonicity_violation
```

v1 tolerance 仅用于浮点舍入，应取极小固定数，不得用宽松阈值掩盖可执行静态套利或错位报价。

### 7.4 Local convexity

任意 D2 stencil 若：

\[
C(K-h)-2C(K)+C(K+h) < -tolerance
\]

则：

```text
quality = blocked_convexity_violation
```

不得将负值 clip 成 0 后继续生成 D3/D4。

### 7.5 缺失 strike

v1 只使用 exact strikes，不做 spline、线性插值或外推。原因：

- 高阶差分对插值方法高度敏感；
- 当前目标是保存可审计事实，不是制造连续曲面；
- wide-chain 数据是否足够本身就是重要 diagnostics。

未来无套利 convex fit 属于单独 challenger，不得混入 v1 feature semantics。

---

## 8. `service.py` 编排改动

`build_expiry_map()` 已经在构造 RN density 之前得到：

```text
underlier
atm_strike
zero_gamma
gamma_flip_zone
put_wall
call_wall
expiry
as_of
```

调用 `build_rn_density()` 时新增：

```python
reference_levels={
    "atm": atm_strike,
    "q_mode": None,
    "zero_gamma": zero,
    "flip_midpoint": midpoint(gamma_flip_zone),
    "put_wall": put_wall,
    "call_wall": call_wall,
}
```

`q_mode` 是 global density 的结果，因此不能作为同一次函数调用的先验输入。实现采用两步：

```text
A. 从 curve 计算 global density 和初始 local context
B. 得到 q_mode 后，若 q_mode 尚未包含在 reference centers，则追加一次 q_mode local observation
```

禁止为了 q_mode 重建第二份 synthetic curve。

若 global density 因 strike coverage 不足而 `INSUFFICIENT_STRIKES`，只要某个 reference 的 exact stencil 完整，local differential observation 仍可 ready。Global density quality 与 local context quality 独立记录。

---

## 9. `strategy_facts.py` 接入

在现有：

```python
"structure": {
    "zero_gamma": ...,
    "flip_zone": ...,
    "put_wall": ...,
    "call_wall": ...,
    "q_mode": ...,
    "q_local_mass_5pt": ...,
}
```

增加：

```python
"strike_differential_context": dict(
    _map(density.get("strike_differential_context"))
),
```

规则：

- analytics 没有产出时使用 `{}`，不是伪造数值；
- `MarketFactPack.quality` 不因该研究字段缺失而由 ready 降级；
- 该字段是 optional advisory context，不属于 pricing authorization 必需条件；
- `available_at` 继续由 option frame 的 `available_at` 约束，不另造时间。

`strategy_select._base_decision()` 已经原样加入：

```text
strategy_decision.market_facts = facts
```

因此不需要修改 `strategy_select.py` 才能进入最终 decision。

---

## 10. 模型可见性与权限边界

### 10.1 当前 GPT memo

当前 `call_strategy_idea_memo(decision)` 把完整 decision 序列化为 JSON。因此字段加入 `market_facts` 后，现有模型天然可见。

v1 不修改 system prompt，原因：

- 用户本期只批准“进入上下文”；
- 目前没有证据支持固定的 D3/D4 解释规则；
- prompt 教模型将某个符号解释成 Pin/De-pin，会提前把研究假设固化成 authority。

允许结果：模型忽略这些字段。v1 的成功条件是数据正确、因果、可回放，不是模型必须引用。

### 10.2 禁止的 authority leakage

**SDCTX-desk-v1.1 phase note（2026-08-10）**：本节原先对 ranker 的全面禁令已被下述窄规则取代；数学语义和 `strike_differential_context.v1` wire payload 不变。§12.4 的全量 non-interference 验收也相应收窄为“低 SNR、缺失或 unavailable 时排序不变”。

以下消费者仍禁止读取该字段并改变结果：

```text
strategy_regime.assess_regime
candidate_factory.enumerate_candidates
strategy_ranker._hard_gate_candidate
ManagementPolicy
notification delivery authority
```

`strategy_ranker` 只允许在所有 hard gates 通过后读取统一 summary：

```text
base selection_score + surface_shape_prior
```

其中 `|surface_shape_prior| <= 0.05`；低/未知 SNR、缺失或 unavailable 必须为 0。该 prior 只能在已枚举、已过硬门的候选间软排序，并记录 `selection_score_base` 与 `surface_shape_prior`。它不得创建候选、不得单独把 `NO_TRADE` 翻为人工候选、不得改变 `manual_action_eligible`、`action_authority` 或 `automatic_ordering`。方向 vertical 仅在高 SNR D3 与 CALL/PUT 方向一致时加分；Butterfly 仅在高 SNR 局部峰形时小幅加分。

### 10.3 人类文案

~~v1 不在卡片中打印 D2/D3/D4。~~ SDCTX-desk-v1.1 允许：

- `strategy_decision.desk_view.surface_shape` 与 `why_not.surface_shape` 保存统一 structured summary；
- Desk Map 最多追加一行 D3 斜率、D4 峰/槽/平与 SNR；
- `why_not.reasons` 可追加 `surface_shape_*` 机器诊断，但不得替换首要交易 blocker。

文案必须保留 `desk_explain_and_rank_soft` authority，不得声称 Alpha、dealer positioning 或 pin certainty；通知继续复用现有 Desk View / why_not 渲染，不新增通知路径。

---

## 11. 因果性、持久化与回放

### 11.1 Point-in-time

每份 context 必须携带：

```text
expiry
as_of
feature_version
source_curve
```

且满足：

```text
source_at <= as_of <= decision_at
available_at <= decision_at
```

任何 future quote 使对应 observation unavailable；不得回退到之后的 quote。

### 11.2 决策持久化

现有 strategy decision 已持久化 `market_facts`。因此 phase 1 不新增 feature table。优点：

- 每个候选或 NoTrade 都保存当时看到的上下文；
- 以后可按 decision_id 与 ManagementPolicy outcome 连接；
- 不产生第二套 current-state writer。

### 11.3 历史回填

v1 不修改旧 decision。后续 offline research 可以从 raw quote lake：

```text
point-in-time chain -> same pure function -> feature snapshot
```

重建。回填产物必须记录：

```text
feature_version
known_bias
source session
available_at
```

不得将按新逻辑回填的 feature 声称为当时生产已经可见。

---

## 12. 测试设计

### 12.1 数学不变量

在 analytics 单元测试中使用确定性多项式曲线：

1. 常数/线性曲线：D2、D3、D4 均为 0；
2. 二次曲线：D2 精确，D3/D4 为 0；
3. 三次曲线：D3 精确，D4 为 0；
4. 四次曲线：D4 精确；
5. 对 Call 曲线加常数或线性项不改变 D2/D3/D4；
6. strike 平移不改变对应局部导数值；
7. spread 扩大时 noise bound 不得下降；
8. 所有 bid=ask 时 noise bound 为 0；
9. 缺 K±2h 时 D2 可用但 D3/D4 unavailable；
10. 负 Butterfly 不得被 clip 成 0。

优先使用 Hypothesis 生成 h、中心和多项式系数，测试 money/time/data invariants，而不是 helper 调用顺序。

### 12.2 Synthetic parity

测试：

- spot 以下使用 Put 合成 Call；
- spot 以上优先 Call；
- parity 合成 bid/ask 顺序正确；
- 不使用 deep ITM 宽报价覆盖更优 OTM side；
- source_right 与 source_at 保留。

### 12.3 Context compactness

断言：

```text
references <= 6
observations <= 24
没有重复 center
labels 去重
缺失为 null / reason，不是 0
```

### 12.4 Decision non-interference

构造相同市场输入，两份 payload 仅差：

```text
strike_differential_context = 极端正值
strike_differential_context = 极端负值
```

断言以下完全相同：

```text
decision_type
candidate_id
opportunity_id
action_authority
execution
risk
targets
candidate order
failed gates
```

该测试是 v1 最重要的安全门。

### 12.5 Serialization

- `RnDensity.to_dict()` 在 context 为 `None` 时维持旧 shape；
- 有值时可 JSON serialize；
- `MarketFactPack` 与最终 `strategy_decision` 中内容一致；
- `available_at` 不早于 context 的 source time；
- 现有 idea memo validator 仍可处理含该上下文的 decision。

### 12.6 Frozen replay

选择少量现有冻结 session：

```text
2026-08-05 PIN_MIGRATING
2026-08-06 CALL_BUTTERFLY
2026-08-07 candidate replay
2026-08-08 NO_TRADE control
```

仅比较交易语义不变，并保存 context 的 presence/quality 作为附加验收证据。

---

## 13. 实施任务卡

### SDCTX-1：Shared synthetic Call curve

修改：

```text
analytics/options/models.py
analytics/options/density.py
```

产出：

- `SyntheticCallPoint`；
- 单一 OTM/parity curve builder；
- 现有 RN density 迁移到该 builder；
- 旧 density 结果冻结测试保持一致。

### SDCTX-2：Local differential kernel

修改：

```text
analytics/options/density.py
```

产出：

- D2/D3/D4；
- PeakVsShoulders；
- bid-ask noise bound / SNR；
- exact-strike / monotonicity / convexity diagnostics；
- compact reference selection。

### SDCTX-3：Options map composition

修改：

```text
analytics/options/service.py
analytics/options/models.py
```

产出：

- 传入 ATM、zero gamma、flip、walls；
- q_mode 第二步补充；
- `RnDensity.strike_differential_context`；
- global/local quality 独立。

### SDCTX-4：Decision context wiring

修改：

```text
application/order_map/strategy_facts.py
```

产出：

```text
market_facts.structure.strike_differential_context
```

不修改 regime、candidate、ranker、prompt。

### SDCTX-5：Acceptance evidence

修改/新增测试：

```text
tests/test_options_map.py
现有 strategy decision 相关测试文件
```

输出研究验收文档：

```text
docs/research/strike-differential-context-acceptance-YYYY-MM-DD.md
```

内容至少包含：

- 数学测试结果；
- 四个冻结 session 的 non-interference 结果；
- context readiness / missing stencil 分布；
- 典型 D2/D3/D4 SNR；
- complexity budget。

---

## 14. 验收门

实现只有在以下全部满足时才可合并：

1. RN density 既有冻结输出无无意变化；
2. 新 context 只来自 decision_at 前的 quotes；
3. exact-strike 缺失 fail closed；
4. 负凸性不 clipping 后继续求高阶差分；
5. 所有 observation 有 quality 和 reasons；
6. context 最多 6 centers / 24 observations；
7. frozen replay 的交易决策完全不变；
8. ranker、candidate factory、regime 没有读取该字段；
9. 未新增配置、服务、存储或 Rust contract；
10. `git diff --check`、相关 pytest、Ruff、Import Linter 通过。

明确不作为本期验收门：

```text
D3/D4 能预测盈利
模型会引用这些字段
Butterfly 命中率提升
出现新的人工候选
```

---

## 15. 后续阶段，但不属于本设计

### 15.1 Interpretation contract

独立定义模型可以如何解释：

```text
D2 level
D3 sign
D4 / PeakVsShoulders
cross-scale agreement
SNR
center migration
```

以及哪些词禁止使用，例如未经校准不得称为：

```text
真实概率
确定 Pin
必然 De-pin
做市商目标位
正 EV
```

### 15.2 Candidate-specific snapshot

未来可给每只 Vertical / Butterfly 附加：

```text
surface_evidence_at_target
surface_evidence_at_body
surface_evidence_between_spot_and_target
```

但这会进入 candidate object，必须另行确认是否只作为 annotation、rank-only 还是 hard gate。

### 15.3 Outcome study

连接现有 ManagementPolicy 标签：

```text
tp_armed
premium_stop
time_to_arm
MFE / MAE
policy_pnl_points
```

做 session-level walk-forward ablation：

```text
baseline existing features
+ D2
+ D2/D3
+ D2/D3/D4
+ multi-scale/SNR
```

只有样本外增量稳定后，才允许提出 rank-only 接入。

### 15.4 Option Transformer challenger

未来模型可以把每个 strike 视为 token，candidate 视为 query，但必须：

- 只排序 deterministic candidate factory 已生成的简单结构；
- 不生成任意多腿组合；
- 不越过 hard gates；
- 不自动下单；
- 在校准通过前不拥有否决权。

---

## 16. Complexity Budget

本设计预期 phase 1：

| 项目 | 变化 |
|---|---:|
| Production files added | 0 |
| Production files modified | 4 |
| Production files deleted | 0 |
| Dependencies added/removed | 0 / 0 |
| Config keys added/removed | 0 / 0 |
| Services/timers added/removed | 0 / 0 |
| Databases/tables added/removed | 0 / 0 |
| Rust contracts changed | 0 |
| Notification paths changed | 0 |
| Strategy authority paths changed | 0 |

预期修改文件：

```text
src/spx_spark/analytics/options/density.py
src/spx_spark/analytics/options/models.py
src/spx_spark/analytics/options/service.py
src/spx_spark/application/order_map/strategy_facts.py
```

若实现需要超出该边界，先更新 Change Brief 并重新批准，不得边写边扩张。

---

## 17. 最终原则

本设计把“Call 是 ReLU、Butterfly 是 strike 上的离散二阶算子、三阶和四阶描述局部密度形状”落实为一个工程上可审计的事实层，但不提前声称它是交易 edge。

最终边界是：

```text
复杂数学可以进入上下文；
解释权和交易权必须由后续证据单独获得。
```

也就是：

```text
先让系统看见，再研究它是否值得相信。
```
