# SPXW 执行价算子扩展与模型研究附录 v1

状态：**已实现（随主文档 SDCTX-1–5；验收见 `docs/research/strike-differential-context-acceptance-2026-08-10.md`）**  
适用仓库：`hzy-hits/SpxOpDaily`  
适用主文档：`docs/spxw-strike-differential-decision-context-v1.md`  
设计边界：**扩充只读决策上下文与后续研究合同，不改变候选生成、排序、硬门、人工权限或自动下单边界**

> 本附录补齐主文档未显式展开的 Richardson extrapolation、Mexican Hat、Simpson Butterfly Strip、相邻 Butterfly relative value、高阶组合使用场景、时间域 Runge–Kutta 边界、ManagementPolicy 路径标签、Option Transformer challenger 与确定性策略编译器。主文档与本附录共同构成 `strike_differential_context.v1` 的实施设计；若同一字段语义冲突，以本附录为准。

---

## 0. 覆盖结论

以下想法在主文档中的状态不同：

| 想法 | 主文档原状态 | 本附录后的状态 |
|---|---|---|
| 普通 D2 Butterfly | 已显式纳入 | 保持 |
| D3：高 strike Fly − 低 strike Fly | 数学上已纳入，但未写经济等价 | 显式加入经济别名 |
| D4：肩部 Fly − 2×中央 Fly | 数学上已纳入，但未写经济等价 | 显式加入经济别名 |
| Mexican Hat：中央 pin vs shoulders | `PeakVsShoulders` 已等价纳入 | 补充严格恒等式与 raw portfolio value |
| Richardson 高精度 D2 | 未纳入 | 加入只读 context 与噪声诊断 |
| Simpson Butterfly Strip | 未纳入 | 加入局部区间 state-price mass proxy；完整 composite strip 保留离线研究 |
| Runge–Kutta | 未纳入 | 明确排除出静态 strike context；仅允许未来进入定价/动态控制研究 |
| 三阶、四阶真实下单策略 | 明确不做 | 继续 research-only，不进入人工候选 enum |
| `P(+50% arm before stop)` 等路径目标 | 仅列 outcome study | 补充正式未来模型输出合同 |
| Option Transformer | 仅有原则性段落 | 补充 token、query、输出与权限约束 |
| 确定性策略编译器 | 未完整定义 | 补充目标函数与硬约束，但不在本期实现 |

核心原则不变：

```text
复杂算子可以进入上下文；
复杂算子不能自动获得解释权；
模型可以排序简单候选；
模型不能自由生成多腿怪结构。
```

---

## 1. 对主文档 v1 wire contract 的修订

主文档每个 `center / scale` observation 已包含：

```text
fly_mid_points
strike_d2
strike_d3
strike_d4
peak_vs_shoulders
noise bounds / SNR
```

本附录要求新增以下可选字段：

```text
adjacent_fly_spread_points
fly_curvature_points
mexican_hat_points
richardson
simpson_local_mass
virtual_portfolio_units
```

这些字段全部属于：

```text
authority = context_only
semantics = risk_neutral_strike_shape
```

缺失时为 `null` 或明确 quality/reason，不使用 0 冒充缺失，也不影响 `MarketFactPack.quality`、候选排序或交易授权。

建议 observation 结构：

```json
{
  "scale_points": 5.0,
  "quality": "ready",
  "fly_mid_points": 0.85,
  "strike_d2": 0.034,
  "adjacent_fly_spread_points": -0.30,
  "strike_d3": -0.0012,
  "fly_curvature_points": 0.05,
  "strike_d4": 0.00008,
  "mexican_hat_points": -0.05,
  "peak_vs_shoulders": -0.001,
  "richardson": {
    "paired_scale_points": 10.0,
    "strike_d2": 0.0312,
    "truncation_disagreement": 0.0021,
    "noise_bound": 0.011,
    "snr": 2.8364,
    "quality": "ready"
  },
  "simpson_local_mass": {
    "lower": 7765.0,
    "upper": 7785.0,
    "state_price_mass_proxy": 0.184,
    "noise_bound": 0.041,
    "snr": 4.4878,
    "quality": "ready"
  },
  "virtual_portfolio_units": {
    "d2_gross": 4,
    "d3_gross": 8,
    "d4_gross": 16,
    "mexican_hat_gross": 16,
    "richardson_gross": 64,
    "simpson_netted_gross": 12
  },
  "reasons": []
}
```

示例数字仅说明 shape，不构成阈值。

---

## 2. D3 的经济等价：相邻局部概率块 relative value

设普通等宽 Butterfly 的未归一化价格为：

\[
B_h(K)=C(K-h)-2C(K)+C(K+h)
\]

主文档的对称五点三阶差分为：

\[
D_3(K,h)=
\frac{-C(K-2h)+2C(K-h)-2C(K+h)+C(K+2h)}{2h^3}
\]

它严格等价于：

\[
D_3(K,h)=\frac{B_h(K+h)-B_h(K-h)}{2h^3}
\]

因此新增 raw economic alias：

```text
adjacent_fly_spread_points
  = B_h(K+h) - B_h(K-h)
```

其虚拟组合系数为：

```text
-1 : +2 : 0 : -2 : +1
```

对应 strikes：

```text
K-2h, K-h, K, K+h, K+2h
```

去掉零权重后可视为四个 strike、gross contract units 为 8。经济描述只能写成：

```text
高 strike 局部 Butterfly 相对低 strike 局部 Butterfly 的价格差
```

不得直接写成：

```text
看涨信号
上涨概率
资金正在向上移动
```

除非后续路径 outcome 研究证明其条件预测意义。

---

## 3. D4 的经济等价：Butterfly 的 Butterfly

主文档的四阶差分：

\[
D_4(K,h)=
\frac{C(K-2h)-4C(K-h)+6C(K)-4C(K+h)+C(K+2h)}{h^4}
\]

严格等价于：

\[
D_4(K,h)=
\frac{B_h(K-h)-2B_h(K)+B_h(K+h)}{h^4}
\]

新增 raw economic alias：

```text
fly_curvature_points
  = B_h(K-h) - 2 B_h(K) + B_h(K+h)
```

其五 strike 系数：

```text
+1 : -4 : +6 : -4 : +1
```

经济上是：

```text
Long 两侧 shoulder flies
Short 2×中央 fly
```

反向组合则是：

```text
Long 2×中央 fly
Short 两侧 shoulder flies
```

本字段描述中央局部概率块相对两侧局部概率块的曲率，不等于全分布 kurtosis。

---

## 4. Mexican Hat 与 `PeakVsShoulders` 的严格关系

定义：

\[
M_h(K)=4B_h(K)-B_{2h}(K)
\]

展开：

\[
M_h(K)=
-C(K-2h)+4C(K-h)-6C(K)+4C(K+h)-C(K+2h)
\]

系数：

```text
-1 : +4 : -6 : +4 : -1
```

新增字段：

```text
mexican_hat_points = M_h(K)
```

主文档的：

\[
PeakVsShoulders(K,h)
=D_2(K,h)-\frac{D_2(K-h,h)+D_2(K+h,h)}{2}
\]

与 Mexican Hat、D4 不是三个独立特征，而是严格线性相关：

\[
M_h(K)=2h^2\,PeakVsShoulders(K,h)
\]

\[
M_h(K)=-h^4D_4(K,h)
\]

因此实现和后续模型必须标记：

```text
dependency_group = d4_equivalent
```

避免把 `strike_d4`、`peak_vs_shoulders`、`mexican_hat_points` 当成三个独立证据重复计权。

字段存在的理由不同：

- `strike_d4`：规范化数学导数；
- `peak_vs_shoulders`：较直观的中心对肩部比较；
- `mexican_hat_points`：对应可构造虚拟组合的原始 option points。

它们进入上下文，不进入 v1 ranker。

---

## 5. Richardson extrapolation：高精度 D2 与高噪声并存

普通中央二阶差分：

\[
D_2(K,h)=\frac{C(K-h)-2C(K)+C(K+h)}{h^2}
\]

在曲线足够光滑时误差为：

\[
O(h^2)
\]

使用 h 与 2h：

\[
D_{2,R}(K,h)=\frac{4D_2(K,h)-D_2(K,2h)}{3}
\]

展开：

\[
D_{2,R}(K,h)=
\frac{-C(K-2h)+16C(K-h)-30C(K)+16C(K+h)-C(K+2h)}{12h^2}
\]

理论截断误差提升为：

\[
O(h^4)
\]

但虚拟组合 gross contract units 为：

\[
1+16+30+16+1=64
\]

所以必须与噪声诊断绑定发布，不能只发布更“漂亮”的点估计。

### 5.1 Wire fields

每个 observation 在 `2h` 也属于支持尺度时，可以输出：

```json
{
  "richardson": {
    "base_scale_points": 5.0,
    "paired_scale_points": 10.0,
    "strike_d2": 0.0312,
    "truncation_disagreement": 0.0021,
    "noise_bound": 0.011,
    "snr": 2.8364,
    "quality": "ready",
    "reasons": []
  }
}
```

其中：

\[
truncation\_disagreement
=\frac{|D_2(K,2h)-D_2(K,h)|}{3}
\]

该量只是网格尺度不一致代理，不是统计置信区间。

### 5.2 支持尺度

当前尺度：

```text
5, 10, 15, 20
```

Richardson 配对仅允许：

```text
h=5  -> 2h=10
h=10 -> 2h=20
```

`15 -> 30` 不允许通过外推制造。

### 5.3 Quality

以下任一出现时 Richardson unavailable/degraded：

- h 或 2h D2 不可用；
- K±2h exact strikes 缺失；
- local monotonicity/convexity 失败；
- 任一 required leg 来自未来或过期 quote；
- BBO 不完整时保留估计值，但 noise/SNR 为 null；
- `richardson_snr < 1` 时标记 `degraded_low_snr`；
- Richardson 与普通 D2 符号冲突时记录 `scale_sign_conflict`，不自动裁决谁正确。

### 5.4 不能声称什么

即使 Richardson 数值更平滑，也不能声称：

```text
更准确的真实概率
更强的交易 edge
更可靠的 pin 预测
```

它只是在给定 smooth curve 假设下减少离散截断误差；0DTE 的报价误差、strike 离散、临近到期尖锐形状和微观结构噪声可能占主导。

---

## 6. Simpson Butterfly Strip：局部区间 state-price mass proxy

目标是估计：

\[
\int_A^B C_{KK}(u)du
\]

在无套利、完整曲线和贴现处理正确时，它对应风险中性区间 state-price mass。v1 不直接称为现实概率。

### 6.1 Reference-center local Simpson

对中心 K 和尺度 h，定义区间：

\[
[K-h,K+h]
\]

使用三点 Simpson：

\[
S_h(K)=
\frac{h}{3}
\left[D_2(K-h,h)+4D_2(K,h)+D_2(K+h,h)\right]
\]

展开为 Call 价格组合：

\[
S_h(K)=
\frac{C(K-2h)+2C(K-h)-6C(K)+2C(K+h)+C(K+2h)}{3h}
\]

净系数：

```text
+1 : +2 : -6 : +2 : +1
```

netted gross contract units：

\[
1+2+6+2+1=12
\]

新增：

```text
simpson_local_mass.state_price_mass_proxy
simpson_local_mass.noise_bound
simpson_local_mass.snr
simpson_local_mass.lower / upper
```

### 6.2 语义限制

字段名称必须是：

```text
state_price_mass_proxy
```

而不是：

```text
probability
real_probability
pin_probability
```

原因：

- 当前 synthetic Call 使用 0DTE 近似 parity；
- 全局 RN density 会 clipping/normalization，而 local Simpson 使用 raw curve；
- 贴现、曲线边界和可执行套利约束尚未完整建模；
- P 与 Q 不同；
- 区间内状态价格质量不代表之后 20 分钟路径命中率。

### 6.3 与当前 RN density 的交叉诊断

若现有 global density 可以给出同一区间的 CDF mass，则 context 可增加：

```text
rn_density_interval_mass
quadrature_mass_gap
```

\[
quadrature\_mass\_gap
=S_h(K)-Q_{density}([K-h,K+h])
\]

该差异用于质量诊断，不用于 v1 交易授权。若 global density 被大量 clipping，而 Simpson raw mass 与其冲突，模型应该看到 disagreement，而不是让其中一个静默覆盖另一个。

### 6.4 Composite Simpson

完整 composite Simpson：

```text
1, 4, 2, 4, 2, ..., 4, 1
```

需要整段 exact strike grid，并会显著扩大 decision payload。v1 决策上下文只保存 reference-center local Simpson；完整 strip 只允许在：

```text
offline research artifact
surface dashboard diagnostic
risk-neutral moment calculation
structured payoff replication study
```

中重建。

不得把整个 composite strip 放入每分钟 `strategy_decision`。

---

## 7. 线性差分组合的统一虚拟组合描述

为了让模型理解“数学值”和“若真下单会有多复杂”之间的差别，每个 operator 可以携带固定元数据：

| Operator | 规范化含义 | Raw coefficients | Gross units | 生产可下单候选 |
|---|---|---|---:|---|
| D2 / Fly | 局部密度水平 | `1,-2,1` | 4 | 只有普通 Butterfly 可枚举 |
| D3 | 相邻 Fly relative value | `-1,2,0,-2,1` | 8 | 否 |
| D4 | Fly curvature | `1,-4,6,-4,1` | 16 | 否 |
| Mexican Hat | 中央 vs shoulders | `-1,4,-6,4,-1` | 16 | 否 |
| Richardson D2 | 四阶精度 D2 | `-1,16,-30,16,-1` | 64 | 否 |
| Simpson local mass | 1:4:1 Fly strip 积分 | `1,2,-6,2,1` | 12 | 否 |

主文档的 `virtual_portfolio_units` 只说明组合复杂度和噪声放大，不构成执行建议。

---

## 8. 三阶、四阶组合的真实使用者与使用场景

这些结构不作为零售 SPX 0DTE 默认交易策略。它们更常见的角色是风险分解和相对价值语言。

### 8.1 Volatility relative-value trader

可能研究：

```text
Long 高 strike Fly - Short 低 strike Fly
```

或：

```text
Long central fly - Short shoulder flies
```

目标是等待：

- smile curvature 重定价；
- event premium 在相邻 strike 之间重新分配；
- pin narrative 消退或迁移；
- 相邻局部状态价格恢复相对关系。

通常关注相对价格变化，不要求持有到到期 payoff。

### 8.2 做市商 / volatility surface desk

客户订单会让整个 book 自然积累：

- 某些 strike 的局部 convexity；
- smile curvature；
- digital/barrier 邻近风险；
- 局部 density concentration。

交易台更可能用普通 Fly、Calendar Fly、Risk Reversal、Condor 等流动性结构净化 book，而不是直接提交一个 64-unit Richardson 组合。

### 8.3 Exotic / structured product desk

Autocall、barrier、digital、range accrual 等 payoff 含大量 kink 和局部曲率。实务上会：

1. 写出目标 payoff；
2. 计算/近似 payoff 二阶导数；
3. 得到连续 Call/Put strip；
4. 在有限流动 strikes 上解复制误差、成本与风险的优化问题。

高阶算子可用于检查复制 residual，不等于最终 hedge portfolio 必须采用 Pascal 系数。

### 8.4 Quant research / model validation

主要用途：

- 风险中性密度提取；
- butterfly arbitrage 检查；
- smile slope/curvature 诊断；
- surface fit 比较；
- 报价异常检测；
- 模型 residual 定位。

所以在 `SpxOpDaily` 中，它们首先属于：

```text
measurement / context / replay features
```

不是：

```text
manual strategy authority
```

---

## 9. Runge–Kutta 的边界：时间推进，不是静态 strike 组合

Runge–Kutta 解决：

\[
\frac{dx}{dt}=F(x,t)
\]

它沿时间推进状态；D2/D3/D4 沿 strike 轴做静态差分。二者不能因为都来自数值分析就放进同一个算子字段。

### 9.1 v1 明确不新增

禁止添加：

```text
rk4_signal
rk4_butterfly
runge_kutta_score
```

原因：

- 系统不进行自动 delta hedging；
- 没有已验证的 Greeks 状态 ODE；
- RK4 的中间 stage 需要模型预测，不是免费可观测未来；
- 更多阶段调仓意味着更多成本；
- 当前项目自动下单禁止。

### 9.2 未来允许的两个位置

Runge–Kutta 未来只可能进入：

1. `pricing / simulation`：求解定价 PDE、SDE 数值路径或状态 ODE；
2. `dynamic policy simulator`：比较 Euler、Heun、RK-style 预测-校正对冲规则的离线成本。

即使进入，也不得直接写入静态：

```text
market_facts.structure.strike_differential_context
```

如需给决策模型时间信息，应优先保存真实、因果的：

```text
D2/D3/D4 at t
delta over 1m / 5m
center migration
sign persistence
scale stability
```

而不是用未验证模型生成伪 RK 中间状态。

---

## 10. 路径型模型目标：围绕真实 ManagementPolicy，而非收盘点

当前仓库的 `management_policy.v1` 是：

```text
entry = conservative combo ask
valuation = conservative combo bid
profit arm = +50% return on debit
premium stop = bid <= 50% of debit（即约 -50%）
trail after arm = 75% of peak bid，且 floor 不低于 entry debit
time stop = 20 minutes
hard exit = 15:45 ET
```

因此正式未来预测 target 应参数化读取 policy，而不是把 `-40%` 或 `+50%` 写死在模型代码中。

### 10.1 推荐输出合同

```json
{
  "schema_version": "strategy_path_outcome_forecast.v1",
  "policy_version": "management_policy.v1",
  "candidate_id": "...",
  "p_profit_arm_before_premium_stop": 0.58,
  "p_premium_stop_before_profit_arm": 0.27,
  "p_time_stop_before_either": 0.15,
  "expected_time_to_arm_seconds": 1080,
  "policy_pnl_points": {
    "p10": -1.05,
    "p50": 0.22,
    "p90": 1.80,
    "mean": 0.31,
    "expected_shortfall_10": 1.20
  },
  "mfe_points": {"p50": 0.90, "p90": 2.40},
  "mae_points": {"p10": -1.40, "p50": -0.35},
  "model_uncertainty": 0.32,
  "n_sessions": 18,
  "authority": "rank_only",
  "model_version": "..."
}
```

概率事件必须互斥且尽量完备：

```text
arm first
premium stop first
time/hard exit before either
quote path censored
```

不能只预测 `p_arm` 而忽略 stop 和 censoring。

### 10.2 训练标签

复用已有：

```text
tp_armed
tp_before_stop
time_to_arm_seconds
mfe_points
mae_points
policy_pnl_points
exit_reason
quote_gap_seconds_max
```

同一 decision 下多个 width/right 候选必须按 `opportunity_id` 和 session 聚类；不得把每个候选行当独立市场机会随机切分。

### 10.3 Ablation

至少比较：

```text
A. existing path / regime / geometry / execution features
B. A + D2
C. B + D3 / adjacent-fly relative value
D. C + D4 / Mexican Hat
E. D + Richardson / Simpson / multi-scale / SNR
```

只有 session-level walk-forward 中 E 相对 A 稳定改善，才允许提出 rank-only 权重；不得仅凭 in-sample AUC 或单日收益提升。

---

## 11. Option Transformer challenger

Transformer 不等于“模型生成任意期权组合”。建议架构是候选查询整条链，而不是自由输出腿权重。

### 11.1 Strike / expiry token

每个 token 可包含：

```text
log_moneyness
minutes_to_expiry
right
bid / ask / relative_spread
IV / Delta / Gamma / Vega
OI / volume / GEX
quote_age
D2 at available scales
D3 / D4
Richardson diagnostics
Simpson local mass
quality / SNR
```

### 11.2 Candidate query token

```text
strategy_type
center / width / strikes
thesis_direction
spot-to-body distance
target / stop geometry
entry_ask / combo_spread
minutes_to_close
ManagementPolicy version
```

### 11.3 Market context token

```text
SPX / ES basis
VWAP / OR / ER / breadth
VIX / VIX1D
zero gamma / walls / value center
macro event state
```

### 11.4 输出

模型只输出第 10 节的路径 outcome forecast 与 uncertainty，不输出：

```text
自由 strike
自由 quantity
市价单
仓位规模
自动执行
```

### 11.5 权限

在校准通过前：

```text
authority = rank_only
```

它不能：

- 越过 deterministic hard gates；
- 把 `manual_authority_eligible=false` 改为 true；
- 覆盖 event/quote/data-quality gate；
- 直接否决所有候选；
- 生成新的多腿结构。

---

## 12. 确定性策略编译器：复杂理解，简单执行

未来选择器只能在 deterministic candidate factory 生成的有限集合中求最优：

\[
\max_{j\in\mathcal J\cup\{NoTrade\}}
\left[
\widehat E(Y_j)
-\lambda ES_{10,j}
-\eta Friction_j
-\rho Complexity_j
-\xi Uncertainty_j
\right]
\]

其中：

```text
NoTrade score = 0
```

硬约束：

```text
candidate 必须来自 versioned factory
max loss 已知
无裸露无限尾部
exact executable BBO ready
quote age / source skew 通过
leg count <= 当前批准策略上限
automatic_ordering = false
manual_action_only = true
account risk policy 通过
event / data-quality hard gate 通过
```

v1 当前候选仍为：

```text
Vertical
standard Butterfly
NoTrade
```

BWB、Condor 或其他结构需要独立设计与批准，不能因为 Transformer 能表示就自动加入。

三阶、四阶、Richardson、Simpson 虚拟组合仅作为输入特征和成本复杂度参考，不进入 `\mathcal J`。

---

## 13. 计算与 payload 尺寸约束

### 13.1 复用 exact five-point stencil

D3、D4、Mexican Hat、Richardson 和 local Simpson 都使用：

```text
K-2h, K-h, K, K+h, K+2h
```

实现必须对同一 center/scale 只读取和校验一次 five-point window，再派生所有字段；不得为每个算子重复构造 curve 或重复做 provider/parity 选择。

### 13.2 冗余字段标记

以下为严格相关组：

```text
D4
PeakVsShoulders
Mexican Hat
```

wire 可以同时保存以兼顾数学和经济可读性，但必须标注 dependency group。后续模型输入默认只选一个规范化数值，其他用于解释/审计，避免重复计权。

### 13.3 Context cap

保持主文档上限：

```text
references <= 6
scales <= 4
observations <= 24
```

新增 nested fields 不改变 observation 数量。若完整 JSON 超过实现期确定的 payload budget，应按以下顺序裁剪：

1. 删除重复可推导 raw aliases；
2. 保留规范化 D2/D3/D4、quality、noise/SNR；
3. 保留 Richardson only at h=5/10；
4. 保留 Simpson only at top reference centers；
5. 不扩大 reference/scale 上限。

不得通过把全 strike grid 塞入 decision 解决问题。

---

## 14. 测试增量

在主文档 SDCTX-2 / SDCTX-5 测试上追加恒等式测试。

### 14.1 Algebraic identities

对任意 five-point curve：

\[
2h^3D_3=B_h(K+h)-B_h(K-h)
\]

\[
h^4D_4=B_h(K-h)-2B_h(K)+B_h(K+h)
\]

\[
M_h=4B_h-B_{2h}=-h^4D_4
\]

\[
M_h=2h^2PeakVsShoulders
\]

\[
D_{2,R}=\frac{4D_2(h)-D_2(2h)}{3}
\]

\[
S_h=\frac{h}{3}\left[D_2(K-h)+4D_2(K)+D_2(K+h)\right]
\]

### 14.2 Polynomial exactness

- Richardson D2 对不超过五次多项式的二阶导数应满足相应 stencil 精度；
- D3 对三次项给出正确常数；
- D4 对四次项给出正确常数；
- Simpson 对低阶平滑 density proxy 的积分与解析值一致。

### 14.3 Noise invariants

- 任一 leg spread 扩大，operator noise bound 不下降；
- 所有 bid=ask 时 noise bound 为 0；
- Richardson gross weights 导致的 noise bound 不得被误用普通 D2 bound；
- strict dependency aliases 在同一输入上满足恒等式；
- missing K±2h 时 Richardson/Mexican/Simpson unavailable，但普通 D2 仍可用。

### 14.4 Non-interference

将 Richardson、Mexican Hat、Simpson 和 D3/D4 设置为极端正/负值，现有：

```text
decision type
candidate rank
gates
authority
execution
risk
```

仍必须完全一致。

---

## 15. 实施任务卡修订

主文档任务卡继续有效，SDCTX-2 扩充为：

```text
SDCTX-2a D2/D3/D4 five-point kernel
SDCTX-2b economic aliases: adjacent fly / fly curvature / Mexican Hat
SDCTX-2c Richardson paired-scale estimator + noise
SDCTX-2d local Simpson mass proxy + density disagreement diagnostic
```

不新增生产模块；仍由：

```text
src/spx_spark/analytics/options/density.py
```

持有纯数学实现。

SDCTX-3 wire 增加本附录字段；SDCTX-4 仍只把整个 context 复制进 `MarketFactPack`；SDCTX-5 增加第 14 节测试。

---

## 16. 本期依然不做

- 不实现完整 composite Simpson strip 的实时 payload；
- 不实现 Runge–Kutta 动态对冲；
- 不实现三阶、四阶、Richardson 或 Simpson 的真实组合订单；
- 不增加 strategy enum；
- 不修改 candidate factory / ranker / regime；
- 不训练 Transformer；
- 不创建自由组合生成器；
- 不改变 `ManagementPolicy` 参数；
- 不把任何 Q-side strike shape 当作 P-side 路径概率；
- 不把研究字段打印成人工执行理由。

---

## 17. 验收增量

除主文档验收门外，必须满足：

1. D3、D4、Mexican Hat、PeakVsShoulders 恒等式通过；
2. Richardson 与 h/2h pairing 完全因果、exact-strike、无外推；
3. Richardson 单独计算 noise bound/SNR；
4. local Simpson 只输出 state-price mass proxy，不冒充现实概率；
5. full composite Simpson 不进入每分钟 decision；
6. RK 不进入静态 strike context；
7. virtual portfolio gross units 和 raw coefficients 有冻结测试；
8. 新字段不改变任何生产策略结果；
9. 未来模型目标引用 `ManagementPolicy.policy_version`，不硬编码 stop；
10. Transformer/编译器仍为后续 challenger，自动下单边界不变。

---

## 18. 最终原则

我们不是因为数值方法名字漂亮，就把每一种 stencil 变成一张真实订单。

正确分层是：

```text
Call/Put quotes
  -> strike-domain numerical operators
  -> compact, quality-aware decision context
  -> causal replay and ManagementPolicy outcomes
  -> simple model baseline
  -> optional Transformer challenger
  -> deterministic simple candidate compiler
  -> manual decision or NoTrade
```

也就是：

```text
用复杂算子观察曲面；
用真实路径标签验证信息；
用简单、有限风险、可成交的结构表达；
没有样本外证据时，保持 NoTrade 和 context-only。
```
