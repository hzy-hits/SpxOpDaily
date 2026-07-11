# SPX Spark 希腊字母与曝露代理指标定义（exposure_map 规范）

日期：2026-07-11
状态：规格文档（Phase 2 已实现；`exposure_map` / 单测 golden 必须与本文档公式一一对应）
适用模块：`src/spx_spark/features/exposure_map.py`、`src/spx_spark/options_map.py`（抽取后消费方）、`src/spx_spark/greek_shadow.py`（符号约定基准）

本文档定义 exposure_map 输出的全部指标：`gex`（现有）、`net_dex_proxy`、`dagex_proxy`、`vanna`、`charm`、`vex_proxy`、`cex_proxy`。
每个指标一节，固定小节结构：公式与输入字段 / dealer 符号约定 / 单位与 scaling / 按 expiry 聚合 / 更新频率 / 数据质量位影响 / 与 vendor 指标差异声明。

---

## 0. 全局约定（先读，后续每节引用）

### 0.1 命名铁律

- 凡是需要「dealer 持仓方向假设」或「vendor 未公开公式」的指标，字段名**必须带 `_proxy` 后缀**：
  `net_dex_proxy`、`dagex_proxy`、`vex_proxy`、`cex_proxy`。
- `gex` 沿用现有 `options_map.signed_gex` 名称与公式，不改名（它同样是代理，但历史输出已固化；
  其 JSON 输出必须携带 `sign_convention` 与 `weighting` 字段自我声明，见 0.3）。
- `vanna`、`charm` 是逐合约的 Black-Scholes 解析希腊，不含持仓方向假设，因此不带 `_proxy`；
  但它们的**曝露聚合版**（乘 OI/volume 权重后的 `vex_proxy`、`cex_proxy`）必须带。

### 0.2 输入来源字段（全部指标共用）

逐合约输入来自归一后的期权行（schema 见 `docs/steven-framework-integration.md` §1.1 `ExposureInputRow`），
字段与现有 `spx_spark.marketdata.Quote` 的映射：

| 输入 | 来源字段 | 说明 |
| --- | --- | --- |
| `strike` (K) | `quote.instrument.strike` | float，必须有限且 > 0 |
| `right` | `quote.instrument.right`（`C`/`P`） | 缺失则整行丢弃 |
| `expiry` | `quote.instrument.expiry`（`YYYYMMDD`） | 缺失记 `"unknown"` 并整行丢弃 |
| `iv` (σ) | `quote.greeks.implied_vol` | vendor IV（IBKR 或 Schwab），> 0 才可用 |
| `delta` | `quote.greeks.delta` | vendor delta，用于 `net_dex_proxy` |
| `gamma` | `quote.greeks.gamma` | vendor gamma，用于 `gex`/`dagex_proxy`，> 0 才可用 |
| `open_interest` (OI) | `quote.open_interest` | 缺失按 0 |
| `volume` (V) | `quote.volume` | 当日累计成交量，缺失按 0 |
| `spot` (S) | `options_map.select_underlier(state)`，退化时 `chain_implied_spot` | 与现有 options_map 完全一致的选取顺序 |
| `tau`（年） | `options_map.time_to_expiry_years(expiry, as_of=state.as_of)` | 到该 expiry 日历 session 收盘，下限 15 分钟 |
| 质量输入 | `quote.quality`、`quote.quote_age_ms(as_of)`、`quote.provider` | 见 0.6 |

vanna/charm 不用 vendor 数值，从 vendor IV 以 r=q=0 的 Black-Scholes 自算（0.5 节），
理由：IBKR/Schwab 都不下发二阶希腊；`greek_reference.py` 已有同款 BS 基建（`bs_delta`/`bs_gamma`/`_d1`）。

### 0.3 dealer 符号约定（与 `greek_shadow._signed_oi_gex_proxy` 一致）

统一采用现有 `SIGNED_GEX_METHOD = "call_positive_put_negative_oi_proxy_not_dealer_position"` 的家族约定：

1. **假设**：customer 净持仓方向近似为「买入并持有」——call 的持仓贡献取正号、put 的持仓贡献取负号；
   dealer 被假设为持有全体 customer 的对手方。这是行业常用粗近似，**不是**对 dealer 实际库存的估计。
2. 因此所有带符号曝露指标输出时必须携带自我声明字段（与 `_signed_oi_gex_proxy` 返回结构对齐）：
   - `sign_convention: "calls_positive_puts_negative"`
   - `dealer_position_sign: "unknown"`
   - `direction: "unknown"`
3. 各指标的符号落法：
   - `gex` / `dagex_proxy` / `vex_proxy` / `cex_proxy`：显式乘 `sign`（call 为 `+1.0`，put 为 `-1.0`），
     因为 gamma/vanna/charm 的 BS 解析值对 call 与 put 相同。
   - `net_dex_proxy`：**不额外乘 sign**，直接用 delta 的天然符号（call delta ∈ (0,1)，put delta ∈ (−1,0)），
     天然符号已经等价于「call 正、put 负」的家族约定。
4. 严禁在任何输出、告警或文本里把这些代理值解释为「dealer 多/空 gamma、dealer 多/空 delta」；
   只能表述为「按自家 call 正 put 负约定的持仓加权敏感度」。

### 0.4 权重的两个版本（oi_weighted 与 volume_weighted 并存）

每个曝露指标都要同时输出两个权重版本，命名固定：

| 版本 | 权重 w | 视角 | 现有对应 |
| --- | --- | --- | --- |
| `oi_weighted` | `w = open_interest`（缺失按 0；w ≤ 0 时该腿记 None） | 隔夜库存 / positioning | `options_map.gex_weight(intraday=False)` |
| `volume_weighted` | `w = volume`（缺失按 0；w ≤ 0 时该腿记 None） | 当日 0DTE 压力（Steven 的 DAGEX 思想） | 新增 |

现有 `options_map` 0DTE intraday 模式的 `oi_plus_volume` 权重（`gex_weight(intraday=True)`）
是第三种内部兼容权重，exposure_map 必须继续提供以保证 options_map 抽取后 golden 一致，
但**不进入** Steven 层的公开指标表（Steven 只消费 `oi_weighted` 与 `volume_weighted`）。

### 0.5 Black-Scholes 解析式（r = q = 0，与 `greek_reference.MODEL_NAME = "bs_r0_q0"` 一致）

记 `m = ln(S/K)`，`τ` 为年化剩余时间：

```text
d1 = (m + 0.5·σ²·τ) / (σ·√τ)
d2 = d1 − σ·√τ
φ(x) = exp(−x²/2)/√(2π)          # 标准正态密度
N(x)                              # 标准正态 CDF
gamma_bs   = φ(d1) / (S·σ·√τ)                     # call = put
delta_call = N(d1)；delta_put = N(d1) − 1
vanna_bs   = −φ(d1)·d2 / σ                        # ∂delta/∂σ，per 1.00 vol，call = put
charm_bs   = φ(d1)·d2 / (2τ)                      # ∂delta/∂t（日历时间前进方向），per 年，call = put
```

charm 推导注记：`∂d1/∂τ = −d2/(2τ)`，日历时间 t 前进时 τ 减小，
故 `∂delta/∂t = φ(d1)·(−∂d1/∂τ) = φ(d1)·d2/(2τ)`。r=q=0 下 call 与 put 的 charm 相同。
边界：`S ≤ 0` 或 `K ≤ 0` 或 `σ ≤ 0` 或 `τ ≤ 0` 时，vanna/charm 一律返回 None（该合约不进曝露聚合），
与 `options_map.bs_gamma` 的 None 语义一致。

### 0.6 数据质量位（随行携带，逐 expiry 汇总）

exposure_map 的每个 expiry 输出块必须携带三个质量位；取值与降级规则：

| 质量位 | 枚举 | 判定 |
| --- | --- | --- |
| `oi_quality` | `ibkr_ok` / `schwab_unverified` / `stale_or_zero` / `missing` | 该 expiry 参与聚合的行里 OI>0 的行为主提供方是 IBKR → `ibkr_ok`；主提供方是 Schwab（盘中 OI 验收未完成期间）→ `schwab_unverified`；OI 全为 0 或快照为非交易日残留 → `stale_or_zero`；无 OI 字段 → `missing` |
| `iv_source` | `vendor_ibkr` / `vendor_schwab` / `mixed` / `missing` | 按参与聚合行的 IV 提供方多数决定；两方都有 → `mixed`；IV 覆盖率（`with_iv/total`）< 0.5 → `missing` |
| `snapshot_age_seconds` | float | 参与聚合行的 `quote_age_ms` 最大值 / 1000（None 视为超限） |

质量位对输出的强制影响（下游不得绕过）：

1. 行级门槛沿用现有函数：gamma 取 `options_map.option_gamma_structural`（结构特征允许 ≤ `STRUCTURE_MAX_AGE_SECONDS = 900` 秒的 stale 样本），IV 取 `options_map.option_iv`，delta 取 `options_map.usable_delta`。硬坏质量（`MISSING`/`ERROR`/`UNKNOWN`）一律整行拒绝。
2. `oi_quality ∈ {stale_or_zero, missing}` → 该 expiry 所有 `oi_weighted` 指标输出 None，`quality` 记 `"no_open_interest"`；`volume_weighted` 版本不受影响。
3. `oi_quality == schwab_unverified` → `oi_weighted` 数值照常输出，但 expiry 级 `warnings` 必须追加 `"schwab_oi_unverified"`，Steven 层 confidence 因此封顶 `low`（见 integration 文档 §2）。
4. `iv_source == missing` → vanna/charm/`vex_proxy`/`cex_proxy` 输出 None（它们完全依赖 IV）；`gex`/`net_dex_proxy`/`dagex_proxy` 仍可用 vendor gamma/delta 输出。
5. `snapshot_age_seconds > 900` → 整个 expiry 块 `quality = "unavailable"`，全部指标 None。下游（Steven）只能出 `regime = "unknown"`、`status = "invalid"` 或 `observe_only`。

### 0.7 通用 scaling 约定

沿用 `options_map.signed_gex` 的「每 1% 标的变动的美元敏感度」口径：
合约乘数固定 100（SPXW），1% 变动因子写作 `0.01`。各指标的具体量纲见各节。
所有输出为 Python float，不做四舍五入；展示层再截断。

### 0.8 golden 数值示例的公共输入（供 Phase 2 数值测试直接引用）

固定输入（构造 Quote 时把 vendor gamma/delta 设为下表的 BS 值，保证 vendor 路径与自算路径一致）：

```text
S = 7500.0，σ = 0.20，τ = 0.01 年（所有合约同一 σ、τ）
strike 7500：call OI=1000, call V=500,  put OI=800, put V=2000
strike 7550：call OI=600,  call V=1500, put OI=200, put V=100
```

逐合约 BS 中间量（erf 实现，双精度）：

| K | d1 | d2 | gamma_bs | delta_call | delta_put | vanna_bs×0.01（per vol point） | charm_bs / 525600（per 分钟） |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 7500 | 0.010000000000 | −0.010000000000 | 0.002659482225 | 0.503989356315 | −0.496010643685 | 0.000199461167 | −3.79492326661e−07 |
| 7550 | −0.322227135933 | −0.342227135933 | 0.002525063695 | 0.373640314205 | −0.626359685795 | 0.006481089873 | −1.23308407015e−05 |

各指标的 golden 期望值列在各节末尾；expiry 聚合汇总表见 §8。

---

## 1. `gex`（现有指标，公式固化）

### 公式与输入

逐腿（`sign`：call `+1.0`，put `−1.0`；`w` 为 0.4 节权重）：

```text
gex_leg = sign × gamma × w × 100 × S × S × 0.01
```

`gamma` 用 vendor gamma（`option_gamma_structural`）。与现有 `options_map.signed_gex` 完全同式。
按 strike 汇总：`call_gex = Σ call 腿`，`put_gex = Σ put 腿`，`net_gex = call_gex + put_gex`，
`abs_gex = |call_gex| + |put_gex|`（对应现有 `StrikeGex` 字段）。

### 符号约定

0.3 节家族约定。`sign_convention = "calls_positive_puts_negative"`，dealer 方向 unknown。

### 单位与 scaling

美元 gamma / 1% 标的变动（USD per 1% move），合约乘数 100。

### 按 expiry 聚合

`net_gex = Σ_strikes net_gex`；`abs_gex = Σ_strikes abs_gex`；
`net_gamma_ratio = net_gex / abs_gex`（`abs_gex ≤ 0` 时 None）。与现有 `ExpiryOptionsMap` 一致。

### 更新频率

随 exposure_map 重算：service loop 每轮（约 5s，IBKR 热窗行）；Schwab 全链行按其采集档速（5s/15s）刷新输入。

### 质量位影响

0.6 节规则 1、2、5。`oi_weighted` 版在 OI 缺失时为 None；`volume_weighted` 版只需 volume。

### 与 vendor 差异声明

与 SpotGamma/SqueezeMetrics 等的 GEX 不可比：他们可能使用 dealer 库存模型、不同乘数、不同 spot 口径。
本值仅是 OI/volume 加权的 |gamma| 有向和。

### golden（strike 级）

| K | 权重 | call_gex | put_gex | net_gex | abs_gex |
| --- | --- | --- | --- | --- | --- |
| 7500 | oi | 149595875.169781 | −119676700.135825 | 29919175.033956 | 269272575.305605 |
| 7500 | volume | 74797937.584890 | −299191750.339562 | −224393812.754671 | 373989687.924452 |
| 7550 | oi | 85220899.703143 | −28406966.567714 | 56813933.135429 | 113627866.270858 |
| 7550 | volume | 213052249.257858 | −14203483.283857 | 198848765.974001 | 227255732.541715 |

---

## 2. `net_dex_proxy`（OI/volume 加权 delta 曝露代理）

### 公式与输入

逐腿（delta 用 vendor delta，`usable_delta`；不乘额外 sign，见 0.3）：

```text
dex_leg = delta × w × 100 × S × 0.01
```

按 strike：`call_dex_proxy = Σ call 腿`，`put_dex_proxy = Σ put 腿`，
`net_dex_proxy = call_dex_proxy + put_dex_proxy`。

### 符号约定

delta 天然符号（call 正、put 负）等价于家族约定。含义：**假设 customer 全体净持有**这些合约时，
其组合的美元 delta / 1% 变动；dealer 假设持对手方，但输出不翻转符号、不声称 dealer 方向。

### 单位与 scaling

美元 delta / 1% 标的变动（USD per 1% move）。
（说明：`× S × 0.01` 与 gex 的 `× S² × 0.01` 相比少一个 S，因 delta 本身无量纲、gamma 是 per point。）

### 按 expiry 聚合

`net_dex_proxy = Σ_strikes net_dex_proxy`；另输出
`net_dex_ratio_proxy = net_dex_proxy / (|call_dex_proxy| + |put_dex_proxy|)`（分母 ≤ 0 时 None）。
**跨到期版本**：`net_dex_proxy_by_expiry` 是 `{expiry: net_dex_proxy}` 映射，Steven regime 层的直接输入。

### 更新频率

同 gex（每轮 5s；输入按各车道档速）。

### 质量位影响

0.6 节规则 1、2、3、5。delta 缺失的腿跳过并计入该 expiry 的 `delta_coverage_ratio`
（`with_delta/total`，< 0.5 时该 expiry 的 net_dex_proxy 记 None 且追加 warning `"low_delta_coverage"`）。

### 与 vendor 差异声明

**这不是任何 vendor（含 Steven 引用的）Net DEX**。vendor 公式未知，可能含 dealer 库存估计、
成交方向推断（signed flow）或不同权重。本代理只用 OI/volume 无方向权重，任何回测结论只对本代理成立。

### golden（strike 级）

| K | 权重 | net_dex_proxy |
| --- | --- | --- |
| 7500 | oi | 803856.310248 |
| 7500 | volume | −5550199.569101 |
| 7550 | oi | 741841.885229 |
| 7550 | volume | 3733683.770458 |

---

## 3. `dagex_proxy`（volume 加权 gamma 曝露，Steven 的 DAGEX 思想）

### 公式与输入

**定义**：`dagex_proxy ≡ gex 的 volume_weighted 版本的 expiry 级 net 值`。不引入新逐腿公式：

```text
dagex_proxy(expiry) = net_gex_volume_weighted(expiry)
dagex_ratio_proxy(expiry) = net_gamma_ratio_volume_weighted(expiry)
```

同时在 expiry 块输出背离度量（Steven 用法的核心是「volume 加权 vs OI 加权的背离」）：

```text
gex_weighting_divergence = net_gamma_ratio_volume_weighted − net_gamma_ratio_oi_weighted
```

（任一侧为 None 时 divergence 为 None。）

### 符号约定 / 单位与 scaling

同 §1 gex（call 正 put 负；USD per 1% move；ratio 无量纲 ∈ [−1, 1]）。

### 按 expiry 聚合

天然就是 expiry 级定义；strike 级明细直接复用 gex 的 volume_weighted 行。

### 更新频率

同 gex。注意 volume 是**当日累计**，开盘初期本指标噪声大：
09:30–10:00 ET 之间输出必须追加 warning `"early_session_low_volume"`
（判定：`state.as_of` 的 ET 时间在 session open 后 30 分钟内）。

### 质量位影响

不依赖 OI，`oi_quality` 不影响本值；但 `gex_weighting_divergence` 需要 OI 侧，OI 不可用时为 None。
0.6 节规则 1、5 适用。

### 与 vendor 差异声明

Steven 语境下的 “DAGEX” 无公开公式；本代理仅是「当日累计成交量加权的 GEX」，
不含开平仓区分（volume 同时含开仓与平仓，是有意的粗近似，与 `gex_weight` 注释一致）。

### golden（expiry 级，两 strike 合计）

`dagex_proxy = −25545046.780670`，`dagex_ratio_proxy = −0.042486887902`，
OI 侧 `net_gamma_ratio = 0.226516082907`，
`gex_weighting_divergence = −0.042486887902 − 0.226516082907 = −0.269002970809`。

---

## 4. `vanna`（逐合约解析希腊，无持仓权重）

### 公式与输入

0.5 节解析式，输入为 vendor IV、spot、strike、tau：

```text
vanna_per_vol_point = (−φ(d1)·d2 / σ) × 0.01
```

`× 0.01` 把「per 1.00 vol」换算成「per 1 vol point（1%）」，
与 `greek_reference.ContractGreekReference.vanna_delta_per_vol_point` 同单位。call 与 put 相同。

### 符号约定

无持仓假设，纯模型敏感度：IV 上升 1 个 vol point 时该合约 delta 的变化量。
OTM call（d2 < 0）为正。不涉及 dealer。

### 单位与 scaling

delta / vol point（无量纲 delta 每 1% IV）。

### 按 expiry 聚合

逐合约值不聚合（聚合版是 §6 `vex_proxy`）。exposure_map 的 strike 行携带
`call_vanna_per_vol_point` / `put_vanna_per_vol_point`（BS 下两者相等，仍分开存以便未来换模型）。

### 更新频率

同 gex；IV 来源行刷新时重算。tau 用 `time_to_expiry_years`，随 as_of 连续衰减。

### 质量位影响

依赖 IV：`iv_source == missing` 或该腿 `option_iv` 为 None → None。0.6 节规则 4、5。

### 与 vendor 差异声明

r=q=0 假设与 vendor 模型（含利率、股息、美式修正）不同；输出必须携带 `model: "bs_r0_q0"`
与所用 IV 的来源（`iv_source`）和时间戳（`as_of`）。

### golden（逐合约）

见 0.8 表：K=7500 → 0.000199461167；K=7550 → 0.006481089873（call = put）。

---

## 5. `charm`（逐合约解析希腊，无持仓权重）

### 公式与输入

```text
charm_per_minute = (φ(d1)·d2 / (2τ)) / 525600
```

`525600 = 365×24×60`（年 → 分钟），与 `greek_reference.charm_delta_per_minute` 同单位。call 与 put 相同。

### 符号约定

纯模型敏感度：日历时间每过 1 分钟该合约 delta 的变化量。ATM 附近（d2<0）为负。不涉及 dealer。

### 单位与 scaling

delta / 分钟。

### 按 expiry 聚合

不聚合（聚合版是 §7 `cex_proxy`）。strike 行携带 `call_charm_per_minute` / `put_charm_per_minute`。

### 更新频率

同 vanna。0DTE 尾盘 τ→下限（15 分钟 floor，`_MIN_TIME_TO_EXPIRY_YEARS`）时数值发散，
τ 触 floor 的合约必须追加行级 warning `"tau_floored"` 且不进 `cex_proxy` 聚合。

### 质量位影响

同 vanna（依赖 IV）。

### 与 vendor 差异声明

同 vanna；另注意 vendor 的 charm 常按「每天」计，本项目按「每分钟」，不可直接比数量级。

### golden（逐合约）

K=7500 → −3.79492326661e−07；K=7550 → −1.23308407015e−05（call = put）。

---

## 6. `vex_proxy`（vanna 曝露代理）

### 公式与输入

逐腿（`sign`：call `+1.0`，put `−1.0`）：

```text
vex_leg = sign × vanna_per_vol_point × w × 100 × S × 0.01
```

按 strike / expiry 求和得 `vex_proxy`。

### 符号约定

0.3 节家族约定（BS 下 vanna call=put，必须显式乘 sign）。含义：按 call 正 put 负约定的持仓组合，
IV 每升 1 vol point、标的每动 1% 的美元 delta 变化代理。dealer 方向 unknown。

### 单位与 scaling

USD delta 变化 /（vol point × 1% move）。

### 按 expiry 聚合

`vex_proxy(expiry) = Σ_strikes Σ_legs vex_leg`，双权重版本并存。

### 更新频率

同 gex。

### 质量位影响

同时依赖 IV（0.6 规则 4）与权重（规则 2、3）；任一缺失该腿为 None，
expiry 级 `with_iv/total < 0.5` 时整个 expiry 的 vex_proxy 为 None。

### 与 vendor 差异声明

vendor 的 “VEX/Vanna Exposure” 公式未知（可能用 vega·d2 口径或 dealer 库存符号）。
本代理只是家族约定 + BS vanna 的加权和，**只进 Steven 的 warnings/解释文本，不参与 regime 判定**
（hard gate 2，见 integration 文档）。

### golden（strike / expiry 级）

| K | 权重 | vex_proxy |
| --- | --- | --- |
| 7500 | oi | 299.191750 |
| 7500 | volume | −2243.938128 |
| 7550 | oi | 19443.269618 |
| 7550 | volume | 68051.443663 |
| **expiry 合计** | oi | **19742.461368** |
| **expiry 合计** | volume | **65807.505536** |

---

## 7. `cex_proxy`（charm 曝露代理）

### 公式与输入

逐腿（`sign`：call `+1.0`，put `−1.0`）：

```text
cex_leg = sign × charm_per_minute × w × 100 × S × 0.01
```

### 符号约定

同 §6（charm call=put，显式乘 sign）。含义：按家族约定的持仓组合，
日历时间每过 1 分钟、标的每动 1% 的美元 delta 衰减代理。dealer 方向 unknown。

### 单位与 scaling

USD delta 变化 /（分钟 × 1% move）。

### 按 expiry 聚合

同 §6，双权重并存。`tau_floored` 的合约不进聚合（§5）。

### 更新频率

同 gex；尾盘发散限制见 §5。

### 质量位影响

同 §6。

### 与 vendor 差异声明

vendor “CEX/Charm Exposure” 公式未知；本代理仅进 warnings/解释文本，不参与 regime 判定（hard gate 2）。

### golden（strike / expiry 级）

| K | 权重 | cex_proxy |
| --- | --- | --- |
| 7500 | oi | −0.5692384900 |
| 7500 | volume | 4.2692886749 |
| 7550 | oi | −36.9925221044 |
| 7550 | volume | −129.4738273653 |
| **expiry 合计** | oi | **−37.5617605944** |
| **expiry 合计** | volume | **−125.2045386903** |

---

## 8. golden 汇总表（expiry 级，0.8 输入）

| 指标 | oi_weighted | volume_weighted |
| --- | --- | --- |
| `net_gex` | 86733108.169385 | −25545046.780670 |
| `abs_gex` | 382900441.576463 | 601245420.466167 |
| `net_gamma_ratio` | 0.226516082907 | −0.042486887902 |
| `net_dex_proxy` | 1545698.195477 | −1816515.798643 |
| `dagex_proxy` | —（定义即右列） | −25545046.780670 |
| `vex_proxy` | 19742.461368 | 65807.505536 |
| `cex_proxy` | −37.5617605944 | −125.2045386903 |

数值容差建议：`pytest.approx(rel=1e-9)`（全部为解析式 + 双精度求和，无迭代）。

---

## 9. 差异声明的输出义务（实现检查项）

exposure_map 序列化输出（JSON）时，每个 expiry 块必须包含：

```json
{
  "method": "call_positive_put_negative_oi_proxy_not_dealer_position",
  "sign_convention": "calls_positive_puts_negative",
  "dealer_position_sign": "unknown",
  "direction": "unknown",
  "model": "bs_r0_q0",
  "proxy_disclaimer": "all *_proxy metrics are house-defined; not comparable to any vendor metric of similar name"
}
```

这些字段是 hard gate 2 的机器可查证据，Phase 3 测试会断言它们存在。
