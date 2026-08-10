# Strike Differential Context v1 · SDCTX-1–5 验收 · 2026-08-10

状态：**代码与冻结 fixture 验收通过；未部署；不构成交易 edge 证据。**

权威合同：

- `docs/spxw-strike-differential-decision-context-v1.md`
- `docs/spxw-strike-operator-extension-addendum-v1.md`（字段冲突时优先）
- `docs/strategy-signal-engine-v2.md`

## 实施范围

- SDCTX-1：RN density 与 local context 共用一份 typed OTM/parity synthetic Call curve。
- SDCTX-2a–2d：同一个 exact five-point window 派生 D2/D3/D4、经济别名、
  PeakVsShoulders、Mexican Hat、Richardson、local Simpson、独立 noise/SNR 和
  global-density disagreement。
- SDCTX-3：Options Map 传入 ATM、zero gamma、flip midpoint、put/call wall，并在
  global mode 得到后合并 q_mode reference；global 与 local quality 独立。
- SDCTX-4：`strategy_facts.py` 只复制完整 context 到
  `market_facts.structure.strike_differential_context`。
- SDCTX-5：增加数学、因果、质量、尺寸、序列化与四个冻结 session 的 non-interference
  测试。

未增加 composite Simpson strip、Runge–Kutta、候选类型、rank 输入、regime 输入、prompt、
自动下单、服务、timer、配置、store、schema/table 或 Rust contract。

## 数学与质量结果

确定性多项式 fixture 在 `K=100, h=5` 的结果：

| 检查 | 结果 |
|---|---:|
| Quartic fixture ordinary D2 | `0.02005`（保留中央差分的 `O(h²)` 项） |
| Cubic D3 | `0.0006` |
| Quartic D4 | `0.000024` |
| Richardson D2 | `0.0200000` |
| Quartic local Simpson raw proxy | `0.2015` |
| Quadratic-density local Simpson | `0.2000`（与解析积分一致） |
| Quintic Call 的 Richardson D2 | `0.0200025`（与解析二阶导数一致） |

测试冻结了以下严格恒等式：

```text
2 h³ D3 = B_h(K+h) - B_h(K-h)
h⁴ D4 = B_h(K-h) - 2 B_h(K) + B_h(K+h)
MexicanHat = 4 B_h(K) - B_2h(K) = -h⁴ D4
MexicanHat = 2 h² PeakVsShoulders
D2_R = (4 D2(h) - D2(2h)) / 3
```

质量边界结果：

- 任一相关腿 spread 扩大时 D3、D4、Richardson、Simpson noise bound 不下降；
- 全部 `bid == ask` 时各 operator noise bound 为 `0`；
- 缺 `K±2h` 时 ordinary exact-three-point D2 保留，D3/D4/Richardson/Mexican/Simpson
  fail closed；
- 缺 BBO 时数学值可保留，但 noise/SNR 为 `null`，quality 为
  `degraded_missing_bbo`；
- 负 central Butterfly `-1.0` 原样保留为 `D2=-0.04`，quality 为
  `blocked_convexity_violation`，不 clipping，也不继续生成 D3/D4；
- 任一所需 source quote 晚于 `as_of` 时 observation unavailable；若整条 global curve
  含未来点，q_mode reference 与 RN-CDF disagreement 均被抑制，避免间接 lookahead；
- 上限测试为 `6 references / 24 observations`，每个 observation 均有 quality/reasons。

虚拟组合 raw coefficients 与 gross units 已冻结：D2 `1,-2,1 / 4`；D3
`-1,2,0,-2,1 / 8`；D4 `1,-4,6,-4,1 / 16`；Mexican Hat
`-1,4,-6,4,-1 / 16`；Richardson `-1,16,-30,16,-1 / 64`；local Simpson
`1,2,-6,2,1 / 12`。

## RN density 非干扰

同一 41-strike Black–Scholes fixture 分别运行 legacy 与 enriched 路径；从 enriched
payload 移除新增 context 后，整个 `RnDensity.to_dict()` 与 legacy payload 严格相等。
既有 `options_map_pre_extraction.json` golden 也保持不变。该 fixture 的原有摘要仍为：

```text
quality=ok, median=7499.9, p10=7439.3, p90=7560.9,
mode=7500.0, clipped_mass_fraction=0.0
```

另一个五点 fixture 同时得到：global `insufficient_strikes`、local D2 可用，证明两者
quality 不互相覆盖。

## 冻结决策非干扰

对既有 2026-08-05..08 fixture，分别注入空 context、`+1e12` context 和 `-1e12`
context；移除新 market-fact 字段后，完整 `strategy_decision` 严格相等，包括 candidate /
opportunity id、candidate order、gate audit、authority、execution、risk 和 targets。

| Session | 冻结 decision_type | 注入前后 |
|---|---|---|
| 2026-08-05 | `NO_TRADE` | identical |
| 2026-08-06 | `CALL_BUTTERFLY` | identical |
| 2026-08-07 | `CALL_DEBIT_VERTICAL` | identical |
| 2026-08-08 | `NO_TRADE` | identical |

新增字段只存在于 `market_facts.structure`；本次 diff 未修改 candidate factory、ranker、
regime 或 LLM prompt。

## Readiness / SNR 注记

仓库内没有可用于本分支重新计算的生产 quote lake，因此本节只报告可复现的 41-strike、
10-point-grid Black–Scholes fixture，不冒充实盘分布：

```text
references=3, observations=12, status=partial
quality: unavailable_missing_strikes=6, degraded_low_snr=6
D2 SNR: n=6, min=1.1985, median=3.4550, max=8.3437
D3 SNR: n=6, min=0.0053, median=0.3546, max=2.6792
D4 SNR: n=6, min=0.0012, median=0.0127, max=0.3494
```

5/15-point observation 在 10-point grid 上按 exact-strike 规则 unavailable；没有插值。
该结果只说明高阶 stencil 的报价噪声放大符合预期。实盘 readiness/SNR 分布仍需后续从
point-in-time quote lake 研究，不能在本次 context-only 改动中解释为预测质量。

## 验证

父 Agent 验收（2026-08-10，Codex CLI 实现后接手）：

```text
uv run pytest -q tests/test_options_map.py tests/test_strategy_payoff.py \
  tests/test_exposure_map.py tests/test_strategy_contract.py \
  tests/test_strategy_outcomes.py tests/test_strategy_readiness.py
→ 195 passed

uv run ruff check <touched paths> → All checks passed
uv run lint-imports → Contracts: 2 kept, 0 broken
git diff --check → clean
rg strike_differential src → only density/models/service/strategy_facts consumers
```

本报告不以 service `active` 代替数据或策略验收。部署状态见提交后的生产验证。

## Complexity budget

| 项目 | 变化 |
|---|---:|
| Production files added / deleted | `0 / 0` |
| Production files modified | `4` |
| Net production LOC | `+548` |
| Test files modified / net test LOC | `2 / +511` |
| Dependencies added / removed | `0 / 0` |
| Config keys added / removed | `0 / 0` |
| Services/timers added / removed | `0 / 0` |
| Databases/tables added / removed | `0 / 0` |
| Rust contracts changed | `0` |
| Notification or strategy authority paths changed | `0` |
| Legacy paths removed | `0`（仅将原 private tuple curve 替换为 shared typed curve） |

残余风险：当前没有实盘 context readiness/SNR 语料；global density 仍保留既有 clipping /
normalization 语义，local operators 始终读取 raw curve，两者的 disagreement 只用于质量诊断。
