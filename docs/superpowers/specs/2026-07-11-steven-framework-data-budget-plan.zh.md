# SPX Spark 规划：数据预算分配 + Steven 框架 / 高阶希腊落地

日期：2026-07-11
状态：规划文档（未实施）
范围：只写计划与架构，不含代码改动。

---

## 0. 目标一句话

在「IBKR 约 100 条行情行 + Schwab 120 请求/分钟、单批 500 symbol」的预算内，
把 **IBKR 做成 SPX 0DTE 的秒级快车道**，把 **Schwab 做成全量/广度慢车道**
（全链、多到期、DJI/NDX/RUT/SPY 参考），并在这套数据之上以
**observe_only 引导层** 的形式落地 Steven SPX Options Framework 与
0DTE 高阶希腊（vanna/charm/DAGEX 类）。

---

## 1. 现状基线（已核实，2026-07-11）

### 1.1 IBKR 流（client 172，5s flush）

| 用途 | 行数 | 配置来源 |
| --- | --- | --- |
| 基础锚 `index:SPX` + `future:ES` | 2（常驻） | `ibkr_stream` base subs |
| SPXW 0DTE 期权 | 60（常驻；hot 70% = 42 行 ≈ ATM ±50 点 @5 点距 × call/put） | `ibkr_stream.max_option_lines=60`, `hot_lane_share=0.7`, `hot_window_points=50` |
| SPY 期权（墙对照） | 16（常驻） | `ibkr_stream.spy_option_lines=16` |
| VIX 家族 + 19 个 ETF/指数 | 轮换（slow poll，每 300s 借行 10s 后释放） | `slow_poll_labels`, `slow_poll_interval_seconds=300` |

常驻合计约 78 行，slow poll 峰值再借几行，距 100 行上限有约 15–20 行余量。
replan（SPX 漂移 20 点触发重排期权订阅）时新旧订阅短暂重叠，也吃这块余量。

### 1.2 Schwab REST（120 req/min，quotes 单批上限 500 symbol）

当前 5s 一轮：1 次 quotes 批（38 个 symbol，含 $SPX/$NDX/$RUT/$DJI/$VIX 家族/ETF）
+ 5 次链（$SPX、$XSP、SPY、QQQ、IWM，`strikeCount=10`）= 6 req/轮 ≈ 72/min。
500 symbol 批上限只用了 38，加 symbol 几乎免费；贵的是**每条链 = 1 个请求**。

### 1.3 已有可复用模块

| 模块 | 现有能力 | 对 Steven 框架的可复用面 |
| --- | --- | --- |
| `options_map` | 按 strike 的 GEX、call/put 墙、gamma 翻转、`net_gamma_ratio`、`gamma_state` | 直接充当「Map 层」的 GEX 部分 |
| `greek_shadow` | 0DTE greeks 影子采样，已有 `_signed_oi_gex_proxy`（自家符号约定的 GEX 代理） | Net DEX 代理可照此模式扩展；research_shadow_only 的先例 |
| `strategy/micopedia.py` | regime 分类 → map focus → trigger watchlist → 表达族 → 失效检查 → 数据警告，observe_only | **Steven 模块的结构模板**（几乎逐层对应） |
| `order_map` | ES 量价事件、Hyperliquid aggressor 流 | 「Flow 层」的现货侧代理 |
| `market_context` | 已消费 `index:NDX/RUT/DJI` 标签 | 参考指数已有落点，不需要新架构 |
| `intraday_shock` | 5s 冲击检测（IBKR 快车道锚） | 「Trigger 层」的价格反应来源 |
| `market_calendar` / 事件相位 | session/event phase | 状态机的 `EVENT_WAIT` 输入 |
| `post_close_review` | 盘后复盘 | episode 审计的挂载点 |
| `human_focus` / `alert_engine` / notifier | 告警分级与推送 | hard gate：Steven 输出只能作为 context，不得抬 severity |

### 1.4 已知数据缺口（决定框架只能 observe_only）

1. **signed option flow / aggressor tape**：Schwab 与 IBKR L1 都不提供，需另购 OPRA 级数据源。
2. **vendor Net DEX / DAGEX / VEX / CEX 公式未知**：只能自建「命名清晰的代理」，不能冒充原指标。
3. **Schwab SPXW OI 待交易时段验收**（周末样本常为 0）；IBKR OI 可靠。

---

## 2. 数据预算分配方案

### 2.1 角色分工（回答"IBKR 快车道、嘉信全量"）

```text
IBKR  = 秒级快车道：SPX/ES 锚 + SPXW ATM 热窗 + SPY 墙对照
        服务对象：intraday_shock、trigger 检测、ATM 高阶希腊的高频重算
Schwab = 广度慢车道：全 symbol 报价批 + 多到期全链
        服务对象：exposure map（GEX/DEX 代理墙）、multi-expiry regime、
        DJI/NDX/RUT/SPY 参考、盘前盘后 ETF 情绪
```

原则：**凡是 Schwab 一批 quotes 能带回来的，绝不占 IBKR 行**。
IBKR 行只留给「必须 5 秒级、必须逐合约」的东西。

### 2.2 IBKR 100 行分配（建议值）

| 项 | 现状 | 建议 | 说明 |
| --- | --- | --- | --- |
| `index:SPX` + `future:ES` | 2 | 2 | 官方锚，不动 |
| SPXW 0DTE 期权行 | 60 | **68**（hot_lane_share 0.7 → ≈48 行热窗） | 热窗从 ±50 扩到 ±55~60 点（5 点距 × 双边），0DTE 午后 gamma 大时少换订阅 |
| 次日到期 SPXW（term 结构） | 含在 60 内（±10 热窗） | 维持 | `next_expiry_hot_window_points=10` 已够 regime 对照 |
| SPY 期权 | 16 | 16 | 墙对照继续，SPY 期权流动性面广，作为 SPX 链的旁证 |
| slow poll 轮换借行 | ~5 峰值 | ~5 | VIX 家族/ETF 维持 300s 轮换 |
| **余量（replan 重叠 + 突发）** | ~17 | **≥9** | replan 时新旧订阅重叠是刚性开销，余量不建议压到 5 以下 |

**明确不做**：DJI/NDX/RUT 指数与期权不占 IBKR 行。指数报价 Schwab quotes 批已含
（`$DJI/$NDX/$RUT`），`market_context` 的标签由 Schwab 或 slow poll 供给即可。
NDX/RUT 期权结构如需参考，用 QQQ/IWM 链（Schwab 已采）代理——这也是行业惯例，
流动性和 strike 粒度都比直接采 NDX/RUT 期权好。

### 2.3 Schwab 120 req/min 分层配置（建议值）

改「所有链同频」为**三档节奏**（cadence tier），预算立刻富余：

| 档 | 内容 | 周期 | req/min |
| --- | --- | --- | --- |
| A：核心 | quotes 全批（38→可扩到 60+ symbol，仍是 1 req）+ `$SPX` 链 | 5s | 24 |
| B：结构 | SPY、QQQ、IWM、XSP 链 | 15s | 16 |
| C：参考 | `$NDX`、`$RUT` 链（可选，先一次性采样评估价值再决定常采） | 60s | 2 |
| 预留 | 重试、盘中手动查询、0DTE 尾盘加密采样 | — | **≥78** |

配套两个调整：

1. **`$SPX` 链 `strikeCount` 从 10 提到 ~40**：现在 10 档只覆盖 ATM ±25 点左右，
   不够画 ±150~200 点的 GEX/DEX 墙。提档后单响应变大，Phase 1 需实测延迟
   （预期仍在 1s 内）；若过大再按 expiration 拆分请求。
2. **quotes 批加 symbol 免费**：把未来 exposure map 需要的现货参考
   （如 ES 对应的 /ES 无法走 marketdata，则维持 IBKR future:ES）尽量塞进这一批。

尾盘 0DTE 模式（可选开关）：14:30 ET 后把 `$SPX` 链提到 3s 一轮（+8/min），
其余档不变，总量仍 <60/min。

### 2.4 各数据面的最终归属表

| 数据 | 来源 | 频率 | 消费方 |
| --- | --- | --- | --- |
| SPX/ES 官方锚 | IBKR 流 | 5s flush | shock、trigger、bar builder |
| SPXW ATM 期权 L1+greeks | IBKR 流（68 行） | 5s | 高阶希腊快车道、trigger |
| SPXW 全链（±100~200 点、多到期） | Schwab `$SPX` 链 | 5s | exposure map、regime |
| SPY/QQQ/IWM/XSP 链 | Schwab | 15s | 墙对照、NDX/RUT 结构代理 |
| DJI/NDX/RUT 指数报价 | Schwab quotes 批 | 5s | market_context 宽度参考 |
| VIX 家族 / ETF 情绪 | Schwab quotes 批（主）+ IBKR slow poll（备） | 5s / 300s | regime、tail-protection 判断 |
| OI | IBKR 期权行（可靠）+ Schwab 链（待验收） | 每日+盘中 | GEX/DEX 代理权重 |
| 现货 aggressor 流代理 | ES 量价（IBKR）+ Hyperliquid | 已有 | Flow 层弱确认 |
| 期权 signed flow | **无**（缺口） | — | 仅体积代理，低置信 |

---

## 3. Steven 框架落地架构

### 3.1 总体形态：一条 observe_only 引导管道

沿用 micopedia 的先例：**纯函数式信号模块 + latest 状态文件 + 盘后审计**，
不进入执行栈，不抬告警 severity。

```text
collector 层（现有，不动）
  IBKR 流 / Schwab REST / order_map / market_calendar
        │
featur 层（新增 1 个模块 + 少量抽取）
  features/exposure_map.py        ← 按 expiry×strike 的希腊曝露表（本计划核心新件）
  options_map（现有，GEX 墙部分逻辑抽取进 exposure_map 后薄化）
        │
strategy 层（新增 1 个模块）
  strategy/steven.py              ← regime→map→flow→trigger→expression 状态机
        │
输出层（现有管道复用）
  latest/steven_state.json        ← Guidance Contract v0.1 JSON
  data lake episodes JSONL        ← daily thesis episode 记录
  human_focus / alert_engine      ← 只作 context 附注（hard gate）
  post_close_review               ← episode 盘后 forward metrics 审计
```

### 3.2 新模块一：`features/exposure_map.py`

职责：把 IBKR 期权行 + Schwab 链归一成一张
**expiry × strike × {gex, dex_proxy, vanna, charm, vex_proxy, cex_proxy, oi, volume, iv}** 的表，
并聚合出墙/翻转/加速区候选。这是 options_map、steven、order_map 共用的地基。

关键设计决定：

1. **所有 DEX 家族指标都带 `_proxy` 后缀**，绝不与 vendor 指标同名。
   符号约定统一写死并文档化：假设「customer 净持仓方向 = OI 变化方向，
   dealer 持对手方」，与 `greek_shadow._signed_oi_gex_proxy` 现有约定一致。
2. **两个权重版本并存**：`oi_weighted`（隔夜库存视角）与 `volume_weighted`
   （当日 0DTE 压力视角，对应 Steven 的 DAGEX 思想——GEX 受 OI 支配会漏掉
   当日 0DTE 成交量，需看 volume 加权版与 OI 加权版的背离）。
3. vanna/charm 从 vendor IV + Black-Scholes 自算（greek_shadow 已有 BS 基建），
   注明输入 IV 来源与时间戳。
4. 数据质量位随行携带：`oi_quality`（IBKR ok / Schwab 未验收 / 周末 stale）、
   `iv_source`、`snapshot_age`。质量不达标 → 下游只能出 `unknown`。

### 3.3 新模块二：`strategy/steven.py`

结构逐层对照 micopedia，输出严格贴 Guidance Contract v0.1：

| 框架层 | 输入（本项目实际可得） | 输出字段 |
| --- | --- | --- |
| 1 Regime | exposure_map 的 `net_dex_proxy_by_expiry`（多到期） | `regime`, `regime_breadth`；**因是自家代理，confidence 封顶 `low\|medium`** |
| 2 Map | exposure_map 墙/翻转/加速区 + 多到期共振 | `map.support/resistance/pin/acceleration` |
| 3 Cross-Greek | volume 加权 vs OI 加权背离、vanna/charm | 只进 `warnings` 与解释文本（hard gate 2：定义未知/代理 → context only） |
| 4 Flow | ES/Hyperliquid aggressor + 期权 volume delta（弱） | `flow_confirmation`，质量标注 `weak_proxy` |
| 5 Trigger | intraday_shock + 1m/5m bar 的 hold/reclaim/accept/reject 判定 | `price_trigger`；无 trigger 永远停在 `watch`（hard gate 3） |
| 6 Expression | 规则映射到有限最大亏损的策略族 | `expression_family`（仅解释，hard gate 7） |
| 7 Exit/行为 | target 达成 / 失效 → `EXIT_REVIEW` → `LOCKOUT_OR_REMAP` | 状态机字段 + episode 关单 |

状态机（DATA_INVALID → OBSERVE_ONLY → *_WATCH → SETUP_CONFIRMED → EXIT_REVIEW
→ LOCKOUT_OR_REMAP）持久化在 `latest/steven_state.json`，边沿变化追加写
episodes JSONL（一天一个 thesis episode，多次修订合并，不按条计数）。

### 3.4 缺失输入的显式降级

Guidance Contract 要求的输入与现实对照：

| 契约输入 | 现状 | 降级策略 |
| --- | --- | --- |
| `official_spx_spot` | ✅ IBKR `index:SPX` | — |
| `spx_spot_1m_and_5m_bars` | ❌ 无 bar builder | **Phase 2 新增**：从 5s 快照聚合 1m/5m bar（纯内存 + latest 落盘，改动小） |
| `net_dex_by_expiry` | ❌ vendor 指标 | 用 `net_dex_proxy`，confidence 封顶，输出 warning |
| `gex_by_strike_and_expiry` | ✅ options_map → exposure_map | — |
| `dagex/vex/cex` | ❌ | volume 加权代理 + vanna/charm，context only |
| `trade_flow_by_strike_and_side` | ❌ 无 OPRA | volume delta 弱代理 + ES/HL 现货 aggressor，`flow_confirmation` 最高只到 `weak` |
| `open_interest_and_intraday_volume` | ✅ IBKR / ⚠ Schwab | 质量位驱动 |
| `event_phase` / `market_session_phase` | ✅ market_calendar | — |

对应七条 hard gate 全部可执行：锚缺失/过期 → `invalid`；代理指标 → context only；
无 trigger → `watch`；事件冲击 → `EVENT_WAIT`；Hyperliquid 单独 → 永不作锚；
无界亏损表达 → 只解释。

### 3.5 需要的重构（刻意最小化）

1. **抽取，不重写**：把 `options_map` 里按 strike 聚合 greeks 的部分抽到
   `exposure_map`，`options_map` 改为消费它。collector 层、alert 管道、notifier
   全部不动。
2. **不加执行面**：本批次不碰下单、不碰 severity 逻辑，`alert_engine` 只加
   「附注 steven context 到既有告警文本」的只读挂钩。
3. **不建通用 features 框架**：只加这一个 exposure_map 文件，等第二个消费场景
   出现再谈抽象。

---

## 4. 需要产出的实施文档（每篇在对应 Phase 开工前写）

1. `docs/greeks-definitions.md`：每个 `_proxy` 指标的公式、dealer 符号约定、
   单位、聚合规则、更新频率、与 vendor 指标的差异声明。（**先于任何计算代码**）
2. `docs/steven-framework-integration.md`：状态机转移表、episode schema、
   hard gate 到代码位置的映射。
3. `docs/data-budget.md`：第 2 节的分配表固化成运维文档，含变更流程
   （改 `runtime.yaml` 哪些键、如何验证行数/req 计数不超限）。

---

## 5. 分阶段实施计划

### Phase 0：交易时段验收（前置，1 个交易日）

- 验收 Schwab SPXW 盘中 OI 与链质量（周末样本不可信）。
- 一次性采样 `$NDX`/`$RUT` 链，评估是否值得进 Tier C（可能结论：QQQ/IWM 代理已够）。
- 实测 `$SPX` 链 `strikeCount=40` 的响应大小与延迟。
- **验收标准**：盘中 SPXW OI 非零率、链延迟 P95 < 2s、决定 Tier C 取舍。

### Phase 1：数据预算重配（0.5 天改配置 + 1 个交易日观测）

- `runtime.yaml`：IBKR `max_option_lines` 60→68、热窗 ±50→±55；
  Schwab 链拆三档节奏、`strikeCount` 提档。
- **验收标准**：IBKR 行数峰值 ≤ 90（含 replan 重叠）；Schwab 实测 ≤ 60 req/min；
  intraday_shock 与现有告警行为无回归（825 测试全绿）。

### Phase 2：exposure_map + bar builder（2–3 天）

- 先写 `docs/greeks-definitions.md`，再实现 `features/exposure_map.py`
  与 1m/5m bar builder；`options_map` 完成抽取。
- **验收标准**：同一输入下 options_map 输出与重构前逐字段一致（golden 测试）；
  exposure_map 双权重版本落 latest；文档与实现公式一一对应。

### Phase 3：strategy/steven observe_only（2–3 天）

- 实现状态机、contract v0.1 输出、episodes JSONL、alert context 附注。
- **验收标准**：七条 hard gate 各有单测；周末/数据缺失时稳定输出
  `observe_only|invalid`；连续 3 个交易日 episode 记录完整且可读。

### Phase 4：验证框架（1 周，离线）

- post_close_review 挂 forward metrics（T+5m/15m/30m/60m/close、MFE/MAE、
  触及/收复/接受判定）；与基线（无条件收益、开盘区间策略、GEX-only 图）对比。
- **验收标准**：每个 episode 自动生成审计行；假设 1–6 各有可复算的对照结果。

### Phase 5（远期，显式挂起）

- signed option flow 数据源（OPRA vendor）选型——这是把 Flow 层从 `weak`
  提到可信的唯一路径，属于**买数据决策**而非工程任务。
- 任何执行面/severity 联动，须在 Phase 4 出正向证据后另立设计文档。

---

## 6. 风险与开放问题

1. **Schwab OI 质量未知**（Phase 0 才能回答）；若不可靠，exposure_map 的
   OI 权重只能靠 IBKR 68 行的窄窗，广域墙精度受限。
2. **Net DEX 代理 ≠ 作者的 Net DEX**：回测结论只对我们的代理成立，
   文档与输出中必须持续声明，防止未来误读为「验证了 Steven 的指标」。
3. **`strikeCount=40` 响应体积**：若延迟超标，退化方案是按 expiration 拆分
   或只对 0DTE 提档。
4. **replan 重叠行数**：热窗扩大后 replan 瞬时占用变大，Phase 1 必须实测
   峰值，不达标则回退到 64 行。
5. **无期权 aggressor 数据时，Flow 层永远是弱确认**：状态机允许从
   `*_WATCH` 到 `SETUP_CONFIRMED` 仅凭价格 trigger + 现货流代理，
   confidence 相应封顶 `medium`。
