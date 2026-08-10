# RTH Signal Unstarve v1

状态：Change Brief 已确认，按现有 owner 落地；不新增 service / DB / queue / Rust / LLM / BWB。  
实现状态（2026-08-11）：P0 已落地并通过相关 pytest；P1 的 OR / VWAP / shock 与 Butterfly 强化未开始。
范围依据：仓库代码、冻结回放证据与生产运行时漏斗；不补盘感故事。  
关联：`docs/strategy-signal-engine-v2.md`；架构简化执行方案 S-track；Import Linter / module-architecture 分层不变。

## 0. 一句话

当前不是“太谨慎”，而是：数据层全局自我否决，触发层确认过晚，候选层覆盖不足，排序层尚未校准，展示层仍在讲旧状态机。

## 1. 现象

系统同时表现为：

1. 大多数时间只给 `NO_TRADE` / `DEGRADED` / `WAIT`；
2. 偶尔给出候选时，也未必有已校准的稳定 edge；
3. Desk Map 不能清楚区分「市场没机会 / 数据坏了 / 候选差一点通过」。

今日坚持 `NO_TRADE` 可以比人为强行 Butterfly 更正确。问题不是“今天没给蝶”，而是系统无法稳定产生其他合法机会，也无法清楚解释弃权链路。

## 2. 三类根因

### 2.1 候选生成前就被数据门控整体否决

`build_strategy_decision()` 在枚举前执行 `_gate_reasons()`。只要 `MarketFactPack` 是 `degraded`，就不进入 Vertical / Butterfly 枚举。

`strategy_facts.py` 把下列问题放进同一全局质量列表：

- SPX 价格不可用；
- `pricing_allowed=false`；
- market frame 不 ready；
- option structure frame 不 ready；
- option L1 不 ready。

`market_features/service.py` 规定：完整 option frame 与 L1 都 ready 时，`pricing_allowed` 才为 true。于是 Gamma/OI/structure 某一层出问题，即使价格路径与两条 Vertical 腿报价可用，整个策略引擎仍被一票否决。

冻结验收基线（示例）：

| 会话 | 主要原因 | 量级 |
| --- | --- | ---: |
| 2026-08-07 | `confirmed_price_trigger_unavailable` | 947 |
| 2026-08-07 | `pricing_not_authorized` | 466 |
| 2026-08-07 | `candidate_probability_event_mismatch` | 209 |
| 2026-08-07 | `vertical_exact_spread_unavailable` | 27 |
| 2026-08-08 控制样本 | `pricing_not_authorized` | 15,383 |

大量 `NO_TRADE` 还没走到“有没有 edge”。

应按能力分层，而不是单一 `quality=degraded`：

```text
price_path_ready
vertical_exact_quote_ready
pin_surface_ready
structure_levels_ready
cross_section_ready
shock_state_ready
```

规则示例：

- 缺 Gamma/OI：禁 Butterfly，不自动禁价格确认后的 Vertical；
- 缺跨指数宽度：降置信，不直接归零；
- 缺三腿密度：不能做 Pin Butterfly，两腿方向价差仍可评估。

### 2.2 价格确认过晚，随后被 Anti-Chase 判追价

Level Decision 完整路径：

```text
FAR → APPROACHING → TESTING → BREAK/REJECT_PENDING
→ ACCEPTED/REJECTED → RETEST → CONFIRMED
```

Breakout 还要求 inside→outside、buffer、持续接受、ES 同步、extension、retest、confirmation hold。默认还包含接受保持、确认移动、确认保持与强制 retest。

统一引擎 RTH Vertical 又要求 `trigger.phase == "confirmed"` 才枚举；之后 Anti-Chase 再检查目标空间、debit/width、ATR 止损、是否已走完大部分 trigger→target。

内在矛盾：上游要求确认够晚，下游要求入场不能太晚。

回放示例量级：新规则 3 TRADE / 16 NO_TRADE；Late Chase 拒绝 5；stop 超出 ATR 5；精确双腿不完整 6。

应拆分：

```text
SETUP_VALID
ENTRY_WINDOW_OPEN
ENTRY_TOO_LATE
```

### 2.3 通过候选的排序仍偏静态收益图

Ranker 已把未校准 P/Q utility 降为 advisory，但 `selection_score` 仍主要看理论 max profit/loss、翼宽惩罚、摩擦与弱 D4 prior，未把 body 距 spot/Value Center、impulse、recent extreme、debit/width、shock、真实 ManagementPolicy PnL 作为 Butterfly 硬门。

`_butterfly_hard_gates()` 基本只检查三腿存在与收益可算。Pin 路径对部分缺失值甚至 fail-open（如 `vix = ... or 0.0`）。校准证据不足（如 1 session、`promotion_ready=false`、要求 25 sessions）时，不得宣称 alpha。

## 3. 具体工程缺陷

### 3.1 Exact-spread bootstrap deadlock

当前流程：

```text
_vertical_candidates()
  → _rth_evidence()
  → 必须先从 call/put spread shadow 或 trade_intent 取得现成 spread
  → 才进入 _rth_width_verticals() 枚举 5/10/15/20
```

没有现成 spread 时 `_rth_evidence()` 返回 `vertical_exact_spread_unavailable`，直接枚举器永不运行。统一候选工厂要求旧系统先给合约，才肯自己生成合约。

### 3.2 GTH/RTH 价格坐标 split-brain

`level_decision` 已有正确坐标：RTH official SPX；GTH SPXW parity；再不行 ES-equivalent。  
但策略入口仍可能单独取 `action_latest.best_quote("index:SPX")`，GTH 无 official SPX 时：

- level_decision：`chain_implied_spx` 可用；
- strategy_decision：`spx_price_unavailable` / DEGRADED。

P0：`resolve_trigger_coordinate()` 成为唯一价格坐标 owner，同一份 `spx_observed_value/source/kind/basis` 供给 level machine、strategy facts、geometry 与展示层。

### 3.3 文档 setup 未落地

文档允许 Trend Pullback 发生在 VWAP、已接受 OR、Wall/Flip retest、趋势腿回撤、最近 HL/LH。  
代码 `_rth_evidence()` 实际只认 `facts["trigger"]=level_decision` 且 `phase=="confirmed"`。  
结果是“确认完成的 Wall/Flip level-event Vertical”，不是完整 Trend Pullback。

已有 `session_episode`（STRUCTURE_BREAK / EXTREME / RECLAIM_PENDING / V_REVERSAL_CONFIRMED / RECOVERY）每轮计算，但未稳定进入 `strategy_payload` / selector。

### 3.4 Butterfly / BWB 边界

生产第一版只允许：`NO_TRADE`、Call/Put Debit Vertical、Call/Put Butterfly。BWB 排除。  
普通 Butterfly 另有 GTH 不枚举、directional confirmation 仅 research、人工候选要求 `PIN_STABLE` 等限制。不频繁给蝶基本按设计；空白应由 Failed Break / Pullback Vertical 填补，而不是硬放宽 Pin 蝶。

### 3.5 Exact quote 与回退冲突

RTH `_option_leg()` `require_schwab=True`；execution quote ≤15s；多腿 skew ≤2s。合理路径：先定 geometry → exact-leg pin request → 短同步快照 → 同 snapshot 构图 → Schwab 失败时允许经 capability gate 的 IBKR RTH fallback。

### 3.6 Desk Map 不是真正策略决策界面

文档：`strategy_decision` = sole human candidate authority。  
`operator_status.py` 仍主要读 level_decision / trade_intent / gth_level_manual / plan_candidates / guidance；只从 strategy_decision 取少量 surface shape。真正人工候选走 `trade_ready` 且需 `action_authority=="manual"`。

Desk Map 应改为：

```text
Decision: NO_TRADE
Primary blocker: ...
Secondary blocker: ...
Nearest candidate: ...
Failed gates: ...
Reauthorize when: ...
```

`level_decision` 降为 Evidence。

### 3.7 Shock 未接入统一 facts

已有 intraday shock 服务，但 MarketFactPack 无明确 shock 字段。应：

```text
SHOCK_ACTIVE → 禁新 Butterfly
POST_SHOCK_DISCOVERY → 等待新 Value Center
SHOCK_RECLAIMED → 恢复竞争
```

## 4. Change Brief：RTH Signal Unstarve v1

### 用户可见目标

RTH 中只要存在有效的 Failed Break/Reclaim、Trend Pullback 或 Stable Pin，系统必须二选一：

1. 给出带新鲜组合报价的人工候选；或
2. 明确说明该具体候选被哪道门拒绝。

禁止继续用模糊文案掩盖“候选没生成 / 报价没刷新 / 数据层误杀”。

### 不新增

不新增 service、数据库、消息队列、Rust contract、LLM、新策略类型、BWB。只改现有 owner。

### 涉及文件（现有 owner）

```text
application/market_features/service.py
application/order_map/strategy_facts.py
application/order_map/strategy_select.py
application/order_map/candidate_factory.py
application/order_map/strategy_ranker.py
application/order_map/operator_status.py
```

必要时小范围触达已有 coordinate resolver / session_episode / shock 读取路径；新增文件不得超过策略引擎 v2 预算。

## 5. 实施顺序

### P0：解除工程性饥饿与误导展示

1. **统一坐标**：`resolve_trigger_coordinate()` 唯一 owner。
2. **能力门控**：全局 option-quality 一票否决改为 per-strategy capability gate。
3. **解除 exact-spread deadlock**：setup evidence 与合约 evidence 分离；direct enumeration 不依赖 legacy spread。
4. **Desk Map**：主结论完全服从 `strategy_decision`，并展示 primary/secondary blocker 与 nearest candidate。
5. **Rejection funnel**（每 session / 决策周期）：

```text
cycles
→ facts_ready
→ setup_detected
→ entry_window_open
→ candidate_enumerated
→ exact_quote_ready
→ hard_gate_pass
→ manual_card_delivered
```

P0 验收不是“信号变多”，而是：

- RTH exact quote 可用时，`pricing_not_authorized` 不再大量误杀 Vertical；
- GTH parity 可用时，不再误报 `spx_price_unavailable`；
- 每条 `NO_TRADE` 能归属：无 setup / 数据问题 / 报价问题 / 赔率问题。

### P1：真实交易逻辑

生产先聚焦：

```text
FAILED_BREAK_RECLAIM_VERTICAL
TREND_PULLBACK_VERTICAL
```

- 接入 `session_episode`；
- OR Failed Break / VWAP Pullback evidence；
- `ENTRY_WINDOW_OPEN` 与 setup confirmation 分离；
- shock 直接禁 Butterfly；
- exact legs 短暂原子 pin；
- RTH 允许经验证的 IBKR fallback；
- `PIN_STABLE` Butterfly 强化硬门（body 对齐、无 recent extreme、debit/width、shock inactive、VIX/breadth fail-closed、风险预算）。

### P2：校准后再谈“好信号”

至少 25 个 session，按 conservative ask/bid、+50% arm、trailing、premium stop、20m time stop、手续费点差评估后，再决定 manual authority、Butterfly 去留、宽度与是否引入 BWB。

## 6. 冻结验收案例

1. **Gamma 降级不杀 Vertical**：official SPX + market frame + exact two-leg ready，structure/Gamma degraded → Vertical 可枚举，Butterfly 禁止。
2. **无 legacy spread 仍枚举**：confirmed/setup 存在，shadow/intent 缺失，Schwab exact legs ready → 5/10/15 点 vertical 被枚举，不再因 `vertical_exact_spread_unavailable` 饿死。
3. **Session Episode Failed Break**：STRUCTURE_BREAK → EXTREME → RECLAIM_PENDING → V_REVERSAL_CONFIRMED → 反向 debit vertical。
4. **午间瀑布 / shock**：shock active → 禁新 Pin Butterfly，进入 POST_SHOCK_DISCOVERY。
5. **漏斗完整**：每个 RTH session 输出第 5 节 funnel，可区分策略无机会 / 工程未生成 / 报价缺失 / 风险拒绝 / 通知未达。

优先冻结会话：生产当日 RTH session（含午间 shock 路径）与既有 2026-08-05 / 2026-08-06 策略验收日。

## 7. 明确不做的“快速修复”

- 打开 legacy `formal_signal` 当完成；
- 全面下调确认阈值；
- 把 quote freshness 从 15s 放到 90s；
- 降低 target-room / ATR stop 门；
- 增加 BWB；
- 让 LLM 自由选方向；
- 强制每天至少一张交易卡。

## 8. 部署一致性备注

若卡片仍写 “Schwab OI unverified，Gamma/Wall 不可用”，而 master 已合入 RTH Schwab OI 保留 `oi_weighted` 的修复，先核对：

```bash
cd /home/ubuntu/spx-spark
git rev-parse --short HEAD
jq '{
  git: .runtime_git_sha,
  decision_type: .decision_type,
  reasons: .why_not.reasons,
  quality: .market_facts.quality,
  spot: .market_facts.spot,
  trigger: .market_facts.trigger
}' "$DATA_ROOT/latest/strategy_decision.json"
```

仓库 SHA、`strategy_decision.runtime_git_sha` 与报告进程必须一致，再调策略阈值。

## 9. 最小落地清单（本变更）

1. [x] 解除 exact-spread bootstrap deadlock：setup evidence 不再依赖 legacy spread，RTH 直接枚举现有宽度集合。
2. [x] 全局 quality gate → per-strategy capability gate：Gamma / OI / structure 降级不再误杀具备 exact 两腿报价的 Vertical；Butterfly fail closed。
3. [x] 统一 official / parity / ES-equivalent SPX 坐标：Market Features 策略入口复用 `resolve_trigger_coordinate()`，underlier / spot / facts 共用同一解析结果。
4. [x] 把 `session_episode` 接进 selector：`V_REVERSAL_CONFIRMED` / `RECOVERY` 映射为反向 `FAILED_BREAK_RECLAIM` Vertical setup。
5. [x] Desk Map 完全服从 `strategy_decision`，level decision 降为 Evidence，并输出逐决策 rejection funnel。
6. [ ] P1：OR Failed Break / VWAP Pullback、shock gate、Butterfly 硬门强化；本阶段未实施。

P0 冻结测试覆盖：

- structure / Gamma degraded + exact two-leg ready → Vertical 可枚举、Butterfly 禁止；
- confirmed setup + 无 legacy spread → direct width enumeration；
- `V_REVERSAL_CONFIRMED` → 反向 Failed Break/Reclaim Vertical；
- Desk Map 的 Decision / blocker / nearest candidate / failed gates / reauthorize 条件只读 `strategy_decision`。

前五项完成后，RTH 才具备正常候选供给；后几项防止错误环境给出脆弱 Butterfly。
