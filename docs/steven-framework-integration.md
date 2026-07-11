# Steven SPX Options Framework 集成规格（exposure_map + strategy/steven）

日期：2026-07-11
状态：规格文档（Phase 2/3 实现依据；细化总规划文档第 3 节）
前置阅读：`docs/superpowers/specs/2026-07-11-steven-framework-data-budget-plan.zh.md`、`docs/greeks-definitions.md`
风格约束：与 `strategy/micopedia.py` 一致——frozen dataclass 输入/输出、纯函数分类、
`runtime_value("steven.<key>")` 读配置、`to_dict()` 序列化、observe_only、绝不下单、绝不抬 severity。

---

## 1. `features/exposure_map.py` 接口协议

新目录 `src/spx_spark/features/`（含空 `__init__.py`）。本模块是 options_map、steven、order_map
共用的希腊聚合地基；options_map 的 `build_gex_by_strike`/`build_wall_ladder`/`gex_weight`/`signed_gex`
逻辑抽取至此（options_map 保留同名符号 re-export 以兼容既有 import，行为逐字段不变——Phase 2 golden 测试保证）。

### 1.1 输入行 schema：`ExposureInputRow`

IBKR 期权行与 Schwab 链行都先归一成这个 frozen dataclass（构造函数
`exposure_input_row_from_quote(quote: Quote, *, as_of: datetime) -> ExposureInputRow | None`，
非 SPXW 期权、strike/right/expiry 缺失时返回 None）：

| 字段 | 类型 | 来源 | 约束 |
| --- | --- | --- | --- |
| `contract_id` | `str` | `quote.instrument.canonical_id` | 例 `option:SPX:SPXW:20260711:7500:C` |
| `expiry` | `str` | `quote.instrument.expiry` | `YYYYMMDD`；缺失 → 行丢弃 |
| `strike` | `float` | `quote.instrument.strike` | 有限且 > 0；否则行丢弃 |
| `right` | `str` | `quote.instrument.right.value` | `"C"` 或 `"P"`；否则行丢弃 |
| `provider` | `str` | `quote.provider.value` | `"ibkr"` / `"schwab"` / … |
| `quality` | `str` | `quote.quality.value` | 原样保留 |
| `bid` | `float \| None` | `quote.bid` | — |
| `ask` | `float \| None` | `quote.ask` | — |
| `mid` | `float \| None` | `quote.mid` | Quote 属性，含负 bid 保护 |
| `iv` | `float \| None` | `options_map.option_iv(quote)` | 已过质量门 |
| `delta` | `float \| None` | `options_map.usable_delta(quote)` | 已过质量门 |
| `gamma` | `float \| None` | `options_map.option_gamma_structural(quote, as_of=as_of)` | 已过结构质量门（≤900s stale 容忍） |
| `open_interest` | `float` | `finite_float(quote.open_interest) or 0.0` | 缺失按 0 |
| `volume` | `float` | `finite_float(quote.volume) or 0.0` | 缺失按 0 |
| `quote_age_seconds` | `float \| None` | `quote.quote_age_ms(as_of) / 1000` | — |
| `pricing_allowed` | `bool` | `configured_quote_use_decision(quote, as_of=as_of).pricing_allowed` | — |

行选取与去重沿用 `options_map.group_spxw_option_quotes(state)`（provider 优先级、stale 降级、
IBKR down 抑制），exposure_map 不重复实现，直接消费其输出再逐 Quote 归一。

### 1.2 输出 schema

```text
ExposureMap
├─ created_at: datetime            # 构建时刻（UTC）
├─ as_of: datetime                 # state.as_of
├─ underlier: UnderlierReference   # 复用 options_map 的 dataclass（price/source）
├─ expiries: tuple[ExpiryExposure, ...]      # 按 expiry 升序
└─ warnings: tuple[str, ...]

ExpiryExposure
├─ expiry: str                                # YYYYMMDD
├─ row_count: int                             # 参与聚合的输入行数
├─ strike_count: int
├─ quality: str                               # "ok" | "degraded" | "no_open_interest" | "unavailable"
├─ oi_quality: str                            # greeks-definitions §0.6 枚举
├─ iv_source: str                             # greeks-definitions §0.6 枚举
├─ snapshot_age_seconds: float | None
├─ delta_coverage_ratio: float                # with_delta / row_count
├─ iv_coverage_ratio: float                   # with_iv / row_count
├─ strikes: tuple[StrikeExposure, ...]        # 按 strike 升序
├─ oi_weighted: ExposureAggregates
├─ volume_weighted: ExposureAggregates
├─ gex_weighting_divergence: float | None     # vol.net_gamma_ratio − oi.net_gamma_ratio
├─ walls: WallSet
├─ zero_gamma: float | None                   # 复用 options_map 的 spot-scan/strike-profile 逻辑
├─ gamma_flip_zone: tuple[float, float] | None
├─ zero_gamma_method: str
├─ sign_convention: str                       # 常量 "calls_positive_puts_negative"
├─ dealer_position_sign: str                  # 常量 "unknown"
├─ direction: str                             # 常量 "unknown"
├─ model: str                                 # 常量 "bs_r0_q0"
└─ warnings: tuple[str, ...]                  # "schwab_oi_unverified" / "low_delta_coverage" /
                                              # "early_session_low_volume" / "tau_floored:<contract_id>" …

StrikeExposure                                 # 每 strike 一行，call/put 合并
├─ strike: float
├─ call_open_interest: float、put_open_interest: float
├─ call_volume: float、put_volume: float
├─ call_iv: float | None、put_iv: float | None
├─ call_delta: float | None、put_delta: float | None
├─ call_gamma: float | None、put_gamma: float | None
├─ call_vanna_per_vol_point: float | None、put_vanna_per_vol_point: float | None
├─ call_charm_per_minute: float | None、put_charm_per_minute: float | None
├─ oi_weighted: StrikeExposureValues
└─ volume_weighted: StrikeExposureValues

StrikeExposureValues                           # 双权重版本共用的数值组
├─ call_gex: float | None、put_gex: float | None
├─ net_gex: float | None、abs_gex: float | None
├─ net_dex_proxy: float | None
├─ vex_proxy: float | None
└─ cex_proxy: float | None

ExposureAggregates                             # expiry 级聚合（公式见 greeks-definitions）
├─ net_gex: float | None、abs_gex: float | None、net_gamma_ratio: float | None
├─ net_dex_proxy: float | None、net_dex_ratio_proxy: float | None
├─ dagex_proxy: float | None                   # 仅 volume_weighted 侧非 None（= net_gex）
├─ vex_proxy: float | None
└─ cex_proxy: float | None

WallSet                                        # 从 oi_weighted GEX 建墙（沿用现有 wall 语义）
├─ call_walls: tuple[WallLevel, ...]           # 复用 options_map.WallLevel，top-4
├─ put_walls: tuple[WallLevel, ...]
├─ wall_method: str                            # "oi_gex" | "volume_fallback"
└─ pin_candidate: float | None                 # |net_gex| 最大且 call/put OI 都 > 0 的 strike；
                                               # 需同时满足 |strike − spot| ≤ steven.pin_max_distance_points
```

公开函数（纯函数，签名固定）：

```python
def build_exposure_map(state: LatestState) -> ExposureMap: ...
def exposure_map_to_dict(exposure: ExposureMap) -> dict[str, Any]: ...   # 或 dataclass 方法 to_dict()
def net_dex_proxy_by_expiry(exposure: ExposureMap, *, weighting: str) -> dict[str, float | None]: ...
    # weighting ∈ {"oi_weighted", "volume_weighted"}；其它值 raise ValueError

# 纯计算层（golden 数值测试的直接入口，必须暴露显式 tau_years 参数）：
def strike_exposure_values(
    rows: tuple[ExposureInputRow, ...],   # 同一 strike 的 call/put 行
    *,
    spot: float,
    tau_years: float,
    weighting: str,                        # "oi_weighted" | "volume_weighted" | "oi_plus_volume"
) -> StrikeExposureValues: ...
```

`build_exposure_map` 内部经 `time_to_expiry_years` 算出 tau 后调用 `strike_exposure_values`；
数值测试直接以固定 `tau_years=0.01` 调用纯函数层（见验收矩阵文档 P2-C 实现提示）。

落盘：service loop 每轮把 `exposure_map_to_dict` 写到
`{data_root}/latest/exposure_map.json`（`state_io.atomic_write_json_secure`）。

### 1.3 options_map 抽取契约（Phase 2 golden 约束）

- `build_gex_by_strike`、`build_wall_ladder`、`gex_weight`、`signed_gex`、`interpolate_zero`、
  `nearest_zero`、`zero_gamma_bracket` 移入 `features/exposure_map.py`；
  `options_map` 通过 `from spx_spark.features.exposure_map import ...` re-export，签名与数值行为零变化。
- `build_options_map(state)` 的 JSON 输出（`to_dict()`）在任意相同输入下与抽取前**逐字段一致**。
- `oi_plus_volume`（intraday）权重继续由 `gex_weight(quote, intraday=True)` 提供，仅供 options_map。

---

## 2. `strategy/steven.py`：Guidance Contract v0.1

### 2.1 完整 JSON Schema

写入 `docs/steven-guidance-contract-v0.1.schema.json`（实现时随代码提交，测试断言与本节一致）。
`latest/steven_state.json` 中的 `contract` 字段与 episodes 的 `revisions[].contract` 都是本 schema 实例。

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "steven_guidance_contract",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "schema_version", "source", "created_at", "as_of", "status", "machine_state",
    "regime", "regime_breadth", "map", "trigger", "invalidation",
    "expression_family", "confidence", "flow_confirmation", "data_quality", "warnings"
  ],
  "properties": {
    "schema_version": { "const": "steven_guidance_contract.v0.1" },
    "source": { "const": "steven_spx_options_framework_house_proxy" },
    "created_at": { "type": "string", "format": "date-time" },
    "as_of": { "type": "string", "format": "date-time" },
    "status": { "enum": ["observe_only", "watch", "confirmed_for_review", "invalid"] },
    "machine_state": {
      "enum": ["DATA_INVALID", "OBSERVE_ONLY", "REGIME_UNKNOWN",
               "BULLISH_DIP_WATCH", "BEARISH_BREAK_WATCH", "RANGE_PIN_WATCH",
               "EVENT_WAIT", "SETUP_CONFIRMED", "EXIT_REVIEW", "LOCKOUT_OR_REMAP"]
    },
    "regime": { "enum": ["bullish", "bearish", "mixed", "unknown"] },
    "regime_breadth": {
      "type": "object", "additionalProperties": false,
      "required": ["expiries_total", "expiries_bullish", "expiries_bearish", "agreement_ratio", "weighting"],
      "properties": {
        "expiries_total": { "type": "integer", "minimum": 0 },
        "expiries_bullish": { "type": "integer", "minimum": 0 },
        "expiries_bearish": { "type": "integer", "minimum": 0 },
        "agreement_ratio": { "type": ["number", "null"], "minimum": 0, "maximum": 1 },
        "weighting": { "enum": ["oi_weighted", "volume_weighted"] }
      }
    },
    "map": {
      "type": "object", "additionalProperties": false,
      "required": ["support", "resistance", "pin", "acceleration"],
      "properties": {
        "support": { "type": "array", "items": { "type": "number" }, "maxItems": 4 },
        "resistance": { "type": "array", "items": { "type": "number" }, "maxItems": 4 },
        "pin": { "type": ["number", "null"] },
        "acceleration": { "type": "array", "items": { "type": "number" }, "maxItems": 2 }
      }
    },
    "trigger": {
      "type": "object", "additionalProperties": false,
      "required": ["kind", "level", "direction", "confirmed", "confirmed_at", "source_event_id"],
      "properties": {
        "kind": { "enum": ["none", "dip_hold", "reclaim", "break_hold", "range_reject"] },
        "level": { "type": ["number", "null"] },
        "direction": { "enum": ["up", "down", "none"] },
        "confirmed": { "type": "boolean" },
        "confirmed_at": { "type": ["string", "null"], "format": "date-time" },
        "source_event_id": { "type": ["string", "null"] }
      }
    },
    "invalidation": {
      "type": "object", "additionalProperties": false,
      "required": ["level", "side", "reason"],
      "properties": {
        "level": { "type": ["number", "null"] },
        "side": { "enum": ["below", "above", "none"] },
        "reason": { "type": "string" }
      }
    },
    "expression_family": {
      "enum": ["none", "bullish_defined_risk", "bearish_defined_risk", "range_defined_risk"]
    },
    "confidence": { "enum": ["low", "medium", "high"] },
    "flow_confirmation": {
      "type": "object", "additionalProperties": false,
      "required": ["status", "sources", "quality"],
      "properties": {
        "status": { "enum": ["none", "weak", "aligned", "opposed"] },
        "sources": { "type": "array", "items": { "enum": ["es_volume", "hl_volume", "option_volume_delta"] } },
        "quality": { "const": "weak_proxy" }
      }
    },
    "data_quality": {
      "type": "object", "additionalProperties": false,
      "required": ["anchor_ok", "exposure_quality", "oi_quality", "iv_source", "snapshot_age_seconds"],
      "properties": {
        "anchor_ok": { "type": "boolean" },
        "exposure_quality": { "enum": ["ok", "degraded", "no_open_interest", "unavailable"] },
        "oi_quality": { "enum": ["ibkr_ok", "schwab_unverified", "stale_or_zero", "missing"] },
        "iv_source": { "enum": ["vendor_ibkr", "vendor_schwab", "mixed", "missing"] },
        "snapshot_age_seconds": { "type": ["number", "null"] }
      }
    },
    "warnings": { "type": "array", "items": { "type": "string" } }
  }
}
```

硬约束（schema 之上的业务规则，纯函数 `build_steven_contract` 内部强制）：

1. **confidence 封顶 medium**：regime/map 全部来自 `_proxy` 指标，`confidence == "high"` 永不输出
   （枚举保留 high 只为向后兼容 v0.2+ 可能接入真实数据源）。实现：`classify_confidence` 最后一行
   `return min(confidence, "medium")`（按 low<medium<high 序）。
2. `oi_quality == "schwab_unverified"` 或 `flow_confirmation.status in {"none"}` → confidence 封顶 `low`。
3. `status` 由 `machine_state` 唯一决定（映射见 2.3 表末列），不允许独立赋值。
4. `trigger.confirmed == false` 时 `status` 不得为 `confirmed_for_review`（hard gate 3）。
5. `expression_family != "none"` 仅当 `machine_state == "SETUP_CONFIRMED"`；且永远只是解释文本级别的
   有界风险策略族名，不含行权价/张数/价格（hard gate 7）。

### 2.2 模块结构（与 micopedia 逐层对应）

```python
# src/spx_spark/strategy/steven.py
@dataclass(frozen=True)
class StevenInputs:            # 类比 MicopediaInputs；__post_init__ 做枚举归一
    created_at: datetime
    as_of: datetime
    underlier_price: float | None
    underlier_source: str | None            # "index:SPX" | "chain_implied" | 其它
    exposure: ExposureMap | None            # features/exposure_map 输出
    bars_1m: tuple[SpxBar, ...]             # §7 bar builder 输出（可空）
    bars_5m: tuple[SpxBar, ...]
    shock_state: dict[str, Any] | None      # intraday_shock 的 monitor state（latest 读取）
    es_volume: dict[str, Any] | None        # order_map.es_volume_signal 的 payload
    hl_volume: dict[str, Any] | None        # order_map.hl_volume_signal 的 payload
    session_phase: str                      # "premarket"|"open"|"midday"|"late"|"closed"|"unknown"
    event_tags: tuple[str, ...]             # 同 micopedia 的 EVENT_ALIASES 归一
    previous_state: str                     # 上一轮 machine_state；首轮 "OBSERVE_ONLY"
    previous_state_since: datetime | None

@dataclass(frozen=True)
class StevenSignal:            # to_dict() 产出 Guidance Contract v0.1
    ...

def classify_regime(inputs) -> tuple[str, dict]          # → regime, regime_breadth
def build_map_levels(inputs) -> dict                     # → map{support,resistance,pin,acceleration}
def evaluate_trigger(inputs, map_levels) -> dict         # → trigger
def evaluate_flow(inputs) -> dict                        # → flow_confirmation
def advance_state(inputs, regime, trigger, ...) -> str   # → machine_state（2.3 状态机）
def build_steven_signal(inputs: StevenInputs) -> StevenSignal
```

regime 判定（`classify_regime`，只用 `net_dex_proxy_by_expiry`）：

```text
weighting = str(runtime_value("steven.regime_weighting"))          # 默认 "oi_weighted"
per_expiry = net_dex_proxy_by_expiry(exposure, weighting=weighting)
非 None 的 expiry 中：value > +steven.regime_dex_neutral_band → 记 bullish
                    value < −steven.regime_dex_neutral_band → 记 bearish
                    否则 → neutral（不计入 bullish/bearish）
agreement_ratio = max(bullish 数, bearish 数) / 非 None expiry 数
regime = bullish|bearish  若同向数 ≥ steven.regime_min_expiries 且
                            agreement_ratio ≥ steven.regime_agreement_min_ratio
       = mixed            若 bullish 与 bearish 同时非零且都不满足上式
       = unknown          若非 None expiry 数 < steven.regime_min_expiries 或全 neutral
```

map 层（`build_map_levels`）：

```text
support      = 前端 expiry walls.put_walls 的 strikes（top-4，按 |gex| 降序）
resistance   = 前端 expiry walls.call_walls 的 strikes
pin          = walls.pin_candidate（gamma_state 为正 gamma 侧时才输出，否则 null）
acceleration = gamma_flip_zone（[low, high]；无 flip 则空数组）
多到期共振：若次日 expiry 的 top-1 put/call wall 与前端墙相距 ≤ steven.wall_confluence_points，
在 warnings 里追加 "multi_expiry_wall_confluence:<strike>"（只作解释，不改 map 数值）。
```

flow 层（`evaluate_flow`）：ES/HL volume payload 里的 `event_id`/`sequence` 与 trigger 方向一致
→ `aligned`；相反 → `opposed`；数据缺失 → `none`；其余 → `weak`。`quality` 恒为 `"weak_proxy"`。

### 2.3 状态机完整转移表

状态持久化于 `latest/steven_state.json`（§5）。每轮评估按下表**自上而下取第一条命中**的转移；
无命中则维持原状态。所有阈值键位于 runtime.yaml `steven:` 段（§6）。

记号：`E` = 前端 expiry 的 `ExpiryExposure`；`spot` = `underlier_price`；
`bar_hold(level, side, n)` = 最近 n 根已收盘 1m bar 的 close 全部位于 level 的 side 侧
（side ∈ above/below；bar 缺失时恒为 False）。

| # | 从 | 到 | 触发条件（字段级） | 可配置参数（runtime.yaml 键） |
| --- | --- | --- | --- | --- |
| T1 | 任意 | `DATA_INVALID` | `underlier_price is None` 或 `underlier_source not in {"index:SPX","chain_implied"}` 或 `E is None` 或 `E.quality == "unavailable"` 或 `E.snapshot_age_seconds > steven.max_snapshot_age_seconds`（hard gate 1、6） | `steven.max_snapshot_age_seconds`（默认 900） |
| T2 | `DATA_INVALID` | `OBSERVE_ONLY` | T1 条件全部消失，且持续 ≥ `steven.data_recovery_hold_seconds` | `steven.data_recovery_hold_seconds`（默认 60） |
| T3 | `OBSERVE_ONLY` / `REGIME_UNKNOWN` / 任一 `*_WATCH` | `EVENT_WAIT` | `shock_state` 中存在未完成 shock 事件（`phase not in {"completed","expired"}`）或 `event_tags ∩ {"fomc","cpi","nfp","pce","headline"} ≠ ∅` 且距事件触发 < `steven.event_wait_cooldown_seconds`（hard gate 5） | `steven.event_wait_cooldown_seconds`（默认 900） |
| T4 | `EVENT_WAIT` | `OBSERVE_ONLY` | T3 条件消失且 spot 相对最近 shock 极值回稳：最近 `steven.event_stabilize_bars` 根 1m bar 的 high−low 均 < `steven.event_stabilize_range_points` | `steven.event_stabilize_bars`（默认 5）、`steven.event_stabilize_range_points`（默认 10） |
| T5 | `OBSERVE_ONLY` | `REGIME_UNKNOWN` | `regime == "unknown"` 或 `regime == "mixed"` | —（由 regime 参数间接控制） |
| T6 | `OBSERVE_ONLY` / `REGIME_UNKNOWN` | `BULLISH_DIP_WATCH` | `regime == "bullish"` 且 `map.support` 非空且 `spot − max(support) ≤ steven.dip_watch_max_distance_points` | `steven.dip_watch_max_distance_points`（默认 30） |
| T7 | `OBSERVE_ONLY` / `REGIME_UNKNOWN` | `BEARISH_BREAK_WATCH` | `regime == "bearish"` 且 `map.support` 非空且 `spot − max(support) ≤ steven.break_watch_max_distance_points` | `steven.break_watch_max_distance_points`（默认 30） |
| T8 | `OBSERVE_ONLY` / `REGIME_UNKNOWN` | `RANGE_PIN_WATCH` | `regime == "mixed"` 允许例外进入：`map.pin` 非 null 且 `abs(spot − pin) ≤ steven.pin_watch_max_distance_points` 且 `E.oi_weighted.net_gamma_ratio ≥ steven.pin_min_net_gamma_ratio` | `steven.pin_watch_max_distance_points`（默认 20）、`steven.pin_min_net_gamma_ratio`（默认 0.15） |
| T9 | `BULLISH_DIP_WATCH` | `SETUP_CONFIRMED` | trigger 确认（hard gate 3 的唯一放行路径）：`trigger.kind == "dip_hold"`——spot 曾进入 `max(support) ± steven.trigger_level_tolerance_points`，随后 `bar_hold(max(support), above, steven.trigger_hold_bars)`；且 `flow_confirmation.status != "opposed"` | `steven.trigger_level_tolerance_points`（默认 5）、`steven.trigger_hold_bars`（默认 2） |
| T10 | `BEARISH_BREAK_WATCH` | `SETUP_CONFIRMED` | `trigger.kind == "break_hold"`——spot 下破 `max(support)` 后 `bar_hold(max(support), below, steven.trigger_hold_bars)`；且 `flow_confirmation.status != "opposed"` | 同 T9 |
| T11 | `RANGE_PIN_WATCH` | `SETUP_CONFIRMED` | `trigger.kind == "range_reject"`——spot 触及 `min(resistance)` 或 `max(support)` 的 `± trigger_level_tolerance_points` 后，反向 `bar_hold(该 level, 反侧, steven.trigger_hold_bars)` | 同 T9 |
| T12 | 任一 `*_WATCH` | `OBSERVE_ONLY` | regime 翻转或不再满足对应 WATCH 的进入条件，持续 ≥ `steven.watch_exit_hold_seconds` | `steven.watch_exit_hold_seconds`（默认 120） |
| T13 | `SETUP_CONFIRMED` | `EXIT_REVIEW` | 目标或失效任一发生：spot 触及 `map` 的对侧第一档（bullish → `min(resistance)`；bearish → 下一档 support；range → pin），或 `invalidation.level` 被 `bar_hold(level, invalidation.side, steven.invalidation_hold_bars)` 确认 | `steven.invalidation_hold_bars`（默认 2） |
| T14 | `SETUP_CONFIRMED` | `EXIT_REVIEW` | 数据失效兜底：T1 任一条件出现（先记 EXIT_REVIEW 关闭 thesis，再由 T1 在下一轮拉到 DATA_INVALID） | — |
| T15 | `EXIT_REVIEW` | `LOCKOUT_OR_REMAP` | 立即（同轮或下一轮）：episode 追加 `final_state` 修订后转入 | — |
| T16 | `LOCKOUT_OR_REMAP` | `OBSERVE_ONLY` | 冷却 ≥ `steven.lockout_minutes` 分钟，且当日 `SETUP_CONFIRMED` 次数 < `steven.max_daily_setups`；否则维持 LOCKOUT 至收盘 | `steven.lockout_minutes`（默认 30）、`steven.max_daily_setups`（默认 2） |
| T17 | 任意 | `OBSERVE_ONLY` | 交易日翻转（`state.as_of` 的 ET 日期 ≠ 状态文件里的 `trading_date`）→ 无条件复位并开新 episode | — |

优先级说明：T1（数据失效）> T17（换日）> T3（事件）> 其余。
`machine_state → status` 映射：`DATA_INVALID → "invalid"`；`SETUP_CONFIRMED → "confirmed_for_review"`；
`*_WATCH / EVENT_WAIT → "watch"`；其余（`OBSERVE_ONLY / REGIME_UNKNOWN / EXIT_REVIEW / LOCKOUT_OR_REMAP`）→ `"observe_only"`。

### 2.4 缺输入时的稳定输出（周末/夜间约束）

`exposure is None`、bars 为空、shock/es/hl payload 缺失都不是异常路径：
`build_steven_signal` 必须照常返回，`machine_state ∈ {DATA_INVALID, OBSERVE_ONLY}`、
`status ∈ {"invalid", "observe_only"}`、`regime == "unknown"`、`expression_family == "none"`，
warnings 说明缺什么。任何输入组合都不得抛异常（Phase 3 有专项测试）。

---

## 3. episodes JSONL schema

- 路径：`{data_root}/lake/steven/episodes/date=YYYY-MM-DD/episode.jsonl`（YYYY-MM-DD 为 ET 交易日）。
- **一天一个 thesis episode**：文件内每行是一次「修订事件」，同一 `episode_id` 贯穿全天；
  盘后审计把整个文件折叠成一个 episode 对象。修订只在**状态机边沿变化**或 map 关键位移动
  超过 `steven.episode_revision_min_level_move_points`（默认 10）时追加，不按轮次刷屏。
- `episode_id = "steven:" + trading_date`（例 `steven:2026-07-13`），天然幂等。

每行（revision event）的 schema：

```json
{
  "schema_version": "steven_episode_event.v0.1",
  "episode_id": "steven:2026-07-13",
  "trading_date": "2026-07-13",
  "seq": 3,
  "recorded_at": "2026-07-13T14:31:05+00:00",
  "event_kind": "pre_market_map | state_transition | map_revision | trigger | final_state",
  "from_state": "BULLISH_DIP_WATCH",
  "to_state": "SETUP_CONFIRMED",
  "contract": { "...": "完整 Guidance Contract v0.1 实例" },
  "note": "机器生成的一句话转移原因（复用转移表条件文本）"
}
```

字段约束：`seq` 从 0 递增（0 必须是 `pre_market_map`，即当日第一次成功评估）；
`final_state` 行在 EXIT_REVIEW→LOCKOUT_OR_REMAP 或收盘后首轮写入，且每个 episode 至多按
setup 次数出现（每次 EXIT_REVIEW 一条）。

盘后折叠出的 episode 汇总对象（由 `post_close_review` 生成，不落在 JSONL 里，落在 review JSON 的
`steven_episode` 键下）：

```json
{
  "episode_id": "steven:2026-07-13",
  "trading_date": "2026-07-13",
  "pre_market_map": { "...": "seq=0 的 contract.map + regime + data_quality" },
  "triggers": [ { "...": "全部 event_kind==trigger 行的 trigger 对象" } ],
  "revisions": [ { "seq": 1, "from_state": "...", "to_state": "...", "recorded_at": "..." } ],
  "final_state": "LOCKOUT_OR_REMAP",
  "setup_count": 1,
  "forward_metrics": null
}
```

`forward_metrics` 是 Phase 4 占位（字段 schema 见 `docs/superpowers/specs/2026-07-11-steven-test-acceptance-matrix.zh.md` 的 Phase 4 节），Phase 3 恒为 null。

---

## 4. 七条 hard gate → 模块/函数职责映射表

| # | Hard gate | 责任模块.函数 | 强制机制 | 违反时输出 |
| --- | --- | --- | --- | --- |
| 1 | SPX/SPXW 锚缺失或过期 → invalid | `steven.advance_state`（T1）+ `exposure_map.build_exposure_map`（quality=unavailable） | `underlier_source` 白名单 `{index:SPX, chain_implied}` + `snapshot_age` 上限 | `machine_state=DATA_INVALID`，`status="invalid"` |
| 2 | 代理指标 → context only，不得抬 confidence/severity | `steven.build_steven_signal`（confidence 封顶 medium）；vanna/charm/vex/cex/divergence 只写入 `warnings` 与解释文本，不进 `classify_regime`/`advance_state` 的任何条件 | 代码审查点：`classify_regime` 与 `advance_state` 的入参签名里**没有** vex/cex/vanna/charm | `confidence ≤ "medium"`；schwab_unverified 时 ≤ "low" |
| 3 | 无价格 trigger → 永远 watch | `steven.evaluate_trigger` + 状态机（唯一进 `SETUP_CONFIRMED` 的路径是 T9/T10/T11，全部要求 `trigger.confirmed==True`） | 2.1 业务规则 4 | `status="watch"` |
| 4 | 回顾性来源 → 不得当历史信号 | `steven.py` 模块常量 `RETROSPECTIVE_SOURCES_ALLOWED = False`；episodes 只记录**当轮实时**评估，禁止用盘后数据回填 `trigger.confirmed_at`；`post_close_review` 折叠时只读 JSONL，不重算信号 | episode 行的 `recorded_at` 必须 ≥ `contract.as_of`（写入时断言） | 写入被拒（raise ValueError） |
| 5 | 事件冲击未稳 → EVENT_WAIT | `steven.advance_state`（T3/T4），输入 `shock_state` + `event_tags` | T3 优先级高于全部 WATCH/CONFIRM 转移 | `machine_state=EVENT_WAIT`，`status="watch"` |
| 6 | Hyperliquid SP500 单独 → 永不作锚 | `StevenInputs` 构造函数 `inputs_from_latest_state`：underlier 只取 `index:SPX` 或 `chain_implied`，**不写** micopedia 那样的 HL fallback；`underlier_source` 白名单在 T1 二次拦截 | HL 价格只可能经 `hl_volume` payload 进 flow 层（且 quality 恒 weak_proxy） | `machine_state=DATA_INVALID` |
| 7 | 无界亏损表达 → 仅解释 | `steven.candidate_expression_family`：返回值枚举只有 `none / bullish_defined_risk / bearish_defined_risk / range_defined_risk`，无任何裸买/裸卖枚举；输出为家族名字符串，不含合约细节 | 枚举封闭 + 2.1 业务规则 5 | 不可能产生（类型层面排除） |

---

## 5. `latest/steven_state.json` 持久化 schema

路径：`{data_root}/latest/steven_state.json`，写入用 `state_io.atomic_write_json_secure`，
读改写周期与 intraday_shock 相同（服务循环内单写者；若跨进程需加 `exclusive_state_lock`）。

```json
{
  "schema_version": "steven_state.v0.1",
  "trading_date": "2026-07-13",
  "machine_state": "BULLISH_DIP_WATCH",
  "state_since": "2026-07-13T13:45:00+00:00",
  "updated_at": "2026-07-13T14:31:05+00:00",
  "episode_id": "steven:2026-07-13",
  "episode_seq_last": 3,
  "daily_setup_count": 0,
  "lockout_until": null,
  "contract": { "...": "最新 Guidance Contract v0.1 完整实例" },
  "transition_history": [
    { "at": "2026-07-13T13:30:00+00:00", "from": "OBSERVE_ONLY", "to": "REGIME_UNKNOWN", "rule": "T5" }
  ]
}
```

约束：`transition_history` 保留最近 50 条（超出裁剪最旧）；`trading_date` 与 as_of 的 ET 日期
不一致时按 T17 复位；文件缺失/损坏（JSONDecodeError）等同于首轮（`previous_state="OBSERVE_ONLY"`），
并在 warnings 追加 `"steven_state_reset:<原因>"`。

---

## 6. runtime.yaml 新增 `steven:` 段（键名清单）

按仓库现有 `value/description` 双键格式追加；默认值与 §2.3 表一致：

```text
steven.enabled                                  (bool, 默认 false —— Phase 3 验收后才开)
steven.regime_weighting                         ("oi_weighted")
steven.regime_dex_neutral_band                  (float，USD，默认 100000.0)
steven.regime_min_expiries                      (int，默认 2)
steven.regime_agreement_min_ratio               (float，默认 0.67)
steven.pin_max_distance_points                  (float，默认 25.0)
steven.wall_confluence_points                   (float，默认 10.0)
steven.max_snapshot_age_seconds                 (float，默认 900.0)
steven.data_recovery_hold_seconds               (float，默认 60.0)
steven.event_wait_cooldown_seconds              (float，默认 900.0)
steven.event_stabilize_bars                     (int，默认 5)
steven.event_stabilize_range_points             (float，默认 10.0)
steven.dip_watch_max_distance_points            (float，默认 30.0)
steven.break_watch_max_distance_points          (float，默认 30.0)
steven.pin_watch_max_distance_points            (float，默认 20.0)
steven.pin_min_net_gamma_ratio                  (float，默认 0.15)
steven.trigger_level_tolerance_points           (float，默认 5.0)
steven.trigger_hold_bars                        (int，默认 2)
steven.watch_exit_hold_seconds                  (float，默认 120.0)
steven.invalidation_hold_bars                   (int，默认 2)
steven.lockout_minutes                          (float，默认 30.0)
steven.max_daily_setups                         (int，默认 2)
steven.episode_revision_min_level_move_points   (float，默认 10.0)
steven.alert_context_enabled                    (bool，默认 false)
steven.alert_context_max_age_seconds            (float，默认 120.0)
steven.bars_source_max_age_seconds              (float，默认 90.0)
```

实现读取方式与 micopedia/intraday_shock 相同：`runtime_value("steven.<key>")`，
可被 `SPX_STEVEN_*` 环境变量覆盖（沿用 `IntradayShockSettings.from_env` 的模式）。

---

## 7. 1m/5m bar builder 协议（`features/bar_builder.py`）

### 7.1 输入

5s 快照序列 `(observed_at: datetime, price: float, provider: str)`，来源为 service loop 每轮的
`index:SPX` 最优报价（`state.best_quote("index:SPX")`，`effective_price`，要求
`configured_quote_use_decision(...).pricing_allowed`）。价格时间取
`quote.quote_time or quote.trade_time or quote.received_at`（与 `intraday_shock._quote_source_at` 同序）。

### 7.2 聚合规则

- **bar 边界对齐**：UTC epoch 秒对齐——1m bar 的 `bar_start = floor(source_time_epoch / 60) × 60`，
  5m bar 为 300 秒对齐（因 ET 与 UTC 偏移是整小时，该对齐与 ET 分钟边界一致；
  与 `post_close_review._five_minute_bucket_count` 的 session-open 对齐口径不同，这是有意选择：
  bar builder 不依赖 session 概念，全天可用）。
- bar 内取 `open/high/low/close = 首/最大/最小/末样本价`（按 source_time 排序，同 tick 后到者覆盖 close），
  `sample_count = 样本数`。
- **收盘判定**：当收到 `source_time ≥ bar_start + 60`（或 300）的样本时，前一根 bar 视为已收盘（closed），
  只有已收盘 bar 进入 `StevenInputs.bars_1m/5m` 与 `bar_hold` 判定。
- **缺样处理**：1m 期望 12 个 5s 样本，`sample_count < 6` 的 bar 标 `quality="partial"`；
  `sample_count == 0` 的分钟**不生成 bar**（不做前值填充），并在下一根 bar 标 `gap_before=True`。
  `bar_hold(level, side, n)` 要求 n 根 bar 全部 `quality=="ok"` 且中间无 gap，否则返回 False。
- 5m bar 由已收盘 1m bar 二次聚合（5 根不满或含 partial → `quality="partial"`）。

### 7.3 数据结构与落盘位置

```python
@dataclass(frozen=True)
class SpxBar:
    bar_start: datetime      # UTC，边界对齐
    interval_seconds: int    # 60 或 300
    open: float; high: float; low: float; close: float
    sample_count: int
    quality: str             # "ok" | "partial"
    gap_before: bool
    provider: str            # 多数样本的 provider
```

- 内存：维持滚动窗口——1m 保留 240 根、5m 保留 96 根（覆盖整个 RTH 加缓冲）。
- 落盘：每轮写 `{data_root}/latest/spx_bars_1m.json` 与 `{data_root}/latest/spx_bars_5m.json`
  （atomic 写，内容 `{"schema_version":"spx_bars.v0.1","interval_seconds":60,"bars":[...]}`)；
  每根 bar 收盘时另追加一行到
  `{data_root}/lake/steven/bars/date=YYYY-MM-DD/spx_bars_1m.jsonl`（5m 同名 `_5m`），供 Phase 4 复算。
- 消费方新鲜度：`latest/spx_bars_1m.json` 的 `updated_at` 距 as_of 超过
  `steven.bars_source_max_age_seconds` 时，Steven 视为 bars 缺失（trigger 永不确认）。

---

## 8. alert_engine 只读挂钩协议

**约束（hard gate 2 的告警侧）**：steven context 只能作为附注文本追加到既有告警，
不改 `Alert.severity`、不改 `Alert.kind`、不产生新的告警 kind、不改变告警的产生与去重逻辑。

接口（放在 `strategy/steven.py`，alert 管道 import 它，方向不可反转以避免层级环）：

```python
def steven_context_note(
    steven_state: Mapping[str, Any] | None,   # latest/steven_state.json 反序列化结果
    *,
    as_of: datetime,
) -> str | None:
    """返回一行附注文本或 None。纯函数，不做 IO。"""
```

规则：

1. `runtime_value("steven.alert_context_enabled")` 为 false → 调用方直接跳过（不读文件）。
2. `steven_state` 为 None、schema_version 不识别、`updated_at` 距 as_of 超过
   `steven.alert_context_max_age_seconds` → 返回 None（宁缺毋滥）。
3. 返回格式固定（单行，≤200 字符）：
   `"[Steven observe_only] state=<machine_state> regime=<regime> conf=<confidence> support=<top1>/resistance=<top1>；代理指标，非交易信号"`。
4. 注入点：`alert_engine` 组装 Alert 之后、`notify_payload` 之前，把 note 追加到 `Alert.detail`
   末尾（`detail + "\n" + note`，用 `dataclasses.replace`）。仅对 kind 属于
   `{intraday_price_shock, intraday_price_reclaim}` 与 `intraday_strategy.STRATEGY_KINDS` 的告警注入；
   系统/运维类告警不注入。
5. 失败（文件损坏、字段缺失）静默降级为不注入，绝不让附注失败阻断原告警。

---

## 附：service loop 集成点（实现提示，非新架构）

每轮顺序：`build_exposure_map` → 更新 bar builder → 组装 `StevenInputs`
（读 shock state / es_volume / hl_volume 的 latest 产物）→ `build_steven_signal` →
写 `latest/steven_state.json` + 条件追加 episode 行 → alert 管道按 §8 注入 context。
`steven.enabled == false` 时整段跳过（Phase 3 验收前的默认态）。
