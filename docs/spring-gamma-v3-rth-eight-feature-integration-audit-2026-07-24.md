# Spring Gamma v3 RTH 八变量接入审计

日期：2026-07-24

> 状态：这是本次改动前的接入审计快照，用来保留根因和输入合同证据。
> 审计提出的 ES 5m bar、八变量 extractor、RTH 状态/option overlay 解耦及报告
> 接线已在同一批改动中实现；当前运行合同见
> `docs/rth-five-minute-market-state-v1.md`。

## 结论

用户指定的八项 RTH 变量是：

1. `price_vs_vwap`
2. `vwap_slope`
3. `opening_range_state`
4. `market_structure`
5. `efficiency_ratio`
6. `vwap_cross_count`
7. `same_time_range_ratio`
8. `breadth_above_vwap`

现有 Spring Gamma v3 并没有使用这套八变量。它的 ES 主干目前使用
5/15/60/180 分钟点数收益、VWAP 距离、VWAP 斜率和 60 分钟趋势效率；
30 分钟收益只作诊断且可能由 15/60 分钟线性插值得到。

八项里，只有 `price_vs_vwap`、`vwap_slope` 和 `efficiency_ratio`
存在直接近似字段；`market_structure` 有不完整的上游组件；
`opening_range_state` 只存在于未接入 v3 的 Steven 验证路径；
其余三项尚未实现。十一只行业 ETF 的实时源已经存在，但当前 breadth
计算的是相对昨收的涨跌广度，不是 `breadth_above_vwap`。

独立纯 scorer `market_state_5m.py` 已经定义八输入合同、D/Q/V 分解和六状态
规则，但它只消费“已经计算好的八项值”，不负责生产这些值。目前还没有
input extractor、持久化、Spring v3 runtime 接线或 15 分钟报告接线。

RTH 看不到状态的另一个独立原因是：现有 v3 把精确到期日的 IV、Greeks、
OI/strike coverage 和新鲜度都放在顶层 gate。任何一个期权结构输入缺失或
超过 15 秒，ES 方向诊断也会被强制降为 `ABSTAIN`。市场状态与期权表达
应该拆开：市场状态可以继续只读展示；期权结构缺失只阻断期权表达，
不能抹掉已经可计算的 RTH 市场状态。

本方案只增加 research shadow。以下字段必须保持：

- `direction_authority = "none"`
- `action_authority = "none"`
- `actionable = false`
- `automatic_ordering = false`

它不得进入 production guidance、候选、限价、trade intent 或下单路径。

## 当前数据流

- `market_features` 每 5 秒运行一次，但市场路径按一分钟间隔持久化。
- `normalized_market_sample()` 保存采样时刻 `at`，同时在每个 quote 内保存
  `source_at` 和 `transport_at`。
- `build_minute_market_frame()` 当前主要按 sample `at` 排列路径，而不是按
  quote `source_at` 排列。
- `session_id` 是 18:00 ET 翻日的 Globex business date，不是事件时间。
- 报告展示用北京时间；会话定义和 opening range 必须用
  `America/New_York` 与交易日历；持久化时间统一用带时区 UTC。
- Spring Gamma v3 每分钟生成 shadow，15 分钟报告只挂载 120 秒以内、
  expiry/session/session segment 全部一致的最新 shadow。

## 八变量逐项审计

| 变量 | 现有输入 | 当前可用性 | 主要语义或质量问题 | 最小修复 |
|---|---|---:|---|---|
| `price_vs_vwap` | `minute_market_frame.es.vwap_distance_points` | 部分可用 | scorer 要求含“两根 5m close 确认”的枚举 `ABOVE_CONFIRMED/ABOVE/AROUND_OR_CROSS/BELOW/BELOW_CONFIRMED`，当前只有数值距离；当前 `session_vwap` 覆盖整个 Globex `session_id`，不是 09:30 ET 重置的 RTH VWAP；VWAP provider 可能与当前 ES price provider 不同；缺少最小分钟覆盖与最大 gap 检查 | 新增 provider-consistent、RTH-only、因果 VWAP；在明确 deadband 外用两根已闭合 5m close 确认枚举；同时保留 raw points、provider、source window 和 coverage |
| `vwap_slope` | `es.vwap_slope_15m_points` | 部分可用 | scorer 合同是 `(VWAP_t - VWAP_t-3) / ATR_5m`；当前字段既没有 ATR 归一，也不是三个 5m closed bars 的 session-VWAP 差；开盘窗口还可能混入盘前 | 新增明确命名的 normalized slope，只用 RTH closed 5m source-time rows；ATR 缺失或三个 lag 不完整时 fail closed |
| `opening_range_state` | `steven_validation.opening_range_direction()` | 当前 v3 不可用 | scorer 09:45 ET 起允许分类，要求 09:30–09:45 的 15m OR 和两根 5m close 确认枚举；Steven 使用 09:30–10:00 的 30m OR，语义不兼容，且使用 SPX event-sampled bars、未挂进 minute frame/v3；Steven 每轮恢复 closed bars 但不恢复 open partial bar，线上 bar lake 可长期为空；回测函数未先裁掉未来 opening bars，无数据还会误写成 `range` | 不复用 Steven 30m OR；用 ES closed 5m bars 构造 09:30–09:45 ET OR；所有输入先裁到 `source_at <= as_of`；输出 scorer 已声明的五个枚举 |
| `market_structure` | `lower_high_60m`、`higher_low_60m`、prior/recent swing extrema | 部分可用 | scorer 要求 `HH_HL/HH_ONLY/HL_ONLY/OVERLAP/LH_ONLY/LL_ONLY/LH_LL`；当前仅四个点就可出结果，按 row count 对半切而非固定时间窗，没有 span、gap 或分钟覆盖门控 | 用 fixed source-time windows 或 closed 5m ES bars，因果地产出 scorer 枚举与 coverage |
| `efficiency_ratio` | `trend_efficiency_60m` | 部分可用 | 公式 `abs(net move)/sum(abs step))` 正确，但只要求两个样本；两个稀疏点必然得到 1.0，正好把数据缺口误判为高趋势效率 | 保留公式，增加最小 span、有效分钟率、最大 gap；窗口写入字段名或 metadata；不足时返回 missing reason，不能补 0 |
| `vwap_cross_count` | 无 | 不可用 | 现有 frame 只有最终 VWAP，没有每分钟因果 VWAP 序列；直接拿最终 VWAP 回看历史会产生 time travel；没有 deadband/hysteresis 会把微小噪声算成反复穿越 | 从每分钟累积成交量因果更新 VWAP；用 ES tick-size/波动归一 deadband；统计固定 trailing window 内已确认 cross，并输出 window 与 cross timestamps |
| `same_time_range_ratio` | 无；只有 same-clock volume pace baseline | 不可用 | 当前 state 只保留 18 小时市场样本，没有历史同进度 range profile；若用全天最终 range 作分母会前视 | `current RTH range through minute k / median(prior N valid sessions range through minute k)`；基线只含更早交易日，按 `minutes_since_rth_open` 分桶并保留样本数/MAD |
| `breadth_above_vwap` | 十一只行业 ETF 实时 quotes 已配置；`spx_sector_breadth` 为相对昨收广度 | 源可用、目标特征不可用 | `TRACKED_INSTRUMENTS` 不保存行业 ETF 历史；当前 breadth 比较 prior close，不比较各自 VWAP；不能把该 proxy 写成 500 个成分股 breadth；GTH equity quote 不连续 | 保存十一只行业 ETF 的 RTH minute price/cumulative-volume path；逐 ETF 算因果 RTH VWAP；至少 8 只 fresh/valid 后发布比例，并声明 `universe=sp500_sector_etfs` |

## 时间、粒度和防前视合同

### 事件时间

所有计算以 quote `source_at` 为主键。`sample.at` 只表示 feature worker 的
观察/计算时间，不能代替市场事件时间。每个 source minute 只保留最后一个
合法 observation，并记录：

- `source_window_start`
- `source_window_end`
- `computed_at`
- `valid_minute_count`
- `expected_minute_count`
- `coverage_ratio`
- `max_gap_seconds`
- `provider`
- `missing_reason`

如果 provider 切换或累计 volume reset，不得跨 reset 直接做 volume delta。

### 会话和时区

- 内部时间：timezone-aware UTC。
- RTH 切片与 opening range：`America/New_York`，使用
  `MarketCalendar.session()`，不能写死 UTC 偏移。
- 报告显示：`Asia/Shanghai`，只在 presentation 层转换。
- `session_id` 只作会话 identity，不参与时间差计算。
- `same_time_range_ratio` 按 `minutes_since_rth_open` 对齐，避免 DST 与半日市
  的同钟点误配。
- 该 scorer 的 opening range 只使用 `[09:30, 09:45)` ET 的三根已闭合
  5m bars；09:45 ET 以前一律 `UNCERTAIN`。
- 15 分钟报告只使用 report as-of 前最后一个完整分钟，不能读取正在形成的
  minute 或该时点之后补写的 bar。

### RTH/GTH 边界

这套八变量是 RTH 状态模型。GTH 时：

- `opening_range_state = not_applicable`
- `breadth_above_vwap = not_applicable`
- RTH `same_time_range_ratio` 不得复用
- 八变量 scorer 输出 `UNCERTAIN`
- 现有 ES-only GTH diagnostic 可以独立保留，但不得伪装成八变量 RTH 状态

## 六状态合同

对外只允许六类：

1. `TREND_UP`
2. `TREND_DOWN`
3. `LOW_VOL_RANGE`
4. `HIGH_VOL_CHOP`
5. `LOW_VOL_PIN`
6. `UNCERTAIN`

`market_state_5m.py` 已经实现纯 scorer 和边界测试。它要求八项全部可用，
任一缺失即 `UNCERTAIN`，不会像当前 composite 一样对剩余字段静默重新归一。
`LOW_VOL_PIN` 当前只保留枚举；满足 proxy 条件时仍保守输出
`LOW_VOL_RANGE`，并标记 `pin_proxy_candidate=true` /
`pin_confirmation=proxy_unconfirmed`，不得由市场特征直接升级为 pin。

建议新增 bounded contract：

```json
{
  "rth_market_state": {
    "schema_version": "market_state_5m.v1",
    "rule_version": "market_state_5m_eight_variable_rules.v1",
    "as_of": "2026-07-24T14:15:00+00:00",
    "session": "rth",
    "state": "TREND_UP",
    "status": "ready",
    "calibration_status": "uncalibrated_shadow",
    "direction_authority": "none",
    "action_authority": "none",
    "actionable": false,
    "input_availability": {
      "available_count": 8,
      "required_count": 8,
      "complete": true
    },
    "features": {
      "price_vs_vwap": {
        "value": "ABOVE_CONFIRMED",
        "raw_distance_points": 6.25,
        "status": "ready"
      },
      "vwap_slope": {
        "value": 0.31,
        "formula": "(VWAP_t - VWAP_t_minus_3x5m) / ATR_5m",
        "status": "ready"
      },
      "opening_range_state": {
        "value": "ABOVE_ORH_CONFIRMED",
        "status": "ready"
      },
      "market_structure": {
        "value": "HH_HL",
        "status": "ready"
      },
      "efficiency_ratio": {
        "value": 0.66,
        "status": "ready"
      },
      "vwap_cross_count": {
        "value": 1,
        "status": "ready"
      },
      "same_time_range_ratio": {
        "value": 1.00,
        "baseline_sessions": 20,
        "status": "ready"
      },
      "breadth_above_vwap": {
        "value": 0.66,
        "universe": "sp500_sector_etfs",
        "usable": 11,
        "minimum_usable": 8,
        "status": "ready"
      }
    }
  }
}
```

在 walk-forward 回测完成前，`state` 只是 research label。scorer 的规则阈值
虽已固定为版本化初始合同，但不能称为已校准概率或生产信号。

## 顶层 gate 解耦

现有 v3 的期权 gate 应继续阻断“期权结构可用/表达可用”，但不再覆盖
`rth_market_state.state`：

```text
rth_market_state.status
    只由八变量 source-time coverage 决定

option_overlay.status
    由 exact expiry、IV/Greek/OI/strike coverage、freshness 决定

top-level actionable / order authority
    始终 false / none
```

兼容性最小的做法是保留现有顶层 `status` 和 `direction.decision` 行为，
只新增 `rth_market_state`。这样期权 gate 仍可让顶层 `ABSTAIN`，但报告能够
诚实显示“市场状态可算、期权结构不可用”，而不是把两者合并成“完全无信号”。

## 最小代码接入方案

为避免与参数敏感性模块冲突，拆成四个小步骤：

1. 保留现有纯 scorer
   `application/market_features/market_state_5m.py`；另建
   `market_state_5m_inputs.py`，只负责八项 causal input、质量 metadata，
   不复制或修改 scorer 规则。
2. 扩展 `market_feature_state.json`：
   增加 prior-session same-progress range profiles 和行业 ETF minute paths；
   所有 baseline 更新发生在当前 frame 计算之后，并排除当前 `session_id`。
3. 在 Spring Gamma v3 runtime 组合结果时只附加
   `rth_market_state` 和 `option_overlay`，不把它们传给 production
   `build_decision_guidance()`、candidate builder 或 trade intent。
4. presentation 从单行改为 bounded 四行；writer payload 只保留 state、
   coverage、一个已有 level gate、trigger phase 和 option quality，不传入
   原始路径或全量 feature history。

## 15 分钟报告文案

固定顺序为“状态 → 等待位置 → 触发确认 → 期权结构”：

```text
Spring Gamma v3 Shadow · 状态 TREND_UP（RTH研究；8/8；READY）
等待位置 · Flip High 7505.00，现价距位 -20.00 点；沿用既有 level gate，仅观察，不是入场位
触发确认 · RETEST，等待既有 level_decision=CONFIRMED；Shadow 不自建触发
期权结构 · UNAVAILABLE（iv_surface_stale）；市场状态保留、期权表达弃权；无方向/执行权限
```

约束：

- “等待位置”只能逐字使用已验证的 `level_gate`，不能由六状态模型新造墙位。
- “触发确认”只能读取既有 level decision phase，不能自行宣告 confirmed。
- “期权结构”只展示 exact-expiry、live/frozen、coverage 和缺失原因。
- 不展示推荐 Call/Put、strike、spread、限价、概率或订单动作。
- 所有数值最多保留两位小数。
- 如果没有 `rth_market_state`，保留当前旧 Shadow 单行作为兼容 fallback。

## 必须增加的集成测试

1. 八变量 ready、期权结构 stale：报告仍显示六状态，同时显示
   `option structure unavailable`；顶层仍不可执行。
2. 同一 production payload 分别挂载 `TREND_UP`/`TREND_DOWN` shadow：
   production guidance、candidate、fingerprint、trade intent 完全相同。
3. 四行严格按“状态、等待位置、触发确认、期权结构”排序。
4. opening range 回测输入先裁到 as-of；未来 bar 不改变当时结果。
5. 两个稀疏点不能令 `efficiency_ratio` 变成 ready/1.0。
6. `same_time_range_ratio` 的分母只含更早 session；当前日和未来日不得进入。
7. sector breadth 少于 8 只、quote stale 或 provider timestamp skew 超限时，
   返回 unavailable 而不是 0。
8. RTH 与 GTH、expiry、session_id 或未来 timestamp 交叉时 fail closed。
9. NBBO、IV 或价格不得插值；仅研究用连续结构函数可以平滑。

## 当前已有的安全边界

以下边界已经存在，应保留：

- report projection 校验 schema、未来时间、最大年龄、expiry、session_id 和
  RTH/GTH segment。
- writer summary 是 bounded subset。
- opposite Shadow 已有测试证明不能改变 production guidance、候选或
  fingerprint。
- Shadow authority 递归校验会拒绝嵌套 production authority。

因此最小变更不需要重写 order 或 guidance 层；重点是补齐八变量的数据合同，
修复 source-time/coverage，并把市场状态从期权结构 gate 中分离。
