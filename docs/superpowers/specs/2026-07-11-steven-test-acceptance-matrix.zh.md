# Steven 框架 Phase 2/3/4 测试用例矩阵与验收标准

日期：2026-07-11
状态：验收矩阵（对应测试文件已合入 `51aefde`；盘中/多日 episode 验收仍待交易日）
前置阅读：`docs/greeks-definitions.md`（公式与 golden 数值）、`docs/steven-framework-integration.md`（schema 与状态机）
已落地测试路径：`tests/test_exposure_map.py`、`tests/test_bar_builder.py`、`tests/test_steven_strategy.py`、`tests/test_steven_episodes.py`、`tests/test_steven_forward_metrics.py`。

通用约定：

- 测试命令：`uv run pytest`；lint：`uv run ruff check .`。两者全绿是每个 Phase 的硬验收项。
- 新测试文件：`tests/test_exposure_map.py`、`tests/test_bar_builder.py`、`tests/test_steven_strategy.py`、
  `tests/test_steven_episodes.py`、`tests/test_steven_forward_metrics.py`。
- Quote 构造沿用 `tests/test_options_map.py::make_option` 的模式（`InstrumentId.option("SPX", ..., trading_class="SPXW")`、
  `Provider.IBKR`、`MarketDataQuality.LIVE`、`OptionGreeks(...)`）；构造参数必须显式给
  `iv/delta/gamma/open_interest`，并新增 `volume` 入参。
- 时间基准统一用带 tz 的 UTC datetime；涉及 session 的用 `2026-07-13`（周一交易日）14:00 UTC = 10:00 ET。
- 浮点断言统一 `pytest.approx(expected, rel=1e-9)`（golden 均为解析式）；比例/ratio 用 `rel=1e-9`。

---

## Phase 2：exposure_map + bar builder + options_map 抽取

### P2-A options_map 抽取后的 golden 一致性

| 测试函数 | 输入构造要点 | 断言要点 |
| --- | --- | --- |
| `test_options_map_golden_unchanged_after_extraction` | 固定 `LatestState`：SPX 现货 7500 + greeks-definitions §0.8 的 4 条期权行（expiry 取当日 research expiry）；构建一次 `build_options_map(state)` 并把 `to_dict()` 结果与**抽取前提交生成的** `tests/golden/options_map_pre_extraction.json` 逐字段比较 | `json.dumps(payload, sort_keys=True)` 与 golden 文件完全一致（datetime 字段用固定 `now` 注入或从比较中剔除 `created_at`）；重构只许动 import，不许动数值 |
| `test_options_map_reexports_extracted_symbols` | 无状态 | `from spx_spark.options_map import build_gex_by_strike, build_wall_ladder, gex_weight, signed_gex` 仍可导入，且 `is` 于 `spx_spark.features.exposure_map` 中的同名对象 |
| `test_intraday_oi_plus_volume_weight_preserved` | 单条 0DTE call：OI=100、volume=50、gamma=0.003 | `gex_weight(quote, intraday=True) == 150.0`；`intraday=False` 时 `== 100.0`；两者驱动的 `signed_gex` 比值为 1.5 |

golden 文件生成流程（写进 PR 描述）：在抽取 commit 之前的工作树运行一个一次性脚本，把上述固定输入的
`build_options_map(...).to_dict()`（剔除 `created_at`）写入 `tests/golden/options_map_pre_extraction.json` 并提交。

### P2-B exposure_map 双权重版本

| 测试函数 | 输入构造要点 | 断言要点 |
| --- | --- | --- |
| `test_exposure_map_oi_and_volume_weighted_coexist` | greeks-definitions §0.8 输入（vendor gamma/delta 设为表中 BS 值） | 前端 expiry 的 `oi_weighted` 与 `volume_weighted` 两个 `ExposureAggregates` 同时非空；数值等于 §8 汇总表两列 |
| `test_exposure_map_strike_rows_sorted_and_paired` | 同上 | `strikes == (7500, 7550)` 升序；每行同时有 call/put 的 OI、volume、iv、delta、gamma 字段 |
| `test_net_dex_proxy_by_expiry_weighting_selector` | 同上 | `net_dex_proxy_by_expiry(exposure, weighting="oi_weighted")[expiry] ≈ 1545698.195477`；`"volume_weighted"` → `≈ −1816515.798643`；非法 weighting raise `ValueError` |
| `test_exposure_map_serialization_carries_sign_convention_fields` | 同上 | `to_dict()` 的每个 expiry 块含 `sign_convention=="calls_positive_puts_negative"`、`dealer_position_sign=="unknown"`、`direction=="unknown"`、`model=="bs_r0_q0"`、`proxy_disclaimer` 非空（greeks-definitions §9） |

### P2-C 每个 greeks 公式的数值测试（golden 输入固定为 greeks-definitions §0.8：S=7500，σ=0.20，τ=0.01 年）

| 测试函数 | 断言要点（全部 `rel=1e-9`） |
| --- | --- |
| `test_gex_strike_rows_match_golden` | K=7500 oi 权重：call_gex≈149595875.169781、put_gex≈−119676700.135825、net≈29919175.033956、abs≈269272575.305605；volume 权重与 K=7550 各值按 §1 golden 表 |
| `test_net_dex_proxy_strike_rows_match_golden` | K=7500：oi≈803856.310248、volume≈−5550199.569101；K=7550：oi≈741841.885229、volume≈3733683.770458 |
| `test_dagex_proxy_and_divergence_match_golden` | expiry 级 `dagex_proxy≈−25545046.780670`、`dagex_ratio_proxy≈−0.042486887902`、`gex_weighting_divergence≈−0.269002970809` |
| `test_vanna_per_vol_point_matches_closed_form` | K=7500→0.000199461167；K=7550→0.006481089873；call 与 put 相等 |
| `test_charm_per_minute_matches_closed_form` | K=7500→−3.79492326661e−07；K=7550→−1.23308407015e−05；call 与 put 相等 |
| `test_vex_proxy_matches_golden` | strike 级 4 个值 + expiry 级 oi≈19742.461368、volume≈65807.505536（§6 golden 表） |
| `test_cex_proxy_matches_golden` | strike 级 4 个值 + expiry 级 oi≈−37.5617605944、volume≈−125.2045386903（§7 golden 表） |
| `test_bs_edge_cases_return_none` | σ=0 / τ=0 / K=0 / S=0 时 vanna、charm 为 None，该合约不进 vex/cex 聚合且不抛异常 |
| `test_tau_floored_contract_excluded_from_cex` | as_of 距 session close < 15 分钟（τ 触 `_MIN_TIME_TO_EXPIRY_YEARS` floor）→ 该合约带 `tau_floored:` warning 且 cex_proxy 聚合不含它 |

实现提示：测试里构造 Quote 时 `OptionGreeks(implied_vol=0.20, delta=<表值>, gamma=<表值>)`，
`time_to_expiry_years` 用 monkeypatch 或选取 as_of 使 τ 恰为 0.01 年不可行——**规格决定**：
`build_exposure_map` 的内部希腊计算函数必须接受显式 `tau_years` 参数
（`strike_exposure_values(..., tau_years: float)`），数值测试直接调用该纯函数层，
端到端测试只断言字段存在性与 None 传播，不断言精确 golden。

### P2-D 质量位降级路径

| 测试函数 | 输入构造要点 | 断言要点 |
| --- | --- | --- |
| `test_missing_oi_disables_oi_weighted_only` | 全部行 `open_interest=0`，volume 正常 | `oi_quality=="stale_or_zero"`；`oi_weighted` 各指标为 None；`volume_weighted` 正常；`quality=="no_open_interest"` |
| `test_schwab_oi_flags_unverified_warning` | OI>0 的行 provider 全为 `Provider.SCHWAB` | `oi_quality=="schwab_unverified"`；`oi_weighted` 数值正常；warnings 含 `"schwab_oi_unverified"` |
| `test_missing_iv_disables_vanna_family_only` | 全部行 `implied_vol=None`，gamma/delta 正常 | `iv_source=="missing"`；vanna/charm/vex/cex 为 None；gex、net_dex_proxy 正常 |
| `test_stale_snapshot_marks_expiry_unavailable` | 全部行 `quote_time = as_of − 20 分钟`（>900s） | `quality=="unavailable"`；两个权重版全部指标 None |
| `test_low_delta_coverage_nulls_net_dex_proxy` | 4 行中 3 行 `delta=None` | `delta_coverage_ratio==0.25`；net_dex_proxy 为 None；warnings 含 `"low_delta_coverage"`；gex 不受影响 |
| `test_early_session_volume_warning` | as_of = session open + 10 分钟 | warnings 含 `"early_session_low_volume"`；dagex_proxy 数值照常输出 |

### P2-E bar builder（`tests/test_bar_builder.py`）

| 测试函数 | 输入构造要点 | 断言要点 |
| --- | --- | --- |
| `test_bar_boundary_epoch_alignment` | 样本落在 14:00:02、14:00:57、14:01:03 UTC | 前两样本进 `bar_start=14:00:00` 的 bar；第三个样本使该 bar 收盘并开新 bar |
| `test_bar_ohlc_and_sample_count` | 一分钟内 12 个 5s 样本，价格 7500→7510→7495→7505 路径 | open=首价、high=7510、low=7495、close=末价、sample_count=12、quality=="ok" |
| `test_partial_bar_flagged` | 一分钟内仅 4 个样本 | `quality=="partial"`；`bar_hold` 对含该 bar 的窗口返回 False |
| `test_empty_minute_creates_gap_not_bar` | 14:01 整分钟无样本，14:02 恢复 | 无 14:01 的 bar；14:02 bar 的 `gap_before is True`；`bar_hold` 跨 gap 返回 False |
| `test_five_minute_bars_from_closed_one_minute_bars` | 5 根完整 1m bar | 5m bar 的 OHLC 为聚合值；任一 1m 为 partial → 5m `quality=="partial"` |
| `test_latest_bars_persisted_atomically` | tmp_path 落盘 | `latest/spx_bars_1m.json` schema_version=="spx_bars.v0.1"；JSONL 追加行数等于收盘 bar 数 |

### Phase 2 验收清单（全部可勾选）

- [ ] `uv run pytest` 全绿（含新增与既有 825+ 测试，无 skip 新增）。
- [ ] `uv run ruff check .` 通过。
- [ ] `tests/golden/options_map_pre_extraction.json` 已按 P2-A 流程在抽取前生成并提交。
- [ ] P2-A/B/C/D/E 全部测试存在且通过；golden 数值与 `docs/greeks-definitions.md` §1–§8 一一对应。
- [ ] `latest/exposure_map.json` 在本地 mock/replay 数据下能生成，且含 §9 自我声明字段。
- [ ] `docs/greeks-definitions.md` 公式与实现逐条对照复查（PR checklist 项，人工）。
- [ ] service loop 集成后 IBKR 行数与 Schwab req/min 无变化（本 Phase 不动 collector）。

---

## Phase 3：strategy/steven observe_only

### P3-A 七条 hard gate（每条至少一个单测，`tests/test_steven_strategy.py`）

| # | 测试函数 | 输入构造要点 | 期望输出 |
| --- | --- | --- | --- |
| 1 | `test_gate1_missing_or_stale_anchor_forces_invalid` | 三个变体：`underlier_price=None`；`underlier_source="future:ES"`；`exposure.expiries[0].snapshot_age_seconds=1200` | 三者均 `machine_state=="DATA_INVALID"`、`status=="invalid"`；即便 regime 输入本可判 bullish |
| 2 | `test_gate2_proxy_metrics_never_raise_confidence_or_drive_regime` | 构造 vex/cex/divergence 极端值（±1e9）而 net_dex_proxy 全 None | `regime=="unknown"`；`confidence=="low"`；vex/cex 只出现在 warnings 文本；另断言 `classify_regime` 函数签名不含 vanna/vex/cex 参数（`inspect.signature`） |
| 2b | `test_gate2_confidence_never_high` | 构造最优数据（IBKR OI、全新鲜、regime 一致、trigger 确认、flow aligned） | `confidence=="medium"`（封顶生效） |
| 3 | `test_gate3_no_price_trigger_stays_watch` | regime bullish、进入 `BULLISH_DIP_WATCH`，bars 为空或从未满足 `bar_hold` | 任意多轮后 `machine_state` 仍是 WATCH、`status=="watch"`、`trigger.confirmed is False` |
| 4 | `test_gate4_episode_rejects_backfilled_timestamps` | 构造 episode 行 `recorded_at < contract.as_of` | 写入函数 raise `ValueError`（信息含 "retrospective"） |
| 5 | `test_gate5_active_shock_forces_event_wait` | `shock_state` 含 `phase=="shock_confirmed"` 的未完成事件；regime bullish、trigger 本可确认 | `machine_state=="EVENT_WAIT"`、`status=="watch"`；T3 优先于 T9 |
| 6 | `test_gate6_hyperliquid_never_used_as_anchor` | `LatestState` 只有 `crypto_perp:xyz:SP500` 报价，无 `index:SPX`、无可用链 | `inputs_from_latest_state` 产出 `underlier_price is None` → `DATA_INVALID`；对照断言 micopedia 式 HL fallback **不存在**（`underlier_source != "crypto_perp:xyz:SP500"`） |
| 7 | `test_gate7_expression_family_enum_is_bounded` | 遍历状态机所有可达状态的输出 | `expression_family ∈ {"none","bullish_defined_risk","bearish_defined_risk","range_defined_risk"}`；非 `SETUP_CONFIRMED` 状态恒为 `"none"`；contract JSON 全文不含裸卖类词（naked/unbounded） |

### P3-B 状态机逐条转移测试（每条转移一个测试）

统一模式：构造 `StevenInputs(previous_state=<from>, ...)` 满足该转移条件 → 断言
`advance_state(...) == <to>`；再构造「条件差一点」的反例断言不转移。参数全部经
`monkeypatch` runtime_value 或直接传 settings dataclass（实现应提供 `StevenSettings.from_env()`
与显式注入两条路，同 `IntradayShockSettings` 模式）。

| 测试函数 | 转移 |
| --- | --- |
| `test_t1_any_state_to_data_invalid` | T1（参数化 from 状态全集） |
| `test_t2_data_invalid_recovers_after_hold` | T2（含未满 `data_recovery_hold_seconds` 的反例） |
| `test_t3_event_tags_or_shock_to_event_wait` | T3（fomc tag 与 shock 两个变体） |
| `test_t4_event_wait_exits_after_stabilization` | T4（5 根小区间 bar；含一根大区间 bar 的反例） |
| `test_t5_unknown_or_mixed_regime_to_regime_unknown` | T5 |
| `test_t6_bullish_near_support_enters_dip_watch` | T6（含距离 > `dip_watch_max_distance_points` 反例） |
| `test_t7_bearish_near_support_enters_break_watch` | T7 |
| `test_t8_mixed_pin_conditions_enter_range_pin_watch` | T8（含 `net_gamma_ratio < pin_min_net_gamma_ratio` 反例） |
| `test_t9_dip_hold_trigger_confirms_setup` | T9（touch + 2 根 hold bar；flow opposed 反例不转移） |
| `test_t10_break_hold_trigger_confirms_setup` | T10 |
| `test_t11_range_reject_trigger_confirms_setup` | T11 |
| `test_t12_watch_exits_on_regime_flip_with_hold` | T12 |
| `test_t13_target_or_invalidation_enters_exit_review` | T13（target 触达与 invalidation hold 两个变体） |
| `test_t14_data_loss_during_setup_enters_exit_review` | T14 |
| `test_t15_exit_review_always_proceeds_to_lockout_or_remap` | T15 |
| `test_t16_lockout_expires_or_daily_cap_holds` | T16（冷却未满、已满、`max_daily_setups` 触顶三个变体） |
| `test_t17_trading_date_rollover_resets_state` | T17 |

### P3-C 稳定输出与 episode 合并

| 测试函数 | 输入构造要点 | 断言要点 |
| --- | --- | --- |
| `test_weekend_or_empty_inputs_stable_observe_only_or_invalid` | 参数化：空 LatestState、周六 as_of、exposure None、bars 空、shock/es/hl 全 None 的组合 | 不抛异常；`status ∈ {"observe_only","invalid"}`；`regime=="unknown"`；`expression_family=="none"`；连续调用 3 次输出稳定（幂等） |
| `test_contract_json_validates_against_schema` | 任意合法输出 | 用 `jsonschema` 或手写校验断言 §2.1 required/enum/additionalProperties 全满足 |
| `test_episode_one_per_day_and_seq_monotonic` | 同一 trading_date 连续 5 次状态边沿 | JSONL 中 `episode_id` 唯一、`seq` 0..4 连续、seq 0 是 `pre_market_map` |
| `test_episode_revision_only_on_edges_or_level_moves` | 连续 10 轮无状态变化、map 位移 < 阈值 | 不追加新行；map 位移 ≥ `episode_revision_min_level_move_points` 时追加 `map_revision` 行 |
| `test_episode_final_state_written_on_exit_review` | 走完 T9→T13→T15 | 存在 `event_kind=="final_state"` 行且 `final_state=="LOCKOUT_OR_REMAP"` 在折叠对象中 |
| `test_steven_state_file_corruption_resets_gracefully` | 状态文件写入非法 JSON | 下一轮 `previous_state=="OBSERVE_ONLY"`；warnings 含 `"steven_state_reset:"` 前缀 |
| `test_alert_context_note_is_readonly_and_bounded` | 构造 shock Alert + steven_state | 注入后 `severity`/`kind` 不变；detail 追加单行 ≤200 字符且含 "observe_only"；`steven_state` 过期（> `alert_context_max_age_seconds`）→ 返回 None；`alert_context_enabled=false` → 不读文件（用 mock 断言零调用） |

### Phase 3 验收清单

- [ ] `uv run pytest` 全绿；`uv run ruff check .` 通过。
- [ ] P3-A 七条 gate 测试逐条对应 integration 文档 §4 映射表，缺一不可。
- [ ] P3-B 覆盖 T1–T17 全部转移（含每条至少一个不转移反例）。
- [ ] 周末/数据缺失输入下（P3-C 第一行的全部参数化组合）稳定输出 `observe_only|invalid`。
- [ ] `latest/steven_state.json` 与 episode JSONL 的 schema 与 integration 文档 §3/§5 逐字段一致。
- [ ] alert 附注：人工核查一条真实（或 replay）shock 告警文本，severity/kind 未变，附注在末尾。
- [ ] 连续 3 个交易日运行：每日 episode 文件存在、`seq` 连续、`pre_market_map` 齐全、无未处理异常日志（运维检查项，非 pytest）。
- [ ] `steven.enabled` 默认 false 已确认（配置验收）。

---

## Phase 4：验证框架（forward metrics + 基线对比）

### forward_metrics schema（episode 折叠对象的占位在此定型）

```json
{
  "computed_at": "2026-07-14T21:30:00+00:00",
  "reference_price": 7495.0,
  "reference_at": "2026-07-13T14:31:05+00:00",
  "direction_hypothesis": "up | down | range",
  "horizons": {
    "t_plus_5m":  { "price": 7501.0, "return_bps": 8.0,  "sample_gap_seconds": 3.0 },
    "t_plus_15m": { "price": null,   "return_bps": null, "sample_gap_seconds": null },
    "t_plus_30m": { "...": "同上" },
    "t_plus_60m": { "...": "同上" },
    "t_close":    { "...": "同上" }
  },
  "mfe_bps": 22.0,
  "mae_bps": -6.5,
  "level_outcomes": {
    "trigger_level": 7490.0,
    "touched": true, "touched_at": "...",
    "reclaimed": false, "reclaimed_at": null,
    "accepted": true, "accepted_at": "..."
  },
  "quality": "ok | partial_bars | missing_bars"
}
```

判定定义（写死在实现与测试里）：

- **reference**：`SETUP_CONFIRMED` 时刻（`trigger.confirmed_at`）的最近一根已收盘 1m bar close；
  无 setup 的 episode 用 seq=0 的 `as_of`（此时 `direction_hypothesis="range"` 仅作统计基线）。
- **T+X**：reference_at + X 分钟处，取距离 ≤ 30s（沿用 `intraday_event_outcomes.max_horizon_sample_distance_seconds` 的口径）内最近 bar close；超距 → 该 horizon 全 null。
- **MFE/MAE**：reference_at 到 min(reference_at+60m, session close) 窗口内，
  按 `direction_hypothesis` 方向的最大有利/最大不利 return（bps，基于 1m bar 的 high/low）；
  `range` 假设下 MFE/MAE 双向取 |偏离| 的 max/min，符号约定写入测试注释。
- **touched**：bar 的 low ≤ level ≤ high；**reclaimed**：touched 后出现 `bar_hold(level, 原方向侧, 2)`；
  **accepted**：touched 后出现 `bar_hold(level, 突破侧, 2)`。三者都限 reference_at 之后、收盘之前。

### P4 测试矩阵（`tests/test_steven_forward_metrics.py`）

| 测试函数 | 输入构造要点 | 断言要点 |
| --- | --- | --- |
| `test_horizon_returns_from_synthetic_bars` | 手工 bar 序列：reference=7500，+5m close 7506、+15m 7494、+30m/60m/close 已知 | `t_plus_5m.return_bps==8.0`、`t_plus_15m.return_bps==−8.0`…（bps = (P/P0−1)×1e4，精确到 approx） |
| `test_horizon_null_when_bar_gap_exceeds_limit` | +15m 处 bar 缺失（gap） | 该 horizon 三字段全 null，`quality=="partial_bars"` |
| `test_mfe_mae_direction_up_and_down` | reference=7500；窗口内 bar high 最大 7516.5、low 最小 7495.125；up 与 down 假设各跑一次同一路径 | up：mfe==22.0、mae==−6.5；down：mfe==6.5、mae==−22.0（bps，符号镜像，`rel=1e-9`） |
| `test_touch_reclaim_accept_judgments` | level=7490；路径：跌破→2 根收在下方→反弹 2 根收在上方 | `touched==True`、`accepted==True`（先发生）、`reclaimed==True`（后发生，时间戳次序断言） |
| `test_close_horizon_uses_session_close_bar` | early-close 日（用 `MarketCalendar` 的 7/3 或感恩节次日） | `t_close` 取 13:00 ET 前最后一根 bar，而非 16:00 |
| `test_forward_metrics_recomputable_from_lake_bars` | 同一 episode + `lake/steven/bars/.../spx_bars_1m.jsonl` 重放两次 | 两次输出逐字段相等（确定性/可复算性）；输入 bars 乱序时结果不变（内部按 bar_start 排序） |
| `test_no_setup_episode_gets_range_baseline_metrics` | 无 SETUP_CONFIRMED 的 episode | forward_metrics 仍生成，`direction_hypothesis=="range"`，reference 取 seq=0 |
| `test_post_close_review_attaches_steven_episode_block` | 走 `build_review_payload_from_data` 风格的纯数据入口 | review payload 含 `steven_episode` 键，`forward_metrics` 非 null，完整性 verdict 不因 steven 块缺失而改变（shadow 层不影响既有 verdict，与 0DTE greeks reference 同款约束） |

### 基线对比的可复算性要求（离线脚本 + 测试）

三条基线，与假设 1–6 的对照结果必须可由同一份 lake 数据重放复算：

| 基线 | 定义（可实现为纯函数） | 测试 |
| --- | --- | --- |
| 无条件收益 | 每日 09:35 ET 买入假设（方向恒 up），同款 horizons/MFE/MAE | `test_baseline_unconditional_matches_manual_example`：手工 bar 序列给定期望值 |
| 开盘区间策略 | 09:30–10:00 区间高低点突破方向作为 direction_hypothesis | `test_baseline_opening_range_direction_rules`：区间内/上破/下破三变体 |
| GEX-only 图 | 只用 oi_weighted 墙（不看 DEX/regime）：靠近 put wall 即 up 假设 | `test_baseline_gex_only_uses_walls_without_dex`：断言其判定函数不读 net_dex_proxy 字段 |

复算性验收：提供 `uv run python -m spx_spark.strategy.steven_replay --date YYYY-MM-DD`（或等价入口），
两次运行同日数据输出字节级一致（时间戳字段用数据内时间，不用 wall clock）。

### Phase 4 验收清单

- [ ] `uv run pytest` 全绿；`uv run ruff check .` 通过。
- [ ] 每个有数据的交易日 episode 自动生成 `forward_metrics`（post_close_review 挂载，`quality` 忠实标注 partial/missing）。
- [ ] P4 测试矩阵全部存在并通过；MFE/MAE 与 touch/reclaim/accept 的判定定义与本文档逐字一致。
- [ ] 三条基线各有可复算对照输出；假设 1–6（规划文档）各对应至少一组「Steven vs 基线」数字，写入周报告。
- [ ] 重放入口两次运行输出一致（确定性验收）。
- [ ] 结果解读文档明确声明：结论仅对自家 `_proxy` 指标成立，不构成对 Steven 原框架的验证（规划文档风险 2 的落地条款）。
