# 量化概率层与 SPY 墙位对照设计(实施规格)

状态: 2026-07-07 设计完成,待实施。
分层约束: 遵守 `module-architecture.md`。本设计不新增跨层依赖:
`options_map`(L3)只读 `LatestState` 中的 quotes,不 import provider 包;
新增 `schwab/collector.py` 属 L2,只向下依赖 L0/L1;`service_loop`(L5)可以 import 它。

包含四个功能块,可独立实施、独立测试:
- A. delta 概率层(options_map)
- B. gamma 分布与翻转临界区暴露(options_map + human_focus)
- C. skew/vix_ratio 进 Micopedia + 自动接线补全
- D. SPY 墙位对照 wall_confluence(schwab collector + options_map)
- E. 告警文案引用概率
- F. 测试清单与验收标准

---

## A. delta 概率层(`src/spx_spark/options_map.py`)

原理: 0DTE 下期权 delta ≈ 风险中性的"收盘价越过该 strike"概率;
触及概率用 barrier 近似 ≈ 2×收盘越过概率(上限 1.0)。

### A1. 新 dataclass(放在 `StrikeGex` 之后)

```python
@dataclass(frozen=True)
class LevelProbability:
    level_name: str            # "put_wall" | "zero_gamma" | "call_wall"
    level: float
    prob_close_beyond: float | None   # 收盘越过该位(向远离当前价方向)的概率
    prob_touch: float | None          # 日内触及该位的概率 = min(1.0, 2*prob_close_beyond)
    source_strike: float | None       # 概率取自哪个 strike 的 delta
    source_delta: float | None        # 该 strike 的原始 delta

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
```

### A2. 新函数

```python
def usable_delta(quote: Quote | None) -> float | None:
    """quality 不在 BAD_QUALITIES、greeks 存在且 delta 有限时返回 delta,否则 None。"""

def median_strike_step(strikes: list[float]) -> float:
    """已排序 strikes 的相邻差值取中位数;不足 2 个 strike 时返回 5.0。"""

def probability_for_level(
    level: float,
    *,
    underlier: float,
    pairs: dict[float, dict[OptionRight, Quote]],   # 来自现有 pair_by_strike
    strike_step: float,
) -> tuple[float | None, float | None, float | None, float | None]:
    """返回 (prob_close_beyond, prob_touch, source_strike, source_delta)。

    规则:
    - level >= underlier: 在 pairs 中找离 level 最近、且 CALL 腿 usable_delta 非 None
      的 strike;prob_close_beyond = clamp(call_delta, 0.0, 1.0)。
    - level <  underlier: 同上但用 PUT 腿;prob_close_beyond = clamp(abs(put_delta), 0.0, 1.0)。
    - 若最近可用 strike 与 level 的距离 > 2*strike_step,返回全 None(数据不够近,不硬算)。
    - prob_touch = min(1.0, 2 * prob_close_beyond)。
    """
```

### A3. `ExpiryOptionsMap` 加字段

```python
level_probabilities: tuple[LevelProbability, ...] = ()
```

`build_expiry_map` 中,在 walls/zero_gamma 计算完成后:
对 `("put_wall", put_wall)`、`("zero_gamma", zero_gamma)`、`("call_wall", call_wall)`
三项中 level 非 None 者各生成一条 `LevelProbability`(调 `probability_for_level`,
underlier 缺失则跳过整个概率层,保持 `()`)。
`to_dict` 用现有 `asdict` 递归即可,不需要特殊处理。

---

## B. gamma 分布与翻转临界区

### B1. `options_map.py`:`ExpiryOptionsMap` 加字段

```python
gamma_flip_zone: tuple[float, float] | None = None
```

新函数(放 `nearest_zero` 旁边,逻辑镜像它):

```python
def zero_gamma_bracket(gex_rows: list[StrikeGex], underlier: float) -> tuple[float, float] | None:
    """返回离 underlier 最近的 net_gex 变号区间 (left.strike, right.strike)。
    若某行 net_gex 恰为 0(abs<=1e-12),返回 (strike, strike)。无变号返回 None。
    选择规则与 nearest_zero 一致:取插值零点距 underlier 最近的那个 crossing。"""
```

`build_expiry_map` 计算并填入。

### B2. `human_focus.py`:`expiry_options_summary` 追加两个键

```python
"level_probabilities": [lp.to_dict() for lp in expiry.level_probabilities],
"gamma_profile": {
    "zero_gamma": expiry.zero_gamma,
    "flip_zone": list(expiry.gamma_flip_zone) if expiry.gamma_flip_zone else None,
    "net_gamma_ratio": expiry.net_gamma_ratio,
    "top_strikes": [
        {
            "strike": row.strike,
            "net_gex": row.net_gex,
            "call_oi": row.call_open_interest,
            "put_oi": row.put_open_interest,
        }
        for row in expiry.top_gex_strikes[:6]
    ],
},
```

---

## C. skew/vix_ratio 进 Micopedia(`src/spx_spark/strategy/micopedia.py`)

### C1. `MicopediaInputs` 加字段(带默认值,放在 `vix` 之后)

```python
skew_index: float | None = None       # CBOE SKEW 指数
put_skew_ratio: float | None = None   # SPXW 前月 25delta put IV / ATM IV(来自 options_map)
```

加只读 property:

```python
@property
def vix_ratio(self) -> float | None:
    """VIX1D/VIX。>=0.95 视为事件定价日;<0.65 视为隔夜 vol 便宜。"""
    if self.vix1d is None or self.vix is None or self.vix <= 0:
        return None
    return self.vix1d / self.vix
```

### C2. `classify_regime` 增强

在 event-tags 判断之后、`vix1d >= 25` 判断之前插入:

```python
if inputs.vix_ratio is not None and inputs.vix_ratio >= 0.95:
    return "high_vol_event"
```

### C3. 新函数 `classify_dip_context`

```python
def classify_dip_context(inputs: MicopediaInputs) -> str:
    """dip 性质分类,输出四选一:
    - dip_acceleration_risk: 尾部保护贵(SKEW>=150 或 put_skew_ratio>=1.15)
      且 gamma_state in {negative, transition} → dip 易被对冲盘放大,不接飞刀。
    - expensive_tail_protection: 仅尾部保护贵 → dip 由对冲流驱动的概率高,存疑。
    - dip_buy_friendly: vix_ratio < 0.65 且尾部保护不贵 → 无事件定价、隔夜 vol
      便宜,纪律性逢低买回可行。
    - neutral: 其余。
    """
```

### C4. `MicopediaSignal` 加字段 `dip_context: str`

`build_micopedia_signal` 填入;`print_signal` 增打印一行;`to_dict` 走 asdict 自动。

`trigger_watchlist` 末尾按 dip_context 追加一条(neutral 不加):
- dip_acceleration_risk: "Dip context: tail protection is bid while gamma is negative/transition; treat dips as potential acceleration, do not knife-catch."
- expensive_tail_protection: "Dip context: SKEW/put skew is rich; check whether hedging flow rather than opinion is driving any dip."
- dip_buy_friendly: "Dip context: overnight/event vol is cheap; disciplined dip-buy setups are viable after level confirmation."

### C5. 自动接线(`src/spx_spark/human_focus.py` 的 `micopedia_context`)

构造 `MicopediaInputs` 时补三个入参:

```python
skew_index=effective_price(state, "index:SKEW"),
put_skew_ratio=(front.put_skew_ratio if front else None),
event_tags=tuple(env_csv("MICOPEDIA_EVENT_TAGS", "")),
```

`env_csv` 从 `spx_spark.config` import(L3→L0 合法)。
`micopedia_context` 返回 dict 增加 `"dip_context": signal.dip_context` 和
`"vix_ratio": inputs.vix_ratio`。

`.env.example` 增(带注释):

```
# 当日事件标签,逗号分隔(fomc/cpi/nfp/opex/jpm_collar/month_end...),盘前人工维护
MICOPEDIA_EVENT_TAGS=
```

`.env` 增 `MICOPEDIA_EVENT_TAGS=`(空值)。

---

## D. SPY 墙位对照 wall_confluence

前提说明: Schwab token 目前不存在(`/srv/data/spx-spark/runtime/schwab-token.json`
缺失),所以本块要实现成**默认关闭、缺数据时优雅降级**,token 就绪后打开即用。

### D1. 新模块 `src/spx_spark/schwab/collector.py`(L2)

职责: 拉 SPY 当日期权链 → 归一化 → 持久化进 LatestState。

```python
"""Schwab option-chain collector: fetch chains and persist normalized quotes."""

def fetch_chain(client: SchwabClient, symbol: str, settings: SchwabSettings) -> Any:
    """GET /marketdata/v1/chains,params:
    symbol, contractType=ALL, strategy=SINGLE,
    strikeCount=settings.option_chain_strike_count,
    includeUnderlyingQuote=true,
    fromDate=今天(America/New_York), toDate=明天(America/New_York)  # 只取 0/1DTE
    格式 yyyy-MM-dd。"""

def run(argv: list[str] | None = None) -> int:
    """CLI 主入口:
    1. settings = SchwabSettings.from_env(); token = load_access_token(settings)
       (两者从 spx_spark.schwab.verifier import——同包内复用)。
    2. token 为空: print json {"ok": false, "skipped": true, "reason": "missing_schwab_token"}
       并 return 0(可选通道,不能让 service loop 报错刷屏)。
    3. symbols = env_csv("SCHWAB_COLLECT_CHAINS", "SPY")。
    4. 对每个 symbol: fetch_chain → snapshot_from_chain_payload(来自 schwab.adapter,
       underlier=symbol) → persist_provider_snapshot(来自 provider_adapter)。
       HTTP 异常捕获后计入 errors,继续下一个 symbol。
    5. print json 汇总 {"ok": bool, "symbols": [...], "quote_counts": {...}, "errors": [...]}
       全部失败 return 1,否则 0。"""

def main() -> None:
    raise SystemExit(run())
```

`pyproject.toml` 的 `[project.scripts]` 增:
`spx-spark-schwab-collector = "spx_spark.schwab.collector:main"`。

### D2. `service_loop.py` 增可选任务(仿照 hyperliquid 任务的写法)

- 开关 env: `SPX_SERVICE_SCHWAB_CHAINS_ENABLED`(默认 false)
- 间隔 env: `SPX_SERVICE_SCHWAB_CHAINS_INTERVAL_SECONDS`(默认 300)
- `ServiceLoopSettings` 加对应两个字段(带默认值);`build_tasks` 按开关追加任务,
  命令为运行 `spx_spark.schwab.collector`(与现有任务用同一种调用方式)。
- `.env.example` / `.env` 各增两行(默认 false / 300),`.env.example` 注释说明
  "需先完成 Schwab OAuth(token 文件就绪)再启用"。

### D3. `options_map.py` 增 confluence(L3,只读 LatestState)

```python
@dataclass(frozen=True)
class WallConfluence:
    spy_underlier: float | None
    spy_front_expiry: str | None
    spy_call_wall_spx: float | None      # SPY call wall strike * 10
    spy_put_wall_spx: float | None
    call_wall_confluent: bool | None     # 两侧墙位距离 <= tolerance 时 True;缺任一侧为 None
    put_wall_confluent: bool | None
    tolerance_points: float
    spy_option_count: int
    quality: str                         # "ok" | "missing_spy_chain" | "missing_spy_underlier"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def is_spy_option(quote: Quote) -> bool:
    """instrument_type == OPTION 且 (underlier or symbol).upper() == "SPY"。"""

def build_spy_confluence(
    state: LatestState,
    front_spxw: ExpiryOptionsMap | None,
) -> WallConfluence:
    """1. 收集 state 中全部 SPY 期权 quotes(state 的遍历方式与 SPXW 相同)。
    无 quotes → quality="missing_spy_chain",其余 None/0。
    2. spy_underlier = state.best_quote("equity:SPY").effective_price;
       缺失 → quality="missing_spy_underlier"。
    3. front expiry = quotes 中最小的 expiry(字符串序即日期序)。
    4. 复用 pair_by_strike + build_gex_by_strike(underlier=spy_underlier)。
    5. SPY call wall = call_gex 最大的 strike;put wall = |put_gex| 最大的 strike
       (与 SPXW 现行墙位定义保持一致——实施时查看 build_expiry_map 中
       call_wall/put_wall 的现行取法并复用同一逻辑/函数)。
    6. ×10 映射到 SPX 点位。tolerance = max(10.0, (front_spxw 的 underlier 或
       spy_underlier*10) * 0.0015)。
    7. confluent 判定: 两侧墙位都存在时 |spy_wall_spx - spxw_wall| <= tolerance。"""
```

`OptionsMap` 加字段 `spy_confluence: WallConfluence | None = None`;
`build_options_map` 末尾计算(front SPXW expiry 传入);`to_dict` 递归自动。

### D4. `human_focus.py` 暴露

`build_human_focus_context` 的 `"spxw_options"` dict 增:

```python
"wall_confluence": options_map.spy_confluence.to_dict() if options_map.spy_confluence else None,
```

---

## E. 告警文案引用概率(`src/spx_spark/alert_engine.py`)

`option_map_alerts` 中 `option_wall_proximity` 告警:
在构造 detail 前,从 `expiry.level_probabilities` 找 `level` 与 `expiry.nearest_wall`
相等(±0.01)的条目;找到且 `prob_touch` 非 None 时,detail 追加:
` touch_prob≈{prob_touch:.0%}, close_beyond≈{prob_close_beyond:.0%}.`

`option_gamma_regime` 告警 detail:若 `expiry.gamma_flip_zone` 非 None,追加
` flip_zone={left:.0f}-{right:.0f}.`

---

## F. 测试清单(全部新增;现有测试不许改语义)

新文件 `tests/test_probability_layer.py`:
1. `test_probability_for_level_uses_call_delta_above_underlier`:
   合成 pairs(strike 7550 call delta=0.20 质量 live),underlier=7500,level=7550
   → prob_close_beyond≈0.20、prob_touch≈0.40、source_strike=7550。
2. `test_probability_for_level_uses_put_delta_below_underlier`:
   strike 7450 put delta=-0.25 → prob_close_beyond≈0.25、prob_touch≈0.50。
3. `test_probability_for_level_refuses_far_strike`:
   最近可用 strike 距 level > 2*step → 全 None。
4. `test_expiry_map_populates_level_probabilities_and_flip_zone`:
   构造带 delta/gamma/OI 的完整 SPXW 合成链跑 `build_expiry_map`,断言
   `level_probabilities` 非空、`gamma_flip_zone` 是 net_gex 变号的相邻 strike 对。

新文件 `tests/test_micopedia_quant.py`:
5. `test_vix_ratio_event_pricing_forces_high_vol_event`(vix1d=19, vix=20 → ratio 0.95)。
6. `test_dip_context_matrix`:四个分支各一组输入断言。
7. `test_signal_carries_dip_context_and_trigger_line`:dip_buy_friendly 时
   trigger_watchlist 末尾含 "Dip context" 行。

新文件 `tests/test_spy_confluence.py`:
8. `test_confluence_missing_spy_chain`:空 state → quality="missing_spy_chain"。
9. `test_confluence_detects_confluent_call_wall`:合成 SPY 链(755 strike 大 call
   OI×gamma)+ SPXW front map call_wall=7550 → spy_call_wall_spx=7550、
   call_wall_confluent=True。
10. `test_confluence_maps_strikes_times_ten`:SPY put wall 748 → 7480。

新文件 `tests/test_schwab_collector.py`:
11. `test_collector_skips_without_token`:token 缺失 → exit 0、输出含
    "missing_schwab_token"(monkeypatch load_access_token 返回 "")。
12. `test_collector_persists_chain_quotes`:注入 fake client 返回最小 chain payload
    → persist_provider_snapshot 被调用(monkeypatch 捕获),quote_counts 正确。

`tests/test_human_focus`(如已存在则追加,否则并入 test_probability_layer):
13. summary 含 `gamma_profile.top_strikes` 与 `level_probabilities`。
14. `micopedia_context` 输出含 `dip_context` 与 `vix_ratio`;
    monkeypatch `MICOPEDIA_EVENT_TAGS=fomc` 后 regime 反映事件。

## 验收标准

1. `uv run pytest -q` 全绿(含 `tests/test_architecture.py` 分层守护)。
2. `uv run ruff check src/ tests/` 无错误。
3. 运行时行为兼容:SPY 链缺失、SKEW 缺失、delta 缺失时所有新字段为 None/()/
   "missing_*",不抛异常、不改变现有告警行为。
4. `.env.example` 与 `.env` 的新增变量齐全;`SPX_SERVICE_SCHWAB_CHAINS_ENABLED`
   在 `.env` 中为 false(token 未就绪)。
5. `pyproject.toml` 脚本入口注册。
