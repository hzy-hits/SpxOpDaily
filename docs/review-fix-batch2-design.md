# 评审修复·批次 2:收盘换月 + 量化修正(S1/S2/S4/S5/S8)(实施规格)

状态: 2026-07-07 设计完成,待实施。对应 docs/architecture-review-kangyu.md 的
S1(volume 加权 GEX)、S2(零 gamma spot 扫描)、S4(skew delta 标准化)、
S5(ATM 插值)、S8(movement 阈值用 expected move 归一),外加用户需求:
**收盘后订阅从当日 0DTE 自动切到次日 1DTE**。

分层约束: 遵守 module-architecture.md。改动集中在 config(L0)、
options_map/iv_surface(L3)、alert_engine(L4)、stream_collector(L2,仅受
default_spxw_expiry 影响,无直接改动)。

背景事实(实施者不需要重新调查):
- `Quote.volume` 已存在且 IBKR adapter 已映射(tick 100),数据在采。
- `Quote.greeks.delta` IBKR model greeks 已映射(ibkr/adapter.py)。
- `stream_collector.ensure_option_plan` 用 `default_spxw_expiry()` 决定订阅到期日,
  `should_replan(..., today_expiry=today)` 在到期日变化时自动重订;SPY lane 复用
  `plan.expiry`。所以块 A 改 `default_spxw_expiry` 一处,订阅链自动跟随。
- `evaluate_alerts`(alert_engine)已接收 `options_map` 参数,块 E 可直接用。
- MOVE_THRESHOLDS_BPS 的 key: critical/high/elevated/normal/low/off。

---

## 块 A: 收盘后到期日切到次日 1DTE

`src/spx_spark/config.py` 的 `default_spxw_expiry`(约 108 行)改为:

```python
SPXW_ROLL_AFTER_ET = time(16, 15)  # 模块级常量,SPXW PM 结算后视为过期

def default_spxw_expiry(today: date | None = None, *, now: datetime | None = None) -> str:
    """Return the active SPXW expiry.

    显式传 today 时行为不变(测试/CLI 用)。today 为 None 时取纽约当前时刻;
    若已过 16:15 ET(当日 0DTE 已结算),滚动到下一天;周末照旧跳到下个工作日。
    """
    if today is None:
        current = (now or datetime.now(tz=NY_TZ)).astimezone(NY_TZ)
        today = current.date()
        if current.time() >= SPXW_ROLL_AFTER_ET:
            today += timedelta(days=1)
    while today.weekday() >= 5:
        today += timedelta(days=1)
    return today.strftime("%Y%m%d")
```

节假日不特判(qualify 失败会优雅降级,次日开盘自动纠正)。
测试: now=周五 16:20 ET → 下周一;周四 16:20 → 周五;周四 15:00 → 周四;
显式 today 传参 → 原样(不滚动)。

注意連带: stream_collector.ensure_option_plan 里 `today = default_spxw_expiry()`
将在 16:15 ET 后返回次日,`should_replan` 检测 plan.expiry != today 触发重订,
SPXW 与 SPY lane 都切到 1DTE——无需改 collector。确认现有
stream_collector 测试不受影响。

## 块 B(S1): 0DTE GEX 用当日 volume 补充加权

`src/spx_spark/options_map.py`:

1. `signed_gex` 加权重模式:

```python
def gex_weight(quote: Quote, *, intraday: bool) -> float | None:
    """非 0DTE: OI(现行为)。0DTE(intraday=True): OI + volume。
    OI/volume 缺失按 0;两者都缺或 <=0 返回 None。
    注: volume 近似当日新开仓(也含平仓,是有意的粗近似,注释说明)。"""

def signed_gex(quote, *, sign, underlier, intraday: bool = False) -> float | None:
    # weight = gex_weight(quote, intraday=intraday); 其余公式不变
```

2. `build_gex_by_strike(pairs, *, underlier, intraday: bool = False)` 透传。
3. `build_expiry_map` 判定 `intraday = (expiry == default_spxw_expiry())`
   (import 自 config;注意块 A 后该函数收盘后返回次日,收盘后当日链自动退回
   纯 OI——正确,因为收盘后 volume 语义已结束)。
4. `ExpiryOptionsMap` 加字段 `gex_weighting: str`("oi" 或 "oi_plus_volume"),
   进 to_dict;`gex_quality` 现有取值不动。
5. human_focus 的 gamma_profile dict 里加 `"gex_weighting"`。

测试: 同一合约 OI=100、volume=400 → intraday=True 时 gex 是 False 时的 5 倍;
OI=None、volume>0 → 非 0DTE None,0DTE 有值。

## 块 C(S2): 零 gamma 改 spot 扫描重估

`src/spx_spark/options_map.py` 新增(纯函数,不依赖 scipy):

```python
def bs_gamma(spot: float, strike: float, iv: float, t_years: float) -> float | None:
    """Black-Scholes gamma,r=q=0: d1=(ln(S/K)+iv^2/2*t)/(iv*sqrt(t)),
    gamma=phi(d1)/(S*iv*sqrt(t))。iv/t/spot/strike 非正返回 None。
    phi 用 math.exp(-d1*d1/2)/math.sqrt(2*math.pi)。"""

def time_to_expiry_years(expiry: str, *, as_of: datetime) -> float:
    """到期日 16:00 ET 距 as_of 的年化时间(365 天制),下限 15 分钟
    (15/(60*24*365)),避免 0DTE 尾盘 gamma 发散。expiry 是 YYYYMMDD。"""

def zero_gamma_spot_scan(
    pairs: dict[float, dict[OptionRight, Quote]],
    *,
    underlier: float,
    expiry: str,
    as_of: datetime,
    intraday: bool,
) -> tuple[float | None, tuple[float, float] | None, str]:
    """返回 (zero_gamma, flip_zone, method)。

    构造合约表: 对每个 strike/right 取 iv=option_iv(quote)(现有 helper)、
    weight=gex_weight(quote, intraday=intraday)、sign=+1 call/-1 put。
    iv 或 weight 缺失的合约跳过;可用合约(有 iv 且有 weight)占比 < 0.6
    或 < 4 个 → 返回 (None, None, "insufficient_iv") 让调用方回退。

    扫描: S 从 min(strikes) 到 max(strikes),步长 = min strike 间距(不超过 5)。
    net(S) = Σ sign_i * weight_i * bs_gamma(S, K_i, iv_i, T) * 100 * S^2 * 0.01。
    找相邻网格点符号翻转,线性插值求根;多个根取离 underlier 最近的。
    flip_zone = 该根所在网格区间 (S_left, S_right)。
    无翻转 → (None, None, "no_flip")。成功 → method="spot_scan"。"""
```

接线 `build_expiry_map`:

```python
zg_scan, flip_scan, zg_method = zero_gamma_spot_scan(...)
if zg_scan is not None:
    zero_gamma, gamma_flip_zone = zg_scan, flip_scan
else:
    zero_gamma = nearest_zero(...)      # 现行为,作 fallback
    gamma_flip_zone = zero_gamma_bracket(...)
    zg_method = f"strike_profile_fallback_{zg_method}"
```

`ExpiryOptionsMap` 加 `zero_gamma_method: str`,进 to_dict;human_focus
gamma_profile 加 `"zero_gamma_method"`。现有 nearest_zero/zero_gamma_bracket
函数保留不动。

测试(手工构造对称链验证):
1. 合成链: strikes 5900-6100 步 25,call/put 各带 iv=0.2、gamma 权重构造成
   put 侧 OI 重于下方、call 侧重于上方 → scan 有根,root 在链内,method=spot_scan,
   flip_zone 覆盖 root。
2. IV 覆盖率不足(全部 iv=None)→ 回退 strike_profile,method 含 fallback。
3. bs_gamma 数值烟测: S=K=6000, iv=0.2, t=1/365 → 与手算值容差 1e-6 比对
   (实施者自己先用公式算出期望值写死在断言里)。

## 块 D(S4+S5): ATM 插值 + 25Δ skew(vol point 差)

`src/spx_spark/options_map.py` `build_expiry_map` 内(约 520-560 行区域):

1. **S5 ATM 插值**: 现 atm_iv 取最近 strike 的 call/put IV 均值。改为:

```python
def interpolated_atm_iv(pairs, underlier) -> float | None:
    """对 call、put 各自: 取 underlier 两侧最近的、有 IV 的 strike,
    在 strike 轴线性插值出 spot 处 IV;只有一侧有值时用最近值(现行为)。
    call/put 各得一值后取均值;都缺 → None。"""
```

   atm_iv 换用该函数。`smile_curvature`、`put/call_skew_ratio` 等下游自动跟随。
   现有 atm_strike 字段照旧(报告用)。

2. **S4 25Δ skew(新增字段,不删旧的)**: `ExpiryOptionsMap` 加:

```python
put_skew_25d: float | None    # iv(|delta|≈0.25 的 put) - atm_iv,vol point 差
call_skew_25d: float | None   # iv(delta≈0.25 的 call) - atm_iv
skew_method: str              # "delta_25" | "moneyness_fallback"
```

```python
def wing_iv_at_delta(quotes_one_side, target_abs_delta=0.25) -> float | None:
    """在单侧(全 put 或全 call)合约里找 |greeks.delta| 最接近 0.25 且
    iv 非空的合约,|delta| 偏离 0.25 超过 0.15 视为无效(太远),返回其 iv。"""
```

   两侧均拿到 25Δ iv 时 skew_method="delta_25";任一侧拿不到(delta 覆盖差)时
   两个 25d 字段用现有 moneyness 带的 wing_iv 减 atm_iv 兜底,
   skew_method="moneyness_fallback"。旧的 put_wing_iv/put_skew_ratio 等字段
   与计算**完全不动**(micopedia/iv_surface/告警继续消费,兼容)。
   to_dict 加新字段;human_focus 的 expiry_options_summary 加
   put_skew_25d/call_skew_25d/skew_method。

3. **iv_surface 接新指标**: `IvSurfaceExpiry` 加 `put_skew_25d`、
   `put_skew_25d_change_5m`(同现有 *_5m 差分 pattern,受 F9 的间隔守卫保护),
   from build_expiry_surface 里从 expiry_map 读、与 previous 差分。to_dict/
   from_dict 同步。
4. **告警切换**: alert_engine 的 skew steepening 告警(约 830 行)改为优先用
   `put_skew_25d_change_5m`,阈值新常量 `SKEW_25D_STEEPENING_THRESHOLD = 0.02`
   (2 个 vol point,env `ALERT_SKEW_25D_THRESHOLD` 可调);该值为 None 时回退
   现有 ratio 逻辑(SKEW_STEEPENING_THRESHOLD 保留)。detail 里带上用的是哪种。

测试: 构造两侧 delta 0.10/0.25/0.40 的合约 → wing_iv_at_delta 选中 0.25;
delta 全缺 → moneyness_fallback 且 25d 字段等于旧 wing 差值;
ATM 插值: spot=6010,strike 6000(iv 0.20)/6025(iv 0.22)→ atm_iv≈0.208
(线性插值,call put 同构)。

## 块 E(S8): movement 阈值用 expected move 归一

`src/spx_spark/alert_engine.py`:

1. 新常量+env:

```python
EM_MOVE_FRACTIONS = {  # 触发所需 |move| 占当日 expected move 的比例
    "critical": 0.20, "high": 0.30, "elevated": 0.40,
    "normal": 0.50, "low": 0.70, "off": 9.0,
}
```

2. 新纯函数:

```python
def effective_move_threshold_bps(
    priority: str,
    expected_move_pct: float | None,   # 来自 options_map 前月(0DTE)的 expected_move_pct
) -> tuple[float, str]:
    """static = MOVE_THRESHOLDS_BPS[priority](fallback normal)。
    expected_move_pct 无效(None/<=0)→ (static, "static")。
    em_bps = expected_move_pct * 10000 * EM_MOVE_FRACTIONS[priority]。
    返回 (max(static, em_bps), "em_normalized" if em_bps > static else "static")。
    static 作下限:EM 归一只会收紧(高波日抬阈值),不放松,防低波日噪声。"""
```

3. `movement_alerts` 与 SPX fallback monitor(约 232/700 行两处)签名加
   `options_map: OptionsMap | None = None`,从 `options_map.expiries[0]`
   (存在且 expiry 非过期时)取 `expected_move_pct` 调上函数;alert detail 加
   `"threshold_bps"`、`"threshold_source"`、`"expected_move_pct"`。
   `evaluate_alerts` 调用处把已有的 options_map 传进去。
4. 兼容: options_map 缺失/None 时行为与现状完全一致。

测试: expected_move_pct=0.015(150bps)、priority=high(static 30,fraction 0.3
→ em 45)→ 阈值 45、source=em_normalized;expected_move_pct=None → 30、static;
低 EM(20bps→em 6)→ 阈值仍 30(static 下限)。

---

## 验收标准

1. `uv run pytest -q` 全绿;`uv run ruff check src/ tests/`。
2. 每块至少规格所列测试;现有测试语义不改(ExpiryOptionsMap 新字段带默认值/
   在构造处补齐,受影响的现有构造调用允许最小改动)。
3. `uv run python -c "from spx_spark.config import default_spxw_expiry; from datetime import datetime; from zoneinfo import ZoneInfo; print(default_spxw_expiry(now=datetime(2026,7,9,16,20,tzinfo=ZoneInfo('America/New_York'))))"` 输出 20260710。
4. 不 commit、不重启服务(验收人做)。
