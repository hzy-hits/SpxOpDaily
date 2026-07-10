# 挂单地图(Order Map)实施规格

- 日期: 2026-07-07
- 状态: 待实施
- 背景: 用户的主要操作方式是**盘前(北京时间 14:00 - 美股开盘)挂限价单**,然后不盯盘。
  现有推送(盘前地图 21:00、盘中告警)都假设"看到消息后再操作",不匹配这个工作流。
  需要一条每天 14:00(北京)推送的"挂单参考":列出关键位、到达概率、
  以及**SPX 到达该位时候选期权合约的预估价格**,帮助用户设出"能被打到"的合理限价。

## 1. 核心思路

用户挂限价单的本质问题是:"如果 SPX 走到位置 L,我想买的那张期权值多少钱?"
用当前合约报价 + delta/gamma 做二阶泰勒外推:

```
projected_mid(L) = mid_now + delta * (L - S) + 0.5 * gamma * (L - S)^2
```

- `S` = 当前底层参考价(options_map.underlier.price)
- `delta`/`gamma` 来自该合约当前 greeks(state 里 IBKR 流已带)
- 结果下限 clamp 到 0.05
- **明确的近似声明**: 忽略 theta/vega。盘前到实际触及之间有时间衰减,
  对 0DTE 尤其明显,所以同时给出保守限价 = projected * 0.85(四舍五入到 tick)。

Tick 规则(SPX 期权): premium < 3.0 → tick 0.05; >= 3.0 → tick 0.10。

## 2. 新文件与修改清单

### 新文件 `src/spx_spark/order_map.py`

模式完全参照 `src/spx_spark/morning_map.py`(读它!),包含:

```python
# 纯函数(必须可单测,不做 IO):

def option_tick(premium: float) -> float:
    """SPX option tick: 0.05 below 3.00, 0.10 at/above."""

def round_to_tick(premium: float) -> float:
    """Round DOWN to tick (limit buy: 挂低一格比挂高一格好)."""

def project_option_price(
    mid: float, delta: float, gamma: float, spot: float, target: float
) -> float:
    """Second-order Taylor projection, clamped to >= 0.05."""

@dataclass(frozen=True)
class OrderCandidate:
    play: str              # legacy plays plus confirmed reclaim/breakout calls
    level: float           # 触发位(SPX 点)
    level_label: str       # "put wall 7500" 等
    contract_id: str       # option:SPX:SPXW:...:C 的 canonical id
    strike: int
    right: str             # "C"/"P"
    current_mid: float
    projected_mid: float   # SPX 到达 level 时的预估 mid
    limit_aggressive: float  # round_to_tick(projected_mid)
    limit_conservative: float  # round_to_tick(projected_mid * 0.85)
    prob_touch: float | None      # 到达 level 的概率(来自 probability 层)
    prob_close_beyond: float | None
    delta: float
    gamma: float

def build_candidates(state, options_map) -> list[OrderCandidate]:
    """见第 3 节选取规则。"""

def build_order_payload(state, *, now=None) -> dict:
    """kind="order_map", as_of, underlier(price/source), expiry,
    expected_move_points, candidates(list of asdict), gamma_state,
    zero_gamma, flip_zone, warnings(list[str])."""

def render_template(payload) -> str:
    """确定性中文模板,见第 4 节。"""

def build_order_prompt(payload, template) -> str:
    """给 openclaw agent 的提示词,要求交易员口吻解读(参照
    morning_map.build_map_prompt 的结构与约束:只谈 SPX/SPXW/ES,
    输出以『挂单参考:』开头,禁止编造数字,必须引用模板中的
    prob/limit 数字)。"""

def send_order_map(payload, settings, *, now=None) -> dict:
    """推送逻辑,参照 morning_map.send_morning_map:
    agent 可用则用 agent 文本,否则用模板;send_openclaw_message,
    失败 append_missed(kind="order_map");bark_enabled 则同发 Bark
    (标题『挂单地图』);返回 {weixin_ok, bark_ok, used_agent}."""

# 时间闸门(参照 morning_map,但窗口不同):
def within_send_window(now_utc) -> bool:
    """北京时间(Asia/Shanghai)工作日 13:30-21:25 之间返回 True。
    注意 morning_map 用的是 NY_TZ,这里必须用 Asia/Shanghai,因为
    用户的操作窗口按北京时间定义。周六/周日返回 False。"""

def already_sent(state_path, trading_date) -> bool   # 同 morning_map
def mark_sent(state_path, trading_date) -> None      # 同 morning_map
def default_state_path(settings) -> str
    # env SPX_ORDER_MAP_STATE_PATH 或 {data_root}/latest/order_map_state.json
    # trading_date 用 NY 交易日(now.astimezone(NY_TZ).date().isoformat()),
    # 与 morning_map 一致。

def run(argv=None, *, now=None) -> int
    # --dry-run / --force,逻辑与 morning_map.run 完全一致
```

### 修改 `pyproject.toml`

`[project.scripts]` 增加:
`spx-spark-order-map = "spx_spark.order_map:run"`

### 新文件 `systemd/spx-spark-order-map.service`

参照 `systemd/spx-spark-morning-map.service`(读它),改 Description 与 ExecStart
(`scripts/run-order-map.sh`)。

### 新文件 `systemd/spx-spark-order-map.timer`

```
OnCalendar=Mon..Fri 14:00 Asia/Shanghai
OnCalendar=Mon..Fri 16:30 Asia/Shanghai
Persistent=false
```

(16:30 第二次触发靠 already_sent 幂等跳过;仅当 14:00 那次因故障没发出时兜底。
注意:already_sent 只在**成功发送后** mark,发送失败不 mark。)

### 新文件 `scripts/run-order-map.sh`

参照 `scripts/run-morning-map.sh`(如果存在;否则参照 run-post-close-review.sh):
cd 仓库根目录, `exec uv run spx-spark-order-map "$@"`。加可执行位。

### 新文件 `tests/test_order_map.py`

见第 5 节。

## 3. 候选合约选取规则(build_candidates)

从 `build_options_map(state)` 的**前月(第一个)expiry**取:
`put_wall`, `call_wall`, `zero_gamma`(spot-scan 优先), `gamma_flip_zone`(低/高界)。
概率用现有 `probability_for_level`(options_map.py 已有,查它的真实签名和返回结构,
拿 touch 概率与收盘越过概率)。

三类常规 play(每类最多 1 条;对应位缺失/质量差则跳过并写入 warnings):

1. **put_wall_bounce_call**: 触发位 L = put_wall。合约 = strike 为
   `round_to_step(L, strike_step)` 的 **call**(到位时的 ATM call,反弹做多)。
2. **flip_breakdown_put**: 触发位 L = flip_zone 下界(无 flip_zone 用 zero_gamma)。
   合约 = strike 为 `round_to_step(L, strike_step)` 的 **put**(跌破进负 gamma 顺势)。
3. **call_wall_fade_put**: 触发位 L = call_wall。合约 = strike 为
   `round_to_step(L, strike_step)` 的 **put**(冲墙回落)。

两个 Call 延续 play 不按 15 分钟机械生成。5 秒 SPX/ES 通道先冻结结构位，
再用两个新的同步样本确认:

4. **flip_reclaim_call**: 急跌 V 反确认后,SPX 连续守住冲击前冻结的
   `flip_high + 3pt`,ES 不背离。候选以冻结 flip 为回踩位,跌回
   `flip_low - 3pt` 失效。
5. **call_wall_breakout_call**: 冻结突破前的旧 call wall;SPX 跨越
   `wall + 3pt` 后连续两组 SPX/ES 样本接受在墙上方。当前墙跳到下一档时
   不追着重置,跌回 `old wall - 3pt` 失效。

确认态形成 5 分钟 `conditional_call_bias`,优先显示对应 Call,并替换已经被
确认路径证伪的同层 Put(收复 flip 后不再同时给 breakdown Put;接受旧 call
wall 后不再同时给 fade Put)。另一侧风险剧本保留,不自动下单。Gamma 正负
只描述波动环境。

结构位只接受严格 SPXW 当日到期、IBKR live-feed、120 秒内且有 OI+Gamma
的来源。flip 两个边界各要求同一 distinct strike 的 Call/Put 双边都新鲜;
call wall 要求该 strike 的 Call OI+Gamma 新鲜。短暂轮换缺口有 30 秒 grace,
超过后 bias/watch 失效。冷启动首个墙下样本可建立 provisional watch,但 crossing
前若 live OI wall 改档必须跟随新墙;只有实际 crossing 后才冻结旧墙。

合约查找: 在 `state.best_quotes` 里找该 expiry/strike/right 的 SPXW 期权 quote;
需要 `mid`(或 effective_price)与 greeks 的 delta、gamma 都非空,quality 不在
BAD_QUALITIES,否则跳过该 play 并记 warning(如 `no_quote_for_7500C`)。
strike 找不到精确匹配时,允许在 ±1 个 strike_step 内取最近的有报价合约。

数据质量:`options_map` 为 None、无 expiries、underlier.price 为 None、
或 gex_quality 表示无 OI 时 → payload.warnings 加说明,candidates 可为空;
模板仍渲染(显示 "-"),推送照发(用户需要知道"今天没图")。

## 4. 模板格式(render_template)

```
【挂单地图 2026-07-07】(北京 14:00,0DTE=20260707)
参考价: 7569.2(future:ES), 预期波幅 ±41 点
gamma: positive_gamma_pin, zero gamma 7533.3, flip zone 7530-7535

1) put wall 7500 反弹买 call → SPXW 7500C
   触达概率≈24%, 到位时预估价≈12.30(现价 4.20)
   挂单参考: 激进 12.30 / 保守 10.40
2) flip zone 7530 跌破买 put → SPXW 7530P
   触达概率≈41%, 到位时预估价≈15.80(现价 9.10)
   挂单参考: 激进 15.80 / 保守 13.40
3) call wall 7550 冲墙买 put → SPXW 7550P
   触达概率≈35%, 到位时预估价≈6.40(现价 11.20)
   挂单参考: 激进 6.40 / 保守 5.40

注: 预估价按当前 delta/gamma 外推,未计时间衰减(0DTE 下午触发会更便宜);
保守价≈预估×0.85。仅供挂单参考,不是订单指令。
```

- 数字全部来自 payload,浮点用 `f"{x:.2f}"`(期权价)/`f"{x:.0%}"`(概率)/
  `_dash` 风格处理 None(显示 "-")。
- warnings 非空时在末尾追加 `数据警告: ...` 一行。

## 5. 测试要求(tests/test_order_map.py)

参照 `tests/test_morning_map.py` 的构造方式(make_state / make_quote 辅助,
或该文件里现成的 fixture 模式)。至少覆盖:

1. `option_tick` / `round_to_tick`: 2.97→0.05 tick(2.95), 3.2→0.10 tick(3.2),
   向下取整。
2. `project_option_price`: 手算一个 call 案例(mid=4.2, delta=0.35,
   gamma=0.008, S=7569, L=7500 → 4.2 + 0.35*(-69) + 0.5*0.008*69^2 = 负值被
   clamp 到 0.05?换参数使结果为正,断言公式);put 案例 delta 为负。
3. `build_candidates`: 构造带 put/call 墙与合约报价+greeks 的假 state,断言
   无确认态保持 3 个 play;确认态以 1 个 Call play 替换同层 Put,并验证
   strike/right、projected 与 limit 一致性
   (limit_aggressive = round_to_tick(projected))。
4. 缺 greeks 的合约被跳过并产生 warning。
5. `render_template` 包含关键行(play 标签、触达概率、挂单参考)。
6. `within_send_window`: 北京 14:00 True, 12:00 False, 周六 14:00 False
   (用带 tz 的 datetime 构造)。
7. `already_sent`/`mark_sent` 幂等(tmp_path)。

## 6. 约束

- 遵守分层: order_map 与 morning_map 同层,允许 import 的模块以 morning_map
  现有 import 为准(storage/options_map/human_focus/notifier.* /config)。
  运行 `uv run pytest tests/test_architecture.py` 必须通过。
- 不改 alert_engine/notifier 现有行为。
- `uv run pytest -q` 全绿, `uv run ruff check src tests` 干净。
- 不要执行 systemctl/推送等有副作用的操作(单元安装与实测由验收人做)。
