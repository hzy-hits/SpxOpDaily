# 盘前地图推送 + 盘后复盘推送 + agent 解读风格(实施规格)

状态: 2026-07-07 设计完成,待实施。
分层约束: 遵守 `module-architecture.md`;新模块 `morning_map.py` 属 L5 orchestration,
允许 import config(L0)/storage(L1)/options_map/iv_surface/human_focus(L3)/notifier(L4)。
不许 import service_loop/alert_engine 内部私有函数;需要的上下文一律自己组装。

背景事实(实施者不需要重新调查):
- 用户作息: 北京时间,美股开盘(21:30/22:30 CST)后前 2 小时内入场,之后挂限价单睡觉,
  早上醒来想看一份复盘。服务器时区 Asia/Shanghai。
- 盘前地图内容全部已在 `build_human_focus_context`(human_focus.py)产出:
  `spxw_options.expiries[*].level_probabilities / gamma_profile(flip_zone、zero_gamma、
  top_strikes 含 call/put OI)`、`wall_confluence`、`micopedia(regime、vix_ratio、
  dip_context、trading_guidance、trigger_watchlist)`、`vol_context`。
- 隔夜 gap: `state.best_quote("future:ES")` 的 `effective_price` vs `close`(昨结算),
  `state.best_quote("index:SPX")` 的 `close`(昨收)。Quote 字段见 marketdata.py。
- 发送通道复用 notifier.sinks: `send_openclaw_message(settings, text)`、
  `send_bark_message(settings, title, body)`、`run_openclaw_agent(settings, prompt)`
  (返回 `(SinkResult, str)`,str 是 agent 回复文本)。
  `NotificationSettings.from_env()` 拿配置。
- 盘后复盘 `post_close_review.py` 已存在(07:15 CST timer 触发,写 markdown/json 报告,
  可选 DeepSeek LLM writer),只缺"推送到微信/Bark"一步。

---

## 块 A: agent 解读风格(notifier/prompts.py)

目标: 推送不再是干巴巴字段罗列,要求 agent 给交易员口吻的解读,并引用概率数字。

`build_codex_prompt` 修改(保持现有 gate 规则行不动,只改输出要求两行):
- 把 "输出中文，最多 7 行" 改为 "输出中文，最多 10 行"。
- 在输出结构行后追加一行指令:
  "解读要求：用交易员口吻先说人话——这条告警对今天买 call/put 或做价差意味着什么环境
  （只描述环境，不给下单指令）。凡 human_focus_context 里有 level_probabilities、
  gamma_profile.flip_zone、micopedia.dip_context，必须引用具体数字，例如
  『7550 墙今天触及概率约 24%、收在上方约 12%』『flip zone 7475-7495，跌进去 gamma 转负』
  『尾部保护贵(dip_context=expensive_tail_protection)，急跌大概率是保护盘驱动』。"

`build_agent_prompt` 同样追加解读要求一行(措辞可同上),输出结构行改为:
"输出结构：1. 一句话人话解读 2. 发生了什么 3. gamma 地形与概率(引用 touch/close 概率、
flip_zone) 4. vol regime 与 dip_context 5. 风险/数据质量 6. 人类需要看的 SPX/SPXW 检查项。"

## 块 B: 盘前地图推送 `src/spx_spark/morning_map.py`(新文件)

### B1. payload 组装

```python
def build_morning_payload(state: LatestState, *, now: datetime | None = None) -> dict[str, Any]:
```
- `options_map = build_options_map(state)`;iv surface 按 alert_engine.load_current_iv_surface
  的同款方式加载(IvSurfaceSettings.from_env() + load_latest_snapshot,异常吞掉返回 None;
  这段逻辑简短,直接在 morning_map 里复制,不 import alert_engine)。
- `focus = build_human_focus_context(state, options_map=..., iv_surface=..., iv_surface_history_1h=None, window={"name": "premarket_map", "priority": "info"})`
- 隔夜 gap 块:
```python
def overnight_gap(state: LatestState) -> dict[str, Any]:
    """es_last=ES effective_price, es_prev_close=ES close, spx_prev_close=SPX close,
    gap_points=es_last-es_prev_close, gap_pct=同比(两值都>0时),否则 None。
    返回 {"es_last":…, "es_prev_close":…, "spx_prev_close":…, "gap_points":…, "gap_pct":…}"""
```
- 返回 `{"kind": "morning_map", "as_of": state.as_of.isoformat(), "overnight": …, "human_focus_context": focus}`。

### B2. 确定性模板(fallback + dry-run 展示)

```python
def render_template(payload: dict[str, Any]) -> str:
```
中文多行文本,依序(字段缺失时该行写 "-"):
```
【盘前地图 {ET 交易日 YYYY-MM-DD}】
隔夜: ES {es_last}({gap_points:+.0f} 点/{gap_pct:+.2%} vs 昨结), SPX 昨收 {spx_prev_close}
gamma 地形: call wall {cw}(OI {cw_oi:.0f}), put wall {pw}(OI {pw_oi:.0f}), zero gamma {zg}, flip zone {lo}-{hi}
概率锥: {对 expiries[0].level_probabilities 每条 → "触及 {level:.0f}≈{prob_touch:.0%}/收破≈{prob_close_beyond:.0%}" 用 "; " 连接}
SPY 对照: {wall_confluence 存在时 → "put 墙折算 {spy_put_wall_spx:.0f}({共振|不共振}), call 墙折算 {spy_call_wall_spx:.0f}({共振|不共振})";否则 "无 SPY 数据"}
regime: {micopedia.regime}, VIX1D/VIX={vix_ratio:.2f}, dip_context={dip_context}
事件: {micopedia.event_tags 逗号连接或 "无"}
开盘前 2 小时关注: {micopedia.trigger_watchlist 前 3 条,分号连接}
```
(wall OI 数字从 `gamma_profile.top_strikes` 里找 strike==call_wall/put_wall 的条目取
`call_open_interest`/`put_open_interest`;找不到就省略括号。)

### B3. agent 润色

```python
def build_map_prompt(payload: dict[str, Any], template: str) -> str:
```
指令(逐行):
- "你是 SPX Spark 的盘前地图写手，为一个只交易 SPX/SPXW 0DTE/1DTE 期权(买 call/put 或垂直价差)的人写开盘前简报。"
- "只依据下面 JSON 与模板事实，不得编造数字、新闻或仓位；不给下单指令。"
- "输出中文，最多 14 行，第一行必须是模板的第一行。"
- "必须覆盖：隔夜 gap、gamma 地形(墙位+OI+zero gamma+flip zone)、各关键位触及/收破概率、SPY 墙位对照、regime 与 dip_context、事件标签。"
- "在数字之外，用 2-3 句交易员口吻解读：今天的地形偏 pin 还是易加速、急跌该当回调买点还是风险、开盘后两小时最该盯什么。"
- "数据 degraded 时如实说明，不给方向判断。"
- 然后 "JSON:" + json.dumps(payload compact) + "模板:" + template。

发送流程 `send_morning_map(payload, settings, *, runner=default_runner) -> dict`:
1. `template = render_template(payload)`
2. 若 `settings.openclaw_agent_enabled`: `sink, reply = run_openclaw_agent(settings, build_map_prompt(...))`;
   reply 非空且 sink.ok → `text = reply`,否则 `text = template`(fallback)。
   agent 未启用 → 直接 `text = template`。
3. `send_openclaw_message(settings, text)`;失败时 `append_missed(settings.missed_queue_path, text, kind="morning_map", at=now)`(复用 missed_queue)。
4. `settings.bark_enabled` 时 `send_bark_message(settings, "盘前地图", text)`。
5. 返回 {"text": text, "used_agent": bool, "weixin_ok": bool, "bark_ok": bool}。

### B4. 时间 gate 与幂等

```python
ET_WINDOW_START = time(8, 30)   # 美东
ET_WINDOW_END = time(9, 30)

def within_send_window(now_utc: datetime) -> bool:
    """转 NY_TZ(config 里已有),工作日且 time 在 [08:30, 09:30) 才 True。
    (节假日不特判——非交易日 state 是旧的,agent/模板照发也无害,保持简单。)"""

def already_sent(state_path: str, trading_date: str) -> bool / mark_sent(...)
    """state 文件 {data_root}/latest/morning_map_state.json 记 {"last_sent_date": "YYYY-MM-DD"}。
    路径由 env SPX_MORNING_MAP_STATE_PATH 覆盖,默认按 StorageSettings.data_root 拼。"""
```

### B5. CLI

```python
def run(argv=None) -> int:
```
`--dry-run`(只打印模板与 agent 文本,不发送、不 mark_sent)、`--force`(跳过时间窗与幂等)。
流程: settings 加载 → gate 检查(不在窗口/已发送 → print json {"skipped": true, reason} 返回 0)
→ LatestStateStore(StorageSettings.from_env()).load() (注意返回可能是 tuple,取 [0];
以 storage.py 实际签名为准) → build_morning_payload → send_morning_map → mark_sent → 打印
结果 json,微信+bark 都失败时返回 1。
`main()` 照 digest_cli 风格。pyproject 注册 `spx-spark-morning-map`。

### B6. systemd

`systemd/spx-spark-morning-map.service`(Type=oneshot,ExecStart=scripts/run-morning-map.sh,
照抄 post-close-review service 的结构)与 `.timer`:
```
OnCalendar=Mon..Fri 21:00 Asia/Shanghai
OnCalendar=Mon..Fri 22:00 Asia/Shanghai
Persistent=false
```
(两个触发点由代码内 ET 窗口 gate 自动选中正确的那个: 夏令时 21:00 CST=9:00 ET 命中,
22:00 被拦;冬令时反之。)
`scripts/run-morning-map.sh` 照抄 run-post-close-review.sh(exec uv run spx-spark-morning-map "$@",
记得 export PATH="$HOME/.local/bin:$PATH")。
`scripts/install-spx-spark-services.sh` 里加对应 ln -sfn 与 enable。

## 块 C: 盘后复盘推送(post_close_review.py)

### C1. 推送摘要

```python
def build_push_summary(payload: dict[str, Any]) -> str:
```
确定性中文摘要(≤12 行):
```
【盘后复盘 {trading_date}】
SPX: {first}→{last}({change_points:+.1f} 点/{change_bps:+.0f}bp), 区间 {range_points:.1f} 点(低 {low} 高 {high})
0DTE 收盘墙位: put {put_wall_last} call {call_wall_last}, zero gamma {zero_gamma_last}, gamma {gamma_state_last}
ATM IV: {atm_iv first→last}, put skew: {put_skew_ratio first→last}
数据: {verdict.status}{warnings 有则列出}
完整报告: {latest_markdown_path}
```
(墙位/IV 取 iv_surface.expiries[0],缺失写 "-"。)

### C2. agent 解读(与盘前地图同 pattern)

```python
def build_review_push_prompt(payload, summary) -> str
```
- "你是 SPX Spark 的盘后复盘播报员，对象是白天睡觉、只交易 SPX/SPXW 0DTE 期权的人。"
- "只依据 JSON 与摘要事实。输出中文最多 12 行，第一行必须是摘要第一行。"
- "必须包含：当日价格路径一句话、墙位/zero gamma/gamma state 收盘位、IV 与 skew 当日变化、
  用 2-3 句交易员口吻点评当日结构(pin 住了吗？墙被打穿过吗？IV 是 crush 还是抬升？)，
  最后给出 2-3 条『下一交易日开盘前检查项』。数据 degraded 时如实说明。"

```python
def push_review(payload: dict[str, Any], *, runner=default_runner) -> dict[str, Any]:
```
- `NotificationSettings.from_env()`;openclaw_agent_enabled 时 agent 润色,失败 fallback 摘要;
  send_openclaw_message(失败 append_missed kind="post_close_review")+ bark(标题 "盘后复盘")。
- 环境开关 `SPX_REVIEW_PUSH_ENABLED`(env_bool,默认 True);CLI 加 `--no-push`。
- `run()` 中: 在写完报告之后、quiet-if-empty 判断之前执行;coverage 全零(raw_quote_rows==0
  且 iv_surface_snapshots==0)时跳过推送。payload["push"] 记录结果。
- 注意 post_close_review 不在 notifier 包内,import `spx_spark.notifier`(L4)合法;
  确认 tests/test_architecture.py 通过。

## 块 D: 测试

`tests/test_morning_map.py`:
1. `test_within_send_window_summer_et`: 用固定 UTC 时刻(2026-07-07 13:00 UTC=9:00 EDT)
   → True;2026-07-07 14:00 UTC → False;周六 → False。
2. `test_already_sent_roundtrip`(tmp_path)。
3. `test_render_template_contains_walls_probs_regime`: 手工构造最小 payload(含
   level_probabilities 两条、flip_zone、dip_context、wall_confluence)→ 模板含
   "触及"、"flip zone"、"dip_context" 等关键字与数字。
4. `test_send_morning_map_falls_back_to_template_when_agent_fails`: fake runner 让 agent
   失败、message send 成功 → used_agent False,发送文本==模板。
5. `test_send_morning_map_queues_on_weixin_failure`(tmp_path 的 missed_queue_path)。
6. `test_run_skips_outside_window`(monkeypatch now/时间函数或注入参数,设计成 run 接受
   可注入 `now` 便于测试)。

`tests/test_post_close_push.py`:
7. `test_build_push_summary_format`: 用最小 payload → 首行含 trading_date,含 "put"/"call" 墙数字。
8. `test_push_review_respects_disabled_env`(monkeypatch SPX_REVIEW_PUSH_ENABLED=false → 不调 runner)。
9. `test_push_review_agent_fallback`(agent 失败 → 发确定性摘要)。

`tests/test_notifier.py` 不改语义;若 prompts 断言字符串的现有测试受影响,同步更新断言
(允许,属于文案变更)。

## 验收标准

1. `uv run pytest -q` 全绿;`uv run ruff check src/ tests/` 通过。
2. `uv run spx-spark-morning-map --dry-run --force` 用真实 latest state 打印出模板
   (含真实墙位/概率数字)不报错。
3. `uv run spx-spark-post-close-review --date auto --no-write --no-push --markdown` 正常。
4. systemd 单元文件 + install 脚本更新;`bash -n` 两个新/改 shell 脚本。
5. 不 commit、不 systemctl 操作(验收人做)。
