# SPY 期权直采(IBKR)与微信离线消息时间线汇总(实施规格)

状态: 2026-07-07 设计完成,待实施。
分层约束: 遵守 `module-architecture.md`。改动集中在 `ibkr/stream_collector.py`(L2)、
`config.py`(L0)、`notifier/`(L4)、keepalive 脚本;无新增跨层依赖。

背景事实(实施者不需要重新调查):
- VIX/VIX1D/VIX9D/VIX3M/VVIX/SKEW 已在 `IBKR_VERIFY_INDEXES` 基础订阅中,**不要动**。
- 期权行情线现状: `IBKR_STREAM_MAX_OPTION_LINES=60` 全部给 SPXW(hot lane 70% + 轮换)。
- 本设计把总量维持 60,重新分配: SPXW 44 + SPY 16。
- SPY 期权经 IBKR 订阅后,`ibkr/verifier.py` 的 `generic_ticks_for_contract` 会自动
  带上 tick 100/101(它按合约类型判断,不分 symbol),OI 数据自动可得。
- `instrument_from_ibkr_label`(ibkr/adapter.py)对 label `option:SPY:...` 会产出
  underlier=SPY 的 InstrumentId;`options_map.build_spy_confluence` 已能消费这些
  quotes——所以本块做完,SPY 墙位对照即自动激活,无需改 options_map。

---

## 块 1: stream collector 增加 SPY 期权常驻 lane

### 1A. `src/spx_spark/config.py` — `IbkrStreamSettings` 加两个字段(带默认值)

```python
spy_option_lines: int = 16      # env IBKR_STREAM_SPY_OPTION_LINES,0 表示关闭
spy_strike_step: int = 2        # env IBKR_STREAM_SPY_STRIKE_STEP
```

`from_env` 对应读取 `env_int("IBKR_STREAM_SPY_OPTION_LINES", 16)`、
`env_int("IBKR_STREAM_SPY_STRIKE_STEP", 2)`。

### 1B. `src/spx_spark/ibkr/stream_collector.py` 新增纯函数(便于测试)

```python
def estimate_spy_reference(rows: list[VerifyRow]) -> float | None:
    """找 label == "stock:SPY" 的行,依次取第一个非 None 且 >0 的:
    market_price、last、(bid+ask)/2(两者都 >0 时)、close。没有则 None。
    (实现前先看 verifier.estimate_atm_reference 的字段访问方式,保持一致风格。)"""

def build_spy_option_strikes(spy_price: float, *, lines: int, step: int) -> list[int]:
    """n_strikes = max(2, lines // 2)(每 strike 占 C/P 两条线)。
    atm = round(spy_price / step) * step。
    返回 [atm + step*i for i in range(-(n_strikes // 2), n_strikes - n_strikes // 2)]
    (16 条线、step=2 → 8 个 strike,覆盖 atm-8 到 atm+6)。"""

def spy_option_spec_label(expiry: str, strike: int, right: str) -> str:
    return f"option:SPY:{expiry}:{strike}:{right}"

def spy_option_contracts(expiry: str, strikes: list[int]) -> list[tuple[str, str, Any]]:
    """对每个 strike 产出 C/P 两条:
    (spy_option_spec_label(...), "option",
     Option("SPY", expiry, float(strike), right, "SMART",
            multiplier="100", currency="USD", tradingClass="SPY"))
    (from ib_async import Option 按现有 option_contracts_from_specs 的局部 import 风格。)"""
```

### 1C. `Streamer` 类改动

- `__init__` 加 `self.spy_subs: dict[str, tuple[Any, VerifyRow]] = {}`。
- `ensure_option_plan(rows)` 末尾(SPXW 重订完成后)追加 SPY 重订逻辑,
  与 SPXW 共用同一次 replan 触发(即只在 `should_replan` 为 True 走到重订分支时执行):

```python
if self.stream_settings.spy_option_lines >= 2 and not self.skip_options:
    spy_price = estimate_spy_reference(rows)
    if spy_price is not None:
        strikes = build_spy_option_strikes(
            spy_price,
            lines=self.stream_settings.spy_option_lines,
            step=self.stream_settings.spy_strike_step,
        )
        cancel_subscriptions(self.ib, self.spy_subs)
        self.spy_subs = qualify_and_subscribe(
            self.ib,
            spy_option_contracts(plan.expiry, strikes),
            qualify=self.ibkr_settings.qualify_contracts,
        )
        log_event({
            "task": "ibkr_stream", "event": "spy_option_replan",
            "spy_atm": strikes[len(strikes) // 2] if strikes else None,
            "contracts": len(strikes) * 2,
        })
```

  (expiry 用 SPXW plan 同一个 `plan.expiry`——SPY 现在有每日到期,同日到期存在;
  若某天该到期不存在,qualify 会失败計入 error rows,属可接受降级,不需特判。)
- `flush()` 的 `subscriptions` 合并加入 `self.spy_subs`:
  `{**self.base_subs, **self.hot_subs, **self.rotation_subs, **self.spy_subs}`。
- 断连/重连清理:找到现有 `cancel_subscriptions(self.ib, self.hot_subs)` 的所有
  调用点(重连、关停路径),同样处理 `self.spy_subs` 并重置为 `{}`。

### 1D. env 文件

`.env`:
```
IBKR_STREAM_MAX_OPTION_LINES=44
IBKR_STREAM_SPY_OPTION_LINES=16
IBKR_STREAM_SPY_STRIKE_STEP=2
```
(60→44 是既有行的修改;后两行新增,加在它后面。)

`.env.example`: 同样三行 + 注释说明"总期权线预算 = MAX_OPTION_LINES(SPXW)+
SPY_OPTION_LINES(SPY 墙位对照),两者合计不要超过 IBKR 行情线上限的富余"。

---

## 块 2: 微信离线消息时间线汇总

需求: 微信 contextToken 失活期间,被判定"需要外发"的消息(Bark 已实时推手机)
同时落盘排队;通道恢复后(保活成功 或 下一条消息成功外发)把错过的消息按时间线
合成**一条**微信消息补推,然后清队。不调用 LLM,纯确定性拼装(比 agent 总结便宜、
可靠、无失真;openclaw 只作为发送通道)。

### 2A. `src/spx_spark/config.py` — `NotificationSettings` 加字段(带默认值)

```python
missed_queue_path: str = ""
```

`from_env` 中:
```python
missed_queue_path=env_str(
    "ALERT_NOTIFY_MISSED_QUEUE_PATH",
    f"{data_root.rstrip('/')}/latest/weixin_missed_queue.jsonl",
),
```
(`data_root` 变量在 from_env 里已存在,与 state_path 同款写法。)

### 2B. 新文件 `src/spx_spark/notifier/missed_queue.py`

依赖方向: 只 import stdlib、`spx_spark.config`、`notifier.model`、`notifier.sinks`
(在包内依赖图中位于 sinks 之后、pipeline 之前;pipeline 和 CLI import 它)。

```python
"""Missed-message queue: park approved WeChat messages while the channel is
dead, then flush them as a single timeline digest when it recovers."""

def append_missed(path: str, message: str, *, kind: str, at: datetime) -> None:
    """追加一行 JSON {"at": iso-utc, "kind": kind, "message": message}。
    父目录 mkdir(parents=True, exist_ok=True)。写失败(OSError)静默忽略——
    队列是尽力而为,不能影响主流程。"""

def load_missed(path: str) -> list[dict[str, Any]]:
    """读全部行,跳过坏行。文件不存在返回 []。"""

def clear_missed(path: str) -> None:
    """删除文件(missing_ok=True)。"""

DIGEST_MAX_ENTRIES = 12
DIGEST_MAX_CHARS = 1800

def build_digest(entries: list[dict[str, Any]], *, now: datetime | None = None) -> str:
    """时间线格式(北京时间 UTC+8):
    第一行: f"微信离线期间错过 {len(entries)} 条提醒,时间线如下:"
    随后最多 DIGEST_MAX_ENTRIES 条(取**最新**的 N 条,按时间升序排列):
      f"- {HH:MM} {first_line}"   # first_line = message 第一行,超 120 字符截断加 …
    若 len(entries) > DIGEST_MAX_ENTRIES,末行加:
      f"(另有 {len(entries) - DIGEST_MAX_ENTRIES} 条更早的已省略)"
    整体超 DIGEST_MAX_CHARS 时从尾部截断并加 "\n..."。"""

def flush_missed(
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
) -> SinkResult | None:
    """队列为空返回 None。否则 build_digest → send_openclaw_message;
    发送 ok 时 clear_missed;返回 SinkResult(不论成败)。"""
```

### 2C. `src/spx_spark/notifier/pipeline.py` 接线

1. **入队**——三处外发路径中,凡"已通过 gate、`send_openclaw_message` 返回
   `ok=False`"的,调用:
   `append_missed(settings.missed_queue_path, <该消息文本>, kind=<"direct"|"agent"|"codex">, at=now or datetime.now(tz=timezone.utc))`
   - direct 路径(openclaw_enabled)与 bypass 路径: kind="direct"
   - agent 审阅通过路径: kind="agent"
   - codex 路径: kind="codex"
   注意: 只有微信失败才入队;Bark 成败不影响入队(Bark 是即时提醒,微信补时间线)。
2. **机会性冲队**——`notify_payload` 中,`selected` 非空且
   (`settings.openclaw_enabled` 或 `settings.openclaw_agent_enabled` 或
   `settings.codex_enabled`)时,在进入任何外发路径**之前**:
   ```python
   digest_result = flush_missed(settings, runner=runner)
   if digest_result is not None:
       sinks.append(digest_result)
   ```
   冲队失败不影响后续流程(该 SinkResult ok=False 仅作记录)。
   注: `sent_count` 现按 `sink.sink in ("openclaw_message", "bark")` 统计,digest 的
   SinkResult.sink 就是 "openclaw_message",成功冲队会 +1,可接受,不需特判。

### 2D. 新 CLI `src/spx_spark/notifier/digest_cli.py`

```python
"""Flush the missed-message queue as one WeChat digest. Called by the
keepalive timer after it proves the channel is alive."""

def run(argv: list[str] | None = None) -> int:
    settings = NotificationSettings.from_env()
    entries = load_missed(settings.missed_queue_path)
    if not entries:
        print(json.dumps({"flushed": False, "count": 0}))
        return 0
    result = flush_missed(settings)
    print(json.dumps({"flushed": result is not None and result.ok,
                      "count": len(entries),
                      "error": result.error if result else None}))
    return 0 if (result and result.ok) else 1

def main() -> None:
    raise SystemExit(run())
```

`pyproject.toml` 注册: `spx-spark-weixin-digest = "spx_spark.notifier.digest_cli:main"`。
`notifier/__init__.py` re-export `flush_missed`、`append_missed`(加进 `__all__`)。

### 2E. `scripts/run-openclaw-weixin-keepalive.sh`

1. 文件顶部 `set -euo pipefail` 之后加:`export PATH="$HOME/.local/bin:$PATH"`
   (uv 在 ~/.local/bin,systemd 环境 PATH 不含它;参考 ibc-watchdog.sh 的先例)。
2. 脚本末尾、`if [[ "$ok" != "true" ]]` 分支**之前**(即成功路径)加:

```bash
if [[ "$ok" == "true" ]]; then
  # Channel proven alive; flush any missed-message digest (best effort).
  uv run spx-spark-weixin-digest || true
fi
```

`.env.example` 增(注释): `ALERT_NOTIFY_MISSED_QUEUE_PATH=`(留空用默认);
`.env` 不需要加(用默认路径)。

---

## 块 3: 测试(新文件,现有测试不改语义)

`tests/test_spy_option_lane.py`:
1. `test_build_spy_option_strikes_symmetric_window`:
   price=628.3, lines=16, step=2 → 8 个 strike,含 628,范围 [620, 634],全部偶数。
2. `test_build_spy_option_strikes_minimum_two_lines`: lines=2 → 恰 1 个 strike(atm)。
3. `test_spy_option_contracts_labels_and_trading_class`:
   labels 形如 `option:SPY:20260707:628:C`;Option 对象 symbol=="SPY"、
   tradingClass=="SPY"(直接断言返回的 contract 属性)。
4. `test_estimate_spy_reference_prefers_market_price_then_mid`:
   合成 VerifyRow(label="stock:SPY")两组:有 market_price 用之;只有 bid/ask 用 mid。
5. `test_stream_settings_read_spy_lane_env`(monkeypatch env):
   IBKR_STREAM_SPY_OPTION_LINES=0 → spy_option_lines==0。

`tests/test_missed_digest.py`(runner/fs 全部用 tmp_path 与 fake runner,风格照抄
tests/test_notifier.py 的 make_settings/make_payload,复制过来按需加
missed_queue_path 字段):
6. `test_append_load_clear_roundtrip`。
7. `test_build_digest_timeline_format`: 3 条 entries(不同 at)→ 首行含 "3 条",
   每条含北京时间 HH:MM 且升序,message 只取第一行。
8. `test_build_digest_caps_entries`: 15 条 → 显示 12 条 + "另有 3 条" 字样。
9. `test_pipeline_queues_message_when_weixin_fails_and_bark_ok`:
   agent 路径,openclaw message send 返回失败(ret 非零 stdout)、bark ok →
   queue 文件出现 1 条 kind="agent";告警仍进冷却(Bark 送达)。
10. `test_pipeline_flushes_queue_before_new_send`:
    预先 append 一条;新一轮 notify_payload(weixin 恢复,全部 ok)→
    runner 收到的第一条 `openclaw message send` 的 --message 含 "微信离线期间错过";
    之后 queue 文件不存在;新消息照常发送。
11. `test_digest_cli_returns_zero_on_empty_queue`(monkeypatch settings/env)。
12. `test_flush_missed_keeps_queue_on_send_failure`: send 失败 → 文件仍在。

bash 语法自检: `bash -n scripts/run-openclaw-weixin-keepalive.sh`。

## 验收标准

1. `uv run pytest -q` 全绿(含分层守护 tests/test_architecture.py)。
2. `uv run ruff check src/ tests/` 无错误。
3. `.env`: IBKR_STREAM_MAX_OPTION_LINES=44 + 两个 SPY 新变量;`.env.example` 同步。
4. `pyproject.toml` 注册 spx-spark-weixin-digest。
5. 优雅降级: SPY 价格缺失/qualify 失败不影响 SPXW lane;队列文件损坏/不可写不影响
   告警主流程。
6. 不 commit、不重启 systemd 服务(由验收人做)。
