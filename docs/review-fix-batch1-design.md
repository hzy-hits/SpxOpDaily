# kangyu 评审修复·批次 1:正确性修复(实施规格)

状态: 2026-07-07 设计完成,待实施。对应 docs/architecture-review-kangyu.md 的
P1-2、P1-3(残留)、P1-4、P1-5、P2-3(残留)、P2-5、P3-1、S3、S6、A5。
已在此前改造中修复、本批不再处理: P1-1(冷却 key 已改为 kind|instrument|dedup_group)、
P2-1(storage 已有 flock exclusive_lock)、P2-4(tick 101 OI 已接)、S7(micopedia 已自动接线)、
A1(已有流式 stream collector)。

分层约束: 遵守 module-architecture.md。每个修复点独立、互不依赖,按顺序做。

---

## F1(P1-2)无源时间戳的报价永不降级 → 用 received_at 兜底

`src/spx_spark/marketdata.py` 的 `Quote.quote_age_ms`(约 303 行):
`source_time = self.quote_time or self.trade_time` 为 None 时返回 None,
导致 `storage.degrade_stale_quote` 对这类行直接跳过,昨天的 LIVE 报价永生。

改法: source_time 为 None 时退回 `self.received_at`:

```python
source_time = self.quote_time or self.trade_time or self.received_at
```

`received_at` 是必填字段,不会再返回 None——把 `-> float | None` 保持不变
(调用方仍容 None,防御未来变化),但移除 None 分支。
注意: 采集入库时 `as_of=received_at` → age=0,不影响入库判定;只影响后续
load/degrade 时的老化,这正是想要的。

测试(tests/test_storage.py 或新 test): 构造 quote_time=None、trade_time=None、
received_at=2 小时前、quality=LIVE 的 Quote → degrade_stale_quote 后 quality 变 STALE。

## F2(P1-3)过期期权行残留 → storage 层按到期日淘汰

现状: IBKR stream collector 已用 replace_provider_quotes=True(主路径不膨胀),
但 Schwab collector 与快照式 ibkr/collector 是纯合并;且历史 state 里已有的
过期行没人清。

改法: `src/spx_spark/storage.py` 加纯函数:

```python
def prune_expired_option_quotes(quotes: Iterable[Quote], *, now: datetime) -> tuple[Quote, ...]:
    """丢弃 instrument_type==OPTION 且 expiry 早于纽约当日的行。
    expiry 解析: 先看 InstrumentId.expiry 的实际存储格式(YYYYMMDD 或 YYYY-MM-DD,
    读 marketdata.py 确认;两种都容错)。解析失败的行保留(宁可留不误删)。
    "纽约当日" = now.astimezone(NY_TZ).date()。expiry == 当日保留(0DTE 盘后仍要看)。"""
```

接线: `LatestStateStore.update` 里 merge 完、写盘前调用;`load` 里解析完 quotes 后
也调用(清存量)。NY_TZ 从 config import(storage 已 import config 则复用)。

测试: 三条 option quote(昨日到期/今日到期/明日到期)+ 一条 index quote →
prune 后昨日的消失,其余保留;expiry 无法解析的保留。

## F3(P1-4)IV 归一化按数值猜 → 按 provider 约定

现状 `marketdata.normalize_implied_vol`: >3.0 一律 /100,会把 0DTE 深翼的真实
350% IV 掐成 0.035。三个调用点分别处理:

1. `src/spx_spark/ibkr/adapter.py`(model_iv): IBKR model IV 本来就是小数。
   改为不做 /100 启发式——`clean_float` 后做范围守卫: `0 < iv <= 10` 否则 None。
2. `src/spx_spark/schwab/adapter.py`(volatility): Schwab 该字段是百分数
   (且缺失时可能为 -999)。改为: `clean_float` 后 `<= 0` → None,否则一律 `/100`,
   再套同样 `0 < iv <= 10` 守卫。
3. `marketdata.quote_from_dict`(持久化回读): 存进去的已是归一化小数,
   回读绝不能再 /100(否则合法的 3.5 会在一次 load/save 往返中被掐)。改为
   `clean_float` + 范围守卫,不做除法。

`normalize_implied_vol` 改名/重构随意,但最终三处语义如上;若保留旧函数给
mock/hyperliquid 用,先查其余 grep 调用点。

测试: ibkr 3.5 → 3.5;schwab 350(百分数) → 3.5;schwab -999 → None;
quote_from_dict 里 implied_vol=3.5 回读仍 3.5。

## F4(P1-5)StrikeGex OI 可能为 None

`src/spx_spark/options_map.py` 约 341-342 行:
`finite_float(call.open_interest) if call else 0.0` → `finite_float(...) or 0.0 if call else 0.0`,
等价写法自便,保证字段恒为 float。两行(call/put)都改。加一条测试:
open_interest=None 的合约 → StrikeGex 字段为 0.0。

## F5(P2-3 残留)service_loop 自带 env 解析 → 收敛到 config

`src/spx_spark/service_loop.py` 顶部自定义的 `env_bool`(约 89 行,可能还有
env_str/env_float/env_int 同类)删除,改 import config 的公开 helpers
(env_bool/env_str/env_float/env_int)。注意 config.env_bool 对非法值抛异常
(旧实现静默 False)——这是有意收严;确认 .env 现值全部合法即可。

## F6(P2-5)通知未启用时系统事件告警无限重复

`src/spx_spark/alert_engine.py` run() 末尾(约 1055-1062 行)的持久化条件,
把"通知功能未启用"视同"无需等待送达":

```python
notified = notification_result is not None and notification_result.sent_count > 0
settled = not notification_settings.enabled or notified
if not system_event_pending or settled:
    persist_system_event_state(state)
if not movement_pending or settled:
    persist_movement_state_snapshot(state)
```

测试: monkeypatch NotificationSettings.enabled=False + 有 pending 系统事件 →
persist_system_event_state 被调用(可用 monkeypatch 计数)。

## F7(P3-1)期权 bid=0 拒绝出 mid

`marketdata.Quote.mid`(约 265 行): 仅当 instrument_type==OPTION 时允许 bid==0
(ask>0 且 ask>=bid),mid=(0+ask)/2;其他 instrument 维持原语义(bid<=0 拒绝)。
测试: option bid=0/ask=0.1 → mid 0.05;index bid=0 → None。

## F8(S3)expected move = straddle 高估 18%

`src/spx_spark/options_map.py` 约 627 行 `expected_move_points=straddle` 改为:

```python
expected_move_points=straddle * 0.85 if straddle is not None else None,
```

加注释: ATM straddle ≈ 1.25σ,业界 1σ 近似 = 0.85×straddle。
`straddle` 原始字段(若已单独暴露)不动。expected_move_pct 若由 points 推导则
自动跟随,确认一下。更新相关测试断言(如有);
quant-probability-design.md 不用改(历史设计文档)。

## F9(S6)IV surface 5m 差分不校验时间间隔

`src/spx_spark/iv_surface.py`: 找到 `previous = load_latest_snapshot(...)` 之后
做差分的位置,加守卫——`current_as_of - previous.as_of` 超过
`IV_SURFACE_DIFF_MAX_GAP_SECONDS`(env_float,默认 600)时按 previous=None 处理
(所有 *_5m 字段输出 None),避免服务停 2 小时后重启的第一帧把 2 小时变化当 5 分钟
变化触发假告警。测试: previous.as_of=2 小时前 → atm_iv_jump_5m 等为 None;
5 分钟前 → 正常差分。

## F10(A5)agent 门故障时 critical 信号静默漏报 → fail-open 保底

`src/spx_spark/notifier/pipeline.py` agent 审阅路径(openclaw_agent_enabled 分支):
当 `agent_result.ok` 为 False(agent 超时/异常,非"判定不推送")时,若
review_candidates 中存在 severity=="critical" 的告警,直接把
`format_alert_message(payload, critical_alerts)` 走 send_openclaw_message + bark
兜底外发(失败照常 append_missed,kind="failopen"),并把这些 critical 告警
mark sent 进冷却。仅 critical;high 及以下维持现状(宁静默不扰民)。
codex 路径(codex_enabled 分支)如果结构对称、改动小,同样处理;不对称就只做 agent 路径并留注释。

测试: fake runner 让 agent 命令抛错,payload 含一条 critical + 一条 high →
openclaw message send 被调用且消息含 critical 那条的 title;仅含 high 时不外发。

---

## 验收标准

1. `uv run pytest -q` 全绿;`uv run ruff check src/ tests/`。
2. 每个 F 点至少一条对应测试(F5 除外,现有测试覆盖即可)。
3. 现有测试语义不改;`expected_move` 相关既有断言按 0.85 系数更新属允许。
4. 不 commit、不重启服务(验收人做)。
