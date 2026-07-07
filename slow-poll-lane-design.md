# 慢速轮询 lane:VIX 族与上下文 ETF 改为轮询,腾行情线给期权链(实施规格)

状态: 2026-07-07 设计完成,待实施。
分层约束: 改动集中在 `config.py`(L0)与 `ibkr/stream_collector.py`(L2),无新增跨层依赖。

## 背景与预算(实施者不需要重新调查)

- 现状常驻订阅: 23 条基础(7 指数 + 14 股票 + ES/MES)+ SPXW 44 + SPY 16 = 83 条。
- VIX/VIX1D/VIX9D/VIX3M/VVIX/SKEW 更新慢;QQQ/IWM/DIA/HYG/LQD/TLT/IEF/SHY/UUP/GLD/USO/RSP/XLU
  这 13 只 ETF 只作隐藏算法上下文。这 19 条改为每 5 分钟轮询一次(分批订阅→取值→退订),
  常驻基础只留 index:SPX、stock:SPY(SPY 期权 ATM 参考)、future:ES、future:MES = 4 条。
- 腾出的预算还给期权链: SPXW 44→60(恢复),SPY 16→20(10 个 strike ±$10)。
  稳态 4+60+20=84,轮询瞬时峰值 +6(单批)=90,仍低于 100 上限。
- 宽容机制已存在,直接复用:
  - stream 层: `snapshot_rows(..., slow_index_stale_after_seconds=…, slow_index_labels=…)`
    按 **label**(如 `stock:QQQ`、`index:VIX`)匹配,来自 `IbkrSettings.slow_index_labels`
    (env `IBKR_SLOW_INDEX_LABELS`)。
  - storage 层: `StorageSettings.slow_index_labels`(env `MARKET_DATA_SLOW_INDEX_LABELS`)
    按 **canonical id** 匹配——注意股票的 canonical 是 `equity:QQQ` 不是 `stock:QQQ`
    (指数/期货与 label 同形)。两个 env 都必须扩充,格式不同,这是本设计最大的坑。

## 块 1: 配置(config.py)

`IbkrStreamSettings` 加字段(全部带默认值)+ `from_env` 读取:

```python
slow_poll_labels: tuple[str, ...] = ()      # env IBKR_STREAM_SLOW_POLL_LABELS (csv, label 形式)
slow_poll_interval_seconds: float = 300.0   # env IBKR_STREAM_SLOW_POLL_INTERVAL_SECONDS
slow_poll_hold_seconds: float = 10.0        # env IBKR_STREAM_SLOW_POLL_HOLD_SECONDS
slow_poll_chunk_size: int = 6               # env IBKR_STREAM_SLOW_POLL_CHUNK_SIZE
```

`from_env` 中 slow_poll_labels 默认值(一个模块级常量,便于测试引用):

```python
DEFAULT_SLOW_POLL_LABELS = (
    "index:VIX", "index:VIX1D", "index:VIX9D", "index:VIX3M", "index:VVIX", "index:SKEW",
    "stock:QQQ", "stock:IWM", "stock:DIA", "stock:HYG", "stock:LQD", "stock:TLT",
    "stock:IEF", "stock:SHY", "stock:UUP", "stock:GLD", "stock:USO", "stock:RSP", "stock:XLU",
)
```

用 `tuple(env_csv("IBKR_STREAM_SLOW_POLL_LABELS", ",".join(DEFAULT_SLOW_POLL_LABELS)))`;
env 设为空串时得到 () 即关闭轮询、回到全常驻(env_csv 对空串的行为先确认,若返回
[""] 需过滤空项)。

## 块 2: stream_collector.py

### 2A. 纯函数

```python
def split_base_contracts(
    contracts: list[tuple[str, str, Any]],
    slow_poll_labels: tuple[str, ...],
) -> tuple[list[tuple[str, str, Any]], list[tuple[str, str, Any]]]:
    """按 label 是否在 slow_poll_labels 拆成 (persistent, slow)。
    slow_poll_labels 里没出现在 contracts 的项忽略。"""

def chunked(items: list[T], size: int) -> list[list[T]]:
    """size<=0 时按 1 处理。"""
```

### 2B. Streamer 类

- `__init__`: `self.slow_cache: dict[str, VerifyRow] = {}`、`self.last_slow_poll = 0.0`、
  `self.slow_contracts: list[tuple[str, str, Any]] = []`。
- `subscribe_base()`: `contracts = build_base_contracts(...)` 后
  `persistent, slow = split_base_contracts(contracts, self.stream_settings.slow_poll_labels)`;
  只对 persistent 做 qualify_and_subscribe(现有日志照旧,contracts 数会变小);
  `self.slow_contracts = slow`。订阅完成后立即 `self.poll_slow_contracts()`
  (保证启动就有 VIX/SKEW 数据,不等第一个 interval)。
- 新方法:

```python
def poll_slow_contracts(self) -> None:
    if not self.slow_contracts:
        return
    polled = 0
    for chunk in chunked(self.slow_contracts, self.stream_settings.slow_poll_chunk_size):
        subs = qualify_and_subscribe(self.ib, chunk, qualify=self.ibkr_settings.qualify_contracts)
        self.ib.sleep(self.stream_settings.slow_poll_hold_seconds)
        rows = snapshot_rows(
            subs,
            self.ibkr_settings.stale_after_seconds,
            slow_index_stale_after_seconds=self.ibkr_settings.slow_index_stale_after_seconds,
            slow_index_labels=frozenset(self.stream_settings.slow_poll_labels)
            | self.ibkr_settings.slow_index_labels,
        )
        for row in rows:
            self.slow_cache[row.label] = row
        polled += len(rows)
        cancel_subscriptions(self.ib, subs)
    self.last_slow_poll = time.monotonic()
    log_event({"task": "ibkr_stream", "event": "slow_poll_done",
               "labels": len(self.slow_contracts), "rows": polled})
```

  (`time` 模块与 log_event 的现有 import 方式先看文件头;若现有代码用别的时钟源保持一致。)
- 主循环(找到调用 `flush()`/`rotate_options()` 的 run 循环):在每轮 flush 前加

```python
if (
    self.slow_contracts
    and time.monotonic() - self.last_slow_poll
    >= self.stream_settings.slow_poll_interval_seconds
):
    self.poll_slow_contracts()
```

- `flush()`: 合并缓存行——`rows = snapshot_rows(...)` 之后:

```python
subscribed_labels = set(subscriptions)
rows.extend(row for label, row in self.slow_cache.items() if label not in subscribed_labels)
```

- `teardown()`: `self.slow_cache = {}`(轮询无常驻订阅,无需 cancel)。
- 重连路径: 若重连会重跑 subscribe_base 则自动恢复;确认无残留状态即可。

### 2C. 环境文件

`.env` 改/增(`.env.example` 同步,附注释):

```
IBKR_STREAM_MAX_OPTION_LINES=60
IBKR_STREAM_SPY_OPTION_LINES=20
IBKR_STREAM_SLOW_POLL_INTERVAL_SECONDS=300
IBKR_STREAM_SLOW_POLL_HOLD_SECONDS=10
IBKR_STREAM_SLOW_POLL_CHUNK_SIZE=6
IBKR_SLOW_INDEX_STALE_AFTER_SECONDS=900
IBKR_SLOW_INDEX_LABELS=index:VIX,index:VIX1D,index:VIX9D,index:VIX3M,index:VVIX,index:SKEW,stock:QQQ,stock:IWM,stock:DIA,stock:HYG,stock:LQD,stock:TLT,stock:IEF,stock:SHY,stock:UUP,stock:GLD,stock:USO,stock:RSP,stock:XLU
MARKET_DATA_SLOW_INDEX_LABELS=index:VIX,index:VIX1D,index:VIX9D,index:VIX3M,index:VVIX,index:SKEW,equity:QQQ,equity:IWM,equity:DIA,equity:HYG,equity:LQD,equity:TLT,equity:IEF,equity:SHY,equity:UUP,equity:GLD,equity:USO,equity:RSP,equity:XLU
```

(IBKR_STREAM_SLOW_POLL_LABELS 不写,用代码默认;stale 阈值 900s = 3 个轮询周期,
避免 300s 阈值与 300s 周期打架导致 live/stale 抖动。注意 stream 层 env 用 label 形式、
storage 层 env 用 canonical 形式,股票前缀分别是 stock:/equity:。)

## 块 3: 测试(新文件 tests/test_slow_poll_lane.py)

1. `test_split_base_contracts_partitions_by_label`: 构造含 index:SPX/index:VIX/stock:SPY/
   stock:QQQ/future:ES 的合约列表 + 默认 slow 列表 → persistent 恰含 SPX/SPY/ES,slow 恰含
   VIX/QQQ。
2. `test_chunked_sizes`: 19 项 size 6 → [6,6,6,1];size 0 → 按 1。
3. `test_stream_settings_slow_poll_env`(monkeypatch): 默认 19 个 label;
   IBKR_STREAM_SLOW_POLL_LABELS="" → 空 tuple(轮询关闭)。
4. `test_flush_merges_slow_cache_rows`: 构造 Streamer(用 tests 里现有的 fake ib/构造方式,
   参考 tests/ 里已有 stream collector 测试;若没有可直接实例化并 stub ib)——
   slow_cache 塞一条 label="index:VIX" 的 VerifyRow,subscriptions 不含它 → flush 产出的
   rows 集合含该行;subscriptions 含同名 label 时缓存行不重复出现。
   (若 flush 直接调 persist 不好隔离,允许把"合并 rows"抽成纯函数
   `merge_slow_rows(rows, slow_cache, subscribed_labels)` 并只测纯函数,flush 调它。)
5. `test_poll_slow_contracts_caches_and_cancels`: stub qualify_and_subscribe/
   cancel_subscriptions/snapshot_rows(monkeypatch 模块函数),fake ib.sleep no-op →
   调 poll_slow_contracts 后 slow_cache 有行、cancel 按 chunk 次数被调、last_slow_poll 更新。

现有测试(尤其 test_spy_option_lane.py、任何 stream collector 测试)不许改语义;
IbkrStreamSettings 新字段带默认值,现有构造不受影响。

## 验收标准

1. `uv run pytest -q` 全绿;`uv run ruff check src/ tests/`。
2. `.env` 与 `.env.example` 按块 2C 更新。
3. 优雅降级: slow_poll_labels 为空时行为与改动前完全一致(全部常驻)。
4. 不 commit、不重启服务(验收人做)。
