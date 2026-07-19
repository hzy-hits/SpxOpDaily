# SPXW Exposure Cockpit 数据与表现复核

日期：2026-07-20（Asia/Shanghai）

代码基线：`master` / `609af19`；本文记录其后的数据语义、前端与部署修正。

## 结论

Gamma 的定价公式、矩阵方向、时间/价格网格和三栏 SPX 纵轴映射没有发现转置或符号实现错误。此前图形“怪”主要来自数据语义和表现层，而不是 Canvas 把矩阵画反：

1. Strike Profile 曾把前夜 IBKR/GTH 的局部链快照与白天 Schwab/RTH 快照比较；两边 provider、交易段和合约覆盖不同，不能解释成 SOD 到 Current 的变化。
2. GTH collector 只能证明已观测合约的新鲜度，不能证明整条 SPXW 链完整；相邻帧的合约覆盖会变化。
3. 旧的 peak/trough 允许把价格网格边界极值标成峰谷，产生密集且误导的边界标记。
4. Strike 面板原来只画 signed Gamma proxy，用户看不到实际 OI 数量；baseline 虚线还可能被 current bar 遮住。
5. 完整 GTH→gap→RTH 画布会把 6.5 小时 RTH 压缩在全天约三分之一宽度内。这是固定全时段坐标的真实结果，不是时间轴错误。

当前结果仍是 OI/Volume Exposure Proxy，不是 Market Maker、Dealer 或 participant 实际仓位。

## 数据与契约修正

Session Surface 保持 `schema_version=2`，升级到：

```text
policy_version=spxw_session_surface.v5
cache_contract=8
```

主要契约变化：

- Strike baseline 只允许使用相同 `session_kind + surface_provider + reference_method` 的最早 causal、有效质量快照。
- RTH 当前与 baseline 均为 `rth / schwab / direct_index_spx`。
- GTH 因完整合约宇宙不可证明，baseline 全部返回 `null`，并声明：

  ```text
  baseline_unavailable_reason=gth_contract_universe_completeness_unproven
  gth_complete_chain_available=false
  ```

- 09:25 ET closed gap 的 current、baseline、Strike rows、spot 和 reference 均 fail-closed；缺失值不补零。
- GTH 非 missing surface column 固定为 `quality=degraded`。
- peak/trough 先寻找价格网格内部局部极值，再取最强候选；网格边界不再冒充峰谷。
- `comparison_semantics=snapshot_state_not_position_or_flow`、`exact_sod_available=false` 持续可见。

Gamma proxy 使用项目既有尺度：

```text
contract gamma × OI/volume weight × 100 multiplier × SPX² × 1%
```

Call 取正、Put 取负。这是明确的研究 proxy sign convention，不是已知 dealer inventory 方向。

## 生产数据抽查

固定 Session Canvas 为前一交易日 20:15 ET 至当日 16:00 ET，默认 5 分钟 × 5 SPX 点；2026-07-17 payload 为 237 个时间桶 × 41 个价格点。

| 回放点 | Strike 语义 | OI 抽查 | PIT/边界 |
| --- | --- | ---: | --- |
| 09:05 ET GTH | IBKR partial-chain current only；baseline disabled | current 327,774 | `lookahead_rows_selected=0`；全部 GTH 列 degraded |
| 09:25 ET gap | current/baseline/rows unavailable | 不显示零值 | spot/reference unavailable |
| 10:27 ET | RTH Schwab current vs first validated RTH Schwab snapshot | 329,562 vs 310,394 | `lookahead_rows_selected=0`；边界 extrema=0 |
| 15:30 ET | RTH Schwab current vs 同一 09:34:28 ET baseline | 301,683 vs 310,394 | `lookahead_rows_selected=0`；边界 extrema=0 |

OI 两个快照之间的差异不能称为 signed flow、open/close 或真实持仓变化。

## 前端修正

- 中栏默认显示 `Call + Put OI` 数量，bar 颜色由当前 signed Gamma proxy 决定：正值蓝、负值红、零或缺失为中性色。
- 增加 `OI / Γ Proxy` 原位切换，不替换 Canvas 或重建整页。
- current bar 先绘制，baseline 虚线和 endpoint marker 后绘制，两个快照都可见。
- GTH 顶部、legend、tooltip 和 Audit drawer 均显示 `PARTIAL-CHAIN PROXY` 与 completeness limitation。
- GTH baseline 不可用时不画伪 baseline；closed gap 保持 Waiting/Missing 和斜纹。
- Gamma、Strike、Charm 继续共享完全相同的 SPX 价格范围、current-price line、crosshair 与 tooltip。
- robust color domain 继续使用 p98；零固定中性色，tooltip 保留未截断原值。

## 浏览器验证

Oracle 实际部署入口在 2048×1150 下完成验证：

- 页面 `scrollHeight=clientHeight=1150`，无首屏滚动。
- 三栏 stage 的 top/bottom/height 完全一致。
- 10:27、15:30、09:10 GTH、09:25 gap 均正常。
- OI/Γ 切换 101 次耗时 118 ms，约 1.17 ms/次。
- 连续播放 5 秒约 28.2 visual FPS；DOM=315、Canvas=9 保持不变。
- 强制 GC 后 retained heap 增量约 0.98 MiB。
- Console errors、page errors、HTTP >=400 均为 0。

截图：

- [2026-07-17 10:27 ET，2048×1150](../artifacts/spxw-surface/spxw-exposure-2026-07-17-1027ET-2048x1150.png)
- [2026-07-17 15:30 ET，2048×1150](../artifacts/spxw-surface/spxw-exposure-2026-07-17-1530ET-2048x1150.png)

## 加载性能与资源修正

Replay 数学 kernel 已使用 NumPy 向量路径；生产规模探针由 9.8064 s 降到 0.7172 s，约 13.7×，最大相对误差 4.34e-16。Live/trading scalar path 未被替换。

本次部署还发现 replay service 的冷 GTH 扫描工作集约 1.1 GiB，而原 unit 在 `MemoryHigh=1G` 就触发回收，产生约 1 GiB swap 和 1,638 次 memory-high 事件，冷构建超过 3 分钟仍未完成。资源边界已改为：

```text
MemoryHigh=2G
MemoryMax=3G
```

调整后同一生产数据：

- 一次性全 GTH seed：17.08 s；peak RSS 约 1.12 GiB，无 swap/high event。
- 相邻未缓存 GTH cutoff：0.544 s。
- GTH/RTH disk cache hit：约 0.14 s。
- Redis 未引入；当前瓶颈是首次 DuckDB/GTH 原始扫描和曲面物化，不是缓存查找或网络。

## 测试

- Python 全量：`1808 passed, 1 warning`；warning 仅为上游 `websockets.legacy` deprecation。
- JavaScript 四组 contract tests：通过。
- `node --check site/spxw-surface/public/app.js`：通过。
- Ruff、`git diff --check`、shell `bash -n`、systemd unit verify：通过。
- 覆盖新增：same-provider baseline、GTH baseline fail-closed、09:25 boundary、cache tamper、PIT/no-lookahead、内部 extrema、OI 非负、GTH falsely-ready、色域与 missing-null。

## 仍受外部数据阻塞

以下能力没有用模型推断冒充完成：

- participant/MM/dealer 实际仓位；
- buy/sell signed flow；
- open/close 分类；
- 完整 GTH SPXW contract universe；
- 官方 GTH SPX OHLC（当前为 Schwab ES 减冻结 basis 的 inferred SPX reference）。

## Scripts 审计

`scripts/` 有 62 个 tracked 文件，其中 23 个只是 console-entry shim，20 个被 systemd 直接引用；不能整目录批量删除。

最安全的首批删除候选是：

- `generate_options_map_golden_pre_extraction.py`
- `run-ibkr-positions.sh`
- `run-maintenance-prune.sh`
- `run-maintenance-purge-mock.sh`

四者均无 repo、docs、CI、systemd、cron、运行进程或 shell history 引用；底层 CLI 仍保留。本次只完成审计，没有执行破坏性删除。

更优先的部署债务：repo 的 `spx-spark-order-map.timer` 为 `Persistent=true`，当前用户级部署仍是 `Persistent=false`；另有 8 个已部署 unit 不受统一 installer 管理。应先统一 unit 安装/漂移检查，再单独提交删除上述四个候选。

## 部署与回滚

部署检查：

```bash
systemctl --user daemon-reload
systemctl --user restart spx-spark-surface-replay.service
systemctl --user restart spx-spark-surface-live.service
curl --unix-socket /srv/data/spx-spark/data/published/spxw-surface/runtime/replay-api.sock http://localhost/healthz
curl --unix-socket /srv/data/spx-spark/data/published/spxw-surface/runtime/live/live-api.sock http://localhost/healthz
```

前端由 `site/spxw-surface/public` bind mount，文件更新即时生效。回滚应使用一次可审计的 `git revert`，随后恢复对应 systemd unit 并重启 replay/live service。v5/cache8 与旧 cache contract 隔离，不需要删除历史 cache。
