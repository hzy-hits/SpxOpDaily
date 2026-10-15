# Desk Map 模型身份失败回退与空ATM崩溃修复

<!-- documentation-status: 2026-10-15 -->
> **文档定位：事故修复与时点验收。** 时点验收记录；现场时间、计数和部署结论仅证明文中那次检查，不代表此刻持续健康。
> [全仓文档、当前运行状态与合同优先级](README.md)（目录核对：2026-10-05）。

## 生产证据

2026-09-10 14:00北京时间Python已生成GTH desk_map_projection，observed_through为06:00:03 UTC，源质量ready；Python任务记录rust_report_owner是既有单owner设计。Rust report health处于backoff，last_error_code=unexpected_model，last_response_model=deepseek-flash；最后成功持久化时间为2026-09-09T19:32:17Z。故地图生成失败不能解释成没有策略机会。没有读取Bark记录，尚未核验终端收件。

Core在9月9日13:26至23:54 UTC出现133次NoneType格式化异常，自动重启计数17。按小时分布13时4次、20时33次、21时35次、22时35次、23时26次，主要发生在开盘前/闭市；不能据此声称整个RTH都被该异常阻断。

## 修改与验收

S1：现有options.py同合约衰减比较在front存在但atm_strike缺失时仍执行:g格式化；改为atm_strike_unavailable且不生成衰减。覆盖有历史而当前ATM缺失的输入（含straddles空映射与直接调用两条路径）。不修改经济阈值。

Phase 6冻结Rust生产故障例外：复用spx-report已有projection fallback。HTTP成功但返回未知模型时，模型内容仍被拒绝，改用当前有效、已通过validate的Python DeskMessage原文；不推断模型别名，不使用未知模型内容。原有投影到期检查、slot幂等、owner lease和持久化流程继续生效。针对未知模型返回不可信内容，集成测试要求只持久化原始地图一次。其他transport/HTTP等错误不在本次新增回退范围。

这是已有源消息回退的故障修复，不改变报告owner、Bark代码/配置/投递行为、交易候选范围或自动下单权限。报告client仍拒绝未知model，health明确记录unexpected_model回退原因；不把回退报告描述成模型生成成功。

## 发布边界

Python按现有安装脚本发布并仅重启Core。Rust按rust/docs/OPERATIONS.md使用host system service；报告生产旧release的spx-report/domain/ledger源码与本次修改前仓库一致。创建新不可变release，其他三个binary保持旧文件校验和，仅替换spx-report并重启报告service。不重启Rust core/bridge/delivery，不转移owner。

源文件新增/删除0/0，生产修改3个，净生产LOC +8；依赖、配置键、service/timer、数据库/表增删均0。旧空ATM格式化路径和未知模型导致整张原始地图被阻断的分支被替换。全量测试、部署提交和健康验收结果存于/srv/data/spx-spark/research/desk-map-recovery-2026-09-10/。

## RTH候选预算异常

额外核查9月9日RTH日志：3570个特征周期成功，1次FrozenInstanceError（不能赋值passed）。strategy_select在三个候选路径被否决、仍有第四个候选时向frozen RankResult.passed赋值；改为清空其原有可变列表，保持现有预算及未评估项审计。新增公共build_strategy_decision的四候选反例，验证NO_TRADE和第四候选path_not_evaluated_budget均正常输出。这里修复的是异常，不提高预算、不跳过任何候选hard gate。该故障不能证明全天没有交易机会。

发布验收：全量Python3407项通过（2 warnings），相关206项测试通过；Rust fmt、Clippy、workspace tests及release build通过；新报告binary的生产check-config通过；Ruff、Import Linter与git diff --check通过。

## 2026-10-15：模型失败阻断报告，恢复窗口被等待耗尽

Phase 6 冻结期间生产故障修复，仍复用 `spx-report` 和既有 outbox。
02:06 UTC 告警时，上游与报告端均有 02:00:11.914259 的桌图，桥接 ACK 正常，
数据盘仍有约 51 GiB 可用；报告保存却停在 00:30:35.735661。
这次故障位于报告生成环节，不是磁盘不足或上游没有行情。

现场报告最后尝试的是原投影 `desk-map:9700edd768e8d5766413ee1d`；02:00:12.760647
已镜像的恢复投影 `desk-map:0b067c24cabc662c60e0307a` 没有被消费为新报告。
旧程序记录了一次连续生成失败，随后进入 `awaiting_slot`。其每秒轮询在入口清空
`last_error_code`，且生成错误不写 journal；剩余响应摘要没有模型身份、结束原因或
可见内容长度。因而不能从这些证据断言当时具体是超时、HTTP 错误还是无效响应。

源码确认的故障放大机制：transport/HTTP/响应外壳错误及截断不允许使用已校验的
原始桌图兜底；生产模型请求上限为 180 秒，而恢复投影只接受 120 秒。模型等待跨过
恢复窗口后，最新投影的恢复标记会让该次轮询进入等待状态，无法继续原正常时段重试。
已有测试还将“截断后没有报告”固定成正确行为。这些条件把可选文案生成失败变成了
整张桌图缺失。

修复保留模型返回的严格校验，但上述 provider 故障直接复用当前已通过合同校验的
原始 `DeskMessage`，不采用模型的部分输出。源有效期、session、slot 幂等和持久化
owner 检查保持原有语义，失效源不会通过兜底重新获得有效期；本地配置、授权、
投影或序列化错误仍阻断。生产请求等待恢复为已有配置默认的 90 秒，使超时兜底可在
两分钟恢复窗口内完成，不扩大恢复窗口或补发旧卡。

失败尝试只记录错误类别、HTTP 状态和耗时，不记录响应正文或凭据；健康错误保留到
新的结果明确覆盖，等待下一时段不再将其擦掉。没有响应的 transport 兜底清空旧模型
元数据，不把原始桌图表示成模型成功输出。模型请求期间若出现更新的有效恢复投影，
仍合并为本时段一份报告，保留此前防重复机制。

Rust workspace/all-targets/all-features **318 项测试通过**，fmt、Clippy 与
`git diff --check` 通过。验收包括 HTTP 429/503、无效响应外壳、transport 失败及
截断时只持久化原始地图一次，过期/未来/旧时段输入不发布，本地错误跨时段保持可见。

生产文件新增/删除 **0/0**，净生产 LOC **+21**（不计文件内测试）；依赖、配置键、
service/timer、数据库/表增删 **0**。
仅修改既有 report 逻辑和请求超时值；策略授权、行情采集及 Bark 实现均不改变。
原先禁止 provider 故障兜底的路径和每次轮询清空故障的路径被替换。

上线代码为 `99d095b8`，02:21:37 UTC 已切换到不可变 release，仅替换并重启
`spx-rust-report`，非敏感 `report.toml` 的请求超时由 180 改为 90 秒。Core、Bridge、
Delivery 的 PID 和二进制校验和均未改变，网络 gate、owner、通知目标及凭据未改变。
第一次切换因 `Type=simple` 刚启动时进程仍为 `systemd-executor`，过早校验触发自动
回滚；补上最多五秒的实际 executable 检查后再次成功切换。回滚与最终切换记录保留在
`/srv/data/spx-spark/deploy-backups/20261015-report-fallback/`，其中不含凭据文件。
发布后清理了本轮约 1.9 GiB 构建产物，运行程序位于 `/opt`，未删除运行文件。

02:30 定时任务完成真实验证：02:30:06 开始模型生成，02:30:08.228580 的更新恢复
投影随即到达。02:31:36.704556 的新日志明确记录 `transport_failed`、耗时 **90 秒**；
该证据证明本轮外部模型调用在等待上限失败，不追溯臆测先前已丢失的具体错误码。
程序采用当前恢复投影，以正常半小时 slot 合并保存：
`last_scheduled_persisted_at=2026-10-15T02:31:36.704175821Z`，原因记录为
`data_recovery_coalesced`。三个目标于 02:31:38–39 UTC 均取得 `delivered` 服务端回执，
该时段只有一个报告事件；这是服务端送达验收，不代表已确认手机显示或用户阅读。
未补发 01:00、01:30、02:00 旧卡。模型可用性仍有问题，但不再阻断这轮新鲜桌图。
