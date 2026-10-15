# 2026-09-25 桥接磁盘故障恢复

<!-- documentation-status: 2026-10-05 -->
> **文档定位：事故修复与时点验收。** 时点验收记录；现场时间、计数和部署结论仅证明文中那次检查，不代表此刻持续健康。
> [全仓文档、当前运行状态与合同优先级](README.md)（目录核对：2026-10-05）。

范围：Phase 6 Rust 冻结期间生产故障例外，沿用既有 Core/Bridge 与归档 owner。
无策略权限、Bark、wire schema、数据库或进程边界变化。

## 根因与观察

14:10:17 UTC，Core 记录剩余10,737,340,416字节，写入该帧连同10 GiB reserve需
10,737,619,022字节，拒绝追加。系统随后把可恢复的存储背压统一编码成
`processing_rejected`。桥接累计三次相同帧拒绝后退出；每分钟systemd重启仍会发送
持久化的相同帧，因此持续失败。14:11告警属实，并非行情时间误报。

原归档任务18:30 ET（夏令时22:30 UTC）运行，但只允许归档完整UTC日期：
9/24晚仍不能归档9/24，且16 GiB raw-frame预算允许多个已压缩副本继续占盘。
盘中9/23和9/24各留约7 GB原始帧，加上9/25增长，共约18 GB。
28 GiB Python清理行动线只拥有Python replay源文件，不会自动清理Rust帧。
另发现旧prune在全目录排他锁内做大文件校验，盘中运行会挡住实时append。

## 修复

- Core的`InsufficientFreeSpace`复用现有`server_busy`响应；其余坏合同或处理错误
  保持原拒绝语义。Bridge沿用既有退避与exact pending frame，不新增重试机制。
- prune使用共享目录锁及已选完整日期的排他锁；保持源SHA、归档屏障、修改检查和
  当日/未来日期保护。旧日期校验期间当前日期可以继续写入。
- 原timer改为00:15 UTC；原service仍先archive再prune，保留一日规则配合8 GiB
  raw-frame大小预算。10 GiB磁盘reserve不变。没有新增timer或服务。
- Bridge入口隔离Python专用的`denoising_forward`扩展，再解析冻结的Rust研究合同；
  `automatic_ordering=false`、因果时间和其他未知字段仍严格校验。修复的是研究投影
  被整份拒收的独立降级问题，不把它当作磁盘停机根因。

## 紧急恢复与数据证明

恢复中止了会阻挡实时写入的旧版整目录prune，未绕过归档屏障。临时回收仅针对
2026-09-23已完成日期：先核对`verified` manifest与压缩文件SHA，再逐份核对源文件
SHA/大小/mtime/inode，使用原共享目录锁和日期排他锁后删除冗余。
111个源帧共7,388,018,278字节（约6.88 GiB）完成校验并删除；
`archive/date=2026-09-23/frames.ndjson.zst`及manifest保留。
原始IBKR/Schwab数据湖、当前日期帧和未验证历史均未删除。

本次另清理了可重建Rust调试缓存；它位于根盘，不计入上述数据盘回收量。
14:23 UTC桥接重新ACK；14:30桌图被转发，14:30:28 UTC报告保存成功。
报告保存不等于手机送达；恢复证据分别核对时间戳和健康状态。

## 验收

真实Unix socket注入不可满足的磁盘reserve，确认重复请求返回可重试响应且不写帧；
Bridge连续五次busy后使用完全相同的帧恢复ACK；旧日期锁等待期间当日append成功；
跨进程日期锁、未验证归档拒删和当日保护继续通过。
Python全量3,524项、Rust全workspace/all-targets/all-features共310项测试通过；
Ruff、Import Linter、Rust fmt/Clippy及systemd日历解析通过。发布后的真实链路状态
以最终交付时的source/ACK/projection/report检查为准，未用测试结果替代生产检查。

复杂度：生产文件新增/删除0，生产Rust逻辑净增32行（不含测试）；新增依赖、
配置键、服务/timer、数据库/表均0。
沿用原归档、重试和通知路径，移除prune的全目录排他等待；仅改变两个既有unit参数。

## 上线观察

生产代码`b389ddbe`已部署到不可变release，仅替换Core/Bridge二进制及既有retention
unit。report/delivery二进制SHA与原版本一致。Core停止会经`Requires=`连带停止
report/delivery，已在本次部署中恢复两者；未改变owner、通知配置或Bark实现。

14:42 UTC桥接恢复为`ready`，此前持续被拒的研究合同也已获ACK；14:45:08 UTC新
Desk Map的source与mirror ID/时间相同。14:44–14:45连续检查行情源年龄约0.7–1.1秒、
ACK推进，`desk_pipeline_faults=[]`。14:30那一份报告的三个目标在14:30:29–30 UTC
都有服务端成功回执；这证明投递服务结果，不证明手机已展示或用户已阅读。

只读投递检查还发现9/16、9/24共四条未人工确认的`transport_timeout/uncertain`历史
记录，与本次磁盘故障分别记录；没有改写它们或重新发送过期消息。

14:48 UTC，安装后的原retention service完整执行成功（`Result=success`、exit 0）：
9/24归档verified，113段共7,558,577,672字节压缩到196,619,002字节；prune在归档屏障
通过后回收40段、2,681,659,168字节，保护当日56段。加上紧急回收共释放
10,069,677,446字节（约9.38 GiB冗余源帧），数据盘当时剩余19.10 GiB。
14:44:55至14:48:25每十秒记录一次，22次观察的ACK全部推进、告警项全部为空；
清理期间没有再次阻塞实时写入。既有timer下次运行2026-09-26 00:15 UTC。

## 2026-10-15 复发：已归档原文保留过多

范围仍为 Phase 6 冻结例外，不涉及 P7 数据平台重写。00:09 UTC 数据盘可用空间
约9.98 GiB，低于 Core 的10 GiB写入reserve；Core持续返回`server_busy`，Bridge
保留同一pending frame重试。进程存活不代表转发成功。上游最后桌图为10/14
19:45 UTC，报告端仍为19:15，最后报告保存18:30；本次不能补发已过期桌图。
10/14 00:00–18:00 UTC日志中首次reserve拒写为17:52:04 UTC，之后间歇恢复又复发。

归档定时任务确实执行：10/14回收6,304,726,341字节，但一日保留配合8 GiB预算
仍留下大量已归档原文。10/15检查Rust原文约11 GiB、压缩档案约3.4 GiB，Python
运行数据库约30.3 GiB且freelist为0；删除数据库文件或直接VACUUM不能解决本次问题。
00:10 UTC手动启动原retention service，仍按归档、验证、清理顺序恢复空间。

本次修复：既有`keep_completed_days`允许0，生产unit删除所有通过归档屏障的完整
UTC日期原文；压缩档案、当天/未来日期、未验证或被修改的原文均保留。没有修改
10 GiB保护线或告警阈值。另修复Bridge在重连/背压时写health却不更新`updated_at`
的问题：心跳随持久化推进，源时间和ACK时间保持独立，不把重试当作转发成功。

验收覆盖零日保留的真实CLI归档/清理、未归档拒删、当天和未来日期保护，以及连续
五次busy期间心跳推进但ACK不推进、恢复后发送原pending frame。部署结果另记于下方。
Rust fmt、Clippy、全workspace/all-targets/all-features共316项测试，以及Python
桥接告警7项测试通过；本次未修改Python生产逻辑、策略阈值或Bark。

发布门另完成Python全量3,565项、Ruff和Import Linter。代码`a22ac1be`已推送master，
Core/Bridge部署到不可变`a22ac1bed33a68ff8bedba23cdb7fd079f2c58cd-disk-recovery`
release；report/delivery二进制与上一release逐字节一致，原owner和配置不变。
只重启Rust Core/Bridge及随Core依赖恢复的report/delivery；Python服务未因本次发布重启。

00:23 UTC第一轮清理完成：94段、6,280,915,275字节，归档屏障satisfied。
Bridge恢复ready并持续ACK；上游和报告端均为`desk-map:4c99de43e8350c74db494146`，
available_at均为00:15:14.029938 UTC。00:15那份当前GTH桌图已补齐转发，旧RTH桌图
未重发。每日unit已加载零日保留参数，下次执行10/16 00:15 UTC。

00:25 UTC零日保留再回收76段、5,056,177,653字节；合计170段、11,337,092,928字节
（约10.56 GiB），数据盘可用20.47 GiB。两个完整日期压缩档案仍存在，raw目录只剩
10/15当天帧。00:26再次运行安装后的原service，archive/prune完整退出0，Result=success；
report/delivery与上一release二进制SHA一致，Core/Bridge进程exe确认来自新release。

另有独立上游故障：IBKR在00:14自动处理数据农场断开，00:16重连收到10197，
collector按既有规则退让。桥接恢复不代表GTH执行行情恢复；未抢占用户手机/TWS会话。
00:29自动恢复为`healthy market-data flush`，00:30:08仍健康；这是后续观察，不能
将先前10197归因为磁盘故障，也不证明所有选腿Delta已经齐备。

00:29:26和00:29:50两份原有数据恢复更新各有三个目标成功回执；00:30定时Desk Map
于00:30:35.735661 UTC保存，三个目标在00:30:37.738521 UTC前全部delivered。
source/mirror ID与available_at一致，链路告警项为空；四个Rust unit均active，
发布后NRestarts均0。服务端回执不能证明手机显示或阅读。未重放任何过期通知。

复杂度：生产文件新增/删除0，生产逻辑净增2行；依赖、配置键、服务/timer、数据库/表
新增/删除均0。替换原完整日期保留一日的冗余规则；没有增加第二套归档、重试或通知路径。
运行数据库约30 GiB仍需持续关注增长，本次没有未经归档证明地删除其记录。
同日后续根据用户明确授权完成运行库归档清理，净回收27.80 GiB；验收与新发现的
01:00报告生成异常分别见[运行库维护记录](storage-plan.md)。
