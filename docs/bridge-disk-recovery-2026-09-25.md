# 2026-09-25 桥接磁盘故障恢复

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

复杂度：生产文件新增/删除0；新增依赖、配置键、服务/timer、数据库/表均0。
沿用原归档、重试和通知路径，移除prune的全目录排他等待；仅改变两个既有unit参数。
