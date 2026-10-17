"""Render the documentation diagrams with a fixed, reviewable SVG layout.

Run from any directory with Python 3.12+. Uses only the standard library.
"""
from html import escape
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
COLORS = {
    "cloud": ("#eef4ff", "#3664a1"),
    "live": ("#eaf7f2", "#23765d"),
    "control": ("#f2edfc", "#7556a7"),
    "store": ("#fff5df", "#977024"),
    "warn": ("#fff0eb", "#a45139"),
}


def width(text, size):
    return sum(1 if ord(c) > 255 else 0.56 for c in text) * size


def lines(text, size, maximum):
    result = []
    for explicit in text.split("\n"):
        current = ""
        for token in re.findall(r"[\u2e80-\uffff]|[^\u2e80-\uffff\s]+|\s+", explicit):
            if current and width(current + token, size) > maximum:
                result.append(current.rstrip())
                current = ""
            if current or not token.isspace():
                current += token
        result.append(current.rstrip())
    return result


class Canvas:
    def __init__(self, name, lang, height, title, subtitle):
        self.lang = lang
        self.height = height
        self.name = name
        self.parts = [
            '<svg xmlns="http://www.w3.org/2000/svg" width="1200" '
            f'height="{height}" viewBox="0 0 1200 {height}" role="img" '
            'aria-labelledby="title description">',
            f'<title id="title">{escape(title)}</title>',
            f'<desc id="description">{escape(subtitle)}</desc>',
            '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" '
            'markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
            '<path d="M0 0 L10 5 L0 10 Z" fill="#697c90"/></marker></defs>',
            '<style>text{font-family:Arial,"PingFang SC","Microsoft YaHei",sans-serif;'
            'fill:#20384e} .muted{fill:#597084} .edge{fill:none;stroke:#697c90;'
            'stroke-width:2;stroke-linejoin:round;stroke-linecap:round}</style>',
            f'<rect width="1200" height="{height}" fill="#fff"/>',
        ]
        self.text(40, 48, title, 29, weight=700)
        self.text(40, 80, subtitle, 17, css="muted")

    def t(self, en, zh):
        return zh if self.lang == "zh-CN" else en

    def text(self, x, y, text, size=18, weight=400, css=""):
        self.parts.append(
            f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" '
            f'class="{css}">{escape(text)}</text>'
        )

    def stage(self, y, en, zh):
        self.text(40, y, self.t(en, zh), 17, 700)

    def card(self, x, y, w, h, title, body, color="live"):
        bg, stroke = COLORS[color]
        self.parts.append(f'<g data-card="{escape(title, quote=True)}">')
        self.parts.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" '
            f'fill="{bg}" stroke="{stroke}" stroke-width="1.4"/>'
        )
        cy = y + 30
        for line in lines(title, 20, w - 38):
            self.text(x + 19, cy, line, 20, 700)
            cy += 26
        cy += 3
        for line in lines(body, 17, w - 38):
            self.text(x + 19, cy, line, 17)
            cy += 23
        if cy - 23 > y + h - 12:
            raise ValueError(f"Text exceeds card: {title}")
        self.parts.append("</g>")

    def arrow(self, points, dashed=False):
        d = "M" + " L".join(f"{x},{y}" for x, y in points)
        dash = ' stroke-dasharray="7 6"' if dashed else ""
        self.parts.append(f'<path class="edge" d="{d}" marker-end="url(#arrow)"{dash}/>')

    def save(self):
        self.parts.append("</svg>")
        svg = "\n".join(self.parts) + "\n"
        (ROOT / "diagrams" / f"{self.name}.{self.lang}.svg").write_text(
            svg, encoding="utf-8"
        )
        guide = ROOT / "service-flow.html"
        if guide.exists():
            marker = f"<!-- diagram:{self.name}:{self.lang} -->"
            text = re.sub(
                re.escape(marker) + r".*?<!-- end-diagram -->",
                lambda _: marker + "\n" + svg + "<!-- end-diagram -->",
                guide.read_text(encoding="utf-8"),
                flags=re.DOTALL,
            )
            guide.write_text(text, encoding="utf-8")


def overview(lang):
    c = Canvas("system-overview", lang, 840,
               "SPX Spark · environments and handoffs" if lang == "en" else "SPX Spark · 环境分工与交接",
               "Roles above; the four cross-environment routes below."
               if lang == "en" else "上方说明各环境职责，下方单独列出四条跨环境交接路径。")
    for x, title, color in [(40, c.t("01  CLOUD", "01  云端"), "cloud"),
                            (440, c.t("02  ORACLE", "02  ORACLE"), "live"),
                            (840, c.t("03  HOME", "03  家庭"), "control")]:
        c.text(x, 130, title, 18, 700)
        bg, _ = COLORS[color]
        c.parts.append(f'<rect x="{x}" y="150" width="320" height="380" rx="16" fill="{bg}"/>')
    cards = [
        (40, 170, "Developers / agents", "开发者 / Agent", "Code changes and reviewed PRs", "修改代码与审阅 PR", "cloud"),
        (40, 290, "GitHub Actions", "GitHub Actions", "Test and build AMD64 + ARM64", "测试并构建 AMD64 + ARM64", "cloud"),
        (40, 410, "Releases + GHCR", "Releases + GHCR", "One commit, native bundles and images", "同一提交、原生包与容器镜像", "cloud"),
        (440, 170, "Production services", "生产服务", "Existing Python + Rust under systemd", "systemd 管理既有 Python + Rust", "live"),
        (440, 290, "Live state", "在线状态", "Quotes, operational DBs and report ledgers", "行情、运行数据库与报告账本", "live"),
        (440, 410, "Runtime ownership", "运行职责", "Collect, decide, report and notify", "采集、决策、报告与通知", "live"),
        (840, 170, "Raspberry Pi", "树莓派", "Prefect scheduling + archive transfer", "Prefect 调度 + 归档传输", "control"),
        (840, 290, "Linux laptop", "Linux 笔记本", "Pinned image + dedicated research disk", "固定镜像 + 专用研究盘", "store"),
        (840, 410, "NAS", "NAS", "Verified snapshots + result backups", "核验快照 + 结果备份", "store"),
    ]
    for x, y, en, zh, body_en, body_zh, color in cards:
        c.card(x + 10, y, 300, 105, c.t(en, zh), c.t(body_en, body_zh), color)
    c.stage(579, "HANDOFFS · data and artifacts", "交接路径 · 数据与产物")
    routes = [
        ("1  Release bundle  →  Oracle", "1  发布原生包  →  Oracle", "ARM64 / systemd", "ARM64 / systemd"),
        ("2  Matching image  →  Linux laptop", "2  同提交镜像  →  Linux 笔记本", "AMD64 / isolated replay", "AMD64 / 隔离回放"),
        ("3  Oracle quotes  →  Pi  →  NAS", "3  Oracle 行情  →  树莓派  →  NAS", "Read-only archive + NAS verification", "只读归档 + NAS 本机核验"),
        ("4  NAS snapshot  →  laptop  →  NAS", "4  NAS 快照  →  笔记本  →  NAS", "Research inputs + result receipts", "研究输入 + 结果回执"),
    ]
    for i, (en, zh, note_en, note_zh) in enumerate(routes):
        y = 616 + i * 43
        c.text(48, y, c.t(en, zh), 19, 600)
        c.text(655, y, c.t(note_en, note_zh), 17, css="muted")
    c.text(40, 807, c.t("Mac: review, operations and on-demand research. No scheduled compute role.",
                       "Mac：审阅、运维和按需研究；不承担定时计算 Worker 职责。"), 17, css="muted")
    c.save()


def runtime(lang):
    c = Canvas("runtime", lang, 1040,
               "SPX Spark · service and data flow" if lang == "en" else "SPX Spark · 服务与数据流转",
               "Concurrent owners; arrows show specific handoffs, not one synchronous loop."
               if lang == "en" else "各 owner 并发运行；箭头表示具体交接，不表示一个同步大循环。")
    c.stage(132, "01  INGESTION AND CONCURRENT CORE", "01  采集与并发 Core")
    c.stage(427, "02  DECISION COMMIT AND MANUAL NOTIFICATION", "02  决策提交与人工候选通知")
    c.stage(727, "03  RESEARCH AND THE SEPARATE REPORT LANE", "03  研究与独立报告路径")
    for pts in [([(340, 230), (450, 230)]), ([(750, 230), (860, 230)]),
                ([(1010, 310), (1010, 460)]), ([(860, 540), (750, 540)]),
                ([(450, 540), (340, 540)]), ([(600, 620), (600, 760)]),
                ([(750, 840), (860, 840)])]:
        c.arrow(pts)
    c.arrow([(500, 310), (500, 360), (20, 360), (20, 840), (40, 840)], dashed=True)
    c.arrow([(600, 150), (600, 110), (1180, 110), (1180, 840), (1160, 840)], dashed=True)
    c.text(820, 132, c.t("Normalized mirror → Unix IPC", "标准行情镜像 → Unix IPC"), 16, css="muted")
    cards = [
        (40, 150, "Broker collectors", "券商采集器", "Schwab RTH / IBKR GTH\nIndependent sessions and clocks", "Schwab RTH / IBKR GTH\n独立会话与字段时钟", "live"),
        (450, 150, "Normalized data", "标准化数据", "Append raw quotes, then update latest state\nSeparate writes, not one transaction", "先追加 raw，再更新 latest\n两次独立写入，不是同一事务", "store"),
        (860, 150, "Python Core", "Python Core", "Concurrent samplers, market-feature and shock runners", "采样、市场特征与 shock runner 并发运行", "live"),
        (860, 460, "Final strategy decision", "最终策略决策", "build_strategy_decision\nFacts → candidates → gates\nManual-only authority", "build_strategy_decision\n事实 → 候选 → 门控\n仅人工候选授权", "live"),
        (450, 460, "Commit and publish", "提交 → 导出 → 入队", "SQLite → decision JSON\nThen enqueue event_id\nFailure blocks READY", "Python SQLite → 决策 JSON\n随后入队通知事件\n持久化失败阻止 READY", "live"),
        (40, 460, "Python Worker / Huey", "Python Worker / Huey", "event_id → notification tasks\nQueue DB ≠ operational DB\nIndependent health alerts", "event_id → 通知任务\n队列库 ≠ 运行库\n独立健康告警", "control"),
        (40, 760, "Research entry points", "研究入口", "Raw quotes + source clocks\nConditional live research\nor isolated causal replay\nNo trade authority", "原始报价 + 可用时间\n按条件实时研究或离线回放\n不具有交易授权", "store"),
        (450, 760, "Desk Map export", "Desk Map 导出", "Reuse committed decision\nRecheck current-time validity\ndesk_map_projection.v1", "复用已提交决策\n重验当前时点有效性\ndesk_map_projection.v1", "live"),
        (860, 760, "Rust report + delivery", "Rust 报告与投递", "Bridge → core → report\nLedger / outbox / receipts\nNo broker sessions\nNo strategy leg selection", "Bridge → core → report\n账本 → 投递 → 回执\n不连接券商、不选策略腿", "cloud"),
    ]
    for x, y, en, zh, ben, bzh, color in cards:
        c.card(x, y, 300, 160, c.t(en, zh), c.t(ben, bzh), color)
    c.text(40, 973, c.t("Source map: the earlier HTML atlas + current source contracts. Dashed routes: quote files and IPC.",
                       "参考：早期 HTML 图册 + 现行源码合同。虚线表示报价文件与 IPC 交接。"), 17, css="muted")
    c.text(40, 1006, c.t("Reports and optional research do not create another strategy authority. No automated orders.",
                        "报告与可选研究不产生第二个策略授权出口；没有自动下单。"), 17, css="muted")
    c.save()


def delivery(lang):
    c = Canvas("delivery", lang, 950,
               "SPX Spark · build, release and deployment" if lang == "en" else "SPX Spark · 构建、发布与部署",
               "One commit; native production packages and isolated research images."
               if lang == "en" else "同一提交标识：生产原生包与隔离研究镜像。")
    c.stage(133, "01  CLOUD DELIVERY", "01  云端交付")
    c.stage(402, "02  ORACLE · EXISTING SYSTEMD SERVICES", "02  ORACLE · 既有 SYSTEMD 服务")
    c.stage(703, "03  HOME · RESEARCH IMAGE", "03  家庭 · 研究镜像")
    c.arrow([(340, 235), (450, 235)])
    c.arrow([(750, 235), (860, 235)])
    c.arrow([(1010, 315), (1010, 350), (20, 350), (20, 505), (40, 505)])
    c.arrow([(20, 505), (20, 805), (40, 805)])
    for x in (300, 590, 880):
        c.arrow([(x, 505), (x + 30, 505)])
    c.arrow([(340, 805), (450, 805)])
    c.arrow([(750, 805), (860, 805)])
    for x, title, zh, body, bzh in [
        (40, "Pull request + checks", "PR 与必需检查", "Python + Rust tests\nNative AMD64 / ARM64 checks", "Python + Rust 测试\n原生 AMD64 / ARM64 检查"),
        (450, "Merge to master", "合并至 master", "Eligible mainline build\nValidate packages and images", "符合路径条件的主线构建\n验证运行包与镜像"),
        (860, "Publish artifacts", "发布产物", "GitHub Releases + GHCR\nCommit SHA + checksums/digests", "GitHub Releases + GHCR\n提交 SHA + 校验值 / digest"),
    ]:
        c.card(x, 155, 300, 160, c.t(title, zh), c.t(body, bzh), "cloud")
    for x, en, zh, ben, bzh, color in [
        (40, "Verify package", "校验运行包", "Commit + hash\nMigrations + services", "提交 + 哈希\n迁移 + 服务", "live"),
        (330, "Switch paths", "切换程序路径", "Stop existing writers\nActivate native bundle", "停止既有写入方\n启用原生运行包", "live"),
        (620, "Runtime checks", "运行验收", "Stable processes\nAdvancing bridge health", "进程稳定\nBridge 健康记录推进", "live"),
        (910, "Keep or roll back", "保留或回滚", "Pass: retain release\nFail: previous binaries", "通过：保留新版本\n失败：恢复旧程序", "warn"),
    ]:
        c.card(x, 425, 260, 160, c.t(en, zh), c.t(ben, bzh), color)
    c.text(40, 633, c.t("Host configuration, secrets and databases keep their owners. Binary rollback does not undo database writes.",
                       "主机配置、凭据与数据库保持原归属；程序回滚不会撤销数据库写入。"), 17, css="muted")
    for x, en, zh, ben, bzh in [
        (40, "Load matching image", "加载同版本镜像", "Pinned AMD64 identity\nNo production credentials", "固定 AMD64 镜像身份\n不含生产凭据"),
        (450, "Prepare research disk", "准备研究盘", "Verified snapshot + policy\nRead-only inputs", "已核验快照 + 策略配置\n只读输入"),
        (860, "Isolated replay", "隔离回放", "No network\nBounded CPU and memory", "断网\nCPU 与内存限额"),
    ]:
        c.card(x, 725, 300, 160, c.t(en, zh), c.t(ben, bzh), "store")
    c.text(40, 925, c.t("Documentation-only master changes do not trigger Oracle deployment.",
                       "主线纯文档变更不会触发 Oracle 部署。"), 17, css="muted")
    c.save()


def research(lang):
    c = Canvas("research", lang, 995,
               "SPX Spark · archive and research lifecycle" if lang == "en" else "SPX Spark · 归档与研究生命周期",
               "Daily archive, pinned weekly replay, then verified result retention."
               if lang == "en" else "每日增量归档，固定快照周回放，结果核验后保存。")
    for y, en, zh in [(135, "01  DAILY · WEEKDAYS 17:30 NEW YORK", "01  日归档 · 工作日美东 17:30"),
                       (425, "02  WEEKLY · SATURDAY 10:00 SHANGHAI", "02  周研究 · 周六北京时间 10:00"),
                       (715, "03  RESULTS · AFTER SUCCESS OR FAILURE", "03  结果保存 · 完成或失败后")]:
        c.stage(y, en, zh)
    for pts in [[(340, 235), (450, 235)], [(750, 235), (860, 235)],
                [(1010, 315), (1010, 445)], [(860, 525), (750, 525)],
                [(450, 525), (340, 525)],
                [(190, 605), (190, 650), (20, 650), (20, 815), (40, 815)],
                [(340, 815), (450, 815)], [(750, 815), (860, 815)]]:
        c.arrow(pts)
    for x, y, en, zh, ben, bzh, color in [
        (40, 155, "Oracle partitions", "Oracle 报价分区", "Closed + verified Parquet\nRead-only transfer endpoint", "已关闭 + 已核验 Parquet\n只读传输接口", "live"),
        (450, 155, "Pi archive worker", "树莓派归档 Worker", "Stream to verified NAS mount\nNo SD-card staging", "流式写入已核验 NAS 挂载\n不暂存到存储卡", "control"),
        (860, 155, "NAS verification", "NAS 本机核验", "Reread objects; verify hashes\nPublish immutable snapshot", "重读对象并核验哈希\n发布不可变快照", "store"),
        (860, 445, "Select fixed snapshot", "选择固定快照", "Immutable input manifest\nSafe archived policy settings", "不可变输入清单\n安全归档策略配置", "store"),
        (450, 445, "Pin laptop inputs", "固定笔记本输入", "Research disk + verified data\nMatching policy and code image", "研究盘 + 已核验数据\n匹配策略配置与代码镜像", "store"),
        (40, 445, "Current-strategy replay", "当前策略回放", "Causal quotes; no network\nRTH / GTH kept separate", "因果报价；断网执行\nRTH / GTH 分开", "cloud"),
        (40, 735, "Local run evidence", "本地运行证据", "Reports + logs + manifests\nPreserve gaps and failed outcomes", "报告 + 日志 + 清单\n保留缺口与失败结果", "store"),
        (450, 735, "NAS result backup", "NAS 结果备份", "Independent file verification\nRetry pending backups", "独立文件核验\n待备份结果自动补传", "store"),
        (860, 735, "Receipt + local copy", "回执 + 本地副本", "Record verified retention\nNo new Oracle deletion rule", "记录核验完成\n不新增 Oracle 删除规则", "store"),
    ]:
        c.card(x, y, 300, 160, c.t(en, zh), c.t(ben, bzh), color)
    c.text(40, 951, c.t("The Pi schedules; the laptop computes. Later backup success does not rewrite a failed compute run.",
                       "树莓派负责调度，笔记本负责计算；后续备份成功不改写计算任务的历史失败状态。"), 17, css="muted")
    c.save()


def main():
    for lang in ("en", "zh-CN"):
        for render in (overview, runtime, delivery, research):
            render(lang)
    print("Rendered 8 SVG diagrams")


if __name__ == "__main__":
    main()
