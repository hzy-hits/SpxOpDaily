use spx_domain::{DeskMessageV1, DeskMessageV2, OperatorNotificationRole, OperatorNotificationV1};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedMessage {
    pub title: String,
    pub body: String,
}

pub fn render_desk_message(message: &DeskMessageV1) -> RenderedMessage {
    let title = normalize(message.title.as_str());
    let body = format!(
        "Desk View\n{}\n\nExecution\n{}\n\nRisk\n{}\n\nTargets\n{}\n\nData Quality\n{}",
        normalize(message.desk_view.as_str()),
        normalize(message.execution.as_str()),
        normalize(message.risk.as_str()),
        normalize(message.targets.as_str()),
        normalize(message.data_quality.as_str()),
    );
    RenderedMessage { title, body }
}

pub fn render_desk_message_v2(message: &DeskMessageV2) -> RenderedMessage {
    let body = format!(
        "Base Case\n{}\n\nWhy\nLocation · {}\nStructure · {}\n\nTrigger\n{}\n\nInvalidation\n{}\n\nTargets\n{}\n\nExecution\n{}\n\nPrimary Data Impact\n{}",
        message.desk_view.as_str(),
        message.location.as_str(),
        message.structure.as_str(),
        message.primary_path.as_str(),
        message.alternative_path.as_str(),
        message.targets.as_str(),
        message.execution.as_str(),
        message.data_quality.as_str(),
    );
    RenderedMessage {
        title: message.title.as_str().to_owned(),
        body,
    }
}

/// Returns the frozen message, with a deterministic compact presentation for `TradeReady`.
///
/// The ledger retains the full ingress payload. Only the human delivery projection is compacted,
/// so research prose cannot crowd out the exact manual action, risk and target fields.
pub fn render_operator_notification(notification: &OperatorNotificationV1) -> RenderedMessage {
    let (title, body) = match notification.role {
        OperatorNotificationRole::Setup => (
            "SPX WATCH · 等待确认".to_owned(),
            compact_setup_body(&notification.body),
        ),
        OperatorNotificationRole::TradeReady => (
            trade_ready_title(notification.title.as_str(), &notification.body),
            compact_trade_ready_body(&notification.body),
        ),
        OperatorNotificationRole::Exit => (
            notification.title.as_str().to_owned(),
            notification.body.clone(),
        ),
    };
    RenderedMessage { title, body }
}

fn compact_setup_body(body: &str) -> String {
    let Some(sections) = operator_sections(body) else {
        return body.to_owned();
    };
    let desk_view = select_lines(
        sections[0],
        &[
            "Opportunity",
            "机会",
            "State",
            "Structure",
            "Location",
            "区域",
            "方向来源",
            "LONG条件",
            "SHORT条件",
        ],
        5,
        true,
    );
    let execution = select_lines(
        sections[1],
        &["Next", "动作", "触发", "执行", "追价限制"],
        4,
        true,
    );
    let risk = select_lines(sections[2], &[], 2, true);
    let targets = select_lines(sections[3], &[], 1, true);
    let data_quality = select_lines(sections[4], &[], 1, true);
    format!(
        "Desk View\n🟠 WATCH · 尚未到人工入场\n{desk_view}\n\nExecution\n{execution}\n\nRisk\n{risk}\n\nTargets\n{targets}\n\nData Quality\n{data_quality}",
    )
}

fn compact_trade_ready_body(body: &str) -> String {
    try_compact_trade_ready_body(body).unwrap_or_else(|| body.to_owned())
}

fn trade_ready_title(source_title: &str, body: &str) -> String {
    if source_title.contains("GTH") || body.contains("GTH") {
        "SPX GTH TRADE READY · 人工限价".to_owned()
    } else {
        "SPX TRADE READY · 人工限价".to_owned()
    }
}

fn try_compact_trade_ready_body(body: &str) -> Option<String> {
    let sections = operator_sections(body)?;
    if !trade_ready_sections_are_complete(&sections) {
        return None;
    }
    let desk_view = select_lines(sections[0], &["🟢", "动作", "机会", "触发"], 3, true);
    let execution = select_lines(
        sections[1],
        &[
            "类型", "买入", "卖出", "NBBO", "限价", "有效", "提交", "权限",
        ],
        8,
        true,
    );
    let risk = select_lines(sections[2], &[], 4, true);
    let targets = select_lines(sections[3], &[], 3, true);
    let data_quality = select_lines(sections[4], &[], 1, true);
    Some(format!(
        "Desk View\n{desk_view}\n\nExecution\n{execution}\n\nRisk\n{risk}\n\nTargets\n{targets}\n\nData Quality\n{data_quality}",
    ))
}

fn trade_ready_sections_are_complete(sections: &[&str; 5]) -> bool {
    let spread = [sections[0], sections[1]].into_iter().any(|section| {
        let normalized = section.to_ascii_lowercase();
        normalized.contains("spread") || section.contains("价差") || section.contains("两腿")
    });
    section_has_usable_field(sections[0], &["🟢 MANUAL READY", "🟢 ACT NOW"])
        && section_has_usable_field(sections[1], &["买入"])
        && (!spread || section_has_usable_field(sections[1], &["卖出"]))
        && section_has_usable_field(sections[1], &["NBBO"])
        && section_has_usable_field(sections[1], &["限价"])
        && section_has_usable_field(sections[1], &["有效"])
        && section_has_usable_field(sections[2], &["止损", "失效"])
        && section_has_usable_field(sections[2], &["风险"])
        && section_has_usable_field(sections[3], &["目标"])
        && section_has_usable_field(sections[3], &["赔率", "最大收益/最大亏损"])
        && sections[4]
            .lines()
            .map(str::trim)
            .any(|line| !line.is_empty() && !is_research_line(line))
}

fn section_has_usable_field(section: &str, prefixes: &[&str]) -> bool {
    section.lines().map(str::trim).any(|line| {
        let normalized = line.to_ascii_lowercase();
        !line.contains("不可用")
            && !normalized.contains("unknown")
            && !normalized.contains("null")
            && !normalized.contains("n/a")
            && !line.ends_with('-')
            && prefixes
                .iter()
                .any(|prefix| line.starts_with(prefix) || line.contains(prefix))
    })
}

fn operator_sections(body: &str) -> Option<[&str; 5]> {
    let remainder = body.strip_prefix("## Desk View\n")?;
    let (desk_view, remainder) = remainder.split_once("\n\n## Execution\n")?;
    let (execution, remainder) = remainder.split_once("\n\n## Risk\n")?;
    let (risk, remainder) = remainder.split_once("\n\n## Targets\n")?;
    let (targets, data_quality) = remainder.split_once("\n\n## Data Quality\n")?;
    let sections = [desk_view, execution, risk, targets, data_quality];
    if sections
        .iter()
        .any(|section| section.trim().is_empty() || section.contains("\n## "))
    {
        None
    } else {
        Some(sections)
    }
}

fn select_lines(section: &str, prefixes: &[&str], limit: usize, filter_research: bool) -> String {
    let candidates = section
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter(|line| !filter_research || !is_research_line(line));
    let preferred = candidates
        .clone()
        .filter(|line| {
            prefixes.is_empty() || prefixes.iter().any(|prefix| line.starts_with(prefix))
        })
        .take(limit)
        .collect::<Vec<_>>();
    let selected = if preferred.is_empty() {
        candidates
            .take(if prefixes.is_empty() { limit } else { 1 })
            .collect::<Vec<_>>()
    } else {
        preferred
    };
    if selected.is_empty() {
        "不可用".to_owned()
    } else {
        selected.join("\n")
    }
}

fn is_research_line(line: &str) -> bool {
    ["HMM", "P/Q", "Gamma", "gamma", "Spring", "历史", "研究"]
        .into_iter()
        .any(|marker| line.contains(marker))
}

fn normalize(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use chrono::{TimeDelta, Utc};
    use spx_domain::{
        DeliveryChannel, NotificationTargetV1, OPERATOR_NOTIFICATION_SCHEMA_VERSION,
        OperatorNotificationRole, OperatorNotificationV1, Token,
    };

    use super::*;

    fn token(value: &str) -> Token {
        Token::new(value, "render test").unwrap()
    }

    fn operator_notification(
        role: OperatorNotificationRole,
        title: &str,
        body: String,
    ) -> OperatorNotificationV1 {
        let now = Utc::now();
        OperatorNotificationV1 {
            schema_version: OPERATOR_NOTIFICATION_SCHEMA_VERSION.to_owned(),
            event_id: token("event-render"),
            semantic_id: token("semantic-render"),
            opportunity_id: token("opportunity-render"),
            generation: 2,
            role,
            occurred_at: now,
            expires_at: now + TimeDelta::minutes(10),
            title: token(title),
            body,
            targets: vec![NotificationTargetV1 {
                key: token("primary"),
                channel: DeliveryChannel::Bark,
            }],
            automatic_ordering: false,
        }
    }

    #[test]
    fn renders_fixed_institutional_sections() {
        let rendered = render_desk_message(&DeskMessageV1 {
            title: token("SPX 0DTE | MANUAL CANDIDATE"),
            desk_view: token("Range regime"),
            execution: token("Wait for exact-leg confirmation"),
            risk: token("No automatic order"),
            targets: token("Call wall 6000"),
            data_quality: token("Schwab live; exact NBBO fresh"),
        });
        assert_eq!(rendered.title, "SPX 0DTE | MANUAL CANDIDATE");
        assert_eq!(
            rendered.body,
            "Desk View\nRange regime\n\nExecution\nWait for exact-leg confirmation\n\nRisk\nNo automatic order\n\nTargets\nCall wall 6000\n\nData Quality\nSchwab live; exact NBBO fresh"
        );
    }

    #[test]
    fn renders_operator_facing_v2_sections_without_normalizing_or_truncating() {
        let long_primary = format!("first line\n{}  tail", "x".repeat(3_500));
        let rendered = render_desk_message_v2(&DeskMessageV2 {
            title: token("SPX RTH Desk Map · 10:00 ET"),
            desk_view: token("Bullish  above VWAP"),
            location: token("SPX 7568 | OR15 7565"),
            structure: token("Put 7525 | Flip 7550 | Call 7580"),
            primary_path: token(&long_primary),
            alternative_path: token("Lose VWAP\nand rotate to flip"),
            targets: token("7580 / 7595"),
            execution: token("Wait for retest; no chase"),
            data_quality: token("DEGRADED: clipped mass 28.4%"),
        });

        assert_eq!(rendered.title, "SPX RTH Desk Map · 10:00 ET");
        assert!(rendered.body.contains("Base Case\nBullish  above VWAP"));
        assert!(
            rendered
                .body
                .contains("Why\nLocation · SPX 7568 | OR15 7565\nStructure · Put 7525")
        );
        assert!(rendered.body.contains(&format!("Trigger\n{long_primary}")));
        assert!(
            rendered
                .body
                .contains("Invalidation\nLose VWAP\nand rotate to flip")
        );
        assert!(
            rendered
                .body
                .ends_with("Primary Data Impact\nDEGRADED: clipped mass 28.4%")
        );
    }

    #[test]
    fn exit_operator_notification_render_is_byte_for_byte_frozen() {
        let now = Utc::now();
        let body = format!(
            "## Desk View\n前导  {}\n末尾  \n\n## Execution\nmanual only\n\n## Risk\ndefined risk\n\n## Targets\nnext level\n\n## Data Quality\nlive",
            "x".repeat(8_000)
        );
        let notification = OperatorNotificationV1 {
            schema_version: OPERATOR_NOTIFICATION_SCHEMA_VERSION.to_owned(),
            event_id: token("event-1"),
            semantic_id: token("semantic-1"),
            opportunity_id: token("opportunity-1"),
            generation: 0,
            role: OperatorNotificationRole::Exit,
            occurred_at: now,
            expires_at: now + TimeDelta::minutes(10),
            title: token(" SPX Setup  "),
            body: body.clone(),
            targets: vec![NotificationTargetV1 {
                key: token("primary"),
                channel: DeliveryChannel::Bark,
            }],
            automatic_ordering: false,
        };
        assert_eq!(
            render_operator_notification(&notification),
            RenderedMessage {
                title: " SPX Setup  ".to_owned(),
                body,
            }
        );
    }

    #[test]
    fn setup_render_is_watch_only_and_excludes_contract_and_research() {
        let now = Utc::now();
        let notification = OperatorNotificationV1 {
            schema_version: OPERATOR_NOTIFICATION_SCHEMA_VERSION.to_owned(),
            event_id: token("event-watch"),
            semantic_id: token("semantic-watch"),
            opportunity_id: token("opportunity-watch"),
            generation: 1,
            role: OperatorNotificationRole::Setup,
            occurred_at: now,
            expires_at: now + TimeDelta::minutes(10),
            title: token("SPX CALL candidate"),
            body: "## Desk View\n机会  A17\n区域  Call Wall 7760\n方向来源  等待价格确认\nLONG条件  7760上方接受\nSpring Gamma 背景\n\n## Execution\n动作  等待 READY\n合约  SPXW 7760C\n触发  状态机确认后重报\n\n## Risk\n失效  跌回7757\n权限  自动下单关闭\n\n## Targets\n进入 READY 后计算\n\n## Data Quality\nSchwab fresh\nHMM 未校准"
                .to_owned(),
            targets: vec![NotificationTargetV1 {
                key: token("primary"),
                channel: DeliveryChannel::Bark,
            }],
            automatic_ordering: false,
        };

        let rendered = render_operator_notification(&notification);

        assert_eq!(rendered.title, "SPX WATCH · 等待确认");
        assert!(rendered.body.contains("🟠 WATCH · 尚未到人工入场"));
        assert!(rendered.body.contains("LONG条件  7760上方接受"));
        assert!(rendered.body.contains("动作  等待 READY"));
        assert!(!rendered.body.contains("SPXW 7760C"));
        assert!(!rendered.body.contains("Spring Gamma"));
        assert!(!rendered.body.contains("HMM 未校准"));
    }

    #[test]
    fn trade_ready_render_is_short_deterministic_and_excludes_research_prose() {
        let now = Utc::now();
        let notification = OperatorNotificationV1 {
            schema_version: OPERATOR_NOTIFICATION_SCHEMA_VERSION.to_owned(),
            event_id: token("event-ready"),
            semantic_id: token("semantic-ready"),
            opportunity_id: token("opportunity-ready"),
            generation: 2,
            role: OperatorNotificationRole::TradeReady,
            occurred_at: now,
            expires_at: now + TimeDelta::minutes(10),
            title: token("SPX TRADE READY"),
            body: "## Desk View\n🟢 MANUAL READY · CALL\n机会  A17 · generation 2\n触发  SPX 接受 7760\n解释  省略\nSpring Gamma 研究背景\n\n## Execution\n🟢 MANUAL READY · CALL\n类型  单腿 · 仅人工提交\n买入  SPXW 08-05 7760C\nProvider  Schwab\nNBBO  21.40 / 21.60\n限价  ≤ 21.60\n有效  5 分钟\n权限  仅人工提交\n\n## Risk\n止损  SPX 跌回 7757\n风险  单张最大权利金 $2160\n\n## Targets\n目标  SPX 7780\n赔率  1.35:1\n\n## Data Quality\nSchwab fresh\n历史  23 笔\nHMM 未校准"
                .to_owned(),
            targets: vec![NotificationTargetV1 {
                key: token("primary"),
                channel: DeliveryChannel::Bark,
            }],
            automatic_ordering: false,
        };

        let rendered = render_operator_notification(&notification);

        assert_eq!(rendered.title, "SPX TRADE READY · 人工限价");
        assert!(rendered.body.contains("机会  A17 · generation 2"));
        assert!(rendered.body.contains("买入  SPXW 08-05 7760C"));
        assert!(rendered.body.contains("限价  ≤ 21.60"));
        assert!(rendered.body.contains("止损  SPX 跌回 7757"));
        assert!(rendered.body.contains("目标  SPX 7780"));
        assert!(!rendered.body.contains("解释  省略"));
        assert!(!rendered.body.contains("Spring Gamma"));
        assert!(!rendered.body.contains("历史  23 笔"));
        assert!(!rendered.body.contains("HMM 未校准"));
        assert!(!rendered.body.contains("研究背景不进入执行卡"));
    }

    #[test]
    fn malformed_or_incomplete_trade_ready_falls_back_to_the_full_frozen_body() {
        let complete = "## Desk View\n🟢 MANUAL READY · CALL\n机会  A17\n触发  SPX 接受 7760\n\n## Execution\n买入  SPXW 08-05 7760C\nNBBO  21.40 / 21.60\n限价  ≤ 21.60\n有效  剩余 8 分钟\n\n## Risk\n止损  SPX 跌回 7757\n风险  单张最大权利金 $2160\n\n## Targets\n目标  SPX 7780\n赔率  1.35:1\n\n## Data Quality\nSchwab fresh";
        let cases = [
            "malformed body without operator sections".to_owned(),
            complete.replace("## Risk", "## Broken Risk"),
            complete.replace("买入  SPXW 08-05 7760C\n", ""),
            complete.replace("NBBO  21.40 / 21.60", "NBBO  不可用"),
            complete.replace("限价  ≤ 21.60\n", ""),
            complete.replace("有效  剩余 8 分钟\n", ""),
            complete.replace("止损  SPX 跌回 7757\n", ""),
            complete.replace("风险  单张最大权利金 $2160\n", ""),
            complete.replace("目标  SPX 7780\n", ""),
            complete.replace("赔率  1.35:1\n", ""),
            complete.replace("赔率  1.35:1", "赔率  -"),
        ];

        for body in cases {
            let notification = operator_notification(
                OperatorNotificationRole::TradeReady,
                "SPX TRADE READY",
                body.clone(),
            );

            let rendered = render_operator_notification(&notification);

            assert_eq!(rendered.body, body);
            assert!(!rendered.body.contains("Data Quality\n不可用"));
        }
    }

    #[test]
    fn gth_two_leg_trade_ready_keeps_both_legs_and_all_critical_fields() {
        let source = "## Desk View\n🟢 MANUAL READY · CALL SPREAD\n路径  gth_dip_reclaim\n触发  SPX 收复 7760\nSpring Gamma 研究背景\n\n## Execution\n🟢 MANUAL READY · CALL SPREAD\n类型  10点宽 Debit Spread · 仅人工提交\n买入  SPXW 08-05 7760C\n卖出  SPXW 08-05 7770C\nNBBO  4.80 / 5.10；两腿合成\n限价  净借记 ≤ 5.10\n有效  剩余 8 分钟\n提交  仅允许人工限价\n\n## Risk\n止损  SPX 跌回 7757\n退出  10:45 北京时间\n风险  每组最大损失 $510\n\n## Targets\n目标  SPX 7780\n赔率  最大收益/最大亏损 0.96\n\n## Data Quality\nIBKR SPXW 两腿 NBBO fresh\nHMM 未校准";
        let notification = operator_notification(
            OperatorNotificationRole::TradeReady,
            "SPX GTH TRADE READY",
            source.to_owned(),
        );

        let rendered = render_operator_notification(&notification);

        assert_eq!(rendered.title, "SPX GTH TRADE READY · 人工限价");
        assert!(rendered.body.len() < source.len());
        for critical in [
            "买入  SPXW 08-05 7760C",
            "卖出  SPXW 08-05 7770C",
            "NBBO  4.80 / 5.10",
            "限价  净借记 ≤ 5.10",
            "有效  剩余 8 分钟",
            "止损  SPX 跌回 7757",
            "风险  每组最大损失 $510",
            "目标  SPX 7780",
            "赔率  最大收益/最大亏损 0.96",
        ] {
            assert!(rendered.body.contains(critical), "missing {critical}");
        }
        assert!(!rendered.body.contains("Spring Gamma"));
        assert!(!rendered.body.contains("HMM 未校准"));
        assert!(!rendered.body.contains("不可用"));

        let missing_short_leg = source.replace("卖出  SPXW 08-05 7770C\n", "");
        let incomplete = operator_notification(
            OperatorNotificationRole::TradeReady,
            "SPX GTH TRADE READY",
            missing_short_leg.clone(),
        );
        assert_eq!(
            render_operator_notification(&incomplete).body,
            missing_short_leg
        );
    }
}
