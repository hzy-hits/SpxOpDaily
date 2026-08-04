use spx_domain::{DeskMessageV1, DeskMessageV2};

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
        "Desk View\n{}\n\nLocation\n{}\n\nStructure\n{}\n\nPrimary Path\n{}\n\nAlternative Path\n{}\n\nTargets\n{}\n\nExecution\n{}\n\nData Quality\n{}",
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

fn normalize(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use spx_domain::Token;

    use super::*;

    fn token(value: &str) -> Token {
        Token::new(value, "render test").unwrap()
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
    fn renders_complete_v2_sections_without_normalizing_or_truncating() {
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
        assert!(rendered.body.contains("Desk View\nBullish  above VWAP"));
        assert!(
            rendered
                .body
                .contains(&format!("Primary Path\n{long_primary}"))
        );
        assert!(
            rendered
                .body
                .contains("Alternative Path\nLose VWAP\nand rotate to flip")
        );
        assert!(
            rendered
                .body
                .ends_with("Data Quality\nDEGRADED: clipped mass 28.4%")
        );
    }
}
