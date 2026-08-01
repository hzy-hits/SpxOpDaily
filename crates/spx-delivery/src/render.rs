use spx_domain::DeskMessageV1;

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
}
