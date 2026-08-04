use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use spx_domain::{DeskMapProjectionV1, DeskMessageV2, Validate};
use spx_report::{
    DEEPSEEK_CHAT_COMPLETIONS_URL, DEEPSEEK_MODEL_ID, RESEARCH_UNAVAILABLE_DISCLOSURE,
    ReportPrompt, ReportWriterClient, ReportWriterConfig, ReportWriterErrorCode, Transport,
    TransportError, TransportRequest, TransportResponse,
};

const REASONING_MARKER: &str = "private-reasoning-must-not-escape";

#[derive(Clone)]
struct RecordingTransport {
    requests: Arc<Mutex<Vec<TransportRequest>>>,
    response: TransportResponse,
}

impl RecordingTransport {
    fn new(response: TransportResponse) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            response,
        }
    }

    fn requests(&self) -> Vec<TransportRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl Transport for RecordingTransport {
    fn send(&self, request: &TransportRequest) -> Result<TransportResponse, TransportError> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(self.response.clone())
    }
}

fn config(network_enabled: bool, max_tokens: u32) -> ReportWriterConfig {
    ReportWriterConfig::from_toml(&format!(
        r#"
            network_enabled = {network_enabled}
            api_key_env = "DEEPSEEK_API_KEY"
            max_tokens = {max_tokens}
            request_timeout_seconds = 90
        "#
    ))
    .unwrap()
}

fn response(content: &str, finish_reason: &str) -> String {
    json!({
        "id": "completion-test",
        "model": DEEPSEEK_MODEL_ID,
        "choices": [{
            "index": 0,
            "finish_reason": finish_reason,
            "message": {
                "role": "assistant",
                "content": content,
                "reasoning_content": REASONING_MARKER
            }
        }]
    })
    .to_string()
}

fn message_value() -> Value {
    json!({
        "title": "SPX Desk Map",
        "desk_view": "Call breakout confirmed",
        "location": "SPX 7512",
        "structure": "Put wall 7480; flip 7510; call wall 7550",
        "primary_path": "Hold above 7510 and test 7550",
        "alternative_path": "Reject below 7510 and rotate to 7480",
        "targets": "7550 then 7575",
        "execution": "Wait for exact-leg readiness and respect ask cap",
        "data_quality": "Ready"
    })
}

fn projection_with_message(message: &Value) -> DeskMapProjectionV1 {
    projection_with_direction(message, "up")
}

fn projection_with_direction(message: &Value, direction: &str) -> DeskMapProjectionV1 {
    let thesis = if direction == "none" {
        "none"
    } else {
        "breakout"
    };
    serde_json::from_value(json!({
        "schema_version": "desk_map_projection.v1",
        "projection_id": "desk-map:test",
        "source_snapshot_id": "snapshot:test",
        "source_slot": "2026-08-04:10:00",
        "trading_date_et": "2026-08-04",
        "session": "rth",
        "observed_through": "2026-08-04T14:00:00Z",
        "available_at": "2026-08-04T14:00:01Z",
        "valid_until": "2026-08-04T14:20:01Z",
        "structure_fingerprint": "a".repeat(64),
        "stage": "confirmed",
        "phase": "confirmed",
        "direction": direction,
        "thesis": thesis,
        "level_kind": "flip_high",
        "level": 7510.0,
        "quality": "ready",
        "quality_reasons": [],
        "research_context_document_id": null,
        "research_context": null,
        "action_authority": "none",
        "automatic_ordering": false,
        "message": message
    }))
    .unwrap()
}

fn long_section(label: &str, fill: char) -> String {
    format!(
        "{label}:{}:{label}-complete",
        fill.to_string().repeat(3_500)
    )
}

fn long_message_value() -> Value {
    json!({
        "title": "Complete institutional SPX desk map",
        "desk_view": long_section("desk-view", 'a'),
        "location": long_section("location", 'b'),
        "structure": long_section("structure", 'c'),
        "primary_path": long_section("primary-path", 'd'),
        "alternative_path": long_section("alternative-path", 'e'),
        "targets": long_section("targets", 'f'),
        "execution": long_section("execution", 'g'),
        "data_quality": long_section("data-quality", 'h')
    })
}

fn semantic_message_value() -> Value {
    json!({
        "title": "SPX Desk Map",
        "desk_view": "NO TRADE: 尚无价格 trigger 与 ES flow 确认",
        "location": "SPX 7512",
        "structure": "Gamma职责: 只描述已观察运动的压制或放大机制; dealer sign unknown",
        "primary_path": "方向来源: wait for a confirmed price trigger and aligned ES flow",
        "alternative_path": "Remain flat while direction is unconfirmed",
        "targets": "No target is active before confirmation",
        "execution": "WAIT for deterministic confirmation",
        "data_quality": "Quotes ready"
    })
}

fn concise_semantic_message_value() -> Value {
    json!({
        "title": "SPX Desk",
        "desk_view": "NO TRADE",
        "location": "SPX 7512",
        "structure": "Gamma职责: 只说明压制或放大; dealer sign unknown",
        "primary_path": "方向来源: price trigger + ES flow",
        "alternative_path": "Wait",
        "targets": "None before confirmation",
        "execution": "WAIT",
        "data_quality": "Ready"
    })
}

fn numeric_message_value() -> Value {
    json!({
        "title": "Desk 101",
        "desk_view": "View 202",
        "location": "SPX 303.5",
        "structure": "Range 404-405",
        "primary_path": "Probability 60%",
        "alternative_path": "Stop -6",
        "targets": "Targets +10/20",
        "execution": "Ask 1.25",
        "data_quality": "Fresh 900ms"
    })
}

#[test]
fn request_contract_is_fixed_to_flash_max_reasoning_and_non_streaming() {
    let transport = RecordingTransport::new(TransportResponse::new(200, response("desk", "stop")));
    let inspector = transport.clone();
    let client = ReportWriterClient::new(config(true, 12_800), true, transport).unwrap();

    let output = client
        .write(&ReportPrompt::new("system facts", "report facts"))
        .unwrap();

    assert_eq!(output.content, "desk");
    let requests = inspector.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.endpoint(), DEEPSEEK_CHAT_COMPLETIONS_URL);
    assert_eq!(request.api_key_env(), "DEEPSEEK_API_KEY");
    assert_eq!(
        request.body(),
        &json!({
            "model": "deepseek-v4-flash",
            "messages": [
                {"role": "system", "content": "system facts"},
                {"role": "user", "content": "report facts"}
            ],
            "thinking": {"type": "enabled"},
            "reasoning_effort": "max",
            "response_format": {"type": "json_object"},
            "max_tokens": 12800,
            "stream": false
        })
    );
    assert!(!format!("{request:?}").contains("report facts"));
    assert!(
        !format!("{:?}", ReportPrompt::new("system facts", "report facts"))
            .contains("report facts")
    );
}

#[test]
fn complete_long_response_is_not_truncated_and_raw_response_is_not_exposed() {
    let mut content = String::new();
    for index in 0..1_000 {
        writeln!(content, "line-{index:04}: full institutional desk detail").unwrap();
    }
    assert!(content.len() > 30_000);
    assert!(content.lines().count() > 20);
    let raw_response = response(&content, "stop");
    let expected_hash = hex::encode(Sha256::digest(raw_response.as_bytes()));
    let transport = RecordingTransport::new(TransportResponse::new(200, raw_response.clone()));
    let client = ReportWriterClient::new(config(true, 64_000), true, transport).unwrap();

    let output = client.write(&ReportPrompt::new("system", "user")).unwrap();

    assert_eq!(output.content, content);
    assert_eq!(output.metadata.http_status, 200);
    assert_eq!(output.metadata.raw_response_bytes, raw_response.len());
    assert_eq!(output.metadata.raw_response_sha256, expected_hash);
    assert_eq!(output.metadata.finish_reason.as_deref(), Some("stop"));
    assert_eq!(output.metadata.visible_content_bytes, Some(content.len()));
    let output_debug = format!("{output:?}");
    assert!(!output_debug.contains("line-0000"));
    assert!(!output_debug.contains(REASONING_MARKER));

    let response_debug = format!("{:?}", TransportResponse::new(200, raw_response));
    assert!(!response_debug.contains("line-0000"));
    assert!(!response_debug.contains(REASONING_MARKER));
}

#[test]
fn length_finish_reason_fails_closed_with_only_safe_metadata() {
    let raw_response = response("partial-content-must-not-escape", "length");
    let expected_hash = hex::encode(Sha256::digest(raw_response.as_bytes()));
    let transport = RecordingTransport::new(TransportResponse::new(200, raw_response.clone()));
    let client = ReportWriterClient::new(config(true, 8), true, transport).unwrap();

    let error = client
        .write(&ReportPrompt::new("system", "user"))
        .unwrap_err();

    assert_eq!(error.code(), ReportWriterErrorCode::OutputTruncated);
    assert_eq!(error.to_string(), "output_truncated");
    let metadata = error.metadata().unwrap();
    assert_eq!(metadata.raw_response_bytes, raw_response.len());
    assert_eq!(metadata.raw_response_sha256, expected_hash);
    assert_eq!(metadata.finish_reason.as_deref(), Some("length"));
    let error_debug = format!("{error:?}");
    assert!(!error_debug.contains("partial-content-must-not-escape"));
    assert!(!error_debug.contains(REASONING_MARKER));
}

#[test]
fn desk_map_writer_sends_the_complete_projection_and_accepts_a_long_canonical_message() {
    let expected_message: DeskMessageV2 = serde_json::from_value(long_message_value()).unwrap();
    expected_message.validate().unwrap();
    let projection = projection_with_message(&serde_json::to_value(&expected_message).unwrap());
    let visible_content = serde_json::to_string(&expected_message).unwrap();
    assert!(visible_content.len() > 25_000);
    let transport = RecordingTransport::new(TransportResponse::new(
        200,
        response(&visible_content, "stop"),
    ));
    let inspector = transport.clone();
    let client = ReportWriterClient::new(config(true, 64_000), true, transport).unwrap();

    let output = client.write_desk_map(&projection).unwrap();

    assert_eq!(output.message.title, expected_message.title);
    assert_eq!(output.message.desk_view, expected_message.desk_view);
    assert_eq!(output.message.location, expected_message.location);
    assert_eq!(output.message.structure, expected_message.structure);
    assert_eq!(output.message.primary_path, expected_message.primary_path);
    assert_eq!(
        output.message.alternative_path,
        expected_message.alternative_path
    );
    assert_eq!(output.message.targets, expected_message.targets);
    assert_eq!(output.message.execution, expected_message.execution);
    assert!(
        output
            .message
            .data_quality
            .as_str()
            .starts_with(expected_message.data_quality.as_str())
    );
    assert!(
        output
            .message
            .data_quality
            .as_str()
            .contains(RESEARCH_UNAVAILABLE_DISCLOSURE)
    );
    assert_eq!(output.visible_content, visible_content);
    assert_eq!(
        output.metadata.visible_content_bytes,
        Some(output.visible_content.len())
    );
    assert!(
        output
            .message
            .data_quality
            .as_str()
            .contains("data-quality-complete")
    );
    let output_debug = format!("{output:?}");
    assert!(!output_debug.contains("desk-view:"));
    assert!(!output_debug.contains(REASONING_MARKER));

    let requests = inspector.requests();
    assert_eq!(requests.len(), 1);
    let body = requests[0].body();
    assert_eq!(body["model"], "deepseek-v4-flash");
    assert_eq!(body["thinking"], json!({"type": "enabled"}));
    assert_eq!(body["reasoning_effort"], "max");
    assert_eq!(body["response_format"], json!({"type": "json_object"}));
    assert_eq!(body["stream"], false);
    let system_prompt = body["messages"][0]["content"].as_str().unwrap();
    assert!(system_prompt.contains("exactly these string fields"));
    assert!(system_prompt.contains("title, desk_view, location, structure"));
    assert!(system_prompt.contains("no surrounding prose or Markdown fence"));
    assert!(system_prompt.contains("Direction may come only from an explicit price trigger"));
    assert!(system_prompt.contains("Gamma must never be presented as the source"));
    assert!(system_prompt.contains("Dealer sign is unknown"));
    assert!(system_prompt.contains("market makers are buying, selling"));
    assert!(system_prompt.contains("方向来源 in primary_path"));
    assert!(system_prompt.contains("NO TRADE in desk_view"));
    assert!(system_prompt.contains("typed direction is none"));
    assert!(system_prompt.contains("ASCII numeric fact"));
    assert!(system_prompt.contains("READY, HOLD, PAUSED, WAIT, and CLOSED"));
    assert!(system_prompt.contains("do not expose schema names, raw field names, hashes"));
    assert!(system_prompt.contains("single most important human impact first"));
    assert!(!system_prompt.contains("at least as many UTF-8 bytes"));
    for marker in ["方向来源", "Gamma职责", "dealer sign unknown", "NO TRADE"] {
        assert!(system_prompt.contains(marker));
    }
    let user_prompt = body["messages"][1]["content"].as_str().unwrap();
    let prompt_body = user_prompt
        .strip_prefix("desk_map_projection.v1 JSON follows:\n")
        .unwrap();
    let (projection_json, research_block) = prompt_body
        .split_once("\n\nresearch_context_status=")
        .unwrap();
    let prompt_projection: Value = serde_json::from_str(projection_json).unwrap();
    assert_eq!(
        prompt_projection,
        serde_json::to_value(&projection).unwrap()
    );
    assert!(research_block.starts_with("unavailable\n"));
    assert!(research_block.contains(RESEARCH_UNAVAILABLE_DISCLOSURE));
}

#[test]
fn semantic_markers_are_preserved_when_the_source_contains_them() {
    let source = semantic_message_value();
    let projection = projection_with_direction(&source, "none");
    let visible_content = serde_json::to_string(&source).unwrap();
    let client = ReportWriterClient::new(
        config(true, 12_800),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(&visible_content, "stop"),
        )),
    )
    .unwrap();

    let output = client.write_desk_map(&projection).unwrap();

    assert!(output.message.primary_path.as_str().contains("方向来源"));
    assert!(output.message.structure.as_str().contains("Gamma职责"));
    assert!(
        output
            .message
            .structure
            .as_str()
            .contains("dealer sign unknown")
    );
    assert!(output.message.desk_view.as_str().contains("NO TRADE"));
}

#[test]
fn dropping_any_source_semantic_marker_fails_closed() {
    let source = semantic_message_value();
    let projection = projection_with_direction(&source, "none");

    for marker in ["方向来源", "Gamma职责", "dealer sign unknown", "NO TRADE"] {
        let mut rewritten = source.clone();
        for field in [
            "title",
            "desk_view",
            "location",
            "structure",
            "primary_path",
            "alternative_path",
            "targets",
            "execution",
            "data_quality",
        ] {
            let text = rewritten[field].as_str().unwrap();
            rewritten[field] = json!(text.replace(
                marker,
                "required semantic marker deliberately omitted from this otherwise complete section"
            ));
        }
        let visible_content = serde_json::to_string(&rewritten).unwrap();
        let client = ReportWriterClient::new(
            config(true, 12_800),
            true,
            RecordingTransport::new(TransportResponse::new(
                200,
                response(&visible_content, "stop"),
            )),
        )
        .unwrap();

        let error = client.write_desk_map(&projection).unwrap_err();

        assert_eq!(
            error.code(),
            ReportWriterErrorCode::SemanticMarkerFieldMismatch,
            "dropping {marker} must fail closed"
        );
        assert_eq!(error.to_string(), "semantic_marker_field_mismatch");
        assert!(error.metadata().is_some());
        assert!(!format!("{error:?}").contains("required semantic marker deliberately omitted"));
    }
}

#[test]
fn moving_semantic_markers_to_the_wrong_fields_fails_closed() {
    let source = semantic_message_value();
    let projection = projection_with_direction(&source, "none");
    let mut misplaced = source.clone();
    misplaced["desk_view"] = json!("Gamma职责 and dealer sign unknown remain visible");
    misplaced["structure"] = json!("NO TRADE remains visible");
    misplaced["primary_path"] = json!("WAIT for price confirmation");
    misplaced["alternative_path"] = json!("方向来源 remains visible here");
    let visible_content = serde_json::to_string(&misplaced).unwrap();
    let client = ReportWriterClient::new(
        config(true, 12_800),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(&visible_content, "stop"),
        )),
    )
    .unwrap();

    let error = client.write_desk_map(&projection).unwrap_err();

    assert_eq!(
        error.code(),
        ReportWriterErrorCode::SemanticMarkerFieldMismatch
    );
    assert_eq!(error.to_string(), "semantic_marker_field_mismatch");
}

#[test]
fn none_direction_rejects_actionable_language_in_operator_fields() {
    let source = semantic_message_value();
    let projection = projection_with_direction(&source, "none");
    for (field, forbidden) in [
        ("title", "LONG"),
        ("desk_view", "short"),
        ("execution", "做多"),
        ("title", "做空"),
    ] {
        let mut invented = source.clone();
        invented[field] = json!(format!("{} {forbidden}", invented[field].as_str().unwrap()));
        let visible_content = serde_json::to_string(&invented).unwrap();
        let client = ReportWriterClient::new(
            config(true, 12_800),
            true,
            RecordingTransport::new(TransportResponse::new(
                200,
                response(&visible_content, "stop"),
            )),
        )
        .unwrap();

        let error = client.write_desk_map(&projection).unwrap_err();

        assert_eq!(
            error.code(),
            ReportWriterErrorCode::DirectionAuthorityViolation,
            "{forbidden} in {field} must fail closed"
        );
        assert_eq!(error.to_string(), "direction_authority_violation");
    }
}

#[test]
fn typed_direction_label_cannot_move_out_of_desk_view() {
    for (direction, label) in [("up", "LONG / CALL"), ("down", "SHORT / PUT")] {
        let mut source = message_value();
        source["desk_view"] = json!(format!("{label}: confirmed price trigger"));
        let projection = projection_with_direction(&source, direction);
        let mut moved = source.clone();
        moved["title"] = json!(format!("SPX Desk Map {label}"));
        moved["desk_view"] = json!("Confirmed price trigger");
        let visible_content = serde_json::to_string(&moved).unwrap();
        let client = ReportWriterClient::new(
            config(true, 12_800),
            true,
            RecordingTransport::new(TransportResponse::new(
                200,
                response(&visible_content, "stop"),
            )),
        )
        .unwrap();

        let error = client.write_desk_map(&projection).unwrap_err();

        assert_eq!(
            error.code(),
            ReportWriterErrorCode::DirectionLabelMissing,
            "{label} must remain in desk_view"
        );
        assert_eq!(error.to_string(), "direction_label_missing");
    }
}

#[test]
fn numeric_facts_must_remain_in_their_corresponding_fields() {
    let source = numeric_message_value();
    let projection = projection_with_message(&source);
    for (field, token) in [
        ("title", "101"),
        ("desk_view", "202"),
        ("location", "303.5"),
        ("structure", "404-405"),
        ("primary_path", "60%"),
        ("alternative_path", "-6"),
        ("targets", "+10/20"),
        ("execution", "1.25"),
        ("data_quality", "900"),
    ] {
        let mut moved = source.clone();
        moved[field] = json!(moved[field].as_str().unwrap().replace(token, "omitted"));
        let destination = if field == "title" {
            "data_quality"
        } else {
            "title"
        };
        moved[destination] = json!(format!(
            "{} moved {token}",
            moved[destination].as_str().unwrap()
        ));
        let visible_content = serde_json::to_string(&moved).unwrap();
        let client = ReportWriterClient::new(
            config(true, 12_800),
            true,
            RecordingTransport::new(TransportResponse::new(
                200,
                response(&visible_content, "stop"),
            )),
        )
        .unwrap();

        let error = client.write_desk_map(&projection).unwrap_err();

        assert_eq!(
            error.code(),
            ReportWriterErrorCode::NumericFactMissing,
            "numeric token {token} must remain in {field}"
        );
        assert_eq!(error.to_string(), "numeric_fact_missing");
    }
}

#[test]
fn execution_state_markers_cannot_move_out_of_execution() {
    let mut source = message_value();
    source["execution"] = json!("READY HOLD PAUSED WAIT CLOSED");
    let projection = projection_with_message(&source);
    for marker in ["READY", "HOLD", "PAUSED", "WAIT", "CLOSED"] {
        let mut moved = source.clone();
        moved["execution"] = json!(
            moved["execution"]
                .as_str()
                .unwrap()
                .replace(marker, "state omitted")
        );
        moved["structure"] = json!(format!("{} {marker}", moved["structure"].as_str().unwrap()));
        let visible_content = serde_json::to_string(&moved).unwrap();
        let client = ReportWriterClient::new(
            config(true, 12_800),
            true,
            RecordingTransport::new(TransportResponse::new(
                200,
                response(&visible_content, "stop"),
            )),
        )
        .unwrap();

        let error = client.write_desk_map(&projection).unwrap_err();

        assert_eq!(
            error.code(),
            ReportWriterErrorCode::ExecutionStateMarkerMissing,
            "{marker} must remain in execution"
        );
        assert_eq!(error.to_string(), "execution_state_marker_missing");
    }
}

#[test]
fn concise_reorganization_is_allowed_when_contract_and_semantic_markers_survive() {
    let source = semantic_message_value();
    let projection = projection_with_direction(&source, "none");
    let concise = concise_semantic_message_value();
    let concise_content = serde_json::to_string(&concise).unwrap();
    assert!(concise_content.len() < serde_json::to_string(&source).unwrap().len());
    let client = ReportWriterClient::new(
        config(true, 12_800),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(&concise_content, "stop"),
        )),
    )
    .unwrap();

    let output = client.write_desk_map(&projection).unwrap();

    assert_eq!(output.visible_content, concise_content);
    for marker in ["方向来源", "Gamma职责", "dealer sign unknown", "NO TRADE"] {
        assert!(output.visible_content.contains(marker));
    }
}

#[test]
fn generic_short_card_without_source_semantics_is_rejected() {
    let projection = projection_with_direction(&semantic_message_value(), "none");
    let short_card = json!({
        "title": "SPX Desk",
        "desk_view": "Bullish context only.",
        "location": "Above VWAP.",
        "structure": "Walls nearby.",
        "primary_path": "Hold and rise.",
        "alternative_path": "Lose and rotate.",
        "targets": "Upper wall.",
        "execution": "Observe only.",
        "data_quality": "Quotes ready."
    });
    let short_content = serde_json::to_string(&short_card).unwrap();
    assert!(short_content.len() < 283);
    let client = ReportWriterClient::new(
        config(true, 12_800),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(&short_content, "stop"),
        )),
    )
    .unwrap();

    let error = client.write_desk_map(&projection).unwrap_err();

    assert_eq!(
        error.code(),
        ReportWriterErrorCode::SemanticMarkerFieldMismatch
    );
    assert_eq!(error.to_string(), "semantic_marker_field_mismatch");
    assert!(!format!("{error:?}").contains("Bullish context only"));
}

#[test]
fn embedded_research_context_is_sent_as_a_complete_second_fact_input() {
    let projection: DeskMapProjectionV1 = serde_json::from_str(include_str!(
        "../../../../contracts/golden/domain/v1/desk_map_projection.json"
    ))
    .unwrap();
    projection.validate().unwrap();
    let visible_content = serde_json::to_string(&projection.message).unwrap();
    let transport = RecordingTransport::new(TransportResponse::new(
        200,
        response(&visible_content, "stop"),
    ));
    let inspector = transport.clone();
    let client = ReportWriterClient::new(config(true, 64_000), true, transport).unwrap();

    let output = client.write_desk_map(&projection).unwrap();

    assert_eq!(output.message, projection.message);
    let requests = inspector.requests();
    let user_prompt = requests[0].body()["messages"][1]["content"]
        .as_str()
        .unwrap();
    let (_, research_json) = user_prompt
        .split_once(
            "\n\nresearch_context_status=embedded_contract_valid\nresearch_context.v2 JSON follows:\n",
        )
        .unwrap();
    let prompt_research: Value = serde_json::from_str(research_json).unwrap();
    assert_eq!(
        prompt_research,
        serde_json::to_value(projection.research_context.as_ref().unwrap()).unwrap()
    );
    assert!(!user_prompt.contains("market-maker behavior estimate"));
}

#[test]
fn desk_map_writer_fails_closed_for_non_json_unknown_fields_and_empty_sections() {
    let projection = projection_with_message(&message_value());
    let cases = [
        "not-json".to_owned(),
        {
            let mut value = message_value();
            value["unknown"] = json!("must fail");
            serde_json::to_string(&value).unwrap()
        },
        {
            let mut value = message_value();
            value["execution"] = json!("   \n\t");
            serde_json::to_string(&value).unwrap()
        },
        format!(
            "```json\n{}\n```",
            serde_json::to_string(&message_value()).unwrap()
        ),
    ];

    for invalid_content in cases {
        let raw_response = response(&invalid_content, "stop");
        let client = ReportWriterClient::new(
            config(true, 12_800),
            true,
            RecordingTransport::new(TransportResponse::new(200, raw_response)),
        )
        .unwrap();

        let error = client.write_desk_map(&projection).unwrap_err();

        assert_eq!(error.code(), ReportWriterErrorCode::DeskMessageInvalidJson);
        assert_eq!(error.to_string(), "desk_message_invalid_json");
        let error_debug = format!("{error:?}");
        assert!(!error_debug.contains(&invalid_content));
        assert!(!error_debug.contains(REASONING_MARKER));
    }
}

#[test]
fn invalid_projection_fails_before_transport() {
    let mut projection = projection_with_message(&message_value());
    projection.automatic_ordering = true;
    let transport = RecordingTransport::new(TransportResponse::new(
        200,
        response(&serde_json::to_string(&message_value()).unwrap(), "stop"),
    ));
    let inspector = transport.clone();
    let client = ReportWriterClient::new(config(true, 12_800), true, transport).unwrap();

    let error = client.write_desk_map(&projection).unwrap_err();

    assert_eq!(error.code(), ReportWriterErrorCode::ProjectionInvalid);
    assert!(inspector.requests().is_empty());
}

#[test]
fn both_network_authorization_gates_are_required() {
    let response = TransportResponse::new(200, response("desk", "stop"));
    let config_gate_off = ReportWriterClient::new(
        config(false, 12_800),
        true,
        RecordingTransport::new(response.clone()),
    )
    .err()
    .unwrap();
    assert_eq!(
        config_gate_off.code(),
        ReportWriterErrorCode::NetworkNotAuthorized
    );

    let caller_gate_off = ReportWriterClient::new(
        config(true, 12_800),
        false,
        RecordingTransport::new(response),
    )
    .err()
    .unwrap();
    assert_eq!(
        caller_gate_off.code(),
        ReportWriterErrorCode::NetworkNotAuthorized
    );
}

#[test]
fn config_cannot_override_model_or_embed_api_key() {
    let model_override = ReportWriterConfig::from_toml(
        r#"
            network_enabled = true
            api_key_env = "DEEPSEEK_API_KEY"
            max_tokens = 12800
            model = "deepseek-v4-pro"
        "#,
    );
    assert!(model_override.is_err());

    let embedded_key = ReportWriterConfig::from_toml(
        r#"
            network_enabled = true
            api_key_env = "DEEPSEEK_API_KEY"
            max_tokens = 12800
            api_key = "not-allowed"
        "#,
    );
    assert!(embedded_key.is_err());
}

#[test]
fn max_tokens_is_non_zero_and_provider_bounded() {
    assert!(
        ReportWriterConfig::from_toml(
            r#"
            api_key_env = "DEEPSEEK_API_KEY"
            max_tokens = 0
        "#,
        )
        .is_err()
    );
    assert!(
        ReportWriterConfig::from_toml(
            r#"
            api_key_env = "DEEPSEEK_API_KEY"
            max_tokens = 384001
        "#,
        )
        .is_err()
    );
}
