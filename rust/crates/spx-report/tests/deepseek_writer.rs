use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use spx_domain::{DeskMapProjectionV1, DeskMessageV2, Validate};
use spx_report::{
    DEEPSEEK_CHAT_COMPLETIONS_URL, DEEPSEEK_MODEL_ID, RESEARCH_ADVISORY_DISCLOSURE,
    RESEARCH_UNAVAILABLE_DISCLOSURE, ReportPrompt, ReportWriterClient, ReportWriterConfig,
    ReportWriterErrorCode, Transport, TransportError, TransportRequest, TransportResponse,
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
        "location": format!("active level 7510 {}", long_section("location", 'b')),
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
        "location": "SPX 7512 · active level 7510",
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
        "location": "SPX 7512 · active level 7510",
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

fn assert_compact_prompt_contract(system_prompt: &str) {
    for required in [
        "exactly these string fields",
        "title, desk_view, location, structure",
        "no surrounding prose or Markdown fence",
        "Direction may come only from an explicit price trigger",
        "Gamma must never be presented as the source",
        "Dealer sign is unknown",
        "market makers are buying, selling",
        "方向来源 in primary_path",
        "NO TRADE in desk_view",
        "typed direction is none",
        "operator-facing compact report",
        "desk_view is Base Case",
        "location plus structure explain Why",
        "primary_path is the next Trigger",
        "alternative_path is Invalidation",
        "data_quality states the Primary Data Impact",
        "source-supplied forecast probability",
        "must never create trade direction, READY",
        "READY, HOLD, PAUSED, WAIT, and CLOSED",
        "do not expose schema names, raw field names, hashes",
        "single most important human impact first",
        "Never expose raw audit codes",
        "no per-field byte or numeric-copy floor",
        "Gamma职责",
        "dealer sign unknown",
    ] {
        assert!(system_prompt.contains(required), "missing {required}");
    }
    for forbidden in [
        "Preserve every ASCII numeric fact",
        "at least as many UTF-8 bytes",
    ] {
        assert!(!system_prompt.contains(forbidden), "retained {forbidden}");
    }
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
    assert_compact_prompt_contract(system_prompt);
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
fn shorter_operator_message_is_accepted_when_required_semantics_survive() {
    let mut source = semantic_message_value();
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
        source[field] = json!(format!(
            "{} {}",
            source[field].as_str().unwrap(),
            "repeated source transcript detail ".repeat(20)
        ));
    }
    let projection = projection_with_direction(&source, "none");
    let concise = concise_semantic_message_value();
    let visible_content = serde_json::to_string(&concise).unwrap();
    assert!(visible_content.len() < serde_json::to_string(&source).unwrap().len());
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

    assert_eq!(output.visible_content, visible_content);
    assert!(output.message.desk_view.as_str().contains("NO TRADE"));
    assert!(output.message.primary_path.as_str().contains("方向来源"));
    assert!(output.message.execution.as_str().contains("WAIT"));
}

#[test]
fn field_by_field_ascii_numeric_copy_is_not_an_acceptance_gate() {
    let source = numeric_message_value();
    let mut projection = projection_with_message(&source);
    projection.level_kind = None;
    projection.level = None;
    projection.validate().unwrap();
    let rewritten = message_value();
    let visible_content = serde_json::to_string(&rewritten).unwrap();
    for omitted in [
        "101", "202", "303.5", "404-405", "60%", "-6", "+10/20", "1.25", "900",
    ] {
        assert!(!visible_content.contains(omitted));
    }
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

    assert_eq!(output.visible_content, visible_content);
}

#[test]
fn ready_report_rejects_missing_trigger_target_ask_ttl_or_risk_reward_numbers() {
    let source = json!({
        "title": "SPX READY",
        "desk_view": "LONG / CALL · READY after confirmed acceptance",
        "location": "SPX 7512 · active level 7510",
        "structure": "Gamma职责: 只说明反馈; dealer sign unknown",
        "primary_path": "方向来源: 7510 above accepted for 5m",
        "alternative_path": "Invalid below 7505",
        "targets": "7525 then 7540",
        "execution": "READY · 7510C/7520C · ask cap 1.25 · TTL 90s · R/R 1.5",
        "data_quality": "Ready"
    });
    let mut projection = projection_with_message(&source);
    projection.stage = spx_domain::DeskStage::Ready;
    projection.validate().unwrap();

    for (field, missing) in [
        ("primary_path", "5m"),
        ("alternative_path", "7505"),
        ("targets", "7540"),
        ("execution", "1.25"),
        ("execution", "90s"),
        ("execution", "R/R 1.5"),
    ] {
        let mut rendered = source.clone();
        rendered[field] = json!(
            rendered[field]
                .as_str()
                .unwrap()
                .replace(missing, "omitted")
        );
        let visible_content = serde_json::to_string(&rendered).unwrap();
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

        assert_eq!(error.code(), ReportWriterErrorCode::CriticalFactMissing);
    }
}

#[test]
fn visible_internal_contract_ids_hashes_and_raw_reason_codes_fail_closed() {
    let source = message_value();
    let mut projection = projection_with_message(&source);
    projection.quality = spx_domain::DeskDataQuality::Degraded;
    projection.quality_reasons =
        vec![spx_domain::Token::new("ibkr_feed_unavailable", "quality reason").unwrap()];
    projection.validate().unwrap();
    let leaks = [
        "schema_version=desk_map_projection.v1".to_owned(),
        format!("projection_id={}", projection.projection_id.as_str()),
        format!("hash={}", projection.structure_fingerprint.as_str()),
        "audit=ibkr_feed_unavailable".to_owned(),
        "quality_reasons=[stale]".to_owned(),
        "QUALITY_REASONS=[STALE]".to_owned(),
        "observed_through=2026-08-04T14:00:00Z".to_owned(),
        "regime_reason_codes=[missing]".to_owned(),
        "state_id=state_02 posterior=0.95".to_owned(),
    ];

    for leak in leaks {
        let mut rendered = source.clone();
        rendered["data_quality"] = json!(format!("Degraded · {leak}"));
        let visible_content = serde_json::to_string(&rendered).unwrap();
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

        assert_eq!(error.code(), ReportWriterErrorCode::InternalDetailLeak);
    }
}

#[test]
fn source_research_base_case_cannot_disappear_from_the_visible_desk_view() {
    let mut projection_value: Value = serde_json::from_str(include_str!(
        "../../../../contracts/golden/domain/v1/desk_map_projection.json"
    ))
    .unwrap();
    projection_value["direction"] = json!("none");
    projection_value["thesis"] = json!("none");
    projection_value["message"] = semantic_message_value();
    projection_value["message"]["desk_view"] = json!(
        "NO TRADE\n研究视角（HMM未校准，仅咨询；夜盘ES）：基线=区间/中位收盘 · HMM映射后的主导收盘桶模型权重 90%；不改变价格方向、触发或READY"
    );
    let projection: DeskMapProjectionV1 = serde_json::from_value(projection_value).unwrap();
    projection.validate().unwrap();

    let without_research = concise_semantic_message_value();
    let client = ReportWriterClient::new(
        config(true, 64_000),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(&serde_json::to_string(&without_research).unwrap(), "stop"),
        )),
    )
    .unwrap();
    let error = client.write_desk_map(&projection).unwrap_err();
    assert_eq!(error.code(), ReportWriterErrorCode::ResearchAdvisoryMissing);

    let mut visible = concise_semantic_message_value();
    visible["desk_view"] =
        json!("NO TRADE · 未校准HMM研究 Base Case：区间/中位收盘 · 模型权重 90%");
    visible["data_quality"] = json!("Ready · 未校准研究不产生交易方向或授权 READY");
    let client = ReportWriterClient::new(
        config(true, 64_000),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(&serde_json::to_string(&visible).unwrap(), "stop"),
        )),
    )
    .unwrap();

    let output = client.write_desk_map(&projection).unwrap();
    assert!(output.message.desk_view.as_str().contains("模型权重 90%"));
}

#[test]
fn p_vs_q_evidence_cannot_disappear_or_change_in_the_visible_desk_view() {
    let mut source = semantic_message_value();
    source["desk_view"] = json!(
        "NO TRADE\nP/Q研究（未校准，不产生方向） 5分钟上行终值跟随：P 62%（前日止，n=98/14日，区间52%–71%） · Q代理 49% · P−Q +13pp；未扣点差/滑点，真实成交与净收益标签尚不可用 → NO TRADE"
    );
    let projection = projection_with_direction(&source, "none");

    let without_probability = concise_semantic_message_value();
    let client = ReportWriterClient::new(
        config(true, 64_000),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(
                &serde_json::to_string(&without_probability).unwrap(),
                "stop",
            ),
        )),
    )
    .unwrap();
    let error = client.write_desk_map(&projection).unwrap_err();
    assert_eq!(error.code(), ReportWriterErrorCode::ResearchAdvisoryMissing);

    for (original, changed) in [
        ("Q代理 49%", "Q代理 59%"),
        ("n=98/14日", "n=8/4日"),
        ("区间52%–71%", "区间52%–70%"),
    ] {
        let mut changed_probability = source.clone();
        changed_probability["desk_view"] = json!(
            source["desk_view"]
                .as_str()
                .unwrap()
                .replace(original, changed)
        );
        let client = ReportWriterClient::new(
            config(true, 64_000),
            true,
            RecordingTransport::new(TransportResponse::new(
                200,
                response(
                    &serde_json::to_string(&changed_probability).unwrap(),
                    "stop",
                ),
            )),
        )
        .unwrap();
        let error = client.write_desk_map(&projection).unwrap_err();
        assert_eq!(error.code(), ReportWriterErrorCode::ResearchAdvisoryMissing);
    }

    let mut relocated_probability = source.clone();
    relocated_probability["desk_view"] = json!(
        "NO TRADE · 5 62 98 14 52 71 49 13\nP/Q研究（未校准，不产生方向） · 等待价格确认 → NO TRADE"
    );
    let client = ReportWriterClient::new(
        config(true, 64_000),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(
                &serde_json::to_string(&relocated_probability).unwrap(),
                "stop",
            ),
        )),
    )
    .unwrap();
    let error = client.write_desk_map(&projection).unwrap_err();
    assert_eq!(error.code(), ReportWriterErrorCode::ResearchAdvisoryMissing);

    let client = ReportWriterClient::new(
        config(true, 64_000),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(&serde_json::to_string(&source).unwrap(), "stop"),
        )),
    )
    .unwrap();
    let output = client.write_desk_map(&projection).unwrap();
    assert!(output.message.desk_view.as_str().contains("P 62%"));
    assert!(output.message.desk_view.as_str().contains("Q代理 49%"));
    assert!(output.message.desk_view.as_str().contains("P−Q +13pp"));
}

#[test]
fn required_research_disclosure_is_added_without_a_data_quality_byte_floor() {
    let source = message_value();
    let projection = projection_with_message(&source);
    let mut raw_output = source.clone();
    raw_output["data_quality"] = json!("x");
    assert!(
        raw_output["data_quality"].as_str().unwrap().len()
            < source["data_quality"].as_str().unwrap().len()
    );
    let visible_content = serde_json::to_string(&raw_output).unwrap();
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

    assert_eq!(output.visible_content, visible_content);
    assert!(output.message.data_quality.as_str().starts_with('x'));
    assert!(
        output
            .message
            .data_quality
            .as_str()
            .contains(RESEARCH_UNAVAILABLE_DISCLOSURE)
    );
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
        ("execution", "READY"),
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
fn concise_reorganization_is_accepted_when_contract_and_semantic_markers_survive() {
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
    assert!(concise_content.len() > 283);
    assert_eq!(output.message.execution.as_str(), "WAIT");
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
fn embedded_research_context_is_sent_once_inside_the_projection() {
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

    assert_eq!(output.message.title, projection.message.title);
    assert!(
        output
            .message
            .data_quality
            .as_str()
            .starts_with(projection.message.data_quality.as_str())
    );
    assert!(
        output
            .message
            .data_quality
            .as_str()
            .contains(RESEARCH_ADVISORY_DISCLOSURE)
    );
    let requests = inspector.requests();
    let user_prompt = requests[0].body()["messages"][1]["content"]
        .as_str()
        .unwrap();
    let prompt_body = user_prompt
        .strip_prefix("desk_map_projection.v1 JSON follows:\n")
        .unwrap();
    let (projection_json, research_status) = prompt_body
        .split_once("\n\nresearch_context_status=embedded_contract_valid\n")
        .unwrap();
    let prompt_projection: Value = serde_json::from_str(projection_json).unwrap();
    assert_eq!(
        prompt_projection,
        serde_json::to_value(&projection).unwrap()
    );
    assert!(research_status.contains("appears once inside desk_map_projection.v1"));
    assert!(!research_status.contains("research_context.v2 JSON follows"));
    assert_eq!(user_prompt.matches("\"posterior\"").count(), 1);
    assert!(!user_prompt.contains("market-maker behavior estimate"));
}

#[test]
fn unvalidated_hmm_probability_can_inform_base_case_but_cannot_authorize_ready() {
    let mut projection_value: Value = serde_json::from_str(include_str!(
        "../../../../contracts/golden/domain/v1/desk_map_projection.json"
    ))
    .unwrap();
    projection_value["direction"] = json!("none");
    projection_value["thesis"] = json!("none");
    projection_value["message"] = semantic_message_value();
    let projection: DeskMapProjectionV1 = serde_json::from_value(projection_value).unwrap();
    projection.validate().unwrap();
    let mut advisory = concise_semantic_message_value();
    advisory["desk_view"] =
        json!("NO TRADE · 未校准研究观点：RTH 收盘处于上区概率 84.20975712%，仅作 Base Case");
    advisory["execution"] = json!("WAIT · 研究观点不产生价格触发或操作授权");
    let visible_content = serde_json::to_string(&advisory).unwrap();
    let client = ReportWriterClient::new(
        config(true, 64_000),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(&visible_content, "stop"),
        )),
    )
    .unwrap();

    let output = client.write_desk_map(&projection).unwrap();

    assert!(output.message.desk_view.as_str().contains("未校准研究观点"));
    assert!(output.message.desk_view.as_str().contains("84.20975712%"));
    assert!(output.message.execution.as_str().contains("WAIT"));
    assert!(!output.message.execution.as_str().contains("READY"));
    assert!(
        output
            .message
            .data_quality
            .as_str()
            .contains(RESEARCH_ADVISORY_DISCLOSURE)
    );

    advisory["execution"] = json!("READY · research base case authorizes entry");
    let invented_ready = serde_json::to_string(&advisory).unwrap();
    let client = ReportWriterClient::new(
        config(true, 64_000),
        true,
        RecordingTransport::new(TransportResponse::new(
            200,
            response(&invented_ready, "stop"),
        )),
    )
    .unwrap();

    let error = client.write_desk_map(&projection).unwrap_err();
    assert_eq!(
        error.code(),
        ReportWriterErrorCode::DirectionAuthorityViolation
    );
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
