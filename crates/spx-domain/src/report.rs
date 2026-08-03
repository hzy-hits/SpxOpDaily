use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::validation::{require_schema, unique_tokens};
use crate::{
    DeskMessageV2, DomainError, MarketSession, PositiveF64, ResearchSignalsV1, Token, Validate,
};

pub const DESK_MAP_PROJECTION_SCHEMA_VERSION: &str = "desk_map_projection.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeskStage {
    Observing,
    Watching,
    Armed,
    Confirmed,
    Ready,
    Active,
    Exit,
    Invalidated,
    Expired,
    Paused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeskLevelPhase {
    Far,
    Approaching,
    Testing,
    BreakPending,
    RejectPending,
    Accepted,
    Rejected,
    Retest,
    Confirmed,
    Invalidated,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeskDirection {
    Up,
    Down,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeskThesis {
    Breakout,
    Fade,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeskDataQuality {
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportActionAuthority {
    None,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeskMapProjectionV1 {
    pub schema_version: String,
    pub projection_id: Token,
    pub source_snapshot_id: Token,
    pub source_slot: Token,
    pub trading_date_et: NaiveDate,
    pub session: MarketSession,
    pub observed_through: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub structure_fingerprint: Token,
    pub stage: DeskStage,
    pub phase: DeskLevelPhase,
    pub direction: DeskDirection,
    pub thesis: DeskThesis,
    pub level_kind: Option<Token>,
    pub level: Option<PositiveF64>,
    pub quality: DeskDataQuality,
    pub quality_reasons: Vec<Token>,
    pub research_context_document_id: Option<Token>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub research_context: Option<ResearchSignalsV1>,
    pub action_authority: ReportActionAuthority,
    pub automatic_ordering: bool,
    pub message: DeskMessageV2,
}

impl Validate for DeskMapProjectionV1 {
    fn validate(&self) -> Result<(), DomainError> {
        require_schema(
            &self.schema_version,
            DESK_MAP_PROJECTION_SCHEMA_VERSION,
            "desk map projection",
        )?;
        if self.observed_through > self.available_at {
            return Err(DomainError::TimeOrder(
                "desk map observed_through is after available_at",
            ));
        }
        if self.valid_until <= self.available_at {
            return Err(DomainError::TimeOrder(
                "desk map valid_until must be after available_at",
            ));
        }
        if self.automatic_ordering {
            return Err(DomainError::Invalid {
                field: "automatic_ordering",
                reason: "desk maps cannot place orders",
            });
        }
        if self.level_kind.is_some() != self.level.is_some() {
            return Err(DomainError::Invalid {
                field: "desk map level",
                reason: "level_kind and level must be present together",
            });
        }
        if !is_lower_hex_sha256(self.structure_fingerprint.as_str()) {
            return Err(DomainError::Invalid {
                field: "structure_fingerprint",
                reason: "must be a lowercase SHA-256 digest",
            });
        }
        unique_tokens(&self.quality_reasons, "desk map quality reason")?;
        match self.quality {
            DeskDataQuality::Ready if !self.quality_reasons.is_empty() => {
                return Err(DomainError::Invalid {
                    field: "quality_reasons",
                    reason: "ready desk map cannot contain degradation reasons",
                });
            }
            DeskDataQuality::Degraded | DeskDataQuality::Unavailable
                if self.quality_reasons.is_empty() =>
            {
                return Err(DomainError::Invalid {
                    field: "quality_reasons",
                    reason: "non-ready desk map requires at least one reason",
                });
            }
            DeskDataQuality::Ready | DeskDataQuality::Degraded | DeskDataQuality::Unavailable => {}
        }
        if self.stage == DeskStage::Ready
            && (self.direction == DeskDirection::None || self.thesis == DeskThesis::None)
        {
            return Err(DomainError::Invalid {
                field: "ready desk map",
                reason: "ready stage requires direction and thesis",
            });
        }
        match (&self.research_context_document_id, &self.research_context) {
            (None, None) => {}
            (Some(document_id), Some(research_context)) => {
                research_context.validate()?;
                let Some(context) = research_context.context_v2() else {
                    return Err(DomainError::Invalid {
                        field: "research_context",
                        reason: "desk map projection requires research_context.v2",
                    });
                };
                if &research_context.document_id != document_id {
                    return Err(DomainError::Invalid {
                        field: "research_context_document_id",
                        reason: "does not match embedded research context document_id",
                    });
                }
                if research_context.generated_at > self.available_at {
                    return Err(DomainError::TimeOrder(
                        "research context generated_at is after desk map available_at",
                    ));
                }
                if self.session != MarketSession::Rth
                    || context.cross_index_frame.trading_date_et != self.trading_date_et
                {
                    return Err(DomainError::Invalid {
                        field: "research_context",
                        reason: "must match the desk map RTH trading date",
                    });
                }
            }
            _ => {
                return Err(DomainError::Invalid {
                    field: "research_context",
                    reason: "embedded context and document_id must be present together",
                });
            }
        }
        self.message.validate()
    }
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_projection() -> DeskMapProjectionV1 {
        let research_context: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/domain/v2/research_context.json"
        ))
        .unwrap();
        serde_json::from_value(serde_json::json!({
            "schema_version": "desk_map_projection.v1",
            "projection_id": "desk-map:123",
            "source_snapshot_id": "snapshot:123",
            "source_slot": "2026-08-03:15:15",
            "trading_date_et": "2026-08-03",
            "session": "rth",
            "observed_through": "2026-08-03T19:15:45Z",
            "available_at": "2026-08-03T19:15:47Z",
            "valid_until": "2026-08-03T19:35:47Z",
            "structure_fingerprint": "a".repeat(64),
            "stage": "confirmed",
            "phase": "confirmed",
            "direction": "up",
            "thesis": "breakout",
            "level_kind": "flip_high",
            "level": 7510.0,
            "quality": "ready",
            "quality_reasons": [],
            "research_context_document_id": "research-context:35b62b513a9e9327cab0e069",
            "research_context": research_context,
            "action_authority": "none",
            "automatic_ordering": false,
            "message": {
                "title": "SPX Desk Map",
                "desk_view": "Call breakout confirmed",
                "location": "SPX 7512",
                "structure": "Put Flip Call",
                "primary_path": "Hold above 7510",
                "alternative_path": "Reject below 7510",
                "targets": "7550",
                "execution": "Wait for exact leg",
                "data_quality": "Ready"
            }
        }))
        .unwrap()
    }

    #[test]
    fn validates_complete_observational_projection() {
        valid_projection().validate().unwrap();
    }

    #[test]
    fn rejects_order_authority_and_quality_mismatch() {
        let mut projection = valid_projection();
        projection.automatic_ordering = true;
        assert!(projection.validate().is_err());

        let mut projection = valid_projection();
        projection.quality_reasons = vec![Token::new("stale", "reason").unwrap()];
        assert!(projection.validate().is_err());
    }

    #[test]
    fn rejects_research_context_document_id_mismatch() {
        let mut projection = valid_projection();
        projection.research_context_document_id =
            Some(Token::new("research-context:other", "research context").unwrap());

        assert_eq!(
            projection.validate(),
            Err(DomainError::Invalid {
                field: "research_context_document_id",
                reason: "does not match embedded research context document_id",
            })
        );
    }

    #[test]
    fn rejects_future_research_context() {
        let mut projection = valid_projection();
        projection.research_context.as_mut().unwrap().generated_at =
            projection.available_at + chrono::TimeDelta::seconds(1);

        assert_eq!(
            projection.validate(),
            Err(DomainError::TimeOrder(
                "research context generated_at is after desk map available_at",
            ))
        );
    }

    #[test]
    fn rejects_research_context_from_another_session_or_trading_date() {
        let mut projection = valid_projection();
        projection.trading_date_et = chrono::NaiveDate::from_ymd_opt(2026, 8, 4).unwrap();
        assert_eq!(
            projection.validate(),
            Err(DomainError::Invalid {
                field: "research_context",
                reason: "must match the desk map RTH trading date",
            })
        );

        let mut projection = valid_projection();
        projection.session = MarketSession::Gth;
        assert_eq!(
            projection.validate(),
            Err(DomainError::Invalid {
                field: "research_context",
                reason: "must match the desk map RTH trading date",
            })
        );
    }

    #[test]
    fn rejects_legacy_v1_research_context() {
        let legacy: ResearchSignalsV1 = serde_json::from_str(include_str!(
            "../../../fixtures/domain/v1/experimental_research_signals.json"
        ))
        .unwrap();
        let mut projection = valid_projection();
        projection.research_context_document_id = Some(legacy.document_id.clone());
        projection.research_context = Some(legacy);

        assert_eq!(
            projection.validate(),
            Err(DomainError::Invalid {
                field: "research_context",
                reason: "desk map projection requires research_context.v2",
            })
        );
    }

    #[test]
    fn rejects_dangling_research_context_reference() {
        let mut projection = valid_projection();
        projection.research_context = None;

        assert_eq!(
            projection.validate(),
            Err(DomainError::Invalid {
                field: "research_context",
                reason: "embedded context and document_id must be present together",
            })
        );
    }

    #[test]
    fn requires_explicit_nullable_research_context_field() {
        let mut projection: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/domain/v1/desk_map_projection.json"
        ))
        .unwrap();
        projection
            .as_object_mut()
            .unwrap()
            .remove("research_context");

        serde_json::from_value::<DeskMapProjectionV1>(projection)
            .expect_err("missing atomic research context field must fail closed");
    }
}
