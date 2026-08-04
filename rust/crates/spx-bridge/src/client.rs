use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use spx_domain::{
    AckStatus, CoreAckDisposition, CoreAckV1, IngressEnvelopeV1, IngressMessageV1, Validate,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("core socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("ingress frame exceeds configured byte bound")]
    FrameTooLarge,
    #[error("ingress envelope contract is invalid: {0}")]
    EnvelopeContract(spx_domain::DomainError),
    #[error("ingress envelope JSON encoding failed: {0}")]
    EnvelopeJson(serde_json::Error),
    #[error("core acknowledgement frame is invalid or oversized")]
    InvalidAckFrame,
    #[error("core acknowledgement JSON is invalid: {0}")]
    AckJson(#[from] serde_json::Error),
    #[error("core acknowledgement contract is invalid: {0}")]
    AckContract(#[from] spx_domain::DomainError),
    #[error("core acknowledgement message id does not match in-flight frame")]
    AckMismatch,
    #[error("core acknowledgement disposition does not match in-flight message kind")]
    AckDispositionMismatch,
}

impl ClientError {
    /// Returns true only when no ingress bytes were written and an exact retry cannot help.
    pub const fn is_preflight_failure(&self) -> bool {
        matches!(
            self,
            Self::FrameTooLarge | Self::EnvelopeContract(_) | Self::EnvelopeJson(_)
        )
    }
}

pub struct CoreClient {
    stream: UnixStream,
    maximum_frame_bytes: usize,
}

impl CoreClient {
    pub fn connect(
        path: &Path,
        timeout: Duration,
        maximum_frame_bytes: usize,
    ) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(path)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        Ok(Self {
            stream,
            maximum_frame_bytes,
        })
    }

    pub fn send(&mut self, envelope: &IngressEnvelopeV1) -> Result<CoreAckV1, ClientError> {
        envelope.validate().map_err(ClientError::EnvelopeContract)?;
        let encoded = serde_json::to_vec(envelope).map_err(ClientError::EnvelopeJson)?;
        if encoded.is_empty() || encoded.len() > self.maximum_frame_bytes {
            return Err(ClientError::FrameTooLarge);
        }
        let length = u32::try_from(encoded.len()).map_err(|_| ClientError::FrameTooLarge)?;
        self.stream.write_all(&length.to_be_bytes())?;
        self.stream.write_all(&encoded)?;
        self.stream.flush()?;

        let mut length = [0_u8; 4];
        self.stream.read_exact(&mut length)?;
        let ack_length = usize::try_from(u32::from_be_bytes(length))
            .map_err(|_| ClientError::InvalidAckFrame)?;
        if ack_length == 0 || ack_length > self.maximum_frame_bytes {
            return Err(ClientError::InvalidAckFrame);
        }
        let mut bytes = vec![0_u8; ack_length];
        self.stream.read_exact(&mut bytes)?;
        let ack: CoreAckV1 = serde_json::from_slice(&bytes)?;
        ack.validate()?;
        if (ack.status == AckStatus::Accepted || ack.message_id.is_some())
            && ack.message_id.as_ref() != Some(&envelope.message_id)
        {
            return Err(ClientError::AckMismatch);
        }
        if ack.status == AckStatus::Accepted
            && !ack_disposition_matches(&envelope.message, ack.disposition)
        {
            return Err(ClientError::AckDispositionMismatch);
        }
        Ok(ack)
    }
}

fn ack_disposition_matches(
    message: &IngressMessageV1,
    disposition: Option<CoreAckDisposition>,
) -> bool {
    match message {
        IngressMessageV1::QuoteBatch(_) => matches!(
            disposition,
            Some(
                CoreAckDisposition::Applied
                    | CoreAckDisposition::DuplicateBatch
                    | CoreAckDisposition::StaleBatch
                    | CoreAckDisposition::DuplicateIngress
            )
        ),
        IngressMessageV1::Evaluate(_) => matches!(
            disposition,
            Some(CoreAckDisposition::DecisionAccepted | CoreAckDisposition::DuplicateIngress)
        ),
        IngressMessageV1::ResearchSignals(_) => matches!(
            disposition,
            Some(
                CoreAckDisposition::ResearchUpdated
                    | CoreAckDisposition::ResearchUnchanged
                    | CoreAckDisposition::ResearchStale
                    | CoreAckDisposition::DuplicateIngress
            )
        ),
        IngressMessageV1::DeskMapProjection(_) => matches!(
            disposition,
            Some(
                CoreAckDisposition::DeskMapUpdated
                    | CoreAckDisposition::DeskMapUnchanged
                    | CoreAckDisposition::DeskMapStale
                    | CoreAckDisposition::DuplicateIngress
            )
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;
    use std::thread;

    use chrono::{TimeZone as _, Utc};
    use spx_domain::{
        AuthenticationState, CoreAckDisposition, CoreAckV1, EntitlementState,
        INGRESS_SCHEMA_VERSION, IngressMessageV1, OperationalState, PROVIDER_STATE_SCHEMA_VERSION,
        Provider, ProviderReasonCode, ProviderStateV1, QUOTE_BATCH_SCHEMA_VERSION, QuoteBatchMode,
        QuoteBatchV1, Token, TransportState,
    };
    use tempfile::TempDir;

    use super::*;

    fn envelope() -> IngressEnvelopeV1 {
        let at = Utc.with_ymd_and_hms(2026, 8, 1, 14, 30, 0).unwrap();
        IngressEnvelopeV1 {
            schema_version: INGRESS_SCHEMA_VERSION.to_owned(),
            message_id: Token::new("message:test", "message_id").unwrap(),
            emitted_at: at,
            message: IngressMessageV1::QuoteBatch(QuoteBatchV1 {
                schema_version: QUOTE_BATCH_SCHEMA_VERSION.to_owned(),
                batch_id: Token::new("batch:test", "batch_id").unwrap(),
                provider: Provider::Schwab,
                mode: QuoteBatchMode::ReplaceProviderSnapshot,
                sequence: 1,
                received_at: at,
                provider_state: ProviderStateV1 {
                    schema_version: PROVIDER_STATE_SCHEMA_VERSION.to_owned(),
                    provider: Provider::Schwab,
                    observed_at: at,
                    operational: OperationalState::Degraded,
                    transport: TransportState::Connected,
                    authentication: AuthenticationState::Authenticated,
                    entitlement: EntitlementState::Missing,
                    reason_codes: vec![ProviderReasonCode::EntitlementUnavailable],
                    latency_ms: None,
                    connection_generation: 1,
                },
                quotes: vec![],
            }),
        }
    }

    #[test]
    fn client_requires_matching_typed_ack() {
        let temp = TempDir::new().unwrap();
        let socket = temp.path().join("core.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).unwrap();
            let mut payload = vec![0_u8; u32::from_be_bytes(length) as usize];
            stream.read_exact(&mut payload).unwrap();
            let request: IngressEnvelopeV1 = serde_json::from_slice(&payload).unwrap();
            let ack = CoreAckV1::accepted(request.message_id, CoreAckDisposition::Applied, None);
            let bytes = serde_json::to_vec(&ack).unwrap();
            stream
                .write_all(&u32::try_from(bytes.len()).unwrap().to_be_bytes())
                .unwrap();
            stream.write_all(&bytes).unwrap();
        });
        let mut client = CoreClient::connect(&socket, Duration::from_secs(1), 1_048_576).unwrap();
        let ack = client.send(&envelope()).unwrap();
        assert_eq!(ack.disposition, Some(CoreAckDisposition::Applied));
        server.join().unwrap();
    }

    #[test]
    fn quote_batch_rejects_decision_ack_disposition() {
        assert!(!ack_disposition_matches(
            &envelope().message,
            Some(CoreAckDisposition::DecisionAccepted)
        ));
        assert!(ack_disposition_matches(
            &envelope().message,
            Some(CoreAckDisposition::DuplicateIngress)
        ));
    }

    #[test]
    fn only_local_preflight_failures_are_permanent() {
        assert!(ClientError::FrameTooLarge.is_preflight_failure());
        assert!(
            ClientError::EnvelopeContract(spx_domain::DomainError::Invalid {
                field: "test",
                reason: "invalid",
            })
            .is_preflight_failure()
        );
        assert!(!ClientError::AckMismatch.is_preflight_failure());
        assert!(!ClientError::InvalidAckFrame.is_preflight_failure());
    }
}
