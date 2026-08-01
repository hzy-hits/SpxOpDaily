use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use chrono::Utc;
use spx_domain::{CoreAckDisposition, CoreAckReason, CoreAckV1, IngressEnvelopeV1, Token};
use thiserror::Error;
use tracing::{error, info};

use crate::{CoreEngine, CoreOutcome};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("Unix socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("refusing to replace non-socket path")]
    UnsafeSocketPath,
    #[error("engine mutex poisoned")]
    Poisoned,
    #[error("core engine failed: {0}")]
    Engine(#[from] crate::CoreError),
}

/// Serves strict length-prefixed ingress frames over a permission-restricted Unix socket.
///
/// # Errors
///
/// Returns an error for unsafe socket paths, poisoned runtime state, or socket I/O failure.
pub fn serve_unix(
    engine: CoreEngine,
    socket_path: impl AsRef<Path>,
    max_frame_bytes: usize,
    max_connections: usize,
    stop: &Arc<AtomicBool>,
) -> Result<(), ServerError> {
    let socket_path = socket_path.as_ref();
    prepare_socket(socket_path)?;
    let listener = UnixListener::bind(socket_path)?;
    let _socket_guard = SocketGuard::new(socket_path);
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    let engine = Arc::new(Mutex::new(engine));
    let active = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    let mut fatal_error = None;
    info!(path = %socket_path.display(), "spx-core listening");
    while !stop.load(Ordering::Relaxed) {
        reap_finished(&mut handles);
        engine
            .lock()
            .map_err(|_| ServerError::Poisoned)?
            .heartbeat(Utc::now())?;
        match listener.accept() {
            Ok((mut stream, _)) => {
                let Some(permit) = ConnectionPermit::acquire(&active, max_connections) else {
                    let rejection = stream
                        .set_write_timeout(Some(Duration::from_secs(1)))
                        .and_then(|()| {
                            write_ack(
                                &mut stream,
                                &CoreAckV1::rejected(None, CoreAckReason::ServerBusy),
                            )
                        });
                    if let Err(failure) = rejection {
                        error!(error = %failure, "failed to reject excess ingress connection");
                    }
                    continue;
                };
                let engine = Arc::clone(&engine);
                let connection_stop = Arc::clone(stop);
                match thread::Builder::new()
                    .name("spx-ingress".to_owned())
                    .spawn(move || {
                        let _permit = permit;
                        if let Err(failure) =
                            handle_connection(stream, &engine, max_frame_bytes, &connection_stop)
                        {
                            error!(error = %failure, "ingress connection failed");
                        }
                    }) {
                    Ok(handle) => handles.push(handle),
                    Err(failure) => {
                        fatal_error = Some(failure);
                        break;
                    }
                }
            }
            Err(failure) if failure.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(failure) if failure.kind() == ErrorKind::Interrupted => {}
            Err(failure) => {
                fatal_error = Some(failure);
                break;
            }
        }
    }
    stop.store(true, Ordering::Release);
    drop(listener);
    for handle in handles {
        if handle.join().is_err() {
            error!("ingress thread panicked during shutdown");
        }
    }
    if let Some(failure) = fatal_error {
        Err(failure.into())
    } else {
        Ok(())
    }
}

struct SocketGuard<'a> {
    path: &'a Path,
}

impl<'a> SocketGuard<'a> {
    const fn new(path: &'a Path) -> Self {
        Self { path }
    }
}

impl Drop for SocketGuard<'_> {
    fn drop(&mut self) {
        if fs::symlink_metadata(self.path).is_ok_and(|metadata| metadata.file_type().is_socket()) {
            let _ = fs::remove_file(self.path);
        }
    }
}

struct ConnectionPermit {
    active: Arc<AtomicUsize>,
}

impl ConnectionPermit {
    fn acquire(active: &Arc<AtomicUsize>, maximum: usize) -> Option<Self> {
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < maximum).then_some(current + 1)
            })
            .ok()?;
        Some(Self {
            active: Arc::clone(active),
        })
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

fn reap_finished(handles: &mut Vec<thread::JoinHandle<()>>) {
    let mut index = 0;
    while index < handles.len() {
        if handles[index].is_finished() {
            let handle = handles.swap_remove(index);
            if handle.join().is_err() {
                error!("ingress thread panicked");
            }
        } else {
            index += 1;
        }
    }
}

fn prepare_socket(path: &Path) -> Result<(), ServerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => fs::remove_file(path)?,
        Ok(_) => return Err(ServerError::UnsafeSocketPath),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn handle_connection(
    mut stream: UnixStream,
    engine: &Arc<Mutex<CoreEngine>>,
    max_frame_bytes: usize,
    stop: &AtomicBool,
) -> Result<(), ServerError> {
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    while !stop.load(Ordering::Relaxed) {
        let mut length = [0_u8; 4];
        if !read_exact_until_stopped(&mut stream, &mut length, stop)? {
            return Ok(());
        }
        let length = usize::try_from(u32::from_be_bytes(length))
            .map_err(|_| std::io::Error::new(ErrorKind::InvalidData, "frame size overflow"))?;
        if length == 0 || length > max_frame_bytes {
            write_ack(
                &mut stream,
                &CoreAckV1::rejected(None, CoreAckReason::InvalidFrameSize),
            )?;
            return Ok(());
        }
        let mut payload = vec![0_u8; length];
        if !read_exact_until_stopped(&mut stream, &mut payload, stop)? {
            return Ok(());
        }
        let Ok(envelope) = serde_json::from_slice::<IngressEnvelopeV1>(&payload) else {
            write_ack(
                &mut stream,
                &CoreAckV1::rejected(None, CoreAckReason::InvalidContractJson),
            )?;
            continue;
        };
        let message_id = envelope.message_id.clone();
        let outcome = engine
            .lock()
            .map_err(|_| ServerError::Poisoned)?
            .process(envelope, Utc::now());
        let ack = match outcome {
            Ok(outcome) => accepted_ack(message_id, &outcome),
            Err(failure) => {
                error!(error = %failure, "ingress message rejected");
                CoreAckV1::rejected(Some(message_id), CoreAckReason::ProcessingRejected)
            }
        };
        write_ack(&mut stream, &ack)?;
    }
    Ok(())
}

fn read_exact_until_stopped(
    stream: &mut UnixStream,
    buffer: &mut [u8],
    stop: &AtomicBool,
) -> Result<bool, std::io::Error> {
    let mut offset = 0;
    while offset < buffer.len() {
        match stream.read(&mut buffer[offset..]) {
            Ok(0) if offset == 0 => return Ok(false),
            Ok(0) => return Err(std::io::Error::from(ErrorKind::UnexpectedEof)),
            Ok(read) => offset += read,
            Err(failure)
                if matches!(failure.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                if stop.load(Ordering::Relaxed) {
                    return Ok(false);
                }
            }
            Err(failure) if failure.kind() == ErrorKind::Interrupted => {}
            Err(failure) => return Err(failure),
        }
    }
    Ok(true)
}

fn accepted_ack(message_id: Token, outcome: &CoreOutcome) -> CoreAckV1 {
    let (disposition, decision_id) = match outcome {
        CoreOutcome::Duplicate { .. } => (CoreAckDisposition::DuplicateIngress, None),
        CoreOutcome::QuoteBatch { disposition, .. } => (
            match disposition {
                crate::QuoteDisposition::Applied => CoreAckDisposition::Applied,
                crate::QuoteDisposition::DuplicateBatch => CoreAckDisposition::DuplicateBatch,
                crate::QuoteDisposition::StaleBatch => CoreAckDisposition::StaleBatch,
            },
            None,
        ),
        CoreOutcome::Decision { decision, .. } => (
            CoreAckDisposition::DecisionAccepted,
            Some(decision.decision_id.clone()),
        ),
    };
    CoreAckV1::accepted(message_id, disposition, decision_id)
}

fn write_ack(stream: &mut UnixStream, ack: &CoreAckV1) -> Result<(), std::io::Error> {
    let encoded = serde_json::to_vec(ack).map_err(std::io::Error::other)?;
    let length = u32::try_from(encoded.len())
        .map_err(|_| std::io::Error::new(ErrorKind::InvalidData, "ack too large"))?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&encoded)?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use spx_domain::{CoreAckDisposition, StrategyDecisionV1, Validate};

    use crate::{PersistDisposition, QuoteDisposition};

    #[test]
    fn connection_permits_enforce_and_release_capacity() {
        let active = Arc::new(AtomicUsize::new(0));
        let first = ConnectionPermit::acquire(&active, 1).unwrap();
        assert!(ConnectionPermit::acquire(&active, 1).is_none());
        drop(first);
        assert!(ConnectionPermit::acquire(&active, 1).is_some());
    }

    #[test]
    fn quote_and_duplicate_outcomes_have_typed_bridge_dispositions() {
        let message_id = Token::new("message:ack", "message_id").unwrap();
        for (quote_disposition, expected) in [
            (QuoteDisposition::Applied, CoreAckDisposition::Applied),
            (
                QuoteDisposition::DuplicateBatch,
                CoreAckDisposition::DuplicateBatch,
            ),
            (QuoteDisposition::StaleBatch, CoreAckDisposition::StaleBatch),
        ] {
            let outcome = CoreOutcome::QuoteBatch {
                message_id: message_id.clone(),
                disposition: quote_disposition,
            };
            let ack = accepted_ack(message_id.clone(), &outcome);
            ack.validate().unwrap();
            assert_eq!(ack.disposition, Some(expected));
            let decoded: CoreAckV1 =
                serde_json::from_slice(&serde_json::to_vec(&ack).unwrap()).unwrap();
            assert_eq!(decoded, ack);
        }

        let duplicate = CoreOutcome::Duplicate {
            message_id: message_id.clone(),
        };
        assert_eq!(
            accepted_ack(message_id, &duplicate).disposition,
            Some(CoreAckDisposition::DuplicateIngress)
        );
    }

    #[test]
    fn accepted_decision_ack_contains_decision_id() {
        let decision: StrategyDecisionV1 = serde_json::from_str(include_str!(
            "../../../fixtures/domain/v1/strategy_decision_no_trade.json"
        ))
        .unwrap();
        let expected_decision_id = decision.decision_id.clone();
        let message_id = Token::new("message:decision-ack", "message_id").unwrap();
        let outcome = CoreOutcome::Decision {
            message_id: message_id.clone(),
            decision: Box::new(decision),
            notification_enqueued: false,
            persist_disposition: PersistDisposition::Inserted,
        };

        let ack = accepted_ack(message_id, &outcome);
        ack.validate().unwrap();
        assert_eq!(ack.disposition, Some(CoreAckDisposition::DecisionAccepted));
        assert_eq!(ack.decision_id, Some(expected_decision_id));
    }
}
