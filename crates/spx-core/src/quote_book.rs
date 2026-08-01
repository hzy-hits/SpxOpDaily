use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use spx_domain::{
    ANALYTICAL_SNAPSHOT_SCHEMA_VERSION, AnalyticalOptionSnapshotV1, DomainError, MarketSession,
    Provider, ProviderStateV1, QuoteBatchMode, QuoteBatchV1, QuoteV1, Token, Validate,
    canonical_json_hash,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QuoteBookError {
    #[error("domain contract error: {0}")]
    Domain(#[from] DomainError),
    #[error("provider sequence collision for {provider:?} sequence {sequence}")]
    SequenceCollision { provider: Provider, sequence: u64 },
    #[error("batch identity collision for {0}")]
    BatchIdentityCollision(Token),
    #[error("cannot build a snapshot without an accepted batch")]
    Empty,
    #[error("invalid quote-book configuration: {0}")]
    InvalidConfiguration(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyBatch {
    Applied,
    Duplicate,
    Stale,
}

#[derive(Debug, Clone)]
struct Cursor {
    connection_generation: u64,
    sequence: u64,
    batch_id: Token,
    payload_sha256: String,
}

#[derive(Debug, Clone)]
struct StoredQuote {
    connection_generation: u64,
    source_batch_id: Token,
    accepted_at: DateTime<Utc>,
    quote: QuoteV1,
}

#[derive(Debug, Clone)]
struct SeenBatch {
    payload_sha256: String,
    accepted_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct QuoteBook {
    cursors: BTreeMap<Provider, Cursor>,
    provider_states: BTreeMap<Provider, ProviderStateV1>,
    quotes: BTreeMap<(Provider, MarketSession, String), StoredQuote>,
    seen_batches: BTreeMap<Token, SeenBatch>,
    watermark: Option<DateTime<Utc>>,
    retention: TimeDelta,
    max_quote_entries: usize,
    max_batch_identity_entries: usize,
}

impl Default for QuoteBook {
    fn default() -> Self {
        Self::new(300, 4096, 4096).expect("default quote-book bounds are valid")
    }
}

impl QuoteBook {
    /// Creates a bounded hot quote book. Historical frames remain in the raw
    /// log and are intentionally not retained in this live decision structure.
    ///
    /// # Errors
    ///
    /// Returns an error when retention or either entry limit is zero or cannot
    /// be represented by `chrono`.
    pub fn new(
        retention_seconds: u32,
        max_quote_entries: usize,
        max_batch_identity_entries: usize,
    ) -> Result<Self, QuoteBookError> {
        if retention_seconds == 0 {
            return Err(QuoteBookError::InvalidConfiguration(
                "retention_seconds must be positive",
            ));
        }
        let retention = TimeDelta::try_seconds(i64::from(retention_seconds)).ok_or(
            QuoteBookError::InvalidConfiguration("retention_seconds is not representable"),
        )?;
        if max_quote_entries == 0 {
            return Err(QuoteBookError::InvalidConfiguration(
                "max_quote_entries must be positive",
            ));
        }
        if max_batch_identity_entries == 0 {
            return Err(QuoteBookError::InvalidConfiguration(
                "max_batch_identity_entries must be positive",
            ));
        }
        Ok(Self {
            cursors: BTreeMap::new(),
            provider_states: BTreeMap::new(),
            quotes: BTreeMap::new(),
            seen_batches: BTreeMap::new(),
            watermark: None,
            retention,
            max_quote_entries,
            max_batch_identity_entries,
        })
    }

    /// Applies a validated provider batch with generation, sequence, and identity fencing.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid contract or conflicting sequence/batch identity.
    pub fn apply(&mut self, batch: QuoteBatchV1) -> Result<ApplyBatch, QuoteBookError> {
        batch.validate()?;
        let payload_sha256 = canonical_json_hash(&batch)?;
        let received_at = batch.received_at;
        let candidate_watermark = self
            .watermark
            .map_or(received_at, |current| current.max(received_at));
        let candidate_cutoff = candidate_watermark - self.retention;
        if let Some(seen) = self
            .seen_batches
            .get(&batch.batch_id)
            .filter(|seen| seen.accepted_at >= candidate_cutoff)
        {
            return if seen.payload_sha256 == payload_sha256 {
                Ok(ApplyBatch::Duplicate)
            } else {
                Err(QuoteBookError::BatchIdentityCollision(batch.batch_id))
            };
        }
        let connection_generation = batch.provider_state.connection_generation;
        let provider_live = batch.provider_state.is_live();
        let replaces_provider_snapshot = batch.mode == QuoteBatchMode::ReplaceProviderSnapshot;
        let mut generation_advanced = false;
        if let Some(cursor) = self.cursors.get(&batch.provider) {
            if connection_generation < cursor.connection_generation {
                return Ok(ApplyBatch::Stale);
            }
            if connection_generation == cursor.connection_generation {
                if batch.sequence < cursor.sequence {
                    return Ok(ApplyBatch::Stale);
                }
                if batch.sequence == cursor.sequence {
                    if batch.batch_id == cursor.batch_id {
                        return if payload_sha256 == cursor.payload_sha256 {
                            Ok(ApplyBatch::Duplicate)
                        } else {
                            Err(QuoteBookError::BatchIdentityCollision(batch.batch_id))
                        };
                    }
                    return Err(QuoteBookError::SequenceCollision {
                        provider: batch.provider,
                        sequence: batch.sequence,
                    });
                }
            } else {
                generation_advanced = true;
            }
        }
        self.advance_watermark_and_prune(received_at);
        if generation_advanced || !provider_live || replaces_provider_snapshot {
            self.quotes
                .retain(|(provider, _, _), _| *provider != batch.provider);
        }
        if provider_live {
            for quote in batch.quotes {
                let identity = quote
                    .exact_contract_id()
                    .unwrap_or(&quote.instrument_id)
                    .as_str()
                    .to_owned();
                self.quotes.insert(
                    (batch.provider, quote.market_session, identity),
                    StoredQuote {
                        connection_generation,
                        source_batch_id: batch.batch_id.clone(),
                        accepted_at: received_at,
                        quote,
                    },
                );
            }
        }
        self.provider_states
            .insert(batch.provider, batch.provider_state);
        let batch_id = batch.batch_id;
        self.cursors.insert(
            batch.provider,
            Cursor {
                connection_generation,
                sequence: batch.sequence,
                batch_id: batch_id.clone(),
                payload_sha256: payload_sha256.clone(),
            },
        );
        self.seen_batches.insert(
            batch_id,
            SeenBatch {
                payload_sha256,
                accepted_at: received_at,
            },
        );
        self.enforce_entry_limits();
        Ok(ApplyBatch::Applied)
    }

    fn advance_watermark_and_prune(&mut self, received_at: DateTime<Utc>) {
        let watermark = self
            .watermark
            .map_or(received_at, |current| current.max(received_at));
        self.watermark = Some(watermark);
        let cutoff = watermark - self.retention;
        self.quotes.retain(|_, quote| quote.accepted_at >= cutoff);
        self.seen_batches
            .retain(|_, batch| batch.accepted_at >= cutoff);
    }

    fn enforce_entry_limits(&mut self) {
        let quote_excess = self.quotes.len().saturating_sub(self.max_quote_entries);
        if quote_excess > 0 {
            let mut oldest: Vec<_> = self
                .quotes
                .iter()
                .map(|(key, quote)| (quote.accepted_at, key.clone()))
                .collect();
            oldest.sort_unstable();
            for (_, key) in oldest.into_iter().take(quote_excess) {
                self.quotes.remove(&key);
            }
        }
        let batch_excess = self
            .seen_batches
            .len()
            .saturating_sub(self.max_batch_identity_entries);
        if batch_excess > 0 {
            let mut oldest: Vec<_> = self
                .seen_batches
                .iter()
                .map(|(batch_id, batch)| (batch.accepted_at, batch_id.clone()))
                .collect();
            oldest.sort_unstable();
            for (_, batch_id) in oldest.into_iter().take(batch_excess) {
                self.seen_batches.remove(&batch_id);
            }
        }
    }

    /// Builds one deterministic analytical snapshot from the latest accepted provider state.
    ///
    /// # Errors
    ///
    /// Returns an error when no batch exists or the derived contract cannot be hashed/validated.
    pub fn snapshot(
        &self,
        session: MarketSession,
        built_at: DateTime<Utc>,
    ) -> Result<AnalyticalOptionSnapshotV1, QuoteBookError> {
        if self.cursors.is_empty() {
            return Err(QuoteBookError::Empty);
        }
        let mut source_batch_ids: Vec<Token> = self
            .cursors
            .values()
            .map(|cursor| cursor.batch_id.clone())
            .collect();
        let provider_states: Vec<ProviderStateV1> =
            self.provider_states.values().cloned().collect();
        let stored_quotes: Vec<&StoredQuote> = self
            .quotes
            .values()
            .filter(|stored| stored.quote.market_session == session)
            .filter(|stored| {
                self.provider_states
                    .get(&stored.quote.provider)
                    .is_some_and(|state| {
                        state.connection_generation == stored.connection_generation
                    })
            })
            .collect();
        for stored in &stored_quotes {
            if !source_batch_ids.contains(&stored.source_batch_id) {
                source_batch_ids.push(stored.source_batch_id.clone());
            }
        }
        let quotes: Vec<QuoteV1> = stored_quotes
            .into_iter()
            .map(|stored| stored.quote.clone())
            .collect();
        let provenance_hash = canonical_json_hash(&(
            session,
            built_at,
            &source_batch_ids,
            &provider_states,
            &quotes,
        ))?;
        let snapshot = AnalyticalOptionSnapshotV1 {
            schema_version: ANALYTICAL_SNAPSHOT_SCHEMA_VERSION.to_owned(),
            snapshot_id: Token::new(
                format!("snapshot:{}", &provenance_hash[..24]),
                "snapshot_id",
            )?,
            session,
            built_at,
            source_batch_ids,
            provider_states,
            quotes,
            provenance_hash: Token::new(provenance_hash, "provenance_hash")?,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}
