use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use chrono::{DateTime, NaiveDate, Utc};
use rustix::fs::{FlockOperation, Mode, OFlags, flock, open};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use spx_domain::{IngressEnvelopeV1, Validate};
use thiserror::Error;
use uuid::Uuid;

use crate::raw_log::{date_lock_path, parse_segment_name};

pub const FRAME_ARCHIVE_SCHEMA_VERSION: &str = "spx_raw_frame_archive.v1";
const ARCHIVE_FILE_NAME: &str = "frames.ndjson.zst";
const MANIFEST_FILE_NAME: &str = "manifest.json";
const DIRECTORY_LOCK_FILE: &str = ".spx-raw-log.lock";
const ZSTD_LEVEL: i32 = 1;

#[derive(Debug, Error)]
pub enum FrameArchiveError {
    #[error("frame archive I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame archive JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsafe raw frame directory: {0}")]
    UnsafeRawDirectory(PathBuf),
    #[error("unsafe frame archive root: {0}")]
    UnsafeArchiveRoot(PathBuf),
    #[error("unsafe raw frame segment: {0}")]
    UnsafeSegment(PathBuf),
    #[error("UTC archive date {requested} must be before current UTC date {current}")]
    DateNotCompleted {
        requested: NaiveDate,
        current: NaiveDate,
    },
    #[error("frame archive backlog_days must be within 1..=365, got {0}")]
    InvalidBacklogDays(u32),
    #[error("raw frame source changed while archiving: {0}")]
    SourceChanged(PathBuf),
    #[error("invalid raw frame {segment} record {record}: {reason}")]
    InvalidRecord {
        segment: String,
        record: u64,
        reason: String,
    },
    #[error("frame archive arithmetic overflow")]
    ArithmeticOverflow,
    #[error("existing frame archive is inconsistent: {0}")]
    ExistingArchiveMismatch(String),
    #[error("verified frame archive is unavailable for UTC date {date}: {reason}")]
    ArchiveUnavailable { date: NaiveDate, reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameArchiveStatus {
    Verified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameArchiveReportStatus {
    Created,
    ExistingVerified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameArchiveSource {
    pub name: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub record_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameArchiveManifest {
    pub schema_version: String,
    pub status: FrameArchiveStatus,
    pub utc_date: NaiveDate,
    pub source_segments: Vec<FrameArchiveSource>,
    pub total_source_bytes: u64,
    pub total_record_count: u64,
    pub archive_file: String,
    pub archive_size_bytes: u64,
    pub archive_sha256: String,
    pub compression: String,
    pub compression_level: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FrameArchiveReport {
    pub status: FrameArchiveReportStatus,
    pub archive_root: PathBuf,
    pub archive_dir: PathBuf,
    pub archive_path: PathBuf,
    pub manifest_path: PathBuf,
    pub manifest: FrameArchiveManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FrameArchiveBatchReport {
    pub raw_log_dir: PathBuf,
    pub archive_root: PathBuf,
    pub current_utc_date: NaiveDate,
    pub backlog_days: u32,
    pub candidate_days: usize,
    pub selected_days: Vec<NaiveDate>,
    pub created_days: usize,
    pub existing_verified_days: usize,
    pub total_source_bytes: u64,
    pub total_record_count: u64,
    pub total_archive_bytes: u64,
    pub days: Vec<FrameArchiveReport>,
}

#[derive(Debug)]
struct SourceSegment {
    path: PathBuf,
    name: String,
    index: u32,
}

#[derive(Debug)]
struct ValidatedRecords {
    size_bytes: u64,
    sha256: String,
    record_count: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRecord {
    observed_at: DateTime<Utc>,
    payload_sha256: String,
    payload: Box<RawValue>,
}

/// Archives every strict segment from one completed UTC day into one verified,
/// immutable zstd stream.
///
/// Existing output is accepted only after both the current sources and the
/// archived stream match the stored manifest. An inconsistent artifact is
/// never overwritten.
///
/// # Errors
///
/// Returns an error for an incomplete date, unsafe path, invalid record,
/// changed source, inconsistent existing artifact, or filesystem failure.
pub fn archive_completed_utc_day(
    raw_log_dir: impl AsRef<Path>,
    archive_root: impl AsRef<Path>,
    utc_date: NaiveDate,
    now: DateTime<Utc>,
) -> Result<FrameArchiveReport, FrameArchiveError> {
    let current_date = now.date_naive();
    if utc_date >= current_date {
        return Err(FrameArchiveError::DateNotCompleted {
            requested: utc_date,
            current: current_date,
        });
    }
    let raw_log_dir =
        validate_existing_directory(raw_log_dir.as_ref(), FrameArchiveError::UnsafeRawDirectory)?;
    let archive_root = prepare_archive_root(archive_root.as_ref(), &raw_log_dir)?;
    let _lock = acquire_shared_directory_lock(&raw_log_dir)?;
    let _date_lock = acquire_exclusive_date_lock(&raw_log_dir, utc_date)?;
    let segments = discover_day_segments(&raw_log_dir, utc_date)?;
    let archive_dir = archive_dir(&archive_root, utc_date);
    if archive_dir.exists() {
        return verify_existing_archive(&archive_root, &archive_dir, utc_date, &segments);
    }

    let staging_dir = archive_root.join(format!(
        ".staging-{}-{}",
        utc_date.format("%Y-%m-%d"),
        Uuid::new_v4()
    ));
    fs::create_dir(&staging_dir)?;
    #[cfg(unix)]
    fs::set_permissions(&staging_dir, fs::Permissions::from_mode(0o700))?;
    let result = build_staged_archive(
        &archive_root,
        &archive_dir,
        &staging_dir,
        utc_date,
        now,
        &segments,
    );
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging_dir);
    }
    result
}

/// Archives up to `backlog_days` completed UTC dates present in the raw log.
/// Missing artifacts are selected oldest-first before existing artifacts, so
/// an oversized first-run backlog always advances. Existing artifacts selected
/// into any remaining slots undergo full idempotent verification.
///
/// # Errors
///
/// Returns an error for an unsafe directory, an invalid backlog bound, or the
/// first day that cannot be published or verified. Earlier completed artifacts
/// remain valid and immutable.
pub fn archive_completed_utc_backlog(
    raw_log_dir: impl AsRef<Path>,
    archive_root: impl AsRef<Path>,
    backlog_days: u32,
    now: DateTime<Utc>,
) -> Result<FrameArchiveBatchReport, FrameArchiveError> {
    if !(1..=365).contains(&backlog_days) {
        return Err(FrameArchiveError::InvalidBacklogDays(backlog_days));
    }
    let raw_log_dir =
        validate_existing_directory(raw_log_dir.as_ref(), FrameArchiveError::UnsafeRawDirectory)?;
    let archive_root = prepare_archive_root(archive_root.as_ref(), &raw_log_dir)?;
    let candidate_dates = discover_completed_dates(&raw_log_dir, now.date_naive())?;
    let (missing, existing): (Vec<_>, Vec<_>) = candidate_dates
        .iter()
        .copied()
        .partition(|date| !archive_dir(&archive_root, *date).exists());
    let selected_days: Vec<_> = missing
        .into_iter()
        .chain(existing)
        .take(backlog_days as usize)
        .collect();
    let mut days = Vec::with_capacity(selected_days.len());
    for utc_date in &selected_days {
        days.push(archive_completed_utc_day(
            &raw_log_dir,
            &archive_root,
            *utc_date,
            now,
        )?);
    }
    let created_days = days
        .iter()
        .filter(|report| report.status == FrameArchiveReportStatus::Created)
        .count();
    let existing_verified_days = days.len().saturating_sub(created_days);
    let total_source_bytes =
        checked_sum(days.iter().map(|report| report.manifest.total_source_bytes))?;
    let total_record_count =
        checked_sum(days.iter().map(|report| report.manifest.total_record_count))?;
    let total_archive_bytes =
        checked_sum(days.iter().map(|report| report.manifest.archive_size_bytes))?;
    Ok(FrameArchiveBatchReport {
        raw_log_dir,
        archive_root,
        current_utc_date: now.date_naive(),
        backlog_days,
        candidate_days: candidate_dates.len(),
        selected_days,
        created_days,
        existing_verified_days,
        total_source_bytes,
        total_record_count,
        total_archive_bytes,
        days,
    })
}

fn build_staged_archive(
    archive_root: &Path,
    archive_dir: &Path,
    staging_dir: &Path,
    utc_date: NaiveDate,
    now: DateTime<Utc>,
    segments: &[SourceSegment],
) -> Result<FrameArchiveReport, FrameArchiveError> {
    let staged_archive = staging_dir.join(ARCHIVE_FILE_NAME);
    let output = create_owner_only_file(&staged_archive)?;
    let mut encoder = zstd::stream::write::Encoder::new(output, ZSTD_LEVEL)?;
    encoder.include_checksum(true)?;
    let mut source_segments = Vec::with_capacity(segments.len());
    for segment in segments {
        let file = open_read_segment(&segment.path)?;
        let before = file.metadata()?;
        let validated = validate_records(file, utc_date, &segment.name, Some(&mut encoder))?;
        let after = fs::symlink_metadata(&segment.path)?;
        if !same_file_metadata(&before, &after) || validated.size_bytes != before.len() {
            return Err(FrameArchiveError::SourceChanged(segment.path.clone()));
        }
        source_segments.push(FrameArchiveSource {
            name: segment.name.clone(),
            size_bytes: validated.size_bytes,
            sha256: validated.sha256,
            record_count: validated.record_count,
        });
    }
    let archive_file = encoder.finish()?;
    archive_file.sync_all()?;
    drop(archive_file);

    let archive_size_bytes = fs::symlink_metadata(&staged_archive)?.len();
    let archive_sha256 = sha256_file(&staged_archive)?;
    let total_source_bytes = checked_sum(source_segments.iter().map(|row| row.size_bytes))?;
    let total_record_count = checked_sum(source_segments.iter().map(|row| row.record_count))?;
    let manifest = FrameArchiveManifest {
        schema_version: FRAME_ARCHIVE_SCHEMA_VERSION.to_owned(),
        status: FrameArchiveStatus::Verified,
        utc_date,
        source_segments,
        total_source_bytes,
        total_record_count,
        archive_file: ARCHIVE_FILE_NAME.to_owned(),
        archive_size_bytes,
        archive_sha256,
        compression: "zstd".to_owned(),
        compression_level: ZSTD_LEVEL,
        created_at: now,
    };
    validate_manifest_contract(&manifest, utc_date)?;
    verify_archive_payload(&staged_archive, &manifest)?;
    write_manifest(&staging_dir.join(MANIFEST_FILE_NAME), &manifest)?;
    sync_directory(staging_dir)?;

    match fs::rename(staging_dir, archive_dir) {
        Ok(()) => {
            sync_directory(archive_root)?;
            Ok(report(
                FrameArchiveReportStatus::Created,
                archive_root,
                archive_dir,
                manifest,
            ))
        }
        Err(error) if archive_dir.exists() => {
            let _ = fs::remove_dir_all(staging_dir);
            verify_existing_archive(archive_root, archive_dir, utc_date, segments).map_err(
                |failure| {
                    FrameArchiveError::ExistingArchiveMismatch(format!(
                        "concurrent publish failed ({error}); {failure}"
                    ))
                },
            )
        }
        Err(error) => Err(error.into()),
    }
}

fn verify_existing_archive(
    archive_root: &Path,
    archive_dir: &Path,
    utc_date: NaiveDate,
    segments: &[SourceSegment],
) -> Result<FrameArchiveReport, FrameArchiveError> {
    let manifest = load_verified_manifest(archive_root, utc_date)?;
    for segment in segments {
        verify_source_against_manifest(segment, &manifest)?;
    }
    Ok(report(
        FrameArchiveReportStatus::ExistingVerified,
        archive_root,
        archive_dir,
        manifest,
    ))
}

fn report(
    status: FrameArchiveReportStatus,
    archive_root: &Path,
    archive_dir: &Path,
    manifest: FrameArchiveManifest,
) -> FrameArchiveReport {
    FrameArchiveReport {
        status,
        archive_root: archive_root.to_path_buf(),
        archive_dir: archive_dir.to_path_buf(),
        archive_path: archive_dir.join(ARCHIVE_FILE_NAME),
        manifest_path: archive_dir.join(MANIFEST_FILE_NAME),
        manifest,
    }
}

pub(crate) fn load_verified_manifest(
    archive_root: &Path,
    utc_date: NaiveDate,
) -> Result<FrameArchiveManifest, FrameArchiveError> {
    let manifest = load_archive_barrier_manifest(archive_root, utc_date)?;
    let archive_path = archive_dir(archive_root, utc_date).join(ARCHIVE_FILE_NAME);
    verify_archive_payload(&archive_path, &manifest).map_err(|error| {
        FrameArchiveError::ArchiveUnavailable {
            date: utc_date,
            reason: error.to_string(),
        }
    })?;
    Ok(manifest)
}

pub(crate) fn load_archive_barrier_manifest(
    archive_root: &Path,
    utc_date: NaiveDate,
) -> Result<FrameArchiveManifest, FrameArchiveError> {
    let archive_root =
        validate_existing_directory(archive_root, FrameArchiveError::UnsafeArchiveRoot)?;
    let directory = archive_dir(&archive_root, utc_date);
    let metadata = fs::symlink_metadata(&directory).map_err(|error| {
        FrameArchiveError::ArchiveUnavailable {
            date: utc_date,
            reason: error.to_string(),
        }
    })?;
    if !metadata.file_type().is_dir() {
        return Err(FrameArchiveError::ArchiveUnavailable {
            date: utc_date,
            reason: "archive date path is not a directory".to_owned(),
        });
    }
    let manifest_path = directory.join(MANIFEST_FILE_NAME);
    let manifest_file = open_read_regular(&manifest_path).map_err(|error| {
        FrameArchiveError::ArchiveUnavailable {
            date: utc_date,
            reason: error.to_string(),
        }
    })?;
    let manifest: FrameArchiveManifest =
        serde_json::from_reader(manifest_file).map_err(|error| {
            FrameArchiveError::ArchiveUnavailable {
                date: utc_date,
                reason: error.to_string(),
            }
        })?;
    validate_manifest_contract(&manifest, utc_date).map_err(|error| {
        FrameArchiveError::ArchiveUnavailable {
            date: utc_date,
            reason: error.to_string(),
        }
    })?;
    let archive_path = directory.join(ARCHIVE_FILE_NAME);
    let archive_metadata = fs::symlink_metadata(&archive_path).map_err(|error| {
        FrameArchiveError::ArchiveUnavailable {
            date: utc_date,
            reason: error.to_string(),
        }
    })?;
    if !archive_metadata.file_type().is_file()
        || archive_metadata.len() != manifest.archive_size_bytes
        || sha256_file(&archive_path)? != manifest.archive_sha256
    {
        return Err(FrameArchiveError::ArchiveUnavailable {
            date: utc_date,
            reason: "archive size or SHA-256 does not match manifest".to_owned(),
        });
    }
    Ok(manifest)
}

pub(crate) fn verify_source_path(
    path: &Path,
    expected_size: u64,
    manifest: &FrameArchiveManifest,
) -> Result<(), FrameArchiveError> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| FrameArchiveError::UnsafeSegment(path.to_path_buf()))?;
    let (_, index) = parse_segment_name(name)
        .filter(|(date, _)| *date == manifest.utc_date)
        .ok_or_else(|| FrameArchiveError::UnsafeSegment(path.to_path_buf()))?;
    let segment = SourceSegment {
        path: path.to_path_buf(),
        name: name.to_owned(),
        index,
    };
    let source = manifest
        .source_segments
        .iter()
        .find(|row| row.name == name)
        .ok_or_else(|| {
            FrameArchiveError::ExistingArchiveMismatch(format!(
                "segment {name} is absent from verified manifest"
            ))
        })?;
    if source.size_bytes != expected_size {
        return Err(FrameArchiveError::ExistingArchiveMismatch(format!(
            "segment {name} size changed before prune"
        )));
    }
    verify_source_fingerprint_against_manifest(&segment, manifest)
}

fn verify_source_fingerprint_against_manifest(
    segment: &SourceSegment,
    manifest: &FrameArchiveManifest,
) -> Result<(), FrameArchiveError> {
    let expected = manifest
        .source_segments
        .iter()
        .find(|row| row.name == segment.name)
        .ok_or_else(|| {
            FrameArchiveError::ExistingArchiveMismatch(format!(
                "segment {} is absent from verified manifest",
                segment.name
            ))
        })?;
    let metadata = fs::symlink_metadata(&segment.path)?;
    if !metadata.file_type().is_file() || metadata.len() != expected.size_bytes {
        return Err(FrameArchiveError::ExistingArchiveMismatch(format!(
            "segment {} size or file type changed",
            segment.name
        )));
    }
    let actual = fingerprint_records(open_read_segment(&segment.path)?)?;
    if actual.sha256 != expected.sha256
        || actual.size_bytes != expected.size_bytes
        || actual.record_count != expected.record_count
    {
        return Err(FrameArchiveError::ExistingArchiveMismatch(format!(
            "segment {} content changed",
            segment.name
        )));
    }
    Ok(())
}

fn verify_source_against_manifest(
    segment: &SourceSegment,
    manifest: &FrameArchiveManifest,
) -> Result<(), FrameArchiveError> {
    let expected = manifest
        .source_segments
        .iter()
        .find(|row| row.name == segment.name)
        .ok_or_else(|| {
            FrameArchiveError::ExistingArchiveMismatch(format!(
                "segment {} is absent from verified manifest",
                segment.name
            ))
        })?;
    let metadata = fs::symlink_metadata(&segment.path)?;
    if !metadata.file_type().is_file() || metadata.len() != expected.size_bytes {
        return Err(FrameArchiveError::ExistingArchiveMismatch(format!(
            "segment {} size or file type changed",
            segment.name
        )));
    }
    let file = open_read_segment(&segment.path)?;
    let actual = validate_records(file, manifest.utc_date, &segment.name, None)?;
    if actual.sha256 != expected.sha256
        || actual.size_bytes != expected.size_bytes
        || actual.record_count != expected.record_count
    {
        return Err(FrameArchiveError::ExistingArchiveMismatch(format!(
            "segment {} content changed",
            segment.name
        )));
    }
    Ok(())
}

fn validate_manifest_contract(
    manifest: &FrameArchiveManifest,
    expected_date: NaiveDate,
) -> Result<(), FrameArchiveError> {
    if manifest.schema_version != FRAME_ARCHIVE_SCHEMA_VERSION
        || manifest.status != FrameArchiveStatus::Verified
        || manifest.utc_date != expected_date
        || manifest.archive_file != ARCHIVE_FILE_NAME
        || manifest.compression != "zstd"
        || manifest.compression_level != ZSTD_LEVEL
        || !is_sha256(&manifest.archive_sha256)
    {
        return Err(FrameArchiveError::ExistingArchiveMismatch(
            "manifest header is invalid".to_owned(),
        ));
    }
    let mut names = BTreeSet::new();
    let mut previous_index = None;
    for source in &manifest.source_segments {
        let Some((date, index)) = parse_segment_name(&source.name) else {
            return Err(FrameArchiveError::ExistingArchiveMismatch(
                "manifest contains an invalid segment name".to_owned(),
            ));
        };
        if date != expected_date
            || previous_index.is_some_and(|previous| index <= previous)
            || !names.insert(source.name.clone())
            || !is_sha256(&source.sha256)
        {
            return Err(FrameArchiveError::ExistingArchiveMismatch(
                "manifest source ordering or identity is invalid".to_owned(),
            ));
        }
        previous_index = Some(index);
    }
    if checked_sum(manifest.source_segments.iter().map(|row| row.size_bytes))?
        != manifest.total_source_bytes
        || checked_sum(manifest.source_segments.iter().map(|row| row.record_count))?
            != manifest.total_record_count
    {
        return Err(FrameArchiveError::ExistingArchiveMismatch(
            "manifest totals do not match sources".to_owned(),
        ));
    }
    Ok(())
}

fn verify_archive_payload(
    archive_path: &Path,
    manifest: &FrameArchiveManifest,
) -> Result<(), FrameArchiveError> {
    let file = open_read_regular(archive_path)?;
    let mut decoder = zstd::stream::read::Decoder::new(file)?;
    for source in &manifest.source_segments {
        let limited = (&mut decoder).take(source.size_bytes);
        let actual = validate_records(limited, manifest.utc_date, &source.name, None)?;
        if actual.size_bytes != source.size_bytes
            || actual.sha256 != source.sha256
            || actual.record_count != source.record_count
        {
            return Err(FrameArchiveError::ExistingArchiveMismatch(format!(
                "archived source {} does not match manifest",
                source.name
            )));
        }
    }
    let mut trailing = [0_u8; 1];
    if decoder.read(&mut trailing)? != 0 {
        return Err(FrameArchiveError::ExistingArchiveMismatch(
            "archive contains trailing unmanifested bytes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_records<R: Read>(
    reader: R,
    expected_date: NaiveDate,
    source_name: &str,
    mut sink: Option<&mut dyn Write>,
) -> Result<ValidatedRecords, FrameArchiveError> {
    let mut reader = BufReader::new(reader);
    let mut digest = Sha256::new();
    let mut size_bytes = 0_u64;
    let mut record_count = 0_u64;
    let mut line = Vec::new();
    loop {
        line.clear();
        let bytes = reader.read_until(b'\n', &mut line)?;
        if bytes == 0 {
            break;
        }
        record_count = record_count
            .checked_add(1)
            .ok_or(FrameArchiveError::ArithmeticOverflow)?;
        size_bytes = size_bytes
            .checked_add(u64::try_from(bytes).map_err(|_| FrameArchiveError::ArithmeticOverflow)?)
            .ok_or(FrameArchiveError::ArithmeticOverflow)?;
        if line.last() != Some(&b'\n') {
            return Err(invalid_record(
                source_name,
                record_count,
                "record is not newline terminated",
            ));
        }
        let record: RawRecord = serde_json::from_slice(&line).map_err(|error| {
            invalid_record(source_name, record_count, format!("invalid JSON: {error}"))
        })?;
        if record.observed_at.date_naive() != expected_date {
            return Err(invalid_record(
                source_name,
                record_count,
                format!(
                    "observed_at date {} does not match {expected_date}",
                    record.observed_at.date_naive()
                ),
            ));
        }
        let payload_sha256 = hex::encode(Sha256::digest(record.payload.get().as_bytes()));
        if payload_sha256 != record.payload_sha256 {
            return Err(invalid_record(
                source_name,
                record_count,
                "payload canonical SHA-256 mismatch",
            ));
        }
        let payload: IngressEnvelopeV1 =
            serde_json::from_str(record.payload.get()).map_err(|error| {
                invalid_record(
                    source_name,
                    record_count,
                    format!("invalid ingress payload JSON: {error}"),
                )
            })?;
        payload.validate().map_err(|error| {
            invalid_record(
                source_name,
                record_count,
                format!("invalid ingress payload: {error}"),
            )
        })?;
        digest.update(&line);
        if let Some(writer) = sink.as_deref_mut() {
            writer.write_all(&line)?;
        }
    }
    Ok(ValidatedRecords {
        size_bytes,
        sha256: hex::encode(digest.finalize()),
        record_count,
    })
}

fn fingerprint_records<R: Read>(reader: R) -> Result<ValidatedRecords, FrameArchiveError> {
    let mut reader = BufReader::new(reader);
    let mut digest = Sha256::new();
    let mut size_bytes = 0_u64;
    let mut record_count = 0_u64;
    let mut last_byte = None;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let bytes = reader.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        size_bytes = size_bytes
            .checked_add(u64::try_from(bytes).map_err(|_| FrameArchiveError::ArithmeticOverflow)?)
            .ok_or(FrameArchiveError::ArithmeticOverflow)?;
        let new_records = bytecount::count(&buffer[..bytes], b'\n');
        record_count = record_count
            .checked_add(
                u64::try_from(new_records).map_err(|_| FrameArchiveError::ArithmeticOverflow)?,
            )
            .ok_or(FrameArchiveError::ArithmeticOverflow)?;
        last_byte = buffer.get(bytes - 1).copied();
        digest.update(&buffer[..bytes]);
    }
    if size_bytes > 0 && last_byte != Some(b'\n') {
        return Err(FrameArchiveError::ExistingArchiveMismatch(
            "raw source is not newline terminated".to_owned(),
        ));
    }
    Ok(ValidatedRecords {
        size_bytes,
        sha256: hex::encode(digest.finalize()),
        record_count,
    })
}

fn invalid_record(source: &str, record: u64, reason: impl Into<String>) -> FrameArchiveError {
    FrameArchiveError::InvalidRecord {
        segment: source.to_owned(),
        record,
        reason: reason.into(),
    }
}

fn discover_day_segments(
    raw_log_dir: &Path,
    utc_date: NaiveDate,
) -> Result<Vec<SourceSegment>, FrameArchiveError> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(raw_log_dir)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some((date, index)) = parse_segment_name(&name) else {
            continue;
        };
        if date != utc_date {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.file_type().is_file() {
            return Err(FrameArchiveError::UnsafeSegment(entry.path()));
        }
        segments.push(SourceSegment {
            path: entry.path(),
            name,
            index,
        });
    }
    segments.sort_by_key(|row| row.index);
    Ok(segments)
}

fn discover_completed_dates(
    raw_log_dir: &Path,
    current_utc_date: NaiveDate,
) -> Result<Vec<NaiveDate>, FrameArchiveError> {
    let _lock = acquire_shared_directory_lock(raw_log_dir)?;
    let mut dates = BTreeSet::new();
    for entry in fs::read_dir(raw_log_dir)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some((date, _)) = parse_segment_name(&name) else {
            continue;
        };
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.file_type().is_file() {
            return Err(FrameArchiveError::UnsafeSegment(entry.path()));
        }
        if date < current_utc_date {
            dates.insert(date);
        }
    }
    Ok(dates.into_iter().collect())
}

fn prepare_archive_root(
    archive_root: &Path,
    raw_log_dir: &Path,
) -> Result<PathBuf, FrameArchiveError> {
    if !archive_root.is_absolute() || archive_root.parent().is_none() {
        return Err(FrameArchiveError::UnsafeArchiveRoot(
            archive_root.to_path_buf(),
        ));
    }
    let canonical = match fs::symlink_metadata(archive_root) {
        Ok(metadata) => {
            if !metadata.file_type().is_dir() {
                return Err(FrameArchiveError::UnsafeArchiveRoot(
                    archive_root.to_path_buf(),
                ));
            }
            validate_existing_directory(archive_root, FrameArchiveError::UnsafeArchiveRoot)?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = archive_root
                .parent()
                .ok_or_else(|| FrameArchiveError::UnsafeArchiveRoot(archive_root.to_path_buf()))?;
            let parent = validate_existing_directory(parent, FrameArchiveError::UnsafeArchiveRoot)?;
            let file_name = archive_root
                .file_name()
                .ok_or_else(|| FrameArchiveError::UnsafeArchiveRoot(archive_root.to_path_buf()))?;
            let candidate = parent.join(file_name);
            validate_archive_root_separation(&candidate, raw_log_dir)?;
            fs::create_dir(&candidate)?;
            #[cfg(unix)]
            fs::set_permissions(&candidate, fs::Permissions::from_mode(0o700))?;
            sync_directory(&parent)?;
            validate_existing_directory(&candidate, FrameArchiveError::UnsafeArchiveRoot)?
        }
        Err(error) => return Err(error.into()),
    };
    validate_archive_root_separation(&canonical, raw_log_dir)?;
    #[cfg(unix)]
    fs::set_permissions(&canonical, fs::Permissions::from_mode(0o700))?;
    Ok(canonical)
}

fn validate_archive_root_separation(
    archive_root: &Path,
    raw_log_dir: &Path,
) -> Result<(), FrameArchiveError> {
    if archive_root == raw_log_dir
        || archive_root.starts_with(raw_log_dir)
        || raw_log_dir.starts_with(archive_root)
    {
        return Err(FrameArchiveError::UnsafeArchiveRoot(
            archive_root.to_path_buf(),
        ));
    }
    Ok(())
}

fn validate_existing_directory(
    directory: &Path,
    error: impl FnOnce(PathBuf) -> FrameArchiveError,
) -> Result<PathBuf, FrameArchiveError> {
    if !directory.is_absolute() || directory.parent().is_none() {
        return Err(error(directory.to_path_buf()));
    }
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.file_type().is_dir() {
        return Err(error(directory.to_path_buf()));
    }
    let canonical = fs::canonicalize(directory)?;
    if canonical != directory {
        return Err(error(directory.to_path_buf()));
    }
    Ok(canonical)
}

fn acquire_shared_directory_lock(raw_log_dir: &Path) -> Result<File, FrameArchiveError> {
    let path = raw_log_dir.join(DIRECTORY_LOCK_FILE);
    let descriptor = open(
        &path,
        OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::from_raw_mode(0o600),
    )
    .map_err(std::io::Error::from)?;
    let file = File::from(descriptor);
    if !file.metadata()?.file_type().is_file() {
        return Err(FrameArchiveError::UnsafeSegment(path));
    }
    flock(&file, FlockOperation::LockShared).map_err(std::io::Error::from)?;
    Ok(file)
}

fn acquire_exclusive_date_lock(
    raw_log_dir: &Path,
    utc_date: NaiveDate,
) -> Result<File, FrameArchiveError> {
    let path = date_lock_path(raw_log_dir, &utc_date.format("%Y-%m-%d").to_string());
    let descriptor = open(
        &path,
        OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::from_raw_mode(0o600),
    )
    .map_err(std::io::Error::from)?;
    let file = File::from(descriptor);
    if !file.metadata()?.file_type().is_file() {
        return Err(FrameArchiveError::UnsafeSegment(path));
    }
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    flock(&file, FlockOperation::LockExclusive).map_err(std::io::Error::from)?;
    Ok(file)
}

fn open_read_segment(path: &Path) -> Result<File, FrameArchiveError> {
    open_read_regular(path)
}

fn open_read_regular(path: &Path) -> Result<File, FrameArchiveError> {
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(std::io::Error::from)?;
    let file = File::from(descriptor);
    if !file.metadata()?.file_type().is_file() {
        return Err(FrameArchiveError::UnsafeSegment(path.to_path_buf()));
    }
    Ok(file)
}

fn create_owner_only_file(path: &Path) -> Result<File, FrameArchiveError> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

fn write_manifest(path: &Path, manifest: &FrameArchiveManifest) -> Result<(), FrameArchiveError> {
    let mut file = create_owner_only_file(path)?;
    let mut encoded = serde_json::to_vec_pretty(manifest)?;
    encoded.push(b'\n');
    file.write_all(&encoded)?;
    file.sync_all()?;
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, FrameArchiveError> {
    let mut file = open_read_regular(path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn sync_directory(directory: &Path) -> Result<(), FrameArchiveError> {
    let descriptor = open(
        directory,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(std::io::Error::from)?;
    File::from(descriptor).sync_all()?;
    Ok(())
}

fn archive_dir(root: &Path, utc_date: NaiveDate) -> PathBuf {
    root.join(format!("date={}", utc_date.format("%Y-%m-%d")))
}

fn checked_sum(mut values: impl Iterator<Item = u64>) -> Result<u64, FrameArchiveError> {
    values.try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or(FrameArchiveError::ArithmeticOverflow)
    })
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(unix)]
fn same_file_metadata(before: &fs::Metadata, after: &fs::Metadata) -> bool {
    before.file_type().is_file()
        && after.file_type().is_file()
        && before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
}

#[cfg(not(unix))]
fn same_file_metadata(before: &fs::Metadata, after: &fs::Metadata) -> bool {
    before.file_type().is_file()
        && after.file_type().is_file()
        && before.len() == after.len()
        && before.modified().ok() == after.modified().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike as _, TimeZone as _};
    use tempfile::TempDir;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 4, 12, 0, 0)
            .single()
            .expect("valid current time")
    }

    fn canonical_subdir(temp: &TempDir, name: &str) -> PathBuf {
        let path = temp.path().join(name);
        fs::create_dir(&path).expect("create directory");
        fs::canonicalize(path).expect("canonical directory")
    }

    fn envelope() -> IngressEnvelopeV1 {
        let batch: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../contracts/golden/domain/v1/quote_batch_schwab_rth.json"
        ))
        .expect("valid quote batch fixture");
        let envelope: IngressEnvelopeV1 = serde_json::from_value(serde_json::json!({
            "schema_version": "spx_ingress.v1",
            "message_id": "message:frame-archive-test",
            "emitted_at": "2026-07-31T14:30:00Z",
            "message": {
                "kind": "quote_batch",
                "payload": batch
            }
        }))
        .expect("valid ingress envelope");
        envelope.validate().expect("valid ingress contract");
        envelope
    }

    fn record_bytes(observed_at: DateTime<Utc>) -> Vec<u8> {
        let payload = envelope();
        let raw_payload = serde_json::to_string(&payload).expect("encode ingress payload");
        let payload_sha256 = hex::encode(Sha256::digest(raw_payload.as_bytes()));
        format!(
            "{{\"observed_at\":{},\"payload_sha256\":{},\"payload\":{raw_payload}}}\n",
            serde_json::to_string(&observed_at).expect("serialize observed_at"),
            serde_json::to_string(&payload_sha256).expect("serialize payload hash"),
        )
        .into_bytes()
    }

    fn reordered_record_bytes(observed_at: DateTime<Utc>) -> Vec<u8> {
        let value = serde_json::to_value(envelope()).expect("serialize ingress envelope");
        let raw_payload = format!(
            "{{\"message\":{},\"emitted_at\":{},\"message_id\":{},\"schema_version\":{}}}",
            serde_json::to_string(&value["message"]).expect("serialize message"),
            serde_json::to_string(&value["emitted_at"]).expect("serialize emitted_at"),
            serde_json::to_string(&value["message_id"]).expect("serialize message_id"),
            serde_json::to_string(&value["schema_version"]).expect("serialize schema_version"),
        );
        let payload_sha256 = hex::encode(Sha256::digest(raw_payload.as_bytes()));
        format!(
            "{{\"observed_at\":{},\"payload_sha256\":{},\"payload\":{raw_payload}}}\n",
            serde_json::to_string(&observed_at).expect("serialize observed_at"),
            serde_json::to_string(&payload_sha256).expect("serialize payload hash"),
        )
        .into_bytes()
    }

    fn write_segment(raw_log_dir: &Path, utc_date: NaiveDate, index: u32, seconds: u32) -> Vec<u8> {
        let observed_at = Utc
            .with_ymd_and_hms(
                utc_date.year(),
                utc_date.month(),
                utc_date.day(),
                1,
                2,
                seconds,
            )
            .single()
            .expect("valid observation time");
        let contents = record_bytes(observed_at);
        fs::write(
            raw_log_dir.join(format!("{utc_date}.{index:04}.ndjson")),
            &contents,
        )
        .expect("write raw segment");
        contents
    }

    #[test]
    fn archive_preserves_ordered_ndjson_and_is_idempotently_verified() {
        let temp = TempDir::new().expect("temporary directory");
        let raw = canonical_subdir(&temp, "frames");
        let archive = canonical_subdir(&temp, "archive");
        let date = NaiveDate::from_ymd_opt(2026, 8, 2).expect("date");
        let second = write_segment(&raw, date, 1, 2);
        let first = write_segment(&raw, date, 0, 1);

        let created = archive_completed_utc_day(&raw, &archive, date, now())
            .expect("create verified archive");
        assert_eq!(created.status, FrameArchiveReportStatus::Created);
        assert_eq!(created.manifest.total_record_count, 2);
        assert_eq!(
            created
                .manifest
                .source_segments
                .iter()
                .map(|source| source.name.as_str())
                .collect::<Vec<_>>(),
            ["2026-08-02.0000.ndjson", "2026-08-02.0001.ndjson"]
        );
        let decompressed =
            zstd::stream::decode_all(File::open(&created.archive_path).expect("open archive"))
                .expect("decompress archive");
        assert_eq!(decompressed, [first, second].concat());

        let verified = archive_completed_utc_day(&raw, &archive, date, now())
            .expect("verify existing archive");
        assert_eq!(verified.status, FrameArchiveReportStatus::ExistingVerified);
        assert_eq!(verified.manifest, created.manifest);

        fs::remove_file(raw.join("2026-08-02.0000.ndjson"))
            .expect("simulate authorized source prune");
        let subset_verified = archive_completed_utc_day(&raw, &archive, date, now())
            .expect("verify remaining source subset");
        assert_eq!(
            subset_verified.status,
            FrameArchiveReportStatus::ExistingVerified
        );
    }

    #[test]
    fn archive_hashes_stored_payload_bytes_before_typed_validation() {
        let temp = TempDir::new().expect("temporary directory");
        let raw = canonical_subdir(&temp, "frames");
        let archive = canonical_subdir(&temp, "archive");
        let date = NaiveDate::from_ymd_opt(2026, 8, 2).expect("date");
        let observed_at = Utc
            .with_ymd_and_hms(2026, 8, 2, 1, 2, 3)
            .single()
            .expect("valid observation time");
        let contents = reordered_record_bytes(observed_at);
        fs::write(raw.join("2026-08-02.0000.ndjson"), &contents)
            .expect("write reordered raw segment");

        let created = archive_completed_utc_day(&raw, &archive, date, now())
            .expect("archive historical encoding");

        assert_eq!(created.manifest.total_record_count, 1);
        let decompressed =
            zstd::stream::decode_all(File::open(created.archive_path).expect("open archive"))
                .expect("decompress archive");
        assert_eq!(decompressed, contents);
    }

    #[test]
    fn archive_rejects_uncompleted_dates_before_publishing() {
        let temp = TempDir::new().expect("temporary directory");
        let raw = canonical_subdir(&temp, "frames");
        let archive = temp.path().join("archive");

        for date in [
            NaiveDate::from_ymd_opt(2026, 8, 4).expect("today"),
            NaiveDate::from_ymd_opt(2026, 8, 5).expect("future"),
        ] {
            assert!(matches!(
                archive_completed_utc_day(&raw, &archive, date, now()),
                Err(FrameArchiveError::DateNotCompleted { .. })
            ));
        }
        assert!(!archive.exists());
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_archive_roots_are_rejected_before_filesystem_mutation() {
        let temp = TempDir::new().expect("temporary directory");
        let root = fs::canonicalize(temp.path()).expect("canonical temporary root");
        let raw = root.join("frames");
        fs::create_dir(&raw).expect("create raw directory");
        let raw = fs::canonicalize(raw).expect("canonical raw directory");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o755))
            .expect("set observable root mode");
        let before_mode = fs::metadata(&root)
            .expect("root metadata")
            .permissions()
            .mode()
            & 0o777;
        let date = NaiveDate::from_ymd_opt(2026, 8, 2).expect("date");

        assert!(matches!(
            archive_completed_utc_day(&raw, &root, date, now()),
            Err(FrameArchiveError::UnsafeArchiveRoot(_))
        ));
        assert_eq!(
            fs::metadata(&root)
                .expect("root metadata after rejection")
                .permissions()
                .mode()
                & 0o777,
            before_mode
        );

        let descendant = raw.join("archive");
        assert!(matches!(
            archive_completed_utc_day(&raw, &descendant, date, now()),
            Err(FrameArchiveError::UnsafeArchiveRoot(_))
        ));
        assert!(!descendant.exists());
    }

    #[test]
    fn archive_rejects_invalid_record_and_removes_staging_output() {
        let temp = TempDir::new().expect("temporary directory");
        let raw = canonical_subdir(&temp, "frames");
        let archive = canonical_subdir(&temp, "archive");
        let date = NaiveDate::from_ymd_opt(2026, 8, 2).expect("date");
        let payload = envelope();
        let mut invalid = serde_json::to_vec(&serde_json::json!({
            "observed_at": "2026-08-02T01:02:03Z",
            "payload_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "payload": payload,
        }))
        .expect("encode invalid record");
        invalid.push(b'\n');
        fs::write(raw.join("2026-08-02.0000.ndjson"), invalid).expect("write invalid segment");

        assert!(matches!(
            archive_completed_utc_day(&raw, &archive, date, now()),
            Err(FrameArchiveError::InvalidRecord { .. })
        ));
        assert!(!archive.join("date=2026-08-02").exists());
        assert!(
            fs::read_dir(&archive)
                .expect("read archive root")
                .all(|entry| !entry
                    .expect("archive entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".staging-"))
        );
    }

    #[test]
    fn existing_archive_mismatch_is_never_overwritten() {
        let temp = TempDir::new().expect("temporary directory");
        let raw = canonical_subdir(&temp, "frames");
        let archive = canonical_subdir(&temp, "archive");
        let date = NaiveDate::from_ymd_opt(2026, 8, 2).expect("date");
        write_segment(&raw, date, 0, 1);
        let created =
            archive_completed_utc_day(&raw, &archive, date, now()).expect("create archive");
        let mut tampered = fs::read(&created.archive_path).expect("read archive");
        tampered[0] ^= 0xff;
        fs::write(&created.archive_path, &tampered).expect("tamper archive");

        assert!(archive_completed_utc_day(&raw, &archive, date, now()).is_err());
        assert_eq!(
            fs::read(&created.archive_path).expect("read unchanged tampered archive"),
            tampered
        );
    }

    #[test]
    fn backlog_archives_oldest_completed_dates_with_a_bounded_batch_report() {
        let temp = TempDir::new().expect("temporary directory");
        let raw = canonical_subdir(&temp, "frames");
        let archive = canonical_subdir(&temp, "archive");
        let dates = [
            NaiveDate::from_ymd_opt(2026, 8, 1).expect("first date"),
            NaiveDate::from_ymd_opt(2026, 8, 2).expect("second date"),
            NaiveDate::from_ymd_opt(2026, 8, 3).expect("third date"),
        ];
        for date in dates {
            write_segment(&raw, date, 0, 1);
        }

        let first = archive_completed_utc_backlog(&raw, &archive, 2, now())
            .expect("archive first backlog batch");
        assert_eq!(first.candidate_days, 3);
        assert_eq!(first.selected_days, dates[..2]);
        assert_eq!(first.created_days, 2);
        assert_eq!(first.days.len(), 2);

        let second = archive_completed_utc_backlog(&raw, &archive, 2, now())
            .expect("archive remaining backlog");
        assert_eq!(second.candidate_days, 3);
        assert_eq!(second.selected_days, [dates[2], dates[0]]);
        assert_eq!(second.created_days, 1);
        assert_eq!(second.existing_verified_days, 1);
        assert!(archive.join("date=2026-08-03").exists());
    }
}
