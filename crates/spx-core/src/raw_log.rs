use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use chrono::{DateTime, Days, NaiveDate, Utc};
use rustix::fs::{FlockOperation, Mode, OFlags, flock, open};
use serde::Serialize;
use spx_domain::{DomainError, canonical_json_hash};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RawLogError {
    #[error("raw log I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("raw log contract failed: {0}")]
    Domain(#[from] DomainError),
    #[error("raw log JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("raw log segment size must be positive")]
    InvalidSegmentSize,
    #[error("encoded raw record of {record_bytes} bytes exceeds segment limit {limit_bytes}")]
    RecordTooLarge { record_bytes: u64, limit_bytes: u64 },
    #[error("raw log segment index exhausted for UTC date {date}")]
    SegmentIndexExhausted { date: String },
    #[error("unsafe raw log directory: {0}")]
    UnsafeDirectory(PathBuf),
    #[error("unsafe raw log lock path: {0}")]
    UnsafeLockPath(PathBuf),
    #[error("unsafe raw log segment path: {0}")]
    UnsafeSegmentPath(PathBuf),
    #[error("unsafe raw log retention target: {0}")]
    UnsafeRetentionTarget(PathBuf),
    #[error("raw log active-segment state is poisoned")]
    StatePoisoned,
    #[error("raw log filesystem space accounting overflowed")]
    FreeSpaceArithmeticOverflow,
    #[error(
        "raw log append refused: {available_bytes} bytes available, {required_bytes} required including reserve"
    )]
    InsufficientFreeSpace {
        available_bytes: u64,
        required_bytes: u64,
    },
    #[error("invalid raw log retention policy: {0}")]
    InvalidRetentionPolicy(&'static str),
    #[error("raw log retention candidate changed during pruning: {0}")]
    RetentionCandidateChanged(PathBuf),
    #[error(
        "raw log prune failed after removing {removed_files} files ({removed_bytes} bytes): {cause}; directory_sync_error={directory_sync_error:?}"
    )]
    PruneMutation {
        removed_files: usize,
        removed_bytes: u64,
        cause: String,
        directory_sync_error: Option<String>,
    },
}

const DIRECTORY_LOCK_FILE: &str = ".spx-raw-log.lock";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendDurability {
    /// Ordinary append for high-volume quote frames. A later durable append or
    /// segment rotation flushes preceding writes in the same segment.
    Buffered,
    /// Flush the record and all preceding writes in the segment before return.
    Durable,
}

#[derive(Debug)]
pub struct RawLog {
    directory: PathBuf,
    max_segment_bytes: u64,
    min_free_bytes: u64,
    active_segment: Mutex<Option<PathBuf>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RawLogPruneReport {
    pub raw_log_dir: PathBuf,
    pub current_utc_date: NaiveDate,
    pub keep_completed_days: u32,
    pub max_total_bytes: u64,
    pub dry_run: bool,
    pub matched_files: usize,
    pub ignored_entries: usize,
    pub protected_current_or_future_files: usize,
    pub total_bytes_before: u64,
    pub planned_files: usize,
    pub planned_bytes: u64,
    pub removed_files: usize,
    pub removed_bytes: u64,
    pub projected_bytes_after: u64,
    pub limit_satisfied_after_plan: bool,
}

#[derive(Debug, Clone)]
struct SegmentFile {
    path: PathBuf,
    date: NaiveDate,
    index: u32,
    bytes: u64,
}

#[derive(Debug)]
struct PruneSelection {
    selected: Vec<bool>,
    projected_bytes_after: u64,
    planned_files: usize,
    planned_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
enum DirectoryLockMode {
    Shared,
    Exclusive,
}

#[derive(Debug)]
struct DirectoryLock {
    _file: File,
}

#[derive(Serialize)]
struct RawRecord<'a, T> {
    observed_at: DateTime<Utc>,
    payload_sha256: &'a str,
    payload: &'a T,
}

impl RawLog {
    #[cfg(test)]
    pub fn new(directory: impl AsRef<Path>, max_segment_bytes: u64) -> Result<Self, RawLogError> {
        Self::with_min_free_bytes(directory, max_segment_bytes, 0)
    }

    pub fn with_min_free_bytes(
        directory: impl AsRef<Path>,
        max_segment_bytes: u64,
        min_free_bytes: u64,
    ) -> Result<Self, RawLogError> {
        if max_segment_bytes == 0 {
            return Err(RawLogError::InvalidSegmentSize);
        }
        let directory = directory.as_ref();
        fs::create_dir_all(directory)?;
        let metadata = fs::symlink_metadata(directory)?;
        if !metadata.file_type().is_dir() {
            return Err(RawLogError::UnsafeDirectory(directory.to_path_buf()));
        }
        #[cfg(unix)]
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        let directory = fs::canonicalize(directory)?;
        let _directory_lock = DirectoryLock::acquire(&directory, DirectoryLockMode::Shared)?;
        let active_segment = latest_modified_segment(&directory)?;
        Ok(Self {
            directory,
            max_segment_bytes,
            min_free_bytes,
            active_segment: Mutex::new(active_segment),
        })
    }

    pub fn append<T: Serialize>(
        &self,
        payload: &T,
        observed_at: DateTime<Utc>,
        durability: AppendDurability,
    ) -> Result<String, RawLogError> {
        let payload_sha256 = canonical_json_hash(payload)?;
        let record = RawRecord {
            observed_at,
            payload_sha256: &payload_sha256,
            payload,
        };
        let mut encoded = serde_json::to_vec(&record)?;
        encoded.push(b'\n');
        let record_bytes =
            u64::try_from(encoded.len()).map_err(|_| RawLogError::RecordTooLarge {
                record_bytes: u64::MAX,
                limit_bytes: self.max_segment_bytes,
            })?;
        if record_bytes > self.max_segment_bytes {
            return Err(RawLogError::RecordTooLarge {
                record_bytes,
                limit_bytes: self.max_segment_bytes,
            });
        }
        let _directory_lock = DirectoryLock::acquire(&self.directory, DirectoryLockMode::Shared)?;
        self.ensure_free_space(record_bytes)?;

        let date_prefix = observed_at.format("%Y-%m-%d").to_string();
        let mut active = self
            .active_segment
            .lock()
            .map_err(|_| RawLogError::StatePoisoned)?;
        let path = self.segment_for_append(&date_prefix, record_bytes)?;
        if let Some(previous) = active.as_ref().filter(|previous| *previous != &path) {
            match open_existing_segment(previous) {
                Ok(file) => file.sync_data()?,
                Err(RawLogError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        let mut file = open_append(&path)?;
        file.write_all(&encoded)?;
        if durability == AppendDurability::Durable {
            file.sync_data()?;
        }
        *active = Some(path);
        Ok(payload_sha256)
    }

    fn ensure_free_space(&self, record_bytes: u64) -> Result<(), RawLogError> {
        if self.min_free_bytes == 0 {
            return Ok(());
        }
        let statistics = rustix::fs::statvfs(&self.directory).map_err(std::io::Error::from)?;
        let available_bytes = statistics
            .f_bavail
            .checked_mul(statistics.f_frsize)
            .ok_or(RawLogError::FreeSpaceArithmeticOverflow)?;
        let required_bytes = self
            .min_free_bytes
            .checked_add(record_bytes)
            .ok_or(RawLogError::FreeSpaceArithmeticOverflow)?;
        if available_bytes < required_bytes {
            return Err(RawLogError::InsufficientFreeSpace {
                available_bytes,
                required_bytes,
            });
        }
        Ok(())
    }

    fn segment_for_append(
        &self,
        date_prefix: &str,
        record_bytes: u64,
    ) -> Result<PathBuf, RawLogError> {
        let mut latest: Option<(u32, PathBuf, u64)> = None;
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(index) = segment_index(&name, date_prefix) else {
                continue;
            };
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.file_type().is_file() {
                return Err(RawLogError::UnsafeSegmentPath(entry.path()));
            }
            let bytes = metadata.len();
            if latest
                .as_ref()
                .is_none_or(|(latest_index, _, _)| index > *latest_index)
            {
                latest = Some((index, entry.path(), bytes));
            }
        }

        match latest {
            Some((index, path, bytes))
                if bytes > 0
                    && bytes
                        .checked_add(record_bytes)
                        .is_none_or(|total| total > self.max_segment_bytes) =>
            {
                let next_index = index.checked_add(1).filter(|index| *index <= 9999).ok_or(
                    RawLogError::SegmentIndexExhausted {
                        date: date_prefix.to_owned(),
                    },
                )?;
                Ok(self.segment_path(date_prefix, next_index))
            }
            Some((_, path, _)) => Ok(path),
            None => Ok(self.segment_path(date_prefix, 0)),
        }
    }

    fn segment_path(&self, date_prefix: &str, index: u32) -> PathBuf {
        self.directory
            .join(format!("{date_prefix}.{index:04}.ndjson"))
    }
}

impl DirectoryLock {
    fn acquire(directory: &Path, mode: DirectoryLockMode) -> Result<Self, RawLogError> {
        let path = directory.join(DIRECTORY_LOCK_FILE);
        let descriptor = open(
            &path,
            OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::from_raw_mode(0o600),
        )
        .map_err(|error| unsafe_open_error(error, &path, RawLogError::UnsafeLockPath))?;
        let file = File::from(descriptor);
        if !file.metadata()?.file_type().is_file() {
            return Err(RawLogError::UnsafeLockPath(path));
        }
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        let operation = match mode {
            DirectoryLockMode::Shared => FlockOperation::LockShared,
            DirectoryLockMode::Exclusive => FlockOperation::LockExclusive,
        };
        flock(&file, operation).map_err(std::io::Error::from)?;
        Ok(Self { _file: file })
    }
}

/// Selects and optionally removes completed raw-log segments under a bounded policy.
///
/// # Errors
///
/// Returns an error for an invalid policy, unreadable directory, arithmetic
/// overflow, or a candidate that changes before removal.
pub fn prune_raw_log(
    directory: impl AsRef<Path>,
    current_utc_date: NaiveDate,
    keep_completed_days: u32,
    max_total_bytes: u64,
    dry_run: bool,
) -> Result<RawLogPruneReport, RawLogError> {
    if !(1..=365).contains(&keep_completed_days) {
        return Err(RawLogError::InvalidRetentionPolicy(
            "keep_completed_days must be within 1..=365",
        ));
    }
    if max_total_bytes == 0 {
        return Err(RawLogError::InvalidRetentionPolicy(
            "max_total_bytes must be positive",
        ));
    }
    let directory = validate_retention_target(directory.as_ref())?;
    let _directory_lock = DirectoryLock::acquire(&directory, DirectoryLockMode::Exclusive)?;
    let keep_cutoff = current_utc_date
        .checked_sub_days(Days::new(u64::from(keep_completed_days)))
        .ok_or(RawLogError::InvalidRetentionPolicy(
            "completed-day cutoff is not representable",
        ))?;

    let (segments, ignored_entries) = scan_segment_files(&directory)?;
    let total_bytes_before = total_segment_bytes(&segments)?;
    let protected_current_or_future_files = segments
        .iter()
        .filter(|segment| segment.date >= current_utc_date)
        .count();
    let selection = select_segments(
        &segments,
        current_utc_date,
        keep_cutoff,
        max_total_bytes,
        total_bytes_before,
    )?;
    let (removed_files, removed_bytes) =
        apply_selection(&directory, &segments, &selection.selected, dry_run)?;

    Ok(RawLogPruneReport {
        raw_log_dir: directory,
        current_utc_date,
        keep_completed_days,
        max_total_bytes,
        dry_run,
        matched_files: segments.len(),
        ignored_entries,
        protected_current_or_future_files,
        total_bytes_before,
        planned_files: selection.planned_files,
        planned_bytes: selection.planned_bytes,
        removed_files,
        removed_bytes,
        projected_bytes_after: selection.projected_bytes_after,
        limit_satisfied_after_plan: selection.projected_bytes_after <= max_total_bytes,
    })
}

fn scan_segment_files(directory: &Path) -> Result<(Vec<SegmentFile>, usize), RawLogError> {
    let mut segments = Vec::new();
    let mut ignored_entries = 0_usize;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_name = entry.file_name();
        if file_name == DIRECTORY_LOCK_FILE {
            continue;
        }
        let parsed = file_name.to_str().and_then(parse_segment_name);
        let metadata = fs::symlink_metadata(entry.path())?;
        let Some((date, index)) = parsed.filter(|_| metadata.file_type().is_file()) else {
            ignored_entries = ignored_entries.saturating_add(1);
            continue;
        };
        segments.push(SegmentFile {
            path: entry.path(),
            date,
            index,
            bytes: metadata.len(),
        });
    }
    segments.sort_by(|left, right| {
        (left.date, left.index, &left.path).cmp(&(right.date, right.index, &right.path))
    });
    Ok((segments, ignored_entries))
}

fn total_segment_bytes(segments: &[SegmentFile]) -> Result<u64, RawLogError> {
    segments.iter().try_fold(0_u64, |total, segment| {
        total
            .checked_add(segment.bytes)
            .ok_or(RawLogError::FreeSpaceArithmeticOverflow)
    })
}

fn select_segments(
    segments: &[SegmentFile],
    current_utc_date: NaiveDate,
    keep_cutoff: NaiveDate,
    max_total_bytes: u64,
    total_bytes_before: u64,
) -> Result<PruneSelection, RawLogError> {
    let mut selected = vec![false; segments.len()];
    let mut projected_bytes_after = total_bytes_before;
    for (position, segment) in segments.iter().enumerate() {
        if segment.date < keep_cutoff {
            selected[position] = true;
            projected_bytes_after = projected_bytes_after.saturating_sub(segment.bytes);
        }
    }
    for (position, segment) in segments.iter().enumerate() {
        if projected_bytes_after <= max_total_bytes {
            break;
        }
        if !selected[position] && segment.date < current_utc_date {
            selected[position] = true;
            projected_bytes_after = projected_bytes_after.saturating_sub(segment.bytes);
        }
    }
    let planned_files = selected.iter().filter(|selected| **selected).count();
    let planned_bytes = segments
        .iter()
        .zip(&selected)
        .filter(|(_, selected)| **selected)
        .try_fold(0_u64, |total, (segment, _)| {
            total
                .checked_add(segment.bytes)
                .ok_or(RawLogError::FreeSpaceArithmeticOverflow)
        })?;
    Ok(PruneSelection {
        selected,
        projected_bytes_after,
        planned_files,
        planned_bytes,
    })
}

fn apply_selection(
    directory: &Path,
    segments: &[SegmentFile],
    selected: &[bool],
    dry_run: bool,
) -> Result<(usize, u64), RawLogError> {
    if dry_run {
        return Ok((0, 0));
    }
    for (segment, selected) in segments.iter().zip(selected) {
        if *selected {
            verify_retention_candidate(directory, segment)?;
        }
    }
    apply_verified_selection(directory, segments, selected, |path| fs::remove_file(path))
}

fn apply_verified_selection(
    directory: &Path,
    segments: &[SegmentFile],
    selected: &[bool],
    mut remove: impl FnMut(&Path) -> Result<(), std::io::Error>,
) -> Result<(usize, u64), RawLogError> {
    let mut removed_files = 0_usize;
    let mut removed_bytes = 0_u64;
    for (segment, selected) in segments.iter().zip(selected) {
        if !selected {
            continue;
        }
        let next_removed_bytes = removed_bytes
            .checked_add(segment.bytes)
            .ok_or(RawLogError::FreeSpaceArithmeticOverflow)?;
        if let Err(error) = remove(&segment.path) {
            let directory_sync_error = sync_after_partial_removal(directory, removed_files);
            return Err(RawLogError::PruneMutation {
                removed_files,
                removed_bytes,
                cause: error.to_string(),
                directory_sync_error,
            });
        }
        removed_files = removed_files.saturating_add(1);
        removed_bytes = next_removed_bytes;
    }
    if removed_files > 0
        && let Err(error) = sync_directory(directory)
    {
        return Err(RawLogError::PruneMutation {
            removed_files,
            removed_bytes,
            cause: error.to_string(),
            directory_sync_error: Some(error.to_string()),
        });
    }
    Ok((removed_files, removed_bytes))
}

fn sync_after_partial_removal(directory: &Path, removed_files: usize) -> Option<String> {
    (removed_files > 0)
        .then(|| {
            sync_directory(directory)
                .err()
                .map(|error| error.to_string())
        })
        .flatten()
}

fn sync_directory(directory: &Path) -> Result<(), std::io::Error> {
    let descriptor = open(
        directory,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(std::io::Error::from)?;
    File::from(descriptor).sync_all()
}

fn validate_retention_target(directory: &Path) -> Result<PathBuf, RawLogError> {
    if !directory.is_absolute() {
        return Err(RawLogError::UnsafeRetentionTarget(directory.to_path_buf()));
    }
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.file_type().is_dir() {
        return Err(RawLogError::UnsafeRetentionTarget(directory.to_path_buf()));
    }
    let canonical = fs::canonicalize(directory)?;
    if canonical != directory || canonical.parent().is_none() {
        return Err(RawLogError::UnsafeRetentionTarget(directory.to_path_buf()));
    }
    Ok(canonical)
}

fn verify_retention_candidate(
    directory: &Path,
    candidate: &SegmentFile,
) -> Result<(), RawLogError> {
    if candidate.path.parent() != Some(directory) {
        return Err(RawLogError::RetentionCandidateChanged(
            candidate.path.clone(),
        ));
    }
    let Some(name) = candidate.path.file_name().and_then(|name| name.to_str()) else {
        return Err(RawLogError::RetentionCandidateChanged(
            candidate.path.clone(),
        ));
    };
    if parse_segment_name(name) != Some((candidate.date, candidate.index)) {
        return Err(RawLogError::RetentionCandidateChanged(
            candidate.path.clone(),
        ));
    }
    let metadata = fs::symlink_metadata(&candidate.path)?;
    if !metadata.file_type().is_file() || metadata.len() != candidate.bytes {
        return Err(RawLogError::RetentionCandidateChanged(
            candidate.path.clone(),
        ));
    }
    Ok(())
}

fn latest_modified_segment(directory: &Path) -> Result<Option<PathBuf>, RawLogError> {
    let mut latest = None;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !is_segment_name(&name) {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.file_type().is_file() {
            return Err(RawLogError::UnsafeSegmentPath(entry.path()));
        }
        let modified = metadata.modified()?;
        let candidate = (modified, name, entry.path());
        if latest.as_ref().is_none_or(|current| &candidate > current) {
            latest = Some(candidate);
        }
    }
    Ok(latest.map(|(_, _, path)| path))
}

fn is_segment_name(name: &str) -> bool {
    parse_segment_name(name).is_some()
}

fn parse_segment_name(name: &str) -> Option<(NaiveDate, u32)> {
    let (date, suffix) = name.split_once('.')?;
    if date.len() != 10
        || !date
            .bytes()
            .enumerate()
            .all(|(position, byte)| match position {
                4 | 7 => byte == b'-',
                _ => byte.is_ascii_digit(),
            })
    {
        return None;
    }
    let date = NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?;
    let index = suffix.strip_suffix(".ndjson")?;
    if index.len() != 4 || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((date, index.parse().ok()?))
}

fn segment_index(name: &str, date_prefix: &str) -> Option<u32> {
    let (date, index) = parse_segment_name(name)?;
    (date.format("%Y-%m-%d").to_string() == date_prefix).then_some(index)
}

fn open_append(path: &Path) -> Result<File, RawLogError> {
    open_segment(path, OFlags::CREATE | OFlags::WRONLY | OFlags::APPEND)
}

fn open_existing_segment(path: &Path) -> Result<File, RawLogError> {
    open_segment(path, OFlags::WRONLY)
}

fn open_segment(path: &Path, access: OFlags) -> Result<File, RawLogError> {
    let descriptor = open(
        path,
        access | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::from_raw_mode(0o600),
    )
    .map_err(|error| unsafe_open_error(error, path, RawLogError::UnsafeSegmentPath))?;
    let file = File::from(descriptor);
    if !file.metadata()?.file_type().is_file() {
        return Err(RawLogError::UnsafeSegmentPath(path.to_path_buf()));
    }
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

fn unsafe_open_error(
    error: rustix::io::Errno,
    path: &Path,
    unsafe_path: impl FnOnce(PathBuf) -> RawLogError,
) -> RawLogError {
    if matches!(error, rustix::io::Errno::LOOP | rustix::io::Errno::ISDIR) {
        unsafe_path(path.to_path_buf())
    } else {
        RawLogError::Io(std::io::Error::from(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use tempfile::TempDir;

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    #[derive(Serialize)]
    struct Payload<'a> {
        value: &'a str,
    }

    fn at(value: &str) -> DateTime<Utc> {
        value.parse().expect("valid timestamp")
    }

    #[test]
    fn rotates_bounded_daily_segments_without_overwriting() {
        let temp = TempDir::new().expect("temporary directory");
        let probe = RawLog::new(temp.path(), 4096).expect("probe raw log");
        probe
            .append(
                &Payload { value: "first" },
                at("2026-07-31T14:30:00Z"),
                AppendDurability::Buffered,
            )
            .expect("probe append");
        let first_path = temp.path().join("2026-07-31.0000.ndjson");
        let first_bytes = fs::metadata(&first_path).expect("first segment").len();

        let log = RawLog::new(temp.path(), first_bytes + 1).expect("bounded raw log");
        log.append(
            &Payload { value: "second" },
            at("2026-07-31T14:31:00Z"),
            AppendDurability::Durable,
        )
        .expect("rotated append");

        assert!(first_path.exists());
        assert!(temp.path().join("2026-07-31.0001.ndjson").exists());
        assert_eq!(
            fs::read_to_string(first_path)
                .expect("read original segment")
                .lines()
                .count(),
            1
        );
    }

    #[test]
    fn rejects_record_larger_than_segment_bound() {
        let temp = TempDir::new().expect("temporary directory");
        let log = RawLog::new(temp.path(), 16).expect("raw log");
        assert!(matches!(
            log.append(
                &Payload { value: "too large" },
                at("2026-07-31T14:30:00Z"),
                AppendDurability::Buffered,
            ),
            Err(RawLogError::RecordTooLarge { .. })
        ));
    }

    #[test]
    fn refuses_to_create_a_segment_name_outside_the_retention_contract() {
        let temp = TempDir::new().expect("temporary directory");
        fs::write(temp.path().join("2026-07-31.9999.ndjson"), vec![b'x'; 4096])
            .expect("write final valid segment");
        let log = RawLog::new(temp.path(), 4096).expect("raw log");

        assert!(matches!(
            log.append(
                &Payload { value: "next" },
                at("2026-07-31T14:30:00Z"),
                AppendDurability::Buffered,
            ),
            Err(RawLogError::SegmentIndexExhausted { .. })
        ));
        assert!(!temp.path().join("2026-07-31.10000.ndjson").exists());
    }

    #[test]
    fn changing_date_closes_the_previous_active_segment() {
        let temp = TempDir::new().expect("temporary directory");
        let log = RawLog::new(temp.path(), 4096).expect("raw log");
        log.append(
            &Payload { value: "day one" },
            at("2026-07-31T23:59:59Z"),
            AppendDurability::Buffered,
        )
        .expect("first date append");
        log.append(
            &Payload { value: "day two" },
            at("2026-08-01T00:00:00Z"),
            AppendDurability::Buffered,
        )
        .expect("second date append");

        let active = log.active_segment.lock().expect("active segment lock");
        let expected = fs::canonicalize(temp.path())
            .expect("canonical raw directory")
            .join("2026-08-01.0000.ndjson");
        assert_eq!(active.as_deref(), Some(expected.as_path()));
        assert!(temp.path().join("2026-07-31.0000.ndjson").exists());
    }

    #[test]
    fn free_space_reserve_refuses_append_before_creating_a_segment() {
        let temp = TempDir::new().expect("temporary directory");
        let log =
            RawLog::with_min_free_bytes(temp.path(), 4096, u64::MAX / 2).expect("guarded raw log");

        assert!(matches!(
            log.append(
                &Payload { value: "blocked" },
                at("2026-08-01T00:00:00Z"),
                AppendDurability::Buffered,
            ),
            Err(RawLogError::InsufficientFreeSpace { .. })
        ));
        assert!(
            fs::read_dir(temp.path())
                .expect("read raw directory")
                .all(|entry| !is_segment_name(
                    entry
                        .expect("raw directory entry")
                        .file_name()
                        .to_string_lossy()
                        .as_ref()
                ))
        );
    }

    #[cfg(unix)]
    #[test]
    fn append_and_direct_open_refuse_a_segment_shaped_symlink() {
        let temp = TempDir::new().expect("temporary directory");
        let log = RawLog::new(temp.path(), 4096).expect("raw log");
        let outside = temp.path().join("outside.ndjson");
        fs::write(&outside, b"outside").expect("write outside target");
        let segment = temp.path().join("2026-08-01.0000.ndjson");
        symlink(&outside, &segment).expect("create segment symlink");

        assert!(matches!(
            open_append(&segment),
            Err(RawLogError::UnsafeSegmentPath(_))
        ));
        assert!(matches!(
            log.append(
                &Payload {
                    value: "must not follow"
                },
                at("2026-08-01T00:00:00Z"),
                AppendDurability::Buffered,
            ),
            Err(RawLogError::UnsafeSegmentPath(_))
        ));
        assert_eq!(fs::read(&outside).expect("read outside target"), b"outside");
    }

    #[test]
    fn append_refuses_a_segment_shaped_non_regular_entry() {
        let temp = TempDir::new().expect("temporary directory");
        let log = RawLog::new(temp.path(), 4096).expect("raw log");
        fs::create_dir(temp.path().join("2026-08-01.0000.ndjson"))
            .expect("create segment-shaped directory");

        assert!(matches!(
            log.append(
                &Payload { value: "blocked" },
                at("2026-08-01T00:00:00Z"),
                AppendDurability::Buffered,
            ),
            Err(RawLogError::UnsafeSegmentPath(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn lock_file_is_no_follow_and_never_mutates_its_target() {
        let temp = TempDir::new().expect("temporary directory");
        let directory = temp.path().join("frames");
        fs::create_dir(&directory).expect("create raw directory");
        let outside = temp.path().join("outside.lock");
        fs::write(&outside, b"outside").expect("write outside lock target");
        symlink(&outside, directory.join(DIRECTORY_LOCK_FILE)).expect("create lock symlink");

        assert!(matches!(
            RawLog::new(&directory, 4096),
            Err(RawLogError::UnsafeLockPath(_))
        ));
        assert_eq!(
            fs::read(&outside).expect("read outside lock target"),
            b"outside"
        );
    }

    #[cfg(unix)]
    #[test]
    fn prune_dry_run_selects_only_strict_old_regular_segments() {
        let temp = TempDir::new().expect("temporary directory");
        let directory = temp.path().join("frames");
        fs::create_dir(&directory).expect("create raw directory");
        write_segment(&directory, "2026-07-29.0000.ndjson", b"expired");
        write_segment(&directory, "2026-07-30.0000.ndjson", b"retained");
        write_segment(&directory, "2026-08-01.0000.ndjson", b"current");
        write_segment(&directory, "2026-08-02.0000.ndjson", b"future");
        write_segment(&directory, "2026-02-30.0000.ndjson", b"bad date");
        write_segment(&directory, "2026-07-28.12.ndjson", b"bad index");
        fs::create_dir(directory.join("2026-07-28.0001.ndjson"))
            .expect("create segment-shaped directory");
        let outside = temp.path().join("outside.ndjson");
        fs::write(&outside, b"outside").expect("write symlink target");
        symlink(&outside, directory.join("2026-07-28.0000.ndjson"))
            .expect("create segment-shaped symlink");

        let canonical_directory = fs::canonicalize(&directory).expect("canonical raw directory");
        let report = prune_raw_log(
            &canonical_directory,
            NaiveDate::from_ymd_opt(2026, 8, 1).expect("date"),
            2,
            u64::MAX,
            true,
        )
        .expect("dry-run prune");

        assert_eq!(report.matched_files, 4);
        assert_eq!(report.ignored_entries, 4);
        assert_eq!(report.protected_current_or_future_files, 2);
        assert_eq!(report.planned_files, 1);
        assert_eq!(report.removed_files, 0);
        assert!(directory.join("2026-07-29.0000.ndjson").exists());
        assert!(directory.join("2026-08-01.0000.ndjson").exists());
        assert!(directory.join("2026-07-28.0000.ndjson").exists());

        let applied = prune_raw_log(
            &canonical_directory,
            NaiveDate::from_ymd_opt(2026, 8, 1).expect("date"),
            2,
            u64::MAX,
            false,
        )
        .expect("applied prune");
        assert_eq!(applied.removed_files, 1);
        assert!(!directory.join("2026-07-29.0000.ndjson").exists());
        assert!(directory.join("2026-07-30.0000.ndjson").exists());
        assert!(directory.join("2026-08-01.0000.ndjson").exists());
        assert!(outside.exists());
    }

    #[test]
    fn size_cap_deletes_oldest_completed_segments_but_never_current_day() {
        let temp = TempDir::new().expect("temporary directory");
        for name in [
            "2026-07-28.0000.ndjson",
            "2026-07-29.0000.ndjson",
            "2026-07-30.0000.ndjson",
            "2026-08-01.0000.ndjson",
        ] {
            write_segment(temp.path(), name, b"0123456789");
        }

        let directory = fs::canonicalize(temp.path()).expect("canonical raw directory");
        let report = prune_raw_log(
            &directory,
            NaiveDate::from_ymd_opt(2026, 8, 1).expect("date"),
            365,
            25,
            false,
        )
        .expect("size-cap prune");

        assert_eq!(report.total_bytes_before, 40);
        assert_eq!(report.planned_files, 2);
        assert_eq!(report.removed_files, 2);
        assert_eq!(report.projected_bytes_after, 20);
        assert!(report.limit_satisfied_after_plan);
        assert!(!temp.path().join("2026-07-28.0000.ndjson").exists());
        assert!(!temp.path().join("2026-07-29.0000.ndjson").exists());
        assert!(temp.path().join("2026-07-30.0000.ndjson").exists());
        assert!(temp.path().join("2026-08-01.0000.ndjson").exists());
    }

    #[test]
    fn size_cap_reports_when_current_day_alone_exceeds_limit() {
        let temp = TempDir::new().expect("temporary directory");
        write_segment(temp.path(), "2026-08-01.0000.ndjson", b"0123456789");

        let directory = fs::canonicalize(temp.path()).expect("canonical raw directory");
        let report = prune_raw_log(
            &directory,
            NaiveDate::from_ymd_opt(2026, 8, 1).expect("date"),
            7,
            5,
            false,
        )
        .expect("protected current-day report");

        assert_eq!(report.planned_files, 0);
        assert_eq!(report.removed_files, 0);
        assert!(!report.limit_satisfied_after_plan);
        assert!(temp.path().join("2026-08-01.0000.ndjson").exists());
    }

    #[test]
    fn prune_rejects_relative_root_and_noncanonical_targets() {
        assert!(matches!(
            prune_raw_log(
                "frames",
                NaiveDate::from_ymd_opt(2026, 8, 1).expect("date"),
                7,
                4096,
                true,
            ),
            Err(RawLogError::UnsafeRetentionTarget(_))
        ));
        assert!(matches!(
            prune_raw_log(
                "/",
                NaiveDate::from_ymd_opt(2026, 8, 1).expect("date"),
                7,
                4096,
                true,
            ),
            Err(RawLogError::UnsafeRetentionTarget(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn prune_rejects_a_symlink_directory_target() {
        let temp = TempDir::new().expect("temporary directory");
        let directory = temp.path().join("frames");
        fs::create_dir(&directory).expect("create raw directory");
        let alias = temp.path().join("frames-alias");
        symlink(&directory, &alias).expect("create directory symlink");

        assert!(matches!(
            prune_raw_log(
                &alias,
                NaiveDate::from_ymd_opt(2026, 8, 1).expect("date"),
                7,
                4096,
                true,
            ),
            Err(RawLogError::UnsafeRetentionTarget(_))
        ));
    }

    #[test]
    fn cached_previous_segment_can_be_legally_pruned_before_rollover_append() {
        let temp = TempDir::new().expect("temporary directory");
        let first = RawLog::new(temp.path(), 4096).expect("first raw log");
        first
            .append(
                &Payload { value: "old" },
                at("2026-07-29T23:59:00Z"),
                AppendDurability::Buffered,
            )
            .expect("old append");
        drop(first);
        let restarted = RawLog::new(temp.path(), 4096).expect("restarted raw log");
        let directory = fs::canonicalize(temp.path()).expect("canonical raw directory");
        prune_raw_log(
            &directory,
            NaiveDate::from_ymd_opt(2026, 8, 1).expect("date"),
            1,
            u64::MAX,
            false,
        )
        .expect("prune cached previous segment");

        restarted
            .append(
                &Payload { value: "new" },
                at("2026-08-01T00:01:00Z"),
                AppendDurability::Buffered,
            )
            .expect("rollover append after legal prune");
        assert!(temp.path().join("2026-08-01.0000.ndjson").exists());
    }

    #[test]
    fn shared_append_lock_blocks_exclusive_prune_until_release() {
        use std::sync::mpsc;
        use std::time::Duration;

        let temp = TempDir::new().expect("temporary directory");
        write_segment(temp.path(), "2026-07-29.0000.ndjson", b"expired");
        let directory = fs::canonicalize(temp.path()).expect("canonical raw directory");
        let shared = DirectoryLock::acquire(&directory, DirectoryLockMode::Shared)
            .expect("shared append lock");
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let prune_directory = directory.clone();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).expect("signal prune start");
            let result = prune_raw_log(
                &prune_directory,
                NaiveDate::from_ymd_opt(2026, 8, 1).expect("date"),
                1,
                u64::MAX,
                false,
            );
            done_tx.send(result).expect("send prune result");
        });
        started_rx.recv().expect("receive prune start");
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        drop(shared);
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("prune unblocked")
            .expect("prune succeeded");
        worker.join().expect("join prune worker");
        assert!(!temp.path().join("2026-07-29.0000.ndjson").exists());
    }

    #[test]
    fn partial_prune_failure_reports_removed_files_after_directory_sync() {
        let temp = TempDir::new().expect("temporary directory");
        write_segment(temp.path(), "2026-07-28.0000.ndjson", b"first");
        write_segment(temp.path(), "2026-07-29.0000.ndjson", b"second");
        let directory = fs::canonicalize(temp.path()).expect("canonical raw directory");
        let (segments, _) = scan_segment_files(&directory).expect("scan segments");
        for segment in &segments {
            verify_retention_candidate(&directory, segment).expect("verify candidate");
        }
        let mut calls = 0_u8;
        let result = apply_verified_selection(&directory, &segments, &[true, true], |path| {
            calls = calls.saturating_add(1);
            if calls == 1 {
                fs::remove_file(path)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected second deletion failure",
                ))
            }
        });

        assert!(matches!(
            result,
            Err(RawLogError::PruneMutation {
                removed_files: 1,
                removed_bytes: 5,
                directory_sync_error: None,
                ..
            })
        ));
        assert!(!temp.path().join("2026-07-28.0000.ndjson").exists());
        assert!(temp.path().join("2026-07-29.0000.ndjson").exists());
    }

    fn write_segment(directory: &Path, name: &str, contents: &[u8]) {
        fs::write(directory.join(name), contents).expect("write raw segment");
    }
}
