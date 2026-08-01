use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use chrono::{DateTime, Utc};
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
    #[error("raw log active-segment state is poisoned")]
    StatePoisoned,
}

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
    active_segment: Mutex<Option<PathBuf>>,
}

#[derive(Serialize)]
struct RawRecord<'a, T> {
    observed_at: DateTime<Utc>,
    payload_sha256: &'a str,
    payload: &'a T,
}

impl RawLog {
    pub fn new(directory: impl AsRef<Path>, max_segment_bytes: u64) -> Result<Self, RawLogError> {
        if max_segment_bytes == 0 {
            return Err(RawLogError::InvalidSegmentSize);
        }
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        #[cfg(unix)]
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        let active_segment = latest_modified_segment(&directory)?;
        Ok(Self {
            directory,
            max_segment_bytes,
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

        let date_prefix = observed_at.format("%Y-%m-%d").to_string();
        let path = self.segment_for_append(&date_prefix, record_bytes)?;
        let mut active = self
            .active_segment
            .lock()
            .map_err(|_| RawLogError::StatePoisoned)?;
        if let Some(previous) = active.as_ref().filter(|previous| *previous != &path) {
            OpenOptions::new().write(true).open(previous)?.sync_data()?;
        }
        let mut file = open_append(&path)?;
        file.write_all(&encoded)?;
        if durability == AppendDurability::Durable {
            file.sync_data()?;
        }
        *active = Some(path);
        Ok(payload_sha256)
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
            let bytes = entry.metadata()?.len();
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
                let next_index = index
                    .checked_add(1)
                    .ok_or_else(|| std::io::Error::other("raw log segment index exhausted"))?;
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

fn latest_modified_segment(directory: &Path) -> Result<Option<PathBuf>, std::io::Error> {
    let mut latest = None;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !is_segment_name(&name) {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        let candidate = (modified, name, entry.path());
        if latest.as_ref().is_none_or(|current| &candidate > current) {
            latest = Some(candidate);
        }
    }
    Ok(latest.map(|(_, _, path)| path))
}

fn is_segment_name(name: &str) -> bool {
    let Some((date, suffix)) = name.split_once('.') else {
        return false;
    };
    date.len() == 10
        && date.bytes().enumerate().all(|(index, byte)| match index {
            4 | 7 => byte == b'-',
            _ => byte.is_ascii_digit(),
        })
        && suffix.strip_suffix(".ndjson").is_some_and(|index| {
            index.len() == 4 && index.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn segment_index(name: &str, date_prefix: &str) -> Option<u32> {
    name.strip_prefix(date_prefix)?
        .strip_prefix('.')?
        .strip_suffix(".ndjson")?
        .parse()
        .ok()
}

fn open_append(path: &Path) -> Result<File, std::io::Error> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options.open(path)?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use tempfile::TempDir;

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
        let expected = temp.path().join("2026-08-01.0000.ndjson");
        assert_eq!(active.as_deref(), Some(expected.as_path()));
        assert!(temp.path().join("2026-07-31.0000.ndjson").exists());
    }
}
