use std::path::Path;

use chrono::{
    DateTime, Datelike as _, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, TimeZone as _,
    Timelike as _, Utc, Weekday,
};
use chrono_tz::America::New_York;
use serde::Serialize;
use spx_core::LatestDeskMapProjectionV1;
use spx_domain::MarketSession;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionSourceErrorCode {
    Missing,
    Metadata,
    NotRegularFile,
    TooLarge,
    Read,
    ChangedDuringRead,
    InvalidJson,
    InvalidContract,
    InvalidSourceSlot,
}

impl ProjectionSourceErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Metadata => "metadata",
            Self::NotRegularFile => "not_regular_file",
            Self::TooLarge => "too_large",
            Self::Read => "read",
            Self::ChangedDuringRead => "changed_during_read",
            Self::InvalidJson => "invalid_json",
            Self::InvalidContract => "invalid_contract",
            Self::InvalidSourceSlot => "invalid_source_slot",
        }
    }
}

#[derive(Debug, Error)]
pub enum ProjectionSourceError {
    #[error("latest desk projection is missing")]
    Missing,
    #[error("latest desk projection metadata failed")]
    Metadata(#[source] std::io::Error),
    #[error("latest desk projection is not a regular file")]
    NotRegularFile,
    #[error("latest desk projection exceeds the configured byte bound")]
    TooLarge,
    #[error("latest desk projection read failed")]
    Read(#[source] std::io::Error),
    #[error("latest desk projection changed outside the configured byte bound")]
    ChangedDuringRead,
    #[error("latest desk projection JSON is invalid")]
    InvalidJson(#[source] serde_json::Error),
    #[error("latest desk projection contract is invalid")]
    InvalidContract(#[source] spx_domain::DomainError),
    #[error("latest desk projection source_slot is not canonical ET minute data")]
    InvalidSourceSlot,
}

impl ProjectionSourceError {
    pub const fn code(&self) -> ProjectionSourceErrorCode {
        match self {
            Self::Missing => ProjectionSourceErrorCode::Missing,
            Self::Metadata(_) => ProjectionSourceErrorCode::Metadata,
            Self::NotRegularFile => ProjectionSourceErrorCode::NotRegularFile,
            Self::TooLarge => ProjectionSourceErrorCode::TooLarge,
            Self::Read(_) => ProjectionSourceErrorCode::Read,
            Self::ChangedDuringRead => ProjectionSourceErrorCode::ChangedDuringRead,
            Self::InvalidJson(_) => ProjectionSourceErrorCode::InvalidJson,
            Self::InvalidContract(_) => ProjectionSourceErrorCode::InvalidContract,
            Self::InvalidSourceSlot => ProjectionSourceErrorCode::InvalidSourceSlot,
        }
    }
}

/// Reads one complete core projection without accepting oversized or partial JSON.
///
/// # Errors
///
/// Returns a typed source error for missing, unsafe, unreadable, malformed, or
/// contract-invalid input.
pub fn read_latest_projection(
    path: &Path,
    max_bytes: u64,
) -> Result<LatestDeskMapProjectionV1, ProjectionSourceError> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ProjectionSourceError::Missing
        } else {
            ProjectionSourceError::Metadata(error)
        }
    })?;
    if !metadata.is_file() {
        return Err(ProjectionSourceError::NotRegularFile);
    }
    if metadata.len() > max_bytes {
        return Err(ProjectionSourceError::TooLarge);
    }
    let bytes = std::fs::read(path).map_err(ProjectionSourceError::Read)?;
    let actual_len = u64::try_from(bytes.len()).map_err(|_| ProjectionSourceError::TooLarge)?;
    if actual_len > max_bytes {
        return Err(ProjectionSourceError::ChangedDuringRead);
    }
    let latest: LatestDeskMapProjectionV1 =
        serde_json::from_slice(&bytes).map_err(ProjectionSourceError::InvalidJson)?;
    latest
        .validate()
        .map_err(ProjectionSourceError::InvalidContract)?;
    validate_source_slot(&latest)?;
    Ok(latest)
}

fn validate_source_slot(latest: &LatestDeskMapProjectionV1) -> Result<(), ProjectionSourceError> {
    let value = latest.projection.source_slot.as_str();
    let parsed = NaiveDateTime::parse_from_str(value, "%Y-%m-%d:%H:%M")
        .map_err(|_| ProjectionSourceError::InvalidSourceSlot)?;
    if parsed.format("%Y-%m-%d:%H:%M").to_string() != value
        || parsed.date() != latest.projection.trading_date_et
    {
        return Err(ProjectionSourceError::InvalidSourceSlot);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportSlot {
    source_slot: String,
    ledger_slot: String,
    trading_date_et: NaiveDate,
    starts_at: DateTime<Utc>,
    closes_at: DateTime<Utc>,
}

impl ReportSlot {
    pub fn source_slot(&self) -> &str {
        &self.source_slot
    }

    pub fn ledger_slot(&self) -> &str {
        &self.ledger_slot
    }

    pub const fn trading_date_et(&self) -> NaiveDate {
        self.trading_date_et
    }

    pub const fn starts_at(&self) -> DateTime<Utc> {
        self.starts_at
    }

    pub const fn closes_at(&self) -> DateTime<Utc> {
        self.closes_at
    }
}

/// Returns the active ET half-hour slot only during its bounded grace window.
pub fn active_report_slot(now: DateTime<Utc>, grace_seconds: i64) -> Option<ReportSlot> {
    let now_et = now.with_timezone(&New_York);
    if matches!(now_et.weekday(), Weekday::Sat | Weekday::Sun) {
        return None;
    }
    let boundary_minute = if now_et.minute() < 30 { 0 } else { 30 };
    let local = now_et
        .date_naive()
        .and_hms_opt(now_et.hour(), boundary_minute, 0)?;
    if local.time() < NaiveTime::from_hms_opt(9, 30, 0)?
        || local.time() >= NaiveTime::from_hms_opt(16, 0, 0)?
    {
        return None;
    }
    let slot_start_et = New_York.from_local_datetime(&local).single()?;
    let starts_at = slot_start_et.with_timezone(&Utc);
    let elapsed = now.signed_duration_since(starts_at).num_seconds();
    if elapsed < 0 || elapsed > grace_seconds {
        return None;
    }
    let closes_at = starts_at.checked_add_signed(chrono::TimeDelta::seconds(grace_seconds))?;
    Some(ReportSlot {
        source_slot: local.format("%Y-%m-%d:%H:%M").to_string(),
        ledger_slot: slot_start_et.to_rfc3339_opts(SecondsFormat::Secs, false),
        trading_date_et: local.date(),
        starts_at,
        closes_at,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionEligibility {
    Eligible,
    AwaitingCurrentProjection,
    NotRth,
    NotYetAvailable,
    Expired,
}

impl ProjectionEligibility {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Eligible => "eligible",
            Self::AwaitingCurrentProjection => "awaiting_current_projection",
            Self::NotRth => "not_rth",
            Self::NotYetAvailable => "not_yet_available",
            Self::Expired => "expired",
        }
    }
}

pub fn projection_eligibility(
    latest: &LatestDeskMapProjectionV1,
    slot: &ReportSlot,
    now: DateTime<Utc>,
) -> ProjectionEligibility {
    let projection = &latest.projection;
    if projection.session != MarketSession::Rth {
        return ProjectionEligibility::NotRth;
    }
    if projection.source_slot.as_str() != slot.source_slot()
        || projection.trading_date_et != slot.trading_date_et()
        || projection.available_at < slot.starts_at()
    {
        return ProjectionEligibility::AwaitingCurrentProjection;
    }
    if projection.available_at > now {
        return ProjectionEligibility::NotYetAvailable;
    }
    if now >= projection.valid_until {
        return ProjectionEligibility::Expired;
    }
    ProjectionEligibility::Eligible
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;
    use chrono_tz::Tz;

    use super::*;

    #[test]
    fn half_hour_slots_use_dst_correct_et_offsets_and_bounded_grace() {
        let summer = Utc.with_ymd_and_hms(2026, 8, 4, 14, 1, 53).unwrap();
        let slot = active_report_slot(summer, 180).unwrap();
        assert_eq!(slot.source_slot(), "2026-08-04:10:00");
        assert_eq!(slot.ledger_slot(), "2026-08-04T10:00:00-04:00");

        let winter = Utc.with_ymd_and_hms(2026, 1, 5, 15, 30, 30).unwrap();
        let slot = active_report_slot(winter, 180).unwrap();
        assert_eq!(slot.source_slot(), "2026-01-05:10:30");
        assert_eq!(slot.ledger_slot(), "2026-01-05T10:30:00-05:00");

        assert!(active_report_slot(summer + chrono::TimeDelta::seconds(88), 180).is_none());
    }

    #[test]
    fn does_not_schedule_outside_rth_or_on_weekends() {
        let before_open = Utc.with_ymd_and_hms(2026, 8, 4, 13, 0, 30).unwrap();
        let at_close = Utc.with_ymd_and_hms(2026, 8, 4, 20, 0, 30).unwrap();
        let weekend = Utc.with_ymd_and_hms(2026, 8, 8, 14, 0, 30).unwrap();
        assert!(active_report_slot(before_open, 180).is_none());
        assert!(active_report_slot(at_close, 180).is_none());
        assert!(active_report_slot(weekend, 180).is_none());
    }

    #[test]
    fn timezone_constant_is_new_york() {
        let timezone: Tz = New_York;
        assert_eq!(timezone.name(), "America/New_York");
    }
}
