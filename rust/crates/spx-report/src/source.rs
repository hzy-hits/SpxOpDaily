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
    match latest.projection.session {
        MarketSession::Rth => {
            let parsed = NaiveDateTime::parse_from_str(value, "%Y-%m-%d:%H:%M")
                .map_err(|_| ProjectionSourceError::InvalidSourceSlot)?;
            if parsed.format("%Y-%m-%d:%H:%M").to_string() != value
                || parsed.date() != latest.projection.trading_date_et
                || !is_rth_time(parsed.time())
            {
                return Err(ProjectionSourceError::InvalidSourceSlot);
            }
        }
        MarketSession::Gth => {
            let expected_prefix = format!("{}:gth:", latest.projection.trading_date_et);
            let time = value
                .strip_prefix(&expected_prefix)
                .and_then(|raw| NaiveTime::parse_from_str(raw, "%H:%M").ok())
                .ok_or(ProjectionSourceError::InvalidSourceSlot)?;
            if format!("{expected_prefix}{}", time.format("%H:%M")) != value || !is_gth_time(time) {
                return Err(ProjectionSourceError::InvalidSourceSlot);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportSlot {
    source_slot: String,
    ledger_slot: String,
    trading_date_et: NaiveDate,
    session: MarketSession,
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

    pub const fn session(&self) -> MarketSession {
        self.session
    }

    pub const fn starts_at(&self) -> DateTime<Utc> {
        self.starts_at
    }

    pub const fn closes_at(&self) -> DateTime<Utc> {
        self.closes_at
    }
}

/// Returns an active GTH or RTH ET quarter-hour slot during its bounded grace window.
///
/// Slot identity matches the Python `order-map-status` timer and desk-map
/// `source_slot` contract: ET `:00` / `:15` / `:30` / `:45`, including the
/// Sunday/weekday GTH open at `20:15`.
pub fn active_report_slot(now: DateTime<Utc>, grace_seconds: i64) -> Option<ReportSlot> {
    let now_et = now.with_timezone(&New_York);
    let boundary_minute = (now_et.minute() / 15) * 15;
    let local = now_et
        .date_naive()
        .and_hms_opt(now_et.hour(), boundary_minute, 0)?;
    let (session, trading_date_et) = scheduled_session(local)?;
    if scheduled_session(now_et.naive_local())? != (session, trading_date_et) {
        return None;
    }
    let slot_start_et = New_York.from_local_datetime(&local).single()?;
    let starts_at = slot_start_et.with_timezone(&Utc);
    let elapsed = now.signed_duration_since(starts_at).num_seconds();
    if elapsed < 0 || elapsed > grace_seconds {
        return None;
    }
    let closes_at = starts_at.checked_add_signed(chrono::TimeDelta::seconds(grace_seconds))?;
    let source_slot = match session {
        MarketSession::Rth => local.format("%Y-%m-%d:%H:%M").to_string(),
        MarketSession::Gth => format!("{trading_date_et}:gth:{}", local.time().format("%H:%M")),
    };
    Some(ReportSlot {
        source_slot,
        ledger_slot: slot_start_et.to_rfc3339_opts(SecondsFormat::Secs, false),
        trading_date_et,
        session,
        starts_at,
        closes_at,
    })
}

fn scheduled_session(local: NaiveDateTime) -> Option<(MarketSession, NaiveDate)> {
    let date = local.date();
    let time = local.time();
    let weekday = date.weekday();
    if is_weekday(weekday) && is_rth_session_time(time) {
        return Some((MarketSession::Rth, date));
    }
    if is_weekday(weekday) && time < NaiveTime::from_hms_opt(9, 25, 0)? {
        return Some((MarketSession::Gth, date));
    }
    if matches!(
        weekday,
        Weekday::Sun | Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu
    ) && time >= NaiveTime::from_hms_opt(20, 15, 0)?
    {
        return Some((MarketSession::Gth, date.succ_opt()?));
    }
    None
}

fn is_weekday(weekday: Weekday) -> bool {
    !matches!(weekday, Weekday::Sat | Weekday::Sun)
}

fn is_quarter_minute(time: NaiveTime) -> bool {
    time.minute().is_multiple_of(15)
}

fn is_rth_session_time(time: NaiveTime) -> bool {
    time >= NaiveTime::from_hms_opt(9, 30, 0).expect("valid RTH open")
        && time < NaiveTime::from_hms_opt(16, 0, 0).expect("valid RTH close")
}

fn is_gth_session_time(time: NaiveTime) -> bool {
    time < NaiveTime::from_hms_opt(9, 25, 0).expect("valid GTH close")
        || time >= NaiveTime::from_hms_opt(20, 15, 0).expect("valid GTH open")
}

fn is_rth_time(time: NaiveTime) -> bool {
    is_quarter_minute(time) && is_rth_session_time(time)
}

fn is_gth_time(time: NaiveTime) -> bool {
    is_quarter_minute(time) && is_gth_session_time(time)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionEligibility {
    Eligible,
    AwaitingCurrentProjection,
    SessionMismatch,
    NotYetAvailable,
    Expired,
}

impl ProjectionEligibility {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Eligible => "eligible",
            Self::AwaitingCurrentProjection => "awaiting_current_projection",
            Self::SessionMismatch => "session_mismatch",
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
    if projection.session != slot.session() {
        return ProjectionEligibility::SessionMismatch;
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
    fn quarter_hour_slots_use_dst_correct_et_offsets_and_bounded_grace() {
        let summer = Utc.with_ymd_and_hms(2026, 8, 4, 14, 1, 53).unwrap();
        let slot = active_report_slot(summer, 180).unwrap();
        assert_eq!(slot.source_slot(), "2026-08-04:10:00");
        assert_eq!(slot.ledger_slot(), "2026-08-04T10:00:00-04:00");

        let winter = Utc.with_ymd_and_hms(2026, 1, 5, 15, 30, 30).unwrap();
        let slot = active_report_slot(winter, 180).unwrap();
        assert_eq!(slot.source_slot(), "2026-01-05:10:30");
        assert_eq!(slot.ledger_slot(), "2026-01-05T10:30:00-05:00");

        let rth_quarter = Utc.with_ymd_and_hms(2026, 8, 4, 14, 15, 45).unwrap();
        let slot = active_report_slot(rth_quarter, 180).unwrap();
        assert_eq!(slot.source_slot(), "2026-08-04:10:15");
        assert_eq!(slot.ledger_slot(), "2026-08-04T10:15:00-04:00");

        assert!(active_report_slot(summer + chrono::TimeDelta::seconds(88), 180).is_none());
    }

    #[test]
    fn does_not_schedule_during_session_gaps_or_on_weekends() {
        let before_open = Utc.with_ymd_and_hms(2026, 8, 4, 13, 27, 30).unwrap();
        let at_close = Utc.with_ymd_and_hms(2026, 8, 4, 20, 0, 30).unwrap();
        let weekend = Utc.with_ymd_and_hms(2026, 8, 8, 14, 0, 30).unwrap();
        assert!(active_report_slot(before_open, 180).is_none());
        assert!(active_report_slot(at_close, 180).is_none());
        assert!(active_report_slot(weekend, 180).is_none());
    }

    #[test]
    fn gth_slots_use_next_trading_date_and_explicit_session_key() {
        let evening = Utc.with_ymd_and_hms(2026, 8, 4, 1, 30, 30).unwrap();
        let slot = active_report_slot(evening, 180).unwrap();
        assert_eq!(slot.session(), MarketSession::Gth);
        assert_eq!(slot.trading_date_et().to_string(), "2026-08-04");
        assert_eq!(slot.source_slot(), "2026-08-04:gth:21:30");
        assert_eq!(slot.ledger_slot(), "2026-08-03T21:30:00-04:00");

        let morning = Utc.with_ymd_and_hms(2026, 8, 4, 13, 0, 30).unwrap();
        let slot = active_report_slot(morning, 180).unwrap();
        assert_eq!(slot.source_slot(), "2026-08-04:gth:09:00");

        let gth_open = Utc.with_ymd_and_hms(2026, 8, 4, 0, 15, 30).unwrap();
        let slot = active_report_slot(gth_open, 180).unwrap();
        assert_eq!(slot.session(), MarketSession::Gth);
        assert_eq!(slot.trading_date_et().to_string(), "2026-08-04");
        assert_eq!(slot.source_slot(), "2026-08-04:gth:20:15");
        assert_eq!(slot.ledger_slot(), "2026-08-03T20:15:00-04:00");

        // Outside the 180s start grace for the 20:15 open slot.
        let after_open_grace = Utc.with_ymd_and_hms(2026, 8, 4, 0, 20, 0).unwrap();
        assert!(active_report_slot(after_open_grace, 180).is_none());
    }

    #[test]
    fn timezone_constant_is_new_york() {
        let timezone: Tz = New_York;
        assert_eq!(timezone.name(), "America/New_York");
    }
}
