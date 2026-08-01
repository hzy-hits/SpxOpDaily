use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};
use spx_domain::canonical_json_hash;

use crate::LedgerError;
use crate::schema::{MIGRATION_1, MIGRATION_BOOTSTRAP};

const MIGRATION_VERSION: i64 = 1;

#[derive(Debug, Clone)]
pub struct Ledger {
    path: PathBuf,
}

/// Read-only view of an already initialized operational ledger.
///
/// This type cannot migrate, create, claim, acknowledge, or replay state.
#[derive(Debug, Clone)]
pub struct LedgerReader {
    path: PathBuf,
}

impl Ledger {
    /// Opens the operational ledger and verifies its forward-only migration checksum.
    ///
    /// # Errors
    ///
    /// Returns an error for I/O, `SQLite`, integrity, or migration drift failures.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LedgerError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let ledger = Self { path };
        ledger.migrate()?;
        Ok(ledger)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Runs `SQLite`'s lightweight integrity check.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` reports corruption or cannot read the ledger.
    pub fn quick_check(&self) -> Result<(), LedgerError> {
        quick_check_connection(&self.connection()?)
    }

    fn migrate(&self) -> Result<(), LedgerError> {
        let mut connection = self.connection()?;
        connection.execute_batch(MIGRATION_BOOTSTRAP)?;
        let expected_checksum = expected_migration_checksum()?;
        let migrations = read_migrations(&connection)?;
        if migrations.is_empty() {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(MIGRATION_1)?;
            transaction.execute(
                "INSERT INTO schema_migrations(
                    version, name, checksum_sha256, applied_at_us
                 ) VALUES (?1, 'initial_operational_ledger', ?2, ?3)",
                params![MIGRATION_VERSION, expected_checksum, micros(Utc::now())],
            )?;
            transaction.commit()?;
        } else {
            verify_migrations(&migrations, &expected_checksum)?;
        }
        Ok(())
    }

    pub(crate) fn connection(&self) -> Result<Connection, LedgerError> {
        let connection = Connection::open(&self.path)?;
        harden_file(&self.path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA trusted_schema = OFF;",
        )?;
        harden_file_if_present(Path::new(&format!("{}-wal", self.path.display())))?;
        harden_file_if_present(Path::new(&format!("{}-shm", self.path.display())))?;
        Ok(connection)
    }
}

impl LedgerReader {
    /// Opens an existing ledger without creating directories, files, or applying migrations.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is missing, not a regular file, unreadable, or has an
    /// unsupported migration checksum.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, LedgerError> {
        let path = path.as_ref().to_path_buf();
        if !std::fs::metadata(&path)?.is_file() {
            return Err(LedgerError::InvalidValue(
                "ledger path is not a regular file",
            ));
        }
        let reader = Self { path };
        let connection = reader.connection()?;
        let migrations = read_migrations(&connection)?;
        let expected_checksum = expected_migration_checksum()?;
        verify_migrations(&migrations, &expected_checksum)?;
        Ok(reader)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Runs `SQLite`'s lightweight integrity check through a read-only connection.
    ///
    /// # Errors
    ///
    /// Returns an error when the ledger is unreadable or fails the check.
    pub fn quick_check(&self) -> Result<(), LedgerError> {
        quick_check_connection(&self.connection()?)
    }

    pub(crate) fn connection(&self) -> Result<Connection, LedgerError> {
        let connection = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA query_only = ON;
             PRAGMA foreign_keys = ON;
             PRAGMA trusted_schema = OFF;",
        )?;
        Ok(connection)
    }
}

fn quick_check_connection(connection: &Connection) -> Result<(), LedgerError> {
    let result: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    if result == "ok" {
        Ok(())
    } else {
        Err(LedgerError::InvalidValue("SQLite quick_check"))
    }
}

fn expected_migration_checksum() -> Result<String, LedgerError> {
    Ok(canonical_json_hash(&MIGRATION_1)?)
}

fn read_migrations(connection: &Connection) -> Result<Vec<(i64, String, String)>, LedgerError> {
    let mut statement = connection
        .prepare("SELECT version, name, checksum_sha256 FROM schema_migrations ORDER BY version")?;
    Ok(statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?)
}

fn verify_migrations(
    migrations: &[(i64, String, String)],
    expected_checksum: &str,
) -> Result<(), LedgerError> {
    if migrations
        == [(
            MIGRATION_VERSION,
            "initial_operational_ledger".to_owned(),
            expected_checksum.to_owned(),
        )]
    {
        Ok(())
    } else {
        Err(LedgerError::MigrationDrift)
    }
}

pub(crate) fn micros(value: DateTime<Utc>) -> i64 {
    value.timestamp_micros()
}

#[cfg(unix)]
fn harden_file(path: &Path) -> Result<(), std::io::Error> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn harden_file(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

fn harden_file_if_present(path: &Path) -> Result<(), std::io::Error> {
    match harden_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
