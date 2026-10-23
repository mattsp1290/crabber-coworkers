//! Host-owned SQLite paths and private filesystem creation.
use crabber::session::SqliteStore;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

pub const STORE_FILE_EXTENSION: &str = "sqlite";

#[derive(Debug, thiserror::Error)]
#[error("cannot open conversation store")]
pub struct StoreOpenError;

#[must_use]
pub fn store_path(state_dir: &Path, coworker_id: &str) -> PathBuf {
    state_dir.join(format!("{coworker_id}.{STORE_FILE_EXTENSION}"))
}

/// Create a private store directory and initialize the coworker's database.
///
/// # Errors
/// Returns a fixed error for filesystem, migration, or connection failures.
pub async fn open(state_dir: &Path, coworker_id: &str) -> Result<Arc<SqliteStore>, StoreOpenError> {
    let path = store_path(state_dir, coworker_id);
    create_private_file(state_dir, &path)?;
    SqliteStore::migrate(&path)
        .await
        .map_err(|_| StoreOpenError)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .map_err(|_| StoreOpenError)?;
    }
    SqliteStore::connect(&path)
        .await
        .map(Arc::new)
        .map_err(|_| StoreOpenError)
}

fn create_private_file(state_dir: &Path, path: &Path) -> Result<(), StoreOpenError> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(state_dir).map_err(|_| StoreOpenError)?;
    if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(StoreOpenError);
    }
    let mut options = fs::OpenOptions::new();
    options.create(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(|_| StoreOpenError)?;
    #[cfg(unix)]
    {
        restrict_file(&file)?;
        for suffix in ["-wal", "-shm"] {
            restrict_sidecar(path, suffix)?;
        }
    }
    drop(file);
    Ok(())
}

#[cfg(unix)]
fn restrict_file(file: &fs::File) -> Result<(), StoreOpenError> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| StoreOpenError)
}

#[cfg(unix)]
fn restrict_sidecar(database: &Path, suffix: &str) -> Result<(), StoreOpenError> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut path = database.as_os_str().to_os_string();
    path.push(suffix);
    match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) if file.metadata().map_err(|_| StoreOpenError)?.is_file() => restrict_file(&file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) | Err(_) => Err(StoreOpenError),
    }
}

/// Connect to an existing coworker store without migration or permission changes.
///
/// # Errors
/// Returns a fixed error when an existing database cannot be opened.
pub async fn connect_existing(
    state_dir: &Path,
    coworker_id: &str,
) -> Result<Option<Arc<SqliteStore>>, StoreOpenError> {
    let path = store_path(state_dir, coworker_id);
    match fs::metadata(&path) {
        Ok(metadata) if metadata.is_file() => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(_) | Err(_) => return Err(StoreOpenError),
    }
    let absolute = fs::canonicalize(&path).map_err(|_| StoreOpenError)?;
    let mut uri = url::Url::from_file_path(absolute).map_err(|()| StoreOpenError)?;
    uri.set_query(Some("mode=ro"));
    // The pinned SQLite driver enables URI filenames. mode=ro prevents even a
    // connection-close WAL checkpoint; query_only alone would not do so.
    SqliteStore::connect(Path::new(uri.as_str()))
        .await
        .map(|store| Some(Arc::new(store)))
        .map_err(|_| StoreOpenError)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn preexisting_file_is_private_before_migration() {
        let dir = tempfile::tempdir().unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
        let path = store_path(dir.path(), "scout");
        fs::write(&path, "").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        create_private_file(dir.path(), &path).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        // This helper completes before the first call into SQLite migration.
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    }
}
