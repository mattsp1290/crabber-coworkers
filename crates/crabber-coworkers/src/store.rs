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
    create_private_file(state_dir, &store_path(state_dir, coworker_id))?;
    let path = store_path(state_dir, coworker_id);
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
    options.open(path).map_err(|_| StoreOpenError)?;
    Ok(())
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
    if !path.is_file() {
        return Ok(None);
    }
    SqliteStore::connect(&path)
        .await
        .map(|store| Some(Arc::new(store)))
        .map_err(|_| StoreOpenError)
}
