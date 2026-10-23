use crabber::{
    ManageSession, SessionChange, SessionDefaults,
    core::{ControlId, SessionId, SessionRevision},
    session::Store,
};
use crabber_coworkers::store::{self, StoreOpenError};
use std::path::Path;

#[cfg(unix)]
fn mode(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

#[tokio::test]
async fn open_creates_dir_and_file() {
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("nested");
    let dir = parent.join("state");
    let store = store::open(&dir, "scout").await.unwrap();
    let path = store::store_path(&dir, "scout");
    assert!(path.is_file());
    store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: SessionId::from("weekly"),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "scout".into(),
                directory: ".".into(),
                title: "weekly".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    #[cfg(unix)]
    {
        assert_eq!(mode(&parent), 0o700);
        assert_eq!(mode(&dir), 0o700);
        for file in [
            path.clone(),
            dir.join("scout.sqlite-wal"),
            dir.join("scout.sqlite-shm"),
        ] {
            assert!(file.exists());
            assert_eq!(mode(&file), 0o600);
        }
    }
}

#[tokio::test]
async fn open_is_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    drop(store::open(temp.path(), "scout").await.unwrap());
    assert!(store::open(temp.path(), "scout").await.is_ok());
}

#[tokio::test]
async fn connect_existing_without_file_is_none() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("missing");
    assert!(
        store::connect_existing(&dir, "scout")
            .await
            .unwrap()
            .is_none()
    );
    assert!(!dir.exists());
}

#[tokio::test]
async fn unwritable_state_dir_is_redacted() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("secret-marker");
    std::fs::write(&path, "x").unwrap();
    assert!(store::open(&path, "scout").await.is_err());
    assert_eq!(StoreOpenError.to_string(), "cannot open conversation store");
}

#[test]
fn store_path_shape() {
    assert_eq!(
        store::store_path(Path::new("/x"), "scout"),
        Path::new("/x/scout.sqlite")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn existing_modes_and_read_only_listing() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = store::store_path(temp.path(), "scout");
    std::fs::File::create(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let store = store::open(temp.path(), "scout").await.unwrap();
    assert_eq!(mode(temp.path()), 0o755);
    assert_eq!(mode(&path), 0o600);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    let _existing = store::connect_existing(temp.path(), "scout")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mode(&path), 0o644);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), len);
    drop(store);
    let _restored = store::open(temp.path(), "scout").await.unwrap();
    assert_eq!(mode(&path), 0o600);
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_store_file_is_refused() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let target = temp.path().join("target");
    std::fs::write(&target, "secret-marker").unwrap();
    let original_mode = mode(&target);
    symlink(&target, store::store_path(temp.path(), "scout")).unwrap();
    assert!(store::open(temp.path(), "scout").await.is_err());
    assert_eq!(std::fs::read(&target).unwrap(), b"secret-marker");
    assert_eq!(mode(&target), original_mode);
}
