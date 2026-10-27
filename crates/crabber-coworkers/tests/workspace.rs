mod common;
use crabber_coworkers::{
    ask::AskError,
    config::{self, CoworkerDefinition, WorkspaceAccess},
    workspace::{self, Workspace},
};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
    time::Duration,
};

fn definition(id: &str, access: WorkspaceAccess) -> CoworkerDefinition {
    CoworkerDefinition {
        id: id.into(),
        name: "Test".into(),
        role: "Writer".into(),
        instructions: "Write drafts.".into(),
        slack: None,
        workspace: access,
    }
}
async fn admit(state: &Path, id: &str, access: WorkspaceAccess) -> Result<Workspace, AskError> {
    workspace::admit(
        &definition(id, access),
        state,
        tokio::time::Instant::now() + Duration::from_secs(5),
        &mut common::never(),
    )
    .await
}
fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

#[test]
fn grants_parse_strictly_with_disabled_default() {
    let temp = tempfile::tempdir().unwrap();
    let path = common::write_state_config(temp.path(), 5);
    let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    value["coworkers"][0]
        .as_object_mut()
        .unwrap()
        .remove("workspace");
    fs::write(&path, value.to_string()).unwrap();
    assert_eq!(
        config::load(&path).unwrap().coworkers[0].workspace,
        WorkspaceAccess::Disabled
    );
    for (name, expected) in [
        ("disabled", WorkspaceAccess::Disabled),
        ("read_only", WorkspaceAccess::ReadOnly),
        ("read_write", WorkspaceAccess::ReadWrite),
    ] {
        value["coworkers"][0]["workspace"] = name.into();
        fs::write(&path, value.to_string()).unwrap();
        let def = config::load(&path).unwrap().coworkers.remove(0);
        assert_eq!(def.workspace, expected);
        assert_eq!(serde_json::to_value(def).unwrap()["workspace"], name);
    }
    value["coworkers"][0]["workspace"] = "secret-path-marker".into();
    fs::write(&path, value.to_string()).unwrap();
    let error = config::load(&path).unwrap_err().to_string();
    assert!(error.ends_with("(data)"));
    assert!(!error.contains("secret-path-marker"));
}

#[tokio::test]
async fn roots_are_private_distinct_persistent_and_outside_sqlite() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("absent/state");
    let scout = admit(&state, "scout", WorkspaceAccess::ReadWrite)
        .await
        .unwrap();
    let quill = admit(&state, "quill", WorkspaceAccess::ReadOnly)
        .await
        .unwrap();
    let root = Path::new(scout.directory());
    assert_ne!(scout.directory(), quill.directory());
    assert_eq!(root, state.canonicalize().unwrap().join("workspaces/scout"));
    assert_eq!(mode(&state), 0o700);
    assert_eq!(mode(&state.join("workspaces")), 0o700);
    assert_eq!(mode(root), 0o700);
    assert!(!state.join("scout.sqlite").exists());
    fs::write(root.join("draft.txt"), "saved draft").unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(root, fs::Permissions::from_mode(0o777)).unwrap();
    let reopened = admit(&state, "scout", WorkspaceAccess::ReadWrite)
        .await
        .unwrap();
    assert_eq!(
        fs::read(Path::new(reopened.directory()).join("draft.txt")).unwrap(),
        b"saved draft"
    );
    assert_eq!(mode(&state), 0o755);
    assert_eq!(mode(root), 0o700);
    let other = admit(
        &temp.path().join("other"),
        "scout",
        WorkspaceAccess::ReadWrite,
    )
    .await
    .unwrap();
    assert_ne!(scout.directory(), other.directory());
    assert!(scout.extension().unwrap().is_some());
    assert!(
        scout
            .validate(&definition("quill", WorkspaceAccess::ReadWrite))
            .is_err()
    );
    assert!(
        scout
            .validate(&definition("scout", WorkspaceAccess::ReadOnly))
            .is_err()
    );
}

#[tokio::test]
async fn disabled_cancelled_and_invalid_admission_have_no_side_effects() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("missing");
    let disabled = admit(&state, "scout", WorkspaceAccess::Disabled)
        .await
        .unwrap();
    assert_eq!(disabled.directory(), ".");
    assert_eq!(disabled.tools(), []);
    assert!(disabled.extension().unwrap().is_none());
    assert!(!state.exists());
    let mut cancel: crabber_coworkers::ask::Cancel = Box::pin(std::future::ready(()));
    assert!(matches!(
        workspace::admit(
            &definition("scout", WorkspaceAccess::ReadWrite),
            &state,
            tokio::time::Instant::now() + Duration::from_secs(5),
            &mut cancel
        )
        .await,
        Err(AskError::Interrupted)
    ));
    assert!(matches!(
        admit(&state, "../secret-path-marker", WorkspaceAccess::ReadWrite).await,
        Err(AskError::Workspace)
    ));
    assert!(!state.exists());
    assert_eq!(AskError::Workspace.to_string(), "cannot use workspace");
}

#[tokio::test]
async fn dedicated_symlinks_and_nondirectories_are_refused_without_touching_targets() {
    for component in ["workspaces", "workspaces/scout"] {
        for link in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let state = temp.path().join("state");
            let target = temp.path().join("secret-path-marker");
            fs::create_dir_all(&target).unwrap();
            fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).unwrap();
            fs::write(target.join("sentinel"), "untouched").unwrap();
            let entry = state.join(component);
            fs::create_dir_all(entry.parent().unwrap()).unwrap();
            if link {
                symlink(&target, &entry).unwrap();
            } else {
                fs::write(&entry, "untouched").unwrap();
            }
            assert!(matches!(
                admit(&state, "scout", WorkspaceAccess::ReadWrite).await,
                Err(AskError::Workspace)
            ));
            assert_eq!(mode(&target), 0o755);
            assert_eq!(fs::read(target.join("sentinel")).unwrap(), b"untouched");
            if !link {
                assert_eq!(fs::read(entry).unwrap(), b"untouched");
            }
        }
    }
}

#[tokio::test]
async fn non_utf8_canonical_directory_is_refused() {
    use std::os::unix::ffi::OsStringExt;
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join(std::ffi::OsString::from_vec(vec![255]));
    assert!(matches!(
        admit(&state, "scout", WorkspaceAccess::ReadWrite).await,
        Err(AskError::Workspace)
    ));
}
