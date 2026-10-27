use crate::{
    ask::{AskError, Cancel},
    config::{CoworkerDefinition, WorkspaceAccess, valid_id},
};
use crabber_tools_catalog::prelude::{
    EnabledSet, EnvPolicy, Limits, Policies, SearchPolicy, ShellPolicy, StandardTools, StartupMode,
    ToolId, WorkspaceRoot,
};
use std::{fs, path::Path, sync::Arc, time::Duration};

pub struct Workspace {
    owner: String,
    grant: Grant,
}

enum Grant {
    Disabled,
    ReadOnly(AdmittedRoot),
    ReadWrite(AdmittedRoot),
}

struct AdmittedRoot {
    directory: String,
    root: Arc<WorkspaceRoot>,
    search: SearchPolicy,
}

impl Workspace {
    /// Construct a disabled context without filesystem or executable access.
    ///
    /// # Errors
    /// Rejects invalid owner IDs and enabled grants.
    pub fn disabled(def: &CoworkerDefinition) -> Result<Self, AskError> {
        if !valid_id(&def.id) || def.workspace != WorkspaceAccess::Disabled {
            return Err(AskError::Workspace);
        }
        Ok(Self {
            owner: def.id.clone(),
            grant: Grant::Disabled,
        })
    }

    #[must_use]
    pub fn directory(&self) -> &str {
        match &self.grant {
            Grant::Disabled => ".",
            Grant::ReadOnly(root) | Grant::ReadWrite(root) => &root.directory,
        }
    }

    #[must_use]
    pub fn access(&self) -> WorkspaceAccess {
        match self.grant {
            Grant::Disabled => WorkspaceAccess::Disabled,
            Grant::ReadOnly(_) => WorkspaceAccess::ReadOnly,
            Grant::ReadWrite(_) => WorkspaceAccess::ReadWrite,
        }
    }

    /// Verify that a context belongs to the current definition and grant.
    ///
    /// # Errors
    /// Rejects authority reused for a different coworker or grant.
    pub fn validate(&self, def: &CoworkerDefinition) -> Result<(), AskError> {
        if self.owner == def.id && self.access() == def.workspace {
            Ok(())
        } else {
            Err(AskError::Workspace)
        }
    }

    #[must_use]
    pub fn tools(&self) -> &'static [ToolId] {
        match self.grant {
            Grant::Disabled => &[],
            Grant::ReadOnly(_) => &[ToolId::FileRead, ToolId::FileList],
            Grant::ReadWrite(_) => &[
                ToolId::FileRead,
                ToolId::FileList,
                ToolId::FileWrite,
                ToolId::FileEdit,
            ],
        }
    }

    /// Build only the admitted file tools with fixed execution policies.
    ///
    /// # Errors
    /// Returns a fixed error if catalog construction fails.
    pub fn extension(&self) -> Result<Option<StandardTools>, AskError> {
        let root = match &self.grant {
            Grant::Disabled => return Ok(None),
            Grant::ReadOnly(root) | Grant::ReadWrite(root) => root,
        };
        StandardTools::new(
            root.root.clone(),
            Policies {
                enabled: EnabledSet::only(self.tools().iter().copied()),
                restrict_to_enabled: true,
                shell: ShellPolicy {
                    shell_binary: "/bin/sh".into(),
                    startup_mode: StartupMode::NonLogin,
                    env: EnvPolicy::Replace(vec![]),
                    run_as: None,
                    output_cap_bytes: 65_536,
                },
                search: root.search.clone(),
                url_fetch: None,
                tracker: None,
                user_interact: None,
                limits: Limits {
                    max_in_flight: 4,
                    max_blocking_wait: Duration::from_secs(2),
                },
            },
        )
        .map(Some)
        .map_err(|_| AskError::Workspace)
    }
}

/// Admit only the coworker's dedicated child beneath the execution-side state directory.
///
/// # Errors
/// Returns fixed errors for invalid identity, unsafe directories, missing executables,
/// cancellation, and deadline expiry. A started filesystem operation can finish later.
pub async fn admit(
    def: &CoworkerDefinition,
    state_dir: &Path,
    deadline: tokio::time::Instant,
    cancel: &mut Cancel,
) -> Result<Workspace, AskError> {
    if !valid_id(&def.id) {
        return Err(AskError::Workspace);
    }
    tokio::select! {
        biased;
        () = &mut *cancel => return Err(AskError::Interrupted),
        () = tokio::time::sleep_until(deadline) => return Err(AskError::Timeout),
        () = std::future::ready(()) => {},
    }
    if def.workspace == WorkspaceAccess::Disabled {
        return Workspace::disabled(def);
    }
    let def = def.clone();
    let state_dir = state_dir.to_owned();
    let admission = tokio::task::spawn_blocking(move || {
        let search = SearchPolicy::resolve_rg_from_path(EnvPolicy::Replace(vec![]))
            .map_err(|_| AskError::Workspace)?;
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&state_dir)
            .map_err(|_| AskError::Workspace)?;
        let state_dir = state_dir.canonicalize().map_err(|_| AskError::Workspace)?;
        let container = private_child(&state_dir, "workspaces")?;
        let path = private_child(&container, &def.id)?;
        let directory = path.to_str().ok_or(AskError::Workspace)?.to_owned();
        let root = WorkspaceRoot::open(&path).map_err(|_| AskError::Workspace)?;
        let admitted = AdmittedRoot {
            directory,
            root,
            search,
        };
        let grant = match def.workspace {
            WorkspaceAccess::ReadOnly => Grant::ReadOnly(admitted),
            WorkspaceAccess::ReadWrite => Grant::ReadWrite(admitted),
            WorkspaceAccess::Disabled => {
                unreachable!("disabled returns before filesystem admission")
            }
        };
        Ok(Workspace {
            owner: def.id,
            grant,
        })
    });
    tokio::select! {
        biased;
        () = cancel => Err(AskError::Interrupted),
        () = tokio::time::sleep_until(deadline) => Err(AskError::Timeout),
        result = admission => result.map_err(|_| AskError::Workspace)?,
    }
}

fn private_child(parent: &Path, name: &str) -> Result<std::path::PathBuf, AskError> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    let child = parent.join(name);
    match fs::DirBuilder::new().mode(0o700).create(&child) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(AskError::Workspace),
    }
    let metadata = fs::symlink_metadata(&child).map_err(|_| AskError::Workspace)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(AskError::Workspace);
    }
    let canonical = child.canonicalize().map_err(|_| AskError::Workspace)?;
    if canonical.parent() != Some(parent) {
        return Err(AskError::Workspace);
    }
    fs::set_permissions(&canonical, fs::Permissions::from_mode(0o700))
        .map_err(|_| AskError::Workspace)?;
    Ok(canonical)
}
