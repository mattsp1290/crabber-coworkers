//! What every frontend shares to run a turn: the execution backend, the global turn cap, and
//! each coworker's local conversation store and workspace.
use crate::{
    app::Provider,
    ask::{AskError, Cancel},
    config::Config,
    store,
    workspace::Workspace,
};
use crabber::{Selection, providers::Resolver, session::SqliteStore};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// Where turns execute.
pub enum Backend {
    /// In this process, against each coworker's [`LocalState`].
    Local {
        resolver: Arc<dyn Resolver>,
        selection: Selection,
    },
    /// In the coworker's borb worker.
    Borb(crate::execution::Remote),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BackendError {
    #[error("OPENCODE_GO_API_KEY is not set")]
    MissingApiKey,
    #[error("cannot configure borb execution; check BORB_TOKEN and borb settings")]
    Borb,
}

impl Backend {
    pub(crate) fn new(
        config: &Config,
        provider: Provider,
        selection: Selection,
    ) -> Result<Self, BackendError> {
        Ok(match (&config.borb, provider) {
            (_, Provider::Remote(remote)) => Self::Borb(remote),
            (Some(settings), Provider::FromEnv) => Self::Borb(
                crate::execution::Remote::new(
                    settings,
                    std::env::var("BORB_TOKEN").map_err(|_| BackendError::Borb)?,
                )
                .map_err(|_| BackendError::Borb)?,
            ),
            (_, provider) => Self::Local {
                resolver: provider.resolve().ok_or(BackendError::MissingApiKey)?,
                selection,
            },
        })
    }
}

/// The process-wide cap on concurrent model turns (`limits.max_concurrent_turns`). Clones share
/// one budget, so every frontend draws from the same cap.
#[derive(Clone)]
pub struct TurnPermits(Arc<Semaphore>);

impl TurnPermits {
    #[must_use]
    pub fn new(turns: usize) -> Self {
        Self(Arc::new(Semaphore::new(turns)))
    }

    /// Wait for a free turn. The permit frees it when dropped.
    ///
    /// # Panics
    /// Never: the semaphore is private and is never closed.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        self.0
            .clone()
            .acquire_owned()
            .await
            .expect("turn permits stay open")
    }

    #[must_use]
    pub fn available(&self) -> usize {
        self.0.available_permits()
    }
}

/// One coworker's durable conversations and admitted workspace on this host.
pub struct LocalState {
    pub(crate) store: Arc<SqliteStore>,
    pub(crate) workspace: Workspace,
}

#[derive(Debug, thiserror::Error)]
pub enum LocalError {
    #[error("interrupted")]
    Interrupted,
    #[error("cannot use workspace")]
    Workspace,
    #[error(transparent)]
    Store(#[from] store::StoreOpenError),
}

impl LocalState {
    async fn open(config: &Config, index: usize, stop: &mut Cancel) -> Result<Self, LocalError> {
        let def = &config.coworkers[index];
        let deadline = Instant::now() + Duration::from_secs(config.limits.model_turn_seconds);
        let workspace = crate::workspace::admit(def, &config.state_dir, deadline, stop)
            .await
            .map_err(|error| match error {
                AskError::Interrupted => LocalError::Interrupted,
                _ => LocalError::Workspace,
            })?;
        let store = tokio::select! {
            biased;
            () = &mut *stop => return Err(LocalError::Interrupted),
            result = store::open(&config.state_dir, &def.id) => result?,
        };
        Ok(Self { store, workspace })
    }
}

/// The coworkers a process runs turns for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Served {
    /// Only coworkers with a `slack` block.
    Slack,
    /// Every configured coworker, as the HTTP API serves them all.
    Every,
}

/// Open the local state of every configured coworker, keyed by coworker ID. A borb backend keeps
/// conversations in its workers, so it opens nothing.
///
/// # Errors
/// Fails on the first coworker whose workspace or store cannot be opened, or when `stop` fires.
pub async fn open_all(
    config: &Config,
    backend: &Backend,
    stop: &mut Cancel,
) -> Result<HashMap<String, Arc<LocalState>>, LocalError> {
    open_served(config, backend, Served::Every, stop).await
}

/// Open the local state of the coworkers in `served`, as [`open_all`] does for all of them.
///
/// # Errors
/// Fails on the first coworker whose workspace or store cannot be opened, or when `stop` fires.
pub async fn open_served(
    config: &Config,
    backend: &Backend,
    served: Served,
    stop: &mut Cancel,
) -> Result<HashMap<String, Arc<LocalState>>, LocalError> {
    let mut locals = HashMap::new();
    if matches!(backend, Backend::Local { .. }) {
        for (index, def) in config.coworkers.iter().enumerate() {
            if served == Served::Slack && def.slack.is_none() {
                continue;
            }
            let local = LocalState::open(config, index, stop).await?;
            locals.insert(def.id.clone(), Arc::new(local));
        }
    }
    Ok(locals)
}
