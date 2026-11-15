use crate::{
    bridge,
    config::{Config, CoworkerDefinition},
    workspace::Workspace,
};
use crabber::{
    Selection,
    core::{ContentBlock, Message, PartKind, Role, RunId, RunStatus},
    providers::Resolver,
    session::{MemoryStore, Store},
};
use std::{convert::Infallible, future::Future, pin::Pin, sync::Arc, time::Duration};

mod keyed;
#[cfg(test)]
pub(crate) use keyed::read_faults;
pub use keyed::{
    KeyedOutcome, KeyedRequest, Orphan, Outstanding, Reconciled, Recovery, Unreadable, Unsettled,
    abandon_unstarted, ask_keyed, reconcile_keyed, recover_keyed, retire_started,
};

pub type Cancel = Pin<Box<dyn Future<Output = ()> + Send>>;
pub const INTERRUPT_SETTLE_SECONDS: u64 = 5;
pub enum Session {
    Ephemeral,
    Durable {
        store: Arc<crabber::session::SqliteStore>,
        name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AskError {
    #[error("invalid conversation name")]
    InvalidConversation,
    #[error("cannot open conversation")]
    Conversation,
    #[error("conversation is busy")]
    Busy,
    #[error("interrupted")]
    Interrupted,
    #[error("unknown coworker")]
    UnknownCoworker,
    #[error("prompt is empty")]
    EmptyPrompt,
    #[error("prompt exceeds max_prompt_bytes")]
    PromptTooLarge,
    #[error("cannot use workspace")]
    Workspace,
    #[error("agent construction failed")]
    Build,
    #[error("agent cleanup failed")]
    Cleanup,
    #[error("model run failed")]
    Run,
    #[error("model run did not complete")]
    NotCompleted,
    #[error("model run produced no answer")]
    NoAnswer,
    #[error("model turn timed out")]
    Timeout,
}

impl AskError {
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::UnknownCoworker
            | Self::EmptyPrompt
            | Self::PromptTooLarge
            | Self::InvalidConversation => 2,
            Self::Interrupted => 130,
            Self::Workspace
            | Self::Cleanup
            | Self::Conversation
            | Self::Busy
            | Self::Build
            | Self::Run
            | Self::NotCompleted
            | Self::NoAnswer
            | Self::Timeout => 1,
        }
    }
}

/// Validate caller input before constructing or invoking an agent.
///
/// # Errors
/// Rejects unknown coworker IDs, blank prompts, and prompts above the configured byte limit.
pub fn validate_request<'a>(
    config: &'a Config,
    coworker_id: &str,
    prompt: &str,
    conversation: Option<&str>,
) -> Result<&'a CoworkerDefinition, AskError> {
    let def = config
        .coworker(coworker_id)
        .ok_or(AskError::UnknownCoworker)?;
    if prompt.trim().is_empty() {
        return Err(AskError::EmptyPrompt);
    }
    if prompt.len() > config.limits.max_prompt_bytes {
        return Err(AskError::PromptTooLarge);
    }
    if conversation.is_some_and(|name| !crate::config::valid_id(name)) {
        return Err(AskError::InvalidConversation);
    }
    Ok(def)
}

/// Run a prompt and return only this run's committed assistant text.
///
/// # Errors
/// Returns fixed errors for session, admission, interruption, and answer failures.
#[allow(clippy::too_many_arguments)]
pub async fn ask(
    def: &CoworkerDefinition,
    workspace: &Workspace,
    prompt: &str,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
    deadline: tokio::time::Instant,
    session: Session,
    cancel: Cancel,
) -> Result<String, AskError> {
    let (store, session_id): (Arc<dyn Store>, _) = match session {
        Session::Ephemeral => (Arc::new(MemoryStore::new()), None),
        Session::Durable { store, name } => (store, Some(crabber::SessionId::from(name.as_str()))),
    };
    let host = Host {
        def,
        workspace,
        resolver,
        selection,
        store,
    };
    let (admitted, text) = (session_id.clone(), prompt.to_owned());
    let mut cancel = Signal::new(cancel);
    // An unkeyed admission is awaited to its verdict whatever the cancel signal says: nothing
    // identifies it afterwards, so a wait given up could never be reconciled.
    let turn = turn::<Infallible, Infallible>(
        &host,
        session_id.as_ref(),
        deadline,
        &mut cancel,
        move |agent, _| {
            Box::pin(async move {
                let run = agent.prompt(admitted, text).await;
                run.map(Admitted::Started)
                    .map_err(|error| admission_error(&error))
            })
        },
    )
    .await?;
    match turn {
        Turn::Answered(answer) => Ok(answer),
        Turn::Ended(never) | Turn::Outstanding(never) => match never {},
    }
}

/// A turn's cancel signal, which can be waited on again after it has fired. [`Cancel`] is any
/// future, and a future must not be polled once it has completed.
struct Signal {
    cancel: Cancel,
    fired: bool,
}

impl Signal {
    fn new(cancel: Cancel) -> Self {
        Self {
            cancel,
            fired: false,
        }
    }

    /// Resolves when the signal fires, and at once every time after that.
    async fn fired(&mut self) {
        if !self.fired {
            (&mut self.cancel).await;
            self.fired = true;
        }
    }
}

/// What one turn runs against.
struct Host<'a> {
    def: &'a CoworkerDefinition,
    workspace: &'a Workspace,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
    store: Arc<dyn Store>,
}

/// What an admission call granted.
enum Admitted<R, L> {
    /// The run's only execution handle. The turn waits for the run as [`answer`] does.
    Started(crabber::RunHandle),
    /// The admission started its run and has already waited for it: this is its answer.
    Answered(String),
    /// Nothing is left for the turn to wait for, and `R` says why: nothing may execute here, or
    /// what the admission started has already ended or been given up.
    Ended(R),
    /// No verdict yet. Crabber is still working on the admission, and `L` keeps both it and the
    /// turn's agent alive.
    Outstanding(L),
}

type Admitting<'a, R, L> =
    Pin<Box<dyn Future<Output = Result<Admitted<R, L>, AskError>> + Send + 'a>>;

enum Turn<R, L> {
    Answered(String),
    /// The turn has no answer, and `R` says why.
    Ended(R),
    /// The turn ended without its admission's verdict and without cleaning up its agent. `L`
    /// owns both from here on.
    Outstanding(L),
}

/// Where a turn failed, relative to the run it was meant to start.
enum TurnError {
    /// Before this call started any run.
    Unstarted(AskError),
    /// After this call started its run: while [`answer`] waited for the run, or while an
    /// answered turn's agent was cleaned up.
    Started(AskError),
}

impl From<TurnError> for AskError {
    fn from(error: TurnError) -> Self {
        match error {
            TurnError::Unstarted(error) | TurnError::Started(error) => error,
        }
    }
}

fn admission_error(error: &crabber::RuntimeError) -> AskError {
    match error {
        crabber::RuntimeError::SessionBusy
        | crabber::RuntimeError::Store(crabber::session::StoreError::Busy) => AskError::Busy,
        _ => AskError::Run,
    }
}

/// The body every turn shares: build the agent, establish the session, admit through `admit`,
/// wait for the run under `deadline` and `cancel`, read its committed answer, and clean up.
/// `admit` is handed the turn's cancel signal so that an admission which can be reconciled
/// afterwards may stop waiting for crabber's verdict, and may wait for the run it starts in its
/// own way. It is handed the agent as an [`Arc`] so that such an admission can outlive the turn:
/// crabber goes on preparing an admission whose waiter is dropped, so one that has no verdict
/// yet is kept, not dropped.
async fn turn<R: Send, L: Send>(
    host: &Host<'_>,
    session_id: Option<&crabber::SessionId>,
    deadline: tokio::time::Instant,
    cancel: &mut Signal,
    admit: impl for<'a> FnOnce(&'a Arc<crabber::Agent>, &'a mut Signal) -> Admitting<'a, R, L> + Send,
) -> Result<Turn<R, L>, TurnError> {
    let Host {
        def,
        workspace,
        resolver,
        selection,
        store,
    } = host;
    let agent = bridge::build_agent(
        def,
        workspace,
        store.clone(),
        resolver.clone(),
        selection.clone(),
    )
    .map_err(TurnError::Unstarted)?;
    let agent = Arc::new(agent);
    let result = async {
        if let Some(id) = session_id {
            tokio::select! {
                biased;
                () = cancel.fired() => return Err(TurnError::Unstarted(AskError::Interrupted)),
                () = tokio::time::sleep_until(deadline) => return Err(TurnError::Unstarted(AskError::Timeout)),
                result = ensure_session(&agent, store.as_ref(), def, workspace, id) => result.map_err(TurnError::Unstarted)?,
            }
        }
        tokio::select! {
            biased;
            () = cancel.fired() => return Err(TurnError::Unstarted(AskError::Interrupted)),
            () = tokio::time::sleep_until(deadline) => return Err(TurnError::Unstarted(AskError::Timeout)),
            () = std::future::ready(()) => {},
        }
        let admitted = admit(&agent, cancel).await;
        let run = match admitted.map_err(TurnError::Unstarted)? {
            Admitted::Started(run) => run,
            Admitted::Answered(answer) => return Ok(Turn::Answered(answer)),
            Admitted::Ended(reason) => return Ok(Turn::Ended(reason)),
            Admitted::Outstanding(admission) => return Ok(Turn::Outstanding(admission)),
        };
        answer(store.as_ref(), run, cancel, deadline)
            .await
            .map(Turn::Answered)
            .map_err(TurnError::Started)
    }
    .await;
    if let Ok(Turn::Outstanding(_)) = &result {
        // Closing the agent now would not stop the admission, and draining it would only wait
        // for it. Whoever resolves the admission cleans up.
        return result;
    }
    let cleaned = cleanup(&agent).await;
    match result {
        Ok(Turn::Answered(_)) if !cleaned => Err(TurnError::Started(AskError::Cleanup)),
        // A turn without an answer reports why whether or not its agent cleaned up: an
        // admission still being prepared, or a run not seen to end, is exactly what leaves the
        // agent undrained, and its reason must reach the caller.
        result => result,
    }
}

/// Wait for a run and read its answer. A turn given up first interrupts its run and reports why
/// at once, without waiting to see how the run ends. That suits a turn nobody can ask about
/// again; a keyed turn stands by its run instead, so that what it reports is what crabber holds.
async fn answer(
    store: &dyn Store,
    run: crabber::RunHandle,
    cancel: &mut Signal,
    deadline: tokio::time::Instant,
) -> Result<String, AskError> {
    let control = run.control();
    let mut done = std::pin::pin!(run.done());
    let result = match wait_for_completion(done.as_mut(), cancel, deadline).await {
        Ok(result) => result.map_err(|_| AskError::Run)?,
        Err(error) => {
            control.interrupt();
            return Err(error);
        }
    };
    if result.status != RunStatus::Completed {
        return Err(AskError::NotCompleted);
    }
    let messages = tokio::time::timeout_at(deadline, store.list_messages(&result.session_id, None))
        .await
        .map_err(|_| AskError::Timeout)?
        .map_err(|_| AskError::Run)?;
    committed_text(messages, &result.run_id)
}

async fn cleanup(agent: &crabber::Agent) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(INTERRUPT_SETTLE_SECONDS);
    let closed = matches!(
        tokio::time::timeout_at(deadline, agent.close_extensions()).await,
        Ok(Ok(()))
    );
    let report = agent
        .drain_execution_tasks(deadline.saturating_duration_since(tokio::time::Instant::now()))
        .await;
    closed && drained(&report)
}

fn drained(report: &crabber::ExecutionTaskDrain) -> bool {
    report.pending.is_empty()
        && report.pending_preparations.is_empty()
        && report
            .joined
            .iter()
            .all(|task| task.termination == crabber::TaskTermination::Returned)
        && report
            .joined_preparations
            .iter()
            .all(|task| task.termination == crabber::TaskTermination::Returned)
        && report
            .joined_unscoped_preparations
            .iter()
            .all(|task| task.termination == crabber::TaskTermination::Returned)
        && report
            .resources
            .iter()
            .map(|resource| &resource.resources)
            .chain(
                report
                    .background_tasks
                    .iter()
                    .map(|resource| &resource.resources),
            )
            .all(|resource| {
                resource.sealed && resource.pending == 0 && resource.failures.is_empty()
            })
}

async fn wait_for_completion<F: Future>(
    done: Pin<&mut F>,
    cancel: &mut Signal,
    deadline: tokio::time::Instant,
) -> Result<F::Output, AskError> {
    tokio::select! {
        biased;
        result = done => Ok(result),
        () = cancel.fired() => Err(AskError::Interrupted),
        () = tokio::time::sleep_until(deadline) => Err(AskError::Timeout),
    }
}

async fn ensure_session(
    agent: &crabber::Agent,
    store: &dyn Store,
    def: &CoworkerDefinition,
    workspace: &Workspace,
    id: &crabber::SessionId,
) -> Result<(), AskError> {
    if let Some(session) = store
        .get_session(id)
        .await
        .map_err(|_| AskError::Conversation)?
    {
        return session
            .ensure_identity(&def.id, workspace.directory())
            .map_err(|_| AskError::Workspace);
    }
    let _ = agent
        .manage_session(crabber::ManageSession {
            id: crabber::core::ControlId::new(),
            session_id: id.clone(),
            expected_revision: crabber::core::SessionRevision::INITIAL,
            change: crabber::SessionChange::Create {
                workspace_id: def.id.clone(),
                directory: workspace.directory().into(),
                title: id.to_string(),
                defaults: crabber::SessionDefaults::default(),
            },
        })
        .await;
    store
        .get_session(id)
        .await
        .map_err(|_| AskError::Conversation)?
        .ok_or(AskError::Conversation)?
        .ensure_identity(&def.id, workspace.directory())
        .map_err(|_| AskError::Workspace)
}

fn committed_text(messages: Vec<Message>, run_id: &RunId) -> Result<String, AskError> {
    let message = messages
        .into_iter()
        .rev()
        .find(|m| m.role == Role::Assistant && m.run_id.as_ref() == Some(run_id))
        .ok_or(AskError::NoAnswer)?;
    let mut parts = message.parts;
    parts.sort_by_key(|p| p.ordinal);
    let answer: String = parts
        .into_iter()
        .filter_map(|p| match (p.kind, p.content) {
            (PartKind::AssistantText, ContentBlock::Text { text }) => Some(text),
            _ => None,
        })
        .collect();
    if answer.is_empty() {
        Err(AskError::NoAnswer)
    } else {
        Ok(answer)
    }
}

#[cfg(test)]
mod tests;
