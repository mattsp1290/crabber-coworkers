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
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
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
    mut cancel: Cancel,
) -> Result<String, AskError> {
    let (store, session_id): (Arc<dyn Store>, _) = match session {
        Session::Ephemeral => (Arc::new(MemoryStore::new()), None),
        Session::Durable { store, name } => (store, Some(crabber::SessionId::from(name.as_str()))),
    };
    let agent = bridge::build_agent(def, workspace, store.clone(), resolver, selection)?;
    let result = async {
        if let Some(id) = &session_id {
            tokio::select! {
                biased;
                () = &mut cancel => return Err(AskError::Interrupted),
                () = tokio::time::sleep_until(deadline) => return Err(AskError::Timeout),
                result = ensure_session(&agent, store.as_ref(), def, workspace, id) => result?,
            }
        }
        tokio::select! {
            biased;
            () = &mut cancel => return Err(AskError::Interrupted),
            () = tokio::time::sleep_until(deadline) => return Err(AskError::Timeout),
            () = std::future::ready(()) => {},
        }
        // Admission must finish: dropping it can strand a preparation reservation.
        let run = agent
            .prompt(session_id, prompt)
            .await
            .map_err(|error| match error {
                crabber::RuntimeError::SessionBusy
                | crabber::RuntimeError::Store(crabber::session::StoreError::Busy) => {
                    AskError::Busy
                }
                _ => AskError::Run,
            })?;
        let control = run.control();
        let mut done = std::pin::pin!(run.done());
        let result = match wait_for_completion(done.as_mut(), &mut cancel, deadline).await {
            Ok(result) => result.map_err(|_| AskError::Run)?,
            Err(error) => {
                control.interrupt();
                return Err(error);
            }
        };
        if result.status != RunStatus::Completed {
            return Err(AskError::NotCompleted);
        }
        let messages =
            tokio::time::timeout_at(deadline, store.list_messages(&result.session_id, None))
                .await
                .map_err(|_| AskError::Timeout)?
                .map_err(|_| AskError::Run)?;
        committed_text(messages, &result.run_id)
    }
    .await;
    let cleaned = cleanup(&agent).await;
    match result {
        Ok(answer) if cleaned => Ok(answer),
        Ok(_) => Err(AskError::Cleanup),
        Err(error) => Err(error),
    }
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
    cancel: &mut Cancel,
    deadline: tokio::time::Instant,
) -> Result<F::Output, AskError> {
    tokio::select! {
        biased;
        result = done => Ok(result),
        () = cancel => Err(AskError::Interrupted),
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
