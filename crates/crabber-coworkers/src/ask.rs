use crate::{
    bridge,
    config::{Config, CoworkerDefinition},
};
use crabber::{
    Selection,
    core::{ContentBlock, Message, PartKind, Role, RunId, RunStatus},
    providers::Resolver,
    session::{MemoryStore, Store},
};
use std::{sync::Arc, time::Duration};

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AskError {
    #[error("unknown coworker")]
    UnknownCoworker,
    #[error("prompt is empty")]
    EmptyPrompt,
    #[error("prompt exceeds max_prompt_bytes")]
    PromptTooLarge,
    #[error("agent construction failed")]
    Build,
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
            Self::UnknownCoworker | Self::EmptyPrompt | Self::PromptTooLarge => 2,
            Self::Build | Self::Run | Self::NotCompleted | Self::NoAnswer | Self::Timeout => 1,
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
    Ok(def)
}

/// Run one prompt in a fresh in-memory session and return only committed assistant text.
///
/// # Errors
/// Returns fixed, redacted errors for build, run, timeout, and missing-answer failures.
pub async fn ask(
    def: &CoworkerDefinition,
    prompt: &str,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
    timeout: Duration,
) -> Result<String, AskError> {
    let store = Arc::new(MemoryStore::new());
    let agent = bridge::build_agent(def, store.clone(), resolver, selection)
        .map_err(|_| AskError::Build)?;
    tokio::time::timeout(timeout, async {
        let run = agent
            .prompt(None, prompt)
            .await
            .map_err(|_| AskError::Run)?;
        let result = run.done().await.map_err(|_| AskError::Run)?;
        if result.status != RunStatus::Completed {
            return Err(AskError::NotCompleted);
        }
        let messages = store
            .list_messages(&result.session_id, None)
            .await
            .map_err(|_| AskError::Run)?;
        committed_text(messages, &result.run_id)
    })
    .await
    .map_err(|_| AskError::Timeout)?
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
