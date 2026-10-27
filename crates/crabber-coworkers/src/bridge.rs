pub const BEHAVIOR_VERSION: &str = "crabber-coworkers.v2";

pub const PROVIDER_ID: &str = "opencode-go";
pub const MODEL_ID: &str = "deepseek-v4-flash";
pub const API_KEY_ENV: &str = "OPENCODE_GO_API_KEY";

use crate::{
    ask::AskError,
    config::{CoworkerDefinition, WorkspaceAccess},
    workspace::Workspace,
};
use crabber::{
    Agent, AgentConfig, PermissionDecision, Scope, Selection, StaticPolicy,
    providers::{HttpAdapter, HttpResolver, Protocol, Resolver},
    session::Store,
};
use std::sync::Arc;

#[must_use]
pub fn system_prompt(def: &CoworkerDefinition) -> String {
    let mut prompt = format!(
        "You are {}, a coworker whose role is: {}.\n\n{}\n",
        def.name, def.role, def.instructions
    );
    if def.workspace != WorkspaceAccess::Disabled {
        prompt.push_str("\nWorkspace files use relative paths. Report the relative saved path. Read before editing. Only claim a save after a successful tool result. Failed or truncated reads are not complete evidence. Never claim file changes rolled back on cancellation.\n");
    }
    prompt
}

/// Build an agent with exactly the admitted workspace tools.
///
/// # Errors
/// Returns fixed errors for invalid authority, catalog, or runtime components.
pub fn build_agent(
    def: &CoworkerDefinition,
    workspace: &Workspace,
    store: Arc<dyn Store>,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
) -> Result<Agent, AskError> {
    build_agent_with_behavior(def, workspace, store, resolver, selection, BEHAVIOR_VERSION)
}

/// Build an agent with admitted tools and an explicit host behavior version.
///
/// # Errors
/// Returns fixed errors for invalid authority, catalog, or runtime components.
pub fn build_agent_with_behavior(
    def: &CoworkerDefinition,
    workspace: &Workspace,
    store: Arc<dyn Store>,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
    version: &str,
) -> Result<Agent, AskError> {
    workspace.validate(def)?;
    let policy = workspace.tools().iter().fold(
        StaticPolicy::new(PermissionDecision::Deny),
        |policy, tool| policy.with_rule(tool.name(), PermissionDecision::Allow),
    );
    let mut builder = Agent::builder()
        .behavior_version(version)
        .store(store)
        .provider(resolver)
        .config(AgentConfig {
            selection,
            workspace_id: def.id.clone(),
            directory: workspace.directory().into(),
            title: def.name.clone(),
            system_prompt: Some(system_prompt(def)),
            max_output_tokens: None,
            model_controls: crabber::ModelControls::default(),
        })
        .policy(Arc::new(policy));
    if let Some(extension) = workspace.extension()? {
        builder = builder.shared_extension(Arc::new(extension), Scope::Global);
    }
    builder.build().map_err(|_| AskError::Build)
}

#[must_use]
pub fn opencode_go_resolver(api_key: &str, base_url: Option<&str>) -> Arc<dyn Resolver> {
    let mut adapter = HttpAdapter::opencode_go(Protocol::ChatCompletions).with_api_key(api_key);
    if let Some(url) = base_url {
        adapter = adapter.with_base_url(url);
    }
    Arc::new(HttpResolver::new().with_adapter(adapter))
}

#[must_use]
pub fn opencode_go_resolver_from_env() -> Option<Arc<dyn Resolver>> {
    let key = std::env::var(API_KEY_ENV)
        .ok()
        .filter(|key| !key.trim().is_empty())?;
    Some(opencode_go_resolver(&key, None))
}

#[must_use]
pub fn production_selection() -> Selection {
    Selection {
        provider_id: PROVIDER_ID.into(),
        model_id: MODEL_ID.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_system_prompt() {
        let def = CoworkerDefinition {
            workspace: crate::config::WorkspaceAccess::Disabled,
            slack: None,
            id: "scout".into(),
            name: "Scout".into(),
            role: "Researcher".into(),
            instructions: "Find evidence.".into(),
        };
        assert_eq!(
            system_prompt(&def),
            "You are Scout, a coworker whose role is: Researcher.\n\nFind evidence.\n"
        );
    }
}
