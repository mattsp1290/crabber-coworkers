use crate::config::{
    CoworkerDefinition,
    constants::{API_KEY_ENV, MODEL_ID, PROVIDER_ID},
};
use crabber::{
    Agent, AgentConfig, BuildError, PermissionDecision, Selection, StaticPolicy,
    providers::{HttpAdapter, HttpResolver, Protocol, Resolver},
    session::Store,
};
use std::sync::Arc;

#[must_use]
pub fn system_prompt(def: &CoworkerDefinition) -> String {
    format!(
        "You are {}, a coworker whose role is: {}.\n\n{}\n",
        def.name, def.role, def.instructions
    )
}

/// Build a tool-free agent with an explicit deny policy.
///
/// # Errors
/// Returns crabber's build error if the supplied runtime components are invalid.
pub fn build_agent(
    def: &CoworkerDefinition,
    store: Arc<dyn Store>,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
) -> Result<Agent, BuildError> {
    Agent::builder()
        .store(store)
        .provider(resolver)
        .config(AgentConfig {
            selection,
            workspace_id: def.id.clone(),
            // No filesystem tools exist; revisit this root before adding them.
            directory: ".".into(),
            title: def.name.clone(),
            system_prompt: Some(system_prompt(def)),
        })
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Deny)))
        .build()
}

#[must_use]
pub fn opencode_go_resolver(base_url: Option<&str>, api_key: Option<&str>) -> Arc<dyn Resolver> {
    let mut adapter = HttpAdapter::opencode_go(Protocol::ChatCompletions);
    if let Some(url) = base_url {
        adapter = adapter.with_base_url(url);
    }
    if let Some(key) = api_key {
        adapter = adapter.with_api_key(key);
    }
    Arc::new(HttpResolver::new().with_adapter(adapter))
}

#[must_use]
pub fn opencode_go_resolver_from_env() -> Option<Arc<dyn Resolver>> {
    let key = std::env::var(API_KEY_ENV)
        .ok()
        .filter(|key| !key.trim().is_empty())?;
    Some(opencode_go_resolver(None, Some(&key)))
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
