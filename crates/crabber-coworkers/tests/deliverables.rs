mod common;
#[path = "common/example.rs"]
mod example;

use crabber::{FakeProvider, providers::Resolver, session::Store};
use crabber_coworkers::{
    ask::{self, AskError, Session},
    config::{CoworkerDefinition, WorkspaceAccess},
    workspace,
};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};

fn definition(access: WorkspaceAccess) -> CoworkerDefinition {
    let mut def = example::config().coworker("scout").unwrap().clone();
    def.workspace = access;
    def
}

async fn turn(
    def: &CoworkerDefinition,
    state: &Path,
    provider: Arc<dyn Resolver>,
    conversation: Option<&str>,
) -> Result<String, AskError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut cancel = common::never();
    let workspace = workspace::admit(def, state, deadline, &mut cancel).await?;
    let session = match conversation {
        Some(name) => Session::Durable {
            store: crabber_coworkers::store::open(state, &def.id)
                .await
                .unwrap(),
            name: name.into(),
        },
        None => Session::Ephemeral,
    };
    ask::ask(
        def,
        &workspace,
        "synthetic request",
        provider,
        common::selection(),
        deadline,
        session,
        cancel,
    )
    .await
}

fn tool_provider(name: &str, arguments: &Value) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(vec![
        common::tool_call(name, arguments),
        common::text_answer("done"),
    ]))
}

#[tokio::test]
async fn catalog_creates_lists_reads_and_edits_across_conversations() {
    let disk = tempfile::tempdir().unwrap();
    let def = definition(WorkspaceAccess::ReadWrite);
    let provider = Arc::new(FakeProvider::scripted(vec![
        common::tool_call(
            "file_write",
            &json!({"path":"draft.txt","content":"first draft"}),
        ),
        common::tool_call("file_list", &json!({})),
        common::tool_call("file_read", &json!({"path":"draft.txt"})),
        common::text_answer("saved draft.txt"),
    ]));
    assert_eq!(
        turn(&def, disk.path(), provider.clone(), Some("create"))
            .await
            .unwrap(),
        "saved draft.txt"
    );
    let file = disk.path().join("workspaces/scout/draft.txt");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "first draft");
    let requests = provider.requests();
    let mut tools: Vec<_> = requests[0]
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    tools.sort_unstable();
    assert_eq!(tools, ["file_edit", "file_list", "file_read", "file_write"]);
    let results = common::tool_results(requests.last().unwrap());
    assert_eq!(results.len(), 3);
    assert!(
        results
            .iter()
            .all(|result| result["outcome"] == "succeeded")
    );
    assert!(
        results
            .iter()
            .any(|result| result.to_string().contains("first draft"))
    );
    let revise = tool_provider(
        "file_edit",
        &json!({"path":"draft.txt","anchor":"first","replacement":"revised"}),
    );
    turn(&def, disk.path(), revise.clone(), Some("revise"))
        .await
        .unwrap();
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "revised draft");
    assert_eq!(revise.requests()[0].messages.len(), 1);
    let store = crabber_coworkers::store::open(disk.path(), "scout")
        .await
        .unwrap();
    for name in ["create", "revise"] {
        let session = store
            .get_session(&crabber::SessionId::from(name))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            session.directory,
            file.parent()
                .unwrap()
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        );
    }
}

#[tokio::test]
async fn current_grants_advertise_exact_tools_and_refuse_unsolicited_calls() {
    let disk = tempfile::tempdir().unwrap();
    let def = definition(WorkspaceAccess::ReadWrite);
    turn(
        &def,
        disk.path(),
        tool_provider(
            "file_write",
            &json!({"path":"draft.txt","content":"retained"}),
        ),
        Some("grant"),
    )
    .await
    .unwrap();
    for access in [WorkspaceAccess::ReadOnly, WorkspaceAccess::Disabled] {
        let def = definition(access);
        for name in [
            "file_write",
            "file_edit",
            "shell",
            "url_fetch",
            "file_delete",
        ] {
            let arguments = match name {
                "file_write" => &json!({"path":"draft.txt","content":"forbidden"}),
                "file_edit" => {
                    &json!({"path":"draft.txt","anchor":"retained","replacement":"forbidden"})
                }
                "shell" => &json!({"command":"touch forbidden"}),
                "url_fetch" => &json!({"url":"http://127.0.0.1:9"}),
                _ => &json!({"path":"draft.txt"}),
            };
            let provider = tool_provider(name, arguments);
            let _ = turn(
                &def,
                disk.path(),
                provider.clone(),
                if access == WorkspaceAccess::ReadOnly {
                    Some("grant")
                } else {
                    None
                },
            )
            .await;
            let requests = provider.requests();
            let mut names: Vec<_> = requests[0]
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect();
            names.sort_unstable();
            if access == WorkspaceAccess::ReadOnly {
                assert_eq!(names, ["file_list", "file_read"]);
            } else {
                assert_eq!(names, Vec::<&str>::new());
            }
            assert_eq!(
                std::fs::read_to_string(disk.path().join("workspaces/scout/draft.txt")).unwrap(),
                "retained"
            );
        }
    }
}

#[tokio::test]
async fn session_directory_and_owner_mismatches_fail_before_provider_access() {
    let disk = tempfile::tempdir().unwrap();
    let disabled = definition(WorkspaceAccess::Disabled);
    turn(
        &disabled,
        disk.path(),
        Arc::new(FakeProvider::scripted(vec![common::text_answer("old")])),
        Some("old"),
    )
    .await
    .unwrap();
    let provider = Arc::new(FakeProvider::scripted(vec![]));
    let enabled = definition(WorkspaceAccess::ReadWrite);
    assert_eq!(
        turn(&enabled, disk.path(), provider.clone(), Some("old")).await,
        Err(AskError::Workspace)
    );
    assert_eq!(provider.requests().len(), 0);
    let store = crabber_coworkers::store::open(disk.path(), "scout")
        .await
        .unwrap();
    assert_eq!(
        store
            .get_session(&crabber::SessionId::from("old"))
            .await
            .unwrap()
            .unwrap()
            .directory,
        "."
    );
    let mut other = disabled.clone();
    other.id = "quill".into();
    let workspace = workspace::Workspace::disabled(&other).unwrap();
    assert_eq!(
        ask::ask(
            &other,
            &workspace,
            "synthetic",
            provider.clone(),
            common::selection(),
            tokio::time::Instant::now() + Duration::from_secs(5),
            Session::Durable {
                store,
                name: "old".into()
            },
            common::never()
        )
        .await,
        Err(AskError::Workspace)
    );
    assert_eq!(provider.requests().len(), 0);
}
