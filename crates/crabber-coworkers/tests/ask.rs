use crabber_coworkers::ask::Session;
mod common;
#[path = "common/example.rs"]
mod example;
use crabber::{
    FakeProvider, StreamDelta,
    core::{ContentBlock, Role},
};
use crabber_coworkers::ask::ask;
use crabber_coworkers::ask::{AskError, validate_request};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn happy_path() {
    let config = example::config();
    let provider = Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("Hello ".into()),
        StreamDelta::TextDelta("there".into()),
        StreamDelta::Completed,
    ]]));
    let selection = common::selection();
    let answer = ask(
        config.coworker("scout").unwrap(),
        "Find something",
        provider.clone(),
        selection,
        tokio::time::Instant::now() + Duration::from_secs(5),
        Session::Ephemeral,
        common::never(),
    )
    .await
    .unwrap();
    assert_eq!(answer, "Hello there");
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].system.as_deref(),
        Some(crabber_coworkers::bridge::system_prompt(config.coworker("scout").unwrap()).as_str())
    );
    assert!(requests[0].messages.iter().any(|m| m.role == Role::User && m.parts.iter().any(|p| matches!(&p.content, ContentBlock::Text { text } if text.contains("Find something")))));
    assert_eq!(requests[0].tools.len(), 0);
}

#[tokio::test]
async fn retry_returns_only_committed_text() {
    let config = example::config();
    let provider = Arc::new(FakeProvider::scripted(vec![
        vec![StreamDelta::TextDelta("par".into()), common::failure(true)],
        vec![
            StreamDelta::TextDelta("full answer".into()),
            StreamDelta::Completed,
        ],
    ]));
    let result = ask(
        config.coworker("scout").unwrap(),
        "hi",
        provider.clone(),
        common::selection(),
        tokio::time::Instant::now() + Duration::from_secs(5),
        Session::Ephemeral,
        common::never(),
    )
    .await
    .unwrap();
    assert_eq!(result, "full answer");
    assert_eq!(provider.requests().len(), 2);
}
#[tokio::test]
async fn coworker_isolation() {
    let config = example::config();
    let provider = Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("ok".into()),
        StreamDelta::Completed,
    ]]));
    ask(
        config.coworker("quill").unwrap(),
        "hi",
        provider.clone(),
        common::selection(),
        tokio::time::Instant::now() + Duration::from_secs(5),
        Session::Ephemeral,
        common::never(),
    )
    .await
    .unwrap();
    let system = provider.requests()[0].system.clone().unwrap();
    assert!(system.contains("Writer"));
    assert!(!system.contains("Scout"));
    assert!(!system.contains("Researcher"));
}
#[test]
fn unknown_coworker() {
    assert_eq!(
        validate_request(&example::config(), "nobody", "hi", None).unwrap_err(),
        AskError::UnknownCoworker
    );
}
#[test]
fn empty_prompt() {
    assert_eq!(
        validate_request(&example::config(), "scout", "   ", None).unwrap_err(),
        AskError::EmptyPrompt
    );
}
#[test]
fn oversized_prompt() {
    let config = example::config();
    assert_eq!(
        validate_request(
            &config,
            "scout",
            &"x".repeat(config.limits.max_prompt_bytes + 1),
            None
        )
        .unwrap_err(),
        AskError::PromptTooLarge
    );
}
#[tokio::test]
async fn provider_failure_is_redacted() {
    let config = example::config();
    let error = ask(
        config.coworker("scout").unwrap(),
        "hi",
        Arc::new(FakeProvider::scripted(vec![vec![common::failure(false)]])),
        common::selection(),
        tokio::time::Instant::now() + Duration::from_secs(5),
        Session::Ephemeral,
        common::never(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, AskError::Run);
    assert!(!error.to_string().contains("secret-marker"));
}
#[tokio::test]
async fn timeout_is_bounded() {
    let config = example::config();
    let start = std::time::Instant::now();
    let error = ask(
        config.coworker("scout").unwrap(),
        "hi",
        Arc::new(common::Pending),
        common::selection(),
        tokio::time::Instant::now() + Duration::from_millis(100),
        Session::Ephemeral,
        common::never(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, AskError::Timeout);
    assert!(start.elapsed() < Duration::from_secs(2));
}
#[tokio::test]
async fn no_answer() {
    let config = example::config();
    let error = ask(
        config.coworker("scout").unwrap(),
        "hi",
        Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::ReasoningDelta("internal".into()),
            StreamDelta::Completed,
        ]])),
        common::selection(),
        tokio::time::Instant::now() + Duration::from_secs(5),
        Session::Ephemeral,
        common::never(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, AskError::NoAnswer);
}

#[tokio::test]
async fn empty_response_is_a_redacted_runtime_failure() {
    let config = example::config();
    let error = ask(
        config.coworker("scout").unwrap(),
        "hi",
        Arc::new(FakeProvider::scripted(vec![vec![StreamDelta::Completed]])),
        common::selection(),
        tokio::time::Instant::now() + Duration::from_secs(5),
        Session::Ephemeral,
        common::never(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, AskError::Run);
    assert_eq!(error.to_string(), "model run failed");
}

async fn durable(dir: &std::path::Path, id: &str) -> Session {
    Session::Durable {
        store: crabber_coworkers::store::open(dir, id).await.unwrap(),
        name: "weekly".into(),
    }
}
fn scripted(answer: &str) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta(answer.into()),
        StreamDelta::Completed,
    ]]))
}
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}

#[tokio::test]
async fn conversation_continues_across_store_instances() {
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    let def = config.coworker("scout").unwrap();
    assert_eq!(
        ask(
            def,
            "first question",
            scripted("first answer"),
            common::selection(),
            deadline(),
            durable(dir.path(), "scout").await,
            common::never()
        )
        .await
        .unwrap(),
        "first answer"
    );
    let provider = scripted("second answer");
    assert_eq!(
        ask(
            def,
            "second question",
            provider.clone(),
            common::selection(),
            deadline(),
            durable(dir.path(), "scout").await,
            common::never()
        )
        .await
        .unwrap(),
        "second answer"
    );
    let requests = provider.requests();
    assert!(requests[0].system.as_ref().unwrap().contains("Scout"));
    let messages: Vec<_> = requests[0]
        .messages
        .iter()
        .map(|m| {
            (
                m.role,
                m.parts
                    .iter()
                    .filter_map(|p| match &p.content {
                        ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<String>(),
            )
        })
        .collect();
    assert_eq!(
        messages,
        vec![
            (Role::User, "first question".into()),
            (Role::Assistant, "first answer".into()),
            (Role::User, "second question".into())
        ]
    );
}

#[test]
fn invalid_conversation_names() {
    let config = example::config();
    for name in ["Weekly", "a--b", "", "a_b", &"x".repeat(65)] {
        assert_eq!(
            validate_request(&config, "scout", "hi", Some(name)).unwrap_err(),
            AskError::InvalidConversation
        );
    }
    assert!(validate_request(&config, "scout", "hi", Some("weekly-1")).is_ok());
}

#[tokio::test]
async fn timeout_interrupts_run_so_next_ask_continues() {
    use crabber::session::Store;
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    let def = config.coworker("scout").unwrap();
    let store = crabber_coworkers::store::open(dir.path(), "scout")
        .await
        .unwrap();
    let start = std::time::Instant::now();
    assert_eq!(
        ask(
            def,
            "hi",
            Arc::new(common::Pending),
            common::selection(),
            tokio::time::Instant::now() + Duration::from_millis(100),
            Session::Durable {
                store: store.clone(),
                name: "weekly".into()
            },
            common::never()
        )
        .await
        .unwrap_err(),
        AskError::Timeout
    );
    assert!(start.elapsed() < Duration::from_secs(2));
    assert_eq!(store.list_unfinished_runs().await.unwrap().len(), 0);
    let messages = store
        .list_messages(&crabber::SessionId::from("weekly"), None)
        .await
        .unwrap();
    let id = messages.iter().find_map(|m| m.run_id.as_ref()).unwrap();
    assert_eq!(
        store.get_run(id).await.unwrap().unwrap().status,
        crabber::core::RunStatus::Interrupted
    );
    assert_eq!(
        ask(
            def,
            "again",
            scripted("ok"),
            common::selection(),
            deadline(),
            durable(dir.path(), "scout").await,
            common::never()
        )
        .await
        .unwrap(),
        "ok"
    );
}

#[tokio::test]
async fn cancel_interrupts_run() {
    use crabber::session::Store;
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    let store = crabber_coworkers::store::open(dir.path(), "scout")
        .await
        .unwrap();
    let cancel = Box::pin(async {
        tokio::time::sleep(Duration::from_millis(50)).await;
    });
    assert_eq!(
        ask(
            config.coworker("scout").unwrap(),
            "hi",
            Arc::new(common::Pending),
            common::selection(),
            deadline(),
            Session::Durable {
                store: store.clone(),
                name: "weekly".into()
            },
            cancel
        )
        .await
        .unwrap_err(),
        AskError::Interrupted
    );
    assert_eq!(store.list_unfinished_runs().await.unwrap().len(), 0);
    assert_eq!(
        ask(
            config.coworker("scout").unwrap(),
            "again",
            scripted("ok"),
            common::selection(),
            deadline(),
            durable(dir.path(), "scout").await,
            common::never()
        )
        .await
        .unwrap(),
        "ok"
    );
}

#[tokio::test]
async fn cancel_already_fired_interrupts_immediately() {
    use crabber::session::Store;
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    let store = crabber_coworkers::store::open(dir.path(), "scout")
        .await
        .unwrap();
    assert_eq!(
        ask(
            config.coworker("scout").unwrap(),
            "hi",
            Arc::new(common::Pending),
            common::selection(),
            deadline(),
            Session::Durable {
                store: store.clone(),
                name: "weekly".into()
            },
            Box::pin(std::future::ready(()))
        )
        .await
        .unwrap_err(),
        AskError::Interrupted
    );
    assert_eq!(store.list_unfinished_runs().await.unwrap().len(), 0);
    // Ephemeral admission has no session-creation cancel check.
    assert_eq!(
        ask(
            config.coworker("scout").unwrap(),
            "hi",
            Arc::new(common::Pending),
            common::selection(),
            deadline(),
            Session::Ephemeral,
            Box::pin(std::future::ready(()))
        )
        .await
        .unwrap_err(),
        AskError::Interrupted
    );
}

#[tokio::test]
async fn completed_answer_wins_over_pending_signal() {
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    let store = crabber_coworkers::store::open(dir.path(), "scout")
        .await
        .unwrap();
    // The exact simultaneous-ready ordering is covered by the library unit test.
    let cancel = Box::pin(async {
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    assert_eq!(
        ask(
            config.coworker("scout").unwrap(),
            "hi",
            scripted("ok"),
            common::selection(),
            deadline(),
            Session::Durable {
                store,
                name: "weekly".into()
            },
            cancel
        )
        .await
        .unwrap(),
        "ok"
    );
}

#[tokio::test]
async fn conversations_are_isolated_per_coworker() {
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    for id in ["scout", "quill"] {
        let provider = scripted(id);
        ask(
            config.coworker(id).unwrap(),
            "hi",
            provider.clone(),
            common::selection(),
            deadline(),
            durable(dir.path(), id).await,
            common::never(),
        )
        .await
        .unwrap();
        assert_eq!(provider.requests()[0].messages.len(), 1);
    }
    assert!(dir.path().join("scout.sqlite").is_file());
    assert!(dir.path().join("quill.sqlite").is_file());
}

#[tokio::test]
async fn busy_conversation_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    let def = config.coworker("scout").unwrap();
    // Initialize the named session, then hold a second turn open.
    ask(
        def,
        "setup",
        scripted("ok"),
        common::selection(),
        deadline(),
        durable(dir.path(), "scout").await,
        common::never(),
    )
    .await
    .unwrap();
    let store = crabber_coworkers::store::open(dir.path(), "scout")
        .await
        .unwrap();
    let agent = crabber_coworkers::bridge::build_agent(
        def,
        store,
        Arc::new(common::Pending),
        common::selection(),
    )
    .unwrap();
    let run = agent
        .prompt(Some(crabber::SessionId::from("weekly")), "pending")
        .await
        .unwrap();
    assert_eq!(
        ask(
            def,
            "busy",
            scripted("bad"),
            common::selection(),
            deadline(),
            durable(dir.path(), "scout").await,
            common::never()
        )
        .await
        .unwrap_err(),
        AskError::Busy
    );
    run.control().interrupt();
    run.done().await.unwrap();
}

#[tokio::test]
async fn behavior_version_change_keeps_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    let def = config.coworker("scout").unwrap();
    let store = crabber_coworkers::store::open(dir.path(), "scout")
        .await
        .unwrap();
    let agent = crabber_coworkers::bridge::build_agent_with_behavior(
        def,
        store,
        scripted("old answer"),
        common::selection(),
        "crabber-coworkers.test-v0",
    )
    .unwrap();
    // Auto-created session ID becomes the conversation name on continuation.
    let result = agent
        .prompt(None, "old question")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let provider = scripted("new answer");
    let session = Session::Durable {
        store: crabber_coworkers::store::open(dir.path(), "scout")
            .await
            .unwrap(),
        name: result.session_id.to_string(),
    };
    ask(
        def,
        "new question",
        provider.clone(),
        common::selection(),
        deadline(),
        session,
        common::never(),
    )
    .await
    .unwrap();
    assert_eq!(provider.requests()[0].messages.len(), 3);
}

#[tokio::test]
async fn provider_failure_in_conversation_is_redacted() {
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    let def = config.coworker("scout").unwrap();
    let provider = Arc::new(FakeProvider::scripted(vec![vec![common::failure(false)]]));
    let error = ask(
        def,
        "hi",
        provider,
        common::selection(),
        deadline(),
        durable(dir.path(), "scout").await,
        common::never(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, AskError::Run);
    assert!(!error.to_string().contains("secret-marker"));
    assert_eq!(
        ask(
            def,
            "again",
            scripted("ok"),
            common::selection(),
            deadline(),
            durable(dir.path(), "scout").await,
            common::never()
        )
        .await
        .unwrap(),
        "ok"
    );
}

#[tokio::test]
async fn ephemeral_session_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let mut config = example::config();
    config.state_dir = state.clone();
    ask(
        config.coworker("scout").unwrap(),
        "hi",
        scripted("ok"),
        common::selection(),
        deadline(),
        Session::Ephemeral,
        common::never(),
    )
    .await
    .unwrap();
    assert!(!state.exists());
}

#[tokio::test]
async fn deadline_covers_admission() {
    use crabber::session::Store;
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    let store = crabber_coworkers::store::open(dir.path(), "scout")
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    assert_eq!(
        ask(
            config.coworker("scout").unwrap(),
            "hi",
            Arc::new(common::Pending),
            common::selection(),
            deadline,
            Session::Durable {
                store: store.clone(),
                name: "weekly".into()
            },
            common::never()
        )
        .await
        .unwrap_err(),
        AskError::Timeout
    );
    assert!(started.elapsed() < Duration::from_secs(7));
    assert_eq!(store.list_unfinished_runs().await.unwrap().len(), 0);
}
