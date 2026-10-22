use crabber::{
    FakeProvider, Selection, StreamDelta,
    core::{ContentBlock, Role},
};
use crabber_coworkers::{ask::ask, config};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn happy_path() {
    let config = config::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.json"),
    )
    .unwrap();
    let provider = Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("Hello ".into()),
        StreamDelta::TextDelta("there".into()),
        StreamDelta::Completed,
    ]]));
    let selection = Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    };
    let answer = ask(
        config.coworker("scout").unwrap(),
        "Find something",
        provider.clone(),
        selection,
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(answer, "Hello there");
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .system
            .as_ref()
            .unwrap()
            .contains("You are Scout, a coworker whose role is: Researcher.")
    );
    assert!(requests[0].messages.iter().any(|m| m.role == Role::User && m.parts.iter().any(|p| matches!(&p.content, ContentBlock::Text { text } if text.contains("Find something")))));
    assert_eq!(requests[0].tools.len(), 0);
}

mod common;
use crabber_coworkers::ask::{AskError, validate_request};

#[tokio::test]
async fn retry_returns_only_committed_text() {
    let config = example_config();
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
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(result, "full answer");
    assert_eq!(provider.requests().len(), 2);
}
#[tokio::test]
async fn coworker_isolation() {
    let config = example_config();
    let provider = Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("ok".into()),
        StreamDelta::Completed,
    ]]));
    ask(
        config.coworker("quill").unwrap(),
        "hi",
        provider.clone(),
        common::selection(),
        Duration::from_secs(5),
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
        validate_request(&example_config(), "nobody", "hi").unwrap_err(),
        AskError::UnknownCoworker
    );
}
#[test]
fn empty_prompt() {
    assert_eq!(
        validate_request(&example_config(), "scout", "   ").unwrap_err(),
        AskError::EmptyPrompt
    );
}
#[test]
fn oversized_prompt() {
    let config = example_config();
    assert_eq!(
        validate_request(
            &config,
            "scout",
            &"x".repeat(config.limits.max_prompt_bytes + 1)
        )
        .unwrap_err(),
        AskError::PromptTooLarge
    );
}
#[tokio::test]
async fn provider_failure_is_redacted() {
    let config = example_config();
    let error = ask(
        config.coworker("scout").unwrap(),
        "hi",
        Arc::new(FakeProvider::scripted(vec![vec![common::failure(false)]])),
        common::selection(),
        Duration::from_secs(5),
    )
    .await
    .unwrap_err();
    assert_eq!(error, AskError::Run);
    assert!(!error.to_string().contains("secret-marker"));
}
#[tokio::test]
async fn timeout_is_bounded() {
    let config = example_config();
    let start = std::time::Instant::now();
    let error = ask(
        config.coworker("scout").unwrap(),
        "hi",
        Arc::new(common::Pending),
        common::selection(),
        Duration::from_millis(100),
    )
    .await
    .unwrap_err();
    assert_eq!(error, AskError::Timeout);
    assert!(start.elapsed() < Duration::from_secs(2));
}
#[tokio::test]
async fn no_answer() {
    let config = example_config();
    let error = ask(
        config.coworker("scout").unwrap(),
        "hi",
        Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::ReasoningDelta("internal".into()),
            StreamDelta::Completed,
        ]])),
        common::selection(),
        Duration::from_secs(5),
    )
    .await
    .unwrap_err();
    assert_eq!(error, AskError::NoAnswer);
}

fn example_config() -> config::Config {
    config::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.json"),
    )
    .unwrap()
}

#[tokio::test]
async fn empty_response_is_a_redacted_runtime_failure() {
    let config = example_config();
    let error = ask(
        config.coworker("scout").unwrap(),
        "hi",
        Arc::new(FakeProvider::scripted(vec![vec![StreamDelta::Completed]])),
        common::selection(),
        Duration::from_secs(5),
    )
    .await
    .unwrap_err();
    assert_eq!(error, AskError::Run);
    assert_eq!(error.to_string(), "model run failed");
}
