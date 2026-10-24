mod common;
#[path = "common/example.rs"]
mod example;

use crabber::{FakeProvider, StreamDelta, providers::Resolver};
use crabber_coworkers::{
    ask::AskError,
    worker::{self, TurnRequest, TurnState},
};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

struct Server {
    base: String,
    client: Client,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn new(path: &std::path::Path, resolver: Arc<dyn Resolver>) -> Self {
        let app = worker::router_with("scout".into(), path.into(), resolver, common::selection());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            base,
            client: Client::new(),
            task,
        }
    }

    fn url(&self, id: Uuid) -> String {
        format!("{}/v1/turns/{id}", self.base)
    }

    async fn put(&self, id: Uuid, request: &TurnRequest) -> reqwest::Response {
        self.client
            .put(self.url(id))
            .json(request)
            .send()
            .await
            .unwrap()
    }

    async fn finished(&self, id: Uuid) -> Result<String, AskError> {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let response = self.client.get(self.url(id)).send().await.unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                match response.json::<TurnState>().await.unwrap() {
                    TurnState::Running => tokio::time::sleep(Duration::from_millis(10)).await,
                    TurnState::Finished { result } => return result,
                }
            }
        })
        .await
        .unwrap()
    }

    async fn cancel(&self, id: Uuid) -> reqwest::Response {
        self.client
            .post(format!("{}/cancel", self.url(id)))
            .send()
            .await
            .unwrap()
    }
}

fn request(conversation: &str) -> TurnRequest {
    let mut coworker = example::config().coworker("scout").unwrap().clone();
    coworker.slack = None;
    TurnRequest {
        version: worker::PROTOCOL_VERSION,
        coworker,
        conversation: conversation.into(),
        prompt: "private-question-marker".into(),
        timeout_ms: 60_000,
        expires_at_unix_ms: u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
            + 60_000,
    }
}

fn scripted(answer: &str) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta(answer.into()),
        StreamDelta::Completed,
    ]]))
}

#[tokio::test]
async fn durable_history_survives_worker_restart_and_list_is_remote() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let server = Server::new(&state, scripted("first-answer")).await;
    let health: Value = server
        .client
        .get(format!("{}/healthz", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        health,
        json!({"version":1,"coworker_id":"scout","pid":std::process::id()})
    );
    let list: String = server
        .client
        .get(format!("{}/v1/conversations", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list, "");
    assert!(
        !state.exists(),
        "health and empty list must leave offline import possible"
    );
    let id = Uuid::new_v4();
    assert_eq!(
        server.put(id, &request("weekly")).await.status(),
        StatusCode::OK
    );
    assert_eq!(server.finished(id).await.unwrap(), "first-answer");
    drop(server);
    let provider = scripted("second-answer");
    let server = Server::new(&state, provider.clone()).await;
    let id = Uuid::new_v4();
    assert_eq!(
        server.put(id, &request("weekly")).await.status(),
        StatusCode::OK
    );
    assert_eq!(server.finished(id).await.unwrap(), "second-answer");
    assert_eq!(provider.requests()[0].messages.len(), 3);
    let list: String = server
        .client
        .get(format!("{}/v1/conversations", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(list.starts_with("weekly\t"));
    assert_eq!(list.lines().count(), 1);
    assert!(!list.contains("private-question-marker"));
    assert!(!list.contains("first-answer"));
}

#[tokio::test]
async fn submit_is_owned_and_busy_and_duplicate_are_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let server = Server::new(dir.path(), provider.clone()).await;
    let id = Uuid::new_v4();
    let response = server.put(id, &request("weekly")).await;
    assert_eq!(response.status(), StatusCode::OK);
    drop(response);
    assert_eq!(
        server.put(id, &request("weekly")).await.status(),
        StatusCode::CONFLICT
    );
    let busy = Uuid::new_v4();
    assert_eq!(
        server.put(busy, &request("weekly")).await.status(),
        StatusCode::OK
    );
    assert_eq!(server.finished(busy).await.unwrap_err(), AskError::Busy);
    provider.release();
    assert_eq!(server.finished(id).await.unwrap(), "ok");
    assert_eq!(provider.requests().len(), 1);
    let response = server.cancel(id).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        matches!(response.json::<TurnState>().await.unwrap(), TurnState::Finished { result: Ok(answer) } if answer == "ok")
    );
    let store = crabber_coworkers::store::open(dir.path(), "scout")
        .await
        .unwrap();
    let agent = crabber_coworkers::bridge::build_agent(
        &request("weekly").coworker,
        store,
        Arc::new(common::Pending),
        common::selection(),
    )
    .unwrap();
    let run = agent
        .prompt(
            Some(crabber::SessionId::from("weekly")),
            "held by another agent",
        )
        .await
        .unwrap();
    let busy = Uuid::new_v4();
    assert_eq!(
        server.put(busy, &request("weekly")).await.status(),
        StatusCode::OK
    );
    assert_eq!(server.finished(busy).await.unwrap_err(), AskError::Busy);
    run.control().interrupt();
    run.done().await.unwrap();
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn cancel_is_idempotent_and_unknown_cancel_fences_delayed_submission() {
    use crabber::session::Store;
    let dir = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let server = Server::new(dir.path(), provider.clone()).await;
    let unknown = Uuid::new_v4();
    assert_eq!(server.cancel(unknown).await.status(), StatusCode::NOT_FOUND);
    assert_eq!(server.cancel(unknown).await.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        server.put(unknown, &request("weekly")).await.status(),
        StatusCode::CONFLICT
    );
    let id = Uuid::new_v4();
    assert_eq!(
        server.put(id, &request("weekly")).await.status(),
        StatusCode::OK
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while provider.requests().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(server.cancel(id).await.status(), StatusCode::OK);
    assert_eq!(server.cancel(id).await.status(), StatusCode::OK);
    assert_eq!(
        server.finished(id).await.unwrap_err(),
        AskError::Interrupted
    );
    let store = crabber_coworkers::store::open(dir.path(), "scout")
        .await
        .unwrap();
    assert_eq!(store.list_unfinished_runs().await.unwrap().len(), 0);
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn timeout_and_absolute_expiry_settle_without_restarting_budget() {
    use crabber::session::Store;
    let dir = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let server = Server::new(dir.path(), provider.clone()).await;
    let mut expired = request("expired");
    expired.expires_at_unix_ms = 0;
    let id = Uuid::new_v4();
    assert_eq!(server.put(id, &expired).await.status(), StatusCode::OK);
    assert_eq!(server.finished(id).await.unwrap_err(), AskError::Timeout);
    assert_eq!(provider.requests().len(), 0);
    let mut short = request("weekly");
    short.timeout_ms = 100;
    let id = Uuid::new_v4();
    assert_eq!(server.put(id, &short).await.status(), StatusCode::OK);
    assert_eq!(server.finished(id).await.unwrap_err(), AskError::Timeout);
    let store = crabber_coworkers::store::open(dir.path(), "scout")
        .await
        .unwrap();
    assert_eq!(store.list_unfinished_runs().await.unwrap().len(), 0);
}

#[tokio::test]
async fn request_validation_and_provider_errors_are_redacted() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(FakeProvider::scripted(vec![vec![common::failure(false)]]));
    let server = Server::new(dir.path(), provider.clone()).await;
    let valid = serde_json::to_value(request("weekly")).unwrap();
    let response = server
        .client
        .put(server.url(Uuid::new_v4()))
        .header("content-type", "application/json")
        .body("{\"prompt\":\"private-question-marker\",")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.text().await.unwrap(), "worker request failed");
    for (field, value) in [
        ("version", json!(2)),
        ("conversation", json!("private-question-marker\n")),
        ("prompt", json!(" ")),
        ("prompt", json!("x".repeat(1_048_577))),
        ("timeout_ms", json!(0)),
        ("timeout_ms", json!(3_600_001)),
        ("secret-marker", json!(true)),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        let response = server
            .client
            .put(server.url(Uuid::new_v4()))
            .json(&invalid)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.text().await.unwrap(), "worker request failed");
    }
    for (field, value) in [
        ("id", json!("quill")),
        ("name", json!("private-question-marker\n")),
        ("role", json!("")),
        ("instructions", json!("x".repeat(16_385))),
        (
            "slack",
            json!({"team_id":"T1","allowed_channel_ids":["C1"],"allowed_user_ids":["U1"]}),
        ),
    ] {
        let mut invalid = valid.clone();
        invalid["coworker"][field] = value;
        let response = server
            .client
            .put(server.url(Uuid::new_v4()))
            .json(&invalid)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.text().await.unwrap(), "worker request failed");
    }
    let response = server
        .client
        .put(format!("{}/v1/turns/private-question-marker", server.base))
        .json(&valid)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.text().await.unwrap(), "worker request failed");
    assert_eq!(provider.requests().len(), 0);
    let mut maximum = request("maximum");
    maximum.prompt = "x".repeat(1_048_576);
    maximum.timeout_ms = 3_600_000;
    maximum.expires_at_unix_ms = 0;
    let id = Uuid::new_v4();
    assert_eq!(server.put(id, &maximum).await.status(), StatusCode::OK);
    assert_eq!(server.finished(id).await.unwrap_err(), AskError::Timeout);
    assert_eq!(provider.requests().len(), 0);
    let id = Uuid::new_v4();
    assert_eq!(
        server.put(id, &request("weekly")).await.status(),
        StatusCode::OK
    );
    assert_eq!(server.finished(id).await.unwrap_err(), AskError::Run);
    let body = server
        .client
        .get(server.url(id))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!body.contains("secret-marker"));
    assert!(!body.contains("private-question-marker"));
}

#[tokio::test]
async fn active_and_terminal_registries_are_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::new(dir.path(), Arc::new(common::Pending)).await;
    let mut running = Vec::new();
    for index in 0..32 {
        let id = Uuid::new_v4();
        assert_eq!(
            server
                .put(id, &request(&format!("turn-{index}")))
                .await
                .status(),
            StatusCode::OK
        );
        running.push(id);
    }
    assert_eq!(
        server
            .put(Uuid::new_v4(), &request("overflow"))
            .await
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    for id in running {
        assert_eq!(server.cancel(id).await.status(), StatusCode::OK);
        assert_eq!(
            server.finished(id).await.unwrap_err(),
            AskError::Interrupted
        );
    }
    let first = Uuid::new_v4();
    let mut expired = request("expired");
    expired.expires_at_unix_ms = 0;
    assert_eq!(server.put(first, &expired).await.status(), StatusCode::OK);
    let mut last = first;
    for _ in 0..256 {
        last = Uuid::new_v4();
        assert_eq!(server.put(last, &expired).await.status(), StatusCode::OK);
    }
    assert_eq!(
        server
            .client
            .get(server.url(first))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(server.finished(last).await.unwrap_err(), AskError::Timeout);
    let tombstone = Uuid::new_v4();
    assert_eq!(
        server.cancel(tombstone).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        server.put(tombstone, &expired).await.status(),
        StatusCode::CONFLICT
    );
    for _ in 0..256 {
        assert_eq!(
            server.cancel(Uuid::new_v4()).await.status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        server.put(tombstone, &expired).await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn imported_delete_journal_store_lists_before_first_turn() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let target = directory.path().join("imported");
    std::fs::create_dir(&target).unwrap();
    let original = Server::new(&source, scripted("first-answer")).await;
    let id = Uuid::new_v4();
    assert_eq!(
        original.put(id, &request("weekly")).await.status(),
        StatusCode::OK
    );
    assert_eq!(original.finished(id).await.unwrap(), "first-answer");
    let copied = std::process::Command::new("python3")
        .args(["-c", "import sqlite3,sys; a=sqlite3.connect(sys.argv[1]); b=sqlite3.connect(sys.argv[2]); a.backup(b); b.execute('PRAGMA journal_mode=DELETE'); b.close(); a.close()"])
        .arg(source.join("scout.sqlite"))
        .arg(target.join("scout.sqlite"))
        .output().unwrap();
    assert!(copied.status.success(), "fixture snapshot must succeed");
    let header = std::fs::read(target.join("scout.sqlite")).unwrap();
    assert_eq!(&header[18..20], &[1, 1]);
    let provider = scripted("second-answer");
    let imported = Server::new(&target, provider.clone()).await;
    let response = imported
        .client
        .get(format!("{}/v1/conversations", imported.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let listing: String = response.json().await.unwrap();
    assert!(listing.starts_with("weekly\t"));
    assert_eq!(provider.requests().len(), 0);
    let id = Uuid::new_v4();
    assert_eq!(
        imported.put(id, &request("weekly")).await.status(),
        StatusCode::OK
    );
    assert_eq!(imported.finished(id).await.unwrap(), "second-answer");
    assert_eq!(provider.requests()[0].messages.len(), 3);
}
