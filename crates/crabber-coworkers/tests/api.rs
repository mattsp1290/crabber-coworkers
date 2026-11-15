//! The HTTP API end to end: a real `serve` with an injected model and a fixed API token, driven
//! over TCP on 127.0.0.1.
mod common;

use common::{
    Gated,
    slack::{FakeSlack, identity, mention, wait_until},
};
use crabber::{
    FakeProvider,
    core::{ContentBlock, Role},
    providers::{ModelRequest, Resolver},
};
use crabber_coworkers::{
    api::{
        auth::ApiTokenSource,
        contract::ApiUuid,
        store::{ApiStore, FaultPoint, Faults, Outcome, RequestState},
    },
    app::{Provider, ServeOptions, serve_with_options},
    config::constants::API_TOKEN_ENV,
    serve::{SHUTDOWN_SECONDS, SHUTDOWN_TIMED_OUT},
    slack::{CredentialSource, Credentials},
};
use reqwest::{Client, Method, RequestBuilder, StatusCode, header};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::{self, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

const TOKEN: &str = "api-test-token-0123456789abcdef012345678";
const SOON: Duration = Duration::from_secs(20);

fn uuid(n: u128) -> String {
    uuid::Uuid::from_u128(n).hyphenated().to_string()
}

fn edit(path: &Path, change: impl FnOnce(&mut Value)) {
    let mut value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    change(&mut value);
    std::fs::write(path, value.to_string()).unwrap();
}

/// The example coworkers without Slack, with workspaces disabled, and with `api` as the API block.
fn api_config(directory: &Path, api: Value) -> PathBuf {
    let path = common::write_state_config(directory, 10);
    edit(&path, |config| {
        config["coworkers"][0]
            .as_object_mut()
            .unwrap()
            .remove("slack");
        config["api"] = api;
    });
    path
}

fn options(provider: Arc<dyn Resolver>) -> ServeOptions {
    ServeOptions {
        provider: Provider::Injected(provider),
        selection: common::selection(),
        api_token: ApiTokenSource::Fixed(TOKEN.into()),
        ..ServeOptions::from_env()
    }
}

fn scripted(answers: &[&str]) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(
        answers
            .iter()
            .map(|answer| common::text_answer(answer))
            .collect(),
    ))
}

/// What `serve` writes to stderr, readable while it runs.
#[derive(Clone, Default)]
struct Stderr(Arc<Mutex<Vec<u8>>>);

impl Write for Stderr {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Stderr {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// One response, as a client sees it.
struct Reply {
    status: StatusCode,
    headers: header::HeaderMap,
    text: String,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.text).unwrap_or(Value::Null)
    }

    fn error(&self) -> Option<String> {
        let body = self.json();
        let object = body.as_object()?;
        (object.len() == 1).then_some(())?;
        object.get("error")?.as_str().map(str::to_owned)
    }
}

/// A running `serve` and a client for it. Dropping it stops the service.
struct Server {
    base: String,
    client: Client,
    stderr: Stderr,
    cancel: Option<oneshot::Sender<()>>,
    task: JoinHandle<i32>,
    /// Every response body, in order, for the redaction test.
    bodies: Mutex<Vec<String>>,
}

async fn launch(config: &Path, options: ServeOptions) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (cancel, cancelled) = oneshot::channel::<()>();
    let stderr = Stderr::default();
    let mut writer = stderr.clone();
    let config = config.to_owned();
    let options = ServeOptions {
        cancel: Some(Box::pin(async {
            let _ = cancelled.await;
        })),
        api_listener: Some(listener),
        ..options
    };
    let task = tokio::spawn(async move { serve_with_options(config, options, &mut writer).await });
    let server = Server {
        base,
        client: Client::builder().no_proxy().timeout(SOON).build().unwrap(),
        stderr,
        cancel: Some(cancel),
        task,
        bodies: Mutex::default(),
    };
    server.ready().await;
    server
}

async fn start(directory: &Path, provider: Arc<dyn Resolver>) -> Server {
    launch(&api_config(directory, json!({})), options(provider)).await
}

impl Server {
    async fn ready(&self) {
        tokio::time::timeout(SOON, async {
            loop {
                assert!(
                    !self.task.is_finished(),
                    "serve exited: {}",
                    self.stderr.text()
                );
                let health = self
                    .client
                    .get(self.url("/v1/health"))
                    .bearer_auth(TOKEN)
                    .send()
                    .await;
                if health.is_ok_and(|health| health.status() == StatusCode::OK) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("serve became ready");
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    async fn send(&self, request: RequestBuilder) -> Reply {
        let response = request.send().await.unwrap();
        let reply = Reply {
            status: response.status(),
            headers: response.headers().clone(),
            text: response.text().await.unwrap(),
        };
        self.bodies.lock().unwrap().push(reply.text.clone());
        reply
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.client
            .request(method, self.url(path))
            .bearer_auth(TOKEN)
    }

    async fn get(&self, path: &str) -> Reply {
        self.send(self.request(Method::GET, path)).await
    }

    async fn post(&self, path: &str, body: &Value) -> Reply {
        self.send(self.request(Method::POST, path).json(body)).await
    }

    async fn post_raw(&self, path: &str, body: impl Into<reqwest::Body>) -> Reply {
        self.send(self.request(Method::POST, path).body(body)).await
    }

    async fn open(&self, conversation: &str, coworker: &str) -> Value {
        let reply = self
            .post(
                "/v1/conversations",
                &json!({"conversationId": conversation, "coworkerId": coworker}),
            )
            .await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.text);
        reply.json()
    }

    async fn submit(&self, conversation: &str, request: &str, text: &str) -> Reply {
        self.post(
            &format!("/v1/conversations/{conversation}/requests"),
            &json!({"requestId": request, "text": text}),
        )
        .await
    }

    async fn accepted(&self, conversation: &str, request: &str, text: &str) -> Value {
        let reply = self.submit(conversation, request, text).await;
        assert_eq!(reply.status, StatusCode::ACCEPTED, "{}", reply.text);
        reply.json()
    }

    async fn cancel(&self, conversation: &str, request: &str, body: &str) -> Reply {
        self.post_raw(
            &format!("/v1/conversations/{conversation}/requests/{request}/cancel"),
            body.to_owned(),
        )
        .await
    }

    async fn receipt(&self, conversation: &str, request: &str) -> Value {
        let reply = self
            .get(&format!(
                "/v1/conversations/{conversation}/requests/{request}"
            ))
            .await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.text);
        reply.json()
    }

    async fn reaches(&self, conversation: &str, request: &str, status: &str) -> Value {
        tokio::time::timeout(SOON, async {
            loop {
                let receipt = self.receipt(conversation, request).await;
                if receipt["status"] == status {
                    return receipt;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{request} never became {status}"))
    }

    /// Wait for `serve` to exit on its own, without a stop signal.
    async fn exited(mut self) -> (i32, String) {
        let code = tokio::time::timeout(SOON, &mut self.task)
            .await
            .unwrap_or_else(|_| panic!("serve kept running: {}", self.stderr.text()))
            .unwrap();
        (code, self.stderr.text())
    }

    async fn stop(mut self) -> (i32, String) {
        drop(self.cancel.take());
        let code = tokio::time::timeout(Duration::from_secs(SHUTDOWN_SECONDS + 5), &mut self.task)
            .await
            .expect("serve stopped")
            .unwrap();
        (code, self.stderr.text())
    }
}

fn text_of(request: &ModelRequest) -> Vec<(Role, String)> {
    request
        .messages
        .iter()
        .map(|message| {
            (
                message.role,
                message
                    .parts
                    .iter()
                    .filter_map(|part| match &part.content {
                        ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect(),
            )
        })
        .collect()
}

fn assert_error(reply: &Reply, status: StatusCode, code: &str) {
    assert_eq!(reply.status, status, "{}", reply.text);
    assert_eq!(reply.json(), json!({"error": code}), "{}", reply.text);
    assert_eq!(
        reply.headers[header::CONTENT_TYPE],
        "application/json",
        "{code}"
    );
    assert_eq!(reply.headers[header::CACHE_CONTROL], "no-store", "{code}");
}

fn document() -> Value {
    serde_json::from_str(include_str!("../../../openapi/crabber-coworkers.v1.json")).unwrap()
}

/// Every documented operation as (method, concrete path, documented path).
fn operations(document: &Value, id: &str) -> Vec<(Method, String, String)> {
    let mut operations = Vec::new();
    for (path, item) in document["paths"].as_object().unwrap() {
        for method in item.as_object().unwrap().keys() {
            let method = Method::from_bytes(method.to_uppercase().as_bytes()).unwrap();
            let concrete = path
                .replace("{conversationId}", id)
                .replace("{requestId}", id);
            operations.push((method, concrete, path.clone()));
        }
    }
    operations
}

#[tokio::test]
async fn every_route_requires_the_bearer_token() {
    let directory = tempfile::tempdir().unwrap();
    let server = start(directory.path(), scripted(&[])).await;
    let id = uuid(1);
    let mut targets: Vec<_> = operations(&document(), &id)
        .into_iter()
        .map(|(method, path, _)| (method, path))
        .collect();
    targets.extend([
        (Method::GET, "/v1/members".to_owned()),
        (Method::GET, "/healthz".to_owned()),
        (Method::PUT, "/v1/conversations".to_owned()),
        (Method::HEAD, "/v1/health".to_owned()),
        (Method::OPTIONS, "/v1/health".to_owned()),
        (Method::TRACE, "/v1/health".to_owned()),
        (Method::DELETE, format!("/v1/conversations/{id}")),
    ]);
    let wrong = "Bearer sentinel-wrong-token-0123456789abcdef0123";
    let credentials = [
        None,
        Some(wrong.to_owned()),
        Some(format!("Basic {TOKEN}")),
        Some("Bearer".to_owned()),
        Some(format!("Bearer  {TOKEN}")),
        Some(format!("Bearer {TOKEN}x")),
    ];
    for (method, path) in &targets {
        for credential in &credentials {
            let mut request = server
                .client
                .request(method.clone(), server.url(path))
                .body("{not json");
            if let Some(credential) = credential {
                request = request.header(header::AUTHORIZATION, credential);
            }
            let reply = server.send(request).await;
            assert_eq!(
                reply.status,
                StatusCode::UNAUTHORIZED,
                "{method} {path} {credential:?}"
            );
            assert_eq!(reply.headers[header::WWW_AUTHENTICATE], "Bearer");
            assert_eq!(reply.headers[header::CACHE_CONTROL], "no-store");
            // Routing has not run, so nothing tells a known path from an unknown one.
            assert!(
                !reply.headers.contains_key(header::ALLOW),
                "{method} {path} {credential:?}"
            );
            if *method != Method::HEAD {
                assert_error(&reply, StatusCode::UNAUTHORIZED, "unauthorized");
            }
        }
        let doubled = server
            .request(method.clone(), path)
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"));
        assert_eq!(
            server.send(doubled).await.status,
            StatusCode::UNAUTHORIZED,
            "{method} {path} with two headers"
        );
    }
    let lowercase = server
        .client
        .get(server.url("/v1/health"))
        .header(header::AUTHORIZATION, format!("bearer {TOKEN}"));
    assert_eq!(server.send(lowercase).await.status, StatusCode::OK);
    let (code, stderr) = server.stop().await;
    assert_eq!(code, 0, "{stderr}");
    assert!(!stderr.contains("sentinel-wrong-token"));
}

#[tokio::test]
async fn coworkers_and_capabilities_come_from_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let server = start(directory.path(), scripted(&[])).await;
    let example: Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    let reply = server.get("/v1/coworkers").await;
    assert_eq!(reply.status, StatusCode::OK);
    let mut expected = Vec::new();
    for def in example["coworkers"].as_array().unwrap() {
        let instructions = def["instructions"].as_str().unwrap();
        assert!(!reply.text.contains(instructions));
        expected.push(json!({
            "coworkerId": def["id"],
            "name": def["name"],
            "role": def["role"],
            "workspace": "disabled",
            "slack": false,
            "instructions": {
                "bytes": instructions.len(),
                "digest": format!("{:x}", Sha256::digest(instructions.as_bytes())),
            },
        }));
    }
    assert_eq!(
        reply.json(),
        json!({"apiVersion": "1", "coworkers": expected})
    );
    let capabilities = server.get("/v1/capabilities").await;
    assert_eq!(capabilities.status, StatusCode::OK);
    let capabilities = capabilities.json();
    assert_eq!(capabilities["backend"], "local");
    assert_eq!(
        capabilities["requestContract"],
        "sqlite-outbox-keyed-admission-v1"
    );
    assert_eq!(
        capabilities["limits"],
        json!({"maxPromptBytes": 32_768, "maxQueuedPerConversation": 16,
            "modelTurnSeconds": 10, "maxPageSize": 100})
    );
    assert_eq!(capabilities["statuses"].as_array().unwrap().len(), 7);
    assert_eq!(capabilities["reasons"].as_array().unwrap().len(), 13);
    server.stop().await;
}

#[tokio::test]
async fn submissions_complete_replay_and_conflict() {
    let directory = tempfile::tempdir().unwrap();
    let provider = scripted(&["first answer", "second answer"]);
    let server = start(directory.path(), provider.clone()).await;
    let (conversation, first, second) = (uuid(1), uuid(10), uuid(11));
    let opened = server.open(&conversation, "scout").await;
    let fresh = json!({
        "apiVersion": "1",
        "state": {"conversationId": conversation, "coworkerId": "scout", "phase": "idle"},
        "messages": [],
        "truncated": false,
    });
    assert_eq!(opened, fresh);
    assert_eq!(server.open(&conversation, "scout").await, fresh);
    let receipt = server
        .accepted(&conversation, &first, "first question")
        .await;
    assert_eq!(
        receipt,
        json!({
            "apiVersion": "1",
            "conversationId": conversation,
            "requestId": first,
            "acceptance": {"boundary": "sqlite_outbox"},
            "status": "accepted",
        })
    );
    let completed = server.reaches(&conversation, &first, "completed").await;
    assert_eq!(completed["result"], json!({"text": "first answer"}));
    assert_eq!(
        server
            .accepted(&conversation, &first, "first question")
            .await,
        completed
    );
    assert_eq!(provider.requests().len(), 1);
    assert_error(
        &server
            .submit(&conversation, &first, "first question ")
            .await,
        StatusCode::CONFLICT,
        "conflict",
    );
    server
        .accepted(&conversation, &second, "second question")
        .await;
    let completed = server.reaches(&conversation, &second, "completed").await;
    assert_eq!(completed["result"]["text"], "second answer");
    assert_eq!(
        text_of(&provider.requests()[1]),
        [
            (Role::User, "first question".into()),
            (Role::Assistant, "first answer".into()),
            (Role::User, "second question".into()),
        ]
    );
    let snapshot = server
        .get(&format!("/v1/conversations/{conversation}"))
        .await;
    assert_eq!(snapshot.status, StatusCode::OK);
    assert_eq!(
        snapshot.json(),
        json!({
            "apiVersion": "1",
            "state": {"conversationId": conversation, "coworkerId": "scout", "phase": "idle"},
            "messages": [
                {"requestId": first, "role": "user", "text": "first question"},
                {"requestId": first, "role": "assistant", "text": "first answer"},
                {"requestId": second, "role": "user", "text": "second question"},
                {"requestId": second, "role": "assistant", "text": "second answer"},
            ],
            "truncated": false,
        })
    );
    let (code, stderr) = server.stop().await;
    assert_eq!(code, 0);
    assert!(stderr.ends_with("stopped\n"), "{stderr}");
}

#[tokio::test]
async fn a_second_request_waits_for_the_first() {
    let directory = tempfile::tempdir().unwrap();
    let provider = Gated::new();
    let server = start(directory.path(), provider.clone()).await;
    let (conversation, first, second) = (uuid(1), uuid(10), uuid(11));
    server.open(&conversation, "quill").await;
    server.accepted(&conversation, &first, "draft one").await;
    wait_until(|| provider.requests().len() == 1).await;
    server.accepted(&conversation, &second, "draft two").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(
        server.receipt(&conversation, &first).await["status"],
        "running"
    );
    assert_eq!(
        server.receipt(&conversation, &second).await["status"],
        "accepted"
    );
    let state = server
        .get(&format!("/v1/conversations/{conversation}"))
        .await
        .json();
    assert_eq!(state["state"]["phase"], "running");
    provider.release();
    server.reaches(&conversation, &first, "completed").await;
    wait_until(|| provider.requests().len() == 2).await;
    assert_eq!(
        text_of(&provider.requests()[1]),
        [
            (Role::User, "draft one".into()),
            (Role::Assistant, "ok".into()),
            (Role::User, "draft two".into()),
        ]
    );
    provider.release();
    server.reaches(&conversation, &second, "completed").await;
    server.stop().await;
}

#[tokio::test]
async fn queue_full_once_a_conversation_is_at_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let provider = Gated::new();
    let config = api_config(directory.path(), json!({"max_queued_per_conversation": 2}));
    let server = launch(&config, options(provider.clone())).await;
    let (conversation, other) = (uuid(1), uuid(2));
    server.open(&conversation, "scout").await;
    server.open(&other, "scout").await;
    server.accepted(&conversation, &uuid(10), "one").await;
    wait_until(|| provider.requests().len() == 1).await;
    server.accepted(&conversation, &uuid(11), "two").await;
    assert_error(
        &server.submit(&conversation, &uuid(12), "three").await,
        StatusCode::CONFLICT,
        "queue_full",
    );
    server.accepted(&other, &uuid(20), "elsewhere").await;
    for _ in 0..3 {
        provider.release();
    }
    for request in [uuid(10), uuid(11)] {
        server.reaches(&conversation, &request, "completed").await;
    }
    server.reaches(&other, &uuid(20), "completed").await;
    server.accepted(&conversation, &uuid(12), "three").await;
    provider.release();
    server.reaches(&conversation, &uuid(12), "completed").await;
    server.stop().await;
}

#[tokio::test]
async fn requests_are_validated_with_fixed_error_bodies() {
    let directory = tempfile::tempdir().unwrap();
    let config = api_config(directory.path(), json!({}));
    edit(&config, |config| {
        config["limits"]["max_prompt_bytes"] = json!(1000);
    });
    let server = launch(&config, options(scripted(&["fits"]))).await;
    let conversation = uuid(0xc0_ffee);
    let requests = format!("/v1/conversations/{conversation}/requests");
    server.open(&conversation, "scout").await;
    let bad_request = StatusCode::BAD_REQUEST;
    for body in [
        json!({"requestId": uuid(10).to_uppercase(), "text": "hi"}),
        json!({"requestId": format!("{{{}}}", uuid(10)), "text": "hi"}),
        json!({"requestId": uuid(10), "text": "hi", "extra": true}),
        json!({"requestId": uuid(10), "text": "a\u{0}b"}),
        json!({"requestId": uuid(10), "text": " \n\t"}),
        json!({"requestId": uuid(10)}),
        json!({"requestId": uuid(10), "text": 7}),
    ] {
        assert_error(
            &server.post(&requests, &body).await,
            bad_request,
            "invalid_input",
        );
    }
    for body in ["{", "[]", "\"text\"", "", "{} {}"] {
        assert_error(
            &server.post_raw(&requests, body).await,
            bad_request,
            "invalid_json",
        );
    }
    assert_error(
        &server
            .submit(&conversation.to_uppercase(), &uuid(10), "hi")
            .await,
        bad_request,
        "invalid_input",
    );
    assert_error(
        &server
            .post(
                &format!("{requests}?x=1"),
                &json!({"requestId": uuid(10), "text": "hi"}),
            )
            .await,
        bad_request,
        "invalid_input",
    );
    assert_error(
        &server
            .submit(&conversation, &uuid(10), &"x".repeat(1001))
            .await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );
    let oversized = json!({"requestId": uuid(10), "text": "x".repeat(20_000)});
    assert_error(
        &server.post(&requests, &oversized).await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );
    server
        .accepted(&conversation, &uuid(10), &"x".repeat(1000))
        .await;
    server.reaches(&conversation, &uuid(10), "completed").await;
    assert_error(
        &server.submit(&uuid(2), &uuid(10), "hi").await,
        StatusCode::NOT_FOUND,
        "not_found",
    );
    server.stop().await;
}

fn open_body(conversation: &str, coworker: &str) -> Value {
    json!({"conversationId": conversation, "coworkerId": coworker})
}

#[tokio::test]
async fn other_failures_use_fixed_error_bodies() {
    let directory = tempfile::tempdir().unwrap();
    let server = start(directory.path(), scripted(&[])).await;
    let conversation = uuid(1);
    server.open(&conversation, "scout").await;
    let bad_request = StatusCode::BAD_REQUEST;
    let open = open_body;
    for (body, status, code) in [
        (open(&uuid(3), "nobody"), StatusCode::NOT_FOUND, "not_found"),
        (
            open(&conversation, "quill"),
            StatusCode::CONFLICT,
            "conflict",
        ),
        (open(&uuid(3), "Scout"), bad_request, "invalid_input"),
        (
            json!({"conversationId": uuid(3), "coworkerId": "scout", "extra": 1}),
            bad_request,
            "invalid_input",
        ),
    ] {
        assert_error(&server.post("/v1/conversations", &body).await, status, code);
    }
    for (method, path, status, code) in [
        (
            Method::PUT,
            "/v1/conversations".to_owned(),
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
        ),
        (
            Method::GET,
            format!("/v1/conversations/{}/requests/{}", uuid(1), uuid(99)),
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        (
            Method::GET,
            format!("/v1/conversations/{}", uuid(99)),
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        (
            Method::GET,
            "/v1/conversations/not-a-uuid".to_owned(),
            bad_request,
            "invalid_input",
        ),
        (
            Method::GET,
            "/v1/health?verbose=1".to_owned(),
            bad_request,
            "invalid_input",
        ),
    ] {
        assert_error(
            &server.send(server.request(method, &path)).await,
            status,
            code,
        );
    }
    let head = server
        .send(server.request(Method::HEAD, "/v1/health"))
        .await;
    assert_eq!(head.status, StatusCode::METHOD_NOT_ALLOWED);
    server.stop().await;
}

#[tokio::test]
async fn conversations_page_strictly() {
    let directory = tempfile::tempdir().unwrap();
    let server = start(directory.path(), scripted(&[])).await;
    for (n, coworker) in [(1, "scout"), (2, "scout"), (3, "quill")] {
        server.open(&uuid(n), coworker).await;
    }
    let list = async |query: &str| server.get(&format!("/v1/conversations{query}")).await;
    let ids = |page: &Value| -> Vec<String> {
        page["conversations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|state| state["conversationId"].as_str().unwrap().to_owned())
            .collect()
    };
    let mut after = None;
    for n in 1..=3 {
        let query = after.map_or("?limit=1".to_owned(), |next: String| {
            format!("?limit=1&after={next}")
        });
        let page = list(&query).await;
        assert_eq!(page.status, StatusCode::OK, "{}", page.text);
        let page = page.json();
        assert_eq!(ids(&page), [uuid(n)]);
        after = page["next"].as_str().map(str::to_owned);
        assert_eq!(after.is_none(), n == 3);
    }
    let everything = list("").await.json();
    assert_eq!(ids(&everything), [uuid(1), uuid(2), uuid(3)]);
    assert_eq!(everything["next"], Value::Null);
    assert_eq!(
        everything["conversations"][2],
        json!({"conversationId": uuid(3), "coworkerId": "quill", "phase": "idle"})
    );
    assert_eq!(ids(&list("?coworkerId=quill").await.json()), [uuid(3)]);
    assert_eq!(
        list("?coworkerId=nobody").await.json(),
        json!({"apiVersion": "1", "conversations": [], "next": null})
    );
    assert_eq!(list("?limit=100").await.status, StatusCode::OK);
    let next = list("?limit=1").await.json()["next"]
        .as_str()
        .unwrap()
        .to_owned();
    let tampered = format!(
        "{}{}",
        if next.starts_with('A') { 'B' } else { 'A' },
        &next[1..]
    );
    for query in [
        "?limit=0".to_owned(),
        "?limit=01".to_owned(),
        "?limit=101".to_owned(),
        "?limit=-1".to_owned(),
        "?limit=%2B1".to_owned(),
        "?limit=".to_owned(),
        "?limit=1&limit=1".to_owned(),
        "?unknown=1".to_owned(),
        "?coworkerId=Bad".to_owned(),
        format!("?after={tampered}"),
        format!("?after={next}="),
        "?after=".to_owned(),
    ] {
        assert_error(
            &list(&query).await,
            StatusCode::BAD_REQUEST,
            "invalid_input",
        );
    }
    server.stop().await;
}

#[tokio::test]
async fn health_reports_a_failed_store_probe_in_its_body() {
    let directory = tempfile::tempdir().unwrap();
    let faults = Arc::new(Faults::default());
    let options = ServeOptions {
        api_faults: Some(faults.clone()),
        ..options(scripted(&[]))
    };
    let server = launch(&api_config(directory.path(), json!({})), options).await;
    let ready = server.get("/v1/health").await;
    assert_eq!(ready.status, StatusCode::OK);
    assert_eq!(
        ready.json(),
        json!({"apiVersion": "1", "status": "ready",
            "dependencies": {"database": {"status": "ready"}}, "outstandingAdmissions": 0})
    );
    faults.fail(FaultPoint::Ping, 1);
    let failing = server.get("/v1/health").await;
    assert_eq!(failing.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        failing.json(),
        json!({"apiVersion": "1", "status": "not_ready",
            "dependencies": {"database": {"status": "unavailable"}}, "outstandingAdmissions": 0})
    );
    assert_eq!(failing.headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(server.get("/v1/health").await.status, StatusCode::OK);
    server.stop().await;
}

#[tokio::test]
async fn the_router_serves_exactly_the_documented_operations() {
    let directory = tempfile::tempdir().unwrap();
    let server = start(directory.path(), scripted(&[])).await;
    let id = uuid(1);
    let operations = operations(&document(), &id);
    for (method, path, _) in &operations {
        let reply = server.send(server.request(method.clone(), path)).await;
        assert_ne!(
            reply.status,
            StatusCode::METHOD_NOT_ALLOWED,
            "{method} {path}"
        );
        // Every 405 names exactly the path's documented methods.
        let mut documented: Vec<_> = operations
            .iter()
            .filter(|(_, other, _)| other == path)
            .map(|(method, _, _)| method.as_str())
            .collect();
        documented.sort_unstable();
        for refused in [Method::DELETE, Method::HEAD, Method::PUT] {
            let reply = server.send(server.request(refused.clone(), path)).await;
            assert_eq!(
                reply.status,
                StatusCode::METHOD_NOT_ALLOWED,
                "{refused} {path}"
            );
            if refused != Method::HEAD {
                assert_error(&reply, StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
            }
            let allow = reply
                .headers
                .get(header::ALLOW)
                .map(|allow| allow.to_str().unwrap());
            let mut allowed: Vec<_> = allow.unwrap_or_default().split(',').collect();
            allowed.sort_unstable();
            assert_eq!(allowed, documented, "{refused} {path}: {allow:?}");
        }
    }
    for (method, path) in [
        (Method::GET, "/v1/members".to_owned()),
        (Method::POST, format!("/v1/conversations/{id}/events")),
        (Method::POST, format!("/v1/conversations/{id}/human")),
        (Method::GET, "/healthz".to_owned()),
        (Method::GET, "/v1".to_owned()),
    ] {
        assert_error(
            &server.send(server.request(method, &path)).await,
            StatusCode::NOT_FOUND,
            "not_found",
        );
    }
    server.stop().await;
}

/// Run `serve` in process to completion, for configurations that must not start.
async fn serve_once(config: &Path, options: ServeOptions) -> (i32, String) {
    let mut stderr = Vec::new();
    let options = ServeOptions {
        cancel: Some(common::never()),
        ..options
    };
    let code = tokio::time::timeout(
        SOON,
        serve_with_options(config.to_owned(), options, &mut stderr),
    )
    .await
    .expect("serve exited");
    (code, String::from_utf8(stderr).unwrap())
}

#[tokio::test]
async fn serve_requires_a_frontend_and_a_strong_token() {
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("state");
    let neither = common::write_state_config(directory.path(), 10);
    edit(&neither, |config| {
        config["coworkers"][0]
            .as_object_mut()
            .unwrap()
            .remove("slack");
    });
    assert_eq!(
        serve_once(&neither, options(scripted(&[]))).await,
        (
            2,
            "nothing to serve; add a slack block or an api block\n".into()
        )
    );
    let config = api_config(directory.path(), json!({}));
    let missing = ServeOptions {
        api_token: ApiTokenSource::Fixed(String::new()),
        ..options(scripted(&[]))
    };
    assert_eq!(
        serve_once(&config, missing).await,
        (2, format!("{API_TOKEN_ENV} is not set\n"))
    );
    assert!(!state.exists());
    let binary = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_crabber-coworkers"));
        command
            .args(["serve", "--config"])
            .arg(&config)
            .env("OPENCODE_GO_API_KEY", "test-key")
            .env_remove(API_TOKEN_ENV);
        command
    };
    let output = binary().output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        format!("{API_TOKEN_ENV} is not set\n")
    );
    for weak in [
        "short-token",
        "forty-bytes-with-one space-0123456789abcd",
        "",
    ] {
        let output = binary().env(API_TOKEN_ENV, weak).output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{weak}");
        let expected = if weak.is_empty() {
            format!("{API_TOKEN_ENV} is not set\n")
        } else {
            format!("{API_TOKEN_ENV} must be at least 32 printable bytes\n")
        };
        assert_eq!(String::from_utf8(output.stderr).unwrap(), expected);
        assert_eq!(output.stdout, b"");
    }
    assert!(!state.exists());
}

#[tokio::test]
async fn non_loopback_listen_needs_allow_remote() {
    let directory = tempfile::tempdir().unwrap();
    let config = api_config(directory.path(), json!({"listen": "0.0.0.0:8787"}));
    assert_eq!(
        serve_once(&config, options(scripted(&[]))).await,
        (
            2,
            "api.listen must be a loopback address unless api.allow_remote is true\n".into()
        )
    );
    assert!(!directory.path().join("state").exists());
    let config = api_config(
        directory.path(),
        json!({"listen": "0.0.0.0:8787", "allow_remote": true}),
    );
    let server = launch(&config, options(scripted(&[]))).await;
    let (code, stderr) = server.stop().await;
    assert_eq!(code, 0, "{stderr}");
}

#[tokio::test]
async fn serve_binds_the_configured_address() {
    let directory = tempfile::tempdir().unwrap();
    let config = api_config(directory.path(), json!({"listen": "127.0.0.1:0"}));
    let stderr = Stderr::default();
    let mut writer = stderr.clone();
    let (cancel, cancelled) = oneshot::channel::<()>();
    let options = ServeOptions {
        cancel: Some(Box::pin(async {
            let _ = cancelled.await;
        })),
        ..options(scripted(&[]))
    };
    let task = tokio::spawn(async move { serve_with_options(config, options, &mut writer).await });
    let mut address = String::new();
    wait_until(|| {
        let text = stderr.text();
        text.strip_prefix("API listening on ")
            .and_then(|rest| rest.split_once('\n'))
            .is_some_and(|(found, _)| {
                address = found.to_owned();
                true
            })
    })
    .await;
    assert!(address.starts_with("127.0.0.1:") && !address.ends_with(":0"));
    let health = Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(format!("http://{address}/v1/health"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    drop(cancel);
    assert_eq!(task.await.unwrap(), 0);
    assert_eq!(
        stderr.text(),
        format!("API listening on {address}\nstopped\n")
    );
}

#[tokio::test]
async fn one_process_serves_slack_and_the_api() {
    let directory = tempfile::tempdir().unwrap();
    let config = common::write_state_config(directory.path(), 10);
    edit(&config, |config| {
        config["coworkers"][0]["slack"] =
            json!({"team_id": "T1", "allowed_channel_ids": ["C1"], "allowed_user_ids": ["U1"]});
        config["api"] = json!({});
    });
    let fake = FakeSlack::start(identity()).await;
    let provider = scripted(&["slack answer", "api answer"]);
    let options = ServeOptions {
        credentials: slack_credentials(),
        slack_base: fake.base.clone(),
        ..options(provider.clone())
    };
    let server = launch(&config, options).await;
    fake.send(mention(
        "e1",
        "T1",
        "C1",
        "U1",
        "1.000001",
        None,
        "<@UBOT> from slack",
    ))
    .await;
    wait_until(|| fake.posted().len() == 1).await;
    assert_eq!(fake.posted()[0].text, "slack answer");
    let coworkers = server.get("/v1/coworkers").await.json();
    assert_eq!(coworkers["coworkers"][0]["slack"], true);
    assert_eq!(coworkers["coworkers"][1]["slack"], false);
    let conversation = uuid(1);
    server.open(&conversation, "quill").await;
    server
        .accepted(&conversation, &uuid(10), "from the api")
        .await;
    let completed = server.reaches(&conversation, &uuid(10), "completed").await;
    assert_eq!(completed["result"]["text"], "api answer");
    let (code, stderr) = server.stop().await;
    assert_eq!(code, 0, "{stderr}");
    assert!(
        stderr.starts_with("API listening on 127.0.0.1:"),
        "{stderr}"
    );
    assert!(stderr.contains("Slack connected for scout\n"), "{stderr}");
    assert!(stderr.ends_with("stopped\n"), "{stderr}");
    let state = directory.path().join("state");
    for store in ["scout.sqlite", "quill.sqlite", "api.sqlite"] {
        assert!(state.join(store).exists(), "{store}");
    }
}

fn slack_credentials() -> CredentialSource {
    CredentialSource::Injected(HashMap::from([(
        "scout".into(),
        Credentials::new("xoxb-test".into(), "xapp-test".into()).unwrap(),
    )]))
}

#[tokio::test]
async fn slack_only_serve_opens_only_slack_coworkers() {
    let directory = tempfile::tempdir().unwrap();
    let config = common::write_state_config(directory.path(), 10);
    edit(&config, |config| {
        config["coworkers"][0]["slack"] =
            json!({"team_id": "T1", "allowed_channel_ids": ["C1"], "allowed_user_ids": ["U1"]});
    });
    let fake = FakeSlack::start(identity()).await;
    let (cancel, cancelled) = oneshot::channel::<()>();
    let options = ServeOptions {
        credentials: slack_credentials(),
        slack_base: fake.base.clone(),
        cancel: Some(Box::pin(async {
            let _ = cancelled.await;
        })),
        ..options(scripted(&[]))
    };
    let stderr = Stderr::default();
    let mut writer = stderr.clone();
    let task = tokio::spawn(async move { serve_with_options(config, options, &mut writer).await });
    wait_until(|| stderr.text() == "Slack connected for scout\n").await;
    drop(cancel);
    assert_eq!(task.await.unwrap(), 0);
    assert_eq!(stderr.text(), "Slack connected for scout\nstopped\n");
    let state = directory.path().join("state");
    assert!(state.join("scout.sqlite").exists());
    assert!(!state.join("quill.sqlite").exists());
    assert!(!state.join("api.sqlite").exists());
}

#[tokio::test]
async fn cancel_records_intent_and_keeps_outcomes() {
    let directory = tempfile::tempdir().unwrap();
    let provider = Gated::new();
    let server = start(directory.path(), provider.clone()).await;
    let conversation = uuid(1);
    let (running, queued, later) = (uuid(10), uuid(11), uuid(12));
    server.open(&conversation, "scout").await;
    server.accepted(&conversation, &running, "long task").await;
    wait_until(|| provider.requests().len() == 1).await;
    server.accepted(&conversation, &queued, "queued task").await;
    let cancelled = server.cancel(&conversation, &queued, "{}").await;
    assert_eq!(cancelled.status, StatusCode::ACCEPTED, "{}", cancelled.text);
    let cancelled = cancelled.json();
    assert_eq!(cancelled["status"], "cancelled");
    assert!(cancelled["cancellation"]["requestedAt"].is_string());
    let requested = server.cancel(&conversation, &running, " {} ").await;
    assert_eq!(requested.status, StatusCode::ACCEPTED);
    assert!(
        ["cancellation_requested", "cancelled"]
            .contains(&requested.json()["status"].as_str().unwrap()),
        "{}",
        requested.text
    );
    server.reaches(&conversation, &running, "cancelled").await;
    assert_eq!(provider.requests().len(), 1);
    for (body, code) in [
        (r#"{"reason":"late"}"#, "invalid_input"),
        ("[]", "invalid_json"),
        ("", "invalid_json"),
    ] {
        assert_error(
            &server.cancel(&conversation, &running, body).await,
            StatusCode::BAD_REQUEST,
            code,
        );
    }
    for (conversation, request) in [(conversation.clone(), uuid(99)), (uuid(2), running.clone())] {
        assert_error(
            &server.cancel(&conversation, &request, "{}").await,
            StatusCode::NOT_FOUND,
            "not_found",
        );
    }
    server.accepted(&conversation, &later, "short task").await;
    wait_until(|| provider.requests().len() == 2).await;
    provider.release();
    let completed = server.reaches(&conversation, &later, "completed").await;
    let kept = server.cancel(&conversation, &later, "{}").await;
    assert_eq!(kept.status, StatusCode::ACCEPTED);
    assert_eq!(kept.json(), completed);
    server.stop().await;
}

/// The schema the committed document gives the response to `method path` with `status`.
fn validator(
    document: &Value,
    path: &str,
    method: &Method,
    status: StatusCode,
) -> jsonschema::Validator {
    let operation = &document["paths"][path][method.as_str().to_lowercase()];
    let mut response = &operation["responses"][status.as_str()];
    if let Some(target) = response["$ref"].as_str() {
        response = document.pointer(target.strip_prefix('#').unwrap()).unwrap();
    }
    let mut schema = response["content"]["application/json"]["schema"].clone();
    assert!(schema.is_object(), "{method} {path} documents no {status}");
    schema["components"] = document["components"].clone();
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .build(&schema)
        .unwrap()
}

/// One exchange for the conformance test: the request, and the documented path it exercises.
struct Exchange {
    method: Method,
    documented: &'static str,
    path: String,
    body: Option<Value>,
}

fn exchange(
    method: Method,
    documented: &'static str,
    path: String,
    body: Option<Value>,
) -> Exchange {
    Exchange {
        method,
        documented,
        path,
        body,
    }
}

/// A success and a failure of every command, before the submitted request completes.
fn commands(conversation: &str, other: &str, request: &str) -> Vec<Exchange> {
    let open = |id: &str, coworker: &str| Some(open_body(id, coworker));
    let submit = |text: &str| Some(json!({"requestId": request, "text": text}));
    let conversations = "/v1/conversations";
    let requests = format!("/v1/conversations/{conversation}/requests");
    let documented = "/v1/conversations/{conversationId}/requests";
    vec![
        exchange(
            Method::POST,
            conversations,
            conversations.into(),
            open(conversation, "scout"),
        ),
        exchange(
            Method::POST,
            conversations,
            conversations.into(),
            open(other, "quill"),
        ),
        exchange(
            Method::POST,
            conversations,
            conversations.into(),
            open(&uuid(3), "nobody"),
        ),
        exchange(
            Method::POST,
            documented,
            requests.clone(),
            submit("question"),
        ),
        exchange(Method::POST, documented, requests, submit("changed")),
    ]
}

/// A success and a failure of every read, and of cancel, once the request completed.
fn reads(conversation: &str, request: &str) -> Vec<Exchange> {
    let snapshot = "/v1/conversations/{conversationId}";
    let receipt = "/v1/conversations/{conversationId}/requests/{requestId}";
    let cancel = "/v1/conversations/{conversationId}/requests/{requestId}/cancel";
    let cancel_path = format!("/v1/conversations/{conversation}/requests/{request}/cancel");
    let mut exchanges: Vec<_> = [
        ("/v1/capabilities", ""),
        ("/v1/capabilities", "?x=1"),
        ("/v1/health", ""),
        ("/v1/health", "?x"),
        ("/v1/coworkers", ""),
        ("/v1/coworkers", "?x=1"),
        ("/v1/conversations", "?limit=1"),
        ("/v1/conversations", "?limit=0"),
    ]
    .into_iter()
    .map(|(path, query)| exchange(Method::GET, path, format!("{path}{query}"), None))
    .collect();
    exchanges.extend([
        exchange(
            Method::GET,
            snapshot,
            format!("/v1/conversations/{conversation}"),
            None,
        ),
        exchange(
            Method::GET,
            snapshot,
            format!("/v1/conversations/{}", uuid(9)),
            None,
        ),
        exchange(
            Method::GET,
            receipt,
            format!("/v1/conversations/{conversation}/requests/{request}"),
            None,
        ),
        exchange(
            Method::GET,
            receipt,
            format!("/v1/conversations/{conversation}/requests/{}", uuid(9)),
            None,
        ),
        exchange(Method::POST, cancel, cancel_path.clone(), Some(json!({}))),
        exchange(Method::POST, cancel, cancel_path, Some(json!({"x": 1}))),
    ]);
    exchanges
}

#[tokio::test]
async fn live_bodies_conform_to_the_committed_document() {
    let directory = tempfile::tempdir().unwrap();
    let server = start(directory.path(), scripted(&["answer"])).await;
    let document = document();
    let (conversation, other, request) = (uuid(1), uuid(2), uuid(10));
    let mut checked = Vec::new();
    for phase in [
        commands(&conversation, &other, &request),
        Vec::new(),
        reads(&conversation, &request),
    ] {
        if phase.is_empty() {
            server.reaches(&conversation, &request, "completed").await;
        }
        for exchange in phase {
            let mut builder = server.request(exchange.method.clone(), &exchange.path);
            if let Some(body) = &exchange.body {
                builder = builder.json(body);
            }
            checked.push((exchange, server.send(builder).await));
        }
    }
    let mut successes = HashMap::new();
    let mut errors = HashMap::new();
    for (exchange, reply) in &checked {
        let (method, path, status) = (&exchange.method, exchange.documented, reply.status);
        let body = reply.json();
        if let Err(error) = validator(&document, path, method, status).validate(&body) {
            panic!("{method} {path} {status}: {body} does not conform: {error}");
        }
        let operation = &document["paths"][path][method.as_str().to_lowercase()];
        let id = operation["operationId"].as_str().unwrap().to_owned();
        if status.is_success() {
            let documented: Vec<_> = operation["responses"]
                .as_object()
                .unwrap()
                .keys()
                .filter(|code| code.starts_with('2'))
                .cloned()
                .collect();
            assert_eq!(documented, [status.as_str()], "{id}");
            successes.insert(id, body);
        } else {
            assert!(reply.error().is_some(), "{id}: {body}");
            errors.insert(id, body);
        }
    }
    assert_eq!(successes.len(), 9, "{successes:?}");
    assert_eq!(errors.len(), 9, "{errors:?}");
    assert!(successes["listConversations"]["next"].is_string());
    assert!(successes["requestStatus"]["result"]["text"].is_string());
    server.stop().await;
}

#[tokio::test]
async fn requests_survive_a_restart_through_serve() {
    let directory = tempfile::tempdir().unwrap();
    let config = api_config(directory.path(), json!({}));
    let provider = Gated::new();
    let first = launch(&config, options(provider.clone())).await;
    let conversation = uuid(1);
    let (done, interrupted, queued) = (uuid(10), uuid(11), uuid(12));
    first.open(&conversation, "scout").await;
    first.accepted(&conversation, &done, "first").await;
    wait_until(|| provider.requests().len() == 1).await;
    provider.release();
    let completed = first.reaches(&conversation, &done, "completed").await;
    first.accepted(&conversation, &interrupted, "second").await;
    wait_until(|| provider.requests().len() == 2).await;
    first.accepted(&conversation, &queued, "third").await;
    let in_use = ServeOptions {
        api_listener: Some(TcpListener::bind("127.0.0.1:0").await.unwrap()),
        ..options(scripted(&[]))
    };
    assert_eq!(
        serve_once(&config, in_use).await,
        (1, "API store is in use\n".into())
    );
    let (code, stderr) = first.stop().await;
    assert_eq!(code, 0, "{stderr}");
    assert!(stderr.ends_with("stopped\n"), "{stderr}");
    let provider = scripted(&["third answer"]);
    let second = launch(&config, options(provider.clone())).await;
    let failed = second.receipt(&conversation, &interrupted).await;
    assert_eq!(
        (&failed["status"], &failed["reason"]),
        (&json!("failed"), &json!("interrupted"))
    );
    let answered = second.reaches(&conversation, &queued, "completed").await;
    assert_eq!(answered["result"]["text"], "third answer");
    assert_eq!(second.receipt(&conversation, &done).await, completed);
    assert_eq!(completed["result"]["text"], "ok");
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(
        text_of(&provider.requests()[0]).last().unwrap(),
        &(Role::User, "third".into())
    );
    second.stop().await;
}

#[tokio::test]
async fn no_log_line_or_error_body_carries_secrets() {
    const PROMPT: &str = "sentinel-prompt-4b1d0c";
    const ANSWER: &str = "sentinel-answer-9e7a21";
    const WRONG: &str = "sentinel-wrong-token-0123456789abcdef0123";
    let directory = tempfile::tempdir().unwrap();
    let provider = Arc::new(FakeProvider::scripted(vec![
        common::text_answer(ANSWER),
        vec![common::failure(false)],
    ]));
    let server = start(directory.path(), provider).await;
    let conversation = uuid(1);
    let requests = format!("/v1/conversations/{conversation}/requests");
    for (method, path) in [
        (Method::POST, requests.clone()),
        (Method::POST, "/v1/conversations".to_owned()),
        (Method::GET, format!("/v1/conversations/{conversation}")),
    ] {
        let wrong = server
            .client
            .request(method, server.url(&path))
            .bearer_auth(WRONG)
            .json(&json!({"requestId": uuid(10), "text": PROMPT}));
        assert_eq!(server.send(wrong).await.status, StatusCode::UNAUTHORIZED);
    }
    server.open(&conversation, "scout").await;
    server.accepted(&conversation, &uuid(10), PROMPT).await;
    server.reaches(&conversation, &uuid(10), "completed").await;
    server
        .submit(&conversation, &uuid(10), &format!("{PROMPT}!"))
        .await;
    server
        .accepted(&conversation, &uuid(11), "this one fails")
        .await;
    let failed = server.reaches(&conversation, &uuid(11), "failed").await;
    assert_eq!(failed["reason"], "model_failed");
    server
        .get(&format!("/v1/conversations/{conversation}"))
        .await;
    server.get("/v1/conversations").await;
    let bodies = server.bodies.lock().unwrap().clone();
    let (code, stderr) = server.stop().await;
    assert_eq!(code, 0);
    for secret in [PROMPT, ANSWER, WRONG, TOKEN, "secret-marker"] {
        assert!(
            !stderr.contains(secret),
            "stderr carries {secret}: {stderr}"
        );
    }
    let mut snapshots = 0;
    for body in bodies {
        // Only a JSON snapshot's messages and a completed receipt's result may carry the text;
        // any other body, JSON or not, is checked whole.
        let rest = match serde_json::from_str::<Value>(&body) {
            Ok(Value::Object(mut value)) => {
                if let Some(messages) = value.remove("messages") {
                    for message in messages.as_array().unwrap() {
                        let text = message["text"].as_str().unwrap();
                        match message["role"].as_str().unwrap() {
                            "user" => assert!(!text.contains(ANSWER)),
                            _ => assert!(!text.contains(PROMPT)),
                        }
                    }
                    snapshots += 1;
                }
                if value
                    .get("status")
                    .is_some_and(|status| status == "completed")
                {
                    value.remove("result");
                }
                Value::Object(value).to_string()
            }
            _ => body.clone(),
        };
        for secret in [PROMPT, ANSWER, WRONG, TOKEN, "secret-marker"] {
            assert!(!rest.contains(secret), "a body carries {secret}: {body}");
        }
    }
    assert!(snapshots > 0);
}

#[tokio::test]
async fn stalled_connections_do_not_hold_up_shutdown() {
    use tokio::io::AsyncWriteExt;
    let directory = tempfile::tempdir().unwrap();
    let server = start(directory.path(), scripted(&[])).await;
    let address = server.base.trim_start_matches("http://").to_owned();
    // One client stops partway through its headers, before the token is ever checked; another
    // is admitted and stops partway through its body.
    let mut headers = tokio::net::TcpStream::connect(&address).await.unwrap();
    headers
        .write_all(b"GET /v1/health HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();
    let mut body = tokio::net::TcpStream::connect(&address).await.unwrap();
    let partial = format!(
        "POST /v1/conversations HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\n\
         Content-Type: application/json\r\nContent-Length: 100\r\n\r\n{{"
    );
    body.write_all(partial.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let began = std::time::Instant::now();
    let (code, stderr) = server.stop().await;
    assert_eq!(code, 0, "{stderr}");
    assert!(
        began.elapsed() < Duration::from_secs(SHUTDOWN_SECONDS / 2),
        "{:?}",
        began.elapsed()
    );
    assert!(!stderr.contains(SHUTDOWN_TIMED_OUT), "{stderr}");
    assert!(stderr.ends_with("stopped\n"), "{stderr}");
    drop((headers, body));
}

#[tokio::test]
async fn renewal_finishes_running_turns_and_starts_no_other() {
    let directory = tempfile::tempdir().unwrap();
    let config = api_config(directory.path(), json!({}));
    edit(&config, |config| {
        config["limits"]["max_concurrent_turns"] = json!(1);
    });
    let provider = Gated::new();
    let (renew, renewed) = oneshot::channel::<()>();
    let first = launch(
        &config,
        ServeOptions {
            renewal: Some(Box::pin(async {
                let _ = renewed.await;
            })),
            ..options(provider.clone())
        },
    )
    .await;
    let (conversation, other) = (uuid(1), uuid(2));
    let (running, queued, waiting) = (uuid(10), uuid(11), uuid(20));
    first.open(&conversation, "scout").await;
    first.open(&other, "quill").await;
    first.accepted(&conversation, &running, "running").await;
    wait_until(|| provider.requests().len() == 1).await;
    // One request waits behind the running one in its conversation; another waits for the
    // only turn.
    first.accepted(&conversation, &queued, "queued").await;
    first.accepted(&other, &waiting, "waiting").await;
    renew.send(()).unwrap();
    // Intake has closed once the listener refuses connections.
    let fresh = Client::builder().no_proxy().build().unwrap();
    tokio::time::timeout(SOON, async {
        while fresh.get(first.url("/v1/health")).send().await.is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("intake closed");
    provider.release();
    let (code, stderr) = first.exited().await;
    assert_eq!(code, 0, "{stderr}");
    assert!(stderr.ends_with("stopped\n"), "{stderr}");
    assert_eq!(provider.requests().len(), 1);

    let state = directory.path().join("state");
    let store = ApiStore::open(&state).await.unwrap();
    let state_of = async |conversation: &str, request: &str| {
        let row = store
            .request(
                ApiUuid::parse(conversation).unwrap(),
                ApiUuid::parse(request).unwrap(),
            )
            .await;
        row.unwrap().unwrap().state().clone()
    };
    assert_eq!(
        state_of(&conversation, &running).await,
        RequestState::Settled(Outcome::Completed("ok".into()))
    );
    assert_eq!(
        state_of(&conversation, &queued).await,
        RequestState::Accepted
    );
    assert_eq!(state_of(&other, &waiting).await, RequestState::Accepted);
    drop(store);

    let provider = scripted(&["renewed", "renewed"]);
    let second = launch(&config, options(provider.clone())).await;
    for (conversation, request) in [(&conversation, &queued), (&other, &waiting)] {
        let answered = second.reaches(conversation, request, "completed").await;
        assert_eq!(answered["result"]["text"], "renewed");
    }
    assert_eq!(provider.requests().len(), 2);
    let (code, stderr) = second.stop().await;
    assert_eq!(code, 0, "{stderr}");
}
