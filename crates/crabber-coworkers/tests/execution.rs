use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
};
use crabber_coworkers::{
    ask::{AskError, Cancel},
    config::{BorbConfig, CoworkerDefinition},
    execution::Remote,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Notify, task::JoinHandle, time::Instant};

#[derive(Clone, Copy)]
enum Behavior {
    Complete,
    Running,
    Failure,
    LostSubmission,
    RestartRequired,
    SlowWake,
    CompleteOnCancel,
    CompleteAfterCancel,
    HungBody,
    HungCancellation,
    OversizedBody,
}

struct Recorded {
    method: Method,
    path: String,
    body: Value,
}

struct Fake {
    behavior: Behavior,
    requests: Mutex<Vec<Recorded>>,
    polls: AtomicUsize,
    submitted: Notify,
    cancelled: AtomicBool,
}

struct Server {
    remote: Remote,
    state: Arc<Fake>,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start(behavior: Behavior) -> Server {
    let state = Arc::new(Fake {
        behavior,
        requests: Mutex::default(),
        polls: AtomicUsize::new(0),
        submitted: Notify::new(),
        cancelled: AtomicBool::new(false),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = BorbConfig {
        url: format!("http://{}", listener.local_addr().unwrap()),
        name_prefix: "coworkers-test".into(),
        image: format!("example.invalid/worker@sha256:{}", "a".repeat(64)),
        model_secret: "model-access".into(),
    };
    let router = Router::new().fallback(handler).with_state(state.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let remote = Remote::new(&config, "injected-test-token".into()).unwrap();
    Server {
        remote,
        state,
        task,
    }
}

fn coworker() -> CoworkerDefinition {
    serde_json::from_value(json!({
        "id":"scout", "name":"Scout", "role":"Researcher", "instructions":"private-instructions",
        "slack":{"team_id":"T1", "allowed_channel_ids":["C1"], "allowed_user_ids":["U1"]}
    }))
    .unwrap()
}

fn no_cancel() -> Cancel {
    Box::pin(std::future::pending())
}

fn upstream(value: Value) -> Response {
    let mut response = axum::Json(value).into_response();
    response
        .headers_mut()
        .insert("x-borb-upstream", "true".parse().unwrap());
    response
}

async fn handler(State(state): State<Arc<Fake>>, request: Request) -> Response {
    assert_eq!(
        request.headers().get("authorization").unwrap(),
        "Bearer injected-test-token"
    );
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let bytes = to_bytes(request.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    let body: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    state.requests.lock().unwrap().push(Recorded {
        method: method.clone(),
        path: path.clone(),
        body: body.clone(),
    });
    if path == "/v1/borbs" {
        if matches!(state.behavior, Behavior::SlowWake) {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        return axum::Json(json!({
            "id":"00000000000000000000000001", "client":"test", "spec":body,
            "spec_generation":1,"running_spec_generation":1,
            "restart_required":matches!(state.behavior, Behavior::RestartRequired),
            "arch":"amd64","desired":"running","phase":"running",
            "phase_reason":null,"phase_message":null,"node":"test","generation":1,
            "last_activity_at":"2026-01-01T00:00:00Z", "phase_changed_at":"2026-01-01T00:00:00Z",
            "created_at":"2026-01-01T00:00:00Z", "updated_at":"2026-01-01T00:00:00Z", "deleted_at":null
        })).into_response();
    }
    if path.ends_with("/v1/conversations") {
        return upstream(json!("thread-1\t2026-01-01T00:00:00Z\n"));
    }
    if path.ends_with("/cancel") {
        state.cancelled.store(true, Ordering::SeqCst);
        if matches!(state.behavior, Behavior::HungCancellation) {
            return Response::builder()
                .header("x-borb-upstream", "true")
                .body(Body::from_stream(futures::stream::pending::<
                    Result<axum::body::Bytes, std::io::Error>,
                >()))
                .unwrap();
        }
        return if matches!(state.behavior, Behavior::CompleteOnCancel) {
            upstream(json!({"state":"finished", "result":{"Ok":"committed remote answer"}}))
        } else {
            upstream(json!({"state":"running"}))
        };
    }
    if state.cancelled.load(Ordering::SeqCst) && method == Method::GET {
        return if matches!(state.behavior, Behavior::CompleteAfterCancel) {
            upstream(json!({"state":"finished", "result":{"Ok":"committed remote answer"}}))
        } else {
            upstream(json!({"state":"finished", "result":{"Err":"interrupted"}}))
        };
    }
    if method == Method::PUT {
        state.submitted.notify_one();
        if matches!(state.behavior, Behavior::OversizedBody) {
            return upstream(
                json!({"state":"finished", "result":{"Ok":"x".repeat(8 * 1024 * 1024 + 1)}}),
            );
        }
        if matches!(state.behavior, Behavior::LostSubmission) {
            return upstream(json!({"secret-marker":"untrusted provider failure"}));
        }
        if matches!(state.behavior, Behavior::HungBody) {
            return Response::builder()
                .header("x-borb-upstream", "true")
                .body(Body::from_stream(futures::stream::pending::<
                    Result<axum::body::Bytes, std::io::Error>,
                >()))
                .unwrap();
        }
        return upstream(json!({"state":"running"}));
    }
    if matches!(state.behavior, Behavior::Failure) {
        let mut response = (
            StatusCode::SERVICE_UNAVAILABLE,
            "secret-marker private-provider-error",
        )
            .into_response();
        response
            .headers_mut()
            .insert("x-borb-upstream", "true".parse().unwrap());
        return response;
    }
    let polls = state.polls.fetch_add(1, Ordering::SeqCst);
    if matches!(state.behavior, Behavior::Complete) && polls >= 1 {
        upstream(json!({"state":"finished", "result":{"Ok":"remote answer"}}))
    } else {
        upstream(json!({"state":"running"}))
    }
}

#[tokio::test]
async fn submits_once_then_polls_and_lists_real_remote_history() {
    let server = start(Behavior::Complete).await;
    let answer = server
        .remote
        .answer(
            &coworker(),
            "thread-1",
            "private-prompt",
            Instant::now() + Duration::from_secs(3),
            no_cancel(),
        )
        .await
        .unwrap();
    assert_eq!(answer, "remote answer");
    assert!(
        server
            .remote
            .list(&coworker())
            .await
            .unwrap()
            .starts_with("thread-1\t")
    );
    let requests = server.state.requests.lock().unwrap();
    let creates: Vec<_> = requests
        .iter()
        .filter(|request| request.path == "/v1/borbs")
        .collect();
    assert_eq!(creates.len(), 2);
    let spec = &creates[0].body;
    assert_eq!(spec["name"], "coworkers-test-scout");
    assert_eq!(spec["command"], json!(["/usr/local/bin/crabber-coworkers"]));
    assert_eq!(spec["args"], json!(["worker", "--coworker-id", "scout"]));
    assert_eq!(spec["env"], json!({}));
    assert_eq!(
        spec["secrets"],
        json!([{"name":"model-access", "as":"env"}])
    );
    assert_eq!(spec["ports"], json!([8090]));
    assert_eq!(spec["readiness"]["path"], "/healthz");
    let submits: Vec<_> = requests
        .iter()
        .filter(|request| request.method == Method::PUT)
        .collect();
    assert_eq!(submits.len(), 1);
    assert!(submits[0].body["coworker"]["slack"].is_null());
    assert_eq!(submits[0].body["prompt"], "private-prompt");
    assert_eq!(submits[0].body["conversation"], "thread-1");
    assert_eq!(submits[0].body["version"], 1);
    let expiry = submits[0].body["expires_at_unix_ms"].as_u64().unwrap();
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    assert!(expiry > now && expiry <= now + 3000);
    assert!(
        requests
            .iter()
            .filter(|request| request.method == Method::GET && request.path == submits[0].path)
            .count()
            >= 2
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.path.ends_with("/cancel"))
    );
}

#[tokio::test]
async fn failures_are_fixed_and_never_resubmit() {
    for behavior in [
        Behavior::Failure,
        Behavior::LostSubmission,
        Behavior::OversizedBody,
    ] {
        let server = start(behavior).await;
        let error = server
            .remote
            .answer(
                &coworker(),
                "thread-1",
                "private-prompt",
                Instant::now() + Duration::from_secs(2),
                no_cancel(),
            )
            .await
            .unwrap_err();
        assert_eq!(error, AskError::Run);
        assert_eq!(error.to_string(), "model run failed");
        let requests = server.state.requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == Method::PUT)
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.path.ends_with("/cancel"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn deadline_cancels_pending_turns_and_streaming_bodies() {
    for behavior in [Behavior::Running, Behavior::HungBody] {
        let server = start(behavior).await;
        let start = Instant::now();
        let error = server
            .remote
            .answer(
                &coworker(),
                "thread-1",
                "private-prompt",
                start + Duration::from_millis(180),
                no_cancel(),
            )
            .await
            .unwrap_err();
        assert_eq!(error, AskError::Timeout);
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(
            server
                .state
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.path.ends_with("/cancel"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn interruption_cancels_once_and_committed_completion_wins() {
    for behavior in [
        Behavior::Running,
        Behavior::CompleteOnCancel,
        Behavior::CompleteAfterCancel,
    ] {
        let server = start(behavior).await;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let remote = server.remote.clone();
        let turn = tokio::spawn(async move {
            remote
                .answer(
                    &coworker(),
                    "thread-1",
                    "private-prompt",
                    Instant::now() + Duration::from_secs(2),
                    Box::pin(async {
                        let _ = receiver.await;
                    }),
                )
                .await
        });
        server.state.submitted.notified().await;
        sender.send(()).unwrap();
        let result = turn.await.unwrap();
        if matches!(
            behavior,
            Behavior::CompleteOnCancel | Behavior::CompleteAfterCancel
        ) {
            assert_eq!(result, Ok("committed remote answer".into()));
        } else {
            assert_eq!(result, Err(AskError::Interrupted));
        }
        assert_eq!(
            server
                .state
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.path.ends_with("/cancel"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn wake_uses_turn_deadline_and_configuration_mismatch_fails_closed() {
    for behavior in [Behavior::SlowWake, Behavior::RestartRequired] {
        let server = start(behavior).await;
        let result = server
            .remote
            .answer(
                &coworker(),
                "thread-1",
                "private-prompt",
                Instant::now() + Duration::from_millis(50),
                no_cancel(),
            )
            .await;
        assert_eq!(
            result,
            Err(if matches!(behavior, Behavior::SlowWake) {
                AskError::Timeout
            } else {
                AskError::Run
            })
        );
        assert!(
            !server
                .state
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| request.method == Method::PUT)
        );
    }
}

#[tokio::test]
async fn cancellation_body_has_a_five_second_bound() {
    let server = start(Behavior::HungCancellation).await;
    let start = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(7),
        server.remote.answer(
            &coworker(),
            "thread-1",
            "private-prompt",
            start + Duration::from_millis(100),
            no_cancel(),
        ),
    )
    .await
    .expect("cancellation must not wait for an unbounded response body");
    assert_eq!(result, Err(AskError::Timeout));
    assert!(start.elapsed() >= Duration::from_secs(5));
    assert_eq!(
        server
            .state
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.path.ends_with("/cancel"))
            .count(),
        1
    );
}
