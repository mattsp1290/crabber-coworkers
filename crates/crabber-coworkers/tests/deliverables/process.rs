use super::*;
use axum::{Json, Router, extract::State, response::IntoResponse, routing::post};
use crabber_coworkers::worker::{PROTOCOL_VERSION, TurnRequest, TurnState};
use std::{
    collections::VecDeque,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

struct Script {
    responses: Mutex<VecDeque<Value>>,
    requests: Mutex<Vec<Value>>,
}
async fn model(
    State(script): State<Arc<Script>>,
    Json(request): Json<Value>,
) -> axum::response::Response {
    script.requests.lock().unwrap().push(request);
    let delta = script
        .responses
        .lock()
        .unwrap()
        .pop_front()
        .expect("one response per request");
    if let Some(message) = delta.get("provider_error") {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({"error":{"message":message,"type":"invalid_request_error"}})),
        )
            .into_response();
    }
    let reason = if delta.get("tool_calls").is_some() {
        "tool_calls"
    } else {
        "stop"
    };
    let stream = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":delta}]}),
        json!({"choices":[{"delta":{},"finish_reason":reason}]})
    );
    ([("content-type", "text/event-stream")], stream).into_response()
}
fn call(name: &str, arguments: &Value) -> Value {
    json!({"tool_calls":[{"index":0,"id":uuid::Uuid::new_v4().to_string(),"type":"function","function":{"name":name,"arguments":arguments.to_string()}}]})
}
async fn request_turn(base: &str, name: &str) -> Result<String, AskError> {
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let url = format!("{base}/v2/turns/{}", uuid::Uuid::new_v4());
    let request = TurnRequest {
        version: PROTOCOL_VERSION,
        coworker: definition(WorkspaceAccess::ReadWrite),
        conversation: name.into(),
        prompt: format!("private-prompt-{name}"),
        timeout_ms: 5000,
        expires_at_unix_ms: u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
            + 5000,
    };
    assert!(
        client
            .put(&url)
            .json(&request)
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            match client
                .get(&url)
                .send()
                .await
                .unwrap()
                .json::<TurnState>()
                .await
                .unwrap()
            {
                TurnState::Running => tokio::time::sleep(Duration::from_millis(10)).await,
                TurnState::Finished { result } => break result,
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn actual_worker_process_restart_reads_and_revises_another_conversations_file() {
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("worker-state");
    let script = Arc::new(Script {
        responses: Mutex::new(VecDeque::from([
            call(
                "file_write",
                &json!({"path":"draft.txt","content":"private-file-original"}),
            ),
            json!({"content":"private-answer-created"}),
            call("file_read", &json!({"path":"draft.txt"})),
            call(
                "file_edit",
                &json!({"path":"draft.txt","anchor":"original","replacement":"revised"}),
            ),
            json!({"content":"private-answer-revised"}),
            json!({"provider_error":"private-provider-error"}),
        ])),
        requests: Mutex::default(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/chat/completions", post(model))
        .with_state(script.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let first =
        worker_process::ChildWorker::start(&state, &base, &directory.path().join("ready-first"))
            .await;
    let first_id = first.id();
    assert_eq!(
        request_turn(&first.base, "create").await.unwrap(),
        "private-answer-created"
    );
    let file = state.join("workspaces/scout/draft.txt");
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "private-file-original"
    );
    let first_output = first.stop().await;
    let second =
        worker_process::ChildWorker::start(&state, &base, &directory.path().join("ready-second"))
            .await;
    assert_ne!(second.id(), first_id);
    assert_eq!(
        request_turn(&second.base, "revise").await.unwrap(),
        "private-answer-revised"
    );
    assert_eq!(
        std::fs::read_to_string(file).unwrap(),
        "private-file-revised"
    );
    assert_eq!(
        request_turn(&second.base, "failure").await,
        Err(AskError::Run)
    );
    let second_output = second.stop().await;
    for output in [
        &first_output.0,
        &first_output.1,
        &second_output.0,
        &second_output.1,
    ] {
        for private in [
            "private-prompt",
            "private-file",
            "private-answer",
            "synthetic-test-key",
            "private-provider-error",
        ] {
            assert!(!output.contains(private));
        }
    }
    let requests = script.requests.lock().unwrap();
    assert_eq!(requests.len(), 6);
    assert!(!requests[2].to_string().contains("private-prompt-create"));
    assert!(requests[3].to_string().contains("private-file-original"));
    assert_eq!(requests[0]["tools"].as_array().unwrap().len(), 4);
    server.abort();
}
