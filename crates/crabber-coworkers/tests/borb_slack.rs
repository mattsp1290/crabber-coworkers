mod common;

use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    response::{IntoResponse, Response},
};
use common::slack::{FakeSlack, identity, mention, wait_until};
use crabber::core::{ContentBlock, Role};
use crabber_coworkers::{
    app::serve_remote_with,
    config::BorbConfig,
    execution::Remote,
    slack::{CredentialSource, Credentials},
    worker::router_with,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use tower::ServiceExt;

static SERVICES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
type Grants = std::sync::Arc<std::sync::Mutex<Vec<Value>>>;
#[derive(Clone)]
struct Proxy {
    worker: Router,
    grants: Grants,
}
async fn borb_proxy(State(proxy): State<Proxy>, request: Request) -> Response {
    assert_eq!(request.headers()["authorization"], "Bearer injected-token");
    if request.uri().path() == "/v1/borbs" {
        let bytes = to_bytes(request.into_body(), 131_072).await.unwrap();
        let spec: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(spec["args"], json!(["worker", "--coworker-id", "scout"]));
        assert!(spec["env"].as_object().unwrap().is_empty());
        return Json(json!({
            "id":"00000000000000000000000001", "client":"test", "spec":spec,
            "spec_generation":1, "running_spec_generation":1, "restart_required":false,
            "arch":"arm64", "desired":"running", "phase":"running",
            "phase_reason":null, "phase_message":null, "node":"test-worker", "generation":1,
            "last_activity_at":"2026-01-01T00:00:00Z", "phase_changed_at":"2026-01-01T00:00:00Z",
            "created_at":"2026-01-01T00:00:00Z", "updated_at":"2026-01-01T00:00:00Z", "deleted_at":null
        })).into_response();
    }
    let path = request
        .uri()
        .path()
        .split_once("/proxy/8090/")
        .unwrap()
        .1
        .to_owned();
    let (mut parts, body) = request.into_parts();
    let body = if parts.method == axum::http::Method::PUT && path.starts_with("v2/turns/") {
        let bytes = to_bytes(body, 131_072).await.unwrap();
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        proxy
            .grants
            .lock()
            .unwrap()
            .push(request["coworker"]["workspace"].clone());
        axum::body::Body::from(bytes)
    } else {
        body
    };
    parts.uri = format!("/{path}").parse().unwrap();
    let mut response = proxy
        .worker
        .oneshot(Request::from_parts(parts, body))
        .await
        .unwrap();
    response
        .headers_mut()
        .insert("x-borb-upstream", "true".parse().unwrap());
    response
}

fn credentials() -> CredentialSource {
    CredentialSource::Injected(HashMap::from([(
        "scout".into(),
        Credentials::new("xoxb-test".into(), "xapp-test".into()).unwrap(),
    )]))
}

async fn remote_worker(
    disk: &std::path::Path,
    provider: std::sync::Arc<dyn crabber::providers::Resolver>,
) -> (Remote, tokio::task::JoinHandle<()>, Grants) {
    let worker = router_with("scout".into(), disk.into(), provider, common::selection());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = Remote::new(
        &BorbConfig {
            url: format!("http://{}", listener.local_addr().unwrap()),
            name_prefix: "coworkers".into(),
            image: format!("registry/borb/worker@sha256:{}", "a".repeat(64)),
            model_secret: "model-access".into(),
        },
        "injected-token".into(),
    )
    .unwrap();
    let grants = Grants::default();
    let proxy = Proxy {
        worker,
        grants: grants.clone(),
    };
    let borb = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(borb_proxy).with_state(proxy),
        )
        .await
        .unwrap();
    });
    (remote, borb, grants)
}

#[tokio::test]
async fn slack_mentions_run_in_worker_and_keep_thread_history() {
    let _serial = SERVICES.lock().await;
    let host = tempfile::tempdir().unwrap();
    let disk = tempfile::tempdir().unwrap();
    let path = common::write_state_config(host.path(), 5);
    let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["coworkers"][0]["slack"] = json!({
        "team_id":"T1", "allowed_channel_ids":["C1"], "allowed_user_ids":["U1"]
    });
    std::fs::write(&path, config.to_string()).unwrap();
    let provider = common::Gated::new();
    let (remote, borb, _) = remote_worker(disk.path(), provider.clone()).await;
    let fake = FakeSlack::start(identity()).await;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let base = fake.base.clone();
    let config_path = path.clone();
    let service = tokio::spawn(async move {
        let mut err = Vec::new();
        let code = serve_remote_with(
            config_path,
            remote,
            credentials(),
            Box::pin(async {
                let _ = stopped.await;
            }),
            &base,
            &mut err,
        )
        .await;
        (code, err)
    });
    fake.send(mention(
        "event-1",
        "T1",
        "C1",
        "U1",
        "1.000001",
        None,
        "<@UBOT> first request",
    ))
    .await;
    wait_until(|| provider.requests().len() == 1).await;
    fake.send(mention(
        "event-2",
        "T1",
        "C1",
        "U1",
        "1.000002",
        Some("1.000001"),
        "<@UBOT> follow-up",
    ))
    .await;
    wait_until(|| fake.acks().len() == 2).await;
    assert_eq!(provider.requests().len(), 1);
    assert!(fake.posted().is_empty());
    #[cfg(unix)]
    assert!(
        std::process::Command::new("kill")
            .args(["-USR1", &std::process::id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!service.is_finished());
    provider.release();
    wait_until(|| fake.posted().len() == 1 && provider.requests().len() == 2).await;
    assert!(!service.is_finished());
    provider.release();
    wait_until(|| fake.posted().len() == 2).await;
    #[cfg(not(unix))]
    stop.send(()).unwrap();
    let (code, err) = tokio::time::timeout(std::time::Duration::from_secs(5), service)
        .await
        .unwrap()
        .unwrap();
    #[cfg(unix)]
    drop(stop);
    assert!(
        fake.posted()
            .iter()
            .all(|p| p.thread_ts == "1.000001" && p.text == "ok")
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages.iter().any(|message| message.role == Role::User && message.parts.iter().any(|part| matches!(&part.content, ContentBlock::Text { text } if text == "first request"))));
    assert!(disk.path().join("scout.sqlite").is_file());
    let host_state = std::path::Path::new(config["state_dir"].as_str().unwrap());
    assert!(!host_state.join("scout.sqlite").exists());
    assert_eq!(code, 0);
    assert_eq!(
        String::from_utf8(err).unwrap(),
        "Slack connected for scout\nstopped\n"
    );
    borb.abort();
}

#[tokio::test]
async fn slack_new_thread_revises_worker_file_and_host_keeps_no_files() {
    let _serial = SERVICES.lock().await;
    let host = tempfile::tempdir().unwrap();
    let disk = tempfile::tempdir().unwrap();
    let path = common::write_state_config(host.path(), 5);
    let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["coworkers"][0]["workspace"] = json!("read_write");
    config["coworkers"][0]["slack"] =
        json!({"team_id":"T1","allowed_channel_ids":["C1"],"allowed_user_ids":["U1"]});
    std::fs::write(&path, config.to_string()).unwrap();
    let provider = file_provider();
    let (remote, borb, grants) = remote_worker(disk.path(), provider.clone()).await;
    let fake = FakeSlack::start(identity()).await;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let base = fake.base.clone();
    let service = tokio::spawn(async move {
        let mut err = Vec::new();
        let code = serve_remote_with(
            path,
            remote,
            credentials(),
            Box::pin(async {
                let _ = stopped.await;
            }),
            &base,
            &mut err,
        )
        .await;
        (code, err)
    });
    fake.send(mention(
        "create",
        "T1",
        "C1",
        "U1",
        "2.000001",
        None,
        "<@UBOT> private-prompt-create",
    ))
    .await;
    wait_until(|| fake.posted().len() == 1).await;
    let file = disk.path().join("workspaces/scout/draft.txt");
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "private-file-original"
    );
    fake.send(mention(
        "revise",
        "T1",
        "C1",
        "U1",
        "3.000001",
        None,
        "<@UBOT> private-prompt-revise",
    ))
    .await;
    wait_until(|| fake.posted().len() == 2).await;
    assert_eq!(
        std::fs::read_to_string(file).unwrap(),
        "private-file-revised"
    );
    stop.send(()).unwrap();
    let (code, err) = tokio::time::timeout(std::time::Duration::from_secs(5), service)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(code, 0);
    assert_eq!(
        String::from_utf8(err).unwrap(),
        "Slack connected for scout\nstopped\n"
    );
    let replies = fake.posted();
    assert_eq!(
        (&replies[0].thread_ts, &replies[0].text),
        (&"2.000001".to_owned(), &"created draft.txt".to_owned())
    );
    assert_eq!(
        (&replies[1].thread_ts, &replies[1].text),
        (&"3.000001".to_owned(), &"revised draft.txt".to_owned())
    );
    assert_eq!(
        *grants.lock().unwrap(),
        [json!("read_write"), json!("read_write")]
    );
    let requests = provider.requests();
    assert!(!requests[2].messages.iter().any(|message| message.parts.iter().any(|part| matches!(&part.content, ContentBlock::Text { text } if text.contains("private-prompt-create")))));
    assert!(
        common::tool_results(&requests[3])
            .iter()
            .any(|result| result.to_string().contains("private-file-original"))
    );
    assert!(!host.path().join("state").exists());
    borb.abort();
}

fn file_provider() -> std::sync::Arc<crabber::FakeProvider> {
    std::sync::Arc::new(crabber::FakeProvider::scripted(vec![
        common::tool_call(
            "file_write",
            &json!({"path":"draft.txt","content":"private-file-original"}),
        ),
        common::text_answer("created draft.txt"),
        common::tool_call("file_read", &json!({"path":"draft.txt"})),
        common::tool_call(
            "file_edit",
            &json!({"path":"draft.txt","anchor":"original","replacement":"revised"}),
        ),
        common::text_answer("revised draft.txt"),
    ]))
}
