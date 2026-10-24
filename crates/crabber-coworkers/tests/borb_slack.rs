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

async fn borb_proxy(State(worker): State<Router>, request: Request) -> Response {
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
    parts.uri = format!("/{path}").parse().unwrap();
    let mut response = worker
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
) -> (Remote, tokio::task::JoinHandle<()>) {
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
    let borb = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(borb_proxy).with_state(worker),
        )
        .await
        .unwrap();
    });
    (remote, borb)
}

#[tokio::test]
async fn slack_mentions_run_in_worker_and_keep_thread_history() {
    let host = tempfile::tempdir().unwrap();
    let disk = tempfile::tempdir().unwrap();
    let path = common::write_state_config(host.path(), 5);
    let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["coworkers"][0]["slack"] = json!({
        "team_id":"T1", "allowed_channel_ids":["C1"], "allowed_user_ids":["U1"]
    });
    std::fs::write(&path, config.to_string()).unwrap();
    let provider = common::Gated::new();
    let (remote, borb) = remote_worker(disk.path(), provider.clone()).await;
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
