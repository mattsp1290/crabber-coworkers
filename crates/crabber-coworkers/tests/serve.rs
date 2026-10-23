mod common;
use clap::Parser;
use common::{
    Gated,
    slack::{FakeSlack, identity, mention, wait_until},
};
use crabber::{
    FakeProvider, StreamDelta,
    core::{ContentBlock, Role},
    providers::{ModelRequest, Resolver},
};
use crabber_coworkers::{
    app::{Cli, run_with, serve_with},
    serve::{
        BUSY_NOTICE, EMPTY_NOTICE, SHUTDOWN_SECONDS, THREAD_QUEUE, TIMEOUT_NOTICE, TOO_LONG_NOTICE,
    },
    slack::{CredentialSource, Credentials},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinHandle};

fn config(dir: &Path, seconds: u64) -> PathBuf {
    let path = common::write_state_config(dir, seconds);
    edit(&path, |v| {
        v["coworkers"][0]["slack"] =
            json!({"team_id":"T1","allowed_channel_ids":["C1"],"allowed_user_ids":["U1"]});
    });
    path
}
fn edit(path: &Path, change: impl FnOnce(&mut Value)) {
    let mut v = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    change(&mut v);
    std::fs::write(path, v.to_string()).unwrap();
}
fn credentials(ids: &[&str]) -> CredentialSource {
    CredentialSource::Injected(
        ids.iter()
            .map(|id| {
                (
                    (*id).into(),
                    Credentials::new("xoxb-test".into(), "xapp-test".into()).unwrap(),
                )
            })
            .collect::<HashMap<_, _>>(),
    )
}
fn provider() -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(vec![
        vec![
            StreamDelta::TextDelta("ok".into()),
            StreamDelta::Completed
        ];
        12
    ]))
}
struct Service {
    cancel: oneshot::Sender<()>,
    task: JoinHandle<(i32, String)>,
}
impl Service {
    async fn stop(self) -> (i32, String) {
        let _ = self.cancel.send(());
        finish(self.task).await
    }
}
async fn finish(task: JoinHandle<(i32, String)>) -> (i32, String) {
    tokio::time::timeout(Duration::from_secs(SHUTDOWN_SECONDS + 2), task)
        .await
        .unwrap()
        .unwrap()
}
fn launch(path: &Path, fake: &FakeSlack, resolver: Arc<dyn Resolver>, ids: &[&str]) -> Service {
    let (cancel, rx) = oneshot::channel();
    let path = path.to_owned();
    let base = fake.base.clone();
    let credentials = credentials(ids);
    let task = tokio::spawn(async move {
        let mut err = Vec::new();
        let code = serve_with(
            path,
            resolver,
            credentials,
            common::selection(),
            Box::pin(async {
                let _ = rx.await;
            }),
            &base,
            &mut err,
        )
        .await;
        (code, String::from_utf8(err).unwrap())
    });
    Service { cancel, task }
}
async fn send(fake: &FakeSlack, id: &str, ts: &str, root: Option<&str>, text: &str) {
    fake.send(mention(id, "T1", "C1", "U1", ts, root, text))
        .await;
}
fn texts(request: &ModelRequest) -> Vec<(Role, String)> {
    request
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
                    .collect(),
            )
        })
        .collect()
}
async fn ask_again(path: &Path) {
    let cli = Cli::parse_from([
        "coworkers",
        "ask",
        "--config",
        path.to_str().unwrap(),
        "--conversation",
        "slack-c1-1-000001",
        "scout",
        "hi",
    ]);
    let mut err = Vec::new();
    assert_eq!(
        run_with(
            cli,
            provider(),
            common::selection(),
            Some(common::never()),
            &mut Vec::new(),
            &mut err
        )
        .await,
        0,
        "{}",
        String::from_utf8(err).unwrap()
    );
}

#[tokio::test]
async fn mention_is_answered_in_thread() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 5);
    let fake = FakeSlack::start(identity()).await;
    let provider = provider();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    send(&fake, "e1", "1.000001", None, "<@UBOT> hello").await;
    wait_until(|| fake.posted().len() == 1 && fake.acks().len() == 1).await;
    let post = fake.posted().remove(0);
    assert_eq!(
        (
            post.channel.as_str(),
            post.thread_ts.as_str(),
            post.text.as_str()
        ),
        ("C1", "1.000001", "ok")
    );
    let requests = provider.requests();
    assert!(
        requests[0]
            .system
            .as_ref()
            .unwrap()
            .contains("You are Scout")
    );
    assert_eq!(texts(&requests[0]), [(Role::User, "hello".into())]);
    let (code, log) = service.stop().await;
    assert_eq!(code, 0);
    assert_eq!(log, "Slack connected for scout\nstopped\n");
    assert!(!log.contains("hello") && !log.contains("xoxb") && !log.contains("ok"));
}

#[tokio::test]
async fn thread_mention_continues_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 5);
    let fake = FakeSlack::start(identity()).await;
    let provider = provider();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    send(&fake, "e1", "1.000001", None, "<@UBOT> hello").await;
    wait_until(|| fake.posted().len() == 1).await;
    send(
        &fake,
        "e2",
        "1.000002",
        Some("1.000001"),
        "<@UBOT> follow-up",
    )
    .await;
    wait_until(|| fake.posted().len() == 2).await;
    assert_eq!(
        texts(&provider.requests()[1]),
        [
            (Role::User, "hello".into()),
            (Role::Assistant, "ok".into()),
            (Role::User, "follow-up".into())
        ]
    );
    assert!(fake.posted().iter().all(|p| p.thread_ts == "1.000001"));
    assert_eq!(service.stop().await.0, 0);
}

#[tokio::test]
async fn history_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 5);
    let fake = FakeSlack::start(identity()).await;
    let service = launch(&path, &fake, provider(), &["scout"]);
    send(&fake, "e1", "1.000001", None, "<@UBOT> first").await;
    wait_until(|| fake.posted().len() == 1).await;
    send(&fake, "e2", "1.000002", Some("1.000001"), "<@UBOT> second").await;
    wait_until(|| fake.posted().len() == 2).await;
    assert_eq!(service.stop().await.0, 0);
    let provider = provider();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    wait_until(|| fake.connections() == 2).await;
    send(&fake, "e3", "1.000003", Some("1.000001"), "<@UBOT> third").await;
    wait_until(|| fake.posted().len() == 3).await;
    assert_eq!(
        texts(&provider.requests()[0]),
        [
            (Role::User, "first".into()),
            (Role::Assistant, "ok".into()),
            (Role::User, "second".into()),
            (Role::Assistant, "ok".into()),
            (Role::User, "third".into()),
        ]
    );
    assert_eq!(service.stop().await.0, 0);
    let cli = Cli::parse_from([
        "coworkers",
        "list-conversations",
        "--config",
        path.to_str().unwrap(),
        "scout",
    ]);
    let mut out = Vec::new();
    assert_eq!(
        run_with(
            cli,
            provider,
            common::selection(),
            None,
            &mut out,
            &mut Vec::new()
        )
        .await,
        0
    );
    assert!(
        String::from_utf8(out)
            .unwrap()
            .starts_with("slack-c1-1-000001\t")
    );
}

#[tokio::test]
async fn ignored_events_are_acked_silently() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 5);
    let fake = FakeSlack::start(identity()).await;
    let provider = provider();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    for (i, (key, value)) in [
        ("team_id", json!("T2")),
        ("channel", json!("C2")),
        ("user", json!("U2")),
        ("bot_id", json!("B1")),
        ("subtype", json!("message_changed")),
        ("type", json!("message")),
    ]
    .into_iter()
    .enumerate()
    {
        let mut event = mention(
            &format!("e{i}"),
            "T1",
            "C1",
            "U1",
            "1.000001",
            Some("1.000001"),
            "hello",
        );
        if key == "team_id" {
            event["payload"][key] = value;
        } else {
            event["payload"]["event"][key] = value;
        }
        fake.send(event).await;
    }
    wait_until(|| fake.acks().len() == 6).await;
    assert_eq!(fake.posted().len(), 0);
    assert_eq!(provider.requests().len(), 0);
    assert_eq!(
        service.stop().await,
        (0, "Slack connected for scout\nstopped\n".into())
    );
}

#[tokio::test]
async fn redelivery_is_deduplicated() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 5);
    let fake = FakeSlack::start(identity()).await;
    let provider = provider();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    let mut event = mention("e1", "T1", "C1", "U1", "1.000001", None, "hello");
    fake.send(event.clone()).await;
    wait_until(|| fake.posted().len() == 1).await;
    event["retry_attempt"] = json!(1);
    fake.send(event).await;
    wait_until(|| fake.acks().len() == 2).await;
    assert_eq!(fake.posted().len(), 1);
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(service.stop().await.0, 0);
}

#[tokio::test]
async fn empty_and_oversized_mentions_get_notices() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 5);
    edit(&path, |v| v["limits"]["max_prompt_bytes"] = json!(10));
    let fake = FakeSlack::start(identity()).await;
    let provider = provider();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    send(&fake, "e1", "1.000001", None, "<@UBOT>").await;
    send(
        &fake,
        "e2",
        "1.000002",
        Some("1.000001"),
        "<@UBOT> more than ten bytes",
    )
    .await;
    wait_until(|| fake.posted().len() == 2).await;
    assert_eq!(
        fake.posted()
            .iter()
            .map(|p| p.text.as_str())
            .collect::<Vec<_>>(),
        [EMPTY_NOTICE, TOO_LONG_NOTICE]
    );
    assert_eq!(provider.requests().len(), 0);
    assert_eq!(service.stop().await.0, 0);
}

#[tokio::test]
async fn full_thread_queue_posts_one_busy_notice() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 10);
    let fake = FakeSlack::start(identity()).await;
    let provider = Gated::new();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    send(&fake, "e1", "1.000001", None, "hello").await;
    wait_until(|| provider.requests().len() == 1).await;
    for i in 0..THREAD_QUEUE + 2 {
        send(
            &fake,
            &format!("queue{i}"),
            "1.000002",
            Some("1.000001"),
            "later",
        )
        .await;
    }
    wait_until(|| fake.acks().len() == THREAD_QUEUE + 3 && fake.posted().len() == 1).await;
    assert_eq!(fake.posted()[0].text, BUSY_NOTICE);
    provider.release();
    let (code, log) = service.stop().await;
    assert_eq!(code, 0);
    assert!(log.ends_with("stopped\n"));
}

#[tokio::test]
async fn turn_timeout_posts_notice_and_thread_stays_usable() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 1);
    let fake = FakeSlack::start(identity()).await;
    let provider = Gated::new();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    send(&fake, "e1", "1.000001", None, "hello").await;
    wait_until(|| fake.posted().len() == 1).await;
    assert_eq!(fake.posted()[0].text, TIMEOUT_NOTICE);
    send(&fake, "e2", "1.000002", Some("1.000001"), "later").await;
    wait_until(|| provider.requests().len() == 2).await;
    provider.release();
    wait_until(|| fake.posted().len() == 2).await;
    assert_eq!(fake.posted()[1].text, "ok");
    assert_eq!(service.stop().await.0, 0);
}

#[tokio::test]
async fn permit_covers_only_the_turn() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 10);
    edit(&path, |v| v["limits"]["max_concurrent_turns"] = json!(1));
    let fake = FakeSlack::start(identity()).await;
    fake.fail_next("chat.postMessage", "ratelimited", 5);
    let provider = Gated::new();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    send(&fake, "e1", "1.000001", None, "one").await;
    wait_until(|| provider.requests().len() == 1).await;
    send(&fake, "e2", "2.000001", None, "two").await;
    wait_until(|| fake.acks().len() == 2).await;
    assert_eq!(provider.requests().len(), 1);
    provider.release();
    wait_until(|| {
        provider.requests().len() == 2
            && fake
                .requests()
                .iter()
                .any(|r| r.method == "chat.postMessage")
    })
    .await;
    assert_eq!(fake.posted().len(), 0); // First delivery is still rate-limited.
    provider.release();
    wait_until(|| fake.posted().len() == 1).await;
    assert_eq!(fake.posted()[0].thread_ts, "2.000001");
    assert_eq!(service.stop().await.0, 0);
}

#[tokio::test]
async fn stop_interrupts_turn_and_conversation_remains_usable() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 10);
    let fake = FakeSlack::start(identity()).await;
    let provider = Gated::new();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    send(&fake, "e1", "1.000001", None, "hello").await;
    wait_until(|| provider.requests().len() == 1).await;
    assert_eq!(service.stop().await.0, 0);
    assert_eq!(fake.posted().len(), 0);
    ask_again(&path).await;
}

#[tokio::test]
async fn fatal_loop_interrupts_other_turns() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 10);
    edit(&path, |v| {
        v["coworkers"][1]["slack"] = v["coworkers"][0]["slack"].clone();
    });
    let fake = FakeSlack::start(identity()).await;
    let provider = Gated::new();
    let service = launch(&path, &fake, provider.clone(), &["scout", "quill"]);
    wait_until(|| fake.connections() == 2).await;
    fake.send_to(
        Some(0),
        mention("e1", "T1", "C1", "U1", "1.000001", None, "hello"),
    )
    .await;
    wait_until(|| provider.requests().len() == 1).await;
    fake.send_to(
        Some(1),
        json!({"type":"disconnect","reason":"link_disabled"}),
    )
    .await;
    let (code, log) = finish(service.task).await;
    assert_eq!(code, 1);
    assert!(log.contains("Slack connection stopped for quill\n"));
    assert!(!log.contains("stopped\n"));
    ask_again(&path).await;
}

#[tokio::test]
async fn startup_failures_before_connect() {
    for failure in ["invalid_auth", "team", "missing_scope"] {
        let dir = tempfile::tempdir().unwrap();
        let path = config(dir.path(), 5);
        let mut bot = identity();
        if failure == "team" {
            bot.team_id = "T2".into();
        }
        let fake = FakeSlack::start(bot).await;
        match failure {
            "invalid_auth" => fake.fail_next("auth.test", failure, 1),
            "missing_scope" => fake.fail_next("apps.connections.open", failure, 1),
            _ => {}
        }
        let service = launch(&path, &fake, provider(), &["scout"]);
        let (code, log) = finish(service.task).await;
        assert_eq!(
            (code, log),
            (1, "Slack authentication failed for scout\n".into())
        );
        assert_eq!(fake.connections(), 0);
        assert!(!dir.path().join("state").exists());
    }
}

#[tokio::test]
async fn stop_during_startup_is_interrupted() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 5);
    let fake = FakeSlack::start(identity()).await;
    fake.stall_auth();
    let service = launch(&path, &fake, provider(), &["scout"]);
    wait_until(|| !fake.requests().is_empty()).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), service.stop())
            .await
            .unwrap(),
        (130, "interrupted\n".into())
    );
    assert!(!dir.path().join("state").exists());
}

#[tokio::test]
async fn all_tokens_are_checked_before_authentication() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 5);
    edit(&path, |v| {
        v["coworkers"][1]["slack"] = v["coworkers"][0]["slack"].clone();
    });
    let fake = FakeSlack::start(identity()).await;
    let service = launch(&path, &fake, provider(), &["scout"]);
    assert_eq!(
        finish(service.task).await,
        (2, "SLACK_BOT_TOKEN_QUILL is not set\n".into())
    );
    assert_eq!(fake.requests().len(), 0);
    assert!(!dir.path().join("state").exists());
}

#[tokio::test]
async fn backlog_keeps_arrival_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 10);
    let fake = FakeSlack::start(identity()).await;
    let provider = Gated::new();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    send(&fake, "e1", "1.000001", None, "first").await;
    wait_until(|| provider.requests().len() == 1).await;
    for (id, text) in [("e2", "second"), ("e3", "third"), ("e4", "fourth")] {
        send(&fake, id, "1.000002", Some("1.000001"), text).await;
    }
    wait_until(|| fake.acks().len() == 4).await;
    for (index, text) in ["first", "second", "third", "fourth"]
        .into_iter()
        .enumerate()
    {
        wait_until(|| provider.requests().len() == index + 1).await;
        assert_eq!(
            texts(&provider.requests()[index]).last().unwrap(),
            &(Role::User, text.into())
        );
        provider.release();
        wait_until(|| fake.posted().len() == index + 1).await;
    }
    assert_eq!(service.stop().await.0, 0);
}

#[tokio::test]
async fn reconnect_preserves_dedup_and_thread_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 10);
    let fake = FakeSlack::start(identity()).await;
    let provider = provider();
    let service = launch(&path, &fake, provider.clone(), &["scout"]);
    let event = mention("e1", "T1", "C1", "U1", "1.000001", None, "first");
    fake.send(event.clone()).await;
    wait_until(|| fake.posted().len() == 1).await;
    fake.drop_socket();
    wait_until(|| fake.connections() == 2).await;
    fake.send(event).await; // Even an unmarked duplicate must be ignored.
    wait_until(|| fake.acks().len() == 2).await;
    assert_eq!(provider.requests().len(), 1);
    send(&fake, "e2", "1.000002", Some("1.000001"), "second").await;
    wait_until(|| fake.posted().len() == 2).await;
    assert_eq!(texts(&provider.requests()[1]).len(), 3);
    assert_eq!(service.stop().await.0, 0);
}

#[tokio::test]
async fn stopping_during_delivery_is_not_a_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = config(dir.path(), 10);
    let fake = FakeSlack::start(identity()).await;
    fake.fail_next("chat.postMessage", "ratelimited", 30);
    let service = launch(&path, &fake, provider(), &["scout"]);
    send(&fake, "e1", "1.000001", None, "hello").await;
    wait_until(|| {
        fake.requests()
            .iter()
            .any(|r| r.method == "chat.postMessage")
    })
    .await;
    let (code, log) = service.stop().await;
    assert_eq!(code, 0);
    assert!(!log.contains("Slack delivery failed"));
    assert!(log.ends_with("stopped\n"));
    assert_eq!(fake.posted().len(), 0);
    ask_again(&path).await;
}
