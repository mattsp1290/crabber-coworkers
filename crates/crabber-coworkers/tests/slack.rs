mod common;
use common::slack::{FakeSlack, envelope, identity, mention, wait_until};
use crabber_coworkers::{
    config::SlackConfig,
    slack::{
        Credentials,
        api::{ApiError, SlackApi},
        delivery::{self, CHUNK_CHARS, MAX_CHUNKS, TRUNCATED_MARKER},
        ingress::{self, Classified},
        socket::{self, Inbound, SocketError},
    },
};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

fn api(fake: &FakeSlack) -> Arc<SlackApi> {
    Arc::new(
        SlackApi::new(
            &fake.base,
            Credentials::new("xoxb-test".into(), "xapp-test".into()).unwrap(),
        )
        .unwrap(),
    )
}
fn config() -> SlackConfig {
    SlackConfig {
        team_id: "T1".into(),
        allowed_channel_ids: vec!["C1".into()],
        allowed_user_ids: vec!["U1".into()],
    }
}
fn cancel() -> (oneshot::Sender<()>, crabber_coworkers::ask::Cancel) {
    let (tx, rx) = oneshot::channel();
    (
        tx,
        Box::pin(async {
            let _ = rx.await;
        }),
    )
}

#[tokio::test]
async fn api_calls_carry_bearer_and_body() {
    let fake = FakeSlack::start(identity()).await;
    let api = api(&fake);
    assert_eq!(format!("{api:?}"), "SlackApi(redacted)");
    assert_eq!(api.auth_test().await.unwrap().team_id, "T1");
    assert_eq!(api.connections_open().await.unwrap().scheme(), "ws");
    api.post_message("C1", "1.000001", "ok").await.unwrap();
    let requests = fake.requests();
    assert_eq!(requests[0].bearer, "Bearer xoxb-test");
    assert_eq!(requests[1].bearer, "Bearer xapp-test");
    assert_eq!(requests[2].bearer, "Bearer xoxb-test");
    assert_eq!(
        requests[2].body,
        json!({"channel":"C1","thread_ts":"1.000001","text":"ok","parse":"none","link_names":false,"unfurl_links":false,"unfurl_media":false})
    );
}

#[tokio::test]
async fn api_errors_are_classified_and_redacted() {
    let fake = FakeSlack::start(identity()).await;
    let api = api(&fake);
    for (code, expected) in [
        ("invalid_auth", ApiError::Auth),
        (
            "ratelimited",
            ApiError::RateLimited {
                retry_after_seconds: 2,
            },
        ),
        ("not_in_channel", ApiError::Rejected),
    ] {
        fake.fail_next("auth.test", code, 2);
        let error = api.auth_test().await.err().unwrap();
        assert!(!error.to_string().contains(code));
        assert_eq!(error, expected);
    }
    fake.fail_next("auth.test", "ratelimited", 90);
    assert_eq!(
        api.auth_test().await.err(),
        Some(ApiError::RateLimited {
            retry_after_seconds: 30
        })
    );
    fake.invalid_url();
    assert_eq!(api.connections_open().await, Err(ApiError::Transport));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    drop(listener);
    let api = SlackApi::new(
        &base,
        Credentials::new("xoxb-test".into(), "xapp-test".into()).unwrap(),
    )
    .unwrap();
    assert_eq!(api.auth_test().await.err(), Some(ApiError::Transport));
}

#[tokio::test]
async fn socket_round_trip_acks_after_handle() {
    let fake = FakeSlack::start(identity()).await;
    let api = api(&fake);
    let first = api.connections_open().await.unwrap();
    let (tx, stop) = cancel();
    let (log, _) = mpsc::unbounded_channel();
    let handled = Arc::new(AtomicUsize::new(0));
    let record = handled.clone();
    let task = tokio::spawn(socket::run(
        api,
        Some(first),
        stop,
        log,
        move |event| match event {
            Inbound::Ready => None,
            Inbound::Envelope(event) => {
                record.fetch_add(1, Ordering::SeqCst);
                Some(event.id)
            }
        },
    ));
    fake.send(envelope("e1", 0, json!({}))).await;
    wait_until(|| !fake.acks().is_empty()).await;
    assert_eq!(handled.load(Ordering::SeqCst), 1);
    assert_eq!(fake.acks(), ["e1"]);
    tx.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Ok(())
    );
}

#[tokio::test]
async fn socket_ack_only_when_handle_says_so() {
    let fake = FakeSlack::start(identity()).await;
    let (tx, stop) = cancel();
    let (log, _) = mpsc::unbounded_channel();
    let handled = Arc::new(AtomicUsize::new(0));
    let record = handled.clone();
    let task = tokio::spawn(socket::run(api(&fake), None, stop, log, move |event| {
        if matches!(event, Inbound::Envelope(_)) {
            record.fetch_add(1, Ordering::SeqCst);
        }
        None
    }));
    fake.send(envelope("e1", 0, json!({}))).await;
    wait_until(|| handled.load(Ordering::SeqCst) == 1).await;
    fake.send(json!({"type":"interactive","envelope_id":"e2"}))
        .await;
    wait_until(|| !fake.acks().is_empty()).await;
    assert_eq!(fake.acks(), ["e2"]);
    tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn socket_reconnects_after_drop_and_stops_on_link_disabled() {
    let fake = FakeSlack::start(identity()).await;
    let (_tx, stop) = cancel();
    let (log, mut lines) = mpsc::unbounded_channel();
    let ready = Arc::new(AtomicUsize::new(0));
    let count = ready.clone();
    let task = tokio::spawn(socket::run(api(&fake), None, stop, log, move |event| {
        if matches!(event, Inbound::Ready) {
            count.fetch_add(1, Ordering::SeqCst);
        }
        None
    }));
    wait_until(|| ready.load(Ordering::SeqCst) == 1).await;
    fake.drop_socket();
    wait_until(|| ready.load(Ordering::SeqCst) == 2).await;
    assert_eq!(fake.opens(), 2);
    assert_eq!(lines.try_recv().unwrap(), "Slack reconnecting");
    fake.send(json!({"type":"disconnect","reason":"link_disabled"}))
        .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap(),
        Err(SocketError::Disabled)
    );
}

#[tokio::test]
async fn socket_rejected_reconnect_is_fatal() {
    for error in ["invalid_auth", "missing_scope"] {
        let fake = FakeSlack::start(identity()).await;
        let (_tx, stop) = cancel();
        let (log, _) = mpsc::unbounded_channel();
        let task = tokio::spawn(socket::run(api(&fake), None, stop, log, |_| None));
        wait_until(|| fake.connections() == 1).await;
        fake.fail_next("apps.connections.open", error, 1);
        fake.drop_socket();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap(),
            Err(SocketError::Auth)
        );
    }
}

#[tokio::test]
async fn socket_stops_on_cancel() {
    let fake = FakeSlack::start(identity()).await;
    let (tx, stop) = cancel();
    let (log, _) = mpsc::unbounded_channel();
    let task = tokio::spawn(socket::run(api(&fake), None, stop, log, |_| None));
    wait_until(|| fake.connections() == 1).await;
    tx.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Ok(())
    );
}

#[test]
fn classify_matrix() {
    let root = mention("e1", "T1", "C1", "U1", "1.000001", None, "<@UBOT> hello");
    for (path, value) in [
        (vec!["team_id"], json!("T2")),
        (vec!["type"], json!("other")),
        (vec!["event", "type"], json!("message")),
        (vec!["event", "bot_id"], json!(null)),
        (vec!["event", "subtype"], json!("message_changed")),
        (vec!["event", "user"], json!("UBOT")),
        (vec!["event", "channel"], json!("C2")),
        (vec!["event", "user"], json!("U2")),
        (vec!["event", "ts"], json!("")),
        (vec!["event_id"], json!("")),
    ] {
        let mut payload = root["payload"].clone();
        let mut field = &mut payload;
        for key in path {
            field = &mut field[key];
        }
        *field = value;
        assert!(matches!(
            ingress::classify(&config(), &identity(), &payload),
            Classified::Ignore
        ));
    }
    for thread in [None, Some("1.000001")] {
        let payload = mention("e1", "T1", "C1", "U1", "1.000001", thread, "<@UBOT> hello");
        let Classified::Mention(event) =
            ingress::classify(&config(), &identity(), &payload["payload"])
        else {
            panic!()
        };
        assert_eq!(event.root_ts, "1.000001");
        assert_eq!(event.text, "hello");
    }
    assert!(matches!(
        ingress::classify(&config(), &identity(), &json!(null)),
        Classified::Ignore
    ));
}

#[test]
fn strip_mentions_and_decode() {
    assert_eq!(
        ingress::strip_mentions("<@U1> hi &lt;b&gt; &amp;amp; <@U1|scout>", "U1"),
        "hi <b> &amp;"
    );
    assert_eq!(ingress::strip_mentions("<@U1>", "U1"), "");
    assert_eq!(
        ingress::strip_mentions("<@U10> <@U1|x> <@U1", "U1"),
        "<@U10>  <@U1"
    );
}
#[test]
fn conversation_name_shape() {
    assert_eq!(
        ingress::conversation_name("C0123ABC", "1515449522.000016").unwrap(),
        "slack-c0123abc-1515449522-000016"
    );
    assert!(
        ingress::conversation_name(&format!("C{}", "A".repeat(31)), "1515449522.000016").is_some()
    );
    assert!(ingress::conversation_name("C_1", "1.000001").is_none());
}

#[test]
fn chunks_respect_limits() {
    for (text, expected) in [
        ("a".repeat(3499), 1),
        ("a".repeat(3501), 2),
        ("🦀".repeat(7001), 3),
        ("a".repeat(3000) + "\n" + &"b".repeat(999), 2),
    ] {
        let chunks = delivery::chunks(&text);
        assert_eq!(chunks.len(), expected);
        assert_eq!(chunks.concat(), text);
        assert!(chunks.iter().all(|v| v.chars().count() <= CHUNK_CHARS));
        if text.contains('\n') {
            assert!(chunks[0].ends_with('\n'));
        }
    }
}
#[test]
fn chunks_never_split_entities() {
    let escaped = delivery::escape(&"&".repeat(2000));
    let chunks = delivery::chunks(&escaped);
    assert_eq!(chunks.concat(), escaped);
    for chunk in chunks {
        assert!(chunk.starts_with('&') && chunk.ends_with(';'));
        assert!(chunk.chars().count() <= CHUNK_CHARS);
        assert_eq!(chunk.replace("&amp;", ""), "");
    }
    let chunks = delivery::chunks(&delivery::escape(&"<>&".repeat(10000)));
    assert!(chunks.last().unwrap().ends_with(TRUNCATED_MARKER));
    assert!(
        !chunks
            .last()
            .unwrap()
            .split(TRUNCATED_MARKER)
            .next()
            .unwrap()
            .ends_with('&')
    );
}
#[test]
fn chunks_are_capped() {
    let chunks = delivery::chunks(&"a".repeat(40000));
    assert_eq!(chunks.len(), MAX_CHUNKS);
    assert!(chunks.last().unwrap().ends_with(TRUNCATED_MARKER));
    assert!(chunks.iter().all(|v| v.chars().count() <= CHUNK_CHARS));
}

#[tokio::test]
async fn post_answer_retries_rate_limit_once() {
    let fake = FakeSlack::start(identity()).await;
    let api = api(&fake);
    fake.fail_next_post("ratelimited");
    delivery::post_answer(&api, "C1", "1.000001", "ok", &mut common::never())
        .await
        .unwrap();
    assert_eq!(fake.posted().len(), 1);
    fake.fail_next_post("ratelimited");
    fake.fail_next_post("ratelimited");
    assert!(matches!(
        delivery::post_answer(&api, "C1", "1.000001", "again", &mut common::never()).await,
        Err(ApiError::RateLimited { .. })
    ));
    assert_eq!(fake.posted().len(), 1);
    fake.fail_next("chat.postMessage", "ratelimited", 30);
    let (tx, mut stop) = cancel();
    let sender = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send(()).unwrap();
    });
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            delivery::post_answer(&api, "C1", "1.000001", "cancelled", &mut stop)
        )
        .await
        .unwrap(),
        Err(ApiError::Transport)
    );
    sender.await.unwrap();
    assert_eq!(fake.posted().len(), 1);
}

#[tokio::test]
async fn socket_transport_failure_reconnects_until_cancelled() {
    let fake = FakeSlack::start(identity()).await;
    let (tx, stop) = cancel();
    let (log, mut lines) = mpsc::unbounded_channel();
    let task = tokio::spawn(socket::run(api(&fake), None, stop, log, |_| None));
    wait_until(|| fake.connections() == 1).await;
    fake.stop_http();
    fake.drop_socket();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), lines.recv())
            .await
            .unwrap()
            .unwrap(),
        "Slack reconnecting"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), lines.recv())
            .await
            .unwrap()
            .unwrap(),
        "Slack reconnecting"
    );
    tx.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Ok(())
    );
}
