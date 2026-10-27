#![cfg(unix)]
use crabber_coworkers::app::install_cancel;
use std::{process::Command, time::Duration};

mod common;
#[path = "common/example.rs"]
mod example;

#[tokio::test]
async fn install_cancel_observes_signals_and_settles_runs() {
    use crabber::{FakeProvider, StreamDelta, core::RunStatus, session::Store};
    use crabber_coworkers::{
        ask::{AskError, Session, ask},
        store,
    };
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let config = example::config();
    let def = config.coworker("scout").unwrap();
    for signal in ["-INT", "-TERM", "-HUP"] {
        let cancel = install_cancel().unwrap();
        let db = store::open(dir.path(), "scout").await.unwrap();
        let sender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                Command::new("kill")
                    .args([signal, &std::process::id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
        });
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            ask(
                def,
                &crabber_coworkers::workspace::Workspace::disabled(def).unwrap(),
                "hi",
                Arc::new(common::Pending),
                common::selection(),
                tokio::time::Instant::now() + Duration::from_secs(5),
                Session::Durable {
                    store: db.clone(),
                    name: "weekly".into(),
                },
                cancel,
            ),
        )
        .await
        .unwrap();
        sender.await.unwrap();
        assert_eq!(result.unwrap_err(), AskError::Interrupted);
        assert_eq!(db.list_unfinished_runs().await.unwrap().len(), 0);
        let messages = db
            .list_messages(&crabber::SessionId::from("weekly"), None)
            .await
            .unwrap();
        let id = messages
            .iter()
            .rev()
            .find_map(|m| m.run_id.as_ref())
            .unwrap();
        assert_eq!(
            db.get_run(id).await.unwrap().unwrap().status,
            RunStatus::Interrupted
        );
        let provider = Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("ok".into()),
            StreamDelta::Completed,
        ]]));
        assert_eq!(
            ask(
                def,
                &crabber_coworkers::workspace::Workspace::disabled(def).unwrap(),
                "again",
                provider,
                common::selection(),
                tokio::time::Instant::now() + Duration::from_secs(5),
                Session::Durable {
                    store: db,
                    name: "weekly".into()
                },
                common::never()
            )
            .await
            .unwrap(),
            "ok"
        );
    }
    // Registration must also precede polling the returned future.
    let cancel = install_cancel().unwrap();
    assert!(
        Command::new("kill")
            .args(["-INT", &std::process::id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    tokio::time::timeout(Duration::from_secs(2), cancel)
        .await
        .unwrap();
}

#[test]
fn other_commands_keep_default_disposition() {
    use std::os::unix::process::ExitStatusExt;
    let mut child = Command::new(env!("CARGO_BIN_EXE_crabber-coworkers"))
        .args(["check-config", "--config"])
        .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.json"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let _ = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    let start = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.signal() == Some(libc::SIGINT) || status.success());
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("check-config did not terminate");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
