mod common;
use std::{path::PathBuf, process::Command};
fn example() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config.example.json")
}
fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_crabber-coworkers"))
}
#[test]
fn valid_config() {
    let output = binary()
        .arg("check-config")
        .arg("--config")
        .arg(example())
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"scout\tScout\nquill\tQuill\n");
    assert_eq!(output.stderr, b"");
}
#[test]
fn invalid_config() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "{}").unwrap();
    let output = binary()
        .args(["check-config", "--config"])
        .arg(file.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(output.stdout, b"");
    assert_eq!(
        output.stderr,
        b"invalid configuration JSON at line 1, column 2 (data)\n"
    );
}
#[test]
fn missing_config() {
    let dir = tempfile::tempdir().unwrap();
    let output = binary()
        .args(["check-config", "--config"])
        .arg(dir.path().join("missing.json"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(output.stderr, b"cannot read configuration file\n");
}
#[test]
fn oversized_config() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), vec![b' '; 262_145]).unwrap();
    let output = binary()
        .args(["check-config", "--config"])
        .arg(file.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(output.stderr, b"configuration file exceeds 262144 bytes\n");
}

#[test]
fn missing_or_blank_api_key() {
    for key in [None, Some(""), Some("  ")] {
        let mut command = binary();
        command
            .args(["ask", "--config"])
            .arg(example())
            .args(["scout", "hi"])
            .env_remove("OPENCODE_GO_API_KEY");
        if let Some(key) = key {
            command.env("OPENCODE_GO_API_KEY", key);
        }
        let output = command.output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(output.stderr, b"OPENCODE_GO_API_KEY is not set\n");
        assert_eq!(output.stdout, b"");
    }
}
#[test]
fn validation_precedes_key_check() {
    for key in [None, Some("placeholder-not-a-key")] {
        let mut command = binary();
        command
            .args(["ask", "--config"])
            .arg(example())
            .args(["nobody", "hi"])
            .env_remove("OPENCODE_GO_API_KEY");
        if let Some(key) = key {
            command.env("OPENCODE_GO_API_KEY", key);
        }
        let output = command.output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(output.stderr, b"unknown coworker\n");
        assert_eq!(output.stdout, b"");
    }
}

#[test]
fn check_config_does_not_create_state_dir() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let mut value: serde_json::Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    value["state_dir"] = serde_json::json!(state);
    let config = temp.path().join("config.json");
    std::fs::write(&config, value.to_string()).unwrap();
    let output = binary()
        .args(["check-config", "--config"])
        .arg(config)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(!state.exists());
}

#[test]
fn ask_conversation_without_key_creates_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let file = common::write_state_config(temp.path(), 120);
    for (name, id, message) in [
        ("weekly", "scout", "OPENCODE_GO_API_KEY is not set\n"),
        ("Weekly", "scout", "invalid conversation name\n"),
        ("weekly", "nobody", "unknown coworker\n"),
    ] {
        let output = binary()
            .args(["ask", "--config"])
            .arg(&file)
            .args(["--conversation", name, id, "hi"])
            .env_remove("OPENCODE_GO_API_KEY")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(output.stderr, message.as_bytes());
        assert_eq!(output.stdout.len(), 0);
        assert!(!temp.path().join("state").exists());
    }
}

#[tokio::test]
async fn list_conversations_without_key_while_running() {
    use crabber::{FakeProvider, StreamDelta};
    use crabber_coworkers::{
        ask::{Session, ask},
        bridge, config, store,
    };
    use std::sync::Arc;
    let temp = tempfile::tempdir().unwrap();
    let file = common::write_state_config(temp.path(), 120);
    let listing = || {
        binary()
            .args(["list-conversations", "--config"])
            .arg(&file)
            .arg("scout")
            .env_remove("OPENCODE_GO_API_KEY")
            .output()
            .unwrap()
    };
    let empty = listing();
    assert!(empty.status.success());
    assert_eq!(empty.stdout.len(), 0);
    assert_eq!(empty.stderr.len(), 0);
    assert!(!temp.path().join("state").exists());
    let config = config::load(&file).unwrap();
    let def = config.coworker("scout").unwrap();
    let db = store::open(&config.state_dir, "scout").await.unwrap();
    ask(
        def,
        "hi",
        Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("ok".into()),
            StreamDelta::Completed,
        ]])),
        common::selection(),
        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        Session::Durable {
            store: db.clone(),
            name: "weekly".into(),
        },
        common::never(),
    )
    .await
    .unwrap();
    let agent =
        bridge::build_agent(def, db, Arc::new(common::Pending), common::selection()).unwrap();
    let run = agent
        .prompt(Some(crabber::SessionId::from("weekly")), "pending")
        .await
        .unwrap();
    let output = listing();
    assert!(output.status.success());
    assert_eq!(output.stderr.len(), 0);
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .starts_with("weekly\t")
    );
    run.control().interrupt();
    run.done().await.unwrap();
}

#[tokio::test]
async fn listing_wal_snapshot_does_not_change_database_bytes() {
    use crabber::{FakeProvider, StreamDelta};
    use crabber_coworkers::{
        ask::{Session, ask},
        config, store,
    };
    use std::sync::Arc;
    let temp = tempfile::tempdir().unwrap();
    let file = common::write_state_config(temp.path(), 120);
    let config = config::load(&file).unwrap();
    let db = store::open(&config.state_dir, "scout").await.unwrap();
    ask(
        config.coworker("scout").unwrap(),
        "hi",
        Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("ok".into()),
            StreamDelta::Completed,
        ]])),
        common::selection(),
        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        Session::Durable {
            store: db.clone(),
            name: "weekly".into(),
        },
        common::never(),
    )
    .await
    .unwrap();
    // Copy a quiescent, live store's uncheckpointed WAL to model a stopped writer.
    let snapshot = temp.path().join("state ?#% ü");
    std::fs::create_dir(&snapshot).unwrap();
    for name in ["scout.sqlite", "scout.sqlite-wal", "scout.sqlite-shm"] {
        std::fs::copy(config.state_dir.join(name), snapshot.join(name)).unwrap();
    }
    assert!(
        std::fs::metadata(snapshot.join("scout.sqlite-wal"))
            .unwrap()
            .len()
            > 32
    );
    let path = snapshot.join("scout.sqlite");
    let before = std::fs::read(&path).unwrap();
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    value["state_dir"] = serde_json::json!(snapshot);
    std::fs::write(&file, value.to_string()).unwrap();
    let output = binary()
        .args(["list-conversations", "--config"])
        .arg(&file)
        .arg("scout")
        .env_remove("OPENCODE_GO_API_KEY")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stderr, b"");
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .starts_with("weekly\t")
    );
    let unchanged = before == std::fs::read(&path).unwrap();
    assert!(unchanged, "listing changed database bytes");
    drop(db);
}

#[test]
fn serve_requires_slack_coworker() {
    let temp = tempfile::tempdir().unwrap();
    let path = common::write_state_config(temp.path(), 1);
    let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    v["coworkers"][0].as_object_mut().unwrap().remove("slack");
    std::fs::write(&path, v.to_string()).unwrap();
    let output = binary()
        .args(["serve", "--config"])
        .arg(path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(output.stderr, b"no coworker has a slack block\n");
    assert_eq!(output.stdout, b"");
    assert!(!temp.path().join("state").exists());
}

#[test]
fn serve_requires_tokens_before_network() {
    for (bot, app, message) in [
        (None, None, "SLACK_BOT_TOKEN_SCOUT is not set"),
        (
            Some("xoxp-test"),
            None,
            "SLACK_BOT_TOKEN_SCOUT has the wrong token type",
        ),
        (Some("xoxb-test"), None, "SLACK_APP_TOKEN_SCOUT is not set"),
        (
            Some("xoxb-test"),
            Some("xoxe.xapp-test"),
            "SLACK_APP_TOKEN_SCOUT has the wrong token type",
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = common::write_state_config(temp.path(), 1);
        let mut command = binary();
        command
            .args(["serve", "--config"])
            .arg(path)
            .env("OPENCODE_GO_API_KEY", "test-key")
            .env_remove("SLACK_BOT_TOKEN_SCOUT")
            .env_remove("SLACK_APP_TOKEN_SCOUT");
        if let Some(bot) = bot {
            command.env("SLACK_BOT_TOKEN_SCOUT", bot);
        }
        if let Some(app) = app {
            command.env("SLACK_APP_TOKEN_SCOUT", app);
        }
        let output = command.output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(output.stderr, format!("{message}\n").as_bytes());
        assert_eq!(output.stdout, b"");
        assert!(!temp.path().join("state").exists());
    }
}

#[test]
fn serve_requires_api_key() {
    let temp = tempfile::tempdir().unwrap();
    let path = common::write_state_config(temp.path(), 1);
    let output = binary()
        .args(["serve", "--config"])
        .arg(path)
        .env_remove("OPENCODE_GO_API_KEY")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(output.stderr, b"OPENCODE_GO_API_KEY is not set\n");
    assert_eq!(output.stdout, b"");
    assert!(!temp.path().join("state").exists());
}

#[test]
fn serve_validates_config_before_credentials() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "{}").unwrap();
    let output = binary()
        .args(["serve", "--config"])
        .arg(file.path())
        .env_remove("OPENCODE_GO_API_KEY")
        .env_remove("SLACK_BOT_TOKEN_SCOUT")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        output.stderr,
        b"invalid configuration JSON at line 1, column 2 (data)\n"
    );
    assert_eq!(output.stdout, b"");
}
