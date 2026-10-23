mod common;
use clap::Parser;
use crabber::{FakeProvider, StreamDelta};
use crabber_coworkers::app::{Cli, run_with};
use std::{
    io::{self, Write},
    sync::Arc,
    time::{Duration, Instant},
};

fn cli(file: &std::path::Path, id: &str, prompt: &str) -> Cli {
    Cli::parse_from([
        "crabber-coworkers",
        "ask",
        "--config",
        file.to_str().unwrap(),
        id,
        prompt,
    ])
}
fn config_file() -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut json: serde_json::Value =
        serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
    json["limits"] = serde_json::json!({"model_turn_seconds":1});
    std::fs::write(file.path(), json.to_string()).unwrap();
    file
}
#[tokio::test]
async fn success() {
    let file = config_file();
    let mut out = Vec::new();
    let mut err = Vec::new();
    let provider = Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("ok".into()),
        StreamDelta::Completed,
    ]]));
    assert_eq!(
        run_with(
            cli(file.path(), "scout", "hi"),
            provider,
            common::selection(),
            Some(common::never()),
            &mut out,
            &mut err
        )
        .await,
        0
    );
    assert_eq!(out, b"ok\n");
    assert_eq!(err, b"");
}
#[tokio::test]
async fn redacted_failure() {
    let file = config_file();
    let mut out = Vec::new();
    let mut err = Vec::new();
    let provider = Arc::new(FakeProvider::scripted(vec![vec![common::failure(false)]]));
    assert_eq!(
        run_with(
            cli(file.path(), "scout", "hi"),
            provider,
            common::selection(),
            Some(common::never()),
            &mut out,
            &mut err
        )
        .await,
        1
    );
    assert_eq!(out, b"");
    assert_eq!(err, b"model run failed\n");
}
#[tokio::test]
async fn configured_timeout() {
    let file = config_file();
    let mut out = Vec::new();
    let mut err = Vec::new();
    let start = Instant::now();
    assert_eq!(
        run_with(
            cli(file.path(), "scout", "hi"),
            Arc::new(common::Pending),
            common::selection(),
            Some(common::never()),
            &mut out,
            &mut err
        )
        .await,
        1
    );
    assert_eq!(err, b"model turn timed out\n");
    assert_eq!(out, b"");
    assert!(start.elapsed() < Duration::from_secs(3));
}
struct Broken {
    flush_only: bool,
}
impl Write for Broken {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.flush_only {
            Ok(buf.len())
        } else {
            Err(io::Error::other("secret-marker"))
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("secret-marker"))
    }
}
#[tokio::test]
async fn output_failures() {
    for flush_only in [false, true] {
        let file = config_file();
        let mut err = Vec::new();
        let provider = Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("ok".into()),
            StreamDelta::Completed,
        ]]));
        assert_eq!(
            run_with(
                cli(file.path(), "scout", "hi"),
                provider,
                common::selection(),
                Some(common::never()),
                &mut Broken { flush_only },
                &mut err
            )
            .await,
            1
        );
        assert_eq!(err, b"cannot write output\n");
    }
}
#[tokio::test]
async fn invalid_requests_never_reach_provider() {
    let file = config_file();
    for (id, prompt, message) in [
        ("nobody", "hi", "unknown coworker\n"),
        ("scout", "  ", "prompt is empty\n"),
        (
            "scout",
            &"é".repeat(16_385),
            "prompt exceeds max_prompt_bytes\n",
        ),
    ] {
        let provider = Arc::new(FakeProvider::default());
        let mut out = Vec::new();
        let mut err = Vec::new();
        assert_eq!(
            run_with(
                cli(file.path(), id, prompt),
                provider.clone(),
                common::selection(),
                Some(common::never()),
                &mut out,
                &mut err
            )
            .await,
            2
        );
        assert_eq!(provider.requests().len(), 0);
        assert_eq!(out, b"");
        assert_eq!(err, message.as_bytes());
    }
}

fn conversation_cli(file: &std::path::Path, name: &str) -> Cli {
    Cli::parse_from([
        "crabber-coworkers",
        "ask",
        "--config",
        file.to_str().unwrap(),
        "--conversation",
        name,
        "scout",
        "hi",
    ])
}
fn list_cli(file: &std::path::Path) -> Cli {
    Cli::parse_from([
        "crabber-coworkers",
        "list-conversations",
        "--config",
        file.to_str().unwrap(),
        "scout",
    ])
}
fn ok_provider() -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("ok".into()),
        StreamDelta::Completed,
    ]]))
}

#[tokio::test]
async fn ask_with_conversation_round_trip() {
    let temp = tempfile::tempdir().unwrap();
    let file = common::write_state_config(temp.path(), 1);
    for turn in 0..2 {
        let provider = ok_provider();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        assert_eq!(
            run_with(
                conversation_cli(&file, "weekly"),
                provider.clone(),
                common::selection(),
                Some(common::never()),
                &mut out,
                &mut err
            )
            .await,
            0
        );
        assert_eq!(out, b"ok\n");
        assert_eq!(err.len(), 0);
        assert_eq!(provider.requests()[0].messages.len(), 1 + 2 * turn);
    }
}

#[tokio::test]
async fn ask_without_conversation_leaves_state_dir_absent() {
    let temp = tempfile::tempdir().unwrap();
    let file = common::write_state_config(temp.path(), 1);
    let (mut out, mut err) = (Vec::new(), Vec::new());
    assert_eq!(
        run_with(
            cli(&file, "scout", "hi"),
            ok_provider(),
            common::selection(),
            Some(common::never()),
            &mut out,
            &mut err
        )
        .await,
        0
    );
    assert_eq!(out, b"ok\n");
    assert_eq!(err.len(), 0);
    assert!(!temp.path().join("state").exists());
}

#[tokio::test]
async fn invalid_conversation_exit_2() {
    let temp = tempfile::tempdir().unwrap();
    let file = common::write_state_config(temp.path(), 1);
    let provider = ok_provider();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    assert_eq!(
        run_with(
            conversation_cli(&file, "Weekly"),
            provider.clone(),
            common::selection(),
            Some(common::never()),
            &mut out,
            &mut err
        )
        .await,
        2
    );
    assert_eq!(out.len(), 0);
    assert_eq!(err, b"invalid conversation name\n");
    assert_eq!(provider.requests().len(), 0);
    assert!(!temp.path().join("state").exists());
}

#[tokio::test]
async fn interrupted_exit_130() {
    for durable in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let file = common::write_state_config(temp.path(), 1);
        let command = if durable {
            conversation_cli(&file, "weekly")
        } else {
            cli(&file, "scout", "hi")
        };
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let cancel = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(70)).await;
        });
        assert_eq!(
            run_with(
                command,
                Arc::new(common::Pending),
                common::selection(),
                Some(cancel),
                &mut out,
                &mut err
            )
            .await,
            130
        );
        assert_eq!(out.len(), 0);
        assert_eq!(err, b"interrupted\n");
        if durable {
            assert_eq!(
                run_with(
                    conversation_cli(&file, "weekly"),
                    ok_provider(),
                    common::selection(),
                    Some(common::never()),
                    &mut Vec::new(),
                    &mut Vec::new()
                )
                .await,
                0
            );
        }
    }
}

#[tokio::test]
async fn cancel_before_store_open_starts_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let file = common::write_state_config(temp.path(), 1);
    let provider = ok_provider();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    assert_eq!(
        run_with(
            conversation_cli(&file, "weekly"),
            provider.clone(),
            common::selection(),
            Some(Box::pin(std::future::ready(()))),
            &mut out,
            &mut err
        )
        .await,
        130
    );
    assert_eq!(provider.requests().len(), 0);
    assert!(!temp.path().join("state").exists());
    assert_eq!(out.len(), 0);
    assert_eq!(err, b"interrupted\n");
}

#[tokio::test]
async fn list_conversations_lists_names_and_utc() {
    let temp = tempfile::tempdir().unwrap();
    let file = common::write_state_config(temp.path(), 1);
    for name in ["alpha", "beta"] {
        assert_eq!(
            run_with(
                conversation_cli(&file, name),
                ok_provider(),
                common::selection(),
                Some(common::never()),
                &mut Vec::new(),
                &mut Vec::new()
            )
            .await,
            0
        );
    }
    let path = temp.path().join("state/scout.sqlite");
    let before = std::fs::metadata(&path).unwrap();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    assert_eq!(
        run_with(
            list_cli(&file),
            ok_provider(),
            common::selection(),
            Some(common::never()),
            &mut out,
            &mut err
        )
        .await,
        0
    );
    assert_eq!(err.len(), 0);
    let output = String::from_utf8(out).unwrap();
    let rows: Vec<_> = output
        .lines()
        .map(|line| line.split_once('\t').unwrap())
        .collect();
    assert_eq!(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
    for (_, timestamp) in rows {
        assert!(timestamp.ends_with('Z'));
    }
    assert_eq!(before.len(), std::fs::metadata(&path).unwrap().len());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            before.permissions().mode(),
            std::fs::metadata(&path).unwrap().permissions().mode()
        );
    }
}

#[tokio::test]
async fn list_conversations_without_store_prints_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let file = common::write_state_config(temp.path(), 1);
    let provider = ok_provider();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    assert_eq!(
        run_with(
            list_cli(&file),
            provider.clone(),
            common::selection(),
            Some(common::never()),
            &mut out,
            &mut err
        )
        .await,
        0
    );
    assert_eq!(out.len(), 0);
    assert_eq!(err.len(), 0);
    assert_eq!(provider.requests().len(), 0);
    assert!(!temp.path().join("state").exists());
}

#[tokio::test]
async fn list_conversations_while_running() {
    use crabber_coworkers::{bridge, config, store};
    let temp = tempfile::tempdir().unwrap();
    let file = common::write_state_config(temp.path(), 1);
    assert_eq!(
        run_with(
            conversation_cli(&file, "weekly"),
            ok_provider(),
            common::selection(),
            Some(common::never()),
            &mut Vec::new(),
            &mut Vec::new()
        )
        .await,
        0
    );
    let config = config::load(&file).unwrap();
    let agent = bridge::build_agent(
        config.coworker("scout").unwrap(),
        store::open(&config.state_dir, "scout").await.unwrap(),
        Arc::new(common::Pending),
        common::selection(),
    )
    .unwrap();
    let run = agent
        .prompt(Some(crabber::SessionId::from("weekly")), "pending")
        .await
        .unwrap();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    assert_eq!(
        run_with(
            list_cli(&file),
            ok_provider(),
            common::selection(),
            Some(common::never()),
            &mut out,
            &mut err
        )
        .await,
        0
    );
    assert!(String::from_utf8(out).unwrap().starts_with("weekly\t"));
    assert_eq!(err.len(), 0);
    run.control().interrupt();
    run.done().await.unwrap();
}
