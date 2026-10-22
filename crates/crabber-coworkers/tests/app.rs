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
