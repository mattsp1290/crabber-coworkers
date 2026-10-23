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
