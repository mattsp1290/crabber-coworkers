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
