mod pins;
use serde_json::Value;
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    if std::env::args().skip(1).collect::<Vec<_>>() != ["check"] {
        eprintln!("usage: cargo xtask check");
        return ExitCode::from(2);
    }
    match check() {
        Ok(()) => ExitCode::SUCCESS,
        Err(step) => {
            eprintln!("{step}");
            ExitCode::FAILURE
        }
    }
}

fn cargo(args: &[&str]) -> Result<(), String> {
    let mut command = Command::new("cargo");
    command.args(args);
    if args.first() == Some(&"test") {
        command.env("NO_PROXY", "127.0.0.1,localhost");
    }
    match command.status() {
        Ok(status) if status.success() => Ok(()),
        _ => Err(format!("cargo {}", args.join(" "))),
    }
}

fn check() -> Result<(), String> {
    cargo(&["fmt", "--all", "--check"])?;
    cargo(&[
        "clippy",
        "--workspace",
        "--all-targets",
        "--locked",
        "--",
        "-D",
        "warnings",
    ])?;
    cargo(&["test", "--workspace", "--locked"])?;
    cargo(&["build", "-p", "crabber-coworkers", "--release", "--locked"])?;
    let pin_error = "crabber dependency pin differs from xtask/src/pins.rs";
    let output = Command::new("cargo")
        .args(["metadata", "--locked", "--format-version", "1"])
        .output()
        .map_err(|_| pin_error)?;
    if !output.status.success() {
        return Err(pin_error.into());
    }
    let metadata: Value = serde_json::from_slice(&output.stdout).map_err(|_| pin_error)?;
    if !verify_crabber_source(&metadata, pins::CRABBER_REV) {
        return Err(pin_error.into());
    }
    Ok(())
}

fn verify_crabber_source(metadata: &Value, rev: &str) -> bool {
    let Some(packages) = metadata["packages"].as_array() else {
        return false;
    };
    let crabber: Vec<_> = packages.iter().filter(|p| p["name"] == "crabber").collect();
    let expected = format!("git+ssh://git@github.com/mattsp1290/crabber.git?rev={rev}#{rev}");
    crabber.len() == 1 && crabber[0]["source"].as_str() == Some(expected.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn package(rev: &str) -> Value {
        json!({"name":"crabber", "source":format!("git+ssh://git@github.com/mattsp1290/crabber.git?rev={rev}#{rev}")})
    }
    #[test]
    fn matching() {
        assert!(verify_crabber_source(
            &json!({"packages":[package(pins::CRABBER_REV)]}),
            pins::CRABBER_REV
        ));
    }
    #[test]
    fn different_rev() {
        assert!(!verify_crabber_source(
            &json!({"packages":[package("0000000000000000000000000000000000000000")]}),
            pins::CRABBER_REV
        ));
    }
    #[test]
    fn path_source() {
        assert!(!verify_crabber_source(
            &json!({"packages":[{"name":"crabber","source":"path+file:///tmp/crabber"}]}),
            pins::CRABBER_REV
        ));
    }
    #[test]
    fn duplicate() {
        assert!(!verify_crabber_source(
            &json!({"packages":[package(pins::CRABBER_REV), package(pins::CRABBER_REV)]}),
            pins::CRABBER_REV
        ));
    }
    #[test]
    fn missing() {
        assert!(!verify_crabber_source(
            &json!({"packages":[]}),
            pins::CRABBER_REV
        ));
    }
}
