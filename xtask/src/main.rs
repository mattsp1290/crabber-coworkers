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

fn workspace_root() -> &'static std::path::Path {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask is a workspace child")
}

fn cargo_command() -> Command {
    let mut command = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    command.current_dir(workspace_root());
    command
}

fn cargo(args: &[&str]) -> Result<(), String> {
    let mut command = cargo_command();
    command.args(args);
    if args.first() == Some(&"test") {
        let existing = std::env::var("NO_PROXY")
            .or_else(|_| std::env::var("no_proxy"))
            .unwrap_or_default();
        command.env("NO_PROXY", format!("{existing},127.0.0.1,localhost"));
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
    let output = cargo_command()
        .args(["metadata", "--locked", "--format-version", "1"])
        .output()
        .map_err(|_| "cargo metadata --locked")?;
    if !output.status.success() {
        return Err("cargo metadata --locked".into());
    }
    let metadata: Value = serde_json::from_slice(&output.stdout).map_err(|_| pin_error)?;
    if !verify_crabber_source(&metadata, pins::CRABBER_REV) {
        return Err(pin_error.into());
    }
    let target = metadata["target_directory"]
        .as_str()
        .ok_or("cargo metadata target_directory")?;
    let status = Command::new(
        std::path::Path::new(target)
            .join("release/crabber-coworkers")
            .with_extension(std::env::consts::EXE_EXTENSION),
    )
    .current_dir(workspace_root())
    .args(["check-config", "--config", "config.example.json"])
    .status()
    .map_err(|_| "example config check")?;
    if !status.success() {
        return Err("example config check".into());
    }
    Ok(())
}

fn verify_crabber_source(metadata: &Value, rev: &str) -> bool {
    let Some(packages) = metadata["packages"].as_array() else {
        return false;
    };
    let Some(members) = metadata["workspace_members"].as_array() else {
        return false;
    };
    let external: Vec<_> = packages
        .iter()
        .filter(|p| !members.contains(&p["id"]))
        .filter(|p| {
            p["name"]
                .as_str()
                .is_some_and(|name| name == "crabber" || name.starts_with("crabber-"))
        })
        .collect();
    let expected = format!("git+ssh://git@github.com/mattsp1290/crabber.git?rev={rev}#{rev}");
    external.iter().filter(|p| p["name"] == "crabber").count() == 1
        && external
            .iter()
            .all(|p| p["source"].as_str() == Some(expected.as_str()))
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
            &json!({"workspace_members":[], "packages":[package(pins::CRABBER_REV)]}),
            pins::CRABBER_REV
        ));
    }
    #[test]
    fn different_rev() {
        assert!(!verify_crabber_source(
            &json!({"workspace_members":[], "packages":[package("0000000000000000000000000000000000000000")]}),
            pins::CRABBER_REV
        ));
    }
    #[test]
    fn path_source() {
        assert!(!verify_crabber_source(
            &json!({"workspace_members":[], "packages":[{"name":"crabber","source":"path+file:///tmp/crabber"}]}),
            pins::CRABBER_REV
        ));
    }
    #[test]
    fn duplicate() {
        assert!(!verify_crabber_source(
            &json!({"workspace_members":[], "packages":[package(pins::CRABBER_REV), package(pins::CRABBER_REV)]}),
            pins::CRABBER_REV
        ));
    }
    #[test]
    fn missing() {
        assert!(!verify_crabber_source(
            &json!({"workspace_members":[], "packages":[]}),
            pins::CRABBER_REV
        ));
    }
    #[test]
    fn patched_transitive_crate_fails() {
        let metadata = json!({"workspace_members":[], "packages":[package(pins::CRABBER_REV), {"name":"crabber-runtime", "source":"path+file:///tmp/runtime"}]});
        assert!(!verify_crabber_source(&metadata, pins::CRABBER_REV));
    }
    #[test]
    fn local_workspace_member_is_excluded() {
        let metadata = json!({"workspace_members":["local"], "packages":[package(pins::CRABBER_REV), {"id":"local", "name":"crabber-coworkers", "source":null}]});
        assert!(verify_crabber_source(&metadata, pins::CRABBER_REV));
    }
}
