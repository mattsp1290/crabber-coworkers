use crabber_coworkers::config::{self, Config};
use std::path::{Path, PathBuf};

pub fn path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.json")
}

pub fn config() -> Config {
    let mut config = config::load(&path()).unwrap();
    for def in &mut config.coworkers {
        def.workspace = config::WorkspaceAccess::Disabled;
    }
    config
}
