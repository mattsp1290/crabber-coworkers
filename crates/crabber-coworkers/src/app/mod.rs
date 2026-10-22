use crate::config;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Validate configuration and list coworker IDs and names.
    CheckConfig {
        #[arg(long)]
        config: PathBuf,
    },
}

#[must_use]
pub fn run(cli: &Cli) -> i32 {
    match &cli.command {
        Command::CheckConfig { config } => match config::load(config) {
            Ok(config) => {
                for def in config.coworkers {
                    println!("{}\t{}", def.id, def.name);
                }
                0
            }
            Err(error) => {
                eprintln!("{error}");
                2
            }
        },
    }
}
