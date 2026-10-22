use crate::{
    ask::{ask, validate_request},
    bridge, config,
};
use clap::{Parser, Subcommand};
use crabber::{Selection, providers::Resolver};
use std::{
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

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
    /// Ask one configured coworker a single question.
    Ask {
        #[arg(long)]
        config: PathBuf,
        coworker_id: String,
        prompt: String,
    },
}

pub async fn run(cli: Cli) -> i32 {
    execute(
        cli,
        bridge::opencode_go_resolver_from_env(),
        bridge::production_selection(),
        true,
        &mut io::stdout(),
        &mut io::stderr(),
    )
    .await
}

pub async fn run_with(
    cli: Cli,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
    out: &mut (dyn Write + Send),
    err: &mut (dyn Write + Send),
) -> i32 {
    execute(cli, resolver, selection, false, out, err).await
}

fn fail(err: &mut (dyn Write + Send), message: &str, code: i32) -> i32 {
    let _ = writeln!(err, "{message}");
    code
}

async fn execute(
    cli: Cli,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
    require_key: bool,
    out: &mut (dyn Write + Send),
    err: &mut (dyn Write + Send),
) -> i32 {
    let path = match &cli.command {
        Command::CheckConfig { config } | Command::Ask { config, .. } => config,
    };
    let config = match config::load(path) {
        Ok(config) => config,
        Err(error) => return fail(err, &error.to_string(), 2),
    };
    let output = match cli.command {
        Command::CheckConfig { .. } => {
            let mut listing = String::new();
            for def in &config.coworkers {
                listing.push_str(&def.id);
                listing.push('\t');
                listing.push_str(&def.name);
                listing.push('\n');
            }
            listing
        }
        Command::Ask {
            coworker_id,
            prompt,
            ..
        } => {
            let def = match validate_request(&config, &coworker_id, &prompt) {
                Ok(def) => def,
                Err(error) => return fail(err, &error.to_string(), error.exit_code()),
            };
            if require_key
                && std::env::var("OPENCODE_GO_API_KEY").map_or(true, |key| key.is_empty())
            {
                return fail(err, "OPENCODE_GO_API_KEY is not set", 2);
            }
            match ask(
                def,
                &prompt,
                resolver,
                selection,
                Duration::from_secs(config.limits.model_turn_seconds),
            )
            .await
            {
                Ok(answer) => format!("{answer}\n"),
                Err(error) => return fail(err, &error.to_string(), error.exit_code()),
            }
        }
    };
    if out
        .write_all(output.as_bytes())
        .and_then(|()| out.flush())
        .is_err()
    {
        return fail(err, "cannot write output", 1);
    }
    0
}
