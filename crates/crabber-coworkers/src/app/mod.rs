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
        Provider::FromEnv,
        bridge::production_selection(),
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
    execute(cli, Provider::Injected(resolver), selection, out, err).await
}

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error(transparent)]
    Ask(#[from] crate::ask::AskError),
    #[error("OPENCODE_GO_API_KEY is not set")]
    MissingApiKey,
    #[error("cannot write output")]
    Output,
}

impl AppError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Config(_) | Self::MissingApiKey => 2,
            Self::Ask(error) => error.exit_code(),
            Self::Output => 1,
        }
    }
}

enum Provider {
    FromEnv,
    Injected(Arc<dyn Resolver>),
}

impl Provider {
    fn resolve(self) -> Option<Arc<dyn Resolver>> {
        match self {
            Self::FromEnv => bridge::opencode_go_resolver_from_env(),
            Self::Injected(resolver) => Some(resolver),
        }
    }
}

async fn execute(
    cli: Cli,
    provider: Provider,
    selection: Selection,
    out: &mut (dyn Write + Send),
    err: &mut (dyn Write + Send),
) -> i32 {
    match respond(cli, provider, selection)
        .await
        .and_then(|output| emit(out, &output))
    {
        Ok(()) => 0,
        Err(error) => {
            let _ = writeln!(err, "{error}");
            error.exit_code()
        }
    }
}

async fn respond(cli: Cli, provider: Provider, selection: Selection) -> Result<String, AppError> {
    match cli.command {
        Command::CheckConfig { config } => Ok(listing(&config::load(&config)?)),
        Command::Ask {
            config,
            coworker_id,
            prompt,
        } => {
            let config = config::load(&config)?;
            let def = validate_request(&config, &coworker_id, &prompt)?;
            let resolver = provider.resolve().ok_or(AppError::MissingApiKey)?;
            let timeout = Duration::from_secs(config.limits.model_turn_seconds);
            let answer = ask(def, &prompt, resolver, selection, timeout).await?;
            Ok(format!("{answer}\n"))
        }
    }
}

fn listing(config: &config::Config) -> String {
    let mut listing = String::new();
    for def in &config.coworkers {
        listing.push_str(&def.id);
        listing.push('\t');
        listing.push_str(&def.name);
        listing.push('\n');
    }
    listing
}

fn emit(out: &mut (dyn Write + Send), output: &str) -> Result<(), AppError> {
    out.write_all(output.as_bytes())
        .and_then(|()| out.flush())
        .map_err(|_| AppError::Output)
}
