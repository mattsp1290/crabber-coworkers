use crate::{
    ask::{AskError, Cancel, Session, ask, validate_request},
    bridge, config,
    slack::{CredentialSource, api::DEFAULT_BASE},
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
    /// Run the agent worker inside a borb. Do not expose this listener publicly.
    Worker {
        #[arg(long)]
        coworker_id: String,
        #[arg(long, default_value = "/home/borb/workspace/state")]
        state_dir: PathBuf,
        #[arg(long, default_value = "0.0.0.0:8090")]
        listen: std::net::SocketAddr,
    },
    /// Answer Slack mentions for every coworker that has a slack block, until a signal arrives.
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    /// Validate configuration and list coworker IDs and names.
    CheckConfig {
        #[arg(long)]
        config: PathBuf,
    },
    /// List a coworker's durable conversation names and update times.
    ListConversations {
        #[arg(long)]
        config: PathBuf,
        coworker_id: String,
    },
    /// Ask a configured coworker, optionally continuing a conversation.
    Ask {
        #[arg(long)]
        config: PathBuf,
        coworker_id: String,
        prompt: String,
        #[arg(long)]
        conversation: Option<String>,
    },
}

pub async fn run(cli: Cli) -> i32 {
    execute(
        cli,
        Provider::FromEnv,
        bridge::production_selection(),
        None,
        &mut io::stdout(),
        &mut io::stderr(),
        CredentialSource::FromEnv,
        DEFAULT_BASE,
    )
    .await
}

pub async fn run_with(
    cli: Cli,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
    cancel: Option<Cancel>,
    out: &mut (dyn Write + Send),
    err: &mut (dyn Write + Send),
) -> i32 {
    execute(
        cli,
        Provider::Injected(resolver),
        selection,
        cancel,
        out,
        err,
        CredentialSource::FromEnv,
        DEFAULT_BASE,
    )
    .await
}

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error(transparent)]
    Ask(#[from] crate::ask::AskError),
    #[error("OPENCODE_GO_API_KEY is not set")]
    MissingApiKey,
    #[error(transparent)]
    Store(#[from] crate::store::StoreOpenError),
    #[error("cannot list conversations")]
    List,
    #[error("cannot install signal handlers")]
    Signal,
    #[error("cannot write output")]
    Output,
    #[error("cannot configure borb execution; check BORB_TOKEN and borb settings")]
    Borb,
    #[error("borb execution requires --conversation")]
    RemoteConversation,
}

impl AppError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Config(_) | Self::MissingApiKey | Self::Borb | Self::RemoteConversation => 2,
            Self::Ask(error) => error.exit_code(),
            Self::Output | Self::Store(_) | Self::List | Self::Signal => 1,
        }
    }
}

pub(crate) enum Provider {
    FromEnv,
    Injected(Arc<dyn Resolver>),
    Remote(crate::execution::Remote),
}

impl Provider {
    pub(crate) fn resolve(self) -> Option<Arc<dyn Resolver>> {
        match self {
            Self::FromEnv => bridge::opencode_go_resolver_from_env(),
            Self::Injected(resolver) => Some(resolver),
            Self::Remote(_) => None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    cli: Cli,
    provider: Provider,
    selection: Selection,
    cancel: Option<Cancel>,
    out: &mut (dyn Write + Send),
    err: &mut (dyn Write + Send),
    credentials: CredentialSource,
    slack_base: &str,
) -> i32 {
    if let Command::Worker {
        coworker_id,
        state_dir,
        listen,
    } = cli.command
    {
        let cancel = if let Some(cancel) = cancel {
            cancel
        } else {
            let Ok(cancel) = install_cancel() else {
                let _ = writeln!(err, "cannot install signal handlers");
                return 1;
            };
            cancel
        };
        return if crate::worker::run(coworker_id, state_dir, listen, cancel)
            .await
            .is_ok()
        {
            0
        } else {
            let _ = writeln!(err, "agent worker failed");
            1
        };
    }
    if let Command::Serve { config } = cli.command {
        return match crate::serve::run(
            config,
            provider,
            credentials,
            selection,
            cancel,
            slack_base,
            err,
        )
        .await
        {
            Ok(()) => 0,
            Err(error) => {
                let _ = writeln!(err, "{error}");
                error.exit_code()
            }
        };
    }
    match respond(cli, provider, selection, cancel)
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

async fn respond(
    cli: Cli,
    provider: Provider,
    selection: Selection,
    cancel: Option<Cancel>,
) -> Result<String, AppError> {
    match cli.command {
        Command::Serve { .. } | Command::Worker { .. } => {
            unreachable!("service commands are dispatched before respond")
        }
        Command::CheckConfig { config } => Ok(listing(&config::load(&config)?)),
        Command::ListConversations {
            config,
            coworker_id,
        } => {
            let config = config::load(&config)?;
            let def = config
                .coworker(&coworker_id)
                .ok_or(AskError::UnknownCoworker)?;
            if let Some(remote) = remote_for(&config, &provider)? {
                remote.list(def).await.map_err(AppError::Ask)
            } else {
                list_conversations(&config.state_dir, &def.id).await
            }
        }
        Command::Ask {
            config,
            coworker_id,
            prompt,
            conversation,
        } => {
            let config = config::load(&config)?;
            let def = validate_request(&config, &coworker_id, &prompt, conversation.as_deref())?;
            let remote = remote_for(&config, &provider)?;
            let mut cancel = match cancel {
                Some(cancel) => cancel,
                None => install_cancel().map_err(|_| AppError::Signal)?,
            };
            let deadline =
                tokio::time::Instant::now() + Duration::from_secs(config.limits.model_turn_seconds);
            if let Some(remote) = remote {
                let name = conversation.ok_or(AppError::RemoteConversation)?;
                let answer = remote.answer(def, &name, &prompt, deadline, cancel).await?;
                return Ok(format!("{answer}\n"));
            }
            let resolver = provider.resolve().ok_or(AppError::MissingApiKey)?;
            let workspace =
                crate::workspace::admit(def, &config.state_dir, deadline, &mut cancel).await?;
            let session = if let Some(name) = conversation {
                let store = tokio::select! {
                    biased;
                    () = &mut cancel => return Err(AskError::Interrupted.into()),
                    () = tokio::time::sleep_until(deadline) => return Err(AskError::Timeout.into()),
                    store = crate::store::open(&config.state_dir, &def.id) => store?,
                };
                Session::Durable { store, name }
            } else {
                Session::Ephemeral
            };
            let answer = ask(
                def, &workspace, &prompt, resolver, selection, deadline, session, cancel,
            )
            .await?;
            Ok(format!("{answer}\n"))
        }
    }
}

fn remote_for(
    config: &config::Config,
    provider: &Provider,
) -> Result<Option<crate::execution::Remote>, AppError> {
    match (&config.borb, provider) {
        (_, Provider::Remote(remote)) => Ok(Some(remote.clone())),
        (Some(settings), Provider::FromEnv) => crate::execution::Remote::new(
            settings,
            std::env::var("BORB_TOKEN").map_err(|_| AppError::Borb)?,
        )
        .map(Some)
        .map_err(|_| AppError::Borb),
        _ => Ok(None),
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

/// Register signal handlers synchronously before any durable state is opened.
///
/// # Errors
/// Returns an IO error if the runtime cannot register a signal handler.
pub fn install_cancel() -> io::Result<Cancel> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut terminate = signal(SignalKind::terminate())?;
        let mut hangup = signal(SignalKind::hangup())?;
        Ok(Box::pin(async move {
            tokio::select! {
                _ = interrupt.recv() => {},
                _ = terminate.recv() => {},
                _ = hangup.recv() => {},
            }
        }))
    }
    #[cfg(not(unix))]
    {
        Ok(Box::pin(async {
            let _ = tokio::signal::ctrl_c().await;
        }))
    }
}

async fn list_conversations(state_dir: &std::path::Path, id: &str) -> Result<String, AppError> {
    use crabber::session::Store;
    use std::fmt::Write;
    let Some(store) = crate::store::connect_existing(state_dir, id).await? else {
        return Ok(String::new());
    };
    let mut cursor = None;
    let mut output = String::new();
    loop {
        let page = store
            .list_sessions(crabber::ListSessions {
                workspace_id: id.into(),
                include_archived: false,
                limit: 100,
                cursor,
            })
            .await
            .map_err(|_| AppError::List)?;
        for session in page.sessions {
            let timestamp = session
                .updated_at
                .to_offset(time::UtcOffset::UTC)
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|_| AppError::List)?;
            writeln!(output, "{}\t{timestamp}", session.session_id).map_err(|_| AppError::List)?;
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    Ok(output)
}

/// Run the production serve dispatch with injected providers, tokens, base URL, and cancellation.
pub async fn serve_with(
    config: PathBuf,
    resolver: Arc<dyn Resolver>,
    credentials: CredentialSource,
    selection: Selection,
    cancel: Cancel,
    slack_base: &str,
    err: &mut (dyn Write + Send),
) -> i32 {
    execute(
        Cli {
            command: Command::Serve { config },
        },
        Provider::Injected(resolver),
        selection,
        Some(cancel),
        &mut Vec::new(),
        err,
        credentials,
        slack_base,
    )
    .await
}

/// Run Slack with a remote executor and injected Slack credentials and endpoint.
pub async fn serve_remote_with(
    config: PathBuf,
    remote: crate::execution::Remote,
    credentials: CredentialSource,
    cancel: Cancel,
    slack_base: &str,
    err: &mut (dyn Write + Send),
) -> i32 {
    execute(
        Cli {
            command: Command::Serve { config },
        },
        Provider::Remote(remote),
        bridge::production_selection(),
        Some(cancel),
        &mut Vec::new(),
        err,
        credentials,
        slack_base,
    )
    .await
}
