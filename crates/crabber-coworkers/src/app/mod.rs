use crate::{
    api::{auth::ApiTokenSource, store::Faults},
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
    /// Serve Slack mentions and the HTTP API until a signal arrives.
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
    /// Print the HTTP API contract document.
    #[command(hide = true)]
    Openapi,
}

pub async fn run(cli: Cli) -> i32 {
    execute(
        cli,
        ServeOptions::from_env(),
        &mut io::stdout(),
        &mut io::stderr(),
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
        ServeOptions {
            provider: Provider::Injected(resolver),
            selection,
            cancel,
            ..ServeOptions::from_env()
        },
        out,
        err,
    )
    .await
}

/// Everything a command takes from outside its configuration file. [`ServeOptions::from_env`]
/// is what the binary uses; an embedding test replaces fields to inject its own.
pub struct ServeOptions {
    pub provider: Provider,
    pub selection: Selection,
    /// `None` installs the process's signal handlers.
    pub cancel: Option<Cancel>,
    /// Renews `serve` when it resolves: intake closes, running turns finish, and `serve` exits
    /// without starting another request. `None` installs the `USR1` handler.
    pub renewal: Option<Cancel>,
    pub credentials: CredentialSource,
    pub api_token: ApiTokenSource,
    /// Serve the HTTP API on this listener instead of binding `api.listen`.
    pub api_listener: Option<tokio::net::TcpListener>,
    pub slack_base: String,
    #[doc(hidden)]
    pub api_faults: Option<Arc<Faults>>,
}

impl ServeOptions {
    /// Credentials and tokens from the environment, the production model, signal handlers, and
    /// Slack's public API.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            provider: Provider::FromEnv,
            selection: bridge::production_selection(),
            cancel: None,
            renewal: None,
            credentials: CredentialSource::FromEnv,
            api_token: ApiTokenSource::FromEnv,
            api_listener: None,
            slack_base: DEFAULT_BASE.into(),
            api_faults: None,
        }
    }
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

/// Where turns get their model, or the borb worker that runs them.
pub enum Provider {
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

/// Boxed so that a caller's future need not prove `Send` through every type a command holds,
/// which exceeds the default recursion limit of the crates that spawn it.
fn execute<'a>(
    cli: Cli,
    options: ServeOptions,
    out: &'a mut (dyn Write + Send),
    err: &'a mut (dyn Write + Send),
) -> std::pin::Pin<Box<dyn Future<Output = i32> + Send + 'a>> {
    Box::pin(command(cli, options, out, err))
}

async fn command(
    cli: Cli,
    options: ServeOptions,
    out: &mut (dyn Write + Send),
    err: &mut (dyn Write + Send),
) -> i32 {
    if let Command::Worker {
        coworker_id,
        state_dir,
        listen,
    } = cli.command
    {
        let cancel = if let Some(cancel) = options.cancel {
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
        return match crate::serve::run(config, options, err).await {
            Ok(()) => 0,
            Err(error) => {
                let _ = writeln!(err, "{error}");
                error.exit_code()
            }
        };
    }
    if matches!(cli.command, Command::Openapi) {
        return match emit(out, &crate::api::openapi::document_json()) {
            Ok(()) => 0,
            Err(error) => {
                let _ = writeln!(err, "{error}");
                error.exit_code()
            }
        };
    }
    match respond(cli, options.provider, options.selection, options.cancel)
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
        Command::Serve { .. } | Command::Worker { .. } | Command::Openapi => {
            unreachable!("service and contract commands are dispatched before respond")
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

/// Run the production `serve` dispatch with injected inputs. Returns the process exit code.
pub async fn serve_with_options(
    config: PathBuf,
    options: ServeOptions,
    err: &mut (dyn Write + Send),
) -> i32 {
    execute(
        Cli {
            command: Command::Serve { config },
        },
        options,
        &mut Vec::new(),
        err,
    )
    .await
}

/// [`serve_with_options`] with an injected model, Slack credentials, endpoint, and cancellation.
pub async fn serve_with(
    config: PathBuf,
    resolver: Arc<dyn Resolver>,
    credentials: CredentialSource,
    selection: Selection,
    cancel: Cancel,
    slack_base: &str,
    err: &mut (dyn Write + Send),
) -> i32 {
    let options = ServeOptions {
        provider: Provider::Injected(resolver),
        selection,
        cancel: Some(cancel),
        credentials,
        slack_base: slack_base.into(),
        ..ServeOptions::from_env()
    };
    serve_with_options(config, options, err).await
}

/// [`serve_with_options`] with a remote executor and injected Slack credentials and endpoint.
pub async fn serve_remote_with(
    config: PathBuf,
    remote: crate::execution::Remote,
    credentials: CredentialSource,
    cancel: Cancel,
    slack_base: &str,
    err: &mut (dyn Write + Send),
) -> i32 {
    let options = ServeOptions {
        provider: Provider::Remote(remote),
        cancel: Some(cancel),
        credentials,
        slack_base: slack_base.into(),
        ..ServeOptions::from_env()
    };
    serve_with_options(config, options, err).await
}
