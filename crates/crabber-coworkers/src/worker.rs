use crate::{
    ask::{self, AskError, Cancel, Session},
    bridge,
    config::{Config, CoworkerDefinition, Limits, constants::MAX_PROMPT_BYTES},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use crabber::{Selection, providers::Resolver, session::Store};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Notify, OnceCell, watch};
use uuid::Uuid;

pub const WORKER_PORT: u16 = 8090;
pub const PROTOCOL_VERSION: u16 = 1;
const MAX_ACTIVE: usize = 32;
const MAX_TERMINAL: usize = 256;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnRequest {
    pub version: u16,
    pub coworker: CoworkerDefinition,
    pub conversation: String,
    pub prompt: String,
    pub timeout_ms: u64,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum TurnState {
    Running,
    Finished { result: Result<String, AskError> },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Health {
    pub version: u16,
    pub coworker_id: String,
    pub pid: u32,
}

#[derive(Debug, thiserror::Error)]
#[error("worker failed")]
pub struct WorkerError;

enum Entry {
    Running {
        conversation: String,
        cancel: watch::Sender<bool>,
    },
    Finished(Result<String, AskError>),
    Tombstone,
}

impl Entry {
    fn state(&self) -> Option<TurnState> {
        match self {
            Self::Running { .. } => Some(TurnState::Running),
            Self::Finished(result) => Some(TurnState::Finished {
                result: result.clone(),
            }),
            Self::Tombstone => None,
        }
    }
}

#[derive(Default)]
struct Registry {
    entries: HashMap<Uuid, Entry>,
    terminal: VecDeque<Uuid>,
    shutting_down: bool,
}

impl Registry {
    fn finish(&mut self, id: Uuid, entry: Entry) {
        self.entries.insert(id, entry);
        self.terminal.push_back(id);
        while self.terminal.len() > MAX_TERMINAL {
            if let Some(oldest) = self.terminal.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }

    fn active(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| matches!(entry, Entry::Running { .. }))
            .count()
    }
}

struct Worker {
    coworker_id: String,
    state_dir: PathBuf,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
    store: OnceCell<Arc<crabber::session::SqliteStore>>,
    registry: Mutex<Registry>,
    settled: Notify,
}

impl Worker {
    async fn store(&self) -> Result<Arc<crabber::session::SqliteStore>, AskError> {
        self.store
            .get_or_try_init(|| crate::store::open(&self.state_dir, &self.coworker_id))
            .await
            .cloned()
            .map_err(|_| AskError::Conversation)
    }

    async fn execute(
        self: Arc<Self>,
        id: Uuid,
        request: TurnRequest,
        mut cancel: watch::Receiver<bool>,
        deadline: tokio::time::Instant,
    ) {
        let result = match self.store().await {
            Ok(store) => {
                ask::ask(
                    &request.coworker,
                    &request.prompt,
                    self.resolver.clone(),
                    self.selection.clone(),
                    deadline,
                    Session::Durable {
                        store,
                        name: request.conversation,
                    },
                    Box::pin(async move {
                        let _ = cancel.wait_for(|cancelled| *cancelled).await;
                    }),
                )
                .await
            }
            Err(error) => Err(error),
        };
        self.registry
            .lock()
            .await
            .finish(id, Entry::Finished(result));
        self.settled.notify_one();
    }

    async fn shutdown(&self) {
        {
            let mut registry = self.registry.lock().await;
            registry.shutting_down = true;
            for entry in registry.entries.values() {
                if let Entry::Running { cancel, .. } = entry {
                    let _ = cancel.send(true);
                }
            }
        }
        loop {
            let notified = self.settled.notified();
            if self.registry.lock().await.active() == 0 {
                return;
            }
            notified.await;
        }
    }
}

fn worker(
    coworker_id: String,
    state_dir: PathBuf,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
) -> Arc<Worker> {
    Arc::new(Worker {
        coworker_id,
        state_dir,
        resolver,
        selection,
        store: OnceCell::new(),
        registry: Mutex::new(Registry::default()),
        settled: Notify::new(),
    })
}

fn router(worker: Arc<Worker>) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/turns/{id}", get(status).put(submit))
        .route("/v1/turns/{id}/cancel", post(cancel))
        .route("/v1/conversations", get(conversations))
        .fallback(|| async { fixed(StatusCode::NOT_FOUND) })
        .layer(DefaultBodyLimit::max(MAX_PROMPT_BYTES * 6 + 131_072))
        .with_state(worker)
}

/// Construct a worker with an injected provider and a lazily opened store.
pub fn router_with(
    coworker_id: String,
    state_dir: PathBuf,
    resolver: Arc<dyn Resolver>,
    selection: Selection,
) -> Router {
    router(worker(coworker_id, state_dir, resolver, selection))
}

/// Run a resident worker using environment credentials.
///
/// # Errors
/// Returns a fixed error for invalid identity, missing credentials, or HTTP failures.
pub async fn run(
    coworker_id: String,
    state_dir: PathBuf,
    listen: SocketAddr,
    cancel: Cancel,
) -> Result<(), WorkerError> {
    if !crate::config::valid_id(&coworker_id) {
        return Err(WorkerError);
    }
    let resolver = bridge::opencode_go_resolver_from_env().ok_or(WorkerError)?;
    let worker = worker(
        coworker_id,
        state_dir,
        resolver,
        bridge::production_selection(),
    );
    let app = router(worker.clone());
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|_| WorkerError)?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel.await;
            worker.shutdown().await;
        })
        .await
        .map_err(|_| WorkerError)
}

async fn health(State(worker): State<Arc<Worker>>) -> Json<Health> {
    Json(Health {
        version: PROTOCOL_VERSION,
        coworker_id: worker.coworker_id.clone(),
        pid: std::process::id(),
    })
}

fn request_id(id: &str) -> Result<Uuid, StatusCode> {
    let parsed = Uuid::parse_str(id).map_err(|_| StatusCode::BAD_REQUEST)?;
    if parsed.get_version_num() != 4 || parsed.to_string() != id {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(parsed)
}

fn validate(worker: &Worker, request: &TurnRequest) -> Result<tokio::time::Instant, StatusCode> {
    if request.version != PROTOCOL_VERSION
        || request.coworker.id != worker.coworker_id
        || request.coworker.slack.is_some()
        || !crate::config::valid_id(&request.conversation)
        || request.prompt.trim().is_empty()
        || request.prompt.len() > MAX_PROMPT_BYTES
        || !(1..=3_600_000).contains(&request.timeout_ms)
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Config {
        state_dir: worker.state_dir.clone(),
        coworkers: vec![request.coworker.clone()],
        limits: Limits {
            max_prompt_bytes: MAX_PROMPT_BYTES,
            model_turn_seconds: 3600,
            max_concurrent_turns: MAX_ACTIVE,
        },
        borb: None,
    }
    .validate()
    .map_err(|_| StatusCode::BAD_REQUEST)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .as_millis();
    let remaining = u128::from(request.expires_at_unix_ms).saturating_sub(now);
    Ok(tokio::time::Instant::now()
        + Duration::from_millis(
            request
                .timeout_ms
                .min(u64::try_from(remaining).unwrap_or(u64::MAX)),
        ))
}

async fn submit(
    State(worker): State<Arc<Worker>>,
    Path(id): Path<String>,
    body: Result<Json<TurnRequest>, JsonRejection>,
) -> Response {
    let Ok(id) = request_id(&id) else {
        return fixed(StatusCode::BAD_REQUEST);
    };
    let request = match body {
        Ok(Json(request)) => request,
        Err(error) => {
            return fixed(if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            });
        }
    };
    let deadline = match validate(&worker, &request) {
        Ok(deadline) => deadline,
        Err(status) => return fixed(status),
    };
    let mut registry = worker.registry.lock().await;
    if registry.entries.contains_key(&id) {
        return fixed(StatusCode::CONFLICT);
    }
    if registry.shutting_down {
        return fixed(StatusCode::SERVICE_UNAVAILABLE);
    }
    let rejected = if deadline <= tokio::time::Instant::now() {
        Some(AskError::Timeout)
    } else if registry.entries.values().any(|entry| matches!(entry, Entry::Running { conversation, .. } if conversation == &request.conversation)) {
        Some(AskError::Busy)
    } else {
        None
    };
    if let Some(error) = rejected {
        registry.finish(id, Entry::Finished(Err(error.clone())));
        return Json(TurnState::Finished { result: Err(error) }).into_response();
    }
    if registry.active() >= MAX_ACTIVE {
        return fixed(StatusCode::SERVICE_UNAVAILABLE);
    }
    let (cancel, receiver) = watch::channel(false);
    registry.entries.insert(
        id,
        Entry::Running {
            conversation: request.conversation.clone(),
            cancel,
        },
    );
    tokio::spawn(worker.clone().execute(id, request, receiver, deadline));
    Json(TurnState::Running).into_response()
}

async fn status(State(worker): State<Arc<Worker>>, Path(id): Path<String>) -> Response {
    let id = match request_id(&id) {
        Ok(id) => id,
        Err(status) => return fixed(status),
    };
    worker
        .registry
        .lock()
        .await
        .entries
        .get(&id)
        .and_then(Entry::state)
        .map_or_else(
            || fixed(StatusCode::NOT_FOUND),
            |state| Json(state).into_response(),
        )
}

async fn cancel(State(worker): State<Arc<Worker>>, Path(id): Path<String>) -> Response {
    let id = match request_id(&id) {
        Ok(id) => id,
        Err(status) => return fixed(status),
    };
    let mut registry = worker.registry.lock().await;
    if let Some(entry) = registry.entries.get(&id) {
        if let Entry::Running { cancel, .. } = entry {
            let _ = cancel.send(true);
        }
        return entry.state().map_or_else(
            || fixed(StatusCode::NOT_FOUND),
            |state| Json(state).into_response(),
        );
    }
    registry.finish(id, Entry::Tombstone);
    fixed(StatusCode::NOT_FOUND)
}

async fn conversations(State(worker): State<Arc<Worker>>) -> Response {
    let path = crate::store::store_path(&worker.state_dir, &worker.coworker_id);
    match std::fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Json(String::new()).into_response();
        }
        Err(_) => return fixed(StatusCode::INTERNAL_SERVER_ERROR),
    }
    let Ok(store) = worker.store().await else {
        return fixed(StatusCode::INTERNAL_SERVER_ERROR);
    };
    let mut output = String::new();
    let mut cursor = None;
    loop {
        let Ok(page) = store
            .list_sessions(crabber::ListSessions {
                workspace_id: worker.coworker_id.clone(),
                include_archived: false,
                limit: 100,
                cursor,
            })
            .await
        else {
            return fixed(StatusCode::INTERNAL_SERVER_ERROR);
        };
        for session in page.sessions {
            let Ok(timestamp) = session
                .updated_at
                .to_offset(time::UtcOffset::UTC)
                .format(&time::format_description::well_known::Rfc3339)
            else {
                return fixed(StatusCode::INTERNAL_SERVER_ERROR);
            };
            if writeln!(output, "{}\t{timestamp}", session.session_id).is_err() {
                return fixed(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            return Json(output).into_response();
        }
    }
}

fn fixed(status: StatusCode) -> Response {
    (status, "worker request failed").into_response()
}
