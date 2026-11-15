//! The gateway's durable record of API conversations and requests (`<state_dir>/api.sqlite`).
//!
//! A request's status is a [`RequestState`] that changes only through the transition table in
//! [`RequestState::after`], applied inside one `BEGIN IMMEDIATE` transaction per command.
use super::contract::{
    Acceptance, ApiUuid, ApiVersion, Cancellation, ConversationPhase, ConversationState,
    CoworkerId, FailureReason, RequestReceipt, RequestResult, RequestStatus,
};
use crate::{
    digest::sha256_hex,
    store::{API_LOCK_FILE, API_STORE_FILE, create_private_file},
};
use sqlx::{
    ConnectOptions, Connection, Row, SqliteConnection, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow, SqliteSynchronous},
};
use std::{
    collections::HashMap,
    fmt,
    fs::{File, TryLockError},
    future::Future,
    path::Path,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

const SCHEMA_VERSION: i64 = 1;
const MAX_CONNECTIONS: u32 = 4;
const OPERATION_BOUND: Duration = Duration::from_secs(5);
const PING_BOUND: Duration = Duration::from_secs(2);

const SCHEMA: &str = "
CREATE TABLE conversations (
    conversation_id TEXT NOT NULL PRIMARY KEY,
    coworker_id     TEXT NOT NULL,
    created_at      TEXT NOT NULL
) STRICT;
CREATE TABLE requests (
    seq                 INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id     TEXT NOT NULL REFERENCES conversations (conversation_id),
    request_id          TEXT NOT NULL,
    text                TEXT NOT NULL,
    text_sha256         TEXT NOT NULL,
    status              TEXT NOT NULL,
    reason              TEXT,
    result_text         TEXT,
    cancel_requested_at TEXT,
    retirement_reason   TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    UNIQUE (conversation_id, request_id)
) STRICT;
CREATE INDEX requests_active ON requests (conversation_id, status, seq);
PRAGMA user_version = 1;
";

/// Requests that still hold their conversation's turn order.
const ACTIVE: [RequestStatus; 3] = [
    RequestStatus::Accepted,
    RequestStatus::Running,
    RequestStatus::CancellationRequested,
];

/// Binds the [`ACTIVE`] statuses as the query's next three parameters.
macro_rules! bind_active {
    ($query:expr) => {{
        let [accepted, running, cancellation_requested] = ACTIVE;
        $query
            .bind(accepted.as_str())
            .bind(running.as_str())
            .bind(cancellation_requested.as_str())
    }};
}

macro_rules! select_requests {
    ($tail:literal) => {
        concat!(
            "SELECT seq, conversation_id, request_id, text, text_sha256, status, reason, ",
            "result_text, cancel_requested_at, retirement_reason FROM requests ",
            $tail
        )
    };
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("API store is in use")]
    InUse,
    #[error("cannot open API store")]
    Fault,
}

/// A database failure or timeout. It carries no detail, so nothing raw can reach a log or a body.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("API store unavailable")]
pub struct StoreFault;

impl From<sqlx::Error> for StoreFault {
    fn from(_: sqlx::Error) -> Self {
        Self
    }
}

/// Global FIFO position of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(i64);

/// How a request ended.
#[derive(Clone, PartialEq, Eq)]
pub enum Outcome {
    Completed(String),
    Cancelled,
    Failed(FailureReason),
    Uncertain(FailureReason),
}

impl fmt::Debug for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed(text) => write!(f, "Completed(<{} bytes>)", text.len()),
            Self::Cancelled => f.write_str("Cancelled"),
            Self::Failed(reason) => f.debug_tuple("Failed").field(reason).finish(),
            Self::Uncertain(reason) => f.debug_tuple("Uncertain").field(reason).finish(),
        }
    }
}

/// A request's durable state. `result` and `reason` exist only inside the outcome that owns them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestState {
    Accepted,
    Running,
    CancellationRequested,
    Settled(Outcome),
}

enum Event {
    Start,
    Cancel(String),
    Finish(Outcome),
    /// The request's turn was interrupted. What that settles as depends on the row.
    Interrupt,
    Requeue,
}

impl RequestState {
    #[must_use]
    pub fn status(&self) -> RequestStatus {
        match self {
            Self::Accepted => RequestStatus::Accepted,
            Self::Running => RequestStatus::Running,
            Self::CancellationRequested => RequestStatus::CancellationRequested,
            Self::Settled(Outcome::Completed(_)) => RequestStatus::Completed,
            Self::Settled(Outcome::Cancelled) => RequestStatus::Cancelled,
            Self::Settled(Outcome::Failed(_)) => RequestStatus::Failed,
            Self::Settled(Outcome::Uncertain(_)) => RequestStatus::Uncertain,
        }
    }

    /// The state an event leads to and, for a cancellation, the time to record; `None` when the
    /// event does not apply. A settled request never changes. `Cancelled` settles only a request
    /// whose cancellation was requested, so a cancelled receipt always carries its request time.
    /// An `accepted` request never ran, so `Failed` is the only outcome that can settle it.
    ///
    /// An interruption settles by what the row holds when it is applied: `uncertain` with the
    /// `retirement` reason when reconciliation interrupted the run itself, else `cancelled` when
    /// the client asked for that, else `failed` with `interrupted`.
    fn after(
        &self,
        event: Event,
        retirement: Option<FailureReason>,
    ) -> Option<(Self, Option<String>)> {
        let interrupted = |otherwise| {
            let outcome = retirement.map_or(otherwise, Outcome::Uncertain);
            Some((Self::Settled(outcome), None))
        };
        match (self, event) {
            (Self::Accepted, Event::Start) => Some((Self::Running, None)),
            (Self::Accepted, Event::Cancel(at)) => {
                Some((Self::Settled(Outcome::Cancelled), Some(at)))
            }
            (Self::Accepted, Event::Finish(outcome @ Outcome::Failed(_))) => {
                Some((Self::Settled(outcome), None))
            }
            (Self::Running, Event::Cancel(at)) => Some((Self::CancellationRequested, Some(at))),
            (Self::Running, Event::Requeue) => Some((Self::Accepted, None)),
            (Self::Running, Event::Interrupt) => {
                interrupted(Outcome::Failed(FailureReason::Interrupted))
            }
            (Self::CancellationRequested, Event::Interrupt) => interrupted(Outcome::Cancelled),
            (Self::Accepted, Event::Finish(_) | Event::Interrupt | Event::Requeue)
            | (Self::Running, Event::Start | Event::Finish(Outcome::Cancelled))
            | (Self::CancellationRequested, Event::Start | Event::Cancel(_) | Event::Requeue)
            | (Self::Settled(_), _) => None,
            (Self::Running | Self::CancellationRequested, Event::Finish(outcome)) => {
                Some((Self::Settled(outcome), None))
            }
        }
    }

    fn from_columns(
        status: RequestStatus,
        reason: Option<FailureReason>,
        result: Option<String>,
    ) -> Option<Self> {
        Some(match (status, reason, result) {
            (RequestStatus::Accepted, None, None) => Self::Accepted,
            (RequestStatus::Running, None, None) => Self::Running,
            (RequestStatus::CancellationRequested, None, None) => Self::CancellationRequested,
            (RequestStatus::Completed, None, Some(text)) => Self::Settled(Outcome::Completed(text)),
            (RequestStatus::Cancelled, None, None) => Self::Settled(Outcome::Cancelled),
            (RequestStatus::Failed, Some(reason), None) => Self::Settled(Outcome::Failed(reason)),
            (RequestStatus::Uncertain, Some(reason), None) => {
                Self::Settled(Outcome::Uncertain(reason))
            }
            _ => return None,
        })
    }

    fn reason(&self) -> Option<FailureReason> {
        match self {
            Self::Settled(Outcome::Failed(reason) | Outcome::Uncertain(reason)) => Some(*reason),
            _ => None,
        }
    }

    fn result(&self) -> Option<&str> {
        match self {
            Self::Settled(Outcome::Completed(text)) => Some(text),
            _ => None,
        }
    }

    fn admits_cancellation(&self, recorded: bool) -> bool {
        match self {
            Self::Accepted | Self::Running => !recorded,
            Self::CancellationRequested | Self::Settled(Outcome::Cancelled) => recorded,
            Self::Settled(Outcome::Completed(_) | Outcome::Failed(_) | Outcome::Uncertain(_)) => {
                true
            }
        }
    }
}

/// One stored request. Only the store constructs it, so every row satisfies the state invariants.
#[derive(Clone, PartialEq, Eq)]
pub struct RequestRow {
    seq: Seq,
    conversation_id: ApiUuid,
    request_id: ApiUuid,
    text: String,
    state: RequestState,
    cancel_requested_at: Option<String>,
    retirement: Option<FailureReason>,
}

impl fmt::Debug for RequestRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestRow")
            .field("seq", &self.seq)
            .field("conversation_id", &self.conversation_id)
            .field("request_id", &self.request_id)
            .field("text", &format_args!("<{} bytes>", self.text.len()))
            .field("state", &self.state)
            .field("cancel_requested_at", &self.cancel_requested_at)
            .field("retirement", &self.retirement)
            .finish()
    }
}

impl RequestRow {
    #[must_use]
    pub fn seq(&self) -> Seq {
        self.seq
    }

    #[must_use]
    pub fn conversation_id(&self) -> ApiUuid {
        self.conversation_id
    }

    #[must_use]
    pub fn request_id(&self) -> ApiUuid {
        self.request_id
    }

    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    #[must_use]
    pub fn state(&self) -> &RequestState {
        &self.state
    }

    /// RFC 3339 UTC time cancellation was first requested.
    #[must_use]
    pub fn cancel_requested_at(&self) -> Option<&str> {
        self.cancel_requested_at.as_deref()
    }

    /// Why an interrupted run settles this request `uncertain`: reconciliation recorded the
    /// reason before it made crabber interrupt the request's orphaned run.
    #[must_use]
    pub fn retirement(&self) -> Option<FailureReason> {
        self.retirement
    }

    #[must_use]
    pub fn into_receipt(self) -> RequestReceipt {
        RequestReceipt {
            api_version: ApiVersion::V1,
            conversation_id: self.conversation_id,
            request_id: self.request_id,
            acceptance: Acceptance::default(),
            status: self.state.status(),
            reason: self.state.reason(),
            result: self.state.result().map(|text| RequestResult {
                text: text.to_owned(),
            }),
            cancellation: self
                .cancel_requested_at
                .map(|requested_at| Cancellation { requested_at }),
        }
    }

    fn after(&self, event: Event) -> Option<Self> {
        let (state, requested_at) = self.state.after(event, self.retirement)?;
        // A request back in the queue was never admitted, so no run of it was ever retired.
        let retirement = self.retirement.filter(|_| state != RequestState::Accepted);
        Some(Self {
            state,
            cancel_requested_at: self.cancel_requested_at.clone().or(requested_at),
            retirement,
            ..self.clone()
        })
    }
}

#[derive(Debug)]
pub enum Opened {
    Created,
    Existing,
    /// The ID is already open for another coworker.
    Conflict,
}

#[derive(Debug)]
pub enum Submitted {
    Inserted(RequestRow),
    /// The same identity with byte-identical text; nothing was written.
    Existing(RequestRow),
    TextConflict,
    QueueFull,
    NoConversation,
}

/// What [`ApiStore::mark_running`] found. Each variant carries the row as it now stands.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkedRunning {
    /// This call moved the request from `accepted` to `running`.
    Started(RequestRow),
    /// An earlier call already started it, for example one whose commit landed after its time
    /// bound fired. The row is `running`, or `cancellation_requested` if a cancel followed.
    AlreadyStarted(RequestRow),
    /// A cancellation or a direct failure settled the request before it started.
    Settled(RequestRow),
}

/// What [`ApiStore::finish`] did. Each variant carries the row as it now stands.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finished {
    /// This call settled the request with the given outcome.
    Settled(RequestRow),
    /// The outcome did not apply and nothing was written: the request was already settled, was
    /// `accepted` and the outcome was not `Failed`, or was `running` and the outcome was
    /// `Cancelled` without a cancellation request. The row may still be active.
    Unchanged(RequestRow),
}

/// What [`ApiStore::begin_retirement`] found. Each variant carries the row as it now stands.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retiring {
    /// The request is `running` or `cancellation_requested` and carries this call's reason.
    Recorded(RequestRow),
    /// Nothing was written: the request is `accepted` or settled.
    Kept(RequestRow),
}

/// What [`ApiStore::requeue`] did. Each variant carries the row as it now stands.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Requeued {
    /// This call moved the request from `running` back to `accepted`.
    Requeued(RequestRow),
    /// An earlier call already requeued it, for example one whose commit landed after its time
    /// bound fired.
    AlreadyAccepted(RequestRow),
    /// Nothing was written: a cancellation was requested, or the request is settled.
    Kept(RequestRow),
}

/// One page of conversations in ascending ID order.
#[derive(Debug)]
pub struct ConversationRows {
    pub conversations: Vec<ConversationState>,
    /// The last ID on this page when another page follows.
    pub next: Option<ApiUuid>,
}

/// A conversation's newest requests, oldest first.
#[derive(Debug)]
pub struct SnapshotRows {
    pub rows: Vec<RequestRow>,
    /// Whether older requests exist beyond `rows`.
    pub truncated: bool,
}

/// An operation a test can make fail or slow down.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FaultPoint {
    MarkRunning,
    /// Every settlement write: [`ApiStore::finish`] and [`ApiStore::interrupt`].
    Finish,
    BeginRetirement,
    Ping,
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    Fail,
    Delay(Duration),
}

/// Test seam: fails or delays the next calls of an operation. `Fail` returns before touching the
/// database; `Delay` sleeps after the operation's database work, inside its time bound.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct Faults(Mutex<HashMap<FaultPoint, (Fault, usize)>>);

impl Faults {
    pub fn fail(&self, point: FaultPoint, times: usize) {
        self.arm(point, Fault::Fail, times);
    }

    pub fn delay(&self, point: FaultPoint, by: Duration, times: usize) {
        self.arm(point, Fault::Delay(by), times);
    }

    fn arm(&self, point: FaultPoint, fault: Fault, times: usize) {
        let mut armed = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if times == 0 {
            armed.remove(&point);
        } else {
            armed.insert(point, (fault, times));
        }
    }

    fn take(&self, point: FaultPoint) -> Option<Fault> {
        let mut armed = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let (fault, remaining) = armed.get_mut(&point)?;
        let fault = *fault;
        *remaining -= 1;
        if *remaining == 0 {
            armed.remove(&point);
        }
        Some(fault)
    }
}

/// The gateway store. Clones share one pool and one process lock; the lock is released when the
/// last clone drops.
#[derive(Clone)]
pub struct ApiStore {
    pool: SqlitePool,
    options: SqliteConnectOptions,
    _lock: Arc<File>,
    faults: Option<Arc<Faults>>,
}

impl fmt::Debug for ApiStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiStore").finish_non_exhaustive()
    }
}

impl ApiStore {
    /// Lock `<state_dir>/api.lock`, create `<state_dir>/api.sqlite` privately, and migrate it.
    /// The lock is the evidence that no other process on this `state_dir` runs API turns.
    ///
    /// # Errors
    /// [`OpenError::InUse`] while another store holds the lock; [`OpenError::Fault`] for a
    /// filesystem failure, a symlinked path, or a database at an unknown schema version.
    pub async fn open(state_dir: &Path) -> Result<Self, OpenError> {
        let lock = create_private_file(state_dir, &state_dir.join(API_LOCK_FILE))
            .map_err(|_| OpenError::Fault)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(OpenError::InUse),
            Err(TryLockError::Error(_)) => return Err(OpenError::Fault),
        }
        let path = state_dir.join(API_STORE_FILE);
        create_private_file(state_dir, &path).map_err(|_| OpenError::Fault)?;
        // FULL syncs the WAL on every commit, as crabber's own store does, so each committed
        // `202 accepted` survives power loss. On macOS a plain fsync leaves data in the drive's
        // cache; fullfsync makes those syncs use F_FULLFSYNC, and other platforms ignore it.
        // journal_mode is set once in `migrate`, never per connection.
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(false)
            .foreign_keys(true)
            .busy_timeout(OPERATION_BOUND)
            .synchronous(SqliteSynchronous::Full)
            .pragma("fullfsync", "ON")
            .disable_statement_logging();
        migrate(&options).await.map_err(|_| OpenError::Fault)?;
        let pool = SqlitePoolOptions::new()
            .max_connections(MAX_CONNECTIONS)
            .acquire_timeout(OPERATION_BOUND)
            .connect_with(options.clone())
            .await
            .map_err(|_| OpenError::Fault)?;
        Ok(Self {
            pool,
            options,
            _lock: Arc::new(lock),
            faults: None,
        })
    }

    #[doc(hidden)]
    #[must_use]
    pub fn with_faults(mut self, faults: Arc<Faults>) -> Self {
        self.faults = Some(faults);
        self
    }

    /// # Errors
    /// [`StoreFault`] on a database failure or timeout.
    pub async fn open_conversation(
        &self,
        conversation_id: ApiUuid,
        coworker_id: &CoworkerId,
    ) -> Result<Opened, StoreFault> {
        self.bounded(None, async {
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let id = conversation_id.to_string();
            let existing: Option<String> = sqlx::query_scalar(
                "SELECT coworker_id FROM conversations WHERE conversation_id = ?",
            )
            .bind(&id)
            .fetch_optional(&mut *tx)
            .await?;
            let opened = match existing {
                Some(owner) if owner == coworker_id.as_str() => Opened::Existing,
                Some(_) => Opened::Conflict,
                None => {
                    sqlx::query(
                        "INSERT INTO conversations (conversation_id, coworker_id, created_at) \
                         VALUES (?, ?, ?)",
                    )
                    .bind(&id)
                    .bind(coworker_id.as_str())
                    .bind(now()?)
                    .execute(&mut *tx)
                    .await?;
                    Opened::Created
                }
            };
            tx.commit().await?;
            Ok(opened)
        })
        .await
    }

    /// The conversation's coworker and phase; `running` while any request is active.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout.
    pub async fn conversation(
        &self,
        conversation_id: ApiUuid,
    ) -> Result<Option<ConversationState>, StoreFault> {
        self.bounded(None, async {
            let row = bind_active!(sqlx::query(
                "SELECT c.conversation_id, c.coworker_id, EXISTS (SELECT 1 FROM requests r \
                 WHERE r.conversation_id = c.conversation_id AND r.status IN (?, ?, ?)) AS active \
                 FROM conversations c WHERE c.conversation_id = ?",
            ))
            .bind(conversation_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
            row.as_ref().map(decode_conversation).transpose()
        })
        .await
    }

    /// Up to `limit` conversations with IDs after `after`, optionally only `coworker`'s.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout.
    pub async fn list_conversations(
        &self,
        coworker: Option<&CoworkerId>,
        after: Option<ApiUuid>,
        limit: usize,
    ) -> Result<ConversationRows, StoreFault> {
        let fetch = i64::try_from(limit.saturating_add(1)).map_err(|_| StoreFault)?;
        self.bounded(None, async {
            let rows = bind_active!(sqlx::query(
                "SELECT c.conversation_id, c.coworker_id, EXISTS (SELECT 1 FROM requests r \
                 WHERE r.conversation_id = c.conversation_id AND r.status IN (?1, ?2, ?3)) AS active \
                 FROM conversations c \
                 WHERE (?4 IS NULL OR c.coworker_id = ?4) AND (?5 IS NULL OR c.conversation_id > ?5) \
                 ORDER BY c.conversation_id LIMIT ?6",
            ))
            .bind(coworker.map(CoworkerId::as_str))
            .bind(after.map(|id| id.to_string()))
            .bind(fetch)
            .fetch_all(&self.pool)
            .await?;
            let mut conversations = rows
                .iter()
                .map(decode_conversation)
                .collect::<Result<Vec<_>, _>>()?;
            let next = if conversations.len() > limit {
                conversations.truncate(limit);
                conversations.last().map(|state| state.conversation_id)
            } else {
                None
            };
            Ok(ConversationRows {
                conversations,
                next,
            })
        })
        .await
    }

    /// Record one admission attempt for (`conversation_id`, `request_id`). An existing identity is
    /// compared byte for byte and never counts against `max_active`.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout; the attempt may or may not have committed.
    pub async fn submit(
        &self,
        conversation_id: ApiUuid,
        request_id: ApiUuid,
        text: &str,
        max_active: usize,
    ) -> Result<Submitted, StoreFault> {
        let max_active = i64::try_from(max_active).map_err(|_| StoreFault)?;
        let digest = sha256_hex(text.as_bytes());
        self.bounded(None, async {
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let conversation = conversation_id.to_string();
            let request = request_id.to_string();
            let existing = sqlx::query(select_requests!(
                "WHERE conversation_id = ? AND request_id = ?"
            ))
            .bind(&conversation)
            .bind(&request)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(existing) = existing {
                let stored_digest: String = existing.try_get("text_sha256")?;
                let row = decode_request(&existing)?;
                tx.rollback().await?;
                return Ok(if stored_digest == digest && row.text == text {
                    Submitted::Existing(row)
                } else {
                    Submitted::TextConflict
                });
            }
            let known: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM conversations WHERE conversation_id = ?)",
            )
            .bind(&conversation)
            .fetch_one(&mut *tx)
            .await?;
            if !known {
                tx.rollback().await?;
                return Ok(Submitted::NoConversation);
            }
            let active: i64 = bind_active!(sqlx::query_scalar(
                "SELECT COUNT(*) FROM requests WHERE status IN (?, ?, ?) AND conversation_id = ?",
            ))
            .bind(&conversation)
            .fetch_one(&mut *tx)
            .await?;
            if active >= max_active {
                tx.rollback().await?;
                return Ok(Submitted::QueueFull);
            }
            let at = now()?;
            let seq = sqlx::query(
                "INSERT INTO requests (conversation_id, request_id, text, text_sha256, status, \
                 created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&conversation)
            .bind(&request)
            .bind(text)
            .bind(&digest)
            .bind(RequestStatus::Accepted.as_str())
            .bind(&at)
            .bind(&at)
            .execute(&mut *tx)
            .await?
            .last_insert_rowid();
            tx.commit().await?;
            Ok(Submitted::Inserted(RequestRow {
                seq: Seq(seq),
                conversation_id,
                request_id,
                text: text.to_owned(),
                state: RequestState::Accepted,
                cancel_requested_at: None,
                retirement: None,
            }))
        })
        .await
    }

    /// The receipt source for one identity.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout.
    pub async fn request(
        &self,
        conversation_id: ApiUuid,
        request_id: ApiUuid,
    ) -> Result<Option<RequestRow>, StoreFault> {
        self.bounded(None, async {
            let row = sqlx::query(select_requests!(
                "WHERE conversation_id = ? AND request_id = ?"
            ))
            .bind(conversation_id.to_string())
            .bind(request_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
            row.as_ref().map(decode_request).transpose()
        })
        .await
    }

    /// The conversation's head: its oldest `running` or `cancellation_requested` request, else
    /// its oldest `accepted` one. An unsettled request always runs or reconciles first.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout.
    pub async fn next_work(
        &self,
        conversation_id: ApiUuid,
    ) -> Result<Option<RequestRow>, StoreFault> {
        self.bounded(None, async {
            let row = bind_active!(sqlx::query(select_requests!(
                "WHERE status IN (?, ?, ?) AND conversation_id = ? ORDER BY status = ?, seq LIMIT 1"
            )))
            .bind(conversation_id.to_string())
            .bind(RequestStatus::Accepted.as_str())
            .fetch_optional(&self.pool)
            .await?;
            row.as_ref().map(decode_request).transpose()
        })
        .await
    }

    /// `accepted` becomes `running`. A retry after a [`StoreFault`] may find the request already
    /// started by the attempt that faulted, and reports [`MarkedRunning::AlreadyStarted`].
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout, or when no request has this `seq`. The
    /// attempt may or may not have committed.
    pub async fn mark_running(&self, seq: Seq) -> Result<MarkedRunning, StoreFault> {
        self.bounded(Some(FaultPoint::MarkRunning), async {
            let (row, changed) = self
                .apply(Key::Seq(seq), Event::Start)
                .await?
                .ok_or(StoreFault)?;
            match (changed, row.state()) {
                (true, _) => Ok(MarkedRunning::Started(row)),
                (false, RequestState::Running | RequestState::CancellationRequested) => {
                    Ok(MarkedRunning::AlreadyStarted(row))
                }
                (false, RequestState::Settled(_)) => Ok(MarkedRunning::Settled(row)),
                // `Start` always applies to an `accepted` row.
                (false, RequestState::Accepted) => Err(StoreFault),
            }
        })
        .await
    }

    /// Settle a `running` or `cancellation_requested` request, or fail an `accepted` one that
    /// never ran. A settled request keeps its outcome, so a late `completed` never replaces
    /// `cancelled`, and `completed` still wins over a pending cancellation. `Cancelled` applies
    /// only after a cancellation request. Any outcome that does not apply returns
    /// [`Finished::Unchanged`] with the current row.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout, or when no request has this `seq`. The
    /// attempt may or may not have committed.
    pub async fn finish(&self, seq: Seq, outcome: Outcome) -> Result<Finished, StoreFault> {
        self.bounded(Some(FaultPoint::Finish), async {
            let (row, changed) = self
                .apply(Key::Seq(seq), Event::Finish(outcome))
                .await?
                .ok_or(StoreFault)?;
            Ok(if changed {
                Finished::Settled(row)
            } else {
                Finished::Unchanged(row)
            })
        })
        .await
    }

    /// Settle a request whose turn was interrupted, deciding inside one transaction what the
    /// interruption was: `uncertain` with the recorded reason when reconciliation began retiring
    /// the request's run ([`Self::begin_retirement`]), else `cancelled` when cancellation was
    /// requested, else `failed` with `interrupted`. A cancellation that commits first is
    /// therefore honoured, and one that commits afterwards finds the request settled. An
    /// `accepted` or settled request returns [`Finished::Unchanged`].
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout, or when no request has this `seq`. The
    /// attempt may or may not have committed.
    pub async fn interrupt(&self, seq: Seq) -> Result<Finished, StoreFault> {
        self.bounded(Some(FaultPoint::Finish), async {
            let (row, changed) = self
                .apply(Key::Seq(seq), Event::Interrupt)
                .await?
                .ok_or(StoreFault)?;
            Ok(if changed {
                Finished::Settled(row)
            } else {
                Finished::Unchanged(row)
            })
        })
        .await
    }

    /// Record, before reconciliation makes crabber interrupt a request's orphaned run, why the
    /// request then settles `uncertain`. Once the run is interrupted nothing in crabber says who
    /// interrupted it, so this record is what keeps a later reconciliation from reading the
    /// interruption as a graceful stop or as the client's cancellation. Only a `running` or
    /// `cancellation_requested` request takes the record. A later reason replaces an earlier
    /// one, because it is the latest reconciliation that interrupts the run.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout, or when no request has this `seq`. The
    /// attempt may or may not have committed.
    pub async fn begin_retirement(
        &self,
        seq: Seq,
        reason: FailureReason,
    ) -> Result<Retiring, StoreFault> {
        self.bounded(Some(FaultPoint::BeginRetirement), async {
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let found = sqlx::query(select_requests!("WHERE seq = ?"))
                .bind(seq.0)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(StoreFault)?;
            let row = decode_request(&found)?;
            let started = matches!(
                row.state,
                RequestState::Running | RequestState::CancellationRequested
            );
            if !started || row.retirement == Some(reason) {
                tx.rollback().await?;
                return Ok(if started {
                    Retiring::Recorded(row)
                } else {
                    Retiring::Kept(row)
                });
            }
            sqlx::query("UPDATE requests SET retirement_reason = ?, updated_at = ? WHERE seq = ?")
                .bind(reason.as_str())
                .bind(now()?)
                .bind(seq.0)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(Retiring::Recorded(RequestRow {
                retirement: Some(reason),
                ..row
            }))
        })
        .await
    }

    /// `accepted` becomes `cancelled`, `running` becomes `cancellation_requested`, and any other
    /// state is returned unchanged. The first request time is kept. `None` for an unknown identity.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout.
    pub async fn request_cancel(
        &self,
        conversation_id: ApiUuid,
        request_id: ApiUuid,
        now: OffsetDateTime,
    ) -> Result<Option<RequestRow>, StoreFault> {
        let at = rfc3339(now)?;
        self.bounded(None, async {
            let changed = self
                .apply(
                    Key::Identity(conversation_id, request_id),
                    Event::Cancel(at),
                )
                .await?;
            Ok(changed.map(|(row, _)| row))
        })
        .await
    }

    /// Whether cancellation was ever requested for this request.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout, or when no request has this `seq`.
    pub async fn cancel_requested(&self, seq: Seq) -> Result<bool, StoreFault> {
        self.bounded(None, async {
            let requested: Option<Option<String>> =
                sqlx::query_scalar("SELECT cancel_requested_at FROM requests WHERE seq = ?")
                    .bind(seq.0)
                    .fetch_optional(&self.pool)
                    .await?;
            requested.map(|at| at.is_some()).ok_or(StoreFault)
        })
        .await
    }

    /// `running` returns to `accepted`, keeping its `seq`. Only reconciliation uses this. A retry
    /// after a [`StoreFault`] may find the request already requeued by the attempt that faulted,
    /// and reports [`Requeued::AlreadyAccepted`].
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout, or when no request has this `seq`. The
    /// attempt may or may not have committed.
    pub async fn requeue(&self, seq: Seq) -> Result<Requeued, StoreFault> {
        self.bounded(None, async {
            let (row, changed) = self
                .apply(Key::Seq(seq), Event::Requeue)
                .await?
                .ok_or(StoreFault)?;
            Ok(match (changed, row.state()) {
                (true, _) => Requeued::Requeued(row),
                (false, RequestState::Accepted) => Requeued::AlreadyAccepted(row),
                (false, _) => Requeued::Kept(row),
            })
        })
        .await
    }

    /// Conversations with an active request, ordered by their oldest active request.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout.
    pub async fn conversations_with_work(&self) -> Result<Vec<(ApiUuid, CoworkerId)>, StoreFault> {
        self.bounded(None, async {
            let rows = bind_active!(sqlx::query(
                "SELECT c.conversation_id, c.coworker_id FROM conversations c \
                 JOIN requests r ON r.conversation_id = c.conversation_id \
                 WHERE r.status IN (?, ?, ?) GROUP BY c.conversation_id ORDER BY MIN(r.seq)",
            ))
            .fetch_all(&self.pool)
            .await?;
            rows.iter().map(decode_conversation_ids).collect()
        })
        .await
    }

    /// The newest `max_rows` requests, oldest first.
    ///
    /// # Errors
    /// [`StoreFault`] on a database failure or timeout.
    pub async fn snapshot_rows(
        &self,
        conversation_id: ApiUuid,
        max_rows: usize,
    ) -> Result<SnapshotRows, StoreFault> {
        let fetch = i64::try_from(max_rows.saturating_add(1)).map_err(|_| StoreFault)?;
        self.bounded(None, async {
            let found = sqlx::query(select_requests!(
                "WHERE conversation_id = ? ORDER BY seq DESC LIMIT ?"
            ))
            .bind(conversation_id.to_string())
            .bind(fetch)
            .fetch_all(&self.pool)
            .await?;
            let truncated = found.len() > max_rows;
            let mut rows = found
                .iter()
                .take(max_rows)
                .map(decode_request)
                .collect::<Result<Vec<_>, _>>()?;
            rows.reverse();
            Ok(SnapshotRows { rows, truncated })
        })
        .await
    }

    /// Whether a fresh connection, outside the pool, reads this store's schema version within 2 s.
    pub async fn ping(&self) -> bool {
        let probe = self.guarded(Some(FaultPoint::Ping), PING_BOUND, async {
            let mut connection = SqliteConnection::connect_with(&self.options).await?;
            let version: i64 = sqlx::query_scalar("PRAGMA user_version")
                .fetch_one(&mut connection)
                .await?;
            connection.close().await?;
            Ok(version == SCHEMA_VERSION)
        });
        probe.await.unwrap_or(false)
    }

    async fn apply(
        &self,
        key: Key,
        event: Event,
    ) -> Result<Option<(RequestRow, bool)>, StoreFault> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let query = match key {
            Key::Seq(seq) => sqlx::query(select_requests!("WHERE seq = ?")).bind(seq.0),
            Key::Identity(conversation_id, request_id) => sqlx::query(select_requests!(
                "WHERE conversation_id = ? AND request_id = ?"
            ))
            .bind(conversation_id.to_string())
            .bind(request_id.to_string()),
        };
        let Some(found) = query.fetch_optional(&mut *tx).await? else {
            tx.rollback().await?;
            return Ok(None);
        };
        let row = decode_request(&found)?;
        let Some(next) = row.after(event) else {
            tx.rollback().await?;
            return Ok(Some((row, false)));
        };
        sqlx::query(
            "UPDATE requests SET status = ?, reason = ?, result_text = ?, cancel_requested_at = ?, \
             retirement_reason = ?, updated_at = ? WHERE seq = ?",
        )
        .bind(next.state.status().as_str())
        .bind(next.state.reason().map(FailureReason::as_str))
        .bind(next.state.result())
        .bind(next.cancel_requested_at.as_deref())
        .bind(next.retirement.map(FailureReason::as_str))
        .bind(now()?)
        .bind(next.seq.0)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some((next, true)))
    }

    async fn bounded<T>(
        &self,
        point: Option<FaultPoint>,
        work: impl Future<Output = Result<T, StoreFault>>,
    ) -> Result<T, StoreFault> {
        self.guarded(point, OPERATION_BOUND, work).await
    }

    async fn guarded<T>(
        &self,
        point: Option<FaultPoint>,
        bound: Duration,
        work: impl Future<Output = Result<T, StoreFault>>,
    ) -> Result<T, StoreFault> {
        let fault = point.and_then(|point| self.faults.as_deref()?.take(point));
        if let Some(Fault::Fail) = fault {
            return Err(StoreFault);
        }
        tokio::time::timeout(bound, async {
            let value = work.await?;
            if let Some(Fault::Delay(by)) = fault {
                tokio::time::sleep(by).await;
            }
            Ok(value)
        })
        .await
        .map_err(|_| StoreFault)?
    }
}

enum Key {
    Seq(Seq),
    Identity(ApiUuid, ApiUuid),
}

async fn migrate(options: &SqliteConnectOptions) -> Result<(), StoreFault> {
    let mut connection = SqliteConnection::connect_with(options).await?;
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut connection)
        .await?;
    if version != 0 && version != SCHEMA_VERSION {
        return Err(StoreFault);
    }
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode = WAL")
        .fetch_one(&mut connection)
        .await?;
    if mode != "wal" {
        return Err(StoreFault);
    }
    if version == 0 {
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::raw_sql(SCHEMA).execute(&mut *tx).await?;
        tx.commit().await?;
    }
    connection.close().await?;
    Ok(())
}

fn decode_request(row: &SqliteRow) -> Result<RequestRow, StoreFault> {
    let status = RequestStatus::parse(row.try_get("status")?).ok_or(StoreFault)?;
    let reason = row
        .try_get::<Option<&str>, _>("reason")?
        .map(|reason| FailureReason::parse(reason).ok_or(StoreFault))
        .transpose()?;
    let state = RequestState::from_columns(status, reason, row.try_get("result_text")?)
        .ok_or(StoreFault)?;
    let cancel_requested_at: Option<String> = row.try_get("cancel_requested_at")?;
    if !state.admits_cancellation(cancel_requested_at.is_some()) {
        return Err(StoreFault);
    }
    let retirement = row
        .try_get::<Option<&str>, _>("retirement_reason")?
        .map(|reason| FailureReason::parse(reason).ok_or(StoreFault))
        .transpose()?;
    Ok(RequestRow {
        seq: Seq(row.try_get("seq")?),
        conversation_id: uuid_column(row, "conversation_id")?,
        request_id: uuid_column(row, "request_id")?,
        text: row.try_get("text")?,
        state,
        cancel_requested_at,
        retirement,
    })
}

fn decode_conversation(row: &SqliteRow) -> Result<ConversationState, StoreFault> {
    let (conversation_id, coworker_id) = decode_conversation_ids(row)?;
    let active: bool = row.try_get("active")?;
    Ok(ConversationState {
        conversation_id,
        coworker_id,
        phase: if active {
            ConversationPhase::Running
        } else {
            ConversationPhase::Idle
        },
    })
}

fn decode_conversation_ids(row: &SqliteRow) -> Result<(ApiUuid, CoworkerId), StoreFault> {
    let coworker_id = CoworkerId::parse(row.try_get("coworker_id")?).ok_or(StoreFault)?;
    Ok((uuid_column(row, "conversation_id")?, coworker_id))
}

fn uuid_column(row: &SqliteRow, column: &str) -> Result<ApiUuid, StoreFault> {
    ApiUuid::parse(row.try_get(column)?).ok_or(StoreFault)
}

fn now() -> Result<String, StoreFault> {
    rfc3339(OffsetDateTime::now_utc())
}

fn rfc3339(at: OffsetDateTime) -> Result<String, StoreFault> {
    at.to_offset(UtcOffset::UTC)
        .format(&Rfc3339)
        .map_err(|_| StoreFault)
}
