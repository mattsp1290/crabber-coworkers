//! The dispatcher against a real gateway store and real crabber stores: ordering, the turn cap,
//! cancellation, graceful stop, and what a new process makes of requests an earlier one left
//! unsettled. Every orphaned run is a real crabber run whose owning child process was killed
//! with `SIGKILL`.
mod common;
#[path = "common/example.rs"]
mod example;

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::Method,
    response::{IntoResponse, Response},
};
use crabber::{
    AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest, Admission, AdmissionKey,
    AdmissionOptions, Agent, AttemptControlOutcome, AttemptResolverClaim, AttemptRetirement,
    AttemptRetirementOutcome, AttemptUncertainty, AttemptUncertaintyCause, FakeProvider,
    InputFingerprint, PendingCallDisposition, RuntimeError, Selection, SessionId, StreamDelta,
    core::{ContentBlock, ControlId, Role, Run, RunFence, RunStatus},
    providers::{
        DeltaStream, ModelRequest, ProviderBindingError, ProviderError, Resolver, Streamer,
    },
    session::{
        AdmissionExecutionError, AdmissionExecutionState, AttemptError, AttemptReceipt,
        AttemptState, SqliteStore, Store,
    },
};
use crabber_coworkers::{
    api::{
        contract::{ApiUuid, CoworkerId, FailureReason},
        dispatch::{
            ADMISSION_OUTSTANDING, Dispatcher, RECONCILIATION_FAILED, STORE_WRITE_FAILED, Wiring,
        },
        store::{
            ApiStore, FaultPoint, Faults, MarkedRunning, OpenError, Outcome, RequestRow,
            RequestState, Requeued, Submitted,
        },
    },
    ask::{self, KeyedOutcome, KeyedRequest, Reconciled, Session},
    bridge,
    config::{BorbConfig, Config, CoworkerDefinition, WorkspaceAccess},
    execution::Remote,
    turn::{self, Backend, TurnPermits},
    workspace::{self, Workspace},
};
use futures::{FutureExt, Stream};
use sha2::{Digest, Sha256};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use std::{
    collections::VecDeque,
    fmt::Write,
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    pin::Pin,
    process::{Command, Stdio},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use time::OffsetDateTime;
use tokio::sync::{mpsc, watch};

const CONVERSATION: u128 = 1;
const REQUEST: u128 = 10;
const FOLLOW_UP: u128 = 11;
const TEXT: &str = "synthetic request";
const RECOVERED: &str = "recovered answer";

const KILL_ENV: &str = "COWORKERS_TEST_KILL";
const OWNER_ENV: &str = "COWORKERS_TEST_OWNER";
const STATE_ENV: &str = "COWORKERS_TEST_STATE";
const READY_ENV: &str = "COWORKERS_TEST_READY";

/// Bound on anything that does not wait for a crabber lease.
const SOON: Duration = Duration::from_secs(20);
/// Crabber's run lease is a fixed 30 seconds, judged by the store clock.
const AFTER_LEASE: Duration = Duration::from_secs(60);

fn id(n: u128) -> ApiUuid {
    ApiUuid::parse(&uuid::Uuid::from_u128(n).hyphenated().to_string()).unwrap()
}

fn config(state: &Path) -> Config {
    let mut config = example::config();
    config.state_dir = state.to_path_buf();
    config
}

fn local(provider: Arc<dyn Resolver>) -> Backend {
    Backend::Local {
        resolver: provider,
        selection: common::selection(),
    }
}

fn scripted(answers: &[&str]) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(
        answers
            .iter()
            .map(|answer| common::text_answer(answer))
            .collect(),
    ))
}

fn completed(answer: &str) -> RequestState {
    RequestState::Settled(Outcome::Completed(answer.into()))
}

fn failed(reason: FailureReason) -> RequestState {
    RequestState::Settled(Outcome::Failed(reason))
}

fn uncertain(reason: FailureReason) -> RequestState {
    RequestState::Settled(Outcome::Uncertain(reason))
}

const CANCELLED: RequestState = RequestState::Settled(Outcome::Cancelled);

fn said(request: &ModelRequest, role: Role, text: &str) -> bool {
    request.messages.iter().any(|message| {
        message.role == role
            && message.parts.iter().any(
                |part| matches!(&part.content, ContentBlock::Text { text: said } if said == text),
            )
    })
}

async fn eventually<T>(
    what: &str,
    within: Duration,
    mut probe: impl AsyncFnMut() -> Option<T>,
) -> T {
    let found = tokio::time::timeout(within, async {
        loop {
            if let Some(found) = probe().await {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    found
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

async fn record(
    store: &ApiStore,
    conversation: u128,
    coworker: &str,
    request: u128,
    text: &str,
) -> RequestRow {
    store
        .open_conversation(id(conversation), &CoworkerId::parse(coworker).unwrap())
        .await
        .unwrap();
    match store
        .submit(id(conversation), id(request), text, 16)
        .await
        .unwrap()
    {
        Submitted::Inserted(row) => row,
        other => panic!("request was not inserted: {other:?}"),
    }
}

/// The request as a process that died right after starting it leaves it: `running`.
async fn record_running(store: &ApiStore, conversation: u128, coworker: &str, request: u128) {
    let row = record(store, conversation, coworker, request, TEXT).await;
    let started = store.mark_running(row.seq()).await.unwrap();
    assert!(matches!(started, MarkedRunning::Started(_)));
}

/// A dispatcher over one state directory, wired the way `serve` wires it.
struct Gateway {
    dispatcher: Dispatcher,
    store: ApiStore,
    /// The dispatcher's turn permits, for a test that holds them all.
    permits: TurnPermits,
    stop: watch::Sender<bool>,
    logs: mpsc::UnboundedReceiver<String>,
}

impl Gateway {
    async fn start(config: Config, backend: Backend, faults: Option<Arc<Faults>>) -> Self {
        let locals = turn::open_all(&config, &backend, &mut common::never())
            .await
            .unwrap();
        let store = ApiStore::open(&config.state_dir).await.unwrap();
        let store = match faults {
            Some(faults) => store.with_faults(faults),
            None => store,
        };
        let (stop, stopped) = watch::channel(false);
        let (log, logs) = mpsc::unbounded_channel();
        let permits = TurnPermits::new(config.limits.max_concurrent_turns);
        let dispatcher = Dispatcher::new(Wiring {
            store: store.clone(),
            backend: Arc::new(backend),
            permits: permits.clone(),
            config: Arc::new(config),
            locals,
            stop: stopped,
            log,
        });
        Self {
            dispatcher,
            store,
            permits,
            stop,
            logs,
        }
    }

    async fn serving(state: &Path, provider: Arc<dyn Resolver>) -> Self {
        Self::start(config(state), local(provider), None).await
    }

    async fn faulty(state: &Path, provider: Arc<dyn Resolver>, faults: Arc<Faults>) -> Self {
        Self::start(config(state), local(provider), Some(faults)).await
    }

    /// What the submit route does: commit the request, then wake its conversation.
    async fn submit(&self, conversation: u128, request: u128, text: &str) {
        record(&self.store, conversation, "scout", request, text).await;
        self.dispatcher.wake(id(conversation));
    }

    async fn cancel(&self, conversation: u128, request: u128) -> RequestRow {
        self.dispatcher
            .cancel(id(conversation), id(request))
            .await
            .unwrap()
            .expect("the request exists")
    }

    async fn state(&self, conversation: u128, request: u128) -> RequestState {
        let row = self.store.request(id(conversation), id(request)).await;
        row.unwrap().expect("the request exists").state().clone()
    }

    async fn reaches(&self, conversation: u128, request: u128, wanted: &RequestState) {
        let what = format!("request {request} to become {wanted:?}");
        eventually(&what, SOON, async || {
            (self.state(conversation, request).await == *wanted).then_some(())
        })
        .await;
    }

    async fn settles(&self, conversation: u128, request: u128, within: Duration) -> RequestState {
        let what = format!("request {request} to settle");
        eventually(&what, within, async || {
            let state = self.state(conversation, request).await;
            matches!(state, RequestState::Settled(_)).then_some(state)
        })
        .await
    }

    async fn settled(&self, conversation: u128, request: u128) -> RequestState {
        self.settles(conversation, request, SOON).await
    }

    async fn logs(&mut self, line: &str) {
        self.logs_within(line, SOON).await;
    }

    async fn logs_within(&mut self, line: &str, within: Duration) {
        let logged = tokio::time::timeout(within, async {
            while let Some(logged) = self.logs.recv().await {
                if logged == line {
                    return;
                }
            }
            panic!("the dispatcher went away before logging {line}");
        });
        logged
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for the log line {line}"));
    }

    /// Wait until no conversation task is left: nothing is running or queued.
    async fn idle(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.dispatcher.join())
            .await
            .expect("the dispatcher goes idle");
    }

    /// Stop gracefully and release the store lock, as a clean process exit does. Returns the
    /// log lines not yet read.
    async fn stop(self) -> Vec<String> {
        self.stop.send(true).unwrap();
        self.ended("tasks exit after the stop signal").await
    }

    /// Stop as a process does whose stop budget has run out: abort what is still running, and
    /// release the store lock. Returns the log lines not yet read.
    async fn abort(self) -> Vec<String> {
        self.stop.send(true).unwrap();
        self.dispatcher.abort();
        self.ended("aborted tasks end").await
    }

    async fn ended(mut self, expected: &str) -> Vec<String> {
        tokio::time::timeout(SOON, self.dispatcher.join())
            .await
            .expect(expected);
        let mut lines = Vec::new();
        while let Ok(line) = self.logs.try_recv() {
            lines.push(line);
        }
        lines
    }
}

/// One coworker's crabber state, opened the way `serve` opens it, for looking at runs directly.
struct Host {
    def: CoworkerDefinition,
    workspace: Workspace,
    store: Arc<SqliteStore>,
}

impl Host {
    async fn open(state: &Path, access: WorkspaceAccess) -> Self {
        let def = orphan_config(state, access)
            .coworker("scout")
            .unwrap()
            .clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let workspace = workspace::admit(&def, state, deadline, &mut common::never())
            .await
            .unwrap();
        let store = crabber_coworkers::store::open(state, &def.id)
            .await
            .unwrap();
        Self {
            def,
            workspace,
            store,
        }
    }

    fn agent(&self, provider: Arc<dyn Resolver>) -> Agent {
        bridge::build_agent(
            &self.def,
            &self.workspace,
            self.store.clone(),
            provider,
            common::selection(),
        )
        .unwrap()
    }

    fn keyed<'a>(
        &'a self,
        provider: Arc<dyn Resolver>,
        conversation: &'a str,
        request: u128,
        text: &'a str,
    ) -> KeyedRequest<'a> {
        KeyedRequest {
            def: &self.def,
            workspace: &self.workspace,
            resolver: provider,
            selection: common::selection(),
            store: self.store.clone(),
            conversation,
            key: AdmissionKey::new(id(request).to_string()).unwrap(),
            text,
        }
    }

    /// The crabber run admitted for a request, if any.
    async fn run(&self, conversation: u128, request: u128) -> Option<Run> {
        let key = AdmissionKey::new(id(request).to_string()).unwrap();
        let session = SessionId::from(id(conversation).to_string().as_str());
        let receipt = self.store.lookup_admission(&session, &key).await.unwrap()?;
        self.store.get_run(&receipt.run_id).await.unwrap()
    }

    async fn status(&self, conversation: u128, request: u128) -> Option<RunStatus> {
        self.run(conversation, request).await.map(|run| run.status)
    }
}

fn orphan_config(state: &Path, access: WorkspaceAccess) -> Config {
    let mut config = config(state);
    config.coworkers[0].workspace = access;
    config
}

fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + SOON
}

#[tokio::test]
async fn keyed_admission_replays_instead_of_running_twice() {
    let directory = tempfile::tempdir().unwrap();
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    let provider = scripted(&["only answer"]);
    let conversation = id(CONVERSATION).to_string();
    let request = host.keyed(provider.clone(), &conversation, REQUEST, TEXT);
    assert_eq!(
        ask::reconcile_keyed(&request).await,
        Ok(Reconciled::NotAdmitted)
    );

    for _ in 0..2 {
        assert_eq!(
            ask::ask_keyed(&request, deadline(), common::never()).await,
            Ok(KeyedOutcome::Answered("only answer".into()))
        );
    }
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(
        ask::reconcile_keyed(&request).await,
        Ok(Reconciled::Answered("only answer".into()))
    );

    let changed = host.keyed(provider.clone(), &conversation, REQUEST, "another request");
    assert_eq!(
        ask::ask_keyed(&changed, deadline(), common::never()).await,
        Ok(KeyedOutcome::ConfigurationChanged)
    );
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn requests_in_one_conversation_run_in_order_with_history() {
    let directory = tempfile::tempdir().unwrap();
    let provider = scripted(&["one", "two"]);
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    gateway.submit(CONVERSATION, REQUEST, "first").await;
    gateway.submit(CONVERSATION, FOLLOW_UP, "second").await;

    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        completed("two")
    );
    assert_eq!(gateway.state(CONVERSATION, REQUEST).await, completed("one"));
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(said(&requests[0], Role::User, "first"));
    assert!(!said(&requests[0], Role::User, "second"));
    assert!(said(&requests[1], Role::User, "first"));
    assert!(said(&requests[1], Role::Assistant, "one"));
    assert!(said(&requests[1], Role::User, "second"));
}

#[tokio::test]
async fn conversations_share_the_turn_permits() {
    let directory = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let mut config = config(directory.path());
    config.limits.max_concurrent_turns = 2;
    let gateway = Gateway::start(config, local(provider.clone()), None).await;
    for conversation in 1..=3 {
        gateway.submit(conversation, REQUEST, TEXT).await;
    }

    eventually("two turns to reach the model", SOON, async || {
        (provider.requests().len() == 2).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(provider.requests().len(), 2);

    provider.release();
    eventually("the third turn to reach the model", SOON, async || {
        (provider.requests().len() == 3).then_some(())
    })
    .await;
    provider.release();
    provider.release();
    for conversation in 1..=3 {
        assert_eq!(
            gateway.settled(conversation, REQUEST).await,
            completed("ok")
        );
    }
}

#[tokio::test]
async fn api_only_coworker_writes_a_workspace_file() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = config(directory.path());
    let quill = config
        .coworkers
        .iter_mut()
        .find(|def| def.id == "quill")
        .unwrap();
    assert!(quill.slack.is_none());
    quill.workspace = WorkspaceAccess::ReadWrite;
    let provider = Arc::new(FakeProvider::scripted(vec![
        common::tool_call(
            "file_write",
            &serde_json::json!({"path": "note.txt", "content": "written by quill"}),
        ),
        common::text_answer("saved note.txt"),
    ]));
    let gateway = Gateway::start(config, local(provider), None).await;
    record(&gateway.store, CONVERSATION, "quill", REQUEST, TEXT).await;
    gateway.dispatcher.wake(id(CONVERSATION));

    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed("saved note.txt")
    );
    let note = directory.path().join("workspaces/quill/note.txt");
    assert_eq!(std::fs::read_to_string(note).unwrap(), "written by quill");
}

#[tokio::test]
async fn cancelling_a_queued_request_never_reaches_the_model() {
    let directory = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    gateway.submit(CONVERSATION, REQUEST, "first").await;
    eventually("the first turn to reach the model", SOON, async || {
        (provider.requests().len() == 1).then_some(())
    })
    .await;
    gateway.submit(CONVERSATION, FOLLOW_UP, "never sent").await;

    let cancelled = gateway.cancel(CONVERSATION, FOLLOW_UP).await;
    assert_eq!(cancelled.state(), &CANCELLED);
    provider.release();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed("ok")
    );
    gateway.idle().await;
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert!(!said(&requests[0], Role::User, "never sent"));
}

#[tokio::test]
async fn cancelling_a_running_request_interrupts_its_turn() {
    let directory = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    eventually("the turn to reach the model", SOON, async || {
        (provider.requests().len() == 1).then_some(())
    })
    .await;

    let requested = gateway.cancel(CONVERSATION, REQUEST).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);
    assert!(requested.cancel_requested_at().is_some());
    assert_eq!(gateway.settled(CONVERSATION, REQUEST).await, CANCELLED);
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(
        host.status(CONVERSATION, REQUEST).await,
        Some(RunStatus::Interrupted)
    );
}

#[tokio::test]
async fn a_cancel_that_lands_after_the_answer_leaves_it_completed() {
    let directory = tempfile::tempdir().unwrap();
    let faults = Arc::new(Faults::default());
    faults.fail(FaultPoint::Finish, 3);
    let gateway = Gateway::faulty(directory.path(), scripted(&["done"]), faults).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    eventually("the run to complete", SOON, async || {
        (host.status(CONVERSATION, REQUEST).await == Some(RunStatus::Completed)).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let requested = gateway.cancel(CONVERSATION, REQUEST).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed("done")
    );
    let receipt = gateway.store.request(id(CONVERSATION), id(REQUEST)).await;
    assert!(receipt.unwrap().unwrap().cancel_requested_at().is_some());
}

#[tokio::test]
async fn a_cancel_between_registration_and_start_still_interrupts() {
    let directory = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let faults = Arc::new(Faults::default());
    faults.delay(FaultPoint::MarkRunning, Duration::from_secs(1), 1);
    let gateway = Gateway::faulty(directory.path(), provider.clone(), faults).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    gateway
        .reaches(CONVERSATION, REQUEST, &RequestState::Running)
        .await;

    let requested = gateway.cancel(CONVERSATION, REQUEST).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);
    let settled = gateway.settles(CONVERSATION, REQUEST, Duration::from_secs(5));
    assert_eq!(settled.await, CANCELLED);
    assert_eq!(provider.requests().len(), 0);
}

/// Leaves the request `running` with no crabber admission, as a process does that dies after
/// starting a request and before crabber admits it.
async fn died_before_admission(state: &Path) {
    let store = ApiStore::open(state).await.unwrap();
    record_running(&store, CONVERSATION, "scout", REQUEST).await;
}

#[tokio::test]
async fn an_unadmitted_request_is_requeued_and_runs() {
    let directory = tempfile::tempdir().unwrap();
    died_before_admission(directory.path()).await;

    let provider = scripted(&["requeued answer"]);
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(host.status(CONVERSATION, REQUEST).await, None);
    gateway.dispatcher.wake_pending().await.unwrap();

    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed("requeued answer")
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert!(said(&requests[0], Role::User, TEXT));
}

#[tokio::test]
async fn an_unadmitted_cancelled_request_is_cancelled() {
    let directory = tempfile::tempdir().unwrap();
    died_before_admission(directory.path()).await;

    let provider = scripted(&[]);
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    let requested = gateway.cancel(CONVERSATION, REQUEST).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);

    let settled = gateway.settles(CONVERSATION, REQUEST, Duration::from_secs(5));
    assert_eq!(settled.await, CANCELLED);
    gateway.idle().await;
    assert_eq!(provider.requests().len(), 0);
}

/// Long enough for a conversation task to take its head request as far as it can.
const TASK_SETTLES: Duration = Duration::from_millis(300);

#[tokio::test]
async fn a_request_stopped_while_it_waits_for_a_turn_stays_queued() {
    let directory = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let mut config = config(directory.path());
    config.limits.max_concurrent_turns = 1;
    let gateway = Gateway::start(config, local(provider.clone()), None).await;
    gateway.submit(1, REQUEST, "holds the only turn").await;
    eventually("the first turn to reach the model", SOON, async || {
        (provider.requests().len() == 1).then_some(())
    })
    .await;
    gateway.submit(2, REQUEST, TEXT).await;
    tokio::time::sleep(TASK_SETTLES).await;
    gateway.stop().await;

    let provider = scripted(&["after restart"]);
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    assert_eq!(gateway.state(2, REQUEST).await, RequestState::Accepted);
    assert_eq!(
        gateway.state(1, REQUEST).await,
        failed(FailureReason::Interrupted)
    );
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(2, REQUEST).await,
        completed("after restart")
    );
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn a_stop_before_admission_leaves_the_request_for_the_next_process() {
    let directory = tempfile::tempdir().unwrap();
    let faults = Arc::new(Faults::default());
    faults.delay(FaultPoint::MarkRunning, Duration::from_secs(1), 1);
    let unasked = scripted(&[]);
    let gateway = Gateway::faulty(directory.path(), unasked.clone(), faults).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    gateway
        .reaches(CONVERSATION, REQUEST, &RequestState::Running)
        .await;
    // The stop lands while the start is still being written, so the turn finds it before it
    // asks crabber for anything.
    assert_eq!(gateway.stop().await, [] as [&str; 0]);
    assert_eq!(unasked.requests().len(), 0);
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(host.status(CONVERSATION, REQUEST).await, None);

    let provider = scripted(&["after restart"]);
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed("after restart")
    );
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn a_keyed_turn_cancelled_before_admission_admits_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    let provider = scripted(&["answer"]);
    let conversation = id(CONVERSATION).to_string();
    let request = host.keyed(provider.clone(), &conversation, REQUEST, TEXT);

    let cancelled = Box::pin(std::future::ready(()));
    assert_eq!(
        ask::ask_keyed(&request, deadline(), cancelled).await,
        Ok(KeyedOutcome::NotAdmitted)
    );
    assert_eq!(
        ask::reconcile_keyed(&request).await,
        Ok(Reconciled::NotAdmitted)
    );
    assert_eq!(
        ask::ask_keyed(&request, deadline(), common::never()).await,
        Ok(KeyedOutcome::Answered("answer".into()))
    );
    assert_eq!(provider.requests().len(), 1);
}

/// A provider whose binding crabber cannot describe for its first `failures` lookups. Crabber
/// looks the binding up while it prepares an admission, before anything commits, and reports the
/// failure as an error that carries no documented rejection guarantee.
struct Unbound {
    inner: Arc<FakeProvider>,
    failures: Mutex<usize>,
}

impl Unbound {
    fn new(answers: &[&str], failures: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: scripted(answers),
            failures: Mutex::new(failures),
        })
    }
}

#[async_trait]
impl Resolver for Unbound {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, ProviderBindingError> {
        let mut failures = self.failures.lock().unwrap();
        if let Some(left) = failures.checked_sub(1) {
            *failures = left;
            return Err(ProviderBindingError::Unavailable);
        }
        drop(failures);
        self.inner.binding_descriptor(selection)
    }

    async fn resolve(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.inner.resolve(selection).await
    }
}

/// Crabber's SQLite busy timeout: how long one of its writes waits for the write lock before it
/// fails as the sanitized `SQLite operation failed`.
const CRABBER_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Takes the write lock of a coworker's conversation store. Crabber still reads it.
async fn lock_conversations(state: &Path, coworker: &str) -> SqliteConnection {
    let path = crabber_coworkers::store::store_path(state, coworker);
    let options = SqliteConnectOptions::new().filename(path);
    let mut holder = SqliteConnection::connect_with(&options).await.unwrap();
    let begun = sqlx::query("BEGIN IMMEDIATE").execute(&mut holder).await;
    begun.unwrap();
    holder
}

#[tokio::test]
async fn an_admission_lost_to_a_store_failure_is_retried_under_its_key() {
    let directory = tempfile::tempdir().unwrap();
    let provider = scripted(&["one", "two"]);
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    gateway.submit(CONVERSATION, REQUEST, "first").await;
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed("one")
    );

    // The next admission's first write waits out the busy timeout and fails with an error that
    // says nothing about whether it committed. The retry finds the lock released.
    let mut holder = lock_conversations(directory.path(), "scout").await;
    gateway.submit(CONVERSATION, FOLLOW_UP, "second").await;
    tokio::time::sleep(CRABBER_BUSY_TIMEOUT + Duration::from_millis(700)).await;
    let released = sqlx::query("ROLLBACK").execute(&mut holder).await;
    released.unwrap();

    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        completed("two")
    );
    assert_eq!(provider.requests().len(), 2);
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(
        host.status(CONVERSATION, FOLLOW_UP).await,
        Some(RunStatus::Completed)
    );
    assert!(!gateway.stop().await.contains(&RECONCILIATION_FAILED.into()));
}

#[tokio::test]
async fn an_admission_whose_preparation_failed_fails_its_request() {
    let directory = tempfile::tempdir().unwrap();
    // The failed lookup leaves the key's preparation unretired, and the pinned crabber refuses
    // every retry of that key before restoring a provider.
    let provider = Unbound::new(&["next answer"], 1);
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;

    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        failed(FailureReason::ModelFailed)
    );
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(host.status(CONVERSATION, REQUEST).await, None);
    gateway.submit(CONVERSATION, FOLLOW_UP, "next").await;
    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        completed("next answer")
    );
    let requests = provider.inner.requests();
    assert_eq!(requests.len(), 1);
    assert!(!said(&requests[0], Role::User, TEXT));
    assert!(!gateway.stop().await.contains(&RECONCILIATION_FAILED.into()));
}

#[tokio::test]
async fn an_admission_that_never_gets_a_verdict_parks_its_request_until_cancelled() {
    const LAST: u128 = 12;
    let directory = tempfile::tempdir().unwrap();
    let provider = scripted(&["one", "last answer"]);
    let mut gateway = Gateway::serving(directory.path(), provider.clone()).await;
    // The conversation's session exists, so a later turn writes nothing before it admits.
    gateway.submit(CONVERSATION, REQUEST, "first").await;
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed("one")
    );

    // Every admission's first write waits out the busy timeout and fails with an error that
    // says nothing about whether it committed. Unlike a failed preparation, it leaves nothing
    // behind that refuses the key, so no attempt is definitive.
    let mut holder = lock_conversations(directory.path(), "scout").await;
    gateway.submit(CONVERSATION, FOLLOW_UP, TEXT).await;
    // The admission and its three requeued retries each wait out the busy timeout.
    gateway
        .logs_within(RECONCILIATION_FAILED, CRABBER_BUSY_TIMEOUT * 6)
        .await;
    gateway.idle().await;

    // Nothing is known to have failed, so nothing is settled.
    assert_eq!(
        gateway.state(CONVERSATION, FOLLOW_UP).await,
        RequestState::Running
    );
    let released = sqlx::query("ROLLBACK").execute(&mut holder).await;
    released.unwrap();
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(host.status(CONVERSATION, FOLLOW_UP).await, None);

    let requested = gateway.cancel(CONVERSATION, FOLLOW_UP).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);
    assert_eq!(gateway.settled(CONVERSATION, FOLLOW_UP).await, CANCELLED);
    gateway.submit(CONVERSATION, LAST, "last").await;
    assert_eq!(
        gateway.settled(CONVERSATION, LAST).await,
        completed("last answer")
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(!said(&requests[1], Role::User, TEXT));
}

/// A provider whose first binding lookup blocks until it is released. The admission or recovery
/// that asked for it stalls inside crabber, whoever still waits for it, and once released ends
/// as `released` says. Every other call goes to `inner`.
struct Stalled {
    inner: Arc<dyn Resolver>,
    stall: Mutex<Stall>,
    changed: Condvar,
    released: Released,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stall {
    Armed,
    Reached,
    Released,
}

/// What the stalled lookup does once it is released.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Released {
    /// It fails, so its admission ends without committing.
    Unbound,
    /// It succeeds, so its admission commits and starts a run.
    Bound,
}

/// Releases the stalled lookup when a test ends, however it ends: the lookup blocks a runtime
/// worker thread, and the runtime cannot shut down around it.
struct Release(Arc<Stalled>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl Stalled {
    fn around(inner: Arc<dyn Resolver>) -> (Arc<Self>, Release) {
        Self::new(inner, Released::Unbound)
    }

    /// A stall that only delays its admission: released, the admission commits.
    fn delaying(inner: Arc<dyn Resolver>) -> (Arc<Self>, Release) {
        Self::new(inner, Released::Bound)
    }

    fn new(inner: Arc<dyn Resolver>, released: Released) -> (Arc<Self>, Release) {
        let stalled = Arc::new(Self {
            inner,
            stall: Mutex::new(Stall::Armed),
            changed: Condvar::new(),
            released,
        });
        (stalled.clone(), Release(stalled))
    }

    fn reached(&self) -> bool {
        *self.stall.lock().unwrap() == Stall::Reached
    }

    fn release(&self) {
        *self.stall.lock().unwrap() = Stall::Released;
        self.changed.notify_all();
    }
}

#[async_trait]
impl Resolver for Stalled {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, ProviderBindingError> {
        let mut stall = self.stall.lock().unwrap();
        if *stall != Stall::Armed {
            drop(stall);
            return self.inner.binding_descriptor(selection);
        }
        *stall = Stall::Reached;
        let released = self
            .changed
            .wait_while(stall, |stall| *stall != Stall::Released);
        drop(released.unwrap());
        match self.released {
            Released::Unbound => Err(ProviderBindingError::Unavailable),
            Released::Bound => self.inner.binding_descriptor(selection),
        }
    }

    async fn resolve(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.inner.resolve(selection).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_admission_outlasts_a_graceful_stop_until_it_is_aborted() {
    let directory = tempfile::tempdir().unwrap();
    let unasked = scripted(&[]);
    let (stalled, release) = Stalled::around(unasked.clone());
    let mut gateway = Gateway::serving(directory.path(), stalled.clone()).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    eventually("the admission to stall", SOON, async || {
        stalled.reached().then_some(())
    })
    .await;

    // Crabber never answers. The stopped turn gives its admission up once the grace is over,
    // and the admission is kept all the same: an abort, as when a stop budget runs out, is
    // what ends the wait for it.
    gateway.stop.send(true).unwrap();
    gateway.logs(ADMISSION_OUTSTANDING).await;
    assert_eq!(gateway.abort().await, [] as [&str; 0]);
    drop(release);

    // The admission's outcome was never learned, so the request is left as it was for the next
    // process to reconcile, and its client can still cancel it.
    let gateway = Gateway::serving(directory.path(), scripted(&[])).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    let requested = gateway.cancel(CONVERSATION, REQUEST).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);
    assert_eq!(gateway.settled(CONVERSATION, REQUEST).await, CANCELLED);
    assert_eq!(unasked.requests().len(), 0);
}

/// A stop does not end the wait for an admission that crabber has not answered. Here the
/// admission commits after the stop, and the run it starts is interrupted instead of running
/// in a process that has stopped looking. Until then the gateway store stays locked, so no
/// other dispatcher on the state directory can take the request up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admission_that_commits_after_a_stop_is_interrupted_under_the_store_lock() {
    let directory = tempfile::tempdir().unwrap();
    let (model, stalled, _release) = delayed_admission();
    let mut gateway = Gateway::serving(directory.path(), stalled.clone()).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    eventually("the admission to stall", SOON, async || {
        stalled.reached().then_some(())
    })
    .await;

    // The stopped turn gives its admission up once the grace is over, and its conversation's
    // task ends. All that is left of the gateway is the task that keeps the admission alive.
    gateway.stop.send(true).unwrap();
    gateway.logs(ADMISSION_OUTSTANDING).await;
    let Gateway {
        dispatcher,
        store,
        stop: _stop,
        ..
    } = gateway;
    drop((dispatcher, store));
    tokio::time::sleep(TASK_SETTLES).await;
    let second = ApiStore::open(directory.path()).await;
    assert!(matches!(second, Err(OpenError::InUse)));

    // The preparation succeeds: the admission commits and starts its run. The lock is released
    // only after the run was stopped and the request settled.
    stalled.release();
    let store = eventually("the store lock to be released", SOON, async || {
        ApiStore::open(directory.path()).await.ok()
    })
    .await;
    let row = store.request(id(CONVERSATION), id(REQUEST)).await;
    assert_eq!(
        row.unwrap().unwrap().state(),
        &failed(FailureReason::Interrupted)
    );
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(
        host.status(CONVERSATION, REQUEST).await,
        Some(RunStatus::Interrupted)
    );
    // Its model request, if it got that far, was never answered.
    assert!(model.requests().len() <= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_stalled_admission_keeps_its_turn_and_settles_when_crabber_answers() {
    let directory = tempfile::tempdir().unwrap();
    let provider = scripted(&["next answer"]);
    let (stalled, release) = Stalled::around(provider.clone());
    let mut config = config(directory.path());
    config.limits.max_concurrent_turns = 1;
    let mut gateway = Gateway::start(config, local(stalled.clone()), None).await;
    gateway.submit(1, REQUEST, "stalls").await;
    eventually("the admission to stall", SOON, async || {
        stalled.reached().then_some(())
    })
    .await;
    gateway.submit(2, REQUEST, TEXT).await;

    // The cancelled turn gives its admission up once the grace is over. Crabber is still
    // preparing the admission, so nothing says yet that it will not commit and start a run:
    // the request is not settled, and the only turn is not given to the next conversation.
    let requested = gateway.cancel(1, REQUEST).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);
    gateway.logs(ADMISSION_OUTSTANDING).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        gateway.state(1, REQUEST).await,
        RequestState::CancellationRequested
    );
    assert_eq!(gateway.state(2, REQUEST).await, RequestState::Accepted);
    assert_eq!(provider.requests().len(), 0);

    // The preparation fails. Now nothing was admitted and nothing can be, and the turn is free.
    drop(release);
    assert_eq!(gateway.settled(1, REQUEST).await, CANCELLED);
    assert_eq!(gateway.settled(2, REQUEST).await, completed("next answer"));
    gateway.idle().await;
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(host.status(1, REQUEST).await, None);
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert!(said(&requests[0], Role::User, TEXT));
}

/// What a test wants run when crabber lets go of a model stream.
type Closed = Box<dyn FnOnce() + Send>;

/// A gated model that tells a test when a request's stream is opened, and when crabber lets go
/// of it.
#[derive(Clone)]
struct Watched {
    model: Arc<common::Gated>,
    /// Called when a stream is opened. What it returns runs when crabber lets go of the stream.
    opened: Arc<dyn Fn() -> Closed + Send + Sync>,
}

#[async_trait]
impl Resolver for Watched {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, ProviderBindingError> {
        self.model.binding_descriptor(selection)
    }

    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
impl Streamer for Watched {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        let closed = Some((self.opened)());
        let mut closing = Closing {
            inner: Box::pin(futures::stream::empty()),
            closed,
        };
        closing.inner = self.model.stream(request).await?;
        Ok(Box::pin(closing))
    }
}

struct Closing {
    inner: DeltaStream,
    closed: Option<Closed>,
}

impl Stream for Closing {
    type Item = StreamDelta;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<StreamDelta>> {
        self.inner.as_mut().poll_next(context)
    }
}

impl Drop for Closing {
    fn drop(&mut self) {
        if let Some(closed) = self.closed.take() {
            closed();
        }
    }
}

/// Counts the model requests that are open at once.
#[derive(Clone, Default)]
struct Open {
    now: Arc<AtomicUsize>,
    most: Arc<AtomicUsize>,
}

impl Open {
    fn watching(&self, model: Arc<common::Gated>) -> Arc<Watched> {
        let open = self.clone();
        let opened = move || {
            let now = open.now.fetch_add(1, Ordering::SeqCst) + 1;
            open.most.fetch_max(now, Ordering::SeqCst);
            let open = open.clone();
            Box::new(move || {
                open.now.fetch_sub(1, Ordering::SeqCst);
            }) as Closed
        };
        Arc::new(Watched {
            model,
            opened: Arc::new(opened),
        })
    }

    /// The most model requests that were ever open at once.
    fn most(&self) -> usize {
        self.most.load(Ordering::SeqCst)
    }
}

/// What a test wants done when crabber next lets go of a model stream. Crabber does that once
/// the answer's message is committed, and then, without waiting on anything in between, takes
/// its last look at whether the run was interrupted.
#[derive(Clone, Default)]
struct Parting(Arc<Mutex<Option<Closed>>>);

impl Parting {
    fn watching(&self, model: Arc<common::Gated>) -> Arc<Watched> {
        let parting = self.0.clone();
        let opened = move || {
            let parting = parting.clone();
            Box::new(move || {
                let then = parting.lock().unwrap().take();
                if let Some(then) = then {
                    then();
                }
            }) as Closed
        };
        Arc::new(Watched {
            model,
            opened: Arc::new(opened),
        })
    }

    fn arm(&self, then: impl FnOnce() + Send + 'static) {
        *self.0.lock().unwrap() = Some(Box::new(then));
    }

    fn armed(&self) -> bool {
        self.0.lock().unwrap().is_some()
    }
}

/// How a turn is told to stop.
#[derive(Clone, Copy, Debug)]
enum Told {
    Cancelled,
    Stopped,
}

/// A turn is told to stop after crabber took its last look at the run's interrupt. Crabber
/// completes the run and holds its answer, so the request is completed: not cancelled, and not
/// failed as interrupted.
///
/// Crabber's worker is held where it lets go of the model stream, until the test has the write
/// lock of the conversation store. Everything crabber writes before its last look is committed
/// by then, so the worker goes on past that look and waits at its next write. The turn is told
/// to stop while it waits there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_turn_told_to_stop_after_crabbers_last_look_leaves_its_request_completed() {
    for told in [Told::Cancelled, Told::Stopped] {
        let directory = tempfile::tempdir().unwrap();
        let model = common::Gated::new();
        let parting = Parting::default();
        let provider = parting.watching(model.clone());
        let gateway = Gateway::serving(directory.path(), provider).await;
        gateway.submit(CONVERSATION, REQUEST, TEXT).await;
        eventually("the turn to reach the model", SOON, async || {
            (model.requests().len() == 1).then_some(())
        })
        .await;

        let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
        let (parted, held) = std::sync::mpsc::channel();
        let (go_on, told_to_go_on) = std::sync::mpsc::channel();
        parting.arm(move || {
            parted.send(()).unwrap();
            let _: Result<(), _> = told_to_go_on.recv_timeout(SOON);
        });
        model.release();
        eventually("crabber to let go of the model stream", SOON, async || {
            held.try_recv().ok()
        })
        .await;
        let mut holder = lock_conversations(directory.path(), "scout").await;
        go_on.send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let running = Some(RunStatus::Running);
        assert_eq!(host.status(CONVERSATION, REQUEST).await, running);

        match told {
            Told::Cancelled => {
                let requested = gateway.cancel(CONVERSATION, REQUEST).await;
                assert_eq!(requested.state(), &RequestState::CancellationRequested);
            }
            Told::Stopped => gateway.stop.send(true).unwrap(),
        }
        // The turn interrupts its run, and nothing in crabber looks at that any more.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(host.status(CONVERSATION, REQUEST).await, running);
        let released = sqlx::query("ROLLBACK").execute(&mut holder).await;
        released.unwrap();

        let settled = gateway.settled(CONVERSATION, REQUEST).await;
        assert_eq!(settled, completed("ok"), "{told:?}");
        assert!(!parting.armed());
        assert_eq!(
            host.status(CONVERSATION, REQUEST).await,
            Some(RunStatus::Completed)
        );
        assert_eq!(gateway.stop().await, [] as [&str; 0]);
    }
}

/// A model whose request cannot be interrupted for a while: the call that opens its stream
/// blocks the thread it runs on. After that it never answers.
#[derive(Clone, Copy)]
struct Deaf(Duration);

#[async_trait]
impl Resolver for Deaf {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, ProviderBindingError> {
        common::Pending.binding_descriptor(selection)
    }

    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(*self))
    }
}

#[async_trait]
impl Streamer for Deaf {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        std::thread::sleep(self.0);
        Ok(Box::pin(futures::stream::pending()))
    }
}

/// A turn out of time interrupts its run, and the run does not end while the turn stands by
/// it. The request is not failed on the turn's word alone. Once crabber shows the run
/// interrupted, the request fails for the timeout that interrupted it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_that_outlasts_its_timed_out_turn_fails_as_a_timeout_once_it_is_interrupted() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = config(directory.path());
    config.limits.model_turn_seconds = 1;
    let deaf = Arc::new(Deaf(Duration::from_secs(8)));
    let gateway = Gateway::start(config, local(deaf), None).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;

    // Past the deadline, while the turn stands by the run it interrupted.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(
        host.status(CONVERSATION, REQUEST).await,
        Some(RunStatus::Running)
    );

    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        failed(FailureReason::Timeout)
    );
    assert_eq!(
        host.status(CONVERSATION, REQUEST).await,
        Some(RunStatus::Interrupted)
    );
    assert!(!gateway.stop().await.contains(&RECONCILIATION_FAILED.into()));
}

#[tokio::test]
async fn a_turn_whose_model_fails_fails_its_request() {
    let directory = tempfile::tempdir().unwrap();
    let provider = Arc::new(FakeProvider::scripted(vec![vec![common::failure(false)]]));
    let gateway = Gateway::serving(directory.path(), provider).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;

    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        failed(FailureReason::ModelFailed)
    );
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(
        host.status(CONVERSATION, REQUEST).await,
        Some(RunStatus::Failed)
    );
    assert_eq!(gateway.stop().await, [] as [&str; 0]);
}

/// Reconciliation read a run as live, and the run ended by itself before crabber was asked to
/// retire it. That is a race to read again, not a run whose settlement cannot be proven.
#[tokio::test]
async fn retiring_a_run_that_ended_after_it_was_read_is_a_race() {
    let directory = tempfile::tempdir().unwrap();
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    let model = common::Gated::new();
    let conversation = id(CONVERSATION).to_string();
    let request = host.keyed(model.clone(), &conversation, REQUEST, TEXT);
    let agent = host.agent(model.clone());
    let admission = agent.prompt_keyed(session(), TEXT, options(&agent)).await;
    let Ok(Admission::Started { handle, .. }) = admission else {
        panic!("a fresh key starts");
    };
    eventually("the run to reach the model", SOON, async || {
        (model.requests().len() == 1).then_some(())
    })
    .await;
    let Ok(Reconciled::Started(orphan)) = ask::reconcile_keyed(&request).await else {
        panic!("a run at the model has started");
    };

    model.release();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(
        ask::retire_started(&request, &orphan).await,
        Err(ask::Unsettled::Raced)
    );
    assert_eq!(
        ask::reconcile_keyed(&request).await,
        Ok(Reconciled::Answered("ok".into()))
    );
}

/// The model of a turn whose admission is delayed. It holds every request open until released,
/// so a run that reaches it cannot finish by itself.
fn delayed_admission() -> (Arc<common::Gated>, Arc<Stalled>, Release) {
    let model = common::Gated::new();
    let (stalled, release) = Stalled::delaying(model.clone());
    (model, stalled, release)
}

/// What a request's delayed admission must leave: a run that was interrupted, never completed,
/// whose model request, if it got that far, was never answered. The conversation's session is
/// free, so its next request runs.
async fn its_late_run_was_interrupted(gateway: &Gateway, model: &common::Gated, state: &Path) {
    let host = Host::open(state, WorkspaceAccess::Disabled).await;
    let interrupted = Some(RunStatus::Interrupted);
    assert_eq!(host.status(CONVERSATION, REQUEST).await, interrupted);
    gateway.idle().await;
    let asked = |text: &str| {
        let requests = model.requests();
        let asked = requests
            .iter()
            .filter(|request| said(request, Role::User, text));
        asked.count()
    };
    assert!(asked(TEXT) <= 1);

    model.release();
    gateway.submit(CONVERSATION, FOLLOW_UP, "next").await;
    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        completed("ok")
    );
    assert_eq!(asked("next"), 1);
    assert_eq!(host.status(CONVERSATION, REQUEST).await, interrupted);
}

/// The admission of a cancelled request commits after the turn has given it up: crabber goes on
/// preparing an admission nobody waits for. The request is not settled before crabber answers,
/// and the run that then starts is interrupted instead of running unobserved. That run can
/// reach the model before it is interrupted, so the request's turn is not given to another
/// conversation meanwhile: no two model requests are open at once under a limit of one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admission_that_commits_after_its_cancelled_turn_was_given_up_is_interrupted() {
    const OTHER: u128 = 2;
    let directory = tempfile::tempdir().unwrap();
    let model = common::Gated::new();
    let open = Open::default();
    let (stalled, _release) = Stalled::delaying(open.watching(model.clone()));
    let mut config = config(directory.path());
    config.limits.max_concurrent_turns = 1;
    let mut gateway = Gateway::start(config, local(stalled.clone()), None).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    eventually("the admission to stall", SOON, async || {
        stalled.reached().then_some(())
    })
    .await;
    gateway.submit(OTHER, REQUEST, "other").await;
    let requested = gateway.cancel(CONVERSATION, REQUEST).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);

    // The cancelled turn gives its admission up once the grace is over. Crabber is still
    // preparing that admission and may yet start its run, so the only turn stays taken and the
    // other conversation asks no model anything.
    gateway.logs(ADMISSION_OUTSTANDING).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(model.requests().len(), 0);
    assert_eq!(gateway.state(OTHER, REQUEST).await, RequestState::Accepted);

    // Crabber holds no admission, and that decides nothing yet: a wake, as any submission or
    // cancellation sends one, does not get the request settled as never admitted.
    gateway.dispatcher.wake(id(CONVERSATION));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::CancellationRequested
    );
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(host.status(CONVERSATION, REQUEST).await, None);

    // The preparation succeeds: the admission commits and starts its run, which is stopped.
    // Only then does the other conversation get the turn.
    stalled.release();
    assert_eq!(gateway.settled(CONVERSATION, REQUEST).await, CANCELLED);
    eventually("the other turn to reach the model", SOON, async || {
        let requests = model.requests();
        let mut asked = requests.iter();
        asked
            .any(|request| said(request, Role::User, "other"))
            .then_some(())
    })
    .await;
    model.release();
    assert_eq!(gateway.settled(OTHER, REQUEST).await, completed("ok"));
    assert_eq!(open.most(), 1);
    its_late_run_was_interrupted(&gateway, &model, directory.path()).await;
}

/// The admission commits within the grace a cancelled turn still waits for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admission_that_commits_just_after_its_turn_was_cancelled_is_interrupted() {
    let directory = tempfile::tempdir().unwrap();
    let (model, stalled, _release) = delayed_admission();
    let gateway = Gateway::serving(directory.path(), stalled.clone()).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    eventually("the admission to stall", SOON, async || {
        stalled.reached().then_some(())
    })
    .await;
    let requested = gateway.cancel(CONVERSATION, REQUEST).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::CancellationRequested
    );

    stalled.release();
    assert_eq!(gateway.settled(CONVERSATION, REQUEST).await, CANCELLED);
    its_late_run_was_interrupted(&gateway, &model, directory.path()).await;
}

/// The admission outlasts its turn's deadline and then commits. The request is not failed while
/// crabber still prepares the admission, and it times out once the late run is stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admission_that_outlasts_its_deadline_fails_only_after_crabber_answers() {
    let directory = tempfile::tempdir().unwrap();
    let (model, stalled, _release) = delayed_admission();
    let mut config = config(directory.path());
    config.limits.model_turn_seconds = 2;
    let gateway = Gateway::start(config, local(stalled.clone()), None).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    eventually("the admission to stall", SOON, async || {
        stalled.reached().then_some(())
    })
    .await;

    // Long past the deadline, and past anything a turn does to clean up after itself.
    tokio::time::sleep(Duration::from_secs(9)).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(host.status(CONVERSATION, REQUEST).await, None);

    stalled.release();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        failed(FailureReason::Timeout)
    );
    its_late_run_was_interrupted(&gateway, &model, directory.path()).await;
}

/// The admission outlasts its turn's deadline and the grace after it. Crabber then holds no
/// admission for the request, and the request is still not returned to the queue: admitting it
/// again while the first admission is in flight is refused in a way that reads as final.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admission_given_up_at_its_deadline_is_not_asked_for_again_before_crabber_answers() {
    let directory = tempfile::tempdir().unwrap();
    let (model, stalled, _release) = delayed_admission();
    let mut config = config(directory.path());
    config.limits.model_turn_seconds = 2;
    let mut gateway = Gateway::start(config, local(stalled.clone()), None).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    eventually("the admission to stall", SOON, async || {
        stalled.reached().then_some(())
    })
    .await;

    // The turn gives its admission up once its deadline and the grace after it are over.
    gateway.logs(ADMISSION_OUTSTANDING).await;
    gateway.dispatcher.wake(id(CONVERSATION));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(host.status(CONVERSATION, REQUEST).await, None);

    stalled.release();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        failed(FailureReason::Timeout)
    );
    its_late_run_was_interrupted(&gateway, &model, directory.path()).await;
}

#[tokio::test]
async fn cancelling_a_request_that_waits_for_a_turn_stops_its_wait() {
    let directory = tempfile::tempdir().unwrap();
    let provider = scripted(&[]);
    let mut config = config(directory.path());
    config.limits.max_concurrent_turns = 1;
    let gateway = Gateway::start(config, local(provider.clone()), None).await;
    let only_turn = gateway.permits.acquire().await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    tokio::time::sleep(TASK_SETTLES).await;

    let cancelled = gateway.cancel(CONVERSATION, REQUEST).await;
    assert_eq!(cancelled.state(), &CANCELLED);
    // No turn is free, and the conversation's task is done all the same.
    gateway.idle().await;
    drop(only_turn);
    assert_eq!(provider.requests().len(), 0);
}

#[tokio::test]
async fn a_request_cancelled_before_its_backend_is_invoked_frees_its_turn_before_it_settles() {
    let directory = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let faults = Arc::new(Faults::default());
    faults.delay(FaultPoint::MarkRunning, Duration::from_secs(1), 1);
    // The cancelled request's settlement is retried for about three seconds.
    faults.fail(FaultPoint::Finish, 3);
    let mut config = config(directory.path());
    config.limits.max_concurrent_turns = 1;
    let gateway = Gateway::start(config, local(provider.clone()), Some(faults)).await;
    gateway.submit(1, REQUEST, "cancelled").await;
    gateway.reaches(1, REQUEST, &RequestState::Running).await;
    let requested = gateway.cancel(1, REQUEST).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);
    gateway.submit(2, REQUEST, TEXT).await;

    // The other conversation's turn starts while that settlement is still being retried.
    eventually("the other turn to reach the model", SOON, async || {
        (provider.requests().len() == 1).then_some(())
    })
    .await;
    assert_eq!(
        gateway.state(1, REQUEST).await,
        RequestState::CancellationRequested
    );
    provider.release();
    assert_eq!(gateway.settled(2, REQUEST).await, completed("ok"));
    assert_eq!(gateway.settled(1, REQUEST).await, CANCELLED);
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert!(said(&requests[0], Role::User, TEXT));
}

#[tokio::test]
async fn a_cancel_that_lands_while_an_interruption_settles_is_never_overridden() {
    let directory = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let faults = Arc::new(Faults::default());
    faults.delay(FaultPoint::Finish, Duration::from_secs(2), 1);
    let gateway = Gateway::faulty(directory.path(), provider.clone(), faults).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    eventually("the turn to reach the model", SOON, async || {
        (provider.requests().len() == 1).then_some(())
    })
    .await;

    gateway.stop.send(true).unwrap();
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    eventually("the stop to interrupt the run", SOON, async || {
        (host.status(CONVERSATION, REQUEST).await == Some(RunStatus::Interrupted)).then_some(())
    })
    .await;
    // The dispatcher is now inside its delayed settlement write.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let requested = gateway.cancel(CONVERSATION, REQUEST).await;

    // One write decides what the interruption was. A cancellation the store took as pending
    // is honoured, and any other finds the request already settled.
    let settled = gateway.settled(CONVERSATION, REQUEST).await;
    match requested.state() {
        RequestState::CancellationRequested => assert_eq!(settled, CANCELLED),
        found => {
            assert_eq!(found, &failed(FailureReason::Interrupted));
            assert_eq!(&settled, found);
        }
    }
}

/// Runs one request whose turn ends while every settlement write fails, and returns once the
/// dispatcher has given up: the request is still `running` and its crabber run is settled.
async fn settlement_lost(state: &Path, provider: Arc<dyn Resolver>) -> Gateway {
    let faults = Arc::new(Faults::default());
    faults.fail(FaultPoint::Finish, 4);
    let mut gateway = Gateway::faulty(state, provider, faults).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    gateway.logs(STORE_WRITE_FAILED).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    gateway
}

#[tokio::test]
async fn a_completed_run_whose_settlement_was_lost_is_completed_without_a_new_turn() {
    let directory = tempfile::tempdir().unwrap();
    let first = scripted(&["kept answer"]);
    let gateway = settlement_lost(directory.path(), first.clone()).await;
    gateway.stop().await;
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(
        host.status(CONVERSATION, REQUEST).await,
        Some(RunStatus::Completed)
    );

    let second = scripted(&[]);
    let gateway = Gateway::serving(directory.path(), second.clone()).await;
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed("kept answer")
    );
    assert_eq!(first.requests().len(), 1);
    assert_eq!(second.requests().len(), 0);
}

#[tokio::test]
async fn a_parked_request_is_settled_by_the_next_wake() {
    let directory = tempfile::tempdir().unwrap();
    let provider = scripted(&["kept answer"]);
    let gateway = settlement_lost(directory.path(), provider.clone()).await;
    gateway.idle().await;

    gateway.dispatcher.wake(id(CONVERSATION));
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed("kept answer")
    );
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn a_failed_run_whose_settlement_was_lost_is_failed() {
    let directory = tempfile::tempdir().unwrap();
    let provider = Arc::new(FakeProvider::scripted(vec![vec![common::failure(false)]]));
    let gateway = settlement_lost(directory.path(), provider).await;
    gateway.stop().await;
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(
        host.status(CONVERSATION, REQUEST).await,
        Some(RunStatus::Failed)
    );

    let gateway = Gateway::serving(directory.path(), scripted(&[])).await;
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        failed(FailureReason::ModelFailed)
    );
}

#[tokio::test]
async fn an_interrupted_run_whose_settlement_was_lost_is_failed() {
    let directory = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let faults = Arc::new(Faults::default());
    faults.fail(FaultPoint::Finish, 8);
    let gateway = Gateway::faulty(directory.path(), provider.clone(), faults).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    eventually("the turn to reach the model", SOON, async || {
        (provider.requests().len() == 1).then_some(())
    })
    .await;
    assert_eq!(gateway.stop().await, [STORE_WRITE_FAILED]);
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(
        host.status(CONVERSATION, REQUEST).await,
        Some(RunStatus::Interrupted)
    );

    let gateway = Gateway::serving(directory.path(), scripted(&[])).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        failed(FailureReason::Interrupted)
    );
}

#[tokio::test]
async fn an_interrupted_cancelled_run_whose_settlement_was_lost_is_cancelled() {
    let directory = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let faults = Arc::new(Faults::default());
    faults.fail(FaultPoint::Finish, 4);
    let mut gateway = Gateway::faulty(directory.path(), provider.clone(), faults).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    eventually("the turn to reach the model", SOON, async || {
        (provider.requests().len() == 1).then_some(())
    })
    .await;
    gateway.cancel(CONVERSATION, REQUEST).await;
    gateway.logs(STORE_WRITE_FAILED).await;
    gateway.stop().await;
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    assert_eq!(
        host.status(CONVERSATION, REQUEST).await,
        Some(RunStatus::Interrupted)
    );

    let gateway = Gateway::serving(directory.path(), scripted(&[])).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::CancellationRequested
    );
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(gateway.settled(CONVERSATION, REQUEST).await, CANCELLED);
}

fn remote(url: String) -> Remote {
    let borb = BorbConfig {
        url,
        name_prefix: "coworkers-test".into(),
        image: format!("example.invalid/worker@sha256:{}", "a".repeat(64)),
        model_secret: "model-access".into(),
    };
    Remote::new(&borb, "injected-test-token".into()).unwrap()
}

#[tokio::test]
async fn a_borb_request_left_unsettled_is_uncertain() {
    let directory = tempfile::tempdir().unwrap();
    let store = ApiStore::open(directory.path()).await.unwrap();
    record_running(&store, CONVERSATION, "scout", REQUEST).await;
    drop(store);

    let remote = remote("http://127.0.0.1:9".into());
    let gateway = Gateway::start(config(directory.path()), Backend::Borb(remote), None).await;
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        uncertain(FailureReason::ProcessStopped)
    );
}

const BORB_ANSWER: &str = "borb answer";

/// A borb control plane whose worker finishes every submitted turn at once.
struct Borb {
    remote: Remote,
    turns: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Borb {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Borb {
    async fn start() -> Self {
        let turns = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = remote(format!("http://{}", listener.local_addr().unwrap()));
        let router = Router::new().fallback(borb).with_state(turns.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            remote,
            turns,
            server,
        }
    }

    /// Turns submitted to the worker so far.
    fn turns(&self) -> usize {
        self.turns.load(Ordering::SeqCst)
    }
}

async fn borb(State(turns): State<Arc<AtomicUsize>>, request: Request) -> Response {
    if request.uri().path() == "/v1/borbs" {
        let spec = to_bytes(request.into_body(), 1 << 20).await.unwrap();
        let spec: serde_json::Value = serde_json::from_slice(&spec).unwrap();
        return Json(serde_json::json!({
            "id": "00000000000000000000000001", "client": "test", "spec": spec,
            "spec_generation": 1, "running_spec_generation": 1, "restart_required": false,
            "arch": "arm64", "desired": "running", "phase": "running",
            "phase_reason": null, "phase_message": null, "node": "test", "generation": 1,
            "last_activity_at": "2026-01-01T00:00:00Z",
            "phase_changed_at": "2026-01-01T00:00:00Z",
            "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
            "deleted_at": null,
        }))
        .into_response();
    }
    assert_eq!(request.method(), Method::PUT, "only a turn is submitted");
    turns.fetch_add(1, Ordering::SeqCst);
    let finished = serde_json::json!({"state": "finished", "result": {"Ok": BORB_ANSWER}});
    let mut response = Json(finished).into_response();
    let upstream = "true".parse().unwrap();
    response.headers_mut().insert("x-borb-upstream", upstream);
    response
}

#[tokio::test]
async fn a_borb_request_stopped_while_it_waits_for_a_turn_runs_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let borb = Borb::start().await;
    let one_turn = || {
        let mut config = config(directory.path());
        config.limits.max_concurrent_turns = 1;
        config
    };
    let backend = Backend::Borb(borb.remote.clone());
    let gateway = Gateway::start(one_turn(), backend, None).await;
    let only_turn = gateway.permits.acquire().await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    tokio::time::sleep(TASK_SETTLES).await;
    gateway.stop().await;
    drop(only_turn);

    // The worker was never asked, and the request is still queued instead of unsettled.
    let backend = Backend::Borb(borb.remote.clone());
    let gateway = Gateway::start(one_turn(), backend, None).await;
    assert_eq!(borb.turns(), 0);
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Accepted
    );
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed(BORB_ANSWER)
    );
    assert_eq!(borb.turns(), 1);
}

#[tokio::test]
async fn a_removed_coworker_fails_queued_requests_and_leaves_started_ones_uncertain() {
    let directory = tempfile::tempdir().unwrap();
    let store = ApiStore::open(directory.path()).await.unwrap();
    record_running(&store, CONVERSATION, "quill", REQUEST).await;
    record(&store, CONVERSATION, "quill", FOLLOW_UP, TEXT).await;
    drop(store);

    let mut config = config(directory.path());
    config.coworkers.retain(|def| def.id != "quill");
    let provider = scripted(&[]);
    let gateway = Gateway::start(config, local(provider.clone()), None).await;
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        failed(FailureReason::CoworkerRemoved)
    );
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        uncertain(FailureReason::CoworkerRemoved)
    );
    assert_eq!(provider.requests().len(), 0);
}

#[tokio::test]
async fn graceful_stop_fails_the_running_turn_and_keeps_the_queue() {
    let directory = tempfile::tempdir().unwrap();
    let provider = common::Gated::new();
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    gateway.submit(CONVERSATION, REQUEST, "running").await;
    eventually("the turn to reach the model", SOON, async || {
        (provider.requests().len() == 1).then_some(())
    })
    .await;
    gateway.submit(CONVERSATION, FOLLOW_UP, "queued").await;
    assert_eq!(gateway.stop().await, [] as [&str; 0]);

    let provider = scripted(&["after restart"]);
    let gateway = Gateway::serving(directory.path(), provider.clone()).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        failed(FailureReason::Interrupted)
    );
    assert_eq!(
        gateway.state(CONVERSATION, FOLLOW_UP).await,
        RequestState::Accepted
    );
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        completed("after restart")
    );
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn a_busy_conversation_fails_one_request_and_runs_the_next() {
    let directory = tempfile::tempdir().unwrap();
    let gateway = Gateway::serving(directory.path(), scripted(&["after busy"])).await;
    let host = Host::open(directory.path(), WorkspaceAccess::Disabled).await;
    let holder = common::Gated::new();
    let (held, store) = (holder.clone(), host.store.clone());
    let other_frontend = tokio::spawn(async move {
        let session = Session::Durable {
            store,
            name: id(CONVERSATION).to_string(),
        };
        let def = &host.def;
        let selection = common::selection();
        ask::ask(
            def,
            &host.workspace,
            "held",
            held,
            selection,
            deadline(),
            session,
            common::never(),
        )
        .await
    });
    eventually("the other frontend to hold the session", SOON, async || {
        (holder.requests().len() == 1).then_some(())
    })
    .await;

    gateway.submit(CONVERSATION, REQUEST, "blocked").await;
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        failed(FailureReason::ConversationBusy)
    );
    holder.release();
    assert_eq!(other_frontend.await.unwrap(), Ok("ok".into()));
    gateway.submit(CONVERSATION, FOLLOW_UP, "next").await;
    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        completed("after busy")
    );
}

/// Where in a turn the owning process dies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Killed {
    /// After the admission committed and before the run's worker was ever polled.
    BeforeBegin,
    /// While the provider holds the turn's first model request.
    DuringModelRequest,
    /// While the provider holds the model request that follows a completed `file_write`.
    AfterToolCall,
}

impl Killed {
    const ALL: [Self; 3] = [
        Self::BeforeBegin,
        Self::DuringModelRequest,
        Self::AfterToolCall,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::BeforeBegin => "before_begin",
            Self::DuringModelRequest => "during_model_request",
            Self::AfterToolCall => "after_tool_call",
        }
    }

    fn parse(name: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|killed| killed.name() == name)
            .expect("known kill point")
    }

    fn access(self) -> WorkspaceAccess {
        match self {
            Self::BeforeBegin | Self::DuringModelRequest => WorkspaceAccess::Disabled,
            Self::AfterToolCall => WorkspaceAccess::ReadWrite,
        }
    }

    /// Model responses the child's provider gives before it holds a request open.
    fn script(self) -> Vec<Vec<StreamDelta>> {
        match self {
            Self::BeforeBegin | Self::DuringModelRequest => vec![],
            Self::AfterToolCall => vec![common::tool_call(
                "file_write",
                &serde_json::json!({"path": "draft.txt", "content": "draft"}),
            )],
        }
    }
}

/// What the killed process was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
    /// A bare crabber host with no gateway store, for gate G2a.
    Crabber,
    /// A gateway whose request row is `running`.
    Gateway,
}

impl Owner {
    fn name(self) -> &'static str {
        match self {
            Self::Crabber => "crabber",
            Self::Gateway => "gateway",
        }
    }

    fn parse(name: &str) -> Self {
        [Self::Crabber, Self::Gateway]
            .into_iter()
            .find(|owner| owner.name() == name)
            .expect("known owner")
    }
}

/// Answers from a script, then holds every later model request open, reporting the first hold
/// by creating `marker`. Parent and child share this type so their provider bindings are equal.
#[derive(Clone)]
struct Held {
    script: Arc<Mutex<VecDeque<Vec<StreamDelta>>>>,
    requests: Arc<AtomicUsize>,
    marker: Option<PathBuf>,
}

impl Held {
    fn new(script: Vec<Vec<StreamDelta>>, marker: Option<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            script: Arc::new(Mutex::new(script.into())),
            requests: Arc::default(),
            marker,
        })
    }

    fn answering(answer: &str) -> Arc<Self> {
        Self::new(vec![common::text_answer(answer)], None)
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Resolver for Held {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, crabber::providers::ProviderBindingError>
    {
        common::Pending.binding_descriptor(selection)
    }

    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
impl Streamer for Held {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let next = self.script.lock().unwrap().pop_front();
        if let Some(deltas) = next {
            return Ok(Box::pin(futures::stream::iter(deltas)));
        }
        if let Some(marker) = &self.marker {
            std::fs::write(marker, "").unwrap();
        }
        Ok(Box::pin(futures::stream::pending()))
    }
}

fn session() -> SessionId {
    SessionId::from(id(CONVERSATION).to_string().as_str())
}

fn key() -> AdmissionKey {
    AdmissionKey::new(id(REQUEST).to_string()).unwrap()
}

/// The admission options the gateway is specified to use, computed without its code.
fn options(agent: &Agent) -> AdmissionOptions {
    let digest = Sha256::digest(TEXT.as_bytes())
        .iter()
        .fold(String::new(), |mut hex, byte| {
            write!(hex, "{byte:02x}").unwrap();
            hex
        });
    AdmissionOptions {
        key: key(),
        fingerprint: InputFingerprint::new(digest).unwrap(),
        behavior_fingerprint: agent.behavior_fingerprint().clone(),
    }
}

#[test]
#[ignore = "spawned by the orphan tests; admits one keyed run and waits to be killed"]
fn orphan_child() {
    let Ok(kill) = std::env::var(KILL_ENV) else {
        return;
    };
    let killed = Killed::parse(&kill);
    let owner = Owner::parse(&std::env::var(OWNER_ENV).unwrap());
    let state = PathBuf::from(std::env::var_os(STATE_ENV).unwrap());
    let ready = PathBuf::from(std::env::var_os(READY_ENV).unwrap());
    if owner == Owner::Gateway && killed != Killed::BeforeBegin {
        return tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(serve_until_killed(killed, &state, ready));
    }
    // One thread: after admission returns, the run's worker is polled only when this function
    // awaits again.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(admit_until_killed(owner, killed, &state, &ready));
}

/// A real gateway whose provider holds the model request the kill point names.
async fn serve_until_killed(killed: Killed, state: &Path, ready: PathBuf) {
    let provider = Held::new(killed.script(), Some(ready));
    let config = orphan_config(state, killed.access());
    let gateway = Gateway::start(config, local(provider), None).await;
    gateway.submit(CONVERSATION, REQUEST, TEXT).await;
    std::future::pending::<()>().await;
}

/// Admits through crabber directly, which is the only way to stop between the admission's commit
/// and its executor's first poll. A gateway owner first leaves the row a dispatcher leaves.
async fn admit_until_killed(owner: Owner, killed: Killed, state: &Path, ready: &Path) {
    let _gateway_store = match owner {
        Owner::Crabber => None,
        Owner::Gateway => {
            let store = ApiStore::open(state).await.unwrap();
            record_running(&store, CONVERSATION, "scout", REQUEST).await;
            Some(store)
        }
    };
    let host = Host::open(state, killed.access()).await;
    let held = match killed {
        Killed::BeforeBegin => None,
        Killed::DuringModelRequest | Killed::AfterToolCall => Some(ready.to_path_buf()),
    };
    let agent = host.agent(Held::new(killed.script(), held));
    let admission = agent
        .prompt_keyed(session(), TEXT, options(&agent))
        .await
        .unwrap();
    let Admission::Started { handle, .. } = admission else {
        panic!("a fresh key starts");
    };
    match killed {
        Killed::BeforeBegin => {
            std::fs::write(ready, "").unwrap();
            loop {
                std::thread::park();
            }
        }
        Killed::DuringModelRequest | Killed::AfterToolCall => {
            let _ = handle.done().await;
            panic!("the held model request never completes");
        }
    }
}

/// A keyed run whose owning process was killed, as a new process finds it.
struct Orphan {
    directory: tempfile::TempDir,
    killed: Killed,
    host: Host,
    run: Run,
    execution: Option<AdmissionExecutionState>,
}

impl Orphan {
    async fn leave(owner: Owner, killed: Killed) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let state = directory.path().join("state");
        let ready = directory.path().join("ready");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "orphan_child", "--ignored", "--nocapture"])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env(KILL_ENV, killed.name())
            .env(OWNER_ENV, owner.name())
            .env(STATE_ENV, &state)
            .env(READY_ENV, &ready)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            while !ready.exists() {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "child exited before reaching {killed:?}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let host = Host::open(&state, killed.access()).await;
        let run = host
            .run(CONVERSATION, REQUEST)
            .await
            .expect("the child's admission committed");
        let execution = host.store.admission_execution_state(&run.id).await.unwrap();
        Self {
            directory,
            killed,
            host,
            run,
            execution,
        }
    }

    fn state(&self) -> PathBuf {
        self.directory.path().join("state")
    }

    /// A gateway started on the orphan's state directory with the dead owner's configuration.
    async fn gateway(&self, provider: Arc<dyn Resolver>) -> Gateway {
        self.faulty_gateway(provider, None).await
    }

    async fn faulty_gateway(
        &self,
        provider: Arc<dyn Resolver>,
        faults: Option<Arc<Faults>>,
    ) -> Gateway {
        let config = orphan_config(&self.state(), self.killed.access());
        Gateway::start(config, local(provider), faults).await
    }

    /// The state of the run's execution attempt, if it has one.
    async fn attempt(&self) -> Option<AttemptState> {
        let latest = self.host.store.latest_execution_attempt(&self.run.id);
        latest.await.unwrap().map(|latest| latest.attempt.state)
    }

    async fn status(&self) -> RunStatus {
        self.host.status(CONVERSATION, REQUEST).await.unwrap()
    }

    fn lease_expired(&self) -> bool {
        OffsetDateTime::now_utc() >= self.run.lease_until
    }

    /// Require the state the kill point should have left.
    async fn found(&self, execution: AdmissionExecutionState, attempt: Option<AttemptState>) {
        assert_eq!(self.run.status, RunStatus::Running);
        assert_eq!(self.execution, Some(execution));
        assert_eq!(self.attempt().await, attempt);
    }

    async fn abandon(&self) -> Result<AbandonOutcome, AbandonError> {
        self.host
            .agent(Held::new(vec![], None))
            .abandon(AbandonRequest {
                expected: RunFence {
                    run_id: self.run.id.clone(),
                    claim_token: self.run.claim_token.clone(),
                },
                expected_owner: self.run.owner.clone(),
                authority: AbandonAuthority::ExpiredLease,
            })
            .await
    }

    async fn recover(&self, provider: Arc<Held>) -> Result<Admission, RuntimeError> {
        let agent = self.host.agent(provider);
        let options = options(&agent);
        agent.recover_admission(session(), TEXT, options).await
    }

    /// The first of crabber's native controls: revoke the started attempt's expired owner.
    async fn revoke(&self) -> Result<AttemptReceipt, AttemptError> {
        let latest = self
            .host
            .store
            .latest_execution_attempt(&self.run.id)
            .await?
            .expect("a started run has an attempt");
        let marked = self
            .host
            .agent(Held::new(vec![], None))
            .mark_attempt_uncertain(AttemptUncertainty {
                session_id: session(),
                run_id: self.run.id.clone(),
                id: ControlId::new(),
                expected_revision: latest.revision,
                target: latest.attempt.target,
                cause: AttemptUncertaintyCause::ExpiredOrRevokedOwner,
            })
            .await?;
        let AttemptControlOutcome::Applied(marked) = marked else {
            panic!("a new control ID applies");
        };
        Ok(marked)
    }

    /// Crabber's native reconciliation of a started attempt: revoke the expired owner, then
    /// retire the attempt as interrupted. Returns the attempt's state after each control.
    async fn retire(&self) -> Result<[AttemptState; 2], AttemptError> {
        let marked = self.revoke().await?;
        let retired = self
            .host
            .agent(Held::new(vec![], None))
            .retire_attempt(AttemptRetirement {
                session_id: session(),
                run_id: self.run.id.clone(),
                id: ControlId::new(),
                expected_revision: marked.revision,
                target: marked.attempt.target,
                outcome: AttemptRetirementOutcome::Interrupted,
                joined: AttemptResolverClaim {
                    resolver_id: "g2a-probe".into(),
                    authority_claim: "owning child process was killed and reaped".into(),
                },
                pending: PendingCallDisposition::CancelNeverStarted,
            })
            .await?;
        let AttemptControlOutcome::Applied(retired) = retired else {
            panic!("a new control ID applies");
        };
        Ok([marked.attempt.state, retired.attempt.state])
    }

    /// Whether an ordinary unkeyed turn on the orphan's session is admitted and completes.
    async fn next_turn(&self) -> Result<RunStatus, RuntimeError> {
        let agent = self.host.agent(Held::answering("next answer"));
        let handle = agent.prompt(Some(session()), "follow-up request").await?;
        Ok(handle.done().await?.status)
    }
}

async fn leases_expire(orphans: &[&Orphan]) {
    let latest = orphans
        .iter()
        .map(|orphan| orphan.run.lease_until)
        .max()
        .unwrap();
    let remaining = latest - OffsetDateTime::now_utc() + time::Duration::seconds(1);
    if remaining.is_positive() {
        tokio::time::sleep(remaining.unsigned_abs()).await;
    }
}

/// Gate G2a as amended on 2026-11-14: what the pinned crabber lets a new process do with a keyed
/// run whose owner was killed. Reconciliation is built on exactly these verdicts.
async fn g2a_crabber_settles_orphans_as_reconciliation_assumes() {
    let (unstarted, cancelled, started, after_tool) = tokio::join!(
        Orphan::leave(Owner::Crabber, Killed::BeforeBegin),
        Orphan::leave(Owner::Crabber, Killed::BeforeBegin),
        Orphan::leave(Owner::Crabber, Killed::DuringModelRequest),
        Orphan::leave(Owner::Crabber, Killed::AfterToolCall),
    );
    for orphan in [&unstarted, &cancelled] {
        orphan.found(AdmissionExecutionState::Unstarted, None).await;
    }
    for orphan in [&started, &after_tool] {
        let attempt = Some(AttemptState::Started);
        orphan
            .found(AdmissionExecutionState::Started, attempt)
            .await;
    }

    let live = unstarted.recover(Held::new(vec![], None)).await;
    assert!(matches!(
        live,
        Err(RuntimeError::AdmissionExecution(
            AdmissionExecutionError::LiveLease
        ))
    ));
    assert_eq!(started.abandon().await, Err(AbandonError::LiveLease));
    assert_eq!(started.retire().await, Err(AttemptError::LiveOwner));

    leases_expire(&[&unstarted, &cancelled, &started, &after_tool]).await;
    g2a_unstarted_orphans(&unstarted, &cancelled).await;
    g2a_started_orphan(&started).await;
    g2a_started_orphan(&after_tool).await;
}

/// Probe 1, and the abandonment the cancellation row uses.
async fn g2a_unstarted_orphans(unstarted: &Orphan, cancelled: &Orphan) {
    let provider = Held::answering(RECOVERED);
    let recovered = unstarted.recover(provider.clone()).await;
    let Ok(Admission::Started { handle, .. }) = recovered else {
        panic!("probe 1: recover_admission must start an orphaned Unstarted run: {recovered:?}");
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(provider.requests(), 1);
    let conversation = id(CONVERSATION).to_string();
    let request = unstarted.host.keyed(provider, &conversation, REQUEST, TEXT);
    assert_eq!(
        ask::reconcile_keyed(&request).await,
        Ok(Reconciled::Answered(RECOVERED.into()))
    );

    let abandoned = cancelled.abandon().await.unwrap();
    assert_eq!(abandoned.run.status, RunStatus::Interrupted);
    assert_eq!(cancelled.status().await, RunStatus::Interrupted);
    assert_eq!(cancelled.next_turn().await.unwrap(), RunStatus::Completed);
}

/// Probes 2 and 3 as observed: `abandon` refuses every Started orphan because the runtime
/// reserves an execution attempt for every run, and the native attempt controls settle it.
async fn g2a_started_orphan(orphan: &Orphan) {
    let replayed = orphan.recover(Held::new(vec![], None)).await;
    assert!(matches!(replayed, Ok(Admission::Replayed(_))));
    assert_eq!(
        orphan.abandon().await,
        Err(AbandonError::NativeReconciliationRequired)
    );
    assert_eq!(orphan.status().await, RunStatus::Running);
    assert!(matches!(
        orphan.next_turn().await,
        Err(RuntimeError::SessionBusy)
    ));

    assert_eq!(
        orphan.retire().await,
        Ok([AttemptState::OutcomeUnknown, AttemptState::Interrupted])
    );
    assert_eq!(orphan.status().await, RunStatus::Interrupted);
    assert_eq!(orphan.next_turn().await.unwrap(), RunStatus::Completed);
}

/// Row: `Unstarted`, cancellation not requested.
async fn an_unstarted_orphan_is_recovered_and_completes() {
    let orphan = Orphan::leave(Owner::Gateway, Killed::BeforeBegin).await;
    orphan.found(AdmissionExecutionState::Unstarted, None).await;
    let provider = Held::answering(RECOVERED);
    let gateway = orphan.gateway(provider.clone()).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    gateway.dispatcher.wake_pending().await.unwrap();

    let settled = gateway.settles(CONVERSATION, REQUEST, AFTER_LEASE).await;
    assert_eq!(settled, completed(RECOVERED));
    assert!(orphan.lease_expired());
    assert_eq!(provider.requests(), 1);
    assert_eq!(orphan.status().await, RunStatus::Completed);
}

/// Row: `Unstarted`, cancellation requested.
async fn an_unstarted_cancelled_orphan_is_abandoned_and_cancelled() {
    let orphan = Orphan::leave(Owner::Gateway, Killed::BeforeBegin).await;
    orphan.found(AdmissionExecutionState::Unstarted, None).await;
    let provider = Held::answering("next answer");
    let gateway = orphan.gateway(provider.clone()).await;
    let requested = gateway.cancel(CONVERSATION, REQUEST).await;
    assert_eq!(requested.state(), &RequestState::CancellationRequested);

    let settled = gateway.settles(CONVERSATION, REQUEST, AFTER_LEASE).await;
    assert_eq!(settled, CANCELLED);
    assert!(orphan.lease_expired());
    assert_eq!(provider.requests(), 0);
    assert_eq!(orphan.status().await, RunStatus::Interrupted);
    gateway.submit(CONVERSATION, FOLLOW_UP, "next").await;
    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        completed("next answer")
    );
}

/// Row: an admission that no longer matches the coworker's configuration. Nothing can run it,
/// so it is abandoned and the conversation takes the next request.
async fn an_unstarted_orphan_under_changed_instructions_is_abandoned_and_uncertain() {
    let orphan = Orphan::leave(Owner::Gateway, Killed::BeforeBegin).await;
    orphan.found(AdmissionExecutionState::Unstarted, None).await;
    let provider = Held::answering("next answer");
    let mut config = orphan_config(&orphan.state(), orphan.killed.access());
    config.coworkers[0].instructions.push_str(" Changed.");
    let gateway = Gateway::start(config, local(provider.clone()), None).await;
    gateway.dispatcher.wake_pending().await.unwrap();

    let settled = gateway.settles(CONVERSATION, REQUEST, AFTER_LEASE).await;
    assert_eq!(settled, uncertain(FailureReason::ConfigurationChanged));
    assert_eq!(provider.requests(), 0);
    assert_eq!(orphan.status().await, RunStatus::Interrupted);
    gateway.submit(CONVERSATION, FOLLOW_UP, "next").await;
    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        completed("next answer")
    );
    assert_eq!(provider.requests(), 1);
    assert!(!gateway.stop().await.contains(&RECONCILIATION_FAILED.into()));
}

/// The wait for crabber's verdict on a recovery is bounded like the wait on an admission, and
/// a claim crabber has not answered by then is kept like an admission.
async fn a_stalled_recovery_outlasts_a_graceful_stop_until_it_is_aborted() {
    let orphan = Orphan::leave(Owner::Gateway, Killed::BeforeBegin).await;
    orphan.found(AdmissionExecutionState::Unstarted, None).await;
    let unasked = Held::answering("never asked");
    let (stalled, release) = Stalled::around(unasked.clone());
    let mut gateway = orphan.gateway(stalled.clone()).await;
    gateway.dispatcher.wake_pending().await.unwrap();
    eventually("the recovery to stall", AFTER_LEASE, async || {
        stalled.reached().then_some(())
    })
    .await;
    assert!(orphan.lease_expired());

    // Crabber never answers the claim. The stopped turn gives it up once the grace is over,
    // and an abort ends the wait for it.
    gateway.stop.send(true).unwrap();
    gateway.logs(ADMISSION_OUTSTANDING).await;
    assert_eq!(gateway.abort().await, [] as [&str; 0]);
    drop(release);
    let store = ApiStore::open(&orphan.state()).await.unwrap();
    let row = store.request(id(CONVERSATION), id(REQUEST)).await;
    assert_eq!(row.unwrap().unwrap().state(), &RequestState::Running);
    assert_eq!(unasked.requests(), 0);
}

/// A request that went back to the queue just before its first admission committed after all.
/// Its key replays a run that never began, and that run is recovered instead of being left to
/// hold the conversation's session.
async fn a_requeued_request_whose_admission_had_committed_is_recovered() {
    let orphan = Orphan::leave(Owner::Gateway, Killed::BeforeBegin).await;
    orphan.found(AdmissionExecutionState::Unstarted, None).await;
    let provider = Held::answering(RECOVERED);
    let gateway = orphan.gateway(provider.clone()).await;
    let row = gateway.store.request(id(CONVERSATION), id(REQUEST)).await;
    let requeued = gateway.store.requeue(row.unwrap().unwrap().seq()).await;
    assert!(matches!(requeued.unwrap(), Requeued::Requeued(_)));
    gateway.dispatcher.wake_pending().await.unwrap();

    let settled = gateway.settles(CONVERSATION, REQUEST, AFTER_LEASE).await;
    assert_eq!(settled, completed(RECOVERED));
    assert!(orphan.lease_expired());
    assert_eq!(provider.requests(), 1);
    assert_eq!(orphan.status().await, RunStatus::Completed);
    assert!(!gateway.stop().await.contains(&RECONCILIATION_FAILED.into()));
}

/// A request that went back to the queue, whose first admission had committed after all and
/// never began, in a gateway that now runs its coworker under other instructions. Admitting it
/// again is refused as a conflict. The request is not called uncertain while the admission
/// still holds the conversation's session: its dead owner's lease is waited out and it is
/// abandoned first, so the conversation takes the next request.
async fn a_requeued_request_admitted_under_other_instructions_is_abandoned_and_uncertain() {
    let orphan = Orphan::leave(Owner::Gateway, Killed::BeforeBegin).await;
    orphan.found(AdmissionExecutionState::Unstarted, None).await;
    let provider = Held::answering("next answer");
    let mut config = orphan_config(&orphan.state(), orphan.killed.access());
    config.coworkers[0].instructions.push_str(" Changed.");
    let gateway = Gateway::start(config, local(provider.clone()), None).await;
    let row = gateway.store.request(id(CONVERSATION), id(REQUEST)).await;
    let requeued = gateway.store.requeue(row.unwrap().unwrap().seq()).await;
    assert!(matches!(requeued.unwrap(), Requeued::Requeued(_)));
    gateway.dispatcher.wake_pending().await.unwrap();

    let settled = gateway.settles(CONVERSATION, REQUEST, AFTER_LEASE).await;
    assert_eq!(settled, uncertain(FailureReason::ConfigurationChanged));
    assert!(orphan.lease_expired());
    assert_eq!(provider.requests(), 0);
    assert_eq!(orphan.status().await, RunStatus::Interrupted);
    gateway.submit(CONVERSATION, FOLLOW_UP, "next").await;
    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        completed("next answer")
    );
    assert_eq!(provider.requests(), 1);
    assert!(!gateway.stop().await.contains(&RECONCILIATION_FAILED.into()));
}

/// Row: `Started`. A gateway killed mid-turn leaves it, and the next gateway retires it.
async fn a_started_orphan_is_retired_and_uncertain(killed: Killed, cancel: bool) {
    let orphan = Orphan::leave(Owner::Gateway, killed).await;
    let attempt = Some(AttemptState::Started);
    orphan
        .found(AdmissionExecutionState::Started, attempt)
        .await;
    if killed == Killed::AfterToolCall {
        let draft = orphan.state().join("workspaces/scout/draft.txt");
        assert_eq!(std::fs::read_to_string(draft).unwrap(), "draft");
    }
    retired_by_the_next_gateway(&orphan, cancel).await;
}

/// A reconciliation that revoked the owner and then died leaves the attempt `OutcomeUnknown`.
/// The next one finishes the settlement.
async fn a_started_orphan_revoked_by_an_earlier_reconciliation_is_retired() {
    let orphan = Orphan::leave(Owner::Gateway, Killed::DuringModelRequest).await;
    let attempt = Some(AttemptState::Started);
    orphan
        .found(AdmissionExecutionState::Started, attempt)
        .await;
    leases_expire(&[&orphan]).await;
    let revoked = orphan.revoke().await.unwrap();
    assert_eq!(revoked.attempt.state, AttemptState::OutcomeUnknown);
    assert_eq!(orphan.status().await, RunStatus::Running);
    retired_by_the_next_gateway(&orphan, false).await;
}

/// The amendment's guarantee has to survive lost writes: once reconciliation has retired a
/// Started orphan, its interrupted run must never be read as a graceful stop.
async fn a_started_orphan_stays_uncertain_when_its_settlement_is_lost() {
    let orphan = Orphan::leave(Owner::Gateway, Killed::DuringModelRequest).await;
    let attempt = Some(AttemptState::Started);
    orphan
        .found(AdmissionExecutionState::Started, attempt)
        .await;
    let faults = Arc::new(Faults::default());
    let unasked = Held::answering("never asked");

    // The reason cannot be recorded, so crabber is not asked to interrupt anything.
    faults.fail(FaultPoint::BeginRetirement, 1);
    let mut gateway = orphan
        .faulty_gateway(unasked.clone(), Some(faults.clone()))
        .await;
    gateway.dispatcher.wake_pending().await.unwrap();
    gateway.logs_within(STORE_WRITE_FAILED, AFTER_LEASE).await;
    gateway.idle().await;
    assert!(orphan.lease_expired());
    assert_eq!(orphan.status().await, RunStatus::Running);
    assert_eq!(orphan.attempt().await, attempt);

    // The run is retired, and then no settlement write gets through.
    faults.fail(FaultPoint::Finish, usize::MAX);
    gateway.dispatcher.wake(id(CONVERSATION));
    gateway.logs(STORE_WRITE_FAILED).await;
    assert_eq!(orphan.status().await, RunStatus::Interrupted);
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    gateway.stop().await;

    // The next process finds an interrupted run and nothing else in crabber to explain it.
    let gateway = orphan.gateway(unasked.clone()).await;
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        uncertain(FailureReason::ProcessStopped)
    );
    assert_eq!(unasked.requests(), 0);
}

/// A run under the request's key that is still executing: its owner renews the lease, crabber
/// refuses to settle it, and reconciliation waits instead of calling the request lost.
async fn a_run_whose_owner_is_alive_is_not_settled() {
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("state");
    let store = ApiStore::open(&state).await.unwrap();
    record_running(&store, CONVERSATION, "scout", REQUEST).await;
    drop(store);
    let host = Host::open(&state, WorkspaceAccess::Disabled).await;
    let model = common::Gated::new();
    let agent = host.agent(model.clone());
    let admission = agent.prompt_keyed(session(), TEXT, options(&agent)).await;
    let Ok(Admission::Started { handle, .. }) = admission else {
        panic!("a fresh key starts");
    };
    let owner = tokio::spawn(async move { handle.done().await.map(|done| done.status) });
    eventually("the live run to reach the model", SOON, async || {
        (model.requests().len() == 1).then_some(())
    })
    .await;

    let unasked = Held::answering("never asked");
    let config = || orphan_config(&state, WorkspaceAccess::Disabled);
    let gateway = Gateway::start(config(), local(unasked.clone()), None).await;
    gateway.dispatcher.wake_pending().await.unwrap();
    // Reconciliation records its reason just before it first asks crabber to retire the run.
    eventually("reconciliation to try the run", AFTER_LEASE, async || {
        let row = gateway.store.request(id(CONVERSATION), id(REQUEST)).await;
        row.unwrap().unwrap().retirement().map(drop)
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    let run = host.run(CONVERSATION, REQUEST).await.unwrap();
    assert_eq!(run.status, RunStatus::Running);
    let latest = host.store.latest_execution_attempt(&run.id).await.unwrap();
    assert_eq!(latest.unwrap().attempt.state, AttemptState::Started);
    assert!(!gateway.stop().await.contains(&RECONCILIATION_FAILED.into()));

    // The run then finishes, and its answer is the request's result.
    model.release();
    assert_eq!(owner.await.unwrap().unwrap(), RunStatus::Completed);
    let gateway = Gateway::start(config(), local(unasked.clone()), None).await;
    gateway.dispatcher.wake_pending().await.unwrap();
    assert_eq!(
        gateway.settled(CONVERSATION, REQUEST).await,
        completed("ok")
    );
    assert_eq!(unasked.requests(), 0);
}

/// A new gateway settles the orphan's request as `uncertain`, leaves its run interrupted, and
/// the conversation takes the next request.
async fn retired_by_the_next_gateway(orphan: &Orphan, cancel: bool) {
    let provider = Held::answering("next answer");
    let gateway = orphan.gateway(provider.clone()).await;
    assert_eq!(
        gateway.state(CONVERSATION, REQUEST).await,
        RequestState::Running
    );
    if cancel {
        gateway.cancel(CONVERSATION, REQUEST).await;
    } else {
        gateway.dispatcher.wake_pending().await.unwrap();
    }

    let settled = gateway.settles(CONVERSATION, REQUEST, AFTER_LEASE).await;
    assert_eq!(settled, uncertain(FailureReason::ProcessStopped));
    assert!(orphan.lease_expired());
    assert_eq!(orphan.status().await, RunStatus::Interrupted);
    gateway.submit(CONVERSATION, FOLLOW_UP, "next").await;
    assert_eq!(
        gateway.settled(CONVERSATION, FOLLOW_UP).await,
        completed("next answer")
    );
    assert_eq!(provider.requests(), 1);
    assert!(!gateway.stop().await.contains(&RECONCILIATION_FAILED.into()));
}

/// Every scenario that has to outlast crabber's 30-second lease, run together so the suite
/// waits for it once. Each one is reported by name, so one failure does not hide the others.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphaned_runs_settle_once_their_lease_expires() {
    type Scenario = std::pin::Pin<Box<dyn Future<Output = ()>>>;
    let scenarios: [(&str, Scenario); 12] = [
        (
            "g2a_crabber_settles_orphans_as_reconciliation_assumes",
            Box::pin(g2a_crabber_settles_orphans_as_reconciliation_assumes()),
        ),
        (
            "an_unstarted_orphan_is_recovered_and_completes",
            Box::pin(an_unstarted_orphan_is_recovered_and_completes()),
        ),
        (
            "an_unstarted_cancelled_orphan_is_abandoned_and_cancelled",
            Box::pin(an_unstarted_cancelled_orphan_is_abandoned_and_cancelled()),
        ),
        (
            "an_unstarted_orphan_under_changed_instructions_is_abandoned_and_uncertain",
            Box::pin(an_unstarted_orphan_under_changed_instructions_is_abandoned_and_uncertain()),
        ),
        (
            "a_stalled_recovery_outlasts_a_graceful_stop_until_it_is_aborted",
            Box::pin(a_stalled_recovery_outlasts_a_graceful_stop_until_it_is_aborted()),
        ),
        (
            "a_requeued_request_whose_admission_had_committed_is_recovered",
            Box::pin(a_requeued_request_whose_admission_had_committed_is_recovered()),
        ),
        (
            "a_requeued_request_admitted_under_other_instructions_is_abandoned_and_uncertain",
            Box::pin(
                a_requeued_request_admitted_under_other_instructions_is_abandoned_and_uncertain(),
            ),
        ),
        (
            "a_started_orphan_stays_uncertain_when_its_settlement_is_lost",
            Box::pin(a_started_orphan_stays_uncertain_when_its_settlement_is_lost()),
        ),
        (
            "a_run_whose_owner_is_alive_is_not_settled",
            Box::pin(a_run_whose_owner_is_alive_is_not_settled()),
        ),
        (
            "a_started_orphan_is_retired_and_uncertain",
            Box::pin(a_started_orphan_is_retired_and_uncertain(
                Killed::DuringModelRequest,
                false,
            )),
        ),
        (
            "a_started_cancelled_orphan_after_a_tool_call_is_retired_and_uncertain",
            Box::pin(a_started_orphan_is_retired_and_uncertain(
                Killed::AfterToolCall,
                true,
            )),
        ),
        (
            "a_started_orphan_revoked_by_an_earlier_reconciliation_is_retired",
            Box::pin(a_started_orphan_revoked_by_an_earlier_reconciliation_is_retired()),
        ),
    ];
    let outcomes = futures::future::join_all(scenarios.map(|(name, scenario)| async move {
        let outcome = AssertUnwindSafe(scenario).catch_unwind().await;
        outcome.is_err().then_some(name)
    }));
    let failed: Vec<_> = outcomes.await.into_iter().flatten().collect();
    assert!(failed.is_empty(), "failed scenarios: {failed:?}");
}
