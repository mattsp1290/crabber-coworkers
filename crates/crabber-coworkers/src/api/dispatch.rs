//! Runs accepted requests: one task per conversation takes its requests in `seq` order, runs at
//! most one turn at a time under the shared turn permits, and first settles any request an
//! earlier process left unsettled.
//!
//! The request row in `api.sqlite` is the gateway's only durable state. A task that cannot write
//! or read it, or cannot learn what crabber holds for a request, parks: it leaves the row as it is
//! and exits, and the next wake or the next process start settles the row from crabber's state.
//! It never guesses a status.
//!
//! What crabber durably holds for a request is final only while this process has no admission
//! for it still in flight. Crabber goes on preparing an admission whose waiter gave up, so an
//! admission that outlasts its turn is kept alive by a task of its own, its request is recorded
//! as outstanding, and the conversation parks until crabber has answered: the request is not
//! settled, requeued, or admitted again before then. That task also keeps the turn's permit,
//! because the admission may yet start a run, and the gateway store with its lock, so no other
//! dispatcher on the state directory takes the request up. A stop does not end it. Only
//! crabber's answer or an abort does. A process that ends first takes its admissions with it,
//! which is why the next process may trust what it finds.
//!
//! A request is likewise settled only as crabber reports its run to have ended. A turn that is
//! cancelled, stopped, or out of time interrupts its run and then reports what the run did,
//! and a run completes when the interrupt comes after the model's last answer.
use super::{
    contract::{ApiUuid, CoworkerId, FailureReason},
    store::{
        ApiStore, Finished, MarkedRunning, Outcome, RequestRow, RequestState, Requeued, Retiring,
        Seq, StoreFault,
    },
};
use crate::{
    ask::{
        self, AskError, Cancel, KeyedOutcome, KeyedRequest, Orphan, Outstanding, Reconciled,
        Recovery, Unreadable, Unsettled,
    },
    config::{Config, CoworkerDefinition},
    execution::Remote,
    turn::{Backend, LocalState, TurnPermits},
};
use crabber::{AdmissionKey, Selection, providers::Resolver};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    future::Future,
    sync::{Arc, Mutex, MutexGuard, PoisonError, Weak},
    time::Duration,
};
use time::OffsetDateTime;
use tokio::{
    sync::{OwnedSemaphorePermit, mpsc, watch},
    task::JoinSet,
    time::Instant,
};

pub const STORE_WRITE_FAILED: &str = "API store write failed";
pub const STORE_READ_FAILED: &str = "API store read failed";
pub const RECONCILIATION_FAILED: &str = "API reconciliation failed";
/// Logged each time a conversation parks behind a request whose admission crabber has not
/// answered.
pub const ADMISSION_OUTSTANDING: &str = "API admission outstanding";

/// Retries of a settlement write after its first failure.
pub const FINISH_RETRIES: usize = 3;
const RETRY_SPACING: Duration = Duration::from_secs(1);
/// Times a request is reconciled again after another settlement raced this one.
const RACE_RETRIES: usize = 3;
/// Times one conversation task returns the same request to the queue. A request goes back when
/// crabber holds no admission for it; one whose admission keeps ending without a verdict would
/// otherwise be retried forever.
const REQUEUE_LIMIT: usize = 3;
/// Crabber judges lease expiry by its store clock, so settlement waits this long past it.
const LEASE_MARGIN: time::Duration = time::Duration::seconds(1);

/// What the dispatcher shares with the rest of `serve`.
pub struct Wiring {
    pub store: ApiStore,
    pub backend: Arc<Backend>,
    pub permits: TurnPermits,
    pub config: Arc<Config>,
    /// Local state by coworker ID, from [`crate::turn::open_all`].
    pub locals: HashMap<String, Arc<LocalState>>,
    /// Becomes `true` when the service stops. Running turns are interrupted.
    pub stop: watch::Receiver<bool>,
    /// Becomes `true` when the service stops taking work, before or without a stop. Turns that
    /// are running finish, and no request starts after it: a conversation task exits instead of
    /// reading its next head, a request still waiting for a turn stays `accepted` for the next
    /// process, and a wake starts nothing. A stop implies it.
    pub intake: watch::Receiver<bool>,
    /// Receives fixed operator log lines.
    pub log: mpsc::UnboundedSender<String>,
}

#[derive(Clone)]
pub struct Dispatcher(Arc<Inner>);

struct Inner {
    /// This dispatcher, for the tasks it spawns from inside a conversation task.
    me: Weak<Inner>,
    wiring: Wiring,
    /// Conversations that have a task. Membership is what makes a conversation's task unique.
    conversations: Mutex<HashMap<ApiUuid, Conversation>>,
    /// Requests whose admission this process asked crabber for, gave up waiting on, and keeps
    /// alive in a task of its own. Crabber's durable state for such a request is not final, so
    /// no conversation task acts on it. A request leaves the set when crabber has answered.
    outstanding: Mutex<HashSet<Seq>>,
    /// Conversation tasks, and the tasks that keep outstanding admissions alive.
    tasks: Mutex<JoinSet<()>>,
}

struct Conversation {
    wake: Wake,
    turn: Option<ActiveTurn>,
}

/// Whether a wake arrived after the task last read the conversation's head. A task exits only
/// while this is `Seen`, under the same lock that `wake` takes, so no wake is lost.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Wake {
    Seen,
    Pending,
}

/// The request a task is running or reconciling, and the signal that interrupts it.
struct ActiveTurn {
    seq: Seq,
    cancel: watch::Sender<bool>,
}

/// What a task does after handling its conversation's head request.
enum Step {
    /// Read the head again.
    Next,
    /// Leave the head as it is and exit, unless a wake is already pending.
    Park,
    /// The service is stopping, or its intake closed: exit and leave the head as it is.
    Stop,
}

/// How a turn ended, before the store decides what an interruption was.
enum Verdict {
    Settle(Outcome),
    /// The request's run was interrupted. [`ApiStore::interrupt`] decides, in the transaction
    /// that settles it, whether that was reconciliation, the client's cancellation, or a stop.
    Interrupted,
}

impl Verdict {
    fn failed(reason: FailureReason) -> Self {
        Self::Settle(Outcome::Failed(reason))
    }

    fn uncertain(reason: FailureReason) -> Self {
        Self::Settle(Outcome::Uncertain(reason))
    }

    /// The worker runs unkeyed turns and reports an interruption, timeout or failure without
    /// seeing its run end, so the run may still have completed there.
    fn from_borb(result: Result<String, AskError>) -> Self {
        match result {
            Err(
                error @ (AskError::Interrupted
                | AskError::Timeout
                | AskError::Run
                | AskError::NotCompleted),
            ) => Self::uncertain(FailureReason::from(&error)),
            result => Self::from(result),
        }
    }
}

/// The verdict on a turn whose backend was invoked and gave a definitive result.
impl From<Result<String, AskError>> for Verdict {
    fn from(result: Result<String, AskError>) -> Self {
        match result {
            Ok(text) => Self::Settle(Outcome::Completed(text)),
            Err(AskError::Interrupted) => Self::Interrupted,
            Err(error) => Self::failed(FailureReason::from(&error)),
        }
    }
}

/// What a settlement write left behind.
enum Written {
    /// The request is settled, by this write or by an earlier one.
    Settled,
    /// The verdict did not apply and the request is still active.
    Kept,
    /// The store never confirmed a write.
    Lost,
}

/// What a turn of this process learned about a request that it leaves to reconciliation.
#[derive(Default)]
struct Turned {
    /// Crabber refused the request's key as retained with another text or configuration.
    refused: bool,
    /// Why the turn gave up a run that it started and did not see end.
    gave_up: Option<AskError>,
}

impl Turned {
    /// The verdict on a run that crabber shows interrupted. A turn that gave the run up at its
    /// deadline timed out. What any other interruption was is for the store to decide.
    fn interrupted(&self) -> Verdict {
        Verdict::from(Err(self.gave_up.clone().unwrap_or(AskError::Interrupted)))
    }
}

/// How often this conversation task has returned its current head to the queue.
#[derive(Default)]
struct Requeues {
    head: Option<Seq>,
    count: usize,
}

impl Requeues {
    /// Count one more requeue of `seq` and return how many came before it, or `None` when it
    /// has had [`REQUEUE_LIMIT`].
    fn another(&mut self, seq: Seq) -> Option<usize> {
        if self.head != Some(seq) {
            *self = Self {
                head: Some(seq),
                count: 0,
            };
        }
        let earlier = self.count;
        (earlier < REQUEUE_LIMIT).then(|| {
            self.count += 1;
            earlier
        })
    }
}

/// How this process executes one coworker's turns.
enum Runner<'a> {
    /// The conversation's coworker is no longer configured.
    Removed,
    /// Configured, but this process opened no local state for it.
    Unavailable,
    Ready(Engine<'a>),
}

enum Engine<'a> {
    Local(LocalEngine<'a>),
    Borb {
        def: &'a CoworkerDefinition,
        remote: &'a Remote,
    },
}

struct LocalEngine<'a> {
    def: &'a CoworkerDefinition,
    local: &'a LocalState,
    resolver: &'a Arc<dyn Resolver>,
    selection: &'a Selection,
}

impl LocalEngine<'_> {
    /// `row` as crabber knows it: session `name`, admission key the request ID.
    fn request<'r>(&'r self, row: &'r RequestRow, name: &'r str) -> KeyedRequest<'r> {
        KeyedRequest {
            def: self.def,
            workspace: &self.local.workspace,
            resolver: Arc::clone(self.resolver),
            selection: self.selection.clone(),
            store: self.local.store.clone(),
            conversation: name,
            key: AdmissionKey::new(row.request_id().to_string())
                .expect("a hyphenated UUID is a valid admission key"),
            text: row.text(),
        }
    }
}

/// One turn's interrupt, set by a cancellation request and never cleared.
struct Interrupt {
    set: watch::Sender<bool>,
    seen: watch::Receiver<bool>,
}

impl Interrupt {
    fn new() -> Self {
        let (set, seen) = watch::channel(false);
        Self { set, seen }
    }

    fn requested(&self) -> bool {
        *self.seen.borrow()
    }

    fn request(&self) {
        self.set.send_replace(true);
    }

    /// Resolves once cancellation is requested.
    async fn wait(&self) {
        let mut seen = self.seen.clone();
        if seen.wait_for(|requested| *requested).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// A task's membership in `conversations`, given up on every exit, including a panic or an abort.
struct Seat<'a> {
    inner: &'a Inner,
    conversation_id: ApiUuid,
    held: bool,
}

impl Seat<'_> {
    /// Mark every earlier wake as seen. The head read that follows observes their requests.
    fn observe(&self) {
        if let Some(conversation) = self.inner.conversations().get_mut(&self.conversation_id) {
            conversation.wake = Wake::Seen;
        }
    }

    /// Give up the seat unless a wake arrived since [`Self::observe`]. `false` means stay.
    fn release(&mut self) -> bool {
        let mut conversations = self.inner.conversations();
        let pending = conversations
            .get(&self.conversation_id)
            .is_some_and(|conversation| conversation.wake == Wake::Pending);
        if !pending {
            conversations.remove(&self.conversation_id);
            self.held = false;
        }
        !pending
    }
}

impl Drop for Seat<'_> {
    fn drop(&mut self) {
        if self.held {
            self.inner.conversations().remove(&self.conversation_id);
        }
    }
}

/// What the task that keeps an outstanding admission alive holds besides the admission and its
/// turn, until crabber answers or the task is aborted: the request's place in `outstanding`,
/// and the gateway store with its lock.
struct Kept {
    inner: Arc<Inner>,
    /// Held for its lock. `inner` holds the store as well, and this says that the task must.
    _store: ApiStore,
    conversation_id: ApiUuid,
    seq: Seq,
    answered: bool,
}

impl Kept {
    /// Crabber answered the admission, and what followed from its answer is done.
    fn answered(mut self) {
        self.answered = true;
    }
}

impl Drop for Kept {
    /// Give the request back to its conversation: once crabber has answered, and also when the
    /// task panicked, because nothing waits for crabber's answer after that and the request
    /// would stay parked for as long as the process lives. An aborted task leaves the request
    /// outstanding. The dispatcher is ending, and the next process reconciles.
    fn drop(&mut self) {
        if !self.answered && !std::thread::panicking() {
            return;
        }
        self.inner.outstanding().remove(&self.seq);
        Dispatcher(self.inner.clone()).wake(self.conversation_id);
    }
}

impl Dispatcher {
    #[must_use]
    pub fn new(wiring: Wiring) -> Self {
        Self(Arc::new_cyclic(|me| Inner {
            me: me.clone(),
            wiring,
            conversations: Mutex::default(),
            outstanding: Mutex::default(),
            tasks: Mutex::default(),
        }))
    }

    /// Make sure the conversation's requests get run: start its task, or tell the live one to
    /// read its head again. Call it after every committed submission. Once intake has closed it
    /// starts nothing.
    pub fn wake(&self, conversation_id: ApiUuid) {
        if self.0.closed() {
            return;
        }
        match self.0.conversations().entry(conversation_id) {
            Entry::Occupied(mut conversation) => conversation.get_mut().wake = Wake::Pending,
            Entry::Vacant(seat) => {
                seat.insert(Conversation {
                    wake: Wake::Seen,
                    turn: None,
                });
                let inner = self.0.clone();
                let mut tasks = self.0.tasks();
                while tasks.try_join_next().is_some() {}
                tasks.spawn(async move { inner.converse(conversation_id).await });
            }
        }
    }

    /// Wake every conversation that has unsettled or queued requests. Call it once at startup,
    /// before accepting submissions: reconciliation then runs inside the conversation tasks.
    ///
    /// # Errors
    /// [`StoreFault`] when the store cannot list them.
    pub async fn wake_pending(&self) -> Result<(), StoreFault> {
        for (conversation_id, _) in self.0.wiring.store.conversations_with_work().await? {
            self.wake(conversation_id);
        }
        Ok(())
    }

    /// Record a cancellation request and interrupt the request's turn if it is running here.
    /// Returns the row as it now stands, or `None` for an unknown identity.
    ///
    /// # Errors
    /// [`StoreFault`] when the request could not be recorded; nothing was interrupted.
    pub async fn cancel(
        &self,
        conversation_id: ApiUuid,
        request_id: ApiUuid,
    ) -> Result<Option<RequestRow>, StoreFault> {
        let now = OffsetDateTime::now_utc();
        let row = self
            .0
            .wiring
            .store
            .request_cancel(conversation_id, request_id, now)
            .await?;
        if let Some(row) = &row {
            // The task that holds the request is told whatever this call made of it. A request
            // still waiting for a turn permit was settled here, and its task stops waiting.
            let signalled = self
                .0
                .conversations()
                .get(&conversation_id)
                .and_then(|conversation| conversation.turn.as_ref())
                .filter(|turn| turn.seq == row.seq())
                .map(|turn| turn.cancel.send_replace(true))
                .is_some();
            if !signalled && *row.state() == RequestState::CancellationRequested {
                // No task holds the request: it is parked or waiting behind another one.
                self.wake(conversation_id);
            }
        }
        Ok(row)
    }

    /// Wait until no conversation task is left, and no task keeping an admission alive. Once
    /// intake closes the conversation tasks end as their running turns settle, and after the
    /// stop signal once every running turn has been interrupted and settled. A task keeping an admission alive ends only when crabber answers, which
    /// may be never, so a caller that stops the service bounds this wait by its stop budget.
    /// Dropping this future aborts the tasks it was waiting for.
    pub async fn join(&self) {
        loop {
            let mut tasks = std::mem::take(&mut *self.0.tasks());
            if tasks.is_empty() {
                return;
            }
            while tasks.join_next().await.is_some() {}
        }
    }

    /// Admissions this process stopped waiting for that crabber has not answered yet. Each holds
    /// a turn permit, and an aborted one stays counted.
    #[must_use]
    pub fn outstanding_admissions(&self) -> usize {
        self.0.outstanding().len()
    }

    /// Abort every conversation task, and every task keeping an outstanding admission alive,
    /// without settling anything. An aborted admission is no longer waited for, a run it
    /// starts late is not interrupted, and its task's hold on the gateway store's lock ends, so
    /// this is for a process about to exit: the next process reconciles. Tasks that a pending
    /// [`Self::join`] is waiting for are aborted by dropping that wait, not by this.
    pub fn abort(&self) {
        self.0.tasks().abort_all();
    }
}

#[cfg(test)]
impl Dispatcher {
    /// Leave `row`'s admission outstanding behind a wait that never ends, as a turn leaves an
    /// admission it gave up that crabber never answers. `row` must be `running` on a coworker
    /// this dispatcher runs locally.
    pub(crate) async fn strand(&self, row: &RequestRow) {
        let inner = &self.0;
        let conversation = inner.wiring.store.conversation(row.conversation_id()).await;
        let coworker = conversation.unwrap().unwrap().coworker_id;
        let Runner::Ready(Engine::Local(local)) = inner.runner(&coworker) else {
            panic!("the request's coworker runs locally");
        };
        let name = row.conversation_id().to_string();
        let admission = Outstanding::asked(
            &local.request(row, &name),
            std::future::pending(),
            AskError::Interrupted,
        );
        let turn = inner.wiring.permits.acquire().await;
        inner.outlive(row, admission, turn);
    }
}

async fn stop_signal(mut stop: watch::Receiver<bool>) {
    // A dropped sender means the service is gone, which also stops turns.
    let _ = stop.wait_for(|stopped| *stopped).await;
}

impl Inner {
    fn conversations(&self) -> MutexGuard<'_, HashMap<ApiUuid, Conversation>> {
        self.conversations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn outstanding(&self) -> MutexGuard<'_, HashSet<Seq>> {
        self.outstanding
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn tasks(&self) -> MutexGuard<'_, JoinSet<()>> {
        self.tasks.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn stopped(&self) -> bool {
        *self.wiring.stop.borrow()
    }

    async fn stop(&self) {
        stop_signal(self.wiring.stop.clone()).await;
    }

    /// Whether intake has closed: no request may start.
    fn closed(&self) -> bool {
        self.stopped() || *self.wiring.intake.borrow()
    }

    /// Resolves once intake closes. Only for waits before a request starts, or between
    /// requests, where giving up leaves the request as it is.
    async fn closing(&self) {
        tokio::select! {
            () = stop_signal(self.wiring.stop.clone()) => {},
            () = stop_signal(self.wiring.intake.clone()) => {},
        }
    }

    /// `work`, given up when the service stops. Only for work that leaves its request safe for
    /// the next process wherever it is dropped: a read, or a settlement crabber can be asked for
    /// again.
    async fn unless_stopped<T>(&self, work: impl Future<Output = T>) -> Option<T> {
        tokio::select! {
            biased;
            () = self.stop() => None,
            done = work => Some(done),
        }
    }

    fn log(&self, line: &'static str) {
        let _ = self.wiring.log.send(line.into());
    }

    fn park(&self, line: &'static str) -> Step {
        self.log(line);
        Step::Park
    }

    /// A turn's cancel future: the service stopping, or this request's cancellation.
    fn interruption(&self, interrupt: &Interrupt) -> Cancel {
        let stop = self.wiring.stop.clone();
        let mut seen = interrupt.seen.clone();
        Box::pin(async move {
            tokio::select! {
                () = stop_signal(stop) => {},
                Ok(_) = seen.wait_for(|requested| *requested) => {},
            }
        })
    }

    fn runner(&self, coworker: &CoworkerId) -> Runner<'_> {
        let Some(def) = self.wiring.config.coworker(coworker.as_str()) else {
            return Runner::Removed;
        };
        match self.wiring.backend.as_ref() {
            Backend::Borb(remote) => Runner::Ready(Engine::Borb { def, remote }),
            Backend::Local {
                resolver,
                selection,
            } => match self.wiring.locals.get(&def.id) {
                Some(local) => Runner::Ready(Engine::Local(LocalEngine {
                    def,
                    local,
                    resolver,
                    selection,
                })),
                None => Runner::Unavailable,
            },
        }
    }

    async fn converse(&self, conversation_id: ApiUuid) {
        let mut seat = Seat {
            inner: self,
            conversation_id,
            held: true,
        };
        let store = &self.wiring.store;
        let coworker = match store.conversation(conversation_id).await {
            Ok(Some(conversation)) => conversation.coworker_id,
            Ok(None) => return,
            Err(StoreFault) => return self.log(STORE_READ_FAILED),
        };
        let runner = self.runner(&coworker);
        let mut requeues = Requeues::default();
        while !self.closed() {
            seat.observe();
            let step = match store.next_work(conversation_id).await {
                Ok(Some(head)) => self.head(&runner, head, &mut requeues).await,
                Ok(None) => Step::Park,
                Err(StoreFault) => self.park(STORE_READ_FAILED),
            };
            let exit = match step {
                Step::Next => false,
                Step::Park => seat.release(),
                Step::Stop => true,
            };
            if exit {
                return;
            }
        }
    }

    /// Keep `admission` alive now that its turn was given up, and leave `row` alone until
    /// crabber answers. The admission can still commit and start a run, so until then the
    /// request stays as it is, whatever crabber's durable state says, and `turn` stays taken:
    /// crabber starts a run before it hands the run over, so the run can reach the model
    /// before it is interrupted. A run that does start is interrupted at once. Then the
    /// conversation is woken to settle the request from crabber's state, which is final by
    /// then.
    ///
    /// A stopping service keeps the admission as well, so a run that starts late is still
    /// interrupted and the gateway store's lock is not released under it. Crabber may never
    /// answer, and then only an abort ends the wait.
    fn outlive(
        &self,
        row: &RequestRow,
        admission: Outstanding,
        turn: OwnedSemaphorePermit,
    ) -> Step {
        let Some(inner) = self.me.upgrade() else {
            return Step::Stop;
        };
        let (conversation_id, seq) = (row.conversation_id(), row.seq());
        let kept = Kept {
            inner,
            _store: self.wiring.store.clone(),
            conversation_id,
            seq,
            answered: false,
        };
        self.outstanding().insert(seq);
        self.tasks().spawn(async move {
            // Owned by the task's body from its first poll, so it is dropped as the body
            // unwinds if the wait below panics.
            let kept = kept;
            let ran = admission.resolve().await;
            // Crabber has answered, and a run it started was stopped or has ended.
            drop(turn);
            if let Some(result) = ran {
                // The turn ends as it would have had crabber answered within its grace. While
                // the request is outstanding no conversation task touches it.
                kept.inner.settle(seq, Verdict::from(result)).await;
            }
            kept.answered();
        });
        self.park(ADMISSION_OUTSTANDING)
    }

    /// Handle the conversation's head request while its turn is registered for cancellation.
    async fn head(&self, runner: &Runner<'_>, head: RequestRow, requeues: &mut Requeues) -> Step {
        if self.outstanding().contains(&head.seq()) {
            // The task that keeps the request's admission alive wakes the conversation.
            return self.park(ADMISSION_OUTSTANDING);
        }
        let interrupt = Interrupt::new();
        let conversation_id = head.conversation_id();
        // Registered before the request is started, so a cancellation that lands at any later
        // point finds the turn to interrupt.
        if let Some(conversation) = self.conversations().get_mut(&conversation_id) {
            conversation.turn = Some(ActiveTurn {
                seq: head.seq(),
                cancel: interrupt.set.clone(),
            });
        }
        let step = match head.state() {
            RequestState::Accepted => self.start(runner, &head, &interrupt, requeues).await,
            RequestState::Running | RequestState::CancellationRequested => {
                self.reconcile(runner, &head, &interrupt, requeues).await
            }
            // The store returns only active requests as a head.
            RequestState::Settled(_) => Step::Park,
        };
        if let Some(conversation) = self.conversations().get_mut(&conversation_id) {
            conversation.turn = None;
        }
        step
    }

    async fn start(
        &self,
        runner: &Runner<'_>,
        head: &RequestRow,
        interrupt: &Interrupt,
        requeues: &mut Requeues,
    ) -> Step {
        let seq = head.seq();
        let engine = match runner {
            // A queued request never ran, so it fails outright.
            Runner::Removed => {
                return self
                    .settle(seq, Verdict::failed(FailureReason::CoworkerRemoved))
                    .await;
            }
            Runner::Unavailable => {
                return self
                    .settle(seq, Verdict::failed(FailureReason::WorkspaceUnavailable))
                    .await;
            }
            Runner::Ready(engine) => engine,
        };
        // The permit comes before the request is marked `running`, so `running` means its
        // backend was about to be invoked. A request that only waited for a turn is still
        // `accepted` and runs after a stop, a closed intake, or a crash, on either backend.
        let permit = tokio::select! {
            biased;
            () = self.closing() => return Step::Stop,
            // The request was cancelled while it waited, which settled it, or an earlier write
            // had started it after all and it now has a turn to interrupt. The next head read
            // sees which.
            () = interrupt.wait() => return Step::Next,
            permit = self.wiring.permits.acquire() => permit,
        };
        let row = match self.wiring.store.mark_running(seq).await {
            Ok(MarkedRunning::Started(row) | MarkedRunning::AlreadyStarted(row)) => row,
            Ok(MarkedRunning::Settled(_)) => return Step::Next,
            Err(StoreFault) => return self.park(STORE_WRITE_FAILED),
        };
        // A cancellation that landed after this turn registered set the interrupt; one that
        // followed an earlier write's start shows in the row. Nothing was invoked for either.
        if interrupt.requested() || *row.state() == RequestState::CancellationRequested {
            // Nothing will run, so the turn is free for another conversation while this
            // settlement is written and, if the store fails, retried.
            drop(permit);
            return self.settle(seq, Verdict::Interrupted).await;
        }
        // Intake can close while the start is being written. Nothing was invoked yet, so the
        // request goes back to the queue for the next process on either backend.
        if self.closed() {
            drop(permit);
            return match self.wiring.store.requeue(seq).await {
                Ok(Requeued::Kept(_)) => self.settle(seq, Verdict::Interrupted).await,
                Ok(Requeued::Requeued(_) | Requeued::AlreadyAccepted(_)) | Err(StoreFault) => {
                    Step::Stop
                }
            };
        }
        let deadline =
            Instant::now() + Duration::from_secs(self.wiring.config.limits.model_turn_seconds);
        let cancel = self.interruption(interrupt);
        let name = row.conversation_id().to_string();
        let (request, outcome) = match engine {
            Engine::Local(local) => {
                let request = local.request(&row, &name);
                let outcome = ask::ask_keyed(&request, deadline, cancel).await;
                (request, outcome)
            }
            Engine::Borb { def, remote } => {
                let answer = remote.answer(def, &name, row.text(), deadline, cancel);
                let verdict = Verdict::from_borb(answer.await);
                drop(permit);
                return self.settle(seq, verdict).await;
            }
        };
        let ended = match outcome {
            Ok(KeyedOutcome::Answered(text)) => Ok(Verdict::from(Ok(text))),
            // The key holds an admission made with another configuration, which this turn did
            // not execute. What became of that admission decides, and one that never began
            // holds the conversation's session until it is abandoned.
            Ok(KeyedOutcome::ConfigurationChanged) => Err(Turned {
                refused: true,
                gave_up: None,
            }),
            // Its cancellation was requested, and nothing ran.
            Ok(KeyedOutcome::NotAdmitted | KeyedOutcome::KeyRetired) if interrupt.requested() => {
                Ok(Verdict::Interrupted)
            }
            // The service is stopping and crabber holds nothing for the request, which stays
            // `running`: the next process finds no admission and returns it to the queue.
            Ok(KeyedOutcome::NotAdmitted) => return Step::Stop,
            // Nothing ran, and nothing can under the request's key: the client resubmits under
            // a new request ID.
            Ok(KeyedOutcome::KeyRetired) => Ok(Verdict::failed(FailureReason::ModelFailed)),
            // Crabber may hold an admission that this turn did not execute, or a run that
            // this turn did not see end.
            Ok(KeyedOutcome::Unsettled(gave_up)) => Err(Turned {
                refused: false,
                gave_up,
            }),
            // Crabber may still start the turn's run, so the turn stays taken.
            Ok(KeyedOutcome::Outstanding(admission)) => {
                return self.outlive(&row, admission, permit);
            }
            Err(error) => Ok(Verdict::from(Err(error))),
        };
        // The turn is free for another conversation while the request is settled or
        // reconciled. It is not held for a run that was interrupted and not seen to end.
        drop(permit);
        match ended {
            Ok(verdict) => self.settle(seq, verdict).await,
            Err(turned) => {
                self.reconcile_local(&request, &row, interrupt, requeues, turned)
                    .await
            }
        }
    }

    /// Settle a request that an earlier process, or an earlier parked task, left `running` or
    /// `cancellation_requested`.
    async fn reconcile(
        &self,
        runner: &Runner<'_>,
        head: &RequestRow,
        interrupt: &Interrupt,
        requeues: &mut Requeues,
    ) -> Step {
        let seq = head.seq();
        // The head was read before this turn registered: a cancellation since then found no
        // turn to interrupt, so the durable flag is read once more.
        match self.wiring.store.cancel_requested(seq).await {
            Ok(true) => interrupt.request(),
            Ok(false) => {}
            Err(StoreFault) => return self.park(STORE_READ_FAILED),
        }
        let name = head.conversation_id().to_string();
        let request = match runner {
            Runner::Removed => {
                return self
                    .settle(seq, Verdict::uncertain(FailureReason::CoworkerRemoved))
                    .await;
            }
            Runner::Unavailable => return self.park(RECONCILIATION_FAILED),
            // A worker's turn registry is in memory and keyed by IDs the gateway never stored.
            Runner::Ready(Engine::Borb { .. }) => {
                return self
                    .settle(seq, Verdict::uncertain(FailureReason::ProcessStopped))
                    .await;
            }
            Runner::Ready(Engine::Local(local)) => local.request(head, &name),
        };
        self.reconcile_local(&request, head, interrupt, requeues, Turned::default())
            .await
    }

    /// Settle a request from what crabber durably holds under its key. That is final here: the
    /// request has no admission outstanding in this process, and an earlier process took its
    /// own with it. A stop gives up every read and settlement and leaves the request as it is.
    /// `turned` is what a turn of this process learned first, if one led here.
    async fn reconcile_local(
        &self,
        request: &KeyedRequest<'_>,
        row: &RequestRow,
        interrupt: &Interrupt,
        requeues: &mut Requeues,
        mut turned: Turned,
    ) -> Step {
        let seq = row.seq();
        let mut races = 0;
        let mut live = false;
        loop {
            let reconciled = match self.unless_stopped(ask::reconcile_keyed(request)).await {
                None => return Step::Stop,
                Some(Ok(reconciled)) => reconciled,
                Some(Err(Unreadable)) => return self.park(RECONCILIATION_FAILED),
            };
            let verdict = match reconciled {
                Reconciled::NotAdmitted if interrupt.requested() => {
                    Ok(Verdict::Settle(Outcome::Cancelled))
                }
                // Crabber refused the key and holds nothing under it now. Nothing ran, and
                // asking again would only be refused again.
                Reconciled::NotAdmitted if turned.refused => {
                    Ok(Verdict::uncertain(FailureReason::ConfigurationChanged))
                }
                Reconciled::NotAdmitted => return self.requeue(seq, requeues).await,
                Reconciled::Answered(text) => Ok(Verdict::Settle(Outcome::Completed(text))),
                Reconciled::NoAnswer => Ok(Verdict::failed(FailureReason::NoAnswer)),
                Reconciled::Interrupted => Ok(turned.interrupted()),
                Reconciled::Failed => Ok(Verdict::failed(FailureReason::ModelFailed)),
                Reconciled::Unstarted(orphan) => {
                    if !self.outlast(&orphan, live).await {
                        return Step::Stop;
                    }
                    let settling =
                        self.settle_unstarted(request, row, interrupt, &orphan, &mut turned);
                    match settling.await {
                        Ok(settled) => settled,
                        Err(step) => return step,
                    }
                }
                Reconciled::Started(orphan) => {
                    if !self.outlast(&orphan, live).await {
                        return Step::Stop;
                    }
                    // The model was reached and tool effects are unknown, whether or not the
                    // client asked to cancel.
                    let reason = FailureReason::ProcessStopped;
                    if let Err(step) = self.retiring(seq, reason).await {
                        return step;
                    }
                    let retiring = ask::retire_started(request, &orphan);
                    match self.unless_stopped(retiring).await {
                        None => return Step::Stop,
                        Some(retired) => retired.map(|()| Verdict::uncertain(reason)),
                    }
                }
            };
            let refusal = match verdict {
                Ok(verdict) => return self.settle(seq, verdict).await,
                Err(refusal) => refusal,
            };
            let verdict = match refusal {
                Unsettled::LiveLease => {
                    live = true;
                    continue;
                }
                Unsettled::Raced if races < RACE_RETRIES => {
                    races += 1;
                    continue;
                }
                Unsettled::ConfigurationChanged => {
                    Verdict::uncertain(FailureReason::ConfigurationChanged)
                }
                // The session may stay busy: later requests then fail with `conversation_busy`.
                Unsettled::Raced | Unsettled::Unprovable => {
                    self.log(RECONCILIATION_FAILED);
                    Verdict::uncertain(FailureReason::ProcessStopped)
                }
                Unsettled::Unknown => return self.park(RECONCILIATION_FAILED),
            };
            return self.settle(seq, verdict).await;
        }
    }

    /// Settle an admission whose executor never began, once its dead owner's lease has expired:
    /// abandon it when the client asked to cancel, else run it. `Err` is the step to take when
    /// the service stopped or the gateway store failed first, or when crabber has not yet
    /// answered the claim to run it. A run that was claimed and not seen to end is recorded in
    /// `turned`.
    async fn settle_unstarted(
        &self,
        request: &KeyedRequest<'_>,
        row: &RequestRow,
        interrupt: &Interrupt,
        orphan: &Orphan,
        turned: &mut Turned,
    ) -> Result<Result<Verdict, Unsettled>, Step> {
        if interrupt.requested() {
            let abandoning = ask::abandon_unstarted(request, orphan);
            let abandoned = self.unless_stopped(abandoning).await.ok_or(Step::Stop)?;
            return Ok(abandoned.map(|()| Verdict::Settle(Outcome::Cancelled)));
        }
        let recovered = self.recover(request, row, interrupt, turned).await?;
        let Err(Unsettled::ConfigurationChanged) = recovered else {
            return Ok(recovered);
        };
        // Nothing can run the admission as it was made, and left alone it would hold the
        // conversation's session for good. Abandoning it frees the session; the request is
        // uncertain either way.
        let reason = FailureReason::ConfigurationChanged;
        self.retiring(row.seq(), reason).await?;
        let abandoning = ask::abandon_unstarted(request, orphan);
        let abandoned = self.unless_stopped(abandoning).await.ok_or(Step::Stop)?;
        Ok(match abandoned {
            Ok(()) => Ok(Verdict::uncertain(reason)),
            // The session stays busy: later requests then fail with `conversation_busy`.
            Err(Unsettled::Unprovable) => {
                self.log(RECONCILIATION_FAILED);
                Ok(Verdict::uncertain(reason))
            }
            Err(refusal) => Err(refusal),
        })
    }

    /// Record why the request settles `uncertain` before crabber is asked to interrupt its
    /// orphaned run. If the settlement write is then lost, or this process dies, the next
    /// reconciliation finds an interrupted run and this record, not a graceful stop. `Err` is
    /// the step to take when the record could not be made.
    async fn retiring(&self, seq: Seq, reason: FailureReason) -> Result<(), Step> {
        match self.wiring.store.begin_retirement(seq, reason).await {
            Ok(Retiring::Recorded(_)) => Ok(()),
            // The request settled or went back to the queue after it was read.
            Ok(Retiring::Kept(_)) => Err(Step::Next),
            Err(StoreFault) => Err(self.park(STORE_WRITE_FAILED)),
        }
    }

    /// Wait out the dead owner's lease. `false` when intake closed first.
    async fn outlast(&self, orphan: &Orphan, refused_live: bool) -> bool {
        let remaining = orphan.lease_until() + LEASE_MARGIN - OffsetDateTime::now_utc();
        let mut wait = Duration::try_from(remaining).unwrap_or(Duration::ZERO);
        if refused_live {
            wait = wait.max(RETRY_SPACING);
        }
        tokio::select! {
            biased;
            () = self.closing() => false,
            () = tokio::time::sleep(wait) => true,
        }
    }

    /// Run an orphaned admission whose executor never began. `Err` is the step to take when
    /// the service stopped before the turn ran, or crabber has not yet answered the claim. A
    /// run that was claimed and not seen to end is recorded in `turned`.
    async fn recover(
        &self,
        request: &KeyedRequest<'_>,
        row: &RequestRow,
        interrupt: &Interrupt,
        turned: &mut Turned,
    ) -> Result<Result<Verdict, Unsettled>, Step> {
        let permit = tokio::select! {
            biased;
            () = self.closing() => return Err(Step::Stop),
            // Nothing ran: read again and abandon instead.
            () = interrupt.wait() => return Ok(Err(Unsettled::Raced)),
            permit = self.wiring.permits.acquire() => permit,
        };
        let deadline =
            Instant::now() + Duration::from_secs(self.wiring.config.limits.model_turn_seconds);
        let recovery = ask::recover_keyed(request, deadline, self.interruption(interrupt)).await;
        // Every outcome but the last leaves nothing of this turn running, and frees the turn.
        match recovery {
            Recovery::Ran(result) => Ok(Ok(Verdict::from(result))),
            Recovery::Refused(refusal) => Ok(Err(refusal)),
            Recovery::Interrupted if self.stopped() => Err(Step::Stop),
            // Either way nothing ran here, and the run's durable state decides what follows.
            Recovery::Replayed | Recovery::Interrupted => Ok(Err(Unsettled::Raced)),
            // The run's durable state says how it ended, and why its turn gave it up says what
            // an interruption of it was.
            Recovery::Unsettled(gave_up) => {
                turned.gave_up = gave_up;
                Ok(Err(Unsettled::Raced))
            }
            // Crabber may still grant the claim and start the run, so the turn stays taken.
            Recovery::Outstanding(claim) => Err(self.outlive(row, claim, permit)),
        }
    }

    /// Return a request that crabber never admitted to the queue. It runs as the next turn
    /// under the same key, so an admission that commits after all replays instead of running
    /// twice. A request already requeued [`REQUEUE_LIMIT`] times by this task stays `running`
    /// and the task parks: its admission keeps ending without a verdict.
    ///
    /// An admission that fails while crabber prepares it leaves the key's preparation
    /// unretired. The retry under the key is refused for good, and [`KeyedOutcome::KeyRetired`]
    /// settles the request `failed` instead of parking it.
    async fn requeue(&self, seq: Seq, requeues: &mut Requeues) -> Step {
        let Some(earlier) = requeues.another(seq) else {
            return self.park(RECONCILIATION_FAILED);
        };
        if earlier > 0 {
            tokio::select! {
                biased;
                () = self.closing() => return Step::Stop,
                () = tokio::time::sleep(RETRY_SPACING) => {},
            }
        }
        match self.wiring.store.requeue(seq).await {
            // `Kept` means a cancellation arrived, or the request settled, after it was read;
            // the next head read sees which.
            Ok(Requeued::Requeued(_) | Requeued::AlreadyAccepted(_) | Requeued::Kept(_)) => {
                Step::Next
            }
            Err(StoreFault) => self.park(STORE_WRITE_FAILED),
        }
    }

    async fn settle(&self, seq: Seq, verdict: Verdict) -> Step {
        match self.write(seq, &verdict).await {
            Written::Settled => Step::Next,
            Written::Kept | Written::Lost => self.park(STORE_WRITE_FAILED),
        }
    }

    async fn write(&self, seq: Seq, verdict: &Verdict) -> Written {
        let store = &self.wiring.store;
        for attempt in 0..=FINISH_RETRIES {
            if attempt > 0 {
                tokio::select! {
                    biased;
                    () = self.stop() => break,
                    () = tokio::time::sleep(RETRY_SPACING) => {},
                }
            }
            let finished = match verdict {
                Verdict::Settle(outcome) => store.finish(seq, outcome.clone()).await,
                Verdict::Interrupted => store.interrupt(seq).await,
            };
            match finished {
                Ok(Finished::Settled(_)) => return Written::Settled,
                Ok(Finished::Unchanged(row)) => {
                    return match row.state() {
                        RequestState::Settled(_) => Written::Settled,
                        RequestState::Accepted
                        | RequestState::Running
                        | RequestState::CancellationRequested => Written::Kept,
                    };
                }
                Err(StoreFault) => {}
            }
        }
        Written::Lost
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::store::Submitted;

    const CONVERSATION: &str = "00000000-0000-0000-0000-000000000001";

    #[test]
    fn a_borb_turn_the_worker_did_not_see_end_is_uncertain() {
        let settled = |result| match Verdict::from_borb(result) {
            Verdict::Settle(outcome) => Some(outcome),
            Verdict::Interrupted => None,
        };
        for (error, reason) in [
            (AskError::Interrupted, FailureReason::Interrupted),
            (AskError::Timeout, FailureReason::Timeout),
            (AskError::Run, FailureReason::ModelFailed),
            (AskError::NotCompleted, FailureReason::ModelFailed),
        ] {
            assert!(matches!(settled(Err(error)), Some(Outcome::Uncertain(got)) if got == reason));
        }
        assert!(matches!(
            settled(Err(AskError::Busy)),
            Some(Outcome::Failed(FailureReason::ConversationBusy))
        ));
        assert!(
            matches!(settled(Ok("answer".into())), Some(Outcome::Completed(text)) if text == "answer")
        );
    }

    const REQUEST: &str = "00000000-0000-0000-0000-00000000000a";

    struct Fixture {
        _directory: tempfile::TempDir,
        dispatcher: Dispatcher,
        provider: Arc<crabber::FakeProvider>,
        stop: watch::Sender<bool>,
    }

    impl Fixture {
        /// A dispatcher and the accepted request its task would read as the conversation's head.
        async fn queued() -> (Self, RequestRow) {
            Self::answering(&[]).await
        }

        /// [`Self::queued`], with a model that gives `answers` in order.
        async fn answering(answers: &[&str]) -> (Self, RequestRow) {
            let directory = tempfile::tempdir().unwrap();
            let mut config: Config =
                serde_json::from_str(include_str!("../../../../config.example.json")).unwrap();
            config.state_dir = directory.path().to_path_buf();
            for def in &mut config.coworkers {
                def.workspace = crate::config::WorkspaceAccess::Disabled;
            }
            let answers = answers.iter().map(|answer| {
                vec![
                    crabber::StreamDelta::TextDelta((*answer).into()),
                    crabber::StreamDelta::Completed,
                ]
            });
            let provider = Arc::new(crabber::FakeProvider::scripted(answers.collect()));
            let backend = Backend::Local {
                resolver: provider.clone(),
                selection: Selection {
                    provider_id: "fake".into(),
                    model_id: "scripted".into(),
                },
            };
            let mut never: Cancel = Box::pin(std::future::pending());
            let locals = crate::turn::open_all(&config, &backend, &mut never)
                .await
                .unwrap();
            let store = ApiStore::open(directory.path()).await.unwrap();
            let (conversation, request) = (
                ApiUuid::parse(CONVERSATION).unwrap(),
                ApiUuid::parse(REQUEST).unwrap(),
            );
            let scout = CoworkerId::parse("scout").unwrap();
            store.open_conversation(conversation, &scout).await.unwrap();
            let submitted = store.submit(conversation, request, "synthetic request", 16);
            let Submitted::Inserted(head) = submitted.await.unwrap() else {
                panic!("a new request is inserted");
            };
            let (stop, stopped) = watch::channel(false);
            let (log, _) = mpsc::unbounded_channel();
            let dispatcher = Dispatcher::new(Wiring {
                store,
                backend: Arc::new(backend),
                permits: TurnPermits::new(1),
                config: Arc::new(config),
                locals,
                stop: stopped.clone(),
                intake: stopped,
                log,
            });
            let fixture = Self {
                _directory: directory,
                dispatcher,
                provider,
                stop,
            };
            (fixture, head)
        }

        /// Handle `head` as the conversation task does after reading it.
        async fn handle(&self, head: RequestRow) -> RequestState {
            let inner = &self.dispatcher.0;
            let scout = CoworkerId::parse("scout").unwrap();
            let (conversation, request) = (head.conversation_id(), head.request_id());
            let mut requeues = Requeues::default();
            inner.head(&inner.runner(&scout), head, &mut requeues).await;
            let row = inner.wiring.store.request(conversation, request).await;
            row.unwrap().unwrap().state().clone()
        }

        /// Wait for the request of `row` to become `wanted`.
        async fn reaches(&self, row: &RequestRow, wanted: &RequestState) {
            let store = &self.dispatcher.0.wiring.store;
            let reached = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let found = store.request(row.conversation_id(), row.request_id()).await;
                    if found.unwrap().unwrap().state() == wanted {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
            reached.await.expect("the request reaches the wanted state");
        }
    }

    #[tokio::test]
    async fn a_head_cancelled_before_it_starts_never_runs() {
        let (fixture, head) = Fixture::queued().await;
        let cancelled = fixture
            .dispatcher
            .cancel(head.conversation_id(), head.request_id())
            .await;
        assert_eq!(
            cancelled.unwrap().unwrap().state(),
            &RequestState::Settled(Outcome::Cancelled)
        );

        assert_eq!(
            fixture.handle(head).await,
            RequestState::Settled(Outcome::Cancelled)
        );
        assert_eq!(fixture.provider.requests().len(), 0);
    }

    #[tokio::test]
    async fn a_head_started_by_an_earlier_write_and_then_cancelled_is_interrupted() {
        let (fixture, head) = Fixture::queued().await;
        let store = &fixture.dispatcher.0.wiring.store;
        let started = store.mark_running(head.seq()).await.unwrap();
        assert!(matches!(started, MarkedRunning::Started(_)));
        let requested = fixture
            .dispatcher
            .cancel(head.conversation_id(), head.request_id())
            .await;
        assert_eq!(
            requested.unwrap().unwrap().state(),
            &RequestState::CancellationRequested
        );

        assert_eq!(
            fixture.handle(head).await,
            RequestState::Settled(Outcome::Cancelled)
        );
        assert_eq!(fixture.provider.requests().len(), 0);
    }

    #[tokio::test]
    async fn a_stop_during_reconciliation_does_not_wait_for_crabber() {
        let (fixture, head) = Fixture::queued().await;
        let store = &fixture.dispatcher.0.wiring.store;
        let started = store.mark_running(head.seq()).await.unwrap();
        assert!(matches!(started, MarkedRunning::Started(_)));
        let unsettled = store.next_work(head.conversation_id()).await;
        let unsettled = unsettled.unwrap().unwrap();
        assert_eq!(unsettled.state(), &RequestState::Running);

        // Each of the reads reconciliation makes takes half of `serve`'s whole stop budget.
        let slow = ask::read_faults::slow(Duration::from_secs(10));
        let began = Instant::now();
        let stopping = async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            fixture.stop.send(true).unwrap();
        };
        let (state, ()) = tokio::join!(fixture.handle(unsettled), stopping);
        drop(slow);

        assert!(began.elapsed() < Duration::from_secs(3));
        // Nothing was learned, so nothing was decided: the next process reconciles it.
        assert_eq!(state, RequestState::Running);
        assert_eq!(fixture.provider.requests().len(), 0);
    }

    #[tokio::test]
    async fn a_replayed_run_that_cannot_be_read_is_reconciled_once_it_can() {
        let (fixture, head) = Fixture::answering(&["kept answer"]).await;
        let inner = &fixture.dispatcher.0;
        let scout = CoworkerId::parse("scout").unwrap();
        let Runner::Ready(Engine::Local(local)) = inner.runner(&scout) else {
            panic!("the fixture runs scout locally");
        };
        // The queued request's key already holds a completed run, as when an admission that
        // looked lost had committed after all.
        let name = head.conversation_id().to_string();
        let deadline = Instant::now() + Duration::from_secs(20);
        let never: Cancel = Box::pin(std::future::pending());
        let answered = ask::ask_keyed(&local.request(&head, &name), deadline, never).await;
        assert_eq!(answered, Ok(KeyedOutcome::Answered("kept answer".into())));

        // The key replays, and neither its run nor its admission can be read.
        ask::read_faults::fail(2);
        assert_eq!(fixture.handle(head.clone()).await, RequestState::Running);

        let store = &inner.wiring.store;
        let parked = store.next_work(head.conversation_id()).await.unwrap();
        assert_eq!(
            fixture.handle(parked.unwrap()).await,
            RequestState::Settled(Outcome::Completed("kept answer".into()))
        );
        assert_eq!(fixture.provider.requests().len(), 1);
    }

    #[tokio::test]
    async fn a_completed_turn_whose_answer_cannot_be_read_is_settled_from_crabber() {
        let (fixture, head) = Fixture::answering(&["kept answer"]).await;

        // The run completes, and the turn's read of its answer fails. That says nothing about
        // the run, so the request is settled from what crabber holds and not failed.
        ask::read_faults::fail(1);
        assert_eq!(
            fixture.handle(head).await,
            RequestState::Settled(Outcome::Completed("kept answer".into()))
        );
        assert_eq!(fixture.provider.requests().len(), 1);
    }

    #[tokio::test]
    async fn a_request_whose_outstanding_admission_panics_goes_back_to_its_conversation() {
        let (fixture, head) = Fixture::answering(&["answer"]).await;
        let inner = &fixture.dispatcher.0;
        let started = inner.wiring.store.mark_running(head.seq()).await.unwrap();
        let MarkedRunning::Started(row) = started else {
            panic!("a queued request starts");
        };
        let scout = CoworkerId::parse("scout").unwrap();
        let Runner::Ready(Engine::Local(local)) = inner.runner(&scout) else {
            panic!("the fixture runs scout locally");
        };

        // The request's turn was given up, and the wait for its admission then dies.
        let name = row.conversation_id().to_string();
        let admission = Outstanding::asked(
            &local.request(&row, &name),
            async { panic!("synthetic failure of an outstanding admission") },
            AskError::Interrupted,
        );
        let turn = inner.wiring.permits.acquire().await;
        inner.outlive(&row, admission, turn);

        // Nothing waits for crabber any more, and the request is not parked for good: its
        // conversation finds no admission under its key, returns it to the queue, and runs it.
        let completed = RequestState::Settled(Outcome::Completed("answer".into()));
        fixture.reaches(&row, &completed).await;
        assert!(inner.outstanding().is_empty());
        assert_eq!(inner.wiring.permits.available(), 1);
        assert_eq!(fixture.provider.requests().len(), 1);
    }
}
