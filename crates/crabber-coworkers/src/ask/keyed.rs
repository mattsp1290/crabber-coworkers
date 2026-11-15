//! Keyed admission: at most one crabber run per request key, and what a later process may do
//! with a run whose owner died.
//!
//! Crabber's verdicts are classified here, in exhaustive matches, so a variant added by a new
//! crabber pin fails compilation instead of falling into a default.
//!
//! Crabber goes on preparing an admission whose waiter is dropped, and one that commits then
//! starts a run that nothing waits for or interrupts. So a wait for an admission's verdict is
//! never dropped here. A turn that is given up first keeps waiting for a grace, and after that
//! hands the wait on as an [`Outstanding`] admission.
//!
//! Crabber also stops looking at a run's interrupt once the model has given its last answer, so
//! a run that its turn gave up may complete all the same. A keyed turn therefore stands by a
//! run it interrupts until crabber says how the run ended, and reports that: a completed run's
//! answer, never an interruption the run did not honour. What crabber does not report in time
//! is left to the run's durable state.
use super::{
    Admitted, AskError, Cancel, Host, INTERRUPT_SETTLE_SECONDS, Signal, Turn, TurnError, cleanup,
    committed_text, turn, wait_for_completion,
};
use crate::{bridge, config::CoworkerDefinition, digest::sha256_hex, workspace::Workspace};
use crabber::{
    AbandonAuthority, AbandonError, AbandonRequest, Admission, AdmissionKey, AdmissionOptions,
    Agent, AttemptControlOutcome, AttemptResolverClaim, AttemptRetirement,
    AttemptRetirementOutcome, AttemptUncertainty, AttemptUncertaintyCause, InputFingerprint,
    PendingCallDisposition, RunResult, RuntimeError, Selection, SessionId,
    core::{ControlId, RunFence, RunId, RunStatus},
    providers::Resolver,
    session::{
        AdmissionExecutionError, AdmissionExecutionState, AttemptError, AttemptState, SqliteStore,
        Store, StoreError,
    },
};
use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration};
use time::OffsetDateTime;

/// Bound on one settlement or durable read outside a turn's own deadline.
const CONTROL_SECONDS: u64 = 10;
/// How long a turn that was given up still waits for its admission's verdict before it hands
/// the wait on.
const ADMISSION_GRACE: Duration = Duration::from_secs(CONTROL_SECONDS);
/// How long a turn stands by a run it has interrupted: the wait for crabber to say how the run
/// ended and the read of a completed run's answer, together.
const SETTLE: Duration = Duration::from_secs(INTERRUPT_SETTLE_SECONDS);
const RESOLVER_ID: &str = "crabber-coworkers.api";
const JOINED_CLAIM: &str =
    "the owning serve process released the API store lock and its run lease expired";

/// One request of one durable conversation, identified to crabber by `key`.
pub struct KeyedRequest<'a> {
    pub def: &'a CoworkerDefinition,
    pub workspace: &'a Workspace,
    pub resolver: Arc<dyn Resolver>,
    pub selection: Selection,
    pub store: Arc<SqliteStore>,
    /// The conversation's session name.
    pub conversation: &'a str,
    pub key: AdmissionKey,
    pub text: &'a str,
}

impl KeyedRequest<'_> {
    fn session(&self) -> SessionId {
        SessionId::from(self.conversation)
    }

    fn host(&self) -> Host<'_> {
        Host {
            def: self.def,
            workspace: self.workspace,
            resolver: self.resolver.clone(),
            selection: self.selection.clone(),
            store: self.store.clone(),
        }
    }

    fn agent(&self) -> Result<Agent, AskError> {
        bridge::build_agent(
            self.def,
            self.workspace,
            self.store.clone(),
            self.resolver.clone(),
            self.selection.clone(),
        )
    }
}

/// Crabber rejects any behavior fingerprint but the admitting agent's own.
fn options(agent: &Agent, key: AdmissionKey, text: &str) -> Result<AdmissionOptions, AskError> {
    Ok(AdmissionOptions {
        key,
        fingerprint: InputFingerprint::new(sha256_hex(text.as_bytes()))
            .map_err(|_| AskError::Build)?,
        behavior_fingerprint: agent.behavior_fingerprint().clone(),
    })
}

/// How a keyed turn ended when it did not fail.
#[derive(PartialEq, Eq)]
pub enum KeyedOutcome {
    Answered(String),
    /// The cancel signal fired before crabber was asked to admit. Nothing was admitted, so the
    /// request can run later under the same key.
    NotAdmitted,
    /// This turn has no outcome it can report, and what crabber holds under the key is not
    /// known: the key replayed an admission whose run has not settled or was not read, crabber
    /// refused without a documented guarantee that nothing committed, or crabber did not say
    /// how the run this turn started ended. Crabber has answered the admission, so what it
    /// durably holds under the key is the evidence: the caller reconciles the key
    /// ([`reconcile_keyed`]) and never settles from this alone. `Some` is why the turn gave up
    /// a run it started, which is the error to report if that run proves to be interrupted.
    Unsettled(Option<AskError>),
    /// The turn was given up and crabber has still not answered its admission. The caller must
    /// keep the admission alive until it resolves, and until then must neither settle the
    /// request nor admit it again.
    Outstanding(Outstanding),
    /// The key is retained with another text or another coworker configuration.
    ConfigurationChanged,
    /// Nothing can ever run under the key. An earlier admission under it failed while crabber
    /// was preparing it, which leaves the key's preparation unretired, and crabber refuses every
    /// later admission under the key before restoring a provider or acquiring a plan (crabber
    /// `docs/session-owners.md`). No admission is retained under the key. New work needs a new
    /// key.
    KeyRetired,
}

impl fmt::Debug for KeyedOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Answered(text) => write!(f, "Answered(<{} bytes>)", text.len()),
            Self::NotAdmitted => f.write_str("NotAdmitted"),
            Self::Unsettled(gave_up) => f.debug_tuple("Unsettled").field(gave_up).finish(),
            Self::Outstanding(admission) => f.debug_tuple("Outstanding").field(admission).finish(),
            Self::ConfigurationChanged => f.write_str("ConfigurationChanged"),
            Self::KeyRetired => f.write_str("KeyRetired"),
        }
    }
}

enum Replay {
    Retained(crabber::AdmissionReceipt),
    Conflict,
    /// The key's preparation is unretired and no admission is retained under it.
    KeyRetired,
    /// A verdict that does not prove the admission did not commit.
    Unknown,
    /// The admission started its run, and crabber reported a final status for it that gives the
    /// turn no answer. This is the error the turn reports.
    Ended(AskError),
    /// The admission started its run, and crabber did not say how the run ended. `Some` is why
    /// the turn gave the run up.
    Unsettled(Option<AskError>),
}

/// What crabber's refusal of a keyed admission proves.
enum Refusal {
    /// Another run holds the session. Nothing committed.
    Busy,
    /// The key is retained with other input. Nothing committed.
    Conflict,
    /// The key's original preparation was never retired, so crabber refused before restoring a
    /// provider or acquiring a plan. This attempt committed nothing, and no later one under the
    /// key can. Whether an earlier one did is for the key's receipt to say.
    PreparationUnretired,
    /// Nothing is proven. The admission may have committed and its reply been lost.
    Unknown,
}

/// Only crabber's typed, documented rejections are definitive. Every other error, a sanitized
/// store failure included, can be a committed admission whose reply was lost or whose run
/// failed to spawn (crabber `docs/admission-receipts.md`), so a variant this does not name is
/// deliberately unknown.
fn admission_refusal(error: &RuntimeError) -> Refusal {
    match error {
        RuntimeError::SessionBusy | RuntimeError::Store(StoreError::Busy) => Refusal::Busy,
        RuntimeError::Store(StoreError::AdmissionConflict) => Refusal::Conflict,
        RuntimeError::PreparationIncomplete { source, .. } => match source.as_ref() {
            RuntimeError::PreparationRecoveryRequired => Refusal::PreparationUnretired,
            _ => Refusal::Unknown,
        },
        _ => Refusal::Unknown,
    }
}

/// After crabber refused a key whose preparation is unretired: the key is retired for good only
/// when no admission is retained under it. A retained receipt, or a lookup that fails, does not
/// answer in time, or is given up when `cancel` fires, leaves the key to be reconciled.
async fn retired(
    agent: &Agent,
    session: &SessionId,
    key: &AdmissionKey,
    cancel: &mut Signal,
) -> Replay {
    let looked = tokio::select! {
        biased;
        () = cancel.fired() => return Replay::Unknown,
        looked = bounded(agent.lookup_admission(session, key)) => looked,
    };
    match looked {
        Some(Ok(None)) => Replay::KeyRetired,
        Some(Ok(Some(_)) | Err(_)) | None => Replay::Unknown,
    }
}

/// A keyed admission or recovery that crabber was asked for. It owns its agent, so it can
/// outlive the turn that asked.
type Asked = Pin<Box<dyn Future<Output = Result<Admission, RuntimeError>> + Send>>;

/// An admission that crabber is still working on after the turn that asked for it was given up
/// and its grace ran out.
///
/// Dropping it would not stop crabber: the admission could still commit and start a run that
/// nothing waits for, interrupts, or bounds, while the key's durable state said for a while that
/// nothing was admitted. So it is kept, with the agent it runs on, until [`Self::resolve`] has
/// crabber's verdict. Until then nothing crabber holds under the request's key is final, an
/// absent admission included. Only the process ending makes dropping it safe: the admission's
/// owner is then gone, and crabber fences whatever it leaves behind.
pub struct Outstanding {
    agent: Arc<Agent>,
    store: Arc<SqliteStore>,
    admission: Asked,
    gave_up: AskError,
}

impl Outstanding {
    /// Wait for crabber's verdict for as long as it takes. A run the admission starts after all
    /// is interrupted at once and stood by, and the turn's agent is cleaned up.
    ///
    /// Returns how that run ended, as its turn reports it: the run's answer when it completed
    /// in spite of the interrupt, else the error for the final status crabber reported. `None`
    /// means that only the key's durable state, which is final from here on, says what it
    /// holds.
    pub async fn resolve(mut self) -> Option<Result<String, AskError>> {
        let ran = match self.admission.as_mut().await {
            Ok(Admission::Started { handle, .. }) => {
                match stop(&self.store, handle, self.gave_up).await {
                    Ran::Answered(answer) => Some(Ok(answer)),
                    Ran::Ended(error) => Some(Err(error)),
                    Ran::Unsettled(_) => None,
                }
            }
            Ok(Admission::Replayed(_)) | Err(_) => None,
        };
        cleanup(&self.agent).await;
        ran
    }

    /// An admission given up for `gave_up` whose verdict is whatever `admission` resolves to.
    #[cfg(test)]
    pub(crate) fn asked(
        request: &KeyedRequest<'_>,
        admission: impl Future<Output = Result<Admission, RuntimeError>> + Send + 'static,
        gave_up: AskError,
    ) -> Self {
        Self {
            agent: Arc::new(request.agent().expect("the test's agent builds")),
            store: request.store.clone(),
            admission: Box::pin(admission),
            gave_up,
        }
    }
}

/// An outstanding admission is equal only to itself.
impl PartialEq for Outstanding {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.agent, &other.agent)
    }
}

impl Eq for Outstanding {}

impl fmt::Debug for Outstanding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outstanding")
            .field("gave_up", &self.gave_up)
            .finish_non_exhaustive()
    }
}

/// How a run that a keyed turn waited for, or stopped, ended.
enum Ran {
    /// The run completed, whether or not its turn had been given up by then.
    Answered(String),
    /// Crabber reported a final status that gives the turn no answer. This is the error the
    /// turn reports for it.
    Ended(AskError),
    /// Crabber did not say how the run ended, or its answer could not be read, so the run's
    /// durable state decides. `Some` is why the turn gave the run up.
    Unsettled(Option<AskError>),
}

impl Ran {
    /// What the turn's admission granted, now that the run it started was waited for.
    fn admitted<R, L>(
        self,
        ended: impl FnOnce(AskError) -> R,
        unsettled: impl FnOnce(Option<AskError>) -> R,
    ) -> Admitted<R, L> {
        match self {
            Self::Answered(answer) => Admitted::Answered(answer),
            Self::Ended(error) => Admitted::Ended(ended(error)),
            Self::Unsettled(gave_up) => Admitted::Ended(unsettled(gave_up)),
        }
    }
}

/// Wait for the run a keyed turn started, under the turn's `cancel` and `deadline`. A turn
/// given up first interrupts the run and stands by it.
async fn stand_by(
    store: &SqliteStore,
    run: crabber::RunHandle,
    cancel: &mut Signal,
    deadline: tokio::time::Instant,
) -> Ran {
    let control = run.control();
    let mut done = std::pin::pin!(run.done());
    match wait_for_completion(done.as_mut(), cancel, deadline).await {
        Ok(ended) => ran(store, Some(ended), None, deadline).await,
        Err(gave_up) => {
            control.interrupt();
            stopped(store, done, gave_up).await
        }
    }
}

/// Interrupt a run that started after its turn was given up for `gave_up`, and stand by it.
async fn stop(store: &SqliteStore, run: crabber::RunHandle, gave_up: AskError) -> Ran {
    run.interrupt();
    stopped(store, run.done(), gave_up).await
}

/// Stand by an interrupted run for [`SETTLE`]. Crabber honours an interrupt only up to the end
/// of the model's last answer, so the run may complete all the same.
async fn stopped(
    store: &SqliteStore,
    done: impl Future<Output = Result<RunResult, RuntimeError>>,
    gave_up: AskError,
) -> Ran {
    let by = tokio::time::Instant::now() + SETTLE;
    let ended = tokio::time::timeout_at(by, done).await.ok();
    ran(store, ended, Some(gave_up), by).await
}

/// What crabber's report of a run's end means for the run's turn. `ended` is `None` for a run
/// not seen to end, and `gave_up` is why the turn gave the run up, if it did. A completed run's
/// answer is read by `read_by`.
async fn ran(
    store: &SqliteStore,
    ended: Option<Result<RunResult, RuntimeError>>,
    gave_up: Option<AskError>,
    read_by: tokio::time::Instant,
) -> Ran {
    // Crabber reports a run it settled as failed with an error, and a run whose settlement it
    // could not confirm with an error too. Neither names the run's final status.
    let Some(Ok(result)) = ended else {
        return Ran::Unsettled(gave_up);
    };
    match result.status {
        RunStatus::Completed => {
            let messages = read(store.list_messages(&result.session_id, None));
            match tokio::time::timeout_at(read_by, messages).await {
                Ok(Ok(messages)) => match committed_text(messages, &result.run_id) {
                    Ok(answer) => Ran::Answered(answer),
                    Err(error) => Ran::Ended(error),
                },
                Ok(Err(Unreadable)) | Err(_) => Ran::Unsettled(gave_up),
            }
        }
        // A run interrupted without its turn giving it up was interrupted from elsewhere.
        RunStatus::Interrupted => Ran::Ended(gave_up.unwrap_or(AskError::Interrupted)),
        RunStatus::Failed => Ran::Ended(AskError::Run),
        RunStatus::Pending | RunStatus::Running | RunStatus::Paused => Ran::Unsettled(gave_up),
    }
}

/// Wait for a keyed admission verdict no longer than its turn may run, so a stalled admission
/// does not hold its conversation's task for longer than the grace that follows. A turn given
/// up first, by `cancel` or by `deadline`, is the `Some` beside a verdict that came within that
/// grace: a run the verdict starts must then be interrupted, not executed. An admission with no
/// verdict by then is returned still alive, never dropped. It may yet start a run, so whoever
/// keeps it also keeps the turn that run would take.
async fn verdict(
    agent: &Arc<Agent>,
    store: &Arc<SqliteStore>,
    mut admission: Asked,
    cancel: &mut Signal,
    deadline: tokio::time::Instant,
) -> Result<(Result<Admission, RuntimeError>, Option<AskError>), Outstanding> {
    let gave_up = tokio::select! {
        biased;
        verdict = admission.as_mut() => return Ok((verdict, None)),
        () = cancel.fired() => AskError::Interrupted,
        () = tokio::time::sleep_until(deadline) => AskError::Timeout,
    };
    match tokio::time::timeout(ADMISSION_GRACE, admission.as_mut()).await {
        Ok(verdict) => Ok((verdict, Some(gave_up))),
        Err(_) => Err(Outstanding {
            agent: agent.clone(),
            store: store.clone(),
            admission,
            gave_up,
        }),
    }
}

/// Run `request` as a turn admitted under its key. A key that crabber already holds starts no
/// second run: its original run's durable outcome is returned instead.
///
/// # Errors
/// The same fixed errors as [`super::ask`], each of them definitive: before admission nothing
/// was admitted, and afterwards crabber has reported the final status of the run this call
/// started. `Interrupted` and `Timeout` are reported only for a run that crabber shows
/// interrupted, admitted by this call or under a replayed key: a run that completed although
/// its turn was given up is answered instead. A replayed run that failed reports `Run`.
pub async fn ask_keyed(
    request: &KeyedRequest<'_>,
    deadline: tokio::time::Instant,
    cancel: Cancel,
) -> Result<KeyedOutcome, AskError> {
    let session = request.session();
    let mut cancel = Signal::new(cancel);
    let (admitted, key, text, store) = (
        session.clone(),
        request.key.clone(),
        request.text.to_owned(),
        request.store.clone(),
    );
    let turn = turn(
        &request.host(),
        Some(&session),
        deadline,
        &mut cancel,
        move |agent, cancel| {
            Box::pin(async move {
                let options = options(agent, key.clone(), &text)?;
                let (owner, session) = (agent.clone(), admitted.clone());
                let admission: Asked =
                    Box::pin(async move { owner.prompt_keyed(session, text, options).await });
                let waited = verdict(agent, &store, admission, &mut *cancel, deadline).await;
                let (verdict, late) = match waited {
                    Ok(waited) => waited,
                    Err(outstanding) => return Ok(Admitted::Outstanding(outstanding)),
                };
                Ok(match (verdict, late) {
                    (Ok(Admission::Started { handle, .. }), None) => {
                        let ran = stand_by(&store, handle, cancel, deadline).await;
                        ran.admitted(Replay::Ended, Replay::Unsettled)
                    }
                    (Ok(Admission::Started { handle, .. }), Some(gave_up)) => {
                        let ran = stop(&store, handle, gave_up).await;
                        ran.admitted(Replay::Ended, Replay::Unsettled)
                    }
                    (Ok(Admission::Replayed(receipt)), _) => {
                        Admitted::Ended(Replay::Retained(receipt))
                    }
                    (Err(error), _) => match admission_refusal(&error) {
                        Refusal::Busy => return Err(AskError::Busy),
                        Refusal::Conflict => Admitted::Ended(Replay::Conflict),
                        Refusal::PreparationUnretired => {
                            Admitted::Ended(retired(agent, &admitted, &key, cancel).await)
                        }
                        Refusal::Unknown => Admitted::Ended(Replay::Unknown),
                    },
                })
            })
        },
    )
    .await;
    let receipt = match turn {
        Ok(Turn::Answered(answer)) => return Ok(KeyedOutcome::Answered(answer)),
        Ok(Turn::Ended(Replay::Conflict)) => return Ok(KeyedOutcome::ConfigurationChanged),
        Ok(Turn::Ended(Replay::KeyRetired)) => return Ok(KeyedOutcome::KeyRetired),
        Ok(Turn::Ended(Replay::Unknown)) => return Ok(KeyedOutcome::Unsettled(None)),
        Ok(Turn::Ended(Replay::Unsettled(gave_up))) => {
            return Ok(KeyedOutcome::Unsettled(gave_up));
        }
        Ok(Turn::Ended(Replay::Ended(error))) => return Err(error),
        Ok(Turn::Ended(Replay::Retained(receipt))) => receipt,
        Ok(Turn::Outstanding(admission)) => return Ok(KeyedOutcome::Outstanding(admission)),
        // An unstarted turn is interrupted only by a cancel signal that fired before crabber was
        // asked to admit. A turn given up later has its admission's verdict or is outstanding.
        Err(TurnError::Unstarted(AskError::Interrupted)) => return Ok(KeyedOutcome::NotAdmitted),
        Err(error) => return Err(error.into()),
    };
    let held = tokio::select! {
        biased;
        // Whoever reconciles the key reads the run, and knows whether the service is stopping.
        () = cancel.fired() => return Ok(KeyedOutcome::Unsettled(None)),
        held = held(&request.store, &session, &receipt.run_id) => held,
    };
    match held {
        Ok(Held::Answered(answer)) => Ok(KeyedOutcome::Answered(answer)),
        Ok(Held::NoAnswer) => Err(AskError::NoAnswer),
        Ok(Held::Interrupted) => Err(AskError::Interrupted),
        Ok(Held::Failed) => Err(AskError::Run),
        // A run that is live, or that cannot be read right now, may yet hold an answer.
        Ok(Held::Live(_)) | Err(Unreadable) => Ok(KeyedOutcome::Unsettled(None)),
    }
}

/// The conversation store could not be read, so nothing is known about the run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("conversation store unavailable")]
pub struct Unreadable;

/// A non-terminal run whose owner is gone, with the ownership a settlement must name.
#[derive(Clone, PartialEq, Eq)]
pub struct Orphan {
    run_id: RunId,
    claim_token: String,
    owner: String,
    lease_until: OffsetDateTime,
}

impl Orphan {
    /// When the dead owner's lease expires by the store's clock. Crabber refuses every
    /// settlement before then.
    #[must_use]
    pub fn lease_until(&self) -> OffsetDateTime {
        self.lease_until
    }
}

impl fmt::Debug for Orphan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Orphan")
            .field("lease_until", &self.lease_until)
            .finish_non_exhaustive()
    }
}

/// What crabber durably holds for one request key.
#[derive(Clone, PartialEq, Eq)]
pub enum Reconciled {
    /// No admission committed under the key.
    NotAdmitted,
    Answered(String),
    /// The run completed without assistant text.
    NoAnswer,
    Interrupted,
    Failed,
    /// Admitted, but its executor never began: no model or tool was reached.
    Unstarted(Orphan),
    /// Its executor began. The model may have been reached and tools may have run.
    Started(Orphan),
}

impl fmt::Debug for Reconciled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAdmitted => f.write_str("NotAdmitted"),
            Self::Answered(text) => write!(f, "Answered(<{} bytes>)", text.len()),
            Self::NoAnswer => f.write_str("NoAnswer"),
            Self::Interrupted => f.write_str("Interrupted"),
            Self::Failed => f.write_str("Failed"),
            Self::Unstarted(orphan) => f.debug_tuple("Unstarted").field(orphan).finish(),
            Self::Started(orphan) => f.debug_tuple("Started").field(orphan).finish(),
        }
    }
}

/// Read what crabber holds for `request`'s key without executing or claiming anything.
///
/// # Errors
/// [`Unreadable`] when the store fails or does not answer in time.
pub async fn reconcile_keyed(request: &KeyedRequest<'_>) -> Result<Reconciled, Unreadable> {
    let session = request.session();
    let store = &request.store;
    let Some(receipt) = read(store.lookup_admission(&session, &request.key)).await? else {
        return Ok(Reconciled::NotAdmitted);
    };
    let orphan = match held(store, &session, &receipt.run_id).await? {
        Held::Answered(answer) => return Ok(Reconciled::Answered(answer)),
        Held::NoAnswer => return Ok(Reconciled::NoAnswer),
        Held::Interrupted => return Ok(Reconciled::Interrupted),
        Held::Failed => return Ok(Reconciled::Failed),
        Held::Live(orphan) => orphan,
    };
    let execution = read(store.admission_execution_state(&orphan.run_id)).await?;
    Ok(match execution {
        Some(AdmissionExecutionState::Unstarted) => Reconciled::Unstarted(orphan),
        // Without a retained execution record nothing proves the executor never began.
        Some(AdmissionExecutionState::Started) | None => Reconciled::Started(orphan),
    })
}

/// A keyed run's durable outcome, or the ownership of a run that has none yet.
enum Held {
    Answered(String),
    NoAnswer,
    Interrupted,
    Failed,
    Live(Orphan),
}

async fn held(
    store: &SqliteStore,
    session: &SessionId,
    run_id: &RunId,
) -> Result<Held, Unreadable> {
    let run = read(store.get_run(run_id)).await?.ok_or(Unreadable)?;
    Ok(match run.status {
        RunStatus::Completed => {
            let messages = read(store.list_messages(session, None)).await?;
            match committed_text(messages, run_id) {
                Ok(answer) => Held::Answered(answer),
                Err(_) => Held::NoAnswer,
            }
        }
        RunStatus::Interrupted => Held::Interrupted,
        RunStatus::Failed => Held::Failed,
        RunStatus::Pending | RunStatus::Running | RunStatus::Paused => Held::Live(Orphan {
            run_id: run.id,
            claim_token: run.claim_token,
            owner: run.owner,
            lease_until: run.lease_until,
        }),
    })
}

async fn read<T>(call: impl Future<Output = Result<T, StoreError>>) -> Result<T, Unreadable> {
    #[cfg(test)]
    if let Some(lag) = read_faults::lag() {
        tokio::time::sleep(lag).await;
    }
    #[cfg(test)]
    if read_faults::take() {
        return Err(Unreadable);
    }
    match bounded(call).await {
        Some(Ok(value)) => Ok(value),
        Some(Err(_)) | None => Err(Unreadable),
    }
}

async fn bounded<T>(call: impl Future<Output = T>) -> Option<T> {
    tokio::time::timeout(Duration::from_secs(CONTROL_SECONDS), call)
        .await
        .ok()
}

/// Unit-test seam: fails the next durable reads made on the calling thread before they touch the
/// store, or makes every one of them slow. It is per thread so tests running in parallel cannot
/// consume each other's faults, which holds for a test that drives its turn on a current-thread
/// runtime.
#[cfg(test)]
pub(crate) mod read_faults {
    use std::{cell::Cell, time::Duration};

    thread_local! {
        static ARMED: Cell<usize> = const { Cell::new(0) };
        static LAG: Cell<Option<Duration>> = const { Cell::new(None) };
    }

    pub(crate) fn fail(times: usize) {
        ARMED.set(times);
    }

    pub(super) fn take() -> bool {
        let armed = ARMED.get();
        ARMED.set(armed.saturating_sub(1));
        armed > 0
    }

    /// Until the guard drops, every read first waits `by`, as against a slow store.
    pub(crate) fn slow(by: Duration) -> Slow {
        LAG.set(Some(by));
        Slow
    }

    pub(crate) struct Slow;

    impl Drop for Slow {
        fn drop(&mut self) {
            LAG.set(None);
        }
    }

    pub(super) fn lag() -> Option<Duration> {
        LAG.get()
    }
}

/// Why crabber did not recover or settle an orphaned run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unsettled {
    /// The dead owner's lease has not expired by the store's clock.
    LiveLease,
    /// The run moved on after it was read, through another settlement or through its own
    /// owner. Its durable state must be read again.
    Raced,
    /// The retained admission no longer matches this coworker's configuration or the text.
    ConfigurationChanged,
    /// Crabber holds no evidence that would make the settlement safe.
    Unprovable,
    /// No verdict: a store failure, a lost commit acknowledgement, or no answer in time.
    Unknown,
}

fn recovery_refusal(error: &RuntimeError) -> Unsettled {
    match error {
        RuntimeError::AdmissionExecution(error) => match error {
            AdmissionExecutionError::LiveLease | AdmissionExecutionError::PreparationBusy => {
                Unsettled::LiveLease
            }
            AdmissionExecutionError::StaleOwner
            | AdmissionExecutionError::AlreadyStarted
            | AdmissionExecutionError::AlreadyTerminal => Unsettled::Raced,
            AdmissionExecutionError::SemanticConflict => Unsettled::ConfigurationChanged,
            AdmissionExecutionError::MissingEvidence | AdmissionExecutionError::Unsupported => {
                Unsettled::Unprovable
            }
            AdmissionExecutionError::UnknownStoreFailure => Unsettled::Unknown,
        },
        RuntimeError::Store(StoreError::AdmissionConflict) => Unsettled::ConfigurationChanged,
        _ => Unsettled::Unknown,
    }
}

fn abandon_refusal(error: &AbandonError) -> Unsettled {
    match error {
        AbandonError::LiveLease => Unsettled::LiveLease,
        AbandonError::StaleOwner | AbandonError::AlreadyTerminal => Unsettled::Raced,
        AbandonError::Unsupported
        | AbandonError::NotFound
        | AbandonError::NativeReconciliationRequired => Unsettled::Unprovable,
        AbandonError::Store(_) => Unsettled::Unknown,
    }
}

/// A live owner is worth waiting for, and a run that moved on after it was read is worth
/// reading again. After any other refusal the attempt stays as it is, and nothing proves the
/// run settled.
fn attempt_refusal(error: AttemptError) -> Unsettled {
    match error {
        AttemptError::LiveOwner => Unsettled::LiveLease,
        // The control named a revision or a state that the run has since left.
        AttemptError::RevisionConflict | AttemptError::InvalidTransition => Unsettled::Raced,
        AttemptError::Unsupported
        | AttemptError::NotFound
        | AttemptError::InvalidRequest
        | AttemptError::SemanticConflict
        | AttemptError::GenerationConflict
        | AttemptError::GenerationExhausted
        | AttemptError::RevisionExhausted
        | AttemptError::Busy
        | AttemptError::StaleOwner
        | AttemptError::CleanupBlocked
        | AttemptError::PendingInput
        | AttemptError::UnknownStoreOutcome => Unsettled::Unprovable,
    }
}

/// How an attempt to run an orphaned, never-begun admission ended.
#[derive(PartialEq, Eq)]
pub enum Recovery {
    /// This call claimed the original run, and crabber reported how it ended: with this answer,
    /// or with the final status this error stands for. A run stopped because its turn had been
    /// given up is answered all the same when it completed first.
    Ran(Result<String, AskError>),
    /// This call claimed the original run, and crabber did not say how it ended. The run's
    /// durable state decides. `Some` is why the turn gave the run up.
    Unsettled(Option<AskError>),
    /// Crabber holds Started or terminal evidence for the run, so nothing may execute it again.
    Replayed,
    Refused(Unsettled),
    /// The cancel signal fired before crabber was asked. Nothing ran and nothing was claimed.
    Interrupted,
    /// The turn was given up and crabber has still not answered the claim. As with
    /// [`KeyedOutcome::Outstanding`], the caller keeps it alive and leaves the request alone
    /// until it resolves.
    Outstanding(Outstanding),
}

impl fmt::Debug for Recovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ran(Ok(text)) => write!(f, "Ran(Ok(<{} bytes>))", text.len()),
            Self::Ran(Err(error)) => write!(f, "Ran(Err({error:?}))"),
            Self::Unsettled(gave_up) => f.debug_tuple("Unsettled").field(gave_up).finish(),
            Self::Replayed => f.write_str("Replayed"),
            Self::Refused(refusal) => f.debug_tuple("Refused").field(refusal).finish(),
            Self::Interrupted => f.write_str("Interrupted"),
            Self::Outstanding(claim) => f.debug_tuple("Outstanding").field(claim).finish(),
        }
    }
}

/// Claim `request`'s admission from its dead owner and run the original turn. Crabber grants
/// the claim only for an [`Reconciled::Unstarted`] run whose lease has expired.
pub async fn recover_keyed(
    request: &KeyedRequest<'_>,
    deadline: tokio::time::Instant,
    cancel: Cancel,
) -> Recovery {
    let session = request.session();
    let mut cancel = Signal::new(cancel);
    let (admitted, key, text, store) = (
        session.clone(),
        request.key.clone(),
        request.text.to_owned(),
        request.store.clone(),
    );
    let turn = turn(
        &request.host(),
        Some(&session),
        deadline,
        &mut cancel,
        move |agent, cancel| {
            Box::pin(async move {
                let options = options(agent, key, &text)?;
                let owner = agent.clone();
                let claim: Asked =
                    Box::pin(async move { owner.recover_admission(admitted, text, options).await });
                let waited = verdict(agent, &store, claim, &mut *cancel, deadline).await;
                let (verdict, late) = match waited {
                    Ok(waited) => waited,
                    Err(outstanding) => return Ok(Admitted::Outstanding(outstanding)),
                };
                let failed = |error| Recovery::Ran(Err(error));
                Ok(match (verdict, late) {
                    (Ok(Admission::Started { handle, .. }), None) => {
                        let ran = stand_by(&store, handle, cancel, deadline).await;
                        ran.admitted(failed, Recovery::Unsettled)
                    }
                    (Ok(Admission::Started { handle, .. }), Some(gave_up)) => {
                        let ran = stop(&store, handle, gave_up).await;
                        ran.admitted(failed, Recovery::Unsettled)
                    }
                    (Ok(Admission::Replayed(_)), _) => Admitted::Ended(Recovery::Replayed),
                    (Err(error), _) => Admitted::Ended(Recovery::Refused(recovery_refusal(&error))),
                })
            })
        },
    )
    .await;
    match turn {
        Ok(Turn::Answered(answer)) => Recovery::Ran(Ok(answer)),
        Err(TurnError::Started(error)) => Recovery::Ran(Err(error)),
        Ok(Turn::Ended(recovery)) => recovery,
        Ok(Turn::Outstanding(claim)) => Recovery::Outstanding(claim),
        Err(TurnError::Unstarted(AskError::Interrupted)) => Recovery::Interrupted,
        Err(TurnError::Unstarted(_)) => Recovery::Refused(Unsettled::Unknown),
    }
}

/// Settle an [`Reconciled::Unstarted`] orphan as interrupted without executing it. Afterwards
/// the session accepts the next turn.
///
/// # Errors
/// The classified refusal. The run is unchanged unless the error is [`Unsettled::Unknown`].
pub async fn abandon_unstarted(
    request: &KeyedRequest<'_>,
    orphan: &Orphan,
) -> Result<(), Unsettled> {
    let agent = request.agent().map_err(|_| Unsettled::Unknown)?;
    let abandoned = bounded(abandon(&agent, orphan)).await;
    cleanup(&agent).await;
    abandoned.unwrap_or(Err(Unsettled::Unknown))
}

/// Crabber abandons only a run with no execution attempt and no tool phase.
async fn abandon(agent: &Agent, orphan: &Orphan) -> Result<(), Unsettled> {
    let abandoned = agent.abandon(AbandonRequest {
        expected: RunFence {
            run_id: orphan.run_id.clone(),
            claim_token: orphan.claim_token.clone(),
        },
        expected_owner: orphan.owner.clone(),
        authority: AbandonAuthority::ExpiredLease,
    });
    match abandoned.await {
        Ok(_) => Ok(()),
        Err(error) => Err(abandon_refusal(&error)),
    }
}

/// Settle a [`Reconciled::Started`] orphan as interrupted through crabber's native attempt
/// controls: revoke its expired owner, then retire the attempt, cancelling tool calls that never
/// began. A run whose owner died before reserving an attempt is abandoned instead. Afterwards
/// the session accepts the next turn. What the run did before its owner died stays unknown.
///
/// # Errors
/// The classified refusal. The session may stay busy unless the error is
/// [`Unsettled::LiveLease`], which a later call can outlast.
pub async fn retire_started(request: &KeyedRequest<'_>, orphan: &Orphan) -> Result<(), Unsettled> {
    let agent = request.agent().map_err(|_| Unsettled::Unknown)?;
    let retired = bounded(retire(&agent, request, orphan)).await;
    cleanup(&agent).await;
    retired.unwrap_or(Err(Unsettled::Unknown))
}

async fn retire(
    agent: &Agent,
    request: &KeyedRequest<'_>,
    orphan: &Orphan,
) -> Result<(), Unsettled> {
    let latest = request.store.latest_execution_attempt(&orphan.run_id);
    let Some(latest) = latest.await.map_err(|_| Unsettled::Unknown)? else {
        // The executor's begin committed and its owner died before reserving an attempt, so
        // there is nothing for the attempt controls to retire. Without attempt evidence crabber
        // lets the run be abandoned like one that never began.
        return abandon(agent, orphan).await;
    };
    let session_id = request.session();
    let revision = match latest.attempt.state {
        AttemptState::Started => {
            let marked = agent
                .mark_attempt_uncertain(AttemptUncertainty {
                    session_id: session_id.clone(),
                    run_id: orphan.run_id.clone(),
                    id: ControlId::new(),
                    expected_revision: latest.revision,
                    target: latest.attempt.target.clone(),
                    // Never `HostStopped`: the API store lock says nothing about another
                    // frontend's process, so only the expired lease is claimed.
                    cause: AttemptUncertaintyCause::ExpiredOrRevokedOwner,
                })
                .await
                .map_err(attempt_refusal)?;
            let (AttemptControlOutcome::Applied(receipt)
            | AttemptControlOutcome::Replayed(receipt)) = marked;
            receipt.revision
        }
        // An earlier reconciliation revoked the owner and stopped before retiring the attempt.
        AttemptState::OutcomeUnknown => latest.revision,
        // The run ended after it was read as live, and its own state says how.
        AttemptState::Completed | AttemptState::Failed | AttemptState::Interrupted => {
            return Err(Unsettled::Raced);
        }
        AttemptState::Unstarted | AttemptState::Suspended => return Err(Unsettled::Unprovable),
    };
    agent
        .retire_attempt(AttemptRetirement {
            session_id,
            run_id: orphan.run_id.clone(),
            id: ControlId::new(),
            expected_revision: revision,
            target: latest.attempt.target,
            outcome: AttemptRetirementOutcome::Interrupted,
            joined: AttemptResolverClaim {
                resolver_id: RESOLVER_ID.into(),
                authority_claim: JOINED_CLAIM.into(),
            },
            pending: PendingCallDisposition::CancelNeverStarted,
        })
        .await
        .map_err(attempt_refusal)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_that_says_the_run_moved_on_is_a_race() {
        for moved_on in [
            AttemptError::RevisionConflict,
            AttemptError::InvalidTransition,
        ] {
            assert_eq!(attempt_refusal(moved_on), Unsettled::Raced);
        }
        assert_eq!(
            attempt_refusal(AttemptError::LiveOwner),
            Unsettled::LiveLease
        );
        for unproven in [AttemptError::StaleOwner, AttemptError::CleanupBlocked] {
            assert_eq!(attempt_refusal(unproven), Unsettled::Unprovable);
        }
    }
}
