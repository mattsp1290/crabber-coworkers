//! Supervise Slack sockets and ordered, durable per-thread workers.
use crate::{
    app::{Provider, install_cancel},
    ask::{self, AskError, Cancel, Session},
    config::{self, Config},
    slack::{
        self, CredentialSource,
        api::SlackApi,
        delivery,
        ingress::{self, Classified, Identity, Mention},
        socket::{self, Inbound},
    },
    store,
};
use crabber::{Selection, providers::Resolver, session::SqliteStore};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

pub const THREAD_QUEUE: usize = 4;
pub const RECENT_EVENTS: usize = 1_024;
pub const SHUTDOWN_SECONDS: u64 = 20;
pub const BUSY_NOTICE_SECONDS: u64 = 30;
pub const IDLE_SECONDS: u64 = 300;

pub const EMPTY_NOTICE: &str = "Mention me with a question and I will answer in this thread.";
pub const TOO_LONG_NOTICE: &str = "That message is too long for me to answer.";
pub const BUSY_NOTICE: &str =
    "I am still working on earlier messages in this thread. Please try again shortly.";
pub const TIMEOUT_NOTICE: &str = "I ran out of time answering that.";
pub const FAILED_NOTICE: &str = "I could not answer that.";

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error("no coworker has a slack block")]
    NoSlackCoworkers,
    #[error("OPENCODE_GO_API_KEY is not set")]
    MissingApiKey,
    #[error(transparent)]
    Credentials(#[from] slack::CredentialError),
    #[error("cannot install signal handlers")]
    Signal,
    #[error("Slack authentication failed for {0}")]
    Auth(String),
    #[error(transparent)]
    Store(#[from] store::StoreOpenError),
    #[error("Slack connection stopped for {0}")]
    Connection(String),
    #[error("interrupted")]
    Interrupted,
}
impl ServeError {
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Config(_)
            | Self::NoSlackCoworkers
            | Self::MissingApiKey
            | Self::Credentials(_) => 2,
            Self::Interrupted => 130,
            Self::Signal | Self::Auth(_) | Self::Store(_) | Self::Connection(_) => 1,
        }
    }
}

type Stop = watch::Receiver<bool>;
type Log = mpsc::UnboundedSender<String>;
fn stop_future(mut rx: Stop) -> Cancel {
    Box::pin(async move {
        let _ = rx.wait_for(|v| *v).await;
    })
}

// Abort the cancellation listener on every exit, including partial startup failure.
struct Supervisor(JoinHandle<()>);
impl Drop for Supervisor {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Runtime {
    resolver: Arc<dyn Resolver>,
    selection: Selection,
    permits: Semaphore,
    stop: Stop,
    log: Log,
    // A synchronous handler can spawn into this set without awaiting a lock.
    // run takes the set after sockets stop, then joins/aborts all workers and notices.
    workers: Mutex<JoinSet<()>>,
}
impl Runtime {
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let mut workers = self.workers.lock().expect("worker set poisoned");
        while let Some(result) = workers.try_join_next() {
            if result.is_err() {
                let _ = self.log.send("Slack worker failed".into());
            }
        }
        workers.spawn(future);
    }
}

struct Coworker {
    config: Arc<Config>,
    index: usize,
    api: Arc<SlackApi>,
    identity: Identity,
    store: Arc<SqliteStore>,
    runtime: Arc<Runtime>,
}
impl Coworker {
    fn id(&self) -> &str {
        &self.config.coworkers[self.index].id
    }
    fn log(&self, message: &str) {
        let _ = self
            .runtime
            .log
            .send(format!("{message} for {}", self.id()));
    }
}

/// Validate all local inputs and Slack identities before creating any stores.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    config_path: PathBuf,
    provider: Provider,
    credentials: CredentialSource,
    selection: Selection,
    cancel: Option<Cancel>,
    slack_base: &str,
    err: &mut (dyn Write + Send),
) -> Result<(), ServeError> {
    let config = Arc::new(config::load(&config_path)?);
    if config.slack_coworkers().next().is_none() {
        return Err(ServeError::NoSlackCoworkers);
    }
    let resolver = provider.resolve().ok_or(ServeError::MissingApiKey)?;
    let tokens = config
        .coworkers
        .iter()
        .enumerate()
        .filter(|(_, def)| def.slack.is_some())
        .map(|(index, def)| credentials.resolve(&def.id).map(|token| (index, token)))
        .collect::<Result<Vec<_>, _>>()?;
    let cancel = match cancel {
        Some(cancel) => cancel,
        None => install_cancel().map_err(|_| ServeError::Signal)?,
    };
    let (stop_tx, stop) = watch::channel(false);
    let supervisor_tx = stop_tx.clone();
    let _supervisor = Supervisor(tokio::spawn(async move {
        cancel.await;
        let _ = supervisor_tx.send(true);
    }));
    let mut startup_stop = stop_future(stop.clone());
    let mut authenticated = Vec::new();
    for (index, token) in tokens {
        let def = &config.coworkers[index];
        let api = Arc::new(
            SlackApi::new(slack_base, token).map_err(|_| ServeError::Auth(def.id.clone()))?,
        );
        let identity = tokio::select! {
            biased;
            () = &mut startup_stop => return Err(ServeError::Interrupted),
            result = api.auth_test() => result.map_err(|_| ServeError::Auth(def.id.clone()))?,
        };
        if identity.team_id != def.slack.as_ref().expect("Slack coworker").team_id {
            return Err(ServeError::Auth(def.id.clone()));
        }
        let url = tokio::select! {
            biased;
            () = &mut startup_stop => return Err(ServeError::Interrupted),
            result = api.connections_open() => result.map_err(|_| ServeError::Auth(def.id.clone()))?,
        };
        authenticated.push((index, api, identity, url));
    }
    let (log, mut logs) = mpsc::unbounded_channel();
    let runtime = Arc::new(Runtime {
        resolver,
        selection,
        permits: Semaphore::new(config.limits.max_concurrent_turns),
        stop: stop.clone(),
        log,
        workers: Mutex::new(JoinSet::new()),
    });
    let mut ready = Vec::new();
    for (index, api, identity, url) in authenticated {
        let store = tokio::select! {
            biased;
            () = &mut startup_stop => return Err(ServeError::Interrupted),
            result = store::open(&config.state_dir, &config.coworkers[index].id) => result?,
        };
        ready.push((
            Arc::new(Coworker {
                config: config.clone(),
                index,
                api,
                identity,
                store,
                runtime: runtime.clone(),
            }),
            url,
        ));
    }
    let mut sockets = JoinSet::new();
    let mut socket_names = HashMap::new();
    for (coworker, url) in ready {
        let name = coworker.id().to_owned();
        let task = sockets.spawn(coworker_loop(coworker, url));
        socket_names.insert(task.id(), name);
    }
    let outcome = supervise(&mut sockets, &socket_names, stop.clone(), &mut logs, err).await;
    shutdown(&stop_tx, &mut sockets, &runtime).await;
    while let Ok(line) = logs.try_recv() {
        let _ = writeln!(err, "{line}");
    }
    if outcome.is_ok() {
        let _ = writeln!(err, "stopped");
    }
    outcome
}

async fn supervise(
    sockets: &mut JoinSet<Result<(), ServeError>>,
    socket_names: &HashMap<tokio::task::Id, String>,
    stop: Stop,
    logs: &mut mpsc::UnboundedReceiver<String>,
    err: &mut (dyn Write + Send),
) -> Result<(), ServeError> {
    loop {
        tokio::select! {
            biased;
            () = stop_future(stop.clone()) => break Ok(()),
            result = sockets.join_next_with_id() => match result {
                Some(Ok((_, Err(error)))) => break Err(error),
                Some(Err(error)) => break Err(ServeError::Connection(socket_names[&error.id()].clone())),
                Some(Ok((id, Ok(())))) => { let _ = id; },
                None => break Ok(()),
            },
            Some(line) = logs.recv() => { let _ = writeln!(err, "{line}"); },
        }
    }
}

async fn shutdown(
    stop: &watch::Sender<bool>,
    sockets: &mut JoinSet<Result<(), ServeError>>,
    runtime: &Runtime,
) {
    let _ = stop.send(true);
    // Sockets own all worker spawns; join them before taking the worker set.
    let finish = async {
        while sockets.join_next().await.is_some() {}
        let mut workers = {
            let mut set = runtime.workers.lock().expect("worker set poisoned");
            std::mem::take(&mut *set)
        };
        while let Some(result) = workers.join_next().await {
            if result.is_err() {
                let _ = runtime.log.send("Slack worker failed".into());
            }
        }
    };
    if tokio::time::timeout(Duration::from_secs(SHUTDOWN_SECONDS), finish)
        .await
        .is_err()
    {
        let _ = runtime.log.send("Slack shutdown timed out".into());
        sockets.abort_all();
        runtime
            .workers
            .lock()
            .expect("worker set poisoned")
            .abort_all();
        while sockets.join_next().await.is_some() {}
    }
}

async fn coworker_loop(coworker: Arc<Coworker>, url: url::Url) -> Result<(), ServeError> {
    let mut handler = Handler {
        coworker: coworker.clone(),
        recent: HashSet::new(),
        order: VecDeque::new(),
        threads: HashMap::new(),
    };
    let (log, mut logs) = mpsc::unbounded_channel();
    let socket = socket::run(
        coworker.api.clone(),
        Some(url),
        stop_future(coworker.runtime.stop.clone()),
        log,
        move |event| handler.handle(event),
    );
    tokio::pin!(socket);
    loop {
        tokio::select! {
            result = &mut socket => return result.map_err(|_| ServeError::Connection(coworker.id().into())),
            Some(line) = logs.recv() => coworker.log(&line),
        }
    }
}

#[derive(Debug)]
struct QueuedMention {
    mention: Mention,
    // Shared across idle-worker generations; released when processing starts.
    slot: OwnedSemaphorePermit,
}
struct Admission {
    slots: Arc<Semaphore>,
    last_busy_notice: Option<Instant>,
}
struct Thread {
    sender: mpsc::Sender<QueuedMention>,
    admission: Admission,
    finished: Stop,
}
impl Thread {
    fn finished(&self) -> bool {
        *self.finished.borrow() || self.finished.has_changed().is_err()
    }
    fn keep(&self) -> bool {
        !self.sender.is_closed()
            || !self.finished()
            || self
                .admission
                .last_busy_notice
                .is_some_and(|last| last.elapsed() < Duration::from_secs(BUSY_NOTICE_SECONDS))
    }
}
struct Handler {
    coworker: Arc<Coworker>,
    recent: HashSet<String>,
    order: VecDeque<String>,
    threads: HashMap<String, Thread>,
}
impl Handler {
    fn handle(&mut self, inbound: Inbound) -> Option<String> {
        let Inbound::Envelope(envelope) = inbound else {
            self.coworker.log("Slack connected");
            return None;
        };
        let def = &self.coworker.config.coworkers[self.coworker.index];
        let Classified::Mention(mention) = ingress::classify(
            def.slack.as_ref().expect("Slack coworker"),
            &self.coworker.identity,
            &envelope.payload,
        ) else {
            return Some(envelope.id);
        };
        if self.recent.contains(&mention.event_id) {
            return Some(envelope.id);
        }
        if self.recent.insert(mention.event_id.clone()) {
            self.order.push_back(mention.event_id.clone());
            if self.order.len() > RECENT_EVENTS
                && let Some(old) = self.order.pop_front()
            {
                self.recent.remove(&old);
            }
        }
        let Some(name) = ingress::conversation_name(&mention.channel, &mention.root_ts) else {
            self.coworker.log("Slack event ignored");
            return Some(envelope.id);
        };
        // A closed receiver can still be draining accepted mentions. Keep its
        // completion fence until that worker has actually exited.
        self.threads.retain(|_, thread| thread.keep());
        let thread = self
            .threads
            .entry(name.clone())
            .or_insert_with(|| spawn_thread(&self.coworker, name.clone(), None));
        let Ok(slot) = thread.admission.slots.clone().try_acquire_owned() else {
            busy_notice(&self.coworker, thread, mention);
            return Some(envelope.id);
        };
        let queued = QueuedMention { mention, slot };
        match thread.sender.try_send(queued) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Closed(mention)) => {
                let admission = Admission {
                    slots: thread.admission.slots.clone(),
                    last_busy_notice: thread.admission.last_busy_notice,
                };
                *thread = spawn_thread_with_admission(
                    &self.coworker,
                    name,
                    Some(thread.finished.clone()),
                    admission,
                );
                let _ = thread.sender.try_send(mention);
            }
            Err(mpsc::error::TrySendError::Full(queued)) => {
                drop(queued.slot);
                busy_notice(&self.coworker, thread, queued.mention);
            }
        }
        Some(envelope.id)
    }
}

fn spawn_thread(coworker: &Arc<Coworker>, name: String, predecessor: Option<Stop>) -> Thread {
    spawn_thread_with_admission(
        coworker,
        name,
        predecessor,
        Admission {
            slots: Arc::new(Semaphore::new(THREAD_QUEUE)),
            last_busy_notice: None,
        },
    )
}

fn busy_notice(coworker: &Arc<Coworker>, thread: &mut Thread, mention: Mention) {
    if thread
        .admission
        .last_busy_notice
        .is_none_or(|last| last.elapsed() >= Duration::from_secs(BUSY_NOTICE_SECONDS))
    {
        thread.admission.last_busy_notice = Some(Instant::now());
        let worker = coworker.clone();
        coworker.runtime.spawn(async move {
            notice(&worker, &mention, BUSY_NOTICE).await;
        });
    }
}

fn spawn_thread_with_admission(
    coworker: &Arc<Coworker>,
    name: String,
    predecessor: Option<Stop>,
    admission: Admission,
) -> Thread {
    let (sender, receiver) = mpsc::channel(THREAD_QUEUE);
    let (finished_tx, finished) = watch::channel(false);
    let worker = coworker.clone();
    coworker.runtime.spawn(async move {
        // An idle worker closes before draining. Its replacement can accept
        // events immediately, but must not start turns ahead of that drain.
        if let Some(predecessor) = predecessor {
            tokio::select! {
                biased;
                () = stop_future(worker.runtime.stop.clone()) => return,
                () = stop_future(predecessor) => {},
            }
        }
        thread_worker(worker, name, receiver).await;
        let _ = finished_tx.send(true);
    });
    Thread {
        sender,
        admission,
        finished,
    }
}

async fn thread_worker(
    coworker: Arc<Coworker>,
    name: String,
    mut queue: mpsc::Receiver<QueuedMention>,
) {
    let mut stop = stop_future(coworker.runtime.stop.clone());
    let mut draining = false;
    loop {
        let mention = if draining {
            if *coworker.runtime.stop.borrow() {
                break;
            }
            match queue.try_recv() {
                Ok(mention) => mention,
                Err(_) => break,
            }
        } else {
            tokio::select! {
                biased;
                () = &mut stop => break,
                next = tokio::time::timeout(Duration::from_secs(IDLE_SECONDS), queue.recv()) => match next {
                    Ok(Some(mention)) => mention,
                    Ok(None) => break,
                    Err(_) => {
                        // Close before draining: accepted sends survive, later sends respawn.
                        queue.close();
                        draining = true;
                        continue;
                    },
                },
            }
        };
        let QueuedMention { mention, slot } = mention;
        drop(slot);
        if !process_mention(&coworker, &name, &mention).await {
            break;
        }
    }
}

async fn process_mention(coworker: &Coworker, name: &str, mention: &Mention) -> bool {
    if mention.text.trim().is_empty() {
        notice(coworker, mention, EMPTY_NOTICE).await;
        return true;
    }
    if mention.text.len() > coworker.config.limits.max_prompt_bytes {
        notice(coworker, mention, TOO_LONG_NOTICE).await;
        return true;
    }
    let mut stop = stop_future(coworker.runtime.stop.clone());
    let permit = tokio::select! {
        biased;
        () = &mut stop => return false,
        result = coworker.runtime.permits.acquire() => result.expect("semaphore stays open"),
    };
    let deadline = Instant::now() + Duration::from_secs(coworker.config.limits.model_turn_seconds);
    // Never race/drop ask: cancellation must interrupt and settle its admitted run.
    let answer = ask::ask(
        &coworker.config.coworkers[coworker.index],
        &mention.text,
        coworker.runtime.resolver.clone(),
        coworker.runtime.selection.clone(),
        deadline,
        Session::Durable {
            store: coworker.store.clone(),
            name: name.into(),
        },
        stop,
    )
    .await;
    drop(permit);
    match answer {
        Ok(answer) => {
            if delivery::post_answer(
                &coworker.api,
                &mention.channel,
                &mention.root_ts,
                &answer,
                &mut stop_future(coworker.runtime.stop.clone()),
            )
            .await
            .is_err()
            {
                coworker.log("Slack delivery failed");
            }
        }
        Err(AskError::Interrupted) => return false,
        Err(AskError::Timeout) => notice(coworker, mention, TIMEOUT_NOTICE).await,
        Err(AskError::Busy) => notice(coworker, mention, BUSY_NOTICE).await,
        Err(_) => notice(coworker, mention, FAILED_NOTICE).await,
    }
    true
}

async fn notice(coworker: &Coworker, mention: &Mention, text: &str) {
    tokio::select! {
        biased;
        () = stop_future(coworker.runtime.stop.clone()) => {},
        result = coworker.api.post_message(&mention.channel, &mention.root_ts, text) => {
            if result.is_err() { coworker.log("Slack delivery failed"); }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn successor_turn_waits_for_idle_drain() {
        let directory = tempfile::tempdir().unwrap();
        let config: Config =
            serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
        let provider = Arc::new(crabber::FakeProvider::scripted(vec![vec![
            crabber::StreamDelta::TextDelta("ok".into()),
            crabber::StreamDelta::Completed,
        ]]));
        let (stop_tx, stop) = watch::channel(false);
        let (log, _) = mpsc::unbounded_channel();
        let runtime = Arc::new(Runtime {
            resolver: provider.clone(),
            selection: Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            },
            permits: Semaphore::new(1),
            stop,
            log,
            workers: Mutex::new(JoinSet::new()),
        });
        let coworker = Arc::new(Coworker {
            config: Arc::new(config),
            index: 0,
            api: Arc::new(
                SlackApi::new(
                    "http://127.0.0.1:9/",
                    slack::Credentials::new("xoxb-test".into(), "xapp-test".into()).unwrap(),
                )
                .unwrap(),
            ),
            identity: Identity {
                team_id: "T1".into(),
                user_id: "UBOT".into(),
                bot_id: "B1".into(),
            },
            store: store::open(directory.path(), "scout").await.unwrap(),
            runtime: runtime.clone(),
        });
        // Represent an idle predecessor whose closed queue is still draining.
        let (drained, predecessor) = watch::channel(false);
        let thread = spawn_thread(&coworker, "slack-c1-1-000001".into(), Some(predecessor));
        thread
            .sender
            .try_send(QueuedMention {
                slot: thread.admission.slots.clone().try_acquire_owned().unwrap(),
                mention: Mention {
                    event_id: "e2".into(),
                    channel: "C1".into(),
                    root_ts: "1.000001".into(),
                    user: "U1".into(),
                    text: "next".into(),
                },
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(provider.requests().len(), 0);
        drained.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while provider.requests().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        shutdown(&stop_tx, &mut JoinSet::new(), &runtime).await;
        assert_eq!(provider.requests().len(), 1);
    }

    async fn test_coworker() -> (Arc<Coworker>, tempfile::TempDir, watch::Sender<bool>) {
        let directory = tempfile::tempdir().unwrap();
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
        value["coworkers"][0]["slack"] = serde_json::json!({"team_id":"T1","allowed_channel_ids":["C1"],"allowed_user_ids":["U1"]});
        let config: Config = serde_json::from_value(value).unwrap();
        let (stop_tx, stop) = watch::channel(false);
        let (log, _) = mpsc::unbounded_channel();
        let runtime = Arc::new(Runtime {
            resolver: Arc::new(crabber::FakeProvider::scripted(vec![])),
            selection: Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            },
            permits: Semaphore::new(0),
            stop,
            log,
            workers: Mutex::new(JoinSet::new()),
        });
        let coworker = Arc::new(Coworker {
            config: Arc::new(config),
            index: 0,
            api: Arc::new(
                SlackApi::new(
                    "http://127.0.0.1:9/",
                    slack::Credentials::new("xoxb-test".into(), "xapp-test".into()).unwrap(),
                )
                .unwrap(),
            ),
            identity: Identity {
                team_id: "T1".into(),
                user_id: "UBOT".into(),
                bot_id: "B1".into(),
            },
            store: store::open(directory.path(), "scout").await.unwrap(),
            runtime,
        });
        (coworker, directory, stop_tx)
    }

    fn test_mention(id: &str) -> Mention {
        Mention {
            event_id: id.into(),
            channel: "C1".into(),
            root_ts: "1.000001".into(),
            user: "U1".into(),
            text: "next".into(),
        }
    }
    fn inbound(id: &str) -> Inbound {
        Inbound::Envelope(socket::Envelope {
            id: id.into(),
            retry_attempt: 0,
            payload: serde_json::json!({"type":"event_callback","team_id":"T1","event_id":id,
                "event":{"type":"app_mention","channel":"C1","user":"U1","ts":"1.000001","text":"next"}}),
        })
    }

    #[tokio::test]
    async fn idle_generations_share_queue_capacity_and_busy_clock() {
        let (coworker, _directory, stop) = test_coworker().await;
        let slots = Arc::new(Semaphore::new(THREAD_QUEUE));
        let (sender, mut predecessor_queue) = mpsc::channel(THREAD_QUEUE);
        for i in 0..THREAD_QUEUE {
            sender
                .try_send(QueuedMention {
                    mention: test_mention(&format!("old{i}")),
                    slot: slots.clone().try_acquire_owned().unwrap(),
                })
                .unwrap();
        }
        predecessor_queue.close();
        let (_finished, finished) = watch::channel(false);
        let last = Instant::now();
        let thread = Thread {
            sender,
            finished,
            admission: Admission {
                slots: slots.clone(),
                last_busy_notice: Some(last),
            },
        };
        let mut handler = Handler {
            coworker: coworker.clone(),
            recent: HashSet::new(),
            order: VecDeque::new(),
            threads: HashMap::from([("slack-c1-1-000001".into(), thread)]),
        };
        assert_eq!(handler.handle(inbound("overflow")), Some("overflow".into()));
        assert_eq!(slots.available_permits(), 0);
        drop(predecessor_queue.try_recv().unwrap());
        // A Closed send creates a successor with the same four-slot budget.
        handler.handle(inbound("successor"));
        let thread = &handler.threads["slack-c1-1-000001"];
        assert!(!thread.sender.is_closed());
        assert_eq!(slots.available_permits(), 0);
        assert_eq!(thread.admission.last_busy_notice, Some(last));
        handler.handle(inbound("overflow-again"));
        assert_eq!(
            handler.threads["slack-c1-1-000001"]
                .admission
                .last_busy_notice,
            Some(last)
        );
        // Expiry re-arms the same busy clock, even while the old generation drains.
        handler
            .threads
            .get_mut("slack-c1-1-000001")
            .unwrap()
            .admission
            .last_busy_notice = Some(Instant::now() - Duration::from_secs(BUSY_NOTICE_SECONDS + 1));
        handler.handle(inbound("overflow-after-expiry"));
        assert!(
            handler.threads["slack-c1-1-000001"]
                .admission
                .last_busy_notice
                .unwrap()
                >= last
        );
        shutdown(&stop, &mut JoinSet::new(), &coworker.runtime).await;
        drop(predecessor_queue);
        assert_eq!(slots.available_permits(), THREAD_QUEUE);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_aborts_stalled_tasks_with_a_fixed_log() {
        let (stop_tx, stop) = watch::channel(false);
        let (log, mut logs) = mpsc::unbounded_channel();
        let runtime = Runtime {
            resolver: Arc::new(crabber::FakeProvider::scripted(vec![])),
            selection: Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            },
            permits: Semaphore::new(1),
            stop,
            log,
            workers: Mutex::new(JoinSet::new()),
        };
        runtime.spawn(std::future::pending());
        shutdown(&stop_tx, &mut JoinSet::new(), &runtime).await;
        assert_eq!(logs.try_recv().unwrap(), "Slack shutdown timed out");
        assert!(runtime.workers.lock().unwrap().is_empty());
    }
    #[test]
    fn ask_future_is_send() {
        fn assert_send<T: Send>(_: T) {}
        let config: Config =
            serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
        assert_send(ask::ask(
            &config.coworkers[0],
            "test",
            Arc::new(crabber::FakeProvider::scripted(vec![])),
            crate::bridge::production_selection(),
            Instant::now(),
            Session::Ephemeral,
            Box::pin(std::future::pending()),
        ));
    }
}
