//! Supervise the frontends: Slack sockets with their ordered, durable per-thread workers, and the
//! HTTP API with its dispatcher. They share one backend, one turn cap, and one shutdown budget.
use crate::{
    api::{
        auth::{TokenDigest, TokenError},
        dispatch::{Dispatcher, Wiring},
        routes::{self, ApiState},
        store::{ApiStore, Faults, OpenError},
    },
    app::{ServeOptions, install_cancel},
    ask::{self, AskError, Cancel, Session},
    config::{self, ApiConfig, Config},
    slack::{
        self,
        api::SlackApi,
        delivery,
        ingress::{self, Classified, Identity, Mention},
        socket::{self, Inbound},
    },
    store,
    turn::{self, Backend, BackendError, LocalError, LocalState, Served, TurnPermits},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::Write,
    path::{Path, PathBuf},
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
/// How long the HTTP API's requests in flight get to finish once intake closes. A client that
/// stalls past it is cut off, so the rest of the shutdown budget is left for running turns.
pub const API_GRACE_SECONDS: u64 = 3;
pub const BUSY_NOTICE_SECONDS: u64 = 30;
pub const IDLE_SECONDS: u64 = 300;

pub const EMPTY_NOTICE: &str = "Mention me with a question and I will answer in this thread.";
pub const TOO_LONG_NOTICE: &str = "That message is too long for me to answer.";
pub const BUSY_NOTICE: &str =
    "I am still working on earlier messages in this thread. Please try again shortly.";
pub const TIMEOUT_NOTICE: &str = "I ran out of time answering that.";
pub const FAILED_NOTICE: &str = "I could not answer that.";
pub const SHUTDOWN_TIMED_OUT: &str = "serve shutdown timed out";

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error("nothing to serve; add a slack block or an api block")]
    NothingToServe,
    #[error("the HTTP API requires Unix")]
    Unsupported,
    #[error("OPENCODE_GO_API_KEY is not set")]
    MissingApiKey,
    #[error("cannot configure borb execution; check BORB_TOKEN and borb settings")]
    Borb,
    #[error(transparent)]
    Credentials(#[from] slack::CredentialError),
    #[error(transparent)]
    ApiToken(#[from] TokenError),
    #[error("cannot install signal handlers")]
    Signal,
    #[error("Slack authentication failed for {0}")]
    Auth(String),
    #[error(transparent)]
    Store(#[from] store::StoreOpenError),
    #[error("cannot use workspace")]
    Workspace,
    #[error("Slack connection stopped for {0}")]
    Connection(String),
    #[error("cannot open API store")]
    ApiStore,
    #[error("API store is in use")]
    ApiInUse,
    #[error("cannot bind API listener")]
    ApiListen,
    #[error("API server stopped")]
    ApiServer,
    #[error("interrupted")]
    Interrupted,
}
impl From<BackendError> for ServeError {
    fn from(error: BackendError) -> Self {
        match error {
            BackendError::MissingApiKey => Self::MissingApiKey,
            BackendError::Borb => Self::Borb,
        }
    }
}
impl From<OpenError> for ServeError {
    fn from(error: OpenError) -> Self {
        match error {
            OpenError::InUse => Self::ApiInUse,
            OpenError::Fault => Self::ApiStore,
        }
    }
}
impl From<LocalError> for ServeError {
    fn from(error: LocalError) -> Self {
        match error {
            LocalError::Interrupted => Self::Interrupted,
            LocalError::Workspace => Self::Workspace,
            LocalError::Store(error) => Self::Store(error),
        }
    }
}
impl ServeError {
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Config(_)
            | Self::NothingToServe
            | Self::Unsupported
            | Self::MissingApiKey
            | Self::Borb
            | Self::Credentials(_)
            | Self::ApiToken(_) => 2,
            Self::Interrupted => 130,
            Self::Signal
            | Self::Auth(_)
            | Self::Store(_)
            | Self::Connection(_)
            | Self::Workspace
            | Self::ApiStore
            | Self::ApiInUse
            | Self::ApiListen
            | Self::ApiServer => 1,
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

fn install_renewal() -> Result<Cancel, ServeError> {
    #[cfg(unix)]
    {
        let mut signal =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
                .map_err(|_| ServeError::Signal)?;
        Ok(Box::pin(async move {
            signal.recv().await;
        }))
    }
    #[cfg(not(unix))]
    Ok(Box::pin(std::future::pending()))
}

// Abort the cancellation listener on every exit, including partial startup failure.
struct Supervisor(JoinHandle<()>);
impl Supervisor {
    fn notify(cancel: Cancel, stops: Vec<watch::Sender<bool>>) -> Self {
        Self(tokio::spawn(async move {
            cancel.await;
            for stop in stops {
                let _ = stop.send(true);
            }
        }))
    }
}
impl Drop for Supervisor {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Runtime {
    backend: Arc<Backend>,
    permits: TurnPermits,
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
    local: Option<Arc<LocalState>>,
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

/// A task the supervisor watches. Any of them ending early stops the service.
enum Frontend {
    Slack(String),
    Api,
}

/// What the HTTP API leaves for shutdown to finish.
struct Hosted {
    dispatcher: Dispatcher,
    store: ApiStore,
}

impl Hosted {
    /// Wait for the dispatcher's tasks. Once intake closes they start no request, and running
    /// turns settle as they end or as the stop signal interrupts them.
    fn join(&self) -> Cancel {
        let dispatcher = self.dispatcher.clone();
        Box::pin(async move { dispatcher.join().await })
    }

    /// Give up on the dispatcher's tasks. One of them may have dropped an admission that crabber
    /// is still preparing, so this process keeps the store's lock until it exits.
    fn abandon(&self) {
        self.dispatcher.abort();
        self.store.keep_lock_until_exit();
    }
}

/// The HTTP API, ready to serve.
struct Api {
    hosted: Hosted,
    listener: tokio::net::TcpListener,
    router: axum::Router,
}

/// The supervised tasks, and which frontend each one is.
#[derive(Default)]
struct Tasks {
    sockets: JoinSet<Result<(), ServeError>>,
    frontends: HashMap<tokio::task::Id, Frontend>,
}

impl Tasks {
    fn spawn(
        &mut self,
        frontend: Frontend,
        task: impl Future<Output = Result<(), ServeError>> + Send + 'static,
    ) {
        let id = self.sockets.spawn(task).id();
        self.frontends.insert(id, frontend);
    }
}

/// What the HTTP API takes from [`ServeOptions`].
struct ApiInputs {
    token: TokenDigest,
    listener: Option<tokio::net::TcpListener>,
    faults: Option<Arc<Faults>>,
}

/// Serve until a signal arrives. Validates all local inputs, credentials, and Slack identities
/// before creating any stores.
pub(crate) async fn run(
    config_path: PathBuf,
    options: ServeOptions,
    err: &mut (dyn Write + Send),
) -> Result<(), ServeError> {
    let ServeOptions {
        provider,
        selection,
        cancel,
        renewal,
        credentials,
        api_token,
        api_listener,
        slack_base,
        api_faults,
    } = options;
    let config = load(&config_path)?;
    let backend = Arc::new(Backend::new(&config, provider, selection)?);
    let tokens = config
        .coworkers
        .iter()
        .enumerate()
        .filter(|(_, def)| def.slack.is_some())
        .map(|(index, def)| credentials.resolve(&def.id).map(|token| (index, token)))
        .collect::<Result<Vec<_>, _>>()?;
    let api_inputs = match &config.api {
        Some(_) => Some(ApiInputs {
            token: api_token.resolve()?,
            listener: api_listener,
            faults: api_faults,
        }),
        None => None,
    };
    let cancel = match cancel {
        Some(cancel) => cancel,
        None => install_cancel().map_err(|_| ServeError::Signal)?,
    };
    let renewal = match renewal {
        Some(renewal) => renewal,
        None => install_renewal()?,
    };
    let (admission_tx, admission) = watch::channel(false);
    let (stop_tx, stop) = watch::channel(false);
    let _supervisor = Supervisor::notify(cancel, vec![stop_tx.clone(), admission_tx.clone()]);
    let _renewal = Supervisor::notify(renewal, vec![admission_tx.clone()]);
    let mut startup_stop = stop_future(stop.clone());
    let authenticated = authenticate(&config, tokens, &slack_base, &mut startup_stop).await?;
    let (log, mut logs) = mpsc::unbounded_channel();
    let runtime = Arc::new(Runtime {
        backend,
        permits: TurnPermits::new(config.limits.max_concurrent_turns),
        stop: stop.clone(),
        log,
        workers: Mutex::new(JoinSet::new()),
    });
    let served = served(&config);
    let locals = turn::open_served(&config, &runtime.backend, served, &mut startup_stop).await?;
    let opened = open_api(
        &config,
        api_inputs,
        &runtime,
        &locals,
        &admission,
        &mut startup_stop,
    );
    let api = opened.await?;
    let mut tasks = Tasks::default();
    let hosted = api.map(|api| {
        if let Ok(address) = api.listener.local_addr() {
            let _ = writeln!(err, "API listening on {address}");
        }
        let serving = serve_api(api.listener, api.router, admission.clone());
        tasks.spawn(Frontend::Api, serving);
        api.hosted
    });
    for (index, api, identity, url) in authenticated {
        let coworker = Arc::new(Coworker {
            config: config.clone(),
            index,
            api,
            identity,
            local: locals.get(&config.coworkers[index].id).cloned(),
            runtime: runtime.clone(),
        });
        let frontend = Frontend::Slack(coworker.id().to_owned());
        tasks.spawn(frontend, coworker_loop(coworker, url, admission.clone()));
    }
    let Tasks {
        mut sockets,
        frontends,
    } = tasks;
    let outcome = supervise(&mut sockets, &frontends, admission, &mut logs, err).await;
    let _ = admission_tx.send(true);
    let graceful = outcome.is_ok();
    settle_workers(graceful, &stop_tx, &mut sockets, &runtime, hosted).await;
    while let Ok(line) = logs.try_recv() {
        let _ = writeln!(err, "{line}");
    }
    if outcome.is_ok() {
        let _ = writeln!(err, "stopped");
    }
    outcome
}

/// The configuration, if it names a frontend that this platform can serve.
fn load(path: &Path) -> Result<Arc<Config>, ServeError> {
    let config = config::load(path)?;
    if config.slack_coworkers().next().is_none() && config.api.is_none() {
        return Err(ServeError::NothingToServe);
    }
    if config.api.is_some() && !cfg!(unix) {
        return Err(ServeError::Unsupported);
    }
    Ok(Arc::new(config))
}

/// Slack-only deployments open nothing for coworkers that only the API could reach.
fn served(config: &Config) -> Served {
    if config.api.is_some() {
        Served::Every
    } else {
        Served::Slack
    }
}

type Authenticated = (usize, Arc<SlackApi>, Identity, url::Url);

/// Check each Slack coworker's identity and open its socket URL.
async fn authenticate(
    config: &Config,
    tokens: Vec<(usize, slack::Credentials)>,
    slack_base: &str,
    startup_stop: &mut Cancel,
) -> Result<Vec<Authenticated>, ServeError> {
    let mut authenticated = Vec::new();
    for (index, token) in tokens {
        let def = &config.coworkers[index];
        let api = Arc::new(
            SlackApi::new(slack_base, token).map_err(|_| ServeError::Auth(def.id.clone()))?,
        );
        let identity = tokio::select! {
            biased;
            () = &mut *startup_stop => return Err(ServeError::Interrupted),
            result = api.auth_test() => result.map_err(|_| ServeError::Auth(def.id.clone()))?,
        };
        if identity.team_id != def.slack.as_ref().expect("Slack coworker").team_id {
            return Err(ServeError::Auth(def.id.clone()));
        }
        let url = tokio::select! {
            biased;
            () = &mut *startup_stop => return Err(ServeError::Interrupted),
            result = api.connections_open() => result.map_err(|_| ServeError::Auth(def.id.clone()))?,
        };
        authenticated.push((index, api, identity, url));
    }
    Ok(authenticated)
}

/// Open the API store, bind the listener, and set the dispatcher to the requests an earlier
/// process left. The listener binds before the dispatcher starts, so a failure here leaves no
/// turn running. Submissions get `503` once `admission` closes. Without `inputs` there is no API.
async fn open_api(
    config: &Arc<Config>,
    inputs: Option<ApiInputs>,
    runtime: &Runtime,
    locals: &HashMap<String, Arc<LocalState>>,
    admission: &Stop,
    startup_stop: &mut Cancel,
) -> Result<Option<Api>, ServeError> {
    let Some(inputs) = inputs else {
        return Ok(None);
    };
    let settings = config.api.clone().unwrap_or_default();
    let store = tokio::select! {
        biased;
        () = &mut *startup_stop => return Err(ServeError::Interrupted),
        opened = ApiStore::open(&config.state_dir) => opened?,
    };
    let store = match inputs.faults {
        Some(faults) => store.with_faults(faults),
        None => store,
    };
    let listener = match inputs.listener {
        Some(listener) => listener,
        None => bind(&settings).await?,
    };
    let dispatcher = Dispatcher::new(Wiring {
        store: store.clone(),
        backend: runtime.backend.clone(),
        permits: runtime.permits.clone(),
        config: config.clone(),
        locals: locals.clone(),
        stop: runtime.stop.clone(),
        intake: admission.clone(),
        log: runtime.log.clone(),
    });
    dispatcher
        .wake_pending()
        .await
        .map_err(|_| ServeError::ApiStore)?;
    let state = ApiState::new(
        inputs.token,
        config.clone(),
        &runtime.backend,
        store.clone(),
        dispatcher.clone(),
        admission.clone(),
    );
    Ok(Some(Api {
        hosted: Hosted { dispatcher, store },
        listener,
        router: routes::router(state),
    }))
}

async fn bind(settings: &ApiConfig) -> Result<tokio::net::TcpListener, ServeError> {
    let address = settings.listen_addr().ok_or(ServeError::ApiListen)?;
    tokio::net::TcpListener::bind(address)
        .await
        .map_err(|_| ServeError::ApiListen)
}

/// Serve until intake stops, then give the requests in flight [`API_GRACE_SECONDS`] to finish.
/// Axum bounds no client's headers or body, so one that stalls would otherwise hold the whole
/// shutdown. Ending any earlier is a failure.
async fn serve_api(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    admission: Stop,
) -> Result<(), ServeError> {
    let serving =
        axum::serve(listener, router).with_graceful_shutdown(stop_future(admission.clone()));
    let closed = stop_future(admission.clone());
    tokio::select! {
        _ = std::future::IntoFuture::into_future(serving) => {},
        () = async {
            closed.await;
            tokio::time::sleep(Duration::from_secs(API_GRACE_SECONDS)).await;
        } => {},
    }
    if *admission.borrow() {
        Ok(())
    } else {
        Err(ServeError::ApiServer)
    }
}

async fn settle_workers(
    graceful: bool,
    stop: &watch::Sender<bool>,
    sockets: &mut JoinSet<Result<(), ServeError>>,
    runtime: &Runtime,
    api: Option<Hosted>,
) {
    if graceful && !*runtime.stop.borrow() {
        let timed_out = {
            let finish = finish_workers(sockets, runtime, api.as_ref().map(Hosted::join));
            tokio::pin!(finish);
            tokio::select! {
                biased;
                () = stop_future(runtime.stop.clone()) => tokio::time::timeout(
                    Duration::from_secs(SHUTDOWN_SECONDS), &mut finish,
                ).await.is_err(),
                () = &mut finish => false,
            }
        };
        if timed_out {
            abandon(sockets, runtime, api.as_ref());
            while sockets.join_next().await.is_some() {}
        }
    } else {
        shutdown(stop, sockets, runtime, api).await;
    }
}

async fn supervise(
    sockets: &mut JoinSet<Result<(), ServeError>>,
    frontends: &HashMap<tokio::task::Id, Frontend>,
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
                Some(Err(error)) => break Err(match &frontends[&error.id()] {
                    Frontend::Slack(name) => ServeError::Connection(name.clone()),
                    Frontend::Api => ServeError::ApiServer,
                }),
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
    api: Option<Hosted>,
) {
    let _ = stop.send(true);
    if tokio::time::timeout(
        Duration::from_secs(SHUTDOWN_SECONDS),
        finish_workers(sockets, runtime, api.as_ref().map(Hosted::join)),
    )
    .await
    .is_err()
    {
        abandon(sockets, runtime, api.as_ref());
        while sockets.join_next().await.is_some() {}
    }
}

/// The shutdown budget ran out: abort every task that is left. The caller then reaps them.
fn abandon(sockets: &mut JoinSet<Result<(), ServeError>>, runtime: &Runtime, api: Option<&Hosted>) {
    let _ = runtime.log.send(SHUTDOWN_TIMED_OUT.into());
    sockets.abort_all();
    runtime
        .workers
        .lock()
        .expect("worker set poisoned")
        .abort_all();
    if let Some(api) = api {
        api.abandon();
    }
}

/// Wait for the sockets, then the Slack workers, then `api`, the dispatcher's join.
async fn finish_workers(
    sockets: &mut JoinSet<Result<(), ServeError>>,
    runtime: &Runtime,
    api: Option<Cancel>,
) {
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
    if let Some(api) = api {
        api.await;
    }
}

async fn coworker_loop(
    coworker: Arc<Coworker>,
    url: url::Url,
    admission: Stop,
) -> Result<(), ServeError> {
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
        stop_future(admission),
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
                *thread = spawn_thread(&self.coworker, name, Some(thread));
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

fn spawn_thread(coworker: &Arc<Coworker>, name: String, previous: Option<&Thread>) -> Thread {
    let (admission, predecessor) = match previous {
        Some(previous) => (
            Admission {
                slots: previous.admission.slots.clone(),
                last_busy_notice: previous.admission.last_busy_notice,
            },
            Some(previous.finished.clone()),
        ),
        None => (
            Admission {
                slots: Arc::new(Semaphore::new(THREAD_QUEUE)),
                last_busy_notice: None,
            },
            None,
        ),
    };
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
    loop {
        let mention = tokio::select! {
            biased;
            () = &mut stop => break,
            next = tokio::time::timeout(Duration::from_secs(IDLE_SECONDS), queue.recv()) => match next {
                Ok(Some(mention)) => mention,
                Ok(None) => break,
                Err(_) => {
                    // Closing preserves accepted sends; recv drains them before None.
                    queue.close();
                    continue;
                },
            },
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
        permit = coworker.runtime.permits.acquire() => permit,
    };
    let deadline = Instant::now() + Duration::from_secs(coworker.config.limits.model_turn_seconds);
    let def = &coworker.config.coworkers[coworker.index];
    let answer = match coworker.runtime.backend.as_ref() {
        Backend::Local {
            resolver,
            selection,
        } => {
            let local = coworker
                .local
                .as_ref()
                .expect("local backend owns its state");
            ask::ask(
                def,
                &local.workspace,
                &mention.text,
                resolver.clone(),
                selection.clone(),
                deadline,
                Session::Durable {
                    store: local.store.clone(),
                    name: name.into(),
                },
                stop,
            )
            .await
        }
        Backend::Borb(remote) => {
            remote
                .answer(def, name, &mention.text, deadline, stop)
                .await
        }
    };
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
    use crabber::Selection;

    #[tokio::test]
    async fn successor_turn_waits_for_idle_drain() {
        let directory = tempfile::tempdir().unwrap();
        let mut config: Config =
            serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
        for def in &mut config.coworkers {
            def.workspace = crate::config::WorkspaceAccess::Disabled;
        }
        let provider = Arc::new(crabber::FakeProvider::scripted(vec![vec![
            crabber::StreamDelta::TextDelta("ok".into()),
            crabber::StreamDelta::Completed,
        ]]));
        let (stop_tx, stop) = watch::channel(false);
        let (log, _) = mpsc::unbounded_channel();
        let runtime = Arc::new(Runtime {
            backend: Arc::new(Backend::Local {
                resolver: provider.clone(),
                selection: Selection {
                    provider_id: "fake".into(),
                    model_id: "scripted".into(),
                },
            }),
            permits: TurnPermits::new(1),
            stop,
            log,
            workers: Mutex::new(JoinSet::new()),
        });
        let coworker = Arc::new(Coworker {
            local: Some(Arc::new(LocalState {
                workspace: crate::workspace::Workspace::disabled(&config.coworkers[0]).unwrap(),
                store: store::open(directory.path(), "scout").await.unwrap(),
            })),
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
            runtime: runtime.clone(),
        });
        // Represent an idle predecessor whose closed queue is still draining.
        let (drained, predecessor) = watch::channel(false);
        let (old_sender, _old_queue) = mpsc::channel(THREAD_QUEUE);
        let previous = Thread {
            sender: old_sender,
            finished: predecessor,
            admission: Admission {
                slots: Arc::new(Semaphore::new(THREAD_QUEUE)),
                last_busy_notice: None,
            },
        };
        let thread = spawn_thread(&coworker, "slack-c1-1-000001".into(), Some(&previous));
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
        shutdown(&stop_tx, &mut JoinSet::new(), &runtime, None).await;
        assert_eq!(provider.requests().len(), 1);
    }

    async fn test_coworker() -> (Arc<Coworker>, tempfile::TempDir, watch::Sender<bool>) {
        let directory = tempfile::tempdir().unwrap();
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
        value["coworkers"][0]["slack"] = serde_json::json!({"team_id":"T1","allowed_channel_ids":["C1"],"allowed_user_ids":["U1"]});
        let mut config: Config = serde_json::from_value(value).unwrap();
        for def in &mut config.coworkers {
            def.workspace = crate::config::WorkspaceAccess::Disabled;
        }
        let (stop_tx, stop) = watch::channel(false);
        let (log, _) = mpsc::unbounded_channel();
        let runtime = Arc::new(Runtime {
            backend: Arc::new(Backend::Local {
                resolver: Arc::new(crabber::FakeProvider::scripted(vec![])),
                selection: Selection {
                    provider_id: "fake".into(),
                    model_id: "scripted".into(),
                },
            }),
            permits: TurnPermits::new(0),
            stop,
            log,
            workers: Mutex::new(JoinSet::new()),
        });
        let coworker = Arc::new(Coworker {
            local: Some(Arc::new(LocalState {
                workspace: crate::workspace::Workspace::disabled(&config.coworkers[0]).unwrap(),
                store: store::open(directory.path(), "scout").await.unwrap(),
            })),
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
        shutdown(&stop, &mut JoinSet::new(), &coworker.runtime, None).await;
        drop(predecessor_queue);
        assert_eq!(slots.available_permits(), THREAD_QUEUE);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_aborts_stalled_tasks_with_a_fixed_log() {
        let (stop_tx, stop) = watch::channel(false);
        let (log, mut logs) = mpsc::unbounded_channel();
        let runtime = Runtime {
            backend: Arc::new(Backend::Local {
                resolver: Arc::new(crabber::FakeProvider::scripted(vec![])),
                selection: Selection {
                    provider_id: "fake".into(),
                    model_id: "scripted".into(),
                },
            }),
            permits: TurnPermits::new(1),
            stop,
            log,
            workers: Mutex::new(JoinSet::new()),
        };
        runtime.spawn(std::future::pending());
        shutdown(&stop_tx, &mut JoinSet::new(), &runtime, None).await;
        assert_eq!(logs.try_recv().unwrap(), SHUTDOWN_TIMED_OUT);
        assert!(runtime.workers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_stranded_admission_is_abandoned_at_the_budget_and_keeps_the_lock() {
        use crate::api::{
            contract::{ApiUuid, CoworkerId},
            dispatch::ADMISSION_OUTSTANDING,
            store::{MarkedRunning, Submitted},
        };
        let directory = tempfile::tempdir().unwrap();
        let mut config: Config =
            serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
        config.state_dir = directory.path().to_path_buf();
        for def in &mut config.coworkers {
            def.workspace = crate::config::WorkspaceAccess::Disabled;
        }
        let backend = Backend::Local {
            resolver: Arc::new(crabber::FakeProvider::scripted(vec![])),
            selection: Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            },
        };
        let mut never: Cancel = Box::pin(std::future::pending());
        let locals = turn::open_all(&config, &backend, &mut never).await.unwrap();
        let store = ApiStore::open(directory.path()).await.unwrap();
        let conversation = ApiUuid::parse("00000000-0000-0000-0000-000000000001").unwrap();
        let request = ApiUuid::parse("00000000-0000-0000-0000-00000000000a").unwrap();
        let scout = CoworkerId::parse("scout").unwrap();
        store.open_conversation(conversation, &scout).await.unwrap();
        let submitted = store.submit(conversation, request, "synthetic request", 16);
        let Submitted::Inserted(row) = submitted.await.unwrap() else {
            panic!("a new request is inserted");
        };
        let MarkedRunning::Started(row) = store.mark_running(row.seq()).await.unwrap() else {
            panic!("a queued request starts");
        };
        let (stop_tx, stop) = watch::channel(false);
        let (log, mut logs) = mpsc::unbounded_channel();
        let runtime = Runtime {
            backend: Arc::new(backend),
            permits: TurnPermits::new(1),
            stop: stop.clone(),
            log: log.clone(),
            workers: Mutex::new(JoinSet::new()),
        };
        let dispatcher = Dispatcher::new(Wiring {
            store: store.clone(),
            backend: runtime.backend.clone(),
            permits: runtime.permits.clone(),
            config: Arc::new(config),
            locals,
            stop: stop.clone(),
            intake: stop,
            log,
        });
        dispatcher.strand(&row).await;
        let hosted = Hosted { dispatcher, store };

        let began = Instant::now();
        shutdown(&stop_tx, &mut JoinSet::new(), &runtime, Some(hosted)).await;
        let took = began.elapsed();
        assert!(took >= Duration::from_secs(SHUTDOWN_SECONDS), "{took:?}");
        assert!(took < Duration::from_secs(SHUTDOWN_SECONDS + 5), "{took:?}");
        assert_eq!(logs.try_recv().unwrap(), ADMISSION_OUTSTANDING);
        assert_eq!(logs.try_recv().unwrap(), SHUTDOWN_TIMED_OUT);
        // The aborted admission let go of its handles; the process still holds the lock.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(matches!(
            ApiStore::open(directory.path()).await,
            Err(OpenError::InUse)
        ));
    }

    #[tokio::test]
    async fn idle_worker_closes_an_empty_queue() {
        let (coworker, _directory, _stop) = test_coworker().await;
        let (sender, receiver) = mpsc::channel(THREAD_QUEUE);
        tokio::time::pause();
        let task = tokio::spawn(thread_worker(
            coworker,
            "slack-c1-1-000001".into(),
            receiver,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(IDLE_SECONDS)).await;
        task.await.unwrap();
        assert!(sender.is_closed());
    }

    #[tokio::test]
    async fn closed_queue_processes_every_accepted_mention() {
        let (mut coworker, _directory, _stop) = test_coworker().await;
        let provider = Arc::new(crabber::FakeProvider::scripted(vec![
            vec![
                crabber::StreamDelta::TextDelta("ok".into()),
                crabber::StreamDelta::Completed,
            ];
            2
        ]));
        let runtime = Arc::get_mut(&mut Arc::get_mut(&mut coworker).unwrap().runtime).unwrap();
        runtime.backend = Arc::new(Backend::Local {
            resolver: provider.clone(),
            selection: Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            },
        });
        runtime.permits = TurnPermits::new(1);
        let (sender, mut receiver) = mpsc::channel(THREAD_QUEUE);
        let slots = Arc::new(Semaphore::new(THREAD_QUEUE));
        for id in ["first", "second"] {
            let mut mention = test_mention(id);
            mention.text = id.into();
            sender
                .try_send(QueuedMention {
                    mention,
                    slot: slots.clone().try_acquire_owned().unwrap(),
                })
                .unwrap();
        }
        receiver.close();
        tokio::time::timeout(
            Duration::from_secs(5),
            thread_worker(coworker, "slack-c1-1-000001".into(), receiver),
        )
        .await
        .unwrap();
        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        for (request, expected) in requests.iter().zip(["first", "second"]) {
            assert!(
                matches!(&request.messages.last().unwrap().parts[0].content, crabber::core::ContentBlock::Text{text} if text==expected)
            );
        }
        assert_eq!(slots.available_permits(), THREAD_QUEUE);
    }
    #[test]
    fn ask_future_is_send() {
        fn assert_send<T: Send>(_: T) {}
        let mut config: Config =
            serde_json::from_str(include_str!("../../../config.example.json")).unwrap();
        for def in &mut config.coworkers {
            def.workspace = crate::config::WorkspaceAccess::Disabled;
        }
        assert_send(ask::ask(
            &config.coworkers[0],
            &crate::workspace::Workspace::disabled(&config.coworkers[0]).unwrap(),
            "test",
            Arc::new(crabber::FakeProvider::scripted(vec![])),
            crate::bridge::production_selection(),
            Instant::now(),
            Session::Ephemeral,
            Box::pin(std::future::pending()),
        ));
    }
}
