use crate::{
    ask::{AskError, Cancel, INTERRUPT_SETTLE_SECONDS},
    config::{BorbConfig, CoworkerDefinition},
    worker::{PROTOCOL_VERSION, TurnRequest, TurnState, WORKER_PORT},
};
use async_trait::async_trait;
use borb_client::{
    Client, Method, TokenSource,
    borb_api::{ApiError, BorbId, BorbSpec, Readiness, SecretMount, SecretRef},
};
use serde::de::DeserializeOwned;
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Mutex as AsyncMutex, time::Instant};
use uuid::Uuid;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const LIST_TIMEOUT: Duration = Duration::from_secs(120);
const RESPONSE_LIMIT: usize = 8 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
#[error("remote execution configuration unavailable")]
pub struct ExecutionError;

struct EnvironmentToken(String);

#[async_trait]
impl TokenSource for EnvironmentToken {
    async fn token(&self) -> Result<String, ApiError> {
        Ok(self.0.clone())
    }
}

#[derive(Clone)]
pub struct Remote {
    client: Client,
    image: String,
    prefix: String,
    secret: String,
    lifecycle: Arc<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>,
}

impl Remote {
    /// Construct a client with the caller's environment-sourced `BORB_TOKEN`.
    ///
    /// # Errors
    /// Returns a fixed error for invalid connection configuration or credentials.
    pub fn new(config: &BorbConfig, token: String) -> Result<Self, ExecutionError> {
        if token.is_empty()
            || token.len() > 16_384
            || !token.is_ascii()
            || token
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err(ExecutionError);
        }
        let client = Client::new(&config.url, Arc::new(EnvironmentToken(token)))
            .and_then(|client| client.with_request_timeout(REQUEST_TIMEOUT))
            .map_err(|_| ExecutionError)?;
        Ok(Self {
            client,
            image: config.image.clone(),
            prefix: config.name_prefix.clone(),
            secret: config.model_secret.clone(),
            lifecycle: Arc::default(),
        })
    }

    /// Run exactly one remote submission, then poll its existing operation.
    ///
    /// # Errors
    /// Returns fixed execution errors without falling back to a local provider.
    pub async fn answer(
        &self,
        def: &CoworkerDefinition,
        conversation: &str,
        prompt: &str,
        deadline: Instant,
        mut cancel: Cancel,
    ) -> Result<String, AskError> {
        let borb = wait(self.ensure_running(def, deadline), deadline, &mut cancel).await?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(AskError::Timeout);
        }
        let timeout_ms = u64::try_from(remaining.as_millis()).map_err(|_| AskError::Run)?;
        let expires_at_unix_ms = SystemTime::now()
            .checked_add(remaining)
            .and_then(|expiry| expiry.duration_since(UNIX_EPOCH).ok())
            .and_then(|expiry| u64::try_from(expiry.as_millis()).ok())
            .ok_or(AskError::Run)?;
        let mut coworker = def.clone();
        coworker.slack = None;
        let body = serde_json::to_vec(&TurnRequest {
            version: PROTOCOL_VERSION,
            coworker,
            conversation: conversation.to_owned(),
            prompt: prompt.to_owned(),
            timeout_ms,
            expires_at_unix_ms,
        })
        .map_err(|_| AskError::Run)?;
        let path = format!("v2/turns/{}", Uuid::new_v4());
        let result = wait(
            self.turn(&borb, &path, body, deadline),
            deadline,
            &mut cancel,
        )
        .await;
        match result {
            Ok(result) => result,
            Err(error) => {
                let settled = self.cancel(&borb, &path).await;
                if matches!(error, AskError::Interrupted | AskError::Timeout)
                    && let Ok(TurnState::Finished { result: Ok(answer) }) = settled
                {
                    return Ok(answer);
                }
                Err(error)
            }
        }
    }

    /// Read conversation history from the resident worker, waking it if needed.
    ///
    /// # Errors
    /// Returns fixed remote failures, never local history.
    pub async fn list(&self, def: &CoworkerDefinition) -> Result<String, AskError> {
        let deadline = Instant::now() + LIST_TIMEOUT;
        let borb = self.ensure_running(def, deadline).await?;
        self.request(&borb, Method::GET, "v2/conversations", None, deadline)
            .await
    }

    async fn turn(
        &self,
        borb: &BorbId,
        path: &str,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<Result<String, AskError>, AskError> {
        let mut state: TurnState = self
            .request(borb, Method::PUT, path, Some(body), deadline)
            .await?;
        loop {
            match state {
                TurnState::Finished { result } => return Ok(result),
                TurnState::Running => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    state = self
                        .request(borb, Method::GET, path, None, deadline)
                        .await?;
                }
            }
        }
    }

    async fn cancel(&self, borb: &BorbId, path: &str) -> Result<TurnState, AskError> {
        let deadline = Instant::now() + Duration::from_secs(INTERRUPT_SETTLE_SECONDS);
        let mut state = self
            .request(
                borb,
                Method::POST,
                &format!("{path}/cancel"),
                None,
                deadline,
            )
            .await?;
        while matches!(state, TurnState::Running) {
            tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(100)))
                .await;
            if Instant::now() >= deadline {
                return Err(AskError::Timeout);
            }
            state = self
                .request(borb, Method::GET, path, None, deadline)
                .await?;
        }
        Ok(state)
    }

    async fn ensure_running(
        &self,
        def: &CoworkerDefinition,
        deadline: Instant,
    ) -> Result<BorbId, AskError> {
        let lock = self
            .lifecycle
            .lock()
            .map_err(|_| AskError::Run)?
            .entry(def.id.clone())
            .or_default()
            .clone();
        let work = async {
            let _guard = lock.lock().await;
            let name = format!("{}-{}", self.prefix, def.id);
            let spec = BorbSpec {
                name: name.clone(),
                from_snapshot: None,
                arch: None,
                min_isolation: None,
                disk_gib: Some(20),
                image: self.image.clone(),
                command: Some(vec!["/usr/local/bin/crabber-coworkers".into()]),
                args: Some(vec![
                    "worker".into(),
                    "--coworker-id".into(),
                    def.id.clone(),
                ]),
                env: BTreeMap::new(),
                secrets: vec![SecretRef {
                    name: self.secret.clone(),
                    mount: SecretMount::Env,
                }],
                ports: vec![WORKER_PORT],
                readiness: Some(Readiness {
                    port: WORKER_PORT,
                    path: Some("/healthz".into()),
                }),
                cpu_millis: Some(1000),
                memory_mib: Some(1024),
                idle_timeout_seconds: Some(900),
                labels: BTreeMap::new(),
            };
            let borb = self
                .client
                .ensure_running(
                    &name,
                    spec,
                    deadline.saturating_duration_since(Instant::now()),
                )
                .await
                .map_err(|_| deadline_error(deadline))?;
            if borb.restart_required {
                return Err(AskError::Run);
            }
            Ok(borb.id)
        };
        tokio::time::timeout_at(deadline, work)
            .await
            .map_err(|_| AskError::Timeout)?
    }

    async fn request<T: DeserializeOwned>(
        &self,
        borb: &BorbId,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<T, AskError> {
        let request_deadline = deadline.min(Instant::now() + REQUEST_TIMEOUT);
        let work = async {
            let mut request = self
                .client
                .proxy(borb, WORKER_PORT)
                .wake(false)
                .method(method)
                .path(path);
            if let Some(body) = body {
                request = request
                    .header(
                        borb_client::header::CONTENT_TYPE,
                        borb_client::header::HeaderValue::from_static("application/json"),
                    )
                    .body(body);
            }
            let mut response = request.send().await.map_err(|_| AskError::Run)?;
            if !response.status().is_success()
                || response
                    .content_length()
                    .is_some_and(|length| length > RESPONSE_LIMIT as u64)
            {
                return Err(AskError::Run);
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| AskError::Run)? {
                if chunk.len() > RESPONSE_LIMIT.saturating_sub(bytes.len()) {
                    return Err(AskError::Run);
                }
                bytes.extend_from_slice(&chunk);
            }
            serde_json::from_slice(&bytes).map_err(|_| AskError::Run)
        };
        tokio::time::timeout_at(request_deadline, work)
            .await
            .map_err(|_| deadline_error(deadline))?
    }
}

fn deadline_error(deadline: Instant) -> AskError {
    if Instant::now() >= deadline {
        AskError::Timeout
    } else {
        AskError::Run
    }
}

async fn wait<T>(
    work: impl Future<Output = Result<T, AskError>>,
    deadline: Instant,
    cancel: &mut Cancel,
) -> Result<T, AskError> {
    tokio::select! {
        biased;
        result = work => result,
        () = cancel => Err(AskError::Interrupted),
        () = tokio::time::sleep_until(deadline) => Err(AskError::Timeout),
    }
}
