//! The v1 routes. Every route sits behind the bearer token ([`super::auth`]), and paths, query
//! strings, and bodies are decoded strictly: every failure is one fixed [`ErrorCode`] and no
//! default rejection body of axum reaches a client.
use super::{
    auth::{self, TokenDigest},
    contract::{
        ApiUuid, ApiVersion, BackendKind, CancelRequest, Capabilities, CapabilityLimits,
        ConversationPage, ConversationSnapshot, ConversationState, Coworker, CoworkerId,
        CoworkerList, DependencyHealth, DependencyStatus, ErrorCode, FailureReason, Health,
        HealthDependencies, HealthStatus, InstructionsSummary, MessageRole, OpenConversation,
        RequestContract, RequestReceipt, RequestStatus, SnapshotMessage, SubmitRequest,
        decode_body,
    },
    dispatch::Dispatcher,
    store::{
        ApiStore, Opened, Outcome, RequestRow, RequestState, SnapshotRows, StoreFault, Submitted,
    },
};
use crate::{
    config::{
        Config, CoworkerDefinition,
        constants::{
            API_PAGE_DEFAULT, API_PAGE_MAX, API_SNAPSHOT_BYTES, API_SNAPSHOT_ROWS,
            DEFAULT_QUEUED_PER_CONVERSATION,
        },
    },
    digest::sha256_hex,
    turn::Backend,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{
        DefaultBodyLimit, FromRequest, FromRequestParts, Path, Request, State,
        rejection::PathRejection,
    },
    handler::Handler,
    http::{StatusCode, header, request::Parts},
    middleware,
    response::{IntoResponse, Response},
    routing::{MethodRouter, get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Serialize, de::DeserializeOwned};
use std::{io, sync::Arc};
use tokio::sync::watch;
use url::form_urlencoded;

/// What a listing cursor encodes before the conversation ID it continues after.
const CURSOR_PREFIX: &str = "conversations:";

/// What every handler shares. Built once per `serve`.
#[derive(Clone)]
pub struct ApiState(Arc<Shared>);

struct Shared {
    token: Arc<TokenDigest>,
    config: Arc<Config>,
    max_queued: usize,
    capabilities: Capabilities,
    coworkers: CoworkerList,
    store: ApiStore,
    dispatcher: Dispatcher,
    /// Becomes `true` when the service stops taking requests.
    intake: watch::Receiver<bool>,
}

impl ApiState {
    /// The state for [`router`]. Without an `api` block in `config` the API's defaults apply.
    ///
    /// # Panics
    /// When a coworker ID in `config` is invalid, which [`crate::config::load`] rules out.
    #[must_use]
    pub fn new(
        token: TokenDigest,
        config: Arc<Config>,
        backend: &Backend,
        store: ApiStore,
        dispatcher: Dispatcher,
        intake: watch::Receiver<bool>,
    ) -> Self {
        let max_queued = config
            .api
            .as_ref()
            .map_or(DEFAULT_QUEUED_PER_CONVERSATION, |api| {
                api.max_queued_per_conversation
            });
        let capabilities = Capabilities {
            api_version: ApiVersion::V1,
            request_contract: RequestContract::default(),
            backend: match backend {
                Backend::Local { .. } => BackendKind::Local,
                Backend::Borb(_) => BackendKind::Borb,
            },
            limits: CapabilityLimits {
                max_prompt_bytes: count(config.limits.max_prompt_bytes),
                max_queued_per_conversation: count(max_queued),
                model_turn_seconds: config.limits.model_turn_seconds,
                max_page_size: count(API_PAGE_MAX),
            },
            statuses: RequestStatus::ALL.to_vec(),
            reasons: FailureReason::ALL.to_vec(),
        };
        let coworkers = CoworkerList {
            api_version: ApiVersion::V1,
            coworkers: config.coworkers.iter().map(coworker).collect(),
        };
        Self(Arc::new(Shared {
            token: Arc::new(token),
            config,
            max_queued,
            capabilities,
            coworkers,
            store,
            dispatcher,
            intake,
        }))
    }

    /// The conversation and its newest messages, or `None` when it was never opened.
    async fn snapshot(&self, id: ApiUuid) -> Result<Option<ConversationSnapshot>, StoreFault> {
        let Some(state) = self.0.store.conversation(id).await? else {
            return Ok(None);
        };
        let rows = self.0.store.snapshot_rows(id, API_SNAPSHOT_ROWS).await?;
        Ok(Some(bounded_snapshot(state, &rows)))
    }
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn coworker(def: &CoworkerDefinition) -> Coworker {
    Coworker {
        coworker_id: CoworkerId::parse(&def.id).expect("configured coworker IDs are valid"),
        name: def.name.clone(),
        role: def.role.clone(),
        workspace: def.workspace.into(),
        slack: def.slack.is_some(),
        instructions: InstructionsSummary {
            bytes: count(def.instructions.len()),
            digest: sha256_hex(def.instructions.as_bytes()),
        },
    }
}

/// Every documented operation, behind the bearer token. This is the only way to build the API's
/// router, so no route can be served without authentication.
pub fn router(state: ApiState) -> Router {
    // JSON may spell one byte of text as six (`\u0001`); 4 KiB covers the rest of a body.
    let body_limit = state
        .0
        .config
        .limits
        .max_prompt_bytes
        .saturating_mul(6)
        .saturating_add(4096);
    let token = state.0.token.clone();
    let routes = Router::new()
        .route("/v1/capabilities", read(capabilities))
        .route("/v1/health", read(health))
        .route("/v1/coworkers", read(list_coworkers))
        .route(
            "/v1/conversations",
            get(list_conversations)
                .post(open_conversation)
                .head(refuse(GET_OR_POST))
                .fallback(refuse(GET_OR_POST)),
        )
        .route("/v1/conversations/{conversationId}", read(conversation))
        .route(
            "/v1/conversations/{conversationId}/requests",
            command(submit),
        )
        .route(
            "/v1/conversations/{conversationId}/requests/{requestId}",
            read(request_status),
        )
        .route(
            "/v1/conversations/{conversationId}/requests/{requestId}/cancel",
            command(cancel),
        )
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state);
    // The guard wraps the whole router rather than each route, so it runs before routing and
    // nothing routing adds, such as an `Allow` header, reaches a client without the token.
    Router::new()
        .fallback_service(routes)
        .layer(middleware::from_fn_with_state(token, auth::guard))
}

const GET: &str = "GET";
const POST: &str = "POST";
const GET_OR_POST: &str = "GET,POST";

/// A `GET` route. Axum's `get` also answers `HEAD`, which the document does not list, so `HEAD`
/// is refused like every other method.
fn read<H, T>(handler: H) -> MethodRouter<ApiState>
where
    H: Handler<T, ApiState>,
    T: 'static,
{
    get(handler).head(refuse(GET)).fallback(refuse(GET))
}

/// A `POST` route.
fn command<H, T>(handler: H) -> MethodRouter<ApiState>
where
    H: Handler<T, ApiState>,
    T: 'static,
{
    post(handler).fallback(refuse(POST))
}

async fn not_found() -> ErrorCode {
    ErrorCode::NotFound
}

/// `405 method_not_allowed`, with an `Allow` header naming exactly the documented methods.
/// Axum's own `Allow` would also name `HEAD`, which is refused.
fn refuse(allow: &'static str) -> impl Fn() -> std::future::Ready<Response> + Clone {
    move || {
        std::future::ready(([(header::ALLOW, allow)], ErrorCode::MethodNotAllowed).into_response())
    }
}

/// A request body, decoded by [`decode_body`] within the router's body limit.
pub(crate) struct Body<T>(T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for Body<T> {
    type Rejection = ErrorCode;

    async fn from_request(request: Request, state: &S) -> Result<Self, ErrorCode> {
        let bytes = Bytes::from_request(request, state)
            .await
            .map_err(|rejection| {
                if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
                    ErrorCode::PayloadTooLarge
                } else {
                    ErrorCode::InvalidJson
                }
            })?;
        decode_body(&bytes).map(Self)
    }
}

fn path_error(rejection: &PathRejection) -> ErrorCode {
    match rejection {
        PathRejection::MissingPathParams(_) => ErrorCode::Internal,
        _ => ErrorCode::InvalidInput,
    }
}

fn uuid(text: &str) -> Result<ApiUuid, ErrorCode> {
    ApiUuid::parse(text).ok_or(ErrorCode::InvalidInput)
}

/// The `{conversationId}` of a conversation route.
pub(crate) struct ConversationPath(ApiUuid);

impl<S: Send + Sync> FromRequestParts<S> for ConversationPath {
    type Rejection = ErrorCode;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ErrorCode> {
        let Path(id) = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(|rejection| path_error(&rejection))?;
        uuid(&id).map(Self)
    }
}

/// The `{conversationId}` and `{requestId}` of a request route.
pub(crate) struct RequestPath {
    conversation_id: ApiUuid,
    request_id: ApiUuid,
}

impl<S: Send + Sync> FromRequestParts<S> for RequestPath {
    type Rejection = ErrorCode;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ErrorCode> {
        let Path((conversation, request)) =
            Path::<(String, String)>::from_request_parts(parts, state)
                .await
                .map_err(|rejection| path_error(&rejection))?;
        Ok(Self {
            conversation_id: uuid(&conversation)?,
            request_id: uuid(&request)?,
        })
    }
}

fn query_pairs(parts: &Parts) -> form_urlencoded::Parse<'_> {
    form_urlencoded::parse(parts.uri.query().unwrap_or_default().as_bytes())
}

/// A route that takes no query parameters: any parameter is `invalid_input`.
pub(crate) struct NoQuery;

impl<S: Send + Sync> FromRequestParts<S> for NoQuery {
    type Rejection = ErrorCode;

    fn from_request_parts(
        parts: &mut Parts,
        _: &S,
    ) -> impl Future<Output = Result<Self, ErrorCode>> + Send {
        let checked = match query_pairs(parts).next() {
            None => Ok(Self),
            Some(_) => Err(ErrorCode::InvalidInput),
        };
        std::future::ready(checked)
    }
}

/// `listConversations` parameters. Each may appear once; any other parameter is `invalid_input`.
pub(crate) struct ListQuery {
    limit: usize,
    after: Option<ApiUuid>,
    coworker: Option<CoworkerId>,
}

impl<S: Send + Sync> FromRequestParts<S> for ListQuery {
    type Rejection = ErrorCode;

    fn from_request_parts(
        parts: &mut Parts,
        _: &S,
    ) -> impl Future<Output = Result<Self, ErrorCode>> + Send {
        std::future::ready(Self::parse(parts))
    }
}

impl ListQuery {
    fn parse(parts: &Parts) -> Result<Self, ErrorCode> {
        let (mut limit, mut after, mut coworker) = (None, None, None);
        for (name, value) in query_pairs(parts) {
            let slot = match name.as_ref() {
                "limit" => &mut limit,
                "after" => &mut after,
                "coworkerId" => &mut coworker,
                _ => return Err(ErrorCode::InvalidInput),
            };
            if slot.replace(value.into_owned()).is_some() {
                return Err(ErrorCode::InvalidInput);
            }
        }
        Ok(Self {
            limit: limit.map_or(Ok(API_PAGE_DEFAULT), |text| page_size(&text))?,
            after: after.map(|text| decode_cursor(&text)).transpose()?,
            coworker: coworker
                .map(|text| CoworkerId::parse(&text).ok_or(ErrorCode::InvalidInput))
                .transpose()?,
        })
    }
}

/// Decimal digits without a sign or a leading zero, which `usize::from_str` would accept.
fn page_size(text: &str) -> Result<usize, ErrorCode> {
    if text.starts_with('0') || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ErrorCode::InvalidInput);
    }
    text.parse()
        .ok()
        .filter(|size| (1..=API_PAGE_MAX).contains(size))
        .ok_or(ErrorCode::InvalidInput)
}

fn encode_cursor(after: ApiUuid) -> String {
    URL_SAFE_NO_PAD.encode(format!("{CURSOR_PREFIX}{after}"))
}

fn decode_cursor(text: &str) -> Result<ApiUuid, ErrorCode> {
    let bytes = URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|_| ErrorCode::InvalidInput)?;
    std::str::from_utf8(&bytes)
        .ok()
        .and_then(|cursor| cursor.strip_prefix(CURSOR_PREFIX))
        .and_then(ApiUuid::parse)
        .ok_or(ErrorCode::InvalidInput)
}

/// A request's messages: its text and, once completed, its answer.
fn messages(row: &RequestRow) -> Vec<SnapshotMessage> {
    let request_id = row.request_id();
    let mut messages = vec![SnapshotMessage {
        request_id,
        role: MessageRole::User,
        text: row.text().to_owned(),
    }];
    if let RequestState::Settled(Outcome::Completed(answer)) = row.state() {
        messages.push(SnapshotMessage {
            request_id,
            role: MessageRole::Assistant,
            text: answer.clone(),
        });
    }
    messages
}

/// The JSON length of `value`, without building it.
fn serialized_len(value: &impl Serialize) -> usize {
    struct Count(usize);
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).map_or(usize::MAX, |()| count.0)
}

/// The snapshot of `rows`, dropping whole requests, oldest first, until its JSON fits in
/// [`API_SNAPSHOT_BYTES`].
fn bounded_snapshot(state: ConversationState, rows: &SnapshotRows) -> ConversationSnapshot {
    let exchanges: Vec<_> = rows.rows.iter().map(messages).collect();
    let mut snapshot = ConversationSnapshot {
        api_version: ApiVersion::V1,
        state,
        messages: Vec::new(),
        truncated: false,
    };
    // Each message also costs the comma that separates it from the next.
    let sizes: Vec<usize> = exchanges
        .iter()
        .map(|exchange| {
            exchange.iter().fold(0, |size: usize, message| {
                size.saturating_add(serialized_len(message))
                    .saturating_add(1)
            })
        })
        .collect();
    let mut size = sizes.iter().fold(serialized_len(&snapshot), |size, more| {
        size.saturating_add(*more)
    });
    let mut dropped = 0;
    while size > API_SNAPSHOT_BYTES && dropped < sizes.len() {
        size -= sizes[dropped];
        dropped += 1;
    }
    snapshot.truncated = rows.truncated || dropped > 0;
    snapshot.messages = exchanges.into_iter().skip(dropped).flatten().collect();
    snapshot
}

#[utoipa::path(
    get,
    path = "/v1/capabilities",
    operation_id = "capabilities",
    description = "Contract identifiers, the execution backend, and the configured limits. A pure read; repeat it freely. Reject an unsupported `apiVersion` or `requestContract` before submitting.",
    responses(
        (status = 200, description = "Capabilities.", body = Capabilities),
    ),
)]
pub(crate) async fn capabilities(State(state): State<ApiState>, _: NoQuery) -> Json<Capabilities> {
    Json(state.0.capabilities.clone())
}

#[utoipa::path(
    get,
    path = "/v1/health",
    operation_id = "health",
    description = "Readiness of the gateway store, probed with a ping bounded to 2 seconds. Returns 200 when ready and 503 with the same body shape when not; a dependency failure is reported in the body, never as an error code. A pure read.",
    responses(
        (status = 200, description = "Ready.", body = Health),
        (status = 503, description = "Not ready.", body = Health),
    ),
)]
pub(crate) async fn health(State(state): State<ApiState>, _: NoQuery) -> Response {
    let (code, status, database) = if state.0.store.ping().await {
        (StatusCode::OK, HealthStatus::Ready, DependencyStatus::Ready)
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            HealthStatus::NotReady,
            DependencyStatus::Unavailable,
        )
    };
    let health = Health {
        api_version: ApiVersion::V1,
        status,
        dependencies: HealthDependencies {
            database: DependencyHealth { status: database },
        },
        outstanding_admissions: count(state.0.dispatcher.outstanding_admissions()),
    };
    (code, Json(health)).into_response()
}

#[utoipa::path(
    get,
    path = "/v1/coworkers",
    operation_id = "listCoworkers",
    description = "Every configured coworker in configuration order, with a digest of its instructions and never their text. Coworkers come from configuration and cannot be created over HTTP. A pure read.",
    responses(
        (status = 200, description = "Configured coworkers.", body = CoworkerList),
    ),
)]
pub(crate) async fn list_coworkers(
    State(state): State<ApiState>,
    _: NoQuery,
) -> Json<CoworkerList> {
    Json(state.0.coworkers.clone())
}

#[utoipa::path(
    post,
    path = "/v1/conversations",
    operation_id = "openConversation",
    description = "Open a conversation under a client-chosen `conversationId`. Idempotent: repeating it with the same `coworkerId` returns 201 with the current snapshot, and another `coworkerId` returns 409 `conflict`. Creates no model session; the first request does. After 503 `retry_same_request`, resend the identical body.",
    request_body = OpenConversation,
    responses(
        (status = 201, description = "The conversation exists. A new conversation has no messages.", body = ConversationSnapshot),
    ),
)]
pub(crate) async fn open_conversation(
    State(state): State<ApiState>,
    _: NoQuery,
    Body(open): Body<OpenConversation>,
) -> Result<Response, ErrorCode> {
    if state.0.config.coworker(open.coworker_id.as_str()).is_none() {
        return Err(ErrorCode::NotFound);
    }
    match state
        .0
        .store
        .open_conversation(open.conversation_id, &open.coworker_id)
        .await
    {
        Ok(Opened::Created | Opened::Existing) => {}
        Ok(Opened::Conflict) => return Err(ErrorCode::Conflict),
        Err(StoreFault) => return Err(ErrorCode::RetrySameRequest),
    }
    let snapshot = state
        .snapshot(open.conversation_id)
        .await
        .map_err(|StoreFault| ErrorCode::RetrySameRequest)?
        .ok_or(ErrorCode::Internal)?;
    Ok((StatusCode::CREATED, Json(snapshot)).into_response())
}

#[utoipa::path(
    get,
    path = "/v1/conversations",
    operation_id = "listConversations",
    description = "Conversations opened through this API, in ascending `conversationId` order, which is not chronological. Send `next` back as `after` with the same parameters to read the following page; `null` marks the last page. A pure read; repeat it freely.",
    params(
        ("limit" = Option<u8>, Query, minimum = 1, maximum = 100, description = "Page size from 1 to 100 in decimal, without sign or leading zeros. Defaults to 50."),
        ("after" = Option<String>, Query, max_length = 128, description = "The previous page's `next`, unchanged."),
        ("coworkerId" = inline(Option<CoworkerId>), Query, description = "Only this coworker's conversations. An unconfigured coworker yields an empty page."),
    ),
    responses(
        (status = 200, description = "One page of conversations.", body = ConversationPage),
    ),
)]
pub(crate) async fn list_conversations(
    State(state): State<ApiState>,
    query: ListQuery,
) -> Result<Json<ConversationPage>, ErrorCode> {
    let unconfigured = query
        .coworker
        .as_ref()
        .is_some_and(|id| state.0.config.coworker(id.as_str()).is_none());
    let (conversations, next) = if unconfigured {
        (Vec::new(), None)
    } else {
        let page = state
            .0
            .store
            .list_conversations(query.coworker.as_ref(), query.after, query.limit)
            .await
            .map_err(|StoreFault| ErrorCode::Unavailable)?;
        (page.conversations, page.next.map(encode_cursor))
    };
    Ok(Json(ConversationPage {
        api_version: ApiVersion::V1,
        conversations,
        next,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/conversations/{conversationId}",
    operation_id = "conversation",
    description = "The conversation's phase and the messages of its API requests, oldest first: each request's text and, once completed, its answer. Turns taken through the CLI or Slack are not shown. `truncated` is true when older messages were dropped to bound the response. A pure read.",
    params(
        ("conversationId" = inline(ApiUuid), Path, description = "The conversation's UUID."),
    ),
    responses(
        (status = 200, description = "The conversation snapshot.", body = ConversationSnapshot),
    ),
)]
pub(crate) async fn conversation(
    State(state): State<ApiState>,
    ConversationPath(id): ConversationPath,
    _: NoQuery,
) -> Result<Json<ConversationSnapshot>, ErrorCode> {
    state
        .snapshot(id)
        .await
        .map_err(|StoreFault| ErrorCode::Unavailable)?
        .map(Json)
        .ok_or(ErrorCode::NotFound)
}

#[utoipa::path(
    post,
    path = "/v1/conversations/{conversationId}/requests",
    operation_id = "submit",
    description = "One admission attempt for the identity (`conversationId`, `requestId`). 202 means the request is durably recorded and will run after the conversation's earlier requests; it does not mean the request ran. Resending the identical text returns the current receipt and never starts a second turn; changed text returns 409 `conflict`. After a lost response or 503 `retry_same_request`, reconcile with `requestStatus` and resend the identical body.",
    params(
        ("conversationId" = inline(ApiUuid), Path, description = "The conversation's UUID."),
    ),
    request_body = SubmitRequest,
    responses(
        (status = 202, description = "The request is recorded.", body = RequestReceipt),
    ),
)]
pub(crate) async fn submit(
    State(state): State<ApiState>,
    ConversationPath(conversation_id): ConversationPath,
    _: NoQuery,
    Body(submit): Body<SubmitRequest>,
) -> Result<Response, ErrorCode> {
    let shared = &state.0;
    if submit.text.trim().is_empty() || submit.text.contains('\0') {
        return Err(ErrorCode::InvalidInput);
    }
    if submit.text.len() > shared.config.limits.max_prompt_bytes {
        return Err(ErrorCode::PayloadTooLarge);
    }
    if *shared.intake.borrow() {
        return Err(ErrorCode::RetrySameRequest);
    }
    let conversation = shared
        .store
        .conversation(conversation_id)
        .await
        .map_err(|StoreFault| ErrorCode::RetrySameRequest)?
        .ok_or(ErrorCode::NotFound)?;
    if shared
        .config
        .coworker(conversation.coworker_id.as_str())
        .is_none()
    {
        return Err(ErrorCode::NotFound);
    }
    let submitted = shared
        .store
        .submit(
            conversation_id,
            submit.request_id,
            &submit.text,
            shared.max_queued,
        )
        .await
        .map_err(|StoreFault| ErrorCode::RetrySameRequest)?;
    match submitted {
        Submitted::Inserted(row) | Submitted::Existing(row) => {
            shared.dispatcher.wake(conversation_id);
            Ok((StatusCode::ACCEPTED, Json(row.into_receipt())).into_response())
        }
        Submitted::TextConflict => Err(ErrorCode::Conflict),
        Submitted::QueueFull => Err(ErrorCode::QueueFull),
        Submitted::NoConversation => Err(ErrorCode::NotFound),
    }
}

#[utoipa::path(
    get,
    path = "/v1/conversations/{conversationId}/requests/{requestId}",
    operation_id = "requestStatus",
    description = "The receipt for one request, with `result.text` once it is `completed`. A pure read that never starts a turn; poll it to reconcile after any submit or cancel.",
    params(
        ("conversationId" = inline(ApiUuid), Path, description = "The conversation's UUID."),
        ("requestId" = inline(ApiUuid), Path, description = "The request's UUID."),
    ),
    responses(
        (status = 200, description = "The request's receipt.", body = RequestReceipt),
    ),
)]
pub(crate) async fn request_status(
    State(state): State<ApiState>,
    path: RequestPath,
    _: NoQuery,
) -> Result<Json<RequestReceipt>, ErrorCode> {
    state
        .0
        .store
        .request(path.conversation_id, path.request_id)
        .await
        .map_err(|StoreFault| ErrorCode::Unavailable)?
        .map(|row| Json(row.into_receipt()))
        .ok_or(ErrorCode::NotFound)
}

#[utoipa::path(
    post,
    path = "/v1/conversations/{conversationId}/requests/{requestId}/cancel",
    operation_id = "cancel",
    description = "Record cancellation intent. An `accepted` request becomes `cancelled` without running; a `running` one becomes `cancellation_requested` until its turn stops. A request that already concluded keeps its outcome, including a completed result. Repeating it returns the current receipt.",
    params(
        ("conversationId" = inline(ApiUuid), Path, description = "The conversation's UUID."),
        ("requestId" = inline(ApiUuid), Path, description = "The request's UUID."),
    ),
    request_body = CancelRequest,
    responses(
        (status = 202, description = "The cancellation intent is recorded.", body = RequestReceipt),
    ),
)]
pub(crate) async fn cancel(
    State(state): State<ApiState>,
    path: RequestPath,
    _: NoQuery,
    Body(CancelRequest {}): Body<CancelRequest>,
) -> Result<Response, ErrorCode> {
    let row = state
        .0
        .dispatcher
        .cancel(path.conversation_id, path.request_id)
        .await
        .map_err(|StoreFault| ErrorCode::RetrySameRequest)?
        .ok_or(ErrorCode::NotFound)?;
    Ok((StatusCode::ACCEPTED, Json(row.into_receipt())).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::{auth::ApiTokenSource, dispatch::Wiring},
        turn::TurnPermits,
    };
    use axum::http::Request as HttpRequest;
    use std::collections::HashMap;
    use tokio::sync::mpsc;
    use tower::ServiceExt;

    const TOKEN: &str = "routes-unit-test-token-0123456789abcdef";
    const CONVERSATION: &str = "00000000-0000-0000-0000-0000000000c1";

    struct Fixture {
        _directory: tempfile::TempDir,
        store: ApiStore,
        router: Router,
        intake: watch::Sender<bool>,
    }

    async fn fixture() -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let mut config: Config =
            serde_json::from_str(include_str!("../../../../config.example.json")).unwrap();
        config.state_dir = directory.path().to_path_buf();
        let config = Arc::new(config);
        let store = ApiStore::open(directory.path()).await.unwrap();
        store
            .open_conversation(conversation(), &CoworkerId::parse("scout").unwrap())
            .await
            .unwrap();
        let backend = Arc::new(Backend::Local {
            resolver: Arc::new(crabber::FakeProvider::scripted(vec![])),
            selection: crabber::Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            },
        });
        let (intake, open) = watch::channel(false);
        let (log, _) = mpsc::unbounded_channel();
        let dispatcher = Dispatcher::new(Wiring {
            store: store.clone(),
            backend: backend.clone(),
            permits: TurnPermits::new(1),
            config: config.clone(),
            locals: HashMap::new(),
            stop: open.clone(),
            intake: open.clone(),
            log,
        });
        let token = ApiTokenSource::Fixed(TOKEN.into()).resolve().unwrap();
        let state = ApiState::new(token, config, &backend, store.clone(), dispatcher, open);
        Fixture {
            _directory: directory,
            store,
            router: router(state),
            intake,
        }
    }

    fn conversation() -> ApiUuid {
        ApiUuid::parse(CONVERSATION).unwrap()
    }

    #[tokio::test]
    async fn submissions_while_stopping_are_retried_and_not_recorded() {
        let fixture = fixture().await;
        fixture.intake.send_replace(true);
        let request = "00000000-0000-0000-0000-0000000000d1";
        let response = fixture
            .router
            .clone()
            .oneshot(
                HttpRequest::post(format!("/v1/conversations/{CONVERSATION}/requests"))
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .body(axum::body::Body::from(format!(
                        r#"{{"requestId":"{request}","text":"late"}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"error":"retry_same_request"}"#);
        let recorded = fixture
            .store
            .request(conversation(), ApiUuid::parse(request).unwrap())
            .await
            .unwrap();
        assert!(recorded.is_none());
    }

    async fn snapshot_of(fixture: &Fixture, texts: &[String]) -> ConversationSnapshot {
        for (n, text) in texts.iter().enumerate() {
            let request = ApiUuid::parse(&format!("00000000-0000-0000-0000-{n:012x}")).unwrap();
            let submitted = fixture.store.submit(conversation(), request, text, 16);
            assert!(matches!(submitted.await.unwrap(), Submitted::Inserted(_)));
        }
        let state = fixture.store.conversation(conversation()).await.unwrap();
        let rows = fixture
            .store
            .snapshot_rows(conversation(), API_SNAPSHOT_ROWS)
            .await
            .unwrap();
        bounded_snapshot(state.unwrap(), &rows)
    }

    #[tokio::test]
    async fn snapshots_drop_the_oldest_requests_to_fit() {
        let few = fixture().await;
        let small = snapshot_of(&few, &["one".into(), "two".into()]).await;
        assert!(!small.truncated);
        assert_eq!(small.messages.len(), 2);
        let large = fixture().await;
        let texts: Vec<_> = (0..5).map(|n| n.to_string().repeat(1_048_576)).collect();
        let snapshot = snapshot_of(&large, &texts).await;
        assert!(snapshot.truncated);
        assert_eq!(snapshot.messages.len(), 3);
        assert_eq!(snapshot.messages[0].text, texts[2]);
        assert!(serde_json::to_vec(&snapshot).unwrap().len() <= API_SNAPSHOT_BYTES);
    }

    #[test]
    fn page_sizes_are_plain_decimals_in_range() {
        assert_eq!(page_size("1"), Ok(1));
        assert_eq!(page_size("100"), Ok(100));
        for text in [
            "",
            "0",
            "01",
            "101",
            "+1",
            "-1",
            " 1",
            "1e1",
            "99999999999999999999999",
        ] {
            assert_eq!(page_size(text), Err(ErrorCode::InvalidInput), "{text}");
        }
    }

    #[test]
    fn cursors_round_trip_and_reject_anything_else() {
        let id = conversation();
        let cursor = encode_cursor(id);
        assert_eq!(decode_cursor(&cursor), Ok(id));
        let other_prefix = URL_SAFE_NO_PAD.encode(format!("requests:{id}"));
        let padded =
            base64::engine::general_purpose::URL_SAFE.encode(format!("{CURSOR_PREFIX}{id}"));
        let uppercase =
            URL_SAFE_NO_PAD.encode(format!("{CURSOR_PREFIX}{}", CONVERSATION.to_uppercase()));
        for text in [
            "",
            "!!",
            other_prefix.as_str(),
            padded.as_str(),
            uppercase.as_str(),
        ] {
            assert_eq!(decode_cursor(text), Err(ErrorCode::InvalidInput), "{text}");
        }
    }
}
