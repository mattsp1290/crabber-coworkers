use super::contract::{
    ApiUuid, CancelRequest, Capabilities, ConversationPage, ConversationSnapshot, CoworkerId,
    CoworkerList, Error, ErrorCode, Health, OpenConversation, RequestReceipt, SubmitRequest,
};
use axum::{
    Json, Router,
    handler::Handler,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{MethodRouter, get, post},
};

/// Every documented operation, routed to its handler. WP4 rebuilds this as `router(state)`.
pub fn router() -> Router {
    Router::new()
        .route("/v1/capabilities", read(capabilities))
        .route("/v1/health", read(health))
        .route("/v1/coworkers", read(list_coworkers))
        .route(
            "/v1/conversations",
            read(list_conversations).post(open_conversation),
        )
        .route("/v1/conversations/{conversationId}", read(conversation))
        .route("/v1/conversations/{conversationId}/requests", post(submit))
        .route(
            "/v1/conversations/{conversationId}/requests/{requestId}",
            read(request_status),
        )
        .route(
            "/v1/conversations/{conversationId}/requests/{requestId}/cancel",
            post(cancel),
        )
}

/// A `GET` route. Axum's `get` also answers `HEAD`, which the document does not list, so `HEAD`
/// is routed to `405` explicitly.
fn read<H, T>(handler: H) -> MethodRouter
where
    H: Handler<T, ()>,
    T: 'static,
{
    get(handler).head(method_not_allowed)
}

async fn method_not_allowed() -> ErrorCode {
    ErrorCode::MethodNotAllowed
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
pub async fn capabilities() -> Response {
    not_implemented()
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
pub async fn health() -> Response {
    not_implemented()
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
pub async fn list_coworkers() -> Response {
    not_implemented()
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
pub async fn open_conversation() -> Response {
    not_implemented()
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
pub async fn list_conversations() -> Response {
    not_implemented()
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
pub async fn conversation() -> Response {
    not_implemented()
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
pub async fn submit() -> Response {
    not_implemented()
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
pub async fn request_status() -> Response {
    not_implemented()
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
pub async fn cancel() -> Response {
    not_implemented()
}

fn not_implemented() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(Error {
            error: ErrorCode::Internal,
        }),
    )
        .into_response()
}
