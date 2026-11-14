use crate::{
    ask::AskError,
    config::{self, constants::MAX_ID_BYTES},
};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, de::DeserializeOwned};
use std::fmt;
use utoipa::{
    PartialSchema, ToSchema,
    openapi::{
        RefOr, Schema,
        schema::{KnownFormat, ObjectBuilder, SchemaFormat, Type},
    },
};

pub const UUID_PATTERN: &str = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";
pub const COWORKER_ID_PATTERN: &str = "^[a-z0-9]+(-[a-z0-9]+)*$";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, ToSchema)]
pub enum ApiVersion {
    #[default]
    #[serde(rename = "1")]
    V1,
}

/// A UUID in its canonical lowercase hyphenated form, the only form the API accepts or emits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ApiUuid(uuid::Uuid);

impl ApiUuid {
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let id = uuid::Uuid::try_parse(text).ok()?;
        (id.hyphenated().to_string() == text).then_some(Self(id))
    }
}

impl fmt::Display for ApiUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0.hyphenated(), f)
    }
}

impl Serialize for ApiUuid {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ApiUuid {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).ok_or_else(|| de::Error::custom("expected a lowercase hyphenated UUID"))
    }
}

impl PartialSchema for ApiUuid {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::String)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Uuid)))
            .pattern(Some(UUID_PATTERN))
            .into()
    }
}

impl ToSchema for ApiUuid {}

/// A configured coworker ID, validated by the same rule as the configuration.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CoworkerId(String);

impl CoworkerId {
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        config::valid_id(text).then(|| Self(text.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for CoworkerId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for CoworkerId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).ok_or_else(|| de::Error::custom("expected a coworker ID"))
    }
}

impl PartialSchema for CoworkerId {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::String)
            .max_length(Some(MAX_ID_BYTES))
            .pattern(Some(COWORKER_ID_PATTERN))
            .into()
    }
}

impl ToSchema for CoworkerId {}

/// A fixed public error code. No error body carries any other detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidJson,
    InvalidInput,
    Unauthorized,
    NotFound,
    MethodNotAllowed,
    Conflict,
    QueueFull,
    PayloadTooLarge,
    Internal,
    RetrySameRequest,
    Unavailable,
}

impl ErrorCode {
    #[must_use]
    pub const fn status(self) -> StatusCode {
        match self {
            Self::InvalidJson | Self::InvalidInput => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::Conflict | Self::QueueFull => StatusCode::CONFLICT,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RetrySameRequest | Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

impl IntoResponse for ErrorCode {
    fn into_response(self) -> Response {
        (self.status(), Json(Error { error: self })).into_response()
    }
}

/// Decodes a request body. Anything but a single JSON object is `invalid_json`: a derived
/// `Deserialize` also accepts a JSON array, so the top-level shape is checked before `T` sees it.
/// A well-formed object that `T` rejects is `invalid_input`.
///
/// # Errors
/// [`ErrorCode::InvalidJson`] for malformed JSON or a non-object top level, and
/// [`ErrorCode::InvalidInput`] for an object `T` does not accept.
pub fn decode_body<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ErrorCode> {
    let Ok(serde_json::Value::Object(_)) = serde_json::from_slice(bytes) else {
        return Err(ErrorCode::InvalidJson);
    };
    // Decoding the bytes rather than the parsed map keeps serde's duplicate-field rejection,
    // which a `Value` (last key wins) would silently drop.
    serde_json::from_slice(bytes).map_err(|_| ErrorCode::InvalidInput)
}

/// The only error body.
#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
pub struct Error {
    pub error: ErrorCode,
}

/// Lifecycle of a submitted request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RequestStatus {
    /// Durably queued; no turn started.
    Accepted,
    /// The dispatcher started this request's turn.
    Running,
    /// The committed answer is in `result.text`; never overwritten by a later cancellation.
    Completed,
    /// Cancellation was recorded for a running turn that has not concluded.
    CancellationRequested,
    /// Cancelled before its turn started, or its running turn was interrupted by that cancellation.
    Cancelled,
    /// The turn concluded without an answer; see `reason`.
    Failed,
    /// The gateway cannot establish whether a turn committed; see `reason`.
    Uncertain,
}

impl RequestStatus {
    pub const ALL: [Self; 7] = [
        Self::Accepted,
        Self::Running,
        Self::Completed,
        Self::CancellationRequested,
        Self::Cancelled,
        Self::Failed,
        Self::Uncertain,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::CancellationRequested => "cancellation_requested",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
            Self::Uncertain => "uncertain",
        }
    }

    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|status| status.as_str() == text)
    }
}

/// Why a request ended `failed` or `uncertain`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureReason {
    /// Another frontend or an orphaned run holds the conversation's session.
    ConversationBusy,
    Timeout,
    /// Service shutdown interrupted the turn. Resubmit under a new `requestId`.
    Interrupted,
    WorkspaceUnavailable,
    StoreUnavailable,
    AgentUnavailable,
    /// The answer may have committed; no result is returned.
    CleanupFailed,
    ModelFailed,
    NoAnswer,
    /// Unreachable after submit validation.
    InvalidRequest,
    /// The coworker's instructions, behavior version, or the text changed since the original admission.
    ConfigurationChanged,
    /// The conversation's coworker is no longer configured.
    CoworkerRemoved,
    /// An earlier process ended while the turn was unsettled.
    ProcessStopped,
}

impl FailureReason {
    pub const ALL: [Self; 13] = [
        Self::ConversationBusy,
        Self::Timeout,
        Self::Interrupted,
        Self::WorkspaceUnavailable,
        Self::StoreUnavailable,
        Self::AgentUnavailable,
        Self::CleanupFailed,
        Self::ModelFailed,
        Self::NoAnswer,
        Self::InvalidRequest,
        Self::ConfigurationChanged,
        Self::CoworkerRemoved,
        Self::ProcessStopped,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConversationBusy => "conversation_busy",
            Self::Timeout => "timeout",
            Self::Interrupted => "interrupted",
            Self::WorkspaceUnavailable => "workspace_unavailable",
            Self::StoreUnavailable => "store_unavailable",
            Self::AgentUnavailable => "agent_unavailable",
            Self::CleanupFailed => "cleanup_failed",
            Self::ModelFailed => "model_failed",
            Self::NoAnswer => "no_answer",
            Self::InvalidRequest => "invalid_request",
            Self::ConfigurationChanged => "configuration_changed",
            Self::CoworkerRemoved => "coworker_removed",
            Self::ProcessStopped => "process_stopped",
        }
    }

    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|reason| reason.as_str() == text)
    }
}

impl From<&AskError> for FailureReason {
    fn from(error: &AskError) -> Self {
        match error {
            AskError::Busy => Self::ConversationBusy,
            AskError::Timeout => Self::Timeout,
            AskError::Interrupted => Self::Interrupted,
            AskError::Workspace => Self::WorkspaceUnavailable,
            AskError::Conversation => Self::StoreUnavailable,
            AskError::Build => Self::AgentUnavailable,
            AskError::Cleanup => Self::CleanupFailed,
            AskError::Run | AskError::NotCompleted => Self::ModelFailed,
            AskError::NoAnswer => Self::NoAnswer,
            AskError::UnknownCoworker
            | AskError::EmptyPrompt
            | AskError::PromptTooLarge
            | AskError::InvalidConversation => Self::InvalidRequest,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAccess {
    Disabled,
    ReadOnly,
    ReadWrite,
}

impl From<config::WorkspaceAccess> for WorkspaceAccess {
    fn from(access: config::WorkspaceAccess) -> Self {
        match access {
            config::WorkspaceAccess::Disabled => Self::Disabled,
            config::WorkspaceAccess::ReadOnly => Self::ReadOnly,
            config::WorkspaceAccess::ReadWrite => Self::ReadWrite,
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct InstructionsSummary {
    pub bytes: u64,
    /// Lowercase hex SHA-256 of the configured instructions.
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub digest: String,
}

/// A configured coworker. Never carries instruction text or Slack identifiers.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Coworker {
    #[schema(inline)]
    pub coworker_id: CoworkerId,
    pub name: String,
    pub role: String,
    #[schema(inline)]
    pub workspace: WorkspaceAccess,
    /// Whether this coworker also answers Slack mentions.
    pub slack: bool,
    #[schema(inline)]
    pub instructions: InstructionsSummary,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CoworkerList {
    #[schema(inline)]
    pub api_version: ApiVersion,
    /// Every configured coworker, in configuration order.
    pub coworkers: Vec<Coworker>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OpenConversation {
    #[schema(inline)]
    pub conversation_id: ApiUuid,
    #[schema(inline)]
    pub coworker_id: CoworkerId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConversationPhase {
    Idle,
    Running,
}

/// `phase` is `running` while any request in the conversation is `accepted`, `running`, or `cancellation_requested`.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConversationState {
    #[schema(inline)]
    pub conversation_id: ApiUuid,
    #[schema(inline)]
    pub coworker_id: CoworkerId,
    #[schema(inline)]
    pub phase: ConversationPhase,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConversationPage {
    #[schema(inline)]
    pub api_version: ApiVersion,
    pub conversations: Vec<ConversationState>,
    /// Opaque cursor for the next page, or `null` on the last page.
    #[schema(required = true)]
    pub next: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotMessage {
    #[schema(inline)]
    pub request_id: ApiUuid,
    #[schema(inline)]
    pub role: MessageRole,
    /// The submitted text for `user`, the committed `result.text` for `assistant`.
    pub text: String,
}

/// Messages from this conversation's API requests only, oldest first.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConversationSnapshot {
    #[schema(inline)]
    pub api_version: ApiVersion,
    pub state: ConversationState,
    pub messages: Vec<SnapshotMessage>,
    /// Whether older messages were dropped to bound the response.
    pub truncated: bool,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubmitRequest {
    #[schema(inline)]
    pub request_id: ApiUuid,
    /// Exact UTF-8 input, compared byte for byte on resubmission. Must contain a non-whitespace character and no NUL. The configured `limits.max_prompt_bytes`, reported by `GET /v1/capabilities`, applies; `x-maxUtf8Bytes` is the hard ceiling.
    // utoipa 6 `Extensions` is a `HashMap`, so a second `x-` key on this schema would make the output order nondeterministic.
    #[schema(min_length = 1, extensions(("x-maxUtf8Bytes" = json!(config::constants::MAX_PROMPT_BYTES))))]
    pub text: String,
}

/// Must be exactly `{}`.
#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CancelRequest {}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AcceptanceBoundary {
    #[default]
    SqliteOutbox,
}

#[derive(Clone, Copy, Debug, Default, Serialize, ToSchema)]
pub struct Acceptance {
    #[schema(inline)]
    pub boundary: AcceptanceBoundary,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct RequestResult {
    pub text: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Cancellation {
    /// RFC 3339 UTC time the cancellation was first recorded.
    #[schema(format = DateTime)]
    pub requested_at: String,
}

/// `result` appears only when `status` is `completed`, `reason` only when it is `failed` or `uncertain`, and `cancellation` once cancellation has been requested.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RequestReceipt {
    #[schema(inline)]
    pub api_version: ApiVersion,
    #[schema(inline)]
    pub conversation_id: ApiUuid,
    #[schema(inline)]
    pub request_id: ApiUuid,
    #[schema(inline)]
    pub acceptance: Acceptance,
    pub status: RequestStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(inline, nullable = false)]
    pub result: Option<RequestResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub reason: Option<FailureReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(inline, nullable = false)]
    pub cancellation: Option<Cancellation>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, ToSchema)]
pub enum RequestContract {
    #[default]
    #[serde(rename = "sqlite-outbox-keyed-admission-v1")]
    SqliteOutboxKeyedAdmissionV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    Local,
    Borb,
}

#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CapabilityLimits {
    pub max_prompt_bytes: u64,
    pub max_queued_per_conversation: u64,
    pub model_turn_seconds: u64,
    pub max_page_size: u64,
}

/// Contract identifiers and configured limits. Reject an unsupported `apiVersion` or `requestContract`.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    #[schema(inline)]
    pub api_version: ApiVersion,
    #[schema(inline)]
    pub request_contract: RequestContract,
    #[schema(inline)]
    pub backend: BackendKind,
    #[schema(inline)]
    pub limits: CapabilityLimits,
    pub statuses: Vec<RequestStatus>,
    pub reasons: Vec<FailureReason>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ready,
    NotReady,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DependencyStatus {
    Ready,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
pub struct DependencyHealth {
    #[schema(inline)]
    pub status: DependencyStatus,
}

#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
pub struct HealthDependencies {
    #[schema(inline)]
    pub database: DependencyHealth,
}

/// The same body is returned with `200` when ready and `503` when not.
#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Health {
    #[schema(inline)]
    pub api_version: ApiVersion,
    #[schema(inline)]
    pub status: HealthStatus,
    #[schema(inline)]
    pub dependencies: HealthDependencies,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ID: &str = "0b5e8f1c-3d2a-4e6f-9a7b-1c2d3e4f5a6b";

    #[test]
    fn request_bodies_reject_unknown_fields() {
        assert!(
            serde_json::from_value::<OpenConversation>(
                json!({"conversationId": ID, "coworkerId": "scout", "extra": 1})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<SubmitRequest>(
                json!({"requestId": ID, "text": "hi", "extra": 1})
            )
            .is_err()
        );
        assert!(serde_json::from_value::<CancelRequest>(json!({"extra": 1})).is_err());
        assert!(serde_json::from_value::<CancelRequest>(json!({})).is_ok());
    }

    fn decode_all(body: &str) -> [Result<(), ErrorCode>; 3] {
        let bytes = body.as_bytes();
        [
            decode_body::<OpenConversation>(bytes).map(drop),
            decode_body::<SubmitRequest>(bytes).map(drop),
            decode_body::<CancelRequest>(bytes).map(drop),
        ]
    }

    #[test]
    fn request_bodies_must_be_one_json_object() {
        let pair = format!(r#"["{ID}","hi"]"#);
        for body in [
            "[]",
            pair.as_str(),
            "null",
            r#""x""#,
            "1",
            "",
            "{",
            "{} {}",
            "{\"requestId\":",
        ] {
            assert_eq!(decode_all(body), [Err(ErrorCode::InvalidJson); 3], "{body}");
        }
        assert_eq!(
            decode_body::<CancelRequest>(&[b'{', 0xff, b'}']).map(drop),
            Err(ErrorCode::InvalidJson)
        );
    }

    #[test]
    fn request_bodies_decode_objects_or_reject_them_as_input() {
        let open = format!(r#"{{"conversationId":"{ID}","coworkerId":"scout"}}"#);
        let submit = format!(r#"{{"requestId":"{ID}","text":"hi"}}"#);
        assert!(decode_body::<OpenConversation>(open.as_bytes()).is_ok());
        assert_eq!(
            decode_body::<SubmitRequest>(submit.as_bytes())
                .unwrap()
                .text,
            "hi"
        );
        assert!(decode_body::<CancelRequest>(b" {} ").is_ok());
        assert_eq!(
            decode_all(r#"{"extra":1}"#),
            [Err(ErrorCode::InvalidInput); 3]
        );
        let duplicate = format!(r#"{{"requestId":"{ID}","text":"a","text":"b"}}"#);
        let uppercase = format!(r#"{{"requestId":"{}","text":"hi"}}"#, ID.to_uppercase());
        for body in [duplicate.as_str(), uppercase.as_str(), r#"{"text":"hi"}"#] {
            assert_eq!(
                decode_body::<SubmitRequest>(body.as_bytes()).map(drop),
                Err(ErrorCode::InvalidInput),
                "{body}"
            );
        }
    }

    #[test]
    fn request_bodies_require_every_field() {
        assert!(serde_json::from_value::<SubmitRequest>(json!({"requestId": ID})).is_err());
        assert!(serde_json::from_value::<OpenConversation>(json!({"conversationId": ID})).is_err());
    }

    #[test]
    fn submit_accepts_only_canonical_uuids() {
        let submit = |id: &str| {
            serde_json::from_value::<SubmitRequest>(json!({"requestId": id, "text": "hi"}))
        };
        let parsed = submit(ID).unwrap();
        assert_eq!(parsed.request_id.to_string(), ID);
        assert_eq!(parsed.text, "hi");
        for id in [
            ID.to_uppercase(),
            format!("{{{ID}}}"),
            ID.replace('-', ""),
            format!("urn:uuid:{ID}"),
            String::new(),
        ] {
            assert!(submit(&id).is_err(), "{id}");
        }
    }

    #[test]
    fn open_conversation_validates_coworker_ids() {
        let open = |id: &str| {
            serde_json::from_value::<OpenConversation>(
                json!({"conversationId": ID, "coworkerId": id}),
            )
        };
        assert_eq!(open("scout-2").unwrap().coworker_id.as_str(), "scout-2");
        for id in ["", "Scout", "a--b", "-a", "a-", &"a".repeat(65)] {
            assert!(open(id).is_err(), "{id}");
        }
    }

    #[test]
    fn api_version_serializes_to_one() {
        assert_eq!(serde_json::to_value(ApiVersion::V1).unwrap(), json!("1"));
    }

    #[test]
    fn receipt_omits_absent_optional_fields() {
        let receipt = RequestReceipt {
            api_version: ApiVersion::V1,
            conversation_id: ApiUuid::parse(ID).unwrap(),
            request_id: ApiUuid::parse(ID).unwrap(),
            acceptance: Acceptance::default(),
            status: RequestStatus::Accepted,
            result: None,
            reason: None,
            cancellation: None,
        };
        assert_eq!(
            serde_json::to_value(&receipt).unwrap(),
            json!({
                "apiVersion": "1",
                "conversationId": ID,
                "requestId": ID,
                "acceptance": {"boundary": "sqlite_outbox"},
                "status": "accepted",
            })
        );
    }

    #[test]
    fn vocabulary_strings_match_their_wire_form() {
        for status in RequestStatus::ALL {
            assert_eq!(
                serde_json::to_value(status).unwrap(),
                json!(status.as_str())
            );
            assert_eq!(RequestStatus::parse(status.as_str()), Some(status));
        }
        for reason in FailureReason::ALL {
            assert_eq!(
                serde_json::to_value(reason).unwrap(),
                json!(reason.as_str())
            );
            assert_eq!(FailureReason::parse(reason.as_str()), Some(reason));
        }
        assert_eq!(RequestStatus::parse("Accepted"), None);
        assert_eq!(FailureReason::parse(""), None);
    }

    #[test]
    fn every_ask_error_maps_to_a_reason() {
        let cases = [
            (AskError::InvalidConversation, "invalid_request"),
            (AskError::Conversation, "store_unavailable"),
            (AskError::Busy, "conversation_busy"),
            (AskError::Interrupted, "interrupted"),
            (AskError::UnknownCoworker, "invalid_request"),
            (AskError::EmptyPrompt, "invalid_request"),
            (AskError::PromptTooLarge, "invalid_request"),
            (AskError::Workspace, "workspace_unavailable"),
            (AskError::Build, "agent_unavailable"),
            (AskError::Cleanup, "cleanup_failed"),
            (AskError::Run, "model_failed"),
            (AskError::NotCompleted, "model_failed"),
            (AskError::NoAnswer, "no_answer"),
            (AskError::Timeout, "timeout"),
        ];
        for (error, reason) in cases {
            assert_eq!(FailureReason::from(&error).as_str(), reason, "{error}");
        }
    }

    #[test]
    fn error_codes_carry_their_status() {
        let cases = [
            (ErrorCode::InvalidJson, "invalid_json", 400),
            (ErrorCode::InvalidInput, "invalid_input", 400),
            (ErrorCode::Unauthorized, "unauthorized", 401),
            (ErrorCode::NotFound, "not_found", 404),
            (ErrorCode::MethodNotAllowed, "method_not_allowed", 405),
            (ErrorCode::Conflict, "conflict", 409),
            (ErrorCode::QueueFull, "queue_full", 409),
            (ErrorCode::PayloadTooLarge, "payload_too_large", 413),
            (ErrorCode::Internal, "internal", 500),
            (ErrorCode::RetrySameRequest, "retry_same_request", 503),
            (ErrorCode::Unavailable, "unavailable", 503),
        ];
        for (code, text, status) in cases {
            assert_eq!(
                serde_json::to_value(Error { error: code }).unwrap(),
                json!({"error": text})
            );
            assert_eq!(code.status().as_u16(), status);
        }
    }
}
