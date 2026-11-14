use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use crabber_coworkers::{
    api::{
        contract::{
            Acceptance, ApiUuid, ApiVersion, BackendKind, CancelRequest, Cancellation,
            Capabilities, CapabilityLimits, ConversationPage, ConversationPhase,
            ConversationSnapshot, ConversationState, Coworker, CoworkerId, CoworkerList,
            DependencyHealth, DependencyStatus, Error, ErrorCode, FailureReason, Health,
            HealthDependencies, HealthStatus, InstructionsSummary, MessageRole, OpenConversation,
            RequestContract, RequestReceipt, RequestResult, RequestStatus, SnapshotMessage,
            SubmitRequest, WorkspaceAccess, decode_body,
        },
        openapi::document_json,
        routes,
    },
    config::constants::MAX_PROMPT_BYTES,
};
use serde_json::{Value, json};
use std::process::Command;
use tower::ServiceExt;

const OPERATIONS: [(&str, &str, &str); 9] = [
    ("get", "/v1/capabilities", "capabilities"),
    ("get", "/v1/health", "health"),
    ("get", "/v1/coworkers", "listCoworkers"),
    ("post", "/v1/conversations", "openConversation"),
    ("get", "/v1/conversations", "listConversations"),
    ("get", "/v1/conversations/{conversationId}", "conversation"),
    (
        "post",
        "/v1/conversations/{conversationId}/requests",
        "submit",
    ),
    (
        "get",
        "/v1/conversations/{conversationId}/requests/{requestId}",
        "requestStatus",
    ),
    (
        "post",
        "/v1/conversations/{conversationId}/requests/{requestId}/cancel",
        "cancel",
    ),
];
const ERROR_STATUSES: [&str; 8] = ["400", "401", "404", "405", "409", "413", "500", "503"];
const COMPONENTS: [&str; 16] = [
    "Error",
    "ErrorCode",
    "Coworker",
    "CoworkerList",
    "OpenConversation",
    "ConversationState",
    "ConversationPage",
    "ConversationSnapshot",
    "SnapshotMessage",
    "SubmitRequest",
    "CancelRequest",
    "RequestReceipt",
    "RequestStatus",
    "FailureReason",
    "Capabilities",
    "Health",
];
const ID: &str = "0b5e8f1c-3d2a-4e6f-9a7b-1c2d3e4f5a6b";

fn document() -> Value {
    serde_json::from_str(&document_json()).unwrap()
}

fn operations(document: &Value) -> Vec<(String, String, Value)> {
    let mut operations = Vec::new();
    for (path, item) in document["paths"].as_object().unwrap() {
        for (method, operation) in item.as_object().unwrap() {
            operations.push((method.clone(), path.clone(), operation.clone()));
        }
    }
    operations
}

fn validator(document: &Value, name: &str) -> jsonschema::Validator {
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .build(&json!({
            "$ref": format!("#/components/schemas/{name}"),
            "components": document["components"],
        }))
        .unwrap_or_else(|error| panic!("{name}: {error}"))
}

fn assert_conforms(document: &Value, name: &str, body: &impl serde::Serialize) {
    let body = serde_json::to_value(body).unwrap();
    if let Err(error) = validator(document, name).validate(&body) {
        panic!("{name} rejects {body}: {error}");
    }
}

fn collect_refs<'a>(value: &'a Value, refs: &mut Vec<&'a str>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(target)) = map.get("$ref") {
                refs.push(target);
            }
            map.values().for_each(|value| collect_refs(value, refs));
        }
        Value::Array(values) => values.iter().for_each(|value| collect_refs(value, refs)),
        _ => {}
    }
}

#[test]
fn openapi_is_deterministic() {
    let json = document_json();
    assert_eq!(json, document_json());
    let document: Value = serde_json::from_str(&json).unwrap();
    assert!(document["openapi"].as_str().unwrap().starts_with("3.1."));
    assert_eq!(
        document["info"]["title"],
        "crabber-coworkers integration API"
    );
    assert_eq!(document["info"]["version"], "1.0.0");
    let mut found: Vec<_> = operations(&document)
        .into_iter()
        .map(|(method, path, operation)| {
            for status in ERROR_STATUSES {
                assert!(
                    operation["responses"].get(status).is_some(),
                    "{method} {path} lacks {status}"
                );
            }
            (
                method,
                path,
                operation["operationId"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    found.sort();
    let mut expected: Vec<_> = OPERATIONS
        .iter()
        .map(|(method, path, id)| ((*method).to_owned(), (*path).to_owned(), (*id).to_owned()))
        .collect();
    expected.sort();
    assert_eq!(found, expected);
    for name in COMPONENTS {
        assert!(
            document["components"]["schemas"].get(name).is_some(),
            "{name}"
        );
    }
}

#[test]
fn committed_openapi_matches() {
    let committed = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../openapi/crabber-coworkers.v1.json"
    ))
    .unwrap_or_default();
    assert!(
        committed == document_json(),
        "openapi/crabber-coworkers.v1.json is stale; run cargo xtask openapi"
    );
}

#[test]
fn document_declares_bearer_auth_and_strict_inputs() {
    let document = document();
    assert_eq!(document["security"], json!([{"bearerAuth": []}]));
    assert_eq!(
        document["components"]["securitySchemes"],
        json!({"bearerAuth": {"type": "http", "scheme": "bearer"}})
    );
    assert!(document.get("x-excluded-routes").is_none());
    let schemas = &document["components"]["schemas"];
    for (method, path, operation) in operations(&document) {
        assert!(operation.get("security").is_none(), "{method} {path}");
        assert!(operation.get("tags").is_none(), "{method} {path}");
        assert_ne!(operation["description"].as_str().unwrap(), "");
        for parameter in operation["parameters"].as_array().into_iter().flatten() {
            if parameter["in"] == "path" {
                assert_eq!(parameter["schema"]["format"], "uuid", "{path}");
            }
        }
        if let Some(body) = operation.get("requestBody") {
            assert_eq!(body["required"], true);
            let target = body["content"]["application/json"]["schema"]["$ref"]
                .as_str()
                .unwrap();
            let name = target.trim_start_matches("#/components/schemas/");
            assert!(
                ["OpenConversation", "SubmitRequest", "CancelRequest"].contains(&name),
                "{name}"
            );
            assert_eq!(schemas[name]["additionalProperties"], false, "{name}");
        }
    }
    let list = &document["paths"]["/v1/conversations"]["get"]["parameters"];
    let coworker = list
        .as_array()
        .unwrap()
        .iter()
        .find(|parameter| parameter["name"] == "coworkerId")
        .unwrap();
    assert_eq!(coworker["schema"]["pattern"], "^[a-z0-9]+(-[a-z0-9]+)*$");
    assert_eq!(coworker["schema"]["maxLength"], 64);
    assert_eq!(
        schemas["SubmitRequest"]["properties"]["text"]["x-maxUtf8Bytes"],
        MAX_PROMPT_BYTES
    );
}

#[test]
fn every_reference_resolves() {
    let document = document();
    let mut refs = Vec::new();
    collect_refs(&document, &mut refs);
    assert_ne!(refs.len(), 0);
    for target in refs {
        let pointer = target.strip_prefix('#').unwrap();
        assert!(document.pointer(pointer).is_some(), "{target}");
    }
    for name in document["components"]["schemas"]
        .as_object()
        .unwrap()
        .keys()
    {
        validator(&document, name);
    }
}

#[test]
fn conversation_bodies_match_their_schemas() {
    let document = document();
    let id = ApiUuid::parse(ID).unwrap();
    let scout = CoworkerId::parse("scout").unwrap();
    let state = ConversationState {
        conversation_id: id,
        coworker_id: scout.clone(),
        phase: ConversationPhase::Running,
    };
    assert_conforms(
        &document,
        "CoworkerList",
        &CoworkerList {
            api_version: ApiVersion::V1,
            coworkers: vec![Coworker {
                coworker_id: scout,
                name: "Scout".into(),
                role: "Research".into(),
                workspace: WorkspaceAccess::ReadWrite,
                slack: true,
                instructions: InstructionsSummary {
                    bytes: 12,
                    digest: "0f".repeat(32),
                },
            }],
        },
    );
    assert_conforms(&document, "ConversationState", &state);
    for next in [Some("Y29udmVyc2F0aW9ucw".to_owned()), None] {
        assert_conforms(
            &document,
            "ConversationPage",
            &ConversationPage {
                api_version: ApiVersion::V1,
                conversations: vec![state.clone()],
                next,
            },
        );
    }
    assert_conforms(
        &document,
        "ConversationSnapshot",
        &ConversationSnapshot {
            api_version: ApiVersion::V1,
            state: state.clone(),
            messages: vec![
                SnapshotMessage {
                    request_id: id,
                    role: MessageRole::User,
                    text: "question".into(),
                },
                SnapshotMessage {
                    request_id: id,
                    role: MessageRole::Assistant,
                    text: "answer".into(),
                },
            ],
            truncated: false,
        },
    );
    assert!(
        !validator(&document, "ConversationState")
            .is_valid(&json!({"conversationId": ID, "coworkerId": "scout", "phase": "busy"}))
    );
}

#[test]
fn receipt_bodies_match_their_schema() {
    let document = document();
    let id = ApiUuid::parse(ID).unwrap();
    let receipt = |status, result, reason, cancellation| RequestReceipt {
        api_version: ApiVersion::V1,
        conversation_id: id,
        request_id: id,
        acceptance: Acceptance::default(),
        status,
        result,
        reason,
        cancellation,
    };
    let cancelled_at = || {
        Some(Cancellation {
            requested_at: "2026-11-14T21:05:35Z".into(),
        })
    };
    for body in [
        receipt(RequestStatus::Accepted, None, None, None),
        receipt(
            RequestStatus::Completed,
            Some(RequestResult {
                text: "answer".into(),
            }),
            None,
            cancelled_at(),
        ),
        receipt(
            RequestStatus::Failed,
            None,
            Some(FailureReason::Interrupted),
            None,
        ),
        receipt(RequestStatus::Cancelled, None, None, cancelled_at()),
    ] {
        assert_conforms(&document, "RequestReceipt", &body);
    }
}

#[test]
fn service_bodies_match_their_schemas() {
    let document = document();
    assert_conforms(
        &document,
        "Capabilities",
        &Capabilities {
            api_version: ApiVersion::V1,
            request_contract: RequestContract::default(),
            backend: BackendKind::Local,
            limits: CapabilityLimits {
                max_prompt_bytes: 32_768,
                max_queued_per_conversation: 16,
                model_turn_seconds: 120,
                max_page_size: 100,
            },
            statuses: RequestStatus::ALL.to_vec(),
            reasons: FailureReason::ALL.to_vec(),
        },
    );
    for (status, database) in [
        (HealthStatus::Ready, DependencyStatus::Ready),
        (HealthStatus::NotReady, DependencyStatus::Unavailable),
    ] {
        assert_conforms(
            &document,
            "Health",
            &Health {
                api_version: ApiVersion::V1,
                status,
                dependencies: HealthDependencies {
                    database: DependencyHealth { status: database },
                },
            },
        );
    }
    for error in [
        ErrorCode::InvalidJson,
        ErrorCode::InvalidInput,
        ErrorCode::Unauthorized,
        ErrorCode::NotFound,
        ErrorCode::MethodNotAllowed,
        ErrorCode::Conflict,
        ErrorCode::QueueFull,
        ErrorCode::PayloadTooLarge,
        ErrorCode::Internal,
        ErrorCode::RetrySameRequest,
        ErrorCode::Unavailable,
    ] {
        assert_conforms(&document, "Error", &Error { error });
    }
    assert!(!validator(&document, "Error").is_valid(&json!({"error": "nope"})));
}

#[test]
fn request_schemas_agree_with_decoding() {
    let document = document();
    let open = validator(&document, "OpenConversation");
    let submit = validator(&document, "SubmitRequest");
    let cancel = validator(&document, "CancelRequest");
    let valid_open = json!({"conversationId": ID, "coworkerId": "scout"});
    let valid_submit = json!({"requestId": ID, "text": "question"});
    let decodes = |body: &Value| {
        [
            decode_body::<OpenConversation>(body.to_string().as_bytes()).is_ok(),
            decode_body::<SubmitRequest>(body.to_string().as_bytes()).is_ok(),
            decode_body::<CancelRequest>(body.to_string().as_bytes()).is_ok(),
        ]
    };
    assert!(open.is_valid(&valid_open));
    assert_eq!(decodes(&valid_open), [true, false, false]);
    assert!(submit.is_valid(&valid_submit));
    assert_eq!(decodes(&valid_submit), [false, true, false]);
    assert!(cancel.is_valid(&json!({})));
    assert_eq!(decodes(&json!({})), [false, false, true]);
    for body in [
        json!([]),
        json!([ID, "question"]),
        json!([ID, "scout"]),
        Value::Null,
        json!("x"),
        json!(1),
    ] {
        for (name, validator) in [
            ("OpenConversation", &open),
            ("SubmitRequest", &submit),
            ("CancelRequest", &cancel),
        ] {
            assert!(!validator.is_valid(&body), "{name} {body}");
        }
        assert_eq!(decodes(&body), [false; 3], "{body}");
    }
    for body in [
        json!({"conversationId": ID.to_uppercase(), "coworkerId": "scout"}),
        json!({"conversationId": ID, "coworkerId": "Scout"}),
        json!({"conversationId": ID, "coworkerId": "scout", "extra": true}),
        json!({"conversationId": ID}),
    ] {
        assert!(!open.is_valid(&body), "{body}");
        assert!(!decodes(&body)[0], "{body}");
    }
    for body in [
        json!({"requestId": format!("{{{ID}}}"), "text": "question"}),
        json!({"requestId": ID, "text": "question", "extra": true}),
        json!({"requestId": ID}),
    ] {
        assert!(!submit.is_valid(&body), "{body}");
        assert!(!decodes(&body)[1], "{body}");
    }
    let body = json!({"reason": "late"});
    assert!(!cancel.is_valid(&body));
    assert!(!decodes(&body)[2]);
}

#[tokio::test]
async fn router_serves_exactly_the_documented_operations() {
    let document = document();
    let paths = document["paths"].as_object().unwrap();
    assert_eq!(paths.len(), 8);
    for (path, item) in paths {
        let uri = path
            .replace("{conversationId}", ID)
            .replace("{requestId}", ID);
        for method in [
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
        ] {
            let documented = item.get(method.as_str().to_lowercase()).is_some();
            let status = routes::router()
                .oneshot(
                    Request::builder()
                        .method(method.clone())
                        .uri(&uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status();
            assert_ne!(status, StatusCode::NOT_FOUND, "{method} {uri}");
            assert_eq!(
                status == StatusCode::METHOD_NOT_ALLOWED,
                !documented,
                "{method} {uri}: {status}"
            );
        }
    }
    for path in [
        "/v1/members",
        "/healthz",
        "/v1/conversations/0b5e8f1c-3d2a-4e6f-9a7b-1c2d3e4f5a6b/events",
    ] {
        let response = routes::router()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
}

#[test]
fn every_success_body_carries_the_api_version() {
    let document = document();
    let mut checked = 0;
    for (method, path, operation) in operations(&document) {
        for (status, response) in operation["responses"].as_object().unwrap() {
            if !status.starts_with('2') {
                continue;
            }
            let target = response["content"]["application/json"]["schema"]["$ref"]
                .as_str()
                .unwrap_or_else(|| panic!("{method} {path} {status}: no schema reference"));
            let schema = document.pointer(target.strip_prefix('#').unwrap()).unwrap();
            assert_eq!(
                schema["properties"]["apiVersion"],
                json!({"type": "string", "enum": ["1"]}),
                "{method} {path} {status}"
            );
            assert!(
                schema["required"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("apiVersion")),
                "{method} {path} {status}"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 9);
}

#[test]
fn vocabulary_enums_match_their_full_lists() {
    let document = document();
    let schemas = &document["components"]["schemas"];
    assert_eq!(
        schemas["RequestStatus"]["enum"],
        json!(RequestStatus::ALL.map(RequestStatus::as_str))
    );
    assert_eq!(
        schemas["FailureReason"]["enum"],
        json!(FailureReason::ALL.map(FailureReason::as_str))
    );
}

#[test]
fn openapi_subcommand_prints_the_document_and_stays_hidden() {
    let binary = env!("CARGO_BIN_EXE_crabber-coworkers");
    let output = Command::new(binary).arg("openapi").output().unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, document_json().as_bytes());
    assert_eq!(output.stderr, b"");
    let help = Command::new(binary).arg("--help").output().unwrap();
    assert!(help.status.success());
    assert!(!String::from_utf8_lossy(&help.stdout).contains("openapi"));
}
