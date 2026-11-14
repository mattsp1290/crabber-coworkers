use super::{contract, routes};
use utoipa::{
    Modify, OpenApi,
    openapi::{
        self, Components, ContentBuilder, Ref, ResponseBuilder,
        security::{Http, HttpAuthScheme, SecurityScheme},
    },
};

const ERROR_STATUSES: [&str; 8] = ["400", "401", "404", "405", "409", "413", "500", "503"];

#[derive(OpenApi)]
#[openapi(
    info(
        title = "crabber-coworkers integration API",
        version = "1.0.0",
        description = "Every `/v1` route this server exposes is in this document, and every route requires the bearer token. Byte limits come from the configured `limits`: `x-maxUtf8Bytes` is the hard ceiling, and `GET /v1/capabilities` reports the values in force.",
    ),
    paths(
        routes::capabilities,
        routes::health,
        routes::list_coworkers,
        routes::open_conversation,
        routes::list_conversations,
        routes::conversation,
        routes::submit,
        routes::request_status,
        routes::cancel,
    ),
    components(schemas(contract::Error)),
    security(("bearerAuth" = [])),
    modifiers(&Conventions),
)]
struct Document;

/// Adds the bearer scheme and attaches the shared error responses to every operation, as Crew's
/// generator does, so a new route cannot omit them. Drops the tags utoipa derives from the module
/// path, which carry no meaning for clients.
struct Conventions;

impl Modify for Conventions {
    fn modify(&self, document: &mut openapi::OpenApi) {
        let components = document.components.get_or_insert_with(Components::new);
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
        for (name, description) in [
            ("Error", "A fixed public error code."),
            (
                "Unavailable",
                "The read could not be completed. Repeating it is safe.",
            ),
            (
                "CommandUnavailable",
                "The command's outcome is unknown. Reconcile with a read of the same identity, then resend the identical request.",
            ),
        ] {
            let response = ResponseBuilder::new()
                .description(description)
                .content(
                    "application/json",
                    ContentBuilder::new()
                        .schema(Some(Ref::from_schema_name("Error")))
                        .build(),
                )
                .build();
            components.responses.insert(name.into(), response.into());
        }
        for item in document.paths.paths.values_mut() {
            for (operation, server_error) in [
                (item.get.as_mut(), "Unavailable"),
                (item.post.as_mut(), "CommandUnavailable"),
            ] {
                let Some(operation) = operation else {
                    continue;
                };
                operation.tags = None;
                for status in ERROR_STATUSES {
                    let name = if status.starts_with('5') {
                        server_error
                    } else {
                        "Error"
                    };
                    operation
                        .responses
                        .responses
                        .entry(status.into())
                        .or_insert_with(|| Ref::from_response_name(name).into());
                }
            }
        }
    }
}

#[must_use]
pub fn document() -> openapi::OpenApi {
    Document::openapi()
}

/// The committed form of [`document`]: pretty JSON with a trailing newline.
///
/// # Panics
/// Panics only if the document cannot serialize, which its string-keyed maps rule out.
#[must_use]
pub fn document_json() -> String {
    let mut json = document()
        .to_pretty_json()
        .expect("the OpenAPI document serializes");
    json.push('\n');
    json
}
