use super::Credentials;
pub use super::ingress::Identity;
use serde_json::{Value, json};
use std::{fmt, time::Duration};
use url::Url;

pub const DEFAULT_BASE: &str = "https://slack.com/api/";
pub const CALL_SECONDS: u64 = 15;

pub struct SlackApi {
    client: reqwest::Client,
    base: Url,
    credentials: Credentials,
}

impl fmt::Debug for SlackApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SlackApi(redacted)")
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ApiError {
    #[error("Slack authentication failed")]
    Auth,
    #[error("Slack rate limited")]
    RateLimited { retry_after_seconds: u64 },
    #[error("Slack request failed")]
    Transport,
    #[error("Slack rejected the request")]
    Rejected,
}

impl SlackApi {
    /// # Errors
    /// Rejects invalid HTTP bases or client configuration with a redacted error.
    pub fn new(base: &str, credentials: Credentials) -> Result<Self, ApiError> {
        let base = Url::parse(base).map_err(|_| ApiError::Transport)?;
        if !matches!(base.scheme(), "http" | "https") {
            return Err(ApiError::Transport);
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(CALL_SECONDS))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ApiError::Transport)?;
        Ok(Self {
            client,
            base,
            credentials,
        })
    }

    async fn call(&self, method: &str, token: &str, body: Value) -> Result<Value, ApiError> {
        let url = self.base.join(method).map_err(|_| ApiError::Transport)?;
        let response = self
            .client
            .post(url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|_| ApiError::Transport)?;
        let status = response.status();
        let retry_header = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ApiError::RateLimited {
                retry_after_seconds: retry_header.unwrap_or(1).min(30),
            });
        }
        if !status.is_success() {
            return Err(ApiError::Transport);
        }
        let value: Value = response.json().await.map_err(|_| ApiError::Transport)?;
        if value["ok"] == true {
            return Ok(value);
        }
        Err(match value["error"].as_str() {
            Some(
                "invalid_auth" | "not_authed" | "account_inactive" | "token_revoked"
                | "token_expired",
            ) => ApiError::Auth,
            Some("ratelimited") => ApiError::RateLimited {
                retry_after_seconds: retry_header
                    .or_else(|| value["retry_after"].as_u64())
                    .unwrap_or(1)
                    .min(30),
            },
            _ => ApiError::Rejected,
        })
    }

    /// # Errors
    /// Returns only fixed error categories; never carries Slack response text.
    pub async fn auth_test(&self) -> Result<Identity, ApiError> {
        let value = self
            .call("auth.test", self.credentials.bot_token(), json!({}))
            .await?;
        let field = |key: &str| {
            value[key]
                .as_str()
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
                .ok_or(ApiError::Transport)
        };
        Ok(Identity {
            team_id: field("team_id")?,
            user_id: field("user_id")?,
            bot_id: field("bot_id")?,
        })
    }

    /// # Errors
    /// Returns a fixed error if Slack rejects the app token or returns an invalid URL.
    pub async fn connections_open(&self) -> Result<Url, ApiError> {
        let value = self
            .call(
                "apps.connections.open",
                self.credentials.app_token(),
                json!({}),
            )
            .await?;
        let url = Url::parse(value["url"].as_str().ok_or(ApiError::Transport)?)
            .map_err(|_| ApiError::Transport)?;
        if !matches!(url.scheme(), "ws" | "wss") {
            return Err(ApiError::Transport);
        }
        Ok(url)
    }

    /// # Errors
    /// Returns a fixed delivery error; transport failures are never retried here.
    pub async fn post_message(
        &self,
        channel: &str,
        thread_ts: &str,
        text: &str,
    ) -> Result<(), ApiError> {
        self.call(
            "chat.postMessage",
            self.credentials.bot_token(),
            json!({
                "channel": channel, "thread_ts": thread_ts, "text": text,
                "parse": "none", "link_names": false, "unfurl_links": false, "unfurl_media": false,
            }),
        )
        .await?;
        Ok(())
    }
}
