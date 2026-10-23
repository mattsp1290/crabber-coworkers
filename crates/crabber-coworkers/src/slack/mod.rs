use std::{collections::HashMap, fmt};

pub const BOT_TOKEN_PREFIX: &str = "SLACK_BOT_TOKEN_";
pub const APP_TOKEN_PREFIX: &str = "SLACK_APP_TOKEN_";

#[derive(Clone)]
pub struct Credentials {
    bot_token: String,
    app_token: String,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credentials(redacted)")
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum CredentialError {
    #[error("{0} is not set")]
    Missing(String),
    #[error("{0} has the wrong token type")]
    WrongType(String),
}

fn token(value: Option<String>, prefix: &str, var: &str) -> Result<String, CredentialError> {
    let value = value
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| CredentialError::Missing(var.into()))?;
    if !value.starts_with(prefix) {
        return Err(CredentialError::WrongType(var.into()));
    }
    Ok(value)
}

impl Credentials {
    /// # Errors
    /// Returns a redacted error when a token has the wrong type or is blank.
    pub fn new(bot_token: String, app_token: String) -> Result<Self, CredentialError> {
        Ok(Self {
            bot_token: token(Some(bot_token), "xoxb-", "")?,
            app_token: token(Some(app_token), "xapp-", "")?,
        })
    }

    #[must_use]
    pub fn bot_token(&self) -> &str {
        &self.bot_token
    }
    #[must_use]
    pub fn app_token(&self) -> &str {
        &self.app_token
    }
}

#[must_use]
pub fn token_suffix(id: &str) -> String {
    id.to_ascii_uppercase().replace('-', "_")
}
#[must_use]
pub fn bot_token_var(id: &str) -> String {
    format!("{BOT_TOKEN_PREFIX}{}", token_suffix(id))
}
#[must_use]
pub fn app_token_var(id: &str) -> String {
    format!("{APP_TOKEN_PREFIX}{}", token_suffix(id))
}

/// # Errors
/// Names the missing or wrongly typed token variable without exposing its value.
pub fn credentials_from(
    id: &str,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Credentials, CredentialError> {
    let bot_var = bot_token_var(id);
    let app_var = app_token_var(id);
    Ok(Credentials {
        bot_token: token(lookup(&bot_var), "xoxb-", &bot_var)?,
        app_token: token(lookup(&app_var), "xapp-", &app_var)?,
    })
}

/// # Errors
/// Returns a redacted token validation error.
pub fn credentials_from_env(id: &str) -> Result<Credentials, CredentialError> {
    credentials_from(id, |key| std::env::var(key).ok())
}

pub enum CredentialSource {
    FromEnv,
    Injected(HashMap<String, Credentials>),
}

impl CredentialSource {
    /// # Errors
    /// Returns a redacted token validation error.
    pub fn resolve(&self, id: &str) -> Result<Credentials, CredentialError> {
        match self {
            Self::FromEnv => credentials_from_env(id),
            Self::Injected(map) => map
                .get(id)
                .cloned()
                .ok_or_else(|| CredentialError::Missing(bot_token_var(id))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_suffix_maps_ids() {
        assert_eq!(token_suffix("scout"), "SCOUT");
        assert_eq!(token_suffix("code-review-2"), "CODE_REVIEW_2");
        assert_eq!(
            bot_token_var("code-review-2"),
            "SLACK_BOT_TOKEN_CODE_REVIEW_2"
        );
    }

    #[test]
    fn credentials_from_lookup() {
        let credentials = credentials_from("scout", |key| {
            Some(
                if key == bot_token_var("scout") {
                    "xoxb-test"
                } else {
                    "xapp-test"
                }
                .into(),
            )
        })
        .unwrap();
        assert_eq!(credentials.bot_token(), "xoxb-test");
        assert_eq!(credentials.app_token(), "xapp-test");
        assert_eq!(format!("{credentials:?}"), "Credentials(redacted)");
        for value in [None, Some(" ".into())] {
            assert_eq!(
                credentials_from("scout", |_| value.clone()).unwrap_err(),
                CredentialError::Missing(bot_token_var("scout"))
            );
        }
        assert_eq!(
            credentials_from("scout", |_| Some("xoxp-test".into())).unwrap_err(),
            CredentialError::WrongType(bot_token_var("scout"))
        );
        assert_eq!(
            credentials_from("scout", |key| Some(
                if key == bot_token_var("scout") {
                    "xoxb-test"
                } else {
                    "xoxe.xapp-test"
                }
                .into()
            ))
            .unwrap_err(),
            CredentialError::WrongType(app_token_var("scout"))
        );
    }

    #[test]
    fn credential_source_injected() {
        let source = CredentialSource::Injected(HashMap::from([(
            "scout".into(),
            Credentials::new("xoxb-test".into(), "xapp-test".into()).unwrap(),
        )]));
        assert!(source.resolve("scout").is_ok());
        assert_eq!(
            source.resolve("quill").unwrap_err(),
            CredentialError::Missing(bot_token_var("quill"))
        );
    }
}

pub mod api;
pub mod delivery;
pub mod ingress;
pub mod socket;
