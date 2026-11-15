//! The operator's bearer token. It is read once at startup and kept only as its SHA-256 digest.
//! Every request must present it before routing extracts anything from the request.
use super::contract::ErrorCode;
use crate::config::constants::{API_TOKEN_ENV, MIN_API_TOKEN_BYTES};
use axum::{
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use std::{fmt, sync::Arc};

/// Where `serve` reads the API token from.
pub enum ApiTokenSource {
    /// `CRABBER_COWORKERS_API_TOKEN`.
    FromEnv,
    /// A token supplied by an embedding test.
    Fixed(String),
}

impl fmt::Debug for ApiTokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FromEnv => f.write_str("FromEnv"),
            Self::Fixed(_) => f.write_str("Fixed(redacted)"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    #[error("CRABBER_COWORKERS_API_TOKEN is not set")]
    Missing,
    #[error("CRABBER_COWORKERS_API_TOKEN must be at least 32 printable bytes")]
    Weak,
}

impl ApiTokenSource {
    /// Read and check the token, keeping only its digest.
    ///
    /// # Errors
    /// [`TokenError::Missing`] when the token is unset or empty, [`TokenError::Weak`] when it is
    /// shorter than 32 bytes or holds anything but printable ASCII.
    pub fn resolve(&self) -> Result<TokenDigest, TokenError> {
        match self {
            Self::FromEnv => {
                let value = std::env::var_os(API_TOKEN_ENV).ok_or(TokenError::Missing)?;
                TokenDigest::of(value.to_str().ok_or(TokenError::Weak)?)
            }
            Self::Fixed(token) => TokenDigest::of(token),
        }
    }
}

/// The SHA-256 digest of the operator token. The token itself is never kept.
#[derive(Clone)]
pub struct TokenDigest([u8; 32]);

impl fmt::Debug for TokenDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TokenDigest(redacted)")
    }
}

impl TokenDigest {
    fn of(token: &str) -> Result<Self, TokenError> {
        if token.is_empty() {
            return Err(TokenError::Missing);
        }
        if token.len() < MIN_API_TOKEN_BYTES || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(TokenError::Weak);
        }
        Ok(Self(Sha256::digest(token.as_bytes()).into()))
    }

    /// Compares digests rather than tokens so that the comparison always covers 32 bytes, and
    /// folds every byte so that its time does not depend on where they differ.
    fn admits(&self, presented: &[u8]) -> bool {
        let presented: [u8; 32] = Sha256::digest(presented).into();
        self.0
            .iter()
            .zip(presented)
            .fold(0, |difference, (expected, given)| {
                difference | (expected ^ given)
            })
            == 0
    }
}

/// The token from exactly one `Authorization: Bearer <token>` header. The scheme is matched
/// without regard to case and is followed by one space.
fn presented(headers: &HeaderMap) -> Option<&[u8]> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let (scheme, token) = value.as_bytes().split_at_checked(b"Bearer ".len())?;
    (scheme.eq_ignore_ascii_case(b"bearer ")
        && !token.is_empty()
        && token.iter().all(u8::is_ascii_graphic))
    .then_some(token)
}

/// Rejects a request without the token before anything reads its path, query, or body, and
/// marks every response, including that rejection, as not cacheable.
pub(super) async fn guard(
    State(token): State<Arc<TokenDigest>>,
    request: Request,
    next: Next,
) -> Response {
    let mut response = match presented(request.headers()) {
        Some(presented) if token.admits(presented) => next.run(request).await,
        _ => {
            let mut response = ErrorCode::Unauthorized.into_response();
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            response
        }
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    fn headers(values: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        }
        headers
    }

    #[test]
    fn tokens_must_be_long_and_printable() {
        let fixed = |token: &str| ApiTokenSource::Fixed(token.into()).resolve().map(drop);
        assert_eq!(fixed(""), Err(TokenError::Missing));
        assert_eq!(fixed(&TOKEN[1..]), Err(TokenError::Weak));
        assert_eq!(fixed(&format!("{TOKEN} ")), Err(TokenError::Weak));
        assert_eq!(fixed(&format!("{TOKEN}\u{7f}")), Err(TokenError::Weak));
        assert_eq!(fixed(&format!("{TOKEN}\u{e9}")), Err(TokenError::Weak));
        assert_eq!(fixed(&" ".repeat(40)), Err(TokenError::Weak));
        assert_eq!(fixed(TOKEN), Ok(()));
        assert_eq!(
            TokenError::Missing.to_string(),
            format!("{API_TOKEN_ENV} is not set")
        );
        assert_eq!(
            TokenError::Weak.to_string(),
            format!("{API_TOKEN_ENV} must be at least {MIN_API_TOKEN_BYTES} printable bytes")
        );
    }

    #[test]
    fn only_one_bearer_header_with_the_token_is_admitted() {
        let digest = ApiTokenSource::Fixed(TOKEN.into()).resolve().unwrap();
        let admitted = |values: &[&str]| {
            presented(&headers(values)).is_some_and(|presented| digest.admits(presented))
        };
        assert!(admitted(&[&format!("Bearer {TOKEN}")]));
        assert!(admitted(&[&format!("bEARER {TOKEN}")]));
        for values in [
            vec![],
            vec![TOKEN.to_owned()],
            vec![format!("Bearer {TOKEN}x")],
            vec![format!("Bearer  {TOKEN}")],
            vec![format!("Bearer {TOKEN} ")],
            vec![format!("Bearer\t{TOKEN}")],
            vec![format!("Basic {TOKEN}")],
            vec!["Bearer ".to_owned()],
            vec![format!("Bearer {TOKEN}"), format!("Bearer {TOKEN}")],
        ] {
            let values: Vec<_> = values.iter().map(String::as_str).collect();
            assert!(!admitted(&values), "{values:?}");
        }
    }

    #[test]
    fn debug_output_is_redacted() {
        let source = ApiTokenSource::Fixed(TOKEN.into());
        let digest = source.resolve().unwrap();
        assert!(!format!("{source:?}{digest:?}").contains(TOKEN));
    }
}
