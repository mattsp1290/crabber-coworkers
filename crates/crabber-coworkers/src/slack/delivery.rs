use super::api::{ApiError, SlackApi};
use crate::ask::Cancel;
use std::time::Duration;

pub const CHUNK_CHARS: usize = 3_500;
pub const MAX_CHUNKS: usize = 8;
pub const TRUNCATED_MARKER: &str = "\n[answer truncated]";

#[must_use]
pub fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// Return a byte boundary after at most cap scalars, retaining whole escaped entities.
fn boundary(text: &str, cap: usize, prefer_newline: bool) -> usize {
    let mut end = text.char_indices().nth(cap).map_or(text.len(), |(i, _)| i);
    if end < text.len() {
        if let Some(amp) = text[..end].rfind('&')
            && !text[amp..end].contains(';')
        {
            end = amp;
        }
        if prefer_newline
            && let Some(newline) = text[..end].rfind('\n')
            && newline + 1 >= end / 2
        {
            end = newline + 1;
        }
    }
    end
}

#[must_use]
pub fn chunks(mut escaped: &str) -> Vec<String> {
    let mut chunks = Vec::new();
    while !escaped.is_empty() && chunks.len() < MAX_CHUNKS {
        let end = boundary(escaped, CHUNK_CHARS, true);
        if chunks.len() == MAX_CHUNKS - 1 && end < escaped.len() {
            let end = boundary(
                escaped,
                CHUNK_CHARS - TRUNCATED_MARKER.chars().count(),
                false,
            );
            chunks.push(format!("{}{TRUNCATED_MARKER}", &escaped[..end]));
            break;
        }
        chunks.push(escaped[..end].to_owned());
        escaped = &escaped[end..];
    }
    chunks
}

#[derive(Debug, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Complete,
    Stopped,
}

/// # Errors
/// Returns the first failed chunk's fixed error category. Cancellation is a normal outcome.
pub async fn post_answer(
    api: &SlackApi,
    channel: &str,
    root_ts: &str,
    answer: &str,
    stop: &mut Cancel,
) -> Result<DeliveryOutcome, ApiError> {
    for chunk in chunks(&escape(answer)) {
        if chunk.trim().is_empty() {
            continue;
        }
        let result = tokio::select! {
            biased;
            () = &mut *stop => return Ok(DeliveryOutcome::Stopped),
            result = api.post_message(channel, root_ts, &chunk) => result,
        };
        if let Err(ApiError::RateLimited {
            retry_after_seconds,
        }) = result
        {
            tokio::select! {
                biased;
                () = &mut *stop => return Ok(DeliveryOutcome::Stopped),
                () = tokio::time::sleep(Duration::from_secs(retry_after_seconds)) => {},
            }
            tokio::select! {
                biased;
                () = &mut *stop => return Ok(DeliveryOutcome::Stopped),
                result = api.post_message(channel, root_ts, &chunk) => result?,
            }
        } else {
            result?;
        }
    }
    Ok(DeliveryOutcome::Complete)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn early_newline_does_not_consume_a_chunk() {
        let input = "\n".to_owned() + &"x".repeat(4_000);
        let parts = chunks(&input);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].chars().count(), CHUNK_CHARS);
        assert_eq!(parts.concat(), input);
    }

    #[test]
    fn newline_preference_preserves_unicode_entity_boundaries() {
        let input = escape(&("🦀".repeat(2_000) + "\n" + &"&".repeat(1_000)));
        let parts = chunks(&input);
        assert_eq!(parts.concat(), input);
        assert!(parts.iter().all(|p| p.chars().count() <= CHUNK_CHARS));
        assert!(
            parts
                .iter()
                .all(|p| p.rfind('&').is_none_or(|amp| p[amp..].contains(';')))
        );
    }
}
