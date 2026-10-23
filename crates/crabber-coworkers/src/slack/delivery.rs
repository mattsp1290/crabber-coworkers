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
        if prefer_newline && let Some(newline) = text[..end].rfind('\n') {
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

/// # Errors
/// Returns the first failed chunk's fixed error category, or Transport on cancellation.
pub async fn post_answer(
    api: &SlackApi,
    channel: &str,
    root_ts: &str,
    answer: &str,
    stop: &mut Cancel,
) -> Result<(), ApiError> {
    for chunk in chunks(&escape(answer)) {
        let result = tokio::select! {
            biased;
            () = &mut *stop => return Err(ApiError::Transport),
            result = api.post_message(channel, root_ts, &chunk) => result,
        };
        if let Err(ApiError::RateLimited {
            retry_after_seconds,
        }) = result
        {
            tokio::select! {
                biased;
                () = &mut *stop => return Err(ApiError::Transport),
                () = tokio::time::sleep(Duration::from_secs(retry_after_seconds)) => {},
            }
            tokio::select! {
                biased;
                () = &mut *stop => return Err(ApiError::Transport),
                result = api.post_message(channel, root_ts, &chunk) => result?,
            }
        } else {
            result?;
        }
    }
    Ok(())
}
