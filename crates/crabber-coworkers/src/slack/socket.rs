use super::api::{ApiError, SlackApi};
use crate::ask::Cancel;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::Instant};
use tokio_tungstenite::tungstenite::Message;
use url::Url;

pub const STALE_AFTER: Duration = Duration::from_secs(120);
pub const STABLE_SESSION: Duration = Duration::from_secs(30);
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);

pub enum Inbound {
    Ready,
    Envelope(Envelope),
}
pub struct Envelope {
    pub id: String,
    pub retry_attempt: u64,
    pub payload: Value,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum SocketError {
    #[error("Slack authentication failed")]
    Auth,
    #[error("Slack disabled the Socket Mode link")]
    Disabled,
}

/// Run one coworker's Socket Mode connection; the caller prefixes fixed log messages with its ID.
///
/// # Errors
/// Returns a fatal error only for rejected credentials or a disabled Socket Mode link.
pub async fn run<F>(
    api: Arc<SlackApi>,
    mut first_url: Option<Url>,
    mut stop: Cancel,
    log: mpsc::UnboundedSender<String>,
    mut handle: F,
) -> Result<(), SocketError>
where
    F: FnMut(Inbound) -> Option<String> + Send,
{
    let mut backoff = Duration::from_secs(1);
    loop {
        let started = Instant::now();
        let url = if let Some(url) = first_url.take() {
            Ok(url)
        } else {
            tokio::select! {
                biased;
                () = &mut stop => return Ok(()),
                result = api.connections_open() => result,
            }
        };
        match url {
            Err(ApiError::Auth | ApiError::Rejected) => return Err(SocketError::Auth),
            Err(ApiError::Transport | ApiError::RateLimited { .. }) => {}
            Ok(url) => {
                let connection = tokio::select! {
                    biased;
                    () = &mut stop => return Ok(()),
                    result = tokio_tungstenite::connect_async(url.as_str()) => result,
                };
                if let Ok((mut socket, _)) = connection
                    && session(&mut socket, &mut stop, &mut handle).await?
                {
                    return Ok(());
                }
            }
        }
        let _ = log.send("Slack reconnecting".into());
        if started.elapsed() >= STABLE_SESSION {
            backoff = Duration::from_secs(1);
        } else {
            tokio::select! {
                biased;
                () = &mut stop => return Ok(()),
                () = tokio::time::sleep(backoff) => {},
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }
}

async fn session<F>(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    stop: &mut Cancel,
    handle: &mut F,
) -> Result<bool, SocketError>
where
    F: FnMut(Inbound) -> Option<String> + Send,
{
    loop {
        let frame = tokio::select! {
            biased;
            () = &mut *stop => {
                let _ = tokio::time::timeout(Duration::from_secs(1), socket.close(None)).await;
                return Ok(true);
            },
            frame = tokio::time::timeout(STALE_AFTER, socket.next()) => frame,
        };
        let Ok(Some(Ok(frame))) = frame else {
            break;
        };
        let ack = match frame {
            Message::Text(text) => {
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                match value["type"].as_str() {
                    Some("hello") => {
                        handle(Inbound::Ready);
                        None
                    }
                    Some("disconnect") => {
                        if value["reason"] == "link_disabled" {
                            return Err(SocketError::Disabled);
                        }
                        break;
                    }
                    kind => {
                        let Some(id) = value["envelope_id"]
                            .as_str()
                            .filter(|v| !v.is_empty() && v.len() <= 128)
                        else {
                            continue;
                        };
                        if kind == Some("events_api") {
                            handle(Inbound::Envelope(Envelope {
                                id: id.into(),
                                retry_attempt: value["retry_attempt"].as_u64().unwrap_or(0),
                                payload: value["payload"].clone(),
                            }))
                        } else {
                            Some(id.to_owned())
                        }
                    }
                }
            }
            Message::Ping(_) => {
                let flushed = tokio::select! {
                    biased;
                    () = &mut *stop => return Ok(true),
                    result = socket.flush() => result,
                };
                if flushed.is_err() {
                    break;
                }
                None
            }
            Message::Close(_) => break,
            _ => None,
        };
        if let Some(id) = ack {
            let sent = tokio::select! {
                biased;
                () = &mut *stop => return Ok(true),
                result = socket.send(Message::Text(json!({"envelope_id":id}).to_string().into())) => result,
            };
            if sent.is_err() {
                break;
            }
        }
    }
    Ok(false)
}
