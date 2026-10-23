use crabber_coworkers::slack::ingress::Identity;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::broadcast,
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Debug)]
pub struct Posted {
    pub channel: String,
    pub thread_ts: String,
    pub text: String,
    pub bearer: String,
}
#[derive(Clone)]
pub struct Request {
    pub method: String,
    pub bearer: String,
    pub body: Value,
}
#[derive(Default)]
struct State {
    posted: Vec<Posted>,
    requests: Vec<Request>,
    acks: Vec<String>,
    failures: HashMap<String, VecDeque<(String, u64)>>,
    connections: usize,
    opens: usize,
    stall_auth: bool,
    invalid_url: bool,
}
#[derive(Clone)]
enum Frame {
    Text(Option<usize>, Value),
    Drop,
}
pub struct FakeSlack {
    pub base: String,
    state: Arc<Mutex<State>>,
    frames: broadcast::Sender<Frame>,
    tasks: Vec<JoinHandle<()>>,
}

impl FakeSlack {
    // tungstenite fixes the callback error type to its HTTP response.
    #[allow(clippy::result_large_err)]
    pub async fn start(identity: Identity) -> Self {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/", http.local_addr().unwrap());
        let ws_url = format!("ws://{}/", ws.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State::default()));
        let (frames, _) = broadcast::channel::<Frame>(128);
        let state_http = state.clone();
        let http_task = http_server(http, ws_url, identity, state_http);
        let state_ws = state.clone();
        let frames_ws = frames.clone();
        let ws_task = tokio::spawn(async move {
            let mut sockets = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = ws.accept() => {
                        let Ok((stream, _)) = accepted else { break; };
                        let state = state_ws.clone();
                        let mut receiver = frames_ws.subscribe();
                        sockets.spawn(async move {
                            let mut index = 0;
                            let mut socket = tokio_tungstenite::accept_hdr_async(stream, |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                                index = request.uri().path().trim_matches('/').parse::<usize>().unwrap();
                                Ok(response)
                            }).await.unwrap();
                            state.lock().unwrap().connections += 1;
                            socket.send(Message::Text(json!({"type":"hello"}).to_string().into())).await.unwrap();
                            loop {
                                tokio::select! {
                                    frame = receiver.recv() => match frame {
                                        Ok(Frame::Text(target, value)) => {
                                            if target.is_none_or(|v| v == index) && socket.send(Message::Text(value.to_string().into())).await.is_err() { break; }
                                        },
                                        Ok(Frame::Drop) | Err(_) => break,
                                    },
                                    incoming = socket.next() => match incoming {
                                        Some(Ok(Message::Text(text))) => {
                                            let value: Value = serde_json::from_str(&text).unwrap();
                                            if let Some(id) = value["envelope_id"].as_str() { state.lock().unwrap().acks.push(id.into()); }
                                        },
                                        Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                                        _ => {},
                                    }
                                }
                            }
                        });
                    },
                    _ = sockets.join_next(), if !sockets.is_empty() => {},
                }
            }
        });
        Self {
            base,
            state,
            frames,
            tasks: vec![http_task, ws_task],
        }
    }

    pub async fn send(&self, value: Value) {
        self.send_to(None, value).await;
    }
    pub async fn send_to(&self, target: Option<usize>, value: Value) {
        wait_until(|| self.frames.receiver_count() > 0).await;
        self.frames
            .send(Frame::Text(target, value))
            .unwrap_or_else(|_| panic!("fake socket missing"));
    }
    pub fn posted(&self) -> Vec<Posted> {
        self.state.lock().unwrap().posted.clone()
    }
    pub fn acks(&self) -> Vec<String> {
        self.state.lock().unwrap().acks.clone()
    }
    pub fn requests(&self) -> Vec<Request> {
        self.state.lock().unwrap().requests.clone()
    }
    pub fn connections(&self) -> usize {
        self.state.lock().unwrap().connections
    }
    pub fn opens(&self) -> usize {
        self.state.lock().unwrap().opens
    }
    pub fn fail_next_post(&self, error: &str) {
        self.fail_next("chat.postMessage", error, 1);
    }
    pub fn fail_next(&self, method: &str, error: &str, seconds: u64) {
        self.state
            .lock()
            .unwrap()
            .failures
            .entry(method.into())
            .or_default()
            .push_back((error.into(), seconds));
    }
    pub fn stall_auth(&self) {
        self.state.lock().unwrap().stall_auth = true;
    }
    pub fn invalid_url(&self) {
        self.state.lock().unwrap().invalid_url = true;
    }
    pub fn stop_http(&self) {
        self.tasks[0].abort();
    }
    pub fn drop_socket(&self) {
        let _ = self.frames.send(Frame::Drop);
    }
}
impl Drop for FakeSlack {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

pub fn identity() -> Identity {
    Identity {
        team_id: "T1".into(),
        user_id: "UBOT".into(),
        bot_id: "B1".into(),
    }
}

pub async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition timed out");
}

pub fn envelope(id: &str, retry_attempt: u64, payload: Value) -> Value {
    {
        let mut value = json!({"type":"events_api","envelope_id":id,"retry_attempt":retry_attempt});
        value["payload"] = payload;
        value
    }
}
pub fn mention(
    event_id: &str,
    team: &str,
    channel: &str,
    user: &str,
    ts: &str,
    thread_ts: Option<&str>,
    text: &str,
) -> Value {
    let mut event = json!({"type":"app_mention","user":user,"text":text,"ts":ts,"channel":channel,"event_ts":ts});
    if let Some(root) = thread_ts {
        event["thread_ts"] = json!(root);
    }
    envelope(
        event_id,
        0,
        json!({"type":"event_callback","team_id":team,"event_id":event_id,"event":event}),
    )
}

fn http_server(
    http: TcpListener,
    ws_url: String,
    identity: Identity,
    state_http: Arc<Mutex<State>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = http.accept() => {
                    let Ok((mut stream, _)) = accepted else { break; };
                    let state = state_http.clone();
                    let ws_url = ws_url.clone();
                    let team = identity.team_id.clone();
                    let user = identity.user_id.clone();
                    let bot = identity.bot_id.clone();
                    connections.spawn(async move {
                        let mut bytes = Vec::new();
                        let mut buf = [0; 4096];
                        let end = loop {
                            let count = stream.read(&mut buf).await.unwrap();
                            if count == 0 { return; }
                            bytes.extend_from_slice(&buf[..count]);
                            if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") { break pos + 4; }
                        };
                        let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                        let header = |name: &str| headers.lines().filter_map(|line| line.split_once(':'))
                            .find(|(key, _)| key.eq_ignore_ascii_case(name)).map_or("", |(_, v)| v.trim());
                        let length: usize = header("content-length").parse().unwrap();
                        while bytes.len() < end + length {
                            let count = stream.read(&mut buf).await.unwrap();
                            if count == 0 { return; }
                            bytes.extend_from_slice(&buf[..count]);
                        }
                        let body: Value = serde_json::from_slice(&bytes[end..end+length]).unwrap();
                        let method = headers.lines().next().unwrap().split_whitespace().nth(1).unwrap().trim_start_matches('/');
                        let bearer = header("authorization").to_owned();
                        let (value, retry, stall) = {
                            let mut state = state.lock().unwrap();
                            state.requests.push(Request { method: method.into(), bearer: bearer.clone(), body: body.clone() });
                            if method == "apps.connections.open" { state.opens += 1; }
                            let failure = state.failures.get_mut(method).and_then(VecDeque::pop_front);
                            if let Some((error, seconds)) = failure {
                                (json!({"ok":false,"error":error,"retry_after":seconds}), seconds, false)
                            } else {
                                let value = match method {
                                    "auth.test" => json!({"ok":true,"team_id":team,"user_id":user,"bot_id":bot}),
                                    "apps.connections.open" => json!({"ok":true,"url":if state.invalid_url {"https://invalid/".into()} else {format!("{ws_url}{}", state.opens - 1)}}),
                                    "chat.postMessage" => {
                                        state.posted.push(Posted {
                                            channel: body["channel"].as_str().unwrap().into(),
                                            thread_ts: body["thread_ts"].as_str().unwrap().into(),
                                            text: body["text"].as_str().unwrap().into(), bearer,
                                        });
                                        json!({"ok":true})
                                    },
                                    _ => json!({"ok":false,"error":"unknown_method"}),
                                };
                                (value, 1, method == "auth.test" && state.stall_auth)
                            }
                        };
                        if stall { std::future::pending::<()>().await; }
                        let value = value.to_string();
                        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nRetry-After: {retry}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{value}", value.len());
                        let _ = stream.write_all(response.as_bytes()).await;
                    });
                },
                _ = connections.join_next(), if !connections.is_empty() => {},
            }
        }
    })
}
