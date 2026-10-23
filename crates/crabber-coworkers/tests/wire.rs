mod common;
use crabber_coworkers::ask::Session;
#[path = "common/example.rs"]
mod example;
const SSE_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"pong\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
use crabber_coworkers::{
    ask::ask,
    bridge::{opencode_go_resolver, production_selection},
};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn opencode_go_request_shape() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0; 1024];
            let header_end = loop {
                let count = socket.read(&mut chunk).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&chunk[..count]);
                if let Some(pos) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
            let lines: Vec<_> = headers.lines().collect();
            assert_eq!(
                lines[0].split_whitespace().nth(1),
                Some("/chat/completions")
            );
            let header = |name: &str| {
                lines
                    .iter()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(key, _)| key.eq_ignore_ascii_case(name))
                    .unwrap()
                    .1
                    .trim()
            };
            assert_eq!(header("authorization"), "Bearer test-key");
            let length: usize = header("content-length").parse().unwrap();
            while request.len() < header_end + length {
                let count = socket.read(&mut chunk).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&chunk[..count]);
            }
            let body: serde_json::Value =
                serde_json::from_slice(&request[header_end..header_end + length]).unwrap();
            assert_eq!(body["model"], "deepseek-v4-flash");
            assert_eq!(body["messages"][0]["role"], "system");
            assert!(
                body["messages"][0]["content"]
                    .as_str()
                    .unwrap()
                    .contains("You are Scout")
            );
            socket.write_all(SSE_RESPONSE).await.unwrap();
            socket.shutdown().await.unwrap();
        });
        let config = example::config();
        let resolver = opencode_go_resolver("test-key", Some(&url));
        let answer = ask(
            config.coworker("scout").unwrap(),
            "ping",
            Arc::clone(&resolver),
            production_selection(),
            tokio::time::Instant::now() + Duration::from_secs(5),
            Session::Ephemeral,
            common::never(),
        )
        .await
        .unwrap();
        assert_eq!(answer, "pong");
        server.await.unwrap();
    })
    .await
    .unwrap();
}
