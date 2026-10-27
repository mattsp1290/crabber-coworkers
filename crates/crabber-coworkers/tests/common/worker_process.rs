use std::{
    io::{Read, Write},
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

pub struct ChildWorker {
    child: Option<Child>,
    pub base: String,
}
impl Drop for ChildWorker {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
impl ChildWorker {
    pub async fn start(state: &Path, provider_base: &str, ready: &Path) -> Self {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "worker_process::child_worker",
                "--ignored",
                "--nocapture",
            ])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("COWORKERS_TEST_CHILD", "1")
            .env("COWORKERS_TEST_STATE", state)
            .env("COWORKERS_TEST_READY", ready)
            .env("COWORKERS_TEST_PROVIDER", provider_base)
            .env("NO_PROXY", "127.0.0.1,localhost")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut worker = Self {
            child: Some(child),
            base: String::new(),
        };
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    worker.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                    "worker exited before readiness"
                );
                if let Ok(address) = std::fs::read_to_string(ready) {
                    worker.base = format!("http://{address}");
                    if let Ok(response) =
                        client.get(format!("{}/healthz", worker.base)).send().await
                        && response.status().is_success()
                    {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        worker
    }
    pub fn id(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }
    pub async fn stop(mut self) -> (String, String) {
        self.child
            .as_mut()
            .unwrap()
            .stdin
            .take()
            .unwrap()
            .write_all(b"x")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let output = self.child.take().unwrap().wait_with_output().unwrap();
        (
            String::from_utf8(output.stdout).unwrap(),
            String::from_utf8(output.stderr).unwrap(),
        )
    }
}

#[tokio::test]
#[ignore = "spawned with synthetic configuration by the parent journey"]
async fn child_worker() {
    if std::env::var("COWORKERS_TEST_CHILD").as_deref() != Ok("1") {
        return;
    }
    let state = std::env::var_os("COWORKERS_TEST_STATE").unwrap();
    let ready = std::env::var_os("COWORKERS_TEST_READY").unwrap();
    let provider = std::env::var("COWORKERS_TEST_PROVIDER").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    std::fs::write(ready, listener.local_addr().unwrap().to_string()).unwrap();
    let stop = tokio::task::spawn_blocking(|| {
        let mut byte = [0];
        std::io::stdin().read_exact(&mut byte).unwrap();
    });
    crabber_coworkers::worker::run_with(
        "scout".into(),
        state.into(),
        crabber_coworkers::bridge::opencode_go_resolver("synthetic-test-key", Some(&provider)),
        crabber_coworkers::bridge::production_selection(),
        listener,
        Box::pin(async move {
            stop.await.unwrap();
        }),
    )
    .await
    .unwrap();
}
