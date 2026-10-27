use super::*;
use clap::Parser;
use crabber_coworkers::app::{Cli, run_with};

#[tokio::test]
async fn unnamed_cli_files_persist_without_sqlite_and_only_answers_reach_stdout() {
    let directory = tempfile::tempdir().unwrap();
    let path = common::write_state_config(directory.path(), 5);
    let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["coworkers"][0]["workspace"] = json!("read_write");
    std::fs::write(&path, config.to_string()).unwrap();
    let state = directory.path().join("state");
    for (tool, arguments) in [
        (
            "file_write",
            json!({"path":"draft.txt","content":"private-file-marker"}),
        ),
        ("file_read", json!({"path":"draft.txt"})),
    ] {
        let provider = tool_provider(tool, &arguments);
        let mut out = Vec::new();
        let mut err = Vec::new();
        let cli = Cli::parse_from([
            "coworkers",
            "ask",
            "--config",
            path.to_str().unwrap(),
            "scout",
            "private-prompt-marker",
        ]);
        assert_eq!(
            run_with(
                cli,
                provider.clone(),
                common::selection(),
                Some(common::never()),
                &mut out,
                &mut err
            )
            .await,
            0
        );
        assert_eq!(out, b"done\n");
        assert_eq!(err, b"");
        let requests = provider.requests();
        assert_eq!(
            common::tool_results(requests.last().unwrap())[0]["outcome"],
            "succeeded"
        );
        assert_eq!(
            std::fs::read_to_string(state.join("workspaces/scout/draft.txt")).unwrap(),
            "private-file-marker"
        );
        assert!(!state.join("scout.sqlite").exists());
    }
    let provider = Arc::new(FakeProvider::scripted(vec![
        common::tool_call(
            "file_write",
            &json!({"path":"draft.txt","content":"committed-before-error"}),
        ),
        vec![common::failure(false)],
    ]));
    let mut out = Vec::new();
    let mut err = Vec::new();
    let cli = Cli::parse_from([
        "coworkers",
        "ask",
        "--config",
        path.to_str().unwrap(),
        "scout",
        "private-prompt-marker",
    ]);
    assert_eq!(
        run_with(
            cli,
            provider,
            common::selection(),
            Some(common::never()),
            &mut out,
            &mut err
        )
        .await,
        1
    );
    assert_eq!(out, b"");
    assert_eq!(err, b"model run failed\n");
    assert_eq!(
        std::fs::read_to_string(state.join("workspaces/scout/draft.txt")).unwrap(),
        "committed-before-error"
    );
    assert!(!state.join("scout.sqlite").exists());
}

#[derive(Clone, Default)]
struct WriteThenWait {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    waiting: Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl Resolver for WriteThenWait {
    fn binding_descriptor(
        &self,
        selection: &crabber::Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, crabber::providers::ProviderBindingError>
    {
        common::Pending.binding_descriptor(selection)
    }
    async fn resolve(
        &self,
        _: &crabber::Selection,
    ) -> Result<Arc<dyn crabber::providers::Streamer>, crabber::providers::ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait::async_trait]
impl crabber::providers::Streamer for WriteThenWait {
    async fn stream(
        &self,
        _: crabber::providers::ModelRequest,
    ) -> Result<crabber::providers::DeltaStream, crabber::providers::ProviderError> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            Ok(Box::pin(futures::stream::iter(common::tool_call(
                "file_write",
                &json!({"path":"draft.txt","content":"committed-before-stop"}),
            ))))
        } else {
            self.waiting.notify_one();
            Ok(Box::pin(futures::stream::pending()))
        }
    }
}

#[tokio::test]
async fn timeout_and_cancellation_preserve_completed_writes() {
    for interrupt in [false, true] {
        let disk = tempfile::tempdir().unwrap();
        let def = definition(WorkspaceAccess::ReadWrite);
        let provider = Arc::new(WriteThenWait::default());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let workspace = workspace::admit(&def, disk.path(), deadline, &mut common::never())
            .await
            .unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let cancel: ask::Cancel = Box::pin(async {
            let _ = stopped.await;
        });
        let turn = ask::ask(
            &def,
            &workspace,
            "synthetic",
            provider.clone(),
            common::selection(),
            deadline,
            Session::Ephemeral,
            cancel,
        );
        let stop_turn = async {
            provider.waiting.notified().await;
            assert_eq!(
                std::fs::read_to_string(disk.path().join("workspaces/scout/draft.txt")).unwrap(),
                "committed-before-stop"
            );
            if interrupt {
                stop.send(()).unwrap();
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(turn, stop_turn);
        let result = tokio::select! {
            result = &mut turn => result,
            () = &mut stop_turn => turn.await,
        };
        assert_eq!(
            result,
            Err(if interrupt {
                AskError::Interrupted
            } else {
                AskError::Timeout
            })
        );
        assert_eq!(
            std::fs::read_to_string(disk.path().join("workspaces/scout/draft.txt")).unwrap(),
            "committed-before-stop"
        );
    }
}

#[tokio::test]
async fn precancelled_ask_cannot_start_a_file_tool() {
    let disk = tempfile::tempdir().unwrap();
    let def = definition(WorkspaceAccess::ReadWrite);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let workspace = workspace::admit(&def, disk.path(), deadline, &mut common::never())
        .await
        .unwrap();
    let provider = tool_provider(
        "file_write",
        &json!({"path":"forbidden","content":"forbidden"}),
    );
    assert_eq!(
        ask::ask(
            &def,
            &workspace,
            "synthetic",
            provider.clone(),
            common::selection(),
            deadline,
            Session::Ephemeral,
            Box::pin(std::future::ready(()))
        )
        .await,
        Err(AskError::Interrupted)
    );
    assert_eq!(provider.requests().len(), 0);
    assert!(!disk.path().join("workspaces/scout/forbidden").exists());
}
