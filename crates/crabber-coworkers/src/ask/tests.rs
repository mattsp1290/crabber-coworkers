use super::*;
#[tokio::test]
async fn completed_answer_wins_when_signal_and_deadline_are_ready() {
    let mut done = std::pin::pin!(std::future::ready("committed answer"));
    let mut cancel: Cancel = Box::pin(std::future::ready(()));
    assert_eq!(
        wait_for_completion(done.as_mut(), &mut cancel, tokio::time::Instant::now()).await,
        Ok("committed answer")
    );
}

#[derive(Default)]
struct OwnedWork {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    finished: std::sync::atomic::AtomicBool,
    fail: bool,
}
struct ResourceTool(Arc<OwnedWork>);
#[async_trait::async_trait]
impl crabber::JsonToolExecutor for ResourceTool {
    async fn execute(
        &self,
        _: serde_json::Value,
    ) -> Result<serde_json::Value, crabber::ExtensionError> {
        unreachable!("fixture requires runtime context")
    }
    async fn execute_with_context(
        &self,
        context: crabber::extension::ToolContext,
        _: serde_json::Value,
    ) -> Result<serde_json::Value, crabber::ExtensionError> {
        let work = self.0.clone();
        context
            .resources()
            .spawn(async move {
                work.release.notified().await;
                work.finished
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                if work.fail {
                    Err(crabber::extension::ResourceFailure::CleanupFailed)
                } else {
                    Ok(())
                }
            })
            .unwrap();
        self.0.entered.notify_one();
        std::future::pending().await
    }
}

fn resource_agent(work: Arc<OwnedWork>) -> crabber::Agent {
    let id = crabber::core::ToolCallId::new();
    crabber::Agent::builder()
        .behavior_version("owned-work-test")
        .store(Arc::new(MemoryStore::new()))
        .provider(Arc::new(crabber::FakeProvider::scripted(vec![vec![
            crabber::StreamDelta::ToolCallStart {
                call_id: id.clone(),
                name: "owned".into(),
            },
            crabber::StreamDelta::ToolCallArgsDelta {
                call_id: id.clone(),
                text: "{}".into(),
            },
            crabber::StreamDelta::ToolCallDone { call_id: id },
            crabber::StreamDelta::Completed,
        ]])))
        .config(crabber::AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(
            crabber::StaticPolicy::new(crabber::PermissionDecision::Deny)
                .with_rule("owned", crabber::PermissionDecision::Allow),
        ))
        .shared_tool(Arc::new(crabber::ToolDefinition {
            info: crabber::core::ToolInfo {
                name: "owned".into(),
                description: "controlled owned work".into(),
                parameters: serde_json::json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(ResourceTool(work)),
        }))
        .build()
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn cleanup_is_bounded_and_retained_work_stays_owned_until_release() {
    let work = Arc::new(OwnedWork::default());
    let agent = resource_agent(work.clone());
    let run = agent.prompt(None, "synthetic").await.unwrap();
    work.entered.notified().await;
    run.interrupt();
    let started = tokio::time::Instant::now();
    assert!(!cleanup(&agent).await);
    assert!(started.elapsed() <= Duration::from_secs(INTERRUPT_SETTLE_SECONDS));
    assert!(!work.finished.load(std::sync::atomic::Ordering::SeqCst));
    assert!(matches!(
        agent.prompt(None, "refused").await,
        Err(crabber::RuntimeError::HostClosed)
    ));
    work.release.notify_one();
    let report = agent.drain_execution_tasks(Duration::from_secs(5)).await;
    assert!(work.finished.load(std::sync::atomic::Ordering::SeqCst));
    assert!(drained(&report));
    assert!(matches!(
        run.done().await,
        Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
    ));
}

#[tokio::test(start_paused = true)]
async fn cleanup_accepts_joined_resources_and_rejects_failed_resources() {
    for fail in [false, true] {
        let work = Arc::new(OwnedWork {
            fail,
            ..OwnedWork::default()
        });
        let agent = resource_agent(work.clone());
        let run = agent.prompt(None, "synthetic").await.unwrap();
        work.entered.notified().await;
        run.interrupt();
        work.release.notify_one();
        assert_eq!(cleanup(&agent).await, !fail);
        assert!(work.finished.load(std::sync::atomic::Ordering::SeqCst));
        assert!(matches!(
            run.done().await,
            Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
        ));
    }
}

#[test]
fn drainage_requires_successful_joins_and_sealed_resource_ownership() {
    let session_id = crabber::SessionId::new();
    let run_id = crabber::core::RunId::new();
    let mut report = crabber::ExecutionTaskDrain::default();
    report.joined.push(crabber::JoinedExecutionTask {
        session_id: session_id.clone(),
        run_id: run_id.clone(),
        termination: crabber::TaskTermination::Returned,
    });
    report.resources.push(crabber::ToolResourceDrain {
        session_id,
        run_id: run_id.clone(),
        call_id: crabber::core::ToolCallId::new(),
        phase: None,
        resources: crabber::extension::ResourceDrain {
            sealed: true,
            joined: 1,
            pending: 0,
            failures: vec![],
        },
    });
    assert!(drained(&report));
    report.resources[0].resources.sealed = false;
    assert!(!drained(&report));
    report.resources[0].resources.sealed = true;
    report.resources[0].resources.pending = 1;
    assert!(!drained(&report));
    report.resources[0].resources.pending = 0;
    report.joined[0].termination = crabber::TaskTermination::Panicked;
    assert!(!drained(&report));
    report.joined[0].termination = crabber::TaskTermination::Returned;
    report.pending.push(run_id);
    assert!(!drained(&report));
}
