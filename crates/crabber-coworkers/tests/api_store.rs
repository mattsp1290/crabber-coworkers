use crabber_coworkers::{
    api::{
        contract::{ApiUuid, CoworkerId, FailureReason},
        store::{
            ApiStore, FaultPoint, Faults, Finished, MarkedRunning, OpenError, Opened, Outcome,
            RequestRow, RequestState, Requeued, Retiring, StoreFault, Submitted,
        },
    },
    store::{API_LOCK_FILE, API_STORE_FILE},
};
use serde_json::{Value, json};
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use std::{path::Path, sync::Arc, time::Duration};
use time::{OffsetDateTime, UtcOffset};
use tokio::{sync::Barrier, task::JoinSet};

const MAX: usize = 16;

fn id(n: u128) -> ApiUuid {
    ApiUuid::parse(&uuid::Uuid::from_u128(n).hyphenated().to_string()).unwrap()
}

fn coworker(name: &str) -> CoworkerId {
    CoworkerId::parse(name).unwrap()
}

fn at(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_800_000_000 + seconds).unwrap()
}

async fn opened() -> (tempfile::TempDir, ApiStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = ApiStore::open(dir.path()).await.unwrap();
    open(&store, 1, "scout").await;
    (dir, store)
}

async fn open(store: &ApiStore, conversation: u128, owner: &str) {
    let opened = store
        .open_conversation(id(conversation), &coworker(owner))
        .await
        .unwrap();
    assert!(matches!(opened, Opened::Created), "{opened:?}");
}

async fn insert(store: &ApiStore, conversation: u128, request: u128, text: &str) -> RequestRow {
    match store
        .submit(id(conversation), id(request), text, MAX)
        .await
        .unwrap()
    {
        Submitted::Inserted(row) => row,
        other => panic!("{other:?}"),
    }
}

async fn start(store: &ApiStore, row: &RequestRow) -> RequestRow {
    match store.mark_running(row.seq()).await.unwrap() {
        MarkedRunning::Started(row) => row,
        other => panic!("{other:?}"),
    }
}

async fn finish(store: &ApiStore, row: &RequestRow, outcome: Outcome) -> RequestRow {
    match store.finish(row.seq(), outcome).await.unwrap() {
        Finished::Settled(row) => row,
        unchanged @ Finished::Unchanged(_) => panic!("{unchanged:?}"),
    }
}

async fn settle(store: &ApiStore, row: &RequestRow, outcome: Outcome) -> RequestRow {
    start(store, row).await;
    finish(store, row, outcome).await
}

fn completed(text: &str) -> Outcome {
    Outcome::Completed(text.to_owned())
}

fn ids(rows: &[RequestRow]) -> Vec<ApiUuid> {
    rows.iter().map(RequestRow::request_id).collect()
}

fn receipt(row: RequestRow) -> Value {
    serde_json::to_value(row.into_receipt()).unwrap()
}

#[cfg(unix)]
fn mode(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

#[cfg(unix)]
#[tokio::test]
async fn open_creates_private_files() {
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("nested");
    let dir = parent.join("state");
    let store = ApiStore::open(&dir).await.unwrap();
    open(&store, 1, "scout").await;
    assert_eq!(mode(&parent), 0o700);
    assert_eq!(mode(&dir), 0o700);
    for file in [
        API_STORE_FILE,
        "api.sqlite-wal",
        "api.sqlite-shm",
        API_LOCK_FILE,
    ] {
        assert_eq!(mode(&dir.join(file)), 0o600, "{file}");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_paths_are_refused() {
    for file in [API_STORE_FILE, API_LOCK_FILE] {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        std::fs::write(&target, "").unwrap();
        std::os::unix::fs::symlink(&target, dir.path().join(file)).unwrap();
        assert!(
            matches!(ApiStore::open(dir.path()).await, Err(OpenError::Fault)),
            "{file}"
        );
        assert_eq!(std::fs::metadata(&target).unwrap().len(), 0, "{file}");
    }
}

#[tokio::test]
async fn second_open_is_in_use_until_every_handle_drops() {
    let dir = tempfile::tempdir().unwrap();
    let first = ApiStore::open(dir.path()).await.unwrap();
    let clone = first.clone();
    assert!(matches!(
        ApiStore::open(dir.path()).await,
        Err(OpenError::InUse)
    ));
    drop(first);
    assert!(matches!(
        ApiStore::open(dir.path()).await,
        Err(OpenError::InUse)
    ));
    drop(clone);
    ApiStore::open(dir.path()).await.unwrap();
    assert_eq!(OpenError::InUse.to_string(), "API store is in use");
    assert_eq!(OpenError::Fault.to_string(), "cannot open API store");
}

#[tokio::test]
async fn unknown_schema_version_or_foreign_file_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    drop(ApiStore::open(dir.path()).await.unwrap());
    let path = dir.path().join(API_STORE_FILE);
    let mut connection =
        SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&path))
            .await
            .unwrap();
    sqlx::query("PRAGMA user_version = 2")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    assert!(matches!(
        ApiStore::open(dir.path()).await,
        Err(OpenError::Fault)
    ));

    let foreign = tempfile::tempdir().unwrap();
    std::fs::write(foreign.path().join(API_STORE_FILE), [0xab; 4096]).unwrap();
    assert!(matches!(
        ApiStore::open(foreign.path()).await,
        Err(OpenError::Fault)
    ));
}

#[tokio::test]
async fn submit_records_one_attempt_per_identity() {
    let (_dir, store) = opened().await;
    let row = insert(&store, 1, 10, "hello").await;
    assert_eq!(
        (row.request_id(), row.text(), row.state()),
        (id(10), "hello", &RequestState::Accepted)
    );
    let Submitted::Existing(replayed) = store.submit(id(1), id(10), "hello", MAX).await.unwrap()
    else {
        panic!("expected a replay");
    };
    assert_eq!(replayed, row);
    assert!(matches!(
        store.submit(id(1), id(10), "hello!", MAX).await.unwrap(),
        Submitted::TextConflict
    ));
    assert!(matches!(
        store.submit(id(2), id(10), "hello", MAX).await.unwrap(),
        Submitted::NoConversation
    ));
    assert_eq!(store.request(id(1), id(10)).await.unwrap(), Some(row));
    assert_eq!(store.request(id(1), id(11)).await.unwrap(), None);
    assert_eq!(store.request(id(2), id(10)).await.unwrap(), None);
}

#[tokio::test]
async fn identity_compares_text_byte_for_byte() {
    let (_dir, store) = opened().await;
    insert(&store, 1, 10, "caf\u{e9}").await;
    insert(&store, 1, 11, "hi").await;
    for (request, text) in [(10, "cafe\u{301}"), (11, "hi "), (11, "Hi")] {
        assert!(
            matches!(
                store.submit(id(1), id(request), text, MAX).await.unwrap(),
                Submitted::TextConflict
            ),
            "{request}"
        );
    }
}

#[tokio::test]
async fn queue_bound_counts_only_this_conversations_active_requests() {
    let (_dir, store) = opened().await;
    open(&store, 2, "scout").await;
    let first = insert(&store, 1, 10, "a").await;
    let second = store.submit(id(1), id(11), "b", 2).await.unwrap();
    assert!(matches!(second, Submitted::Inserted(_)));
    assert!(matches!(
        store.submit(id(1), id(12), "c", 2).await.unwrap(),
        Submitted::QueueFull
    ));
    assert_eq!(store.request(id(1), id(12)).await.unwrap(), None);
    assert!(matches!(
        store.submit(id(1), id(10), "a", 2).await.unwrap(),
        Submitted::Existing(_)
    ));
    assert!(matches!(
        store.submit(id(2), id(13), "d", 1).await.unwrap(),
        Submitted::Inserted(_)
    ));
    settle(&store, &first, completed("done")).await;
    assert!(matches!(
        store.submit(id(1), id(12), "c", 2).await.unwrap(),
        Submitted::Inserted(_)
    ));
}

#[tokio::test]
async fn completed_wins_over_a_pending_cancellation() {
    let (_dir, store) = opened().await;
    let row = insert(&store, 1, 10, "q").await;
    start(&store, &row).await;
    let pending = store
        .request_cancel(id(1), id(10), at(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.state(), &RequestState::CancellationRequested);
    let done = finish(&store, &row, completed("answer")).await;
    assert_eq!(done.state(), &RequestState::Settled(completed("answer")));
    assert_eq!(
        receipt(done),
        json!({
            "apiVersion": "1",
            "conversationId": id(1).to_string(),
            "requestId": id(10).to_string(),
            "acceptance": {"boundary": "sqlite_outbox"},
            "status": "completed",
            "result": {"text": "answer"},
            "cancellation": {"requestedAt": "2027-01-15T08:00:01Z"},
        })
    );
}

#[tokio::test]
async fn settled_requests_never_change() {
    let (_dir, store) = opened().await;
    let outcomes = [
        completed("a"),
        Outcome::Cancelled,
        Outcome::Failed(FailureReason::Timeout),
        Outcome::Uncertain(FailureReason::ProcessStopped),
    ];
    for (request, outcome) in (100..).zip(&outcomes) {
        let row = insert(&store, 1, request, "q").await;
        start(&store, &row).await;
        if *outcome == Outcome::Cancelled {
            store
                .request_cancel(id(1), id(request), at(0))
                .await
                .unwrap();
        }
        let settled = finish(&store, &row, outcome.clone()).await;
        assert_eq!(settled.state(), &RequestState::Settled(outcome.clone()));
        for later in &outcomes {
            assert_eq!(
                store.finish(row.seq(), later.clone()).await.unwrap(),
                Finished::Unchanged(settled.clone())
            );
        }
        assert_eq!(
            store.mark_running(row.seq()).await.unwrap(),
            MarkedRunning::Settled(settled.clone())
        );
        assert_eq!(
            store.requeue(row.seq()).await.unwrap(),
            Requeued::Kept(settled.clone())
        );
        assert_eq!(
            store.interrupt(row.seq()).await.unwrap(),
            Finished::Unchanged(settled.clone())
        );
        assert_eq!(
            store
                .begin_retirement(row.seq(), FailureReason::ProcessStopped)
                .await
                .unwrap(),
            Retiring::Kept(settled.clone())
        );
        assert_eq!(
            store
                .request_cancel(id(1), id(request), at(9))
                .await
                .unwrap()
                .as_ref(),
            Some(&settled)
        );
        assert_eq!(
            store.request(id(1), id(request)).await.unwrap(),
            Some(settled)
        );
    }
}

#[tokio::test]
async fn finish_reports_an_outcome_that_does_not_apply_as_unchanged() {
    let (_dir, store) = opened().await;
    let row = insert(&store, 1, 10, "q").await;
    for outcome in [
        completed("a"),
        Outcome::Cancelled,
        Outcome::Uncertain(FailureReason::ProcessStopped),
    ] {
        assert_eq!(
            store.finish(row.seq(), outcome).await.unwrap(),
            Finished::Unchanged(row.clone())
        );
    }
    let running = start(&store, &row).await;
    assert_eq!(
        store.finish(row.seq(), Outcome::Cancelled).await.unwrap(),
        Finished::Unchanged(running.clone())
    );
    assert_eq!(running.state(), &RequestState::Running);
    assert_eq!(running.cancel_requested_at(), None);
    assert_eq!(store.request(id(1), id(10)).await.unwrap(), Some(running));
}

async fn retire(store: &ApiStore, row: &RequestRow, reason: FailureReason) -> RequestRow {
    match store.begin_retirement(row.seq(), reason).await.unwrap() {
        Retiring::Recorded(row) => row,
        kept @ Retiring::Kept(_) => panic!("{kept:?}"),
    }
}

async fn interrupt(store: &ApiStore, row: &RequestRow) -> RequestRow {
    match store.interrupt(row.seq()).await.unwrap() {
        Finished::Settled(row) => row,
        unchanged @ Finished::Unchanged(_) => panic!("{unchanged:?}"),
    }
}

#[tokio::test]
async fn an_interruption_settles_by_what_the_request_holds() {
    let (_dir, store) = opened().await;
    let stopped = Outcome::Uncertain(FailureReason::ProcessStopped);

    let queued = insert(&store, 1, 10, "q").await;
    assert_eq!(
        store.interrupt(queued.seq()).await.unwrap(),
        Finished::Unchanged(queued)
    );

    let running = insert(&store, 1, 11, "q").await;
    start(&store, &running).await;
    let failed = interrupt(&store, &running).await;
    assert_eq!(
        failed.state(),
        &RequestState::Settled(Outcome::Failed(FailureReason::Interrupted))
    );
    assert_eq!(failed.cancel_requested_at(), None);

    let cancelling = insert(&store, 1, 12, "q").await;
    start(&store, &cancelling).await;
    store.request_cancel(id(1), id(12), at(1)).await.unwrap();
    let cancelled = interrupt(&store, &cancelling).await;
    assert_eq!(
        cancelled.state(),
        &RequestState::Settled(Outcome::Cancelled)
    );
    assert_eq!(
        cancelled.cancel_requested_at(),
        Some("2027-01-15T08:00:01Z")
    );

    // Reconciliation's own interruption of the run outranks a stop and a cancellation.
    let retired = insert(&store, 1, 13, "q").await;
    start(&store, &retired).await;
    retire(&store, &retired, FailureReason::ProcessStopped).await;
    let uncertain = interrupt(&store, &retired).await;
    assert_eq!(uncertain.state(), &RequestState::Settled(stopped.clone()));

    let both = insert(&store, 1, 14, "q").await;
    start(&store, &both).await;
    store.request_cancel(id(1), id(14), at(2)).await.unwrap();
    retire(&store, &both, FailureReason::ProcessStopped).await;
    let uncertain = interrupt(&store, &both).await;
    assert_eq!(uncertain.state(), &RequestState::Settled(stopped));
    assert_eq!(
        uncertain.cancel_requested_at(),
        Some("2027-01-15T08:00:02Z")
    );
    assert_eq!(
        receipt(uncertain),
        json!({
            "apiVersion": "1",
            "conversationId": id(1).to_string(),
            "requestId": id(14).to_string(),
            "acceptance": {"boundary": "sqlite_outbox"},
            "status": "uncertain",
            "reason": "process_stopped",
            "cancellation": {"requestedAt": "2027-01-15T08:00:02Z"},
        })
    );
}

#[tokio::test]
async fn a_retirement_reason_is_recorded_only_for_a_started_request() {
    let (_dir, store) = opened().await;
    let row = insert(&store, 1, 10, "q").await;
    assert_eq!(
        store
            .begin_retirement(row.seq(), FailureReason::ProcessStopped)
            .await
            .unwrap(),
        Retiring::Kept(row.clone())
    );
    assert_eq!(row.retirement(), None);

    let running = start(&store, &row).await;
    let retiring = retire(&store, &row, FailureReason::ConfigurationChanged).await;
    assert_eq!(retiring.state(), &RequestState::Running);
    assert_eq!(
        retiring.retirement(),
        Some(FailureReason::ConfigurationChanged)
    );
    assert_eq!(
        retire(&store, &row, FailureReason::ConfigurationChanged).await,
        retiring
    );
    // The latest reconciliation is the one that interrupts the run, so its reason stands.
    let replaced = retire(&store, &row, FailureReason::ProcessStopped).await;
    assert_eq!(replaced.retirement(), Some(FailureReason::ProcessStopped));
    assert_eq!(store.request(id(1), id(10)).await.unwrap(), Some(replaced));

    // A request back in the queue was never admitted, so nothing of it was retired.
    assert_eq!(
        store.requeue(row.seq()).await.unwrap(),
        Requeued::Requeued(row.clone())
    );
    assert_eq!(start(&store, &row).await, running);

    // An explicit outcome settles a retiring request as given.
    retire(&store, &row, FailureReason::ProcessStopped).await;
    let done = finish(&store, &row, completed("answer")).await;
    assert_eq!(done.state(), &RequestState::Settled(completed("answer")));
    assert_eq!(receipt(done)["status"], "completed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_racing_an_interruption_is_honoured_or_finds_the_request_settled() {
    let (_dir, store) = opened().await;
    let mut races = JoinSet::new();
    for request in 100..116 {
        let row = insert(&store, 1, request, "q").await;
        start(&store, &row).await;
        let barrier = Arc::new(Barrier::new(2));
        let (interrupting, cancelling) = (store.clone(), store.clone());
        let (first, second) = (Arc::clone(&barrier), barrier);
        races.spawn(async move {
            first.wait().await;
            // Whichever commits first, the interruption is what settles the request.
            let interrupted = interrupting.interrupt(row.seq()).await.unwrap();
            assert!(matches!(interrupted, Finished::Settled(_)));
        });
        races.spawn(async move {
            second.wait().await;
            cancelling
                .request_cancel(id(1), id(request), at(0))
                .await
                .unwrap();
        });
    }
    while let Some(raced) = races.join_next().await {
        raced.unwrap();
    }
    for request in 100..116 {
        let row = store.request(id(1), id(request)).await.unwrap().unwrap();
        let expected = match row.cancel_requested_at() {
            Some(_) => Outcome::Cancelled,
            None => Outcome::Failed(FailureReason::Interrupted),
        };
        assert_eq!(row.state(), &RequestState::Settled(expected), "{request}");
    }
}

#[tokio::test]
async fn an_accepted_request_can_fail_without_starting() {
    let (_dir, store) = opened().await;
    let row = insert(&store, 1, 10, "q").await;
    let failed = finish(
        &store,
        &row,
        Outcome::Failed(FailureReason::CoworkerRemoved),
    )
    .await;
    assert_eq!(
        failed.state(),
        &RequestState::Settled(Outcome::Failed(FailureReason::CoworkerRemoved))
    );
    assert_eq!(failed.cancel_requested_at(), None);
    assert_eq!(
        store.mark_running(row.seq()).await.unwrap(),
        MarkedRunning::Settled(failed.clone())
    );
    assert_eq!(
        store.finish(row.seq(), completed("late")).await.unwrap(),
        Finished::Unchanged(failed.clone())
    );
    assert_eq!(
        receipt(failed),
        json!({
            "apiVersion": "1",
            "conversationId": id(1).to_string(),
            "requestId": id(10).to_string(),
            "acceptance": {"boundary": "sqlite_outbox"},
            "status": "failed",
            "reason": "coworker_removed",
        })
    );
    assert_eq!(store.next_work(id(1)).await.unwrap(), None);
}

#[tokio::test]
async fn mark_running_reports_who_started_the_request() {
    let (_dir, store) = opened().await;
    let row = insert(&store, 1, 10, "q").await;
    let running = start(&store, &row).await;
    assert_eq!(running.state(), &RequestState::Running);
    assert_eq!(
        store.mark_running(row.seq()).await.unwrap(),
        MarkedRunning::AlreadyStarted(running)
    );
    let pending = store
        .request_cancel(id(1), id(10), at(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.mark_running(row.seq()).await.unwrap(),
        MarkedRunning::AlreadyStarted(pending.clone())
    );
    let cancelled = finish(&store, &pending, Outcome::Cancelled).await;
    assert_eq!(
        store.mark_running(row.seq()).await.unwrap(),
        MarkedRunning::Settled(cancelled)
    );
}

#[tokio::test]
async fn cancellation_follows_the_request_state() {
    let (_dir, store) = opened().await;
    let queued = insert(&store, 1, 10, "a").await;
    let running = insert(&store, 1, 11, "b").await;
    let done = insert(&store, 1, 12, "c").await;
    start(&store, &running).await;
    settle(&store, &done, completed("done")).await;

    let cancelled = store
        .request_cancel(id(1), id(10), at(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cancelled.state(),
        &RequestState::Settled(Outcome::Cancelled)
    );
    assert_eq!(
        cancelled.cancel_requested_at(),
        Some("2027-01-15T08:00:01Z")
    );
    assert_eq!(
        store.mark_running(queued.seq()).await.unwrap(),
        MarkedRunning::Settled(cancelled.clone())
    );
    assert!(store.cancel_requested(queued.seq()).await.unwrap());

    assert!(!store.cancel_requested(running.seq()).await.unwrap());
    let plus_two = UtcOffset::from_hms(2, 0, 0).unwrap();
    let first = store
        .request_cancel(id(1), id(11), at(2).to_offset(plus_two))
        .await
        .unwrap()
        .unwrap();
    let second = store
        .request_cancel(id(1), id(11), at(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.state(), &RequestState::CancellationRequested);
    assert_eq!(first.cancel_requested_at(), Some("2027-01-15T08:00:02Z"));
    assert_eq!(second, first);
    assert!(store.cancel_requested(running.seq()).await.unwrap());

    let unchanged = store
        .request_cancel(id(1), id(12), at(4))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.state(), &RequestState::Settled(completed("done")));
    assert_eq!(unchanged.cancel_requested_at(), None);
    assert_eq!(
        store.request_cancel(id(1), id(13), at(5)).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn next_work_returns_unsettled_requests_before_accepted_ones() {
    let (_dir, store) = opened().await;
    open(&store, 2, "scout").await;
    let older = insert(&store, 1, 10, "a").await;
    let newer = insert(&store, 1, 11, "b").await;
    let elsewhere = insert(&store, 2, 12, "c").await;
    assert_eq!(store.next_work(id(1)).await.unwrap(), Some(older.clone()));

    start(&store, &newer).await;
    let head = store.next_work(id(1)).await.unwrap().unwrap();
    assert_eq!(
        (head.seq(), head.state()),
        (newer.seq(), &RequestState::Running)
    );
    store.request_cancel(id(1), id(11), at(0)).await.unwrap();
    let head = store.next_work(id(1)).await.unwrap().unwrap();
    assert_eq!(
        store.requeue(newer.seq()).await.unwrap(),
        Requeued::Kept(head.clone())
    );
    assert_eq!(
        (head.seq(), head.state()),
        (newer.seq(), &RequestState::CancellationRequested)
    );
    finish(&store, &newer, Outcome::Cancelled).await;
    assert_eq!(store.next_work(id(1)).await.unwrap(), Some(older.clone()));

    start(&store, &older).await;
    assert_eq!(
        store.requeue(older.seq()).await.unwrap(),
        Requeued::Requeued(older.clone())
    );
    assert_eq!(
        store.requeue(older.seq()).await.unwrap(),
        Requeued::AlreadyAccepted(older.clone())
    );
    assert_eq!(store.next_work(id(1)).await.unwrap(), Some(older.clone()));
    settle(&store, &older, completed("a")).await;
    assert_eq!(store.next_work(id(1)).await.unwrap(), None);
    assert_eq!(store.next_work(id(2)).await.unwrap(), Some(elsewhere));
}

#[tokio::test]
async fn conversations_open_once_and_report_their_phase() {
    let (_dir, store) = opened().await;
    assert!(matches!(
        store
            .open_conversation(id(1), &coworker("scout"))
            .await
            .unwrap(),
        Opened::Existing
    ));
    assert!(matches!(
        store
            .open_conversation(id(1), &coworker("quill"))
            .await
            .unwrap(),
        Opened::Conflict
    ));
    let state = |phase: &str| json!({"conversationId": id(1).to_string(), "coworkerId": "scout", "phase": phase});
    let current = async || serde_json::to_value(store.conversation(id(1)).await.unwrap()).unwrap();
    assert_eq!(current().await, state("idle"));
    let row = insert(&store, 1, 10, "q").await;
    assert_eq!(current().await, state("running"));
    settle(&store, &row, Outcome::Failed(FailureReason::Timeout)).await;
    assert_eq!(current().await, state("idle"));
    assert!(store.conversation(id(2)).await.unwrap().is_none());
}

#[tokio::test]
async fn conversations_list_in_id_order_with_a_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let store = ApiStore::open(dir.path()).await.unwrap();
    open(&store, 3, "scout").await;
    open(&store, 1, "quill").await;
    open(&store, 2, "scout").await;
    let page = async |owner: Option<&str>, after: Option<u128>, limit: usize| {
        let owner = owner.map(coworker);
        let rows = store
            .list_conversations(owner.as_ref(), after.map(id), limit)
            .await
            .unwrap();
        let listed: Vec<_> = rows
            .conversations
            .iter()
            .map(|state| state.conversation_id)
            .collect();
        (listed, rows.next)
    };
    assert_eq!(page(None, None, 2).await, (vec![id(1), id(2)], Some(id(2))));
    assert_eq!(page(None, Some(2), 2).await, (vec![id(3)], None));
    assert_eq!(page(None, None, 3).await, (vec![id(1), id(2), id(3)], None));
    assert_eq!(
        page(Some("scout"), None, 1).await,
        (vec![id(2)], Some(id(2)))
    );
    assert_eq!(page(Some("scout"), Some(2), 1).await, (vec![id(3)], None));
    assert_eq!(page(Some("scribe"), None, 50).await, (vec![], None));
}

#[tokio::test]
async fn conversations_with_work_follow_their_oldest_active_request() {
    let (_dir, store) = opened().await;
    open(&store, 2, "quill").await;
    open(&store, 3, "scout").await;
    let first = insert(&store, 2, 10, "a").await;
    insert(&store, 1, 11, "b").await;
    insert(&store, 2, 12, "c").await;
    assert_eq!(
        store.conversations_with_work().await.unwrap(),
        [(id(2), coworker("quill")), (id(1), coworker("scout"))]
    );
    settle(&store, &first, completed("a")).await;
    assert_eq!(
        store.conversations_with_work().await.unwrap(),
        [(id(1), coworker("scout")), (id(2), coworker("quill"))]
    );
}

#[tokio::test]
async fn snapshot_rows_are_the_newest_requests_oldest_first() {
    let (_dir, store) = opened().await;
    for request in 10..15 {
        insert(&store, 1, request, "q").await;
    }
    let newest = store.snapshot_rows(id(1), 3).await.unwrap();
    assert_eq!(ids(&newest.rows), [id(12), id(13), id(14)]);
    assert!(newest.truncated);
    for max in [5, 10] {
        let all = store.snapshot_rows(id(1), max).await.unwrap();
        assert_eq!(ids(&all.rows), (10..15).map(id).collect::<Vec<_>>());
        assert!(!all.truncated);
    }
    let empty = store.snapshot_rows(id(2), 3).await.unwrap();
    assert!(empty.rows.is_empty() && !empty.truncated);
}

#[tokio::test]
async fn receipts_carry_only_the_fields_their_status_allows() {
    let (_dir, store) = opened().await;
    let base = |request: u128, status: &str| {
        json!({
            "apiVersion": "1",
            "conversationId": id(1).to_string(),
            "requestId": id(request).to_string(),
            "acceptance": {"boundary": "sqlite_outbox"},
            "status": status,
        })
    };
    let with = |mut value: Value, key: &str, field: Value| {
        value[key] = field;
        value
    };
    let accepted = insert(&store, 1, 10, "q").await;
    assert_eq!(receipt(accepted), base(10, "accepted"));
    let failed = insert(&store, 1, 11, "q").await;
    let failed = settle(&store, &failed, Outcome::Failed(FailureReason::Timeout)).await;
    assert_eq!(
        receipt(failed),
        with(base(11, "failed"), "reason", json!("timeout"))
    );
    let uncertain = insert(&store, 1, 12, "q").await;
    let uncertain = settle(
        &store,
        &uncertain,
        Outcome::Uncertain(FailureReason::ConfigurationChanged),
    )
    .await;
    assert_eq!(
        receipt(uncertain),
        with(
            base(12, "uncertain"),
            "reason",
            json!("configuration_changed")
        )
    );
    let answered = insert(&store, 1, 13, "q").await;
    let answered = settle(&store, &answered, completed("hi")).await;
    assert_eq!(
        receipt(answered),
        with(base(13, "completed"), "result", json!({"text": "hi"}))
    );
    let cancelled = store
        .request_cancel(id(1), id(10), at(0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt(cancelled),
        with(
            base(10, "cancelled"),
            "cancellation",
            json!({"requestedAt": "2027-01-15T08:00:00Z"})
        )
    );
}

#[tokio::test]
async fn reopened_store_returns_every_row_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let store = ApiStore::open(dir.path()).await.unwrap();
    open(&store, 1, "scout").await;
    let accepted = insert(&store, 1, 10, "a").await;
    let running = insert(&store, 1, 11, "b").await;
    let pending = insert(&store, 1, 12, "c").await;
    let done = insert(&store, 1, 13, "d").await;
    start(&store, &running).await;
    start(&store, &pending).await;
    store.request_cancel(id(1), id(12), at(0)).await.unwrap();
    retire(&store, &pending, FailureReason::ProcessStopped).await;
    settle(&store, &done, completed("answer")).await;
    let before = store.snapshot_rows(id(1), MAX).await.unwrap().rows;
    drop(store);

    let store = ApiStore::open(dir.path()).await.unwrap();
    assert_eq!(store.snapshot_rows(id(1), MAX).await.unwrap().rows, before);
    assert_eq!(
        before
            .iter()
            .map(RequestRow::retirement)
            .collect::<Vec<_>>(),
        [None, None, Some(FailureReason::ProcessStopped), None]
    );
    assert_eq!(
        before
            .iter()
            .map(|row| row.state().clone())
            .collect::<Vec<_>>(),
        [
            RequestState::Accepted,
            RequestState::Running,
            RequestState::CancellationRequested,
            RequestState::Settled(completed("answer")),
        ]
    );
    assert_eq!(store.request(id(1), id(10)).await.unwrap(), Some(accepted));
    assert!(matches!(
        store.submit(id(1), id(14), "e", MAX).await.unwrap(),
        Submitted::Inserted(row) if row.seq() > before[3].seq()
    ));
}

#[tokio::test]
async fn faults_fail_or_delay_the_named_operation() {
    let (_dir, store) = opened().await;
    let faults = Arc::new(Faults::default());
    let store = store.with_faults(Arc::clone(&faults));
    let seq = insert(&store, 1, 10, "q").await.seq();
    let state = async || {
        store
            .request(id(1), id(10))
            .await
            .unwrap()
            .unwrap()
            .state()
            .clone()
    };

    assert!(store.ping().await);
    faults.fail(FaultPoint::Ping, 1);
    assert!(!store.ping().await);
    assert!(store.ping().await);

    faults.fail(FaultPoint::MarkRunning, 1);
    assert_eq!(store.mark_running(seq).await, Err(StoreFault));
    assert_eq!(state().await, RequestState::Accepted);
    // The commit lands, then the delay outlasts the 5 s bound: the caller sees a fault and its
    // retry finds the request already started.
    faults.delay(FaultPoint::MarkRunning, Duration::from_secs(6), 1);
    let started = tokio::spawn({
        let store = store.clone();
        async move { store.mark_running(seq).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while state().await != RequestState::Running {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!started.is_finished());
    assert_eq!(started.await.unwrap(), Err(StoreFault));
    let Ok(MarkedRunning::AlreadyStarted(running)) = store.mark_running(seq).await else {
        panic!("expected the retry to find the request started");
    };
    assert_eq!(running.state(), &RequestState::Running);

    faults.fail(FaultPoint::BeginRetirement, 1);
    let reason = FailureReason::ProcessStopped;
    assert_eq!(store.begin_retirement(seq, reason).await, Err(StoreFault));
    let unmarked = store.request(id(1), id(10)).await.unwrap().unwrap();
    assert_eq!(unmarked.retirement(), None);

    // Every settlement write shares one fault point.
    faults.fail(FaultPoint::Finish, 5);
    for _ in 0..4 {
        assert_eq!(store.finish(seq, completed("a")).await, Err(StoreFault));
    }
    assert_eq!(store.interrupt(seq).await, Err(StoreFault));
    assert_eq!(state().await, RequestState::Running);
    assert_eq!(
        finish(&store, &running, completed("a")).await.state(),
        &RequestState::Settled(completed("a"))
    );

    faults.delay(FaultPoint::Ping, Duration::from_secs(3), 1);
    assert!(!store.ping().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_replays_of_one_identity_insert_once() {
    const ATTEMPTS: usize = 16;
    let (_dir, store) = opened().await;
    let barrier = Arc::new(Barrier::new(ATTEMPTS));
    let mut attempts = JoinSet::new();
    for _ in 0..ATTEMPTS {
        let (store, barrier) = (store.clone(), Arc::clone(&barrier));
        attempts.spawn(async move {
            barrier.wait().await;
            store.submit(id(1), id(10), "same text", MAX).await.unwrap()
        });
    }
    let (mut inserted, mut replayed) = (Vec::new(), Vec::new());
    while let Some(submitted) = attempts.join_next().await {
        match submitted.unwrap() {
            Submitted::Inserted(row) => inserted.push(row),
            Submitted::Existing(row) => replayed.push(row),
            other => panic!("{other:?}"),
        }
    }
    let [row] = inserted.as_slice() else {
        panic!("expected exactly one insert, got {}", inserted.len());
    };
    assert_eq!(replayed.len(), ATTEMPTS - 1);
    let expected = receipt(row.clone());
    for replay in replayed {
        assert_eq!(&replay, row);
        assert_eq!(receipt(replay), expected);
    }
    assert_eq!(
        store.snapshot_rows(id(1), MAX).await.unwrap().rows,
        std::slice::from_ref(row)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_distinct_submits_never_exceed_the_queue_bound() {
    const ATTEMPTS: usize = 16;
    const BOUND: usize = 4;
    let (_dir, store) = opened().await;
    let barrier = Arc::new(Barrier::new(ATTEMPTS));
    let mut attempts = JoinSet::new();
    for request in (100..).take(ATTEMPTS) {
        let (store, barrier) = (store.clone(), Arc::clone(&barrier));
        attempts.spawn(async move {
            barrier.wait().await;
            store.submit(id(1), id(request), "q", BOUND).await.unwrap()
        });
    }
    let (mut inserted, mut full) = (0, 0);
    while let Some(submitted) = attempts.join_next().await {
        match submitted.unwrap() {
            Submitted::Inserted(_) => inserted += 1,
            Submitted::QueueFull => full += 1,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!((inserted, full), (BOUND, ATTEMPTS - BOUND));
    let rows = store.snapshot_rows(id(1), MAX).await.unwrap().rows;
    assert_eq!(rows.len(), BOUND);
    assert!(
        rows.iter()
            .all(|row| row.state() == &RequestState::Accepted)
    );
}
