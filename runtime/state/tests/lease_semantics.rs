//! Lease semantics found wanting by the PAS orchestrator spike (2026-09-06).
//!
//! - G9: the lease TTL was a 30 s constant. A worker now says how long a
//!   lease it needs when it claims, and renewals honour it.
//! - G6: the claim path's fast reclaim beat the scheduler sweep on every poll
//!   and wrote nothing, so a retry left no trace in the history.
//! - G8: an engine restart longer than the TTL reclaimed a live worker's
//!   lease and forced a full re-run. A heartbeat that arrives while the item
//!   is only pending because of expiry re-attaches the lease instead.
use chrono::Utc;
use jamjet_core::workflow::{ExecutionId, WorkflowExecution, WorkflowStatus};
use jamjet_state::{
    backend::{StateBackend, StateBackendError, WorkItem},
    EventKind, InMemoryBackend, SqliteBackend, DEFAULT_TENANT,
};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

fn temp_db_path() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("jamjet_lease_{}.db", Uuid::new_v4().simple()));
    path
}

async fn open_sqlite() -> (Arc<dyn StateBackend>, SqliteBackend) {
    let path = temp_db_path();
    let url = format!("sqlite://{}", path.display());
    let raw = SqliteBackend::open(&url).await.expect("open test db");
    let shared = SqliteBackend::open(&url).await.expect("open test db again");
    (Arc::new(shared), raw)
}

fn sample_execution(id: &ExecutionId) -> WorkflowExecution {
    let now = Utc::now();
    WorkflowExecution {
        execution_id: id.clone(),
        workflow_id: "wf-lease".into(),
        workflow_version: "1.0.0".into(),
        status: WorkflowStatus::Running,
        initial_input: json!({}),
        current_state: json!({}),
        started_at: now,
        updated_at: now,
        completed_at: None,
        session_type: None,
        parent_execution_id: None,
        segment_number: 0,
    }
}

fn sample_item(execution_id: &ExecutionId, queue: &str) -> WorkItem {
    WorkItem {
        id: Uuid::new_v4(),
        execution_id: execution_id.clone(),
        node_id: "gpu_step".into(),
        queue_type: queue.into(),
        payload: json!({}),
        attempt: 0,
        max_attempts: 3,
        created_at: Utc::now(),
        lease_expires_at: None,
        worker_id: None,
        lease_fence: 0,
        tenant_id: DEFAULT_TENANT.into(),
    }
}

async fn seed(backend: &dyn StateBackend, queue: &str) -> (ExecutionId, Uuid) {
    let execution_id = ExecutionId::new();
    backend
        .create_execution(sample_execution(&execution_id))
        .await
        .unwrap();
    let item = sample_item(&execution_id, queue);
    let id = item.id;
    backend.enqueue_work_item(item).await.unwrap();
    (execution_id, id)
}

/// Force the item's lease into the past on whichever backend is under test.
enum Expirer {
    Sqlite(SqliteBackend),
    Memory(Arc<InMemoryBackend>),
}

impl Expirer {
    async fn expire(&self, id: Uuid) {
        match self {
            Expirer::Sqlite(b) => b.force_lease_expired_for_test(id).await.unwrap(),
            Expirer::Memory(b) => b.force_lease_expired_for_test(id),
        }
    }
}

async fn backends() -> Vec<(&'static str, Arc<dyn StateBackend>, Expirer)> {
    let (sqlite, raw) = open_sqlite().await;
    let memory = Arc::new(InMemoryBackend::new());
    let memory_dyn: Arc<dyn StateBackend> = memory.clone();
    vec![
        ("sqlite", sqlite, Expirer::Sqlite(raw)),
        ("memory", memory_dyn, Expirer::Memory(memory)),
    ]
}

#[tokio::test]
async fn a_claim_names_its_lease_length_and_renewals_keep_it() {
    for (name, backend, _) in backends().await {
        let (_exec, id) = seed(backend.as_ref(), "gpu").await;
        let claimed = backend
            .claim_work_item_with_lease("gpu-worker", &["gpu"], Some(120))
            .await
            .unwrap()
            .expect("claimable");
        let expiry = claimed.lease_expires_at.expect("lease set");
        let secs = (expiry - Utc::now()).num_seconds();
        assert!(
            (110..=121).contains(&secs),
            "{name}: claim lease should be ~120 s, got {secs}"
        );

        backend
            .renew_lease(id, "gpu-worker", claimed.lease_fence)
            .await
            .unwrap();
        let after = backend.get_work_item(id).await.unwrap().expect("item");
        let secs = (after.lease_expires_at.unwrap() - Utc::now()).num_seconds();
        assert!(
            (110..=121).contains(&secs),
            "{name}: renewal should keep the 120 s lease, got {secs}"
        );
    }
}

#[tokio::test]
async fn the_fast_path_reclaim_records_the_retry_in_the_history() {
    for (name, backend, expirer) in backends().await {
        let (exec, id) = seed(backend.as_ref(), "gpu").await;
        let first = backend
            .claim_work_item("w1", &["gpu"])
            .await
            .unwrap()
            .expect("claimable");
        expirer.expire(id).await;

        let second = backend
            .claim_work_item("w2", &["gpu"])
            .await
            .unwrap()
            .expect("{name}: an expired lease must be reclaimable by the next poll");
        assert_eq!(second.id, first.id);
        assert_eq!(
            second.attempt, 1,
            "{name}: the reclaim counts as an attempt"
        );

        let events = backend.get_events(&exec).await.unwrap();
        let failed = events.iter().any(|e| {
            matches!(&e.kind,
            EventKind::NodeFailed { node_id, retryable: true, .. } if node_id == "gpu_step")
        });
        let retry = events.iter().any(|e| {
            matches!(&e.kind,
            EventKind::RetryScheduled { node_id, attempt: 1, .. } if node_id == "gpu_step")
        });
        assert!(
            failed,
            "{name}: node_failed must be recorded for the lost lease, events: {events:?}"
        );
        assert!(
            retry,
            "{name}: retry_scheduled must be recorded, events: {events:?}"
        );
    }
}

#[tokio::test]
async fn a_heartbeat_reattaches_a_lease_lost_to_expiry_when_nobody_else_claimed() {
    for (name, backend, expirer) in backends().await {
        let (_exec, id) = seed(backend.as_ref(), "gpu").await;
        let first = backend
            .claim_work_item("w1", &["gpu"])
            .await
            .unwrap()
            .expect("claimable");
        expirer.expire(id).await;
        // Another worker polls a different queue: the fast path re-queues our
        // item, but nobody takes it.
        assert!(backend
            .claim_work_item("other", &["cpu"])
            .await
            .unwrap()
            .is_none());

        let renewal = backend
            .heartbeat_lease(id, "w1", first.lease_fence)
            .await
            .unwrap_or_else(|e| {
                panic!("{name}: the original worker must get its lease back, got {e}")
            });
        assert!(
            renewal.reattached,
            "{name}: renewal must report the re-attach"
        );
        assert!(
            renewal.lease_fence > first.lease_fence,
            "{name}: a re-attach mints a newer fence"
        );

        let item = backend.get_work_item(id).await.unwrap().expect("item");
        assert_eq!(item.worker_id.as_deref(), Some("w1"), "{name}");
        assert_eq!(
            item.attempt, 0,
            "{name}: a re-attached lease is the same attempt, not a retry"
        );

        let settled = backend
            .complete_work_item_fenced(id, renewal.lease_fence)
            .await
            .unwrap();
        assert!(
            settled,
            "{name}: completion with the re-attached fence must succeed"
        );
    }
}

#[tokio::test]
async fn a_heartbeat_does_not_reattach_once_another_worker_holds_the_item() {
    for (name, backend, expirer) in backends().await {
        let (_exec, id) = seed(backend.as_ref(), "gpu").await;
        let first = backend
            .claim_work_item("w1", &["gpu"])
            .await
            .unwrap()
            .expect("claimable");
        expirer.expire(id).await;
        let taken = backend
            .claim_work_item("w2", &["gpu"])
            .await
            .unwrap()
            .expect("reclaimable");
        assert_eq!(taken.worker_id.as_deref(), Some("w2"));

        let err = backend
            .heartbeat_lease(id, "w1", first.lease_fence)
            .await
            .expect_err(&format!(
                "{name}: a stale worker must not steal the item back"
            ));
        assert!(
            matches!(err, StateBackendError::FenceLost(_)),
            "{name}: got {err}"
        );
    }
}
