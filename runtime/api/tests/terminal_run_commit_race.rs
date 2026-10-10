//! PR #140 review, finding I3: `/complete` checked the run's status and then
//! committed in a separate step, so a cancel landing between the two still
//! appended `node_completed` and a state patch after `workflow_cancelled`.
//! The refusal now lives inside the fenced commit's transaction.
mod support;

use axum::http::StatusCode;
use jamjet_core::workflow::{ExecutionId, WorkflowExecution, WorkflowStatus};
use jamjet_state::backend::{StateBackend, WorkItem, WorkflowDefinition};
use jamjet_state::event::EventKind;
use jamjet_state::Event;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use support::{backends, make_state, post, python_fn_start_ir, register_and_start};

/// A real backend that, once armed, cancels the execution the moment anyone
/// reads it, and hands the reader the status from BEFORE the cancel. That is
/// a cancel landing right after the route's read: the read is stale by the
/// time anything acts on it.
struct CancelAfterRead {
    inner: Arc<dyn StateBackend>,
    armed: AtomicBool,
}

#[async_trait::async_trait]
impl StateBackend for CancelAfterRead {
    async fn get_execution(
        &self,
        id: &ExecutionId,
    ) -> jamjet_state::backend::BackendResult<Option<WorkflowExecution>> {
        let read = self.inner.get_execution(id).await?;
        if self.armed.swap(false, Ordering::SeqCst) {
            self.inner
                .update_execution_status(id, WorkflowStatus::Cancelled)
                .await?;
        }
        Ok(read)
    }

    async fn get_events(
        &self,
        execution_id: &ExecutionId,
    ) -> jamjet_state::backend::BackendResult<Vec<Event>> {
        self.inner.get_events(execution_id).await
    }

    async fn store_workflow(
        &self,
        def: WorkflowDefinition,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner.store_workflow(def).await
    }

    async fn get_workflow(
        &self,
        workflow_id: &str,
        version: &str,
    ) -> jamjet_state::backend::BackendResult<Option<WorkflowDefinition>> {
        self.inner.get_workflow(workflow_id, version).await
    }

    async fn create_execution(
        &self,
        execution: WorkflowExecution,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner.create_execution(execution).await
    }

    async fn update_execution_status(
        &self,
        id: &ExecutionId,
        status: WorkflowStatus,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner.update_execution_status(id, status).await
    }

    async fn update_execution_current_state(
        &self,
        id: &ExecutionId,
        current_state: &Value,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner
            .update_execution_current_state(id, current_state)
            .await
    }

    async fn patch_append_array(
        &self,
        execution_id: &ExecutionId,
        key: &str,
        value: Value,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner
            .patch_append_array(execution_id, key, value)
            .await
    }

    async fn list_executions(
        &self,
        status: Option<WorkflowStatus>,
        limit: u32,
        offset: u32,
    ) -> jamjet_state::backend::BackendResult<Vec<WorkflowExecution>> {
        self.inner.list_executions(status, limit, offset).await
    }

    async fn append_event(
        &self,
        event: Event,
    ) -> jamjet_state::backend::BackendResult<jamjet_state::EventSequence> {
        self.inner.append_event(event).await
    }

    async fn get_events_since(
        &self,
        execution_id: &ExecutionId,
        since_sequence: jamjet_state::EventSequence,
    ) -> jamjet_state::backend::BackendResult<Vec<Event>> {
        self.inner
            .get_events_since(execution_id, since_sequence)
            .await
    }

    async fn latest_sequence(
        &self,
        execution_id: &ExecutionId,
    ) -> jamjet_state::backend::BackendResult<jamjet_state::EventSequence> {
        self.inner.latest_sequence(execution_id).await
    }

    async fn write_snapshot(
        &self,
        snapshot: jamjet_state::Snapshot,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner.write_snapshot(snapshot).await
    }

    async fn latest_snapshot(
        &self,
        execution_id: &ExecutionId,
    ) -> jamjet_state::backend::BackendResult<Option<jamjet_state::Snapshot>> {
        self.inner.latest_snapshot(execution_id).await
    }

    async fn create_segment_atomic(
        &self,
        execution: WorkflowExecution,
        seed_snapshot: jamjet_state::Snapshot,
        started_event: EventKind,
        scheduled_event: EventKind,
        work_item: WorkItem,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner
            .create_segment_atomic(
                execution,
                seed_snapshot,
                started_event,
                scheduled_event,
                work_item,
            )
            .await
    }

    async fn release_tool_reservation(
        &self,
        key: &str,
        owner: &str,
        lease_fence: i64,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner
            .release_tool_reservation(key, owner, lease_fence)
            .await
    }

    async fn reserve_tool_effect(
        &self,

        key: &str,

        execution_id: &ExecutionId,

        node_id: &str,

        owner: &str,

        lease_fence: i64,

        ttl: std::time::Duration,
    ) -> jamjet_state::backend::BackendResult<jamjet_state::backend::ReserveOutcome> {
        self.inner
            .reserve_tool_effect(key, execution_id, node_id, owner, lease_fence, ttl)
            .await
    }

    async fn get_tool_effect(
        &self,
        key: &str,
    ) -> jamjet_state::backend::BackendResult<Option<Value>> {
        self.inner.get_tool_effect(key).await
    }

    async fn put_artifact(
        &self,
        bytes: &[u8],
        media_type: Option<&str>,
    ) -> jamjet_state::backend::BackendResult<jamjet_state::ArtifactRef> {
        self.inner.put_artifact(bytes, media_type).await
    }

    async fn get_artifact(
        &self,
        hash: &str,
    ) -> jamjet_state::backend::BackendResult<Option<Vec<u8>>> {
        self.inner.get_artifact(hash).await
    }

    async fn enqueue_work_item(
        &self,
        item: WorkItem,
    ) -> jamjet_state::backend::BackendResult<jamjet_state::backend::WorkItemId> {
        self.inner.enqueue_work_item(item).await
    }

    async fn claim_work_item(
        &self,
        worker_id: &str,
        queue_types: &[&str],
    ) -> jamjet_state::backend::BackendResult<Option<WorkItem>> {
        self.inner.claim_work_item(worker_id, queue_types).await
    }

    async fn claim_work_item_with_lease(
        &self,
        worker_id: &str,
        queue_types: &[&str],
        lease_secs: Option<u64>,
    ) -> jamjet_state::backend::BackendResult<Option<jamjet_state::backend::WorkItem>> {
        self.inner
            .claim_work_item_with_lease(worker_id, queue_types, lease_secs)
            .await
    }

    async fn heartbeat_lease(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
        worker_id: &str,
        lease_fence: i64,
    ) -> jamjet_state::backend::BackendResult<jamjet_state::LeaseRenewal> {
        self.inner
            .heartbeat_lease(item_id, worker_id, lease_fence)
            .await
    }

    async fn commit_turn_unless_terminal(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
        lease_fence: i64,
        terminal_event: Event,
        write_snapshot: bool,
    ) -> jamjet_state::backend::BackendResult<jamjet_state::TurnCommit> {
        self.inner
            .commit_turn_unless_terminal(item_id, lease_fence, terminal_event, write_snapshot)
            .await
    }

    async fn withdraw_work_item_fenced(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
        lease_fence: i64,
    ) -> jamjet_state::backend::BackendResult<bool> {
        self.inner
            .withdraw_work_item_fenced(item_id, lease_fence)
            .await
    }

    async fn renew_lease(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
        worker_id: &str,
        lease_fence: i64,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner
            .renew_lease(item_id, worker_id, lease_fence)
            .await
    }

    async fn get_work_item(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
    ) -> jamjet_state::backend::BackendResult<Option<WorkItem>> {
        self.inner.get_work_item(item_id).await
    }

    async fn complete_work_item(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner.complete_work_item(item_id).await
    }

    async fn complete_work_item_fenced(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
        lease_fence: i64,
    ) -> jamjet_state::backend::BackendResult<bool> {
        self.inner
            .complete_work_item_fenced(item_id, lease_fence)
            .await
    }

    async fn fail_work_item(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
        error: &str,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner.fail_work_item(item_id, error).await
    }

    async fn fail_work_item_fenced(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
        lease_fence: i64,
        error: &str,
    ) -> jamjet_state::backend::BackendResult<Option<jamjet_state::backend::FailOutcome>> {
        self.inner
            .fail_work_item_fenced(item_id, lease_fence, error)
            .await
    }

    async fn commit_turn(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
        lease_fence: i64,
        terminal_event: Event,
        write_snapshot: bool,
    ) -> jamjet_state::backend::BackendResult<jamjet_state::EventSequence> {
        self.inner
            .commit_turn(item_id, lease_fence, terminal_event, write_snapshot)
            .await
    }

    async fn park_work_item(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
        lease_fence: i64,
        retry_after: &str,
        next_attempt: u32,
    ) -> jamjet_state::backend::BackendResult<bool> {
        self.inner
            .park_work_item(item_id, lease_fence, retry_after, next_attempt)
            .await
    }

    async fn finalize_rollover_fenced(
        &self,
        execution_id: &ExecutionId,
        work_item_id: jamjet_state::backend::WorkItemId,
        lease_fence: i64,
    ) -> jamjet_state::backend::BackendResult<bool> {
        self.inner
            .finalize_rollover_fenced(execution_id, work_item_id, lease_fence)
            .await
    }

    async fn reclaim_expired_leases(
        &self,
    ) -> jamjet_state::backend::BackendResult<jamjet_state::backend::ReclaimResult> {
        self.inner.reclaim_expired_leases().await
    }

    async fn move_to_dead_letter(
        &self,
        item_id: jamjet_state::backend::WorkItemId,
        last_error: &str,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner.move_to_dead_letter(item_id, last_error).await
    }

    async fn create_token(
        &self,
        name: &str,
        role: &str,
    ) -> jamjet_state::backend::BackendResult<(String, jamjet_state::backend::ApiToken)> {
        self.inner.create_token(name, role).await
    }

    async fn validate_token(
        &self,
        token: &str,
    ) -> jamjet_state::backend::BackendResult<Option<jamjet_state::backend::ApiToken>> {
        self.inner.validate_token(token).await
    }

    async fn apply_approval_projection(
        &self,
        row: jamjet_state::ApprovalProjectionRow,
        projection_name: &str,
        new_checkpoint: i64,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner
            .apply_approval_projection(row, projection_name, new_checkpoint)
            .await
    }

    async fn apply_approval_projection_batch(
        &self,
        rows: Vec<jamjet_state::ApprovalProjectionRow>,
        projection_name: &str,
        execution_id: &ExecutionId,
        new_checkpoint: i64,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner
            .apply_approval_projection_batch(rows, projection_name, execution_id, new_checkpoint)
            .await
    }

    async fn get_approval_projection(
        &self,
        execution_id: &ExecutionId,
    ) -> jamjet_state::backend::BackendResult<Vec<jamjet_state::ApprovalProjectionRow>> {
        self.inner.get_approval_projection(execution_id).await
    }

    async fn get_projector_checkpoint(
        &self,
        projection_name: &str,
        execution_id: &ExecutionId,
    ) -> jamjet_state::backend::BackendResult<i64> {
        self.inner
            .get_projector_checkpoint(projection_name, execution_id)
            .await
    }

    async fn set_projector_checkpoint(
        &self,
        projection_name: &str,
        execution_id: &ExecutionId,
        new_checkpoint: i64,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner
            .set_projector_checkpoint(projection_name, execution_id, new_checkpoint)
            .await
    }

    async fn create_tenant(
        &self,
        tenant: jamjet_state::Tenant,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner.create_tenant(tenant).await
    }

    async fn get_tenant(
        &self,
        id: &jamjet_state::TenantId,
    ) -> jamjet_state::backend::BackendResult<Option<jamjet_state::Tenant>> {
        self.inner.get_tenant(id).await
    }

    async fn list_tenants(
        &self,
    ) -> jamjet_state::backend::BackendResult<Vec<jamjet_state::Tenant>> {
        self.inner.list_tenants().await
    }

    async fn update_tenant(
        &self,
        tenant: jamjet_state::Tenant,
    ) -> jamjet_state::backend::BackendResult<()> {
        self.inner.update_tenant(tenant).await
    }
}

#[tokio::test]
async fn a_cancel_landing_between_the_read_and_the_commit_records_nothing() {
    for tb in backends().await {
        let name = tb.name();
        let racy = Arc::new(CancelAfterRead {
            inner: tb.dyn_backend(),
            armed: AtomicBool::new(false),
        });
        let backend: Arc<dyn StateBackend> = racy.clone();
        let state = make_state(backend.clone());
        register_and_start(&state, python_fn_start_ir("wf-cancel-race"), json!({})).await;
        let item = backend
            .claim_work_item("py-worker", &["python_tool"])
            .await
            .unwrap()
            .expect("start node item");

        racy.armed.store(true, Ordering::SeqCst);
        let (st, body) = post(
            &state,
            &format!("/work-items/{}/complete", item.id),
            json!({
                "output": { "gpu_result": "late" },
                "state_patch": { "gpu_result": "late" },
                "duration_ms": 1,
                "lease_fence": item.lease_fence
            }),
        )
        .await;

        let exec = backend
            .get_execution(&item.execution_id)
            .await
            .unwrap()
            .expect("execution");
        assert_eq!(
            exec.status,
            WorkflowStatus::Cancelled,
            "{name}: the cancel landed"
        );
        let events = backend.get_events(&item.execution_id).await.unwrap();
        assert!(
            !events
                .iter()
                .any(|e| matches!(e.kind, EventKind::NodeCompleted { .. })),
            "{name}: a completion was recorded into a run cancelled before the commit \
             ({st} {body})"
        );
        assert_eq!(st, StatusCode::CONFLICT, "{name}: {body}");
    }
}
