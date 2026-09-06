//! Spike findings G10 and G10b (2026-09-06): after `POST /executions/:id/cancel`
//! the engine kept handing out the run's pending work and accepted a late
//! completion into the cancelled run's state, with a `node_completed` event
//! appended after `workflow_cancelled`.
mod support;

use jamjet_state::backend::StateBackend;
use jamjet_state::event::EventKind;
use jamjet_state::InMemoryBackend;
use serde_json::json;
use std::sync::Arc;
use support::{get, make_state, post, python_fn_start_ir, register_and_start};

#[tokio::test]
async fn claim_does_not_hand_out_work_of_a_cancelled_execution() {
    let backend: Arc<dyn StateBackend> = Arc::new(InMemoryBackend::new());
    let state = make_state(backend.clone());
    let exec_id =
        register_and_start(&state, python_fn_start_ir("wf-cancel-claim"), json!({})).await;

    let (st, _) = post(&state, &format!("/executions/{exec_id}/cancel"), json!({})).await;
    assert!(st.is_success(), "cancel must succeed");

    let (st, body) = post(
        &state,
        "/work-items/claim",
        json!({ "worker_id": "py-worker", "queue_types": ["python_tool"] }),
    )
    .await;
    assert!(st.is_success());
    assert_eq!(
        body["claimed"], false,
        "a cancelled execution's pending item must not be claimable, got {body}"
    );
}

#[tokio::test]
async fn completion_after_cancel_is_rejected_and_leaves_no_trace() {
    let backend: Arc<dyn StateBackend> = Arc::new(InMemoryBackend::new());
    let state = make_state(backend.clone());
    let exec_id =
        register_and_start(&state, python_fn_start_ir("wf-cancel-complete"), json!({})).await;

    // The worker claims the step, then the owner cancels while it runs.
    let item = backend
        .claim_work_item("py-worker", &["python_tool"])
        .await
        .expect("claim")
        .expect("start node item");
    let (st, _) = post(&state, &format!("/executions/{exec_id}/cancel"), json!({})).await;
    assert!(st.is_success());

    // The worker finishes late and reports its result.
    let (st, body) = post(
        &state,
        &format!("/work-items/{}/complete", item.id),
        json!({
            "execution_id": exec_id,
            "node_id": item.node_id,
            "output": { "gpu_result": "done-1" },
            "state_patch": { "gpu_result": "done-1" },
            "duration_ms": 40000,
            "lease_fence": item.lease_fence
        }),
    )
    .await;
    assert_eq!(
        st,
        axum::http::StatusCode::CONFLICT,
        "late completion must be refused, got {st} {body}"
    );

    let events = backend
        .get_events(&item.execution_id)
        .await
        .expect("events");
    assert!(
        !events
            .iter()
            .any(|e| matches!(e.kind, EventKind::NodeCompleted { .. })),
        "no node_completed may be appended to a cancelled run"
    );
    let (_, exec) = get(&state, &format!("/executions/{exec_id}")).await;
    assert_eq!(exec["status"], "cancelled");
    assert!(
        exec["current_state"].get("gpu_result").is_none(),
        "the late result must not enter the cancelled run's state, got {}",
        exec["current_state"]
    );
}

#[tokio::test]
async fn failure_report_after_cancel_is_rejected() {
    let backend: Arc<dyn StateBackend> = Arc::new(InMemoryBackend::new());
    let state = make_state(backend.clone());
    let exec_id = register_and_start(&state, python_fn_start_ir("wf-cancel-fail"), json!({})).await;
    let item = backend
        .claim_work_item("py-worker", &["python_tool"])
        .await
        .expect("claim")
        .expect("start node item");
    let (st, _) = post(&state, &format!("/executions/{exec_id}/cancel"), json!({})).await;
    assert!(st.is_success());

    let (st, body) = post(
        &state,
        &format!("/work-items/{}/fail", item.id),
        json!({ "execution_id": exec_id, "node_id": item.node_id, "error": "boom", "lease_fence": item.lease_fence }),
    )
    .await;
    assert_eq!(
        st,
        axum::http::StatusCode::CONFLICT,
        "late failure must be refused, got {st} {body}"
    );
    let events = backend
        .get_events(&item.execution_id)
        .await
        .expect("events");
    assert!(
        !events
            .iter()
            .any(|e| matches!(e.kind, EventKind::NodeFailed { .. })),
        "no node_failed may be appended to a cancelled run"
    );
}
