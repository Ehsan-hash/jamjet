//! Spike finding G10 (2026-09-06): `POST /executions/:id/cancel` flipped the
//! run's status and nothing told the worker holding the running step. The
//! heartbeat is the one channel every external worker already polls, so it
//! carries the signal.
mod support;

use jamjet_state::backend::StateBackend;
use jamjet_state::InMemoryBackend;
use serde_json::json;
use std::sync::Arc;
use support::{make_state, post, python_fn_start_ir, register_and_start};

#[tokio::test]
async fn heartbeat_reports_cancel_requested_once_the_execution_is_cancelled() {
    let backend: Arc<dyn StateBackend> = Arc::new(InMemoryBackend::new());
    let state = make_state(backend.clone());
    let exec_id = register_and_start(&state, python_fn_start_ir("wf-hb-cancel"), json!({})).await;
    let item = backend
        .claim_work_item("py-worker", &["python_tool"])
        .await
        .expect("claim")
        .expect("start node item");

    let (st, body) = post(
        &state,
        &format!("/work-items/{}/heartbeat", item.id),
        json!({ "worker_id": "py-worker", "lease_fence": item.lease_fence }),
    )
    .await;
    assert!(st.is_success(), "heartbeat before cancel: {st} {body}");
    assert_eq!(
        body["cancel_requested"], false,
        "a running execution must not signal cancel, got {body}"
    );

    let (st, _) = post(&state, &format!("/executions/{exec_id}/cancel"), json!({})).await;
    assert!(st.is_success());

    let (st, body) = post(
        &state,
        &format!("/work-items/{}/heartbeat", item.id),
        json!({ "worker_id": "py-worker", "lease_fence": item.lease_fence }),
    )
    .await;
    assert!(
        st.is_success(),
        "heartbeat after cancel must still answer, got {st} {body}"
    );
    assert_eq!(
        body["cancel_requested"], true,
        "the worker must learn about the cancel, got {body}"
    );
}
