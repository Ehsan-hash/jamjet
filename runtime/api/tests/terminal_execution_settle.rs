//! Spike findings G10 and G10b (2026-09-06): after `POST /executions/:id/cancel`
//! the engine kept handing out the run's pending work and accepted a late
//! completion into the cancelled run's state, with a `node_completed` event
//! appended after `workflow_cancelled`.
//!
//! The PR #140 review then found the refusal itself settled items it had no
//! right to touch (I1): it ran an unfenced settle before any fence check, so a
//! retried `/complete` flipped a completed item to failed and a forged fence
//! settled a live one. And memory's "settle" did not settle (I2), which the
//! memory-only tests could not see. Every test here runs on memory AND SQLite.
mod support;

use axum::http::StatusCode;
use jamjet_core::workflow::WorkflowStatus;
use jamjet_state::event::EventKind;
use serde_json::json;
use support::{backends, get, make_state, post, python_fn_start_ir, register_and_start};

#[tokio::test]
async fn claim_does_not_hand_out_work_of_a_cancelled_execution() {
    for tb in backends().await {
        let name = tb.name();
        let backend = tb.dyn_backend();
        let state = make_state(backend.clone());
        let exec_id =
            register_and_start(&state, python_fn_start_ir("wf-cancel-claim"), json!({})).await;

        let (st, _) = post(&state, &format!("/executions/{exec_id}/cancel"), json!({})).await;
        assert!(st.is_success(), "{name}: cancel must succeed");

        let (st, body) = post(
            &state,
            "/work-items/claim",
            json!({ "worker_id": "py-worker", "queue_types": ["python_tool"] }),
        )
        .await;
        assert!(st.is_success());
        assert_eq!(
            body["claimed"], false,
            "{name}: a cancelled execution's pending item must not be claimable, got {body}"
        );

        // The route settled the dead item rather than spinning on it: nothing
        // is left for the next poll to pick up again.
        let again = backend
            .claim_work_item("probe", &["python_tool"])
            .await
            .unwrap();
        assert!(
            again.is_none(),
            "{name}: the cancelled run's item is still claimable after the claim route \
             settled it (attempt {:?} of {:?})",
            again.as_ref().map(|i| i.attempt),
            again.as_ref().map(|i| i.max_attempts)
        );
    }
}

#[tokio::test]
async fn completion_after_cancel_is_rejected_and_leaves_no_trace() {
    for tb in backends().await {
        let name = tb.name();
        let backend = tb.dyn_backend();
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
            StatusCode::CONFLICT,
            "{name}: late completion must be refused, got {st} {body}"
        );
        assert!(
            tb.is_settled(item.id).await,
            "{name}: the holder's refused result still settles its item, got {}",
            tb.item_status(item.id).await
        );

        let events = backend
            .get_events(&item.execution_id)
            .await
            .expect("events");
        assert!(
            !events
                .iter()
                .any(|e| matches!(e.kind, EventKind::NodeCompleted { .. })),
            "{name}: no node_completed may be appended to a cancelled run"
        );
        let (_, exec) = get(&state, &format!("/executions/{exec_id}")).await;
        assert_eq!(exec["status"], "cancelled");
        assert!(
            exec["current_state"].get("gpu_result").is_none(),
            "{name}: the late result must not enter the cancelled run's state, got {}",
            exec["current_state"]
        );
    }
}

#[tokio::test]
async fn failure_report_after_cancel_is_rejected() {
    for tb in backends().await {
        let name = tb.name();
        let backend = tb.dyn_backend();
        let state = make_state(backend.clone());
        let exec_id =
            register_and_start(&state, python_fn_start_ir("wf-cancel-fail"), json!({})).await;
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
            StatusCode::CONFLICT,
            "{name}: late failure must be refused, got {st} {body}"
        );
        assert!(
            tb.is_settled(item.id).await,
            "{name}: the holder's refused failure still settles its item, got {}",
            tb.item_status(item.id).await
        );
        let events = backend
            .get_events(&item.execution_id)
            .await
            .expect("events");
        assert!(
            !events
                .iter()
                .any(|e| matches!(e.kind, EventKind::NodeFailed { .. })),
            "{name}: no node_failed may be appended to a cancelled run"
        );
    }
}

/// I1: a worker completes the last node, the run completes, and the client
/// retries the same `/complete` (a timeout, a network blip). The retry must
/// not touch the item: it was completed, and it stays completed.
#[tokio::test]
async fn a_retried_completion_after_the_run_completed_leaves_the_item_completed() {
    for tb in backends().await {
        let name = tb.name();
        let backend = tb.dyn_backend();
        let state = make_state(backend.clone());
        register_and_start(&state, python_fn_start_ir("wf-retry-complete"), json!({})).await;
        let item = backend
            .claim_work_item("py-worker", &["python_tool"])
            .await
            .unwrap()
            .expect("start node item");
        let body = json!({
            "output": { "ok": true },
            "state_patch": {},
            "duration_ms": 1,
            "lease_fence": item.lease_fence
        });
        let (st, b) = post(
            &state,
            &format!("/work-items/{}/complete", item.id),
            body.clone(),
        )
        .await;
        assert!(st.is_success(), "{name}: first completion: {st} {b}");
        backend
            .update_execution_status(&item.execution_id, WorkflowStatus::Completed)
            .await
            .unwrap();
        let settled_as = tb.item_status(item.id).await;

        let (st, b) = post(&state, &format!("/work-items/{}/complete", item.id), body).await;

        assert_eq!(
            tb.item_status(item.id).await,
            settled_as,
            "{name}: the retried completion answered {st} {b} and changed the settled item"
        );
        let completions = backend
            .get_events(&item.execution_id)
            .await
            .unwrap()
            .iter()
            .filter(|e| matches!(e.kind, EventKind::NodeCompleted { .. }))
            .count();
        assert_eq!(completions, 1, "{name}: the retry must not record again");
    }
}

/// I1: once a run is terminal, a caller presenting a fence it does not hold
/// must not be able to settle somebody else's claimed item, through either
/// route.
#[tokio::test]
async fn a_forged_fence_does_not_settle_a_claimed_item_of_a_terminal_run() {
    for route in ["complete", "fail"] {
        for tb in backends().await {
            let name = tb.name();
            let backend = tb.dyn_backend();
            let state = make_state(backend.clone());
            let exec_id =
                register_and_start(&state, python_fn_start_ir("wf-forged"), json!({})).await;
            let item = backend
                .claim_work_item("py-worker", &["python_tool"])
                .await
                .unwrap()
                .expect("start node item");
            let (st, _) = post(&state, &format!("/executions/{exec_id}/cancel"), json!({})).await;
            assert!(st.is_success());

            let forged = item.lease_fence + 999_999;
            let (st, b) = post(
                &state,
                &format!("/work-items/{}/{route}", item.id),
                json!({ "error": "x", "output": {}, "state_patch": {}, "duration_ms": 1, "lease_fence": forged }),
            )
            .await;

            assert_eq!(st, StatusCode::CONFLICT, "{name} /{route}: {st} {b}");
            assert_eq!(
                tb.item_status(item.id).await,
                "claimed",
                "{name} /{route}: a forged fence settled the holder's item ({st} {b})"
            );
        }
    }
}
