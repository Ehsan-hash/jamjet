//! Spike findings G8 and G9 (2026-09-06) at the HTTP surface: a worker names
//! the lease it needs when it claims, and a heartbeat that finds its lease
//! expired but unclaimed gets it back with a new fence instead of a 409.
mod support;

use jamjet_state::backend::StateBackend;
use jamjet_state::InMemoryBackend;
use serde_json::json;
use std::sync::Arc;
use support::{backends, make_state, post, python_fn_start_ir, register_and_start};

#[tokio::test]
async fn a_claim_can_ask_for_a_longer_lease_and_the_answer_echoes_it() {
    let backend: Arc<dyn StateBackend> = Arc::new(InMemoryBackend::new());
    let state = make_state(backend.clone());
    register_and_start(&state, python_fn_start_ir("wf-lease"), json!({})).await;

    let (st, body) = post(
        &state,
        "/work-items/claim",
        json!({ "worker_id": "gpu-worker", "queue_types": ["python_tool"], "lease_secs": 600 }),
    )
    .await;
    assert!(st.is_success(), "{st} {body}");
    assert_eq!(body["claimed"], true, "{body}");
    assert_eq!(
        body["work_item"]["lease_secs"], 600,
        "the effective lease must be echoed, got {body}"
    );

    let item = backend
        .get_work_item(body["work_item"]["id"].as_str().unwrap().parse().unwrap())
        .await
        .unwrap()
        .expect("item");
    let secs = (item.lease_expires_at.unwrap() - chrono::Utc::now()).num_seconds();
    assert!(
        (590..=601).contains(&secs),
        "lease must be ~600 s, got {secs}"
    );
}

#[tokio::test]
async fn a_heartbeat_after_an_unclaimed_expiry_reattaches_and_returns_the_new_fence() {
    for tb in backends().await {
        let name = tb.name();
        let backend = tb.dyn_backend();
        let state = make_state(backend.clone());
        register_and_start(&state, python_fn_start_ir("wf-reattach"), json!({})).await;
        let item = backend
            .claim_work_item("gpu-worker", &["python_tool"])
            .await
            .unwrap()
            .expect("item");

        // The engine was down longer than the lease: the item expired and a
        // poll on some other queue re-queued it, but nobody claimed it.
        tb.expire_lease(item.id).await;
        assert!(backend
            .claim_work_item("other", &["cpu"])
            .await
            .unwrap()
            .is_none());

        let (st, body) = post(
            &state,
            &format!("/work-items/{}/heartbeat", item.id),
            json!({ "worker_id": "gpu-worker", "lease_fence": item.lease_fence }),
        )
        .await;
        assert!(
            st.is_success(),
            "{name}: the original worker must get its lease back, got {st} {body}"
        );
        assert_eq!(body["reattached"], true, "{name}: {body}");
        let new_fence = body["lease_fence"]
            .as_i64()
            .expect("new fence in the answer");
        assert!(new_fence > item.lease_fence, "{name}: {body}");

        let (st, body) = post(
            &state,
            &format!("/work-items/{}/complete", item.id),
            json!({ "execution_id": null, "node_id": null, "output": {"ok": true}, "state_patch": {"ok": true}, "duration_ms": 1, "lease_fence": new_fence }),
        )
        .await;
        assert!(
            st.is_success(),
            "{name}: completion with the re-attached fence must succeed, got {st} {body}"
        );
    }
}

/// M1/M3 (PR #140 review): the answer carries the lease the claim was
/// GRANTED, held to the engine's bounds, and the item really has that lease.
/// The worker paces its heartbeats from this number (I5), so echoing the
/// request instead would have a worker asking for 20000 s heartbeat every
/// 6667 s against a 3600 s lease.
#[tokio::test]
async fn the_claim_answer_carries_the_granted_lease_not_the_requested_one() {
    for (asked, granted) in [(0_u64, 5_u64), (1, 5), (600, 600), (99_999, 3600)] {
        for tb in backends().await {
            let name = tb.name();
            let backend = tb.dyn_backend();
            let state = make_state(backend.clone());
            register_and_start(&state, python_fn_start_ir("wf-lease-bounds"), json!({})).await;

            let (st, body) = post(
                &state,
                "/work-items/claim",
                json!({ "worker_id": "w", "queue_types": ["python_tool"], "lease_secs": asked }),
            )
            .await;
            assert!(st.is_success(), "{name}: {st} {body}");
            assert_eq!(
                body["work_item"]["lease_secs"], granted,
                "{name}: asked for {asked} s, the answer must carry the {granted} s granted: {body}"
            );
            let item = backend
                .get_work_item(body["work_item"]["id"].as_str().unwrap().parse().unwrap())
                .await
                .unwrap()
                .expect("item");
            let secs = (item.lease_expires_at.unwrap() - chrono::Utc::now()).num_seconds();
            assert!(
                (granted as i64 - 2..=granted as i64).contains(&secs),
                "{name}: asked for {asked} s, the item holds a {secs} s lease, not {granted} s"
            );
        }
    }
}
