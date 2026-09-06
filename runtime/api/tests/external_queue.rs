//! Spike finding G1 (2026-09-06): queue type was derived from node kind, so a
//! worker in any language but Python or Java had nothing to claim. An
//! `external_fn` node names its queue and its handler; any worker that speaks
//! the claim/heartbeat/complete protocol can drain it.
mod support;

use jamjet_state::backend::StateBackend;
use jamjet_state::InMemoryBackend;
use serde_json::{json, Value};
use std::sync::Arc;
use support::{make_state, post, register_and_start};

fn external_fn_ir() -> Value {
    json!({
        "workflow_id": "wf-external",
        "version": "0.1.0",
        "name": null, "description": null, "state_schema": "",
        "start_node": "authorize",
        "nodes": {
            "authorize": {
                "id": "authorize",
                "kind": { "type": "external_fn", "queue": "pas-control", "handler": "authorize", "output_schema": "" },
                "retry_policy": null, "node_timeout_secs": null, "description": null, "labels": {}
            }
        },
        "edges": [ { "from": "authorize", "to": "end", "condition": null } ],
        "retry_policies": {}, "timeouts": {}, "models": {}, "tools": {},
        "mcp_servers": {}, "remote_agents": {}, "labels": {}
    })
}

#[tokio::test]
async fn an_external_fn_node_is_claimable_from_its_named_queue_with_handler_and_input() {
    let backend: Arc<dyn StateBackend> = Arc::new(InMemoryBackend::new());
    let state = make_state(backend);
    register_and_start(&state, external_fn_ir(), json!({ "key": "value" })).await;

    let (st, body) = post(
        &state,
        "/work-items/claim",
        json!({ "worker_id": "ts-worker", "queue_types": ["python_tool"] }),
    )
    .await;
    assert!(st.is_success());
    assert_eq!(
        body["claimed"], false,
        "the python queue must not see it, got {body}"
    );

    let (st, body) = post(
        &state,
        "/work-items/claim",
        json!({ "worker_id": "ts-worker", "queue_types": ["pas-control"] }),
    )
    .await;
    assert!(st.is_success());
    assert_eq!(body["claimed"], true, "got {body}");
    let wi = &body["work_item"];
    assert_eq!(wi["queue_type"], "pas-control");
    assert_eq!(wi["node_id"], "authorize");
    assert_eq!(wi["payload"]["handler"], "authorize");
    assert_eq!(wi["payload"]["input"]["key"], "value");
}
