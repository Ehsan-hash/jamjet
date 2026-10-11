//! Spike finding G2 (2026-09-06): `POST /executions` built the START node's
//! work item with only `workflow_id` and `workflow_version`, while every later
//! node dispatched by the scheduler carries `node_id` plus the kind-specific
//! enrichment (`module`, `function`, `input` for `python_fn`). A workflow whose
//! first step is a Python function therefore failed three times and died.
mod support;

use jamjet_state::backend::StateBackend;
use jamjet_state::InMemoryBackend;
use serde_json::json;
use std::sync::Arc;
use support::{make_state, python_fn_start_ir, register_and_start};

#[tokio::test]
async fn python_fn_start_node_work_item_is_enriched_like_every_other_node() {
    let backend: Arc<dyn StateBackend> = Arc::new(InMemoryBackend::new());
    let state = make_state(backend.clone());

    register_and_start(
        &state,
        python_fn_start_ir("wf-start"),
        json!({ "key": "value" }),
    )
    .await;

    let item = backend
        .claim_work_item("py-worker", &["python_tool"])
        .await
        .expect("claim")
        .expect("the start node must be claimable from python_tool");

    assert_eq!(item.node_id, "py_start");
    assert_eq!(
        item.payload["node_id"], "py_start",
        "base payload carries node_id"
    );
    assert_eq!(
        item.payload["module"], "myapp.tools",
        "python_fn enrichment: module"
    );
    assert_eq!(
        item.payload["function"], "run_check",
        "python_fn enrichment: function"
    );
    assert_eq!(
        item.payload["input"]["key"], "value",
        "python_fn enrichment: input is the initial state"
    );
    assert_eq!(item.payload["workflow_id"], "wf-start");
    assert_eq!(item.payload["workflow_version"], "0.1.0");
}
