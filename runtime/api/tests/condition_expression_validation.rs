//! Spike finding G5 (2026-09-06): a condition branch whose expression lacks the
//! required `state.` prefix registered fine and then evaluated false forever,
//! with only a WARN in the engine log to say why.
mod support;

use axum::http::StatusCode;
use jamjet_state::backend::StateBackend;
use jamjet_state::InMemoryBackend;
use serde_json::{json, Value};
use std::sync::Arc;
use support::{make_state, post};

fn condition_ir(expr: &str) -> Value {
    json!({
        "workflow_id": "wf-cond",
        "version": "0.1.0",
        "name": null, "description": null, "state_schema": "",
        "start_node": "check",
        "nodes": {
            "check": {
                "id": "check",
                "kind": { "type": "condition", "branches": [
                    { "condition": expr, "target": "a" },
                    { "condition": null, "target": "b" }
                ] },
                "retry_policy": null, "node_timeout_secs": null, "description": null, "labels": {}
            },
            "a": { "id": "a", "kind": { "type": "condition", "branches": [] }, "retry_policy": null, "node_timeout_secs": null, "description": null, "labels": {} },
            "b": { "id": "b", "kind": { "type": "condition", "branches": [] }, "retry_policy": null, "node_timeout_secs": null, "description": null, "labels": {} }
        },
        "edges": [
            { "from": "check", "to": "a", "condition": null },
            { "from": "check", "to": "b", "condition": null },
            { "from": "a", "to": "end", "condition": null },
            { "from": "b", "to": "end", "condition": null }
        ],
        "retry_policies": {}, "timeouts": {}, "models": {}, "tools": {},
        "mcp_servers": {}, "remote_agents": {}, "labels": {}
    })
}

#[tokio::test]
async fn registration_rejects_a_branch_expression_without_the_state_prefix() {
    let backend: Arc<dyn StateBackend> = Arc::new(InMemoryBackend::new());
    let state = make_state(backend);
    let (st, body) = post(
        &state,
        "/workflows",
        json!({ "ir": condition_ir("attempt == 1") }),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "got {st} {body}");
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("check") && msg.contains("state."),
        "error must name the node and the rule, got {msg}"
    );
}

#[tokio::test]
async fn registration_rejects_an_unparseable_literal() {
    let backend: Arc<dyn StateBackend> = Arc::new(InMemoryBackend::new());
    let state = make_state(backend);
    let (st, body) = post(
        &state,
        "/workflows",
        json!({ "ir": condition_ir("state.mode == done") }),
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "bare-word literals never match, got {st} {body}"
    );
}

#[tokio::test]
async fn registration_accepts_a_well_formed_expression() {
    let backend: Arc<dyn StateBackend> = Arc::new(InMemoryBackend::new());
    let state = make_state(backend);
    let (st, body) = post(
        &state,
        "/workflows",
        json!({ "ir": condition_ir("state.attempt == 1") }),
    )
    .await;
    assert!(st.is_success(), "got {st} {body}");
}
