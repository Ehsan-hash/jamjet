//! Shared helpers for the spike-regression route tests (2026-09-06).
#![allow(dead_code)]
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jamjet_agents::InMemoryAgentRegistry;
use jamjet_api::{routes::build_router_with_opts, state::AppState};
use jamjet_audit::{AuditEnricher, NoopAuditBackend};
use jamjet_state::backend::StateBackend;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

pub fn make_state(backend: Arc<dyn StateBackend>) -> AppState {
    let backend_for_fn = backend.clone();
    let audit: Arc<dyn jamjet_audit::AuditBackend> = Arc::new(NoopAuditBackend);
    let enricher = Arc::new(AuditEnricher::new(Arc::clone(&audit)));
    AppState {
        backend: backend.clone(),
        backend_for_fn: Arc::new(move |_t: &jamjet_state::TenantId| backend_for_fn.clone()),
        agents: Arc::new(InMemoryAgentRegistry::new()),
        audit,
        enricher,
        protocols: jamjet_api::state::default_protocol_registry(),
        cron_store: None,
    }
}

pub async fn post(state: &AppState, path: &str, body: Value) -> (StatusCode, Value) {
    let resp = build_router_with_opts(state.clone(), true)
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

pub async fn get(state: &AppState, path: &str) -> (StatusCode, Value) {
    let resp = build_router_with_opts(state.clone(), true)
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

/// A one-node workflow whose START node is a `python_fn`, exactly the shape
/// PAS uses for its first real step.
pub fn python_fn_start_ir(workflow_id: &str) -> Value {
    json!({
        "workflow_id": workflow_id,
        "version": "0.1.0",
        "name": null,
        "description": null,
        "state_schema": "",
        "start_node": "py_start",
        "nodes": {
            "py_start": {
                "id": "py_start",
                "kind": { "type": "python_fn", "module": "myapp.tools", "function": "run_check", "output_schema": "" },
                "retry_policy": null,
                "node_timeout_secs": null,
                "description": null,
                "labels": {}
            }
        },
        "edges": [ { "from": "py_start", "to": "end", "condition": null } ],
        "retry_policies": {},
        "timeouts": {},
        "models": {},
        "tools": {},
        "mcp_servers": {},
        "remote_agents": {},
        "labels": {}
    })
}

/// Register `ir` and start one execution of it with `input`; returns the
/// `exec_…` id string the API hands out.
pub async fn register_and_start(state: &AppState, ir: Value, input: Value) -> String {
    let workflow_id = ir["workflow_id"].as_str().unwrap().to_string();
    let version = ir["version"].as_str().unwrap().to_string();
    let (st, body) = post(state, "/workflows", json!({ "ir": ir })).await;
    assert!(st.is_success(), "register failed: {st} {body}");
    let (st, body) = post(
        state,
        "/executions",
        json!({ "workflow_id": workflow_id, "workflow_version": version, "input": input }),
    )
    .await;
    assert!(st.is_success(), "start failed: {st} {body}");
    body["execution_id"]
        .as_str()
        .expect("execution_id in start response")
        .to_string()
}
