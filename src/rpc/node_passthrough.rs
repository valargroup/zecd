//! Node calls zecd forwards to its upstream node unchanged.
//!
//! Mining-pool software (Miningcore in particular) talks to one daemon for both mining and
//! payouts, so the block-template and block-submission calls have to be answered by the same
//! endpoint as the wallet. zecd has no block data of its own for these, so it sends them to the
//! node on the daemon's `[backend]` connection and returns the node's answer as is.
//!
//! What a caller sees:
//! - the node's `result`, with missing mining difficulty filled from `getdifficulty` when available;
//! - the node's own error code and message when the node refuses the call (Miningcore probes
//!   `submitblock` with no params and expects `-1`);
//! - a generic `-1` when the node could not be reached, with the details in the server log
//!   only, since they name the node's address (the same rule as `From<anyhow::Error>` for
//!   [`RpcError`]);
//! - `-32601` on a lightwalletd backend, which has no node JSON-RPC to forward to.

use std::sync::Arc;

use serde_json::Value;

use crate::chain::hub::ChainHub;
use crate::chain::PassthroughError;
use crate::error::{codes, RpcError};
use crate::server::jsonrpc::RpcRequest;
use crate::state::AppState;

/// Forward `req` to the node behind the daemon's global `[backend]`.
pub(crate) async fn forward(
    state: &AppState,
    req: &RpcRequest,
    named: Option<Value>,
) -> Result<Value, RpcError> {
    forward_via(state.node_hub.as_ref(), req, named).await
}

async fn forward_via(
    hub: Option<&Arc<ChainHub>>,
    req: &RpcRequest,
    named: Option<Value>,
) -> Result<Value, RpcError> {
    let Some(hub) = hub else {
        return Err(RpcError::method_not_found(&req.method));
    };
    let params = named.unwrap_or_else(|| Value::Array(req.params.clone()));
    let mut result = hub
        .passthrough(&req.method, params)
        .await
        .map_err(|e| to_rpc_error(&req.method, e))?;
    if req.method == "getmininginfo" {
        add_missing_difficulty(hub, &mut result).await;
    }
    Ok(result)
}

/// zcashd's `getmininginfo` reports the network `difficulty`; Zebra's and Zakura's leave it
/// out. s-nomp reads it from there for its network stats and fails every minute without it, so
/// zecd fills it in from the node's `getdifficulty`. A node that already reports it is left
/// alone, and if the extra call fails the node's answer is returned as it was.
async fn add_missing_difficulty(hub: &Arc<ChainHub>, result: &mut Value) {
    let Some(info) = result.as_object_mut() else {
        return;
    };
    if info.contains_key("difficulty") {
        return;
    }
    match hub.passthrough("getdifficulty", Value::Array(vec![])).await {
        Ok(difficulty) => {
            info.insert("difficulty".into(), difficulty);
        }
        Err(e) => tracing::debug!("getmininginfo: no difficulty from the node: {e:?}"),
    }
}

/// How a forwarding failure reads to the RPC caller.
fn to_rpc_error(method: &str, e: PassthroughError) -> RpcError {
    match e {
        PassthroughError::Rpc { code, message } => {
            let code = i32::try_from(code).unwrap_or(codes::RPC_MISC_ERROR);
            RpcError::new(code, message)
        }
        PassthroughError::Unsupported => RpcError::method_not_found(method),
        PassthroughError::Transport(e) => {
            tracing::warn!(method, "forwarding to the node failed: {e:#}");
            RpcError::misc(format!("{method}: the node did not answer"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};

    use super::*;

    /// A node answering the mining calls the tests forward, and refusing everything else.
    async fn fake_node() -> SocketAddr {
        async fn handler(Json(req): Json<Value>) -> Json<Value> {
            let ok = |v: Value| Json(json!({ "result": v, "error": null, "id": req["id"] }));
            let err = |code: i64, msg: &str| {
                Json(
                    json!({ "result": null, "error": { "code": code, "message": msg }, "id": req["id"] }),
                )
            };
            match req["method"].as_str().unwrap_or_default() {
                // The dial's liveness probe.
                "getblockchaininfo" => {
                    ok(json!({ "chain": "regtest", "blocks": 10, "bestblockhash": "00" }))
                }
                "getdifficulty" => ok(json!(1.5)),
                "getmininginfo" if req["params"][0] == "has-difficulty" => {
                    ok(json!({ "blocks": 10, "difficulty": 7.0 }))
                }
                "getmininginfo" => ok(json!({ "blocks": 10, "networksolps": 3 })),
                "getblock" => ok(json!({ "echo": req["params"].clone() })),
                "submitblock" if req["params"].as_array().is_none_or(|p| p.is_empty()) => {
                    err(-32602, "Invalid params: expected a block")
                }
                "submitblock" if req["params"][0] == "beef" => ok(Value::Null),
                "submitblock" if req["params"][0] == "abcd" => {
                    Json(json!({"id": req["id"], "error": null}))
                }
                "submitblock" => ok(json!("duplicate")),
                other => err(-32601, &format!("Method not found: {other}")),
            }
        }
        let app = Router::new().route("/", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    fn hub_at(addr: SocketAddr) -> Arc<ChainHub> {
        let server = crate::backend::resolve(
            &format!("zebra://{}:{}", addr.ip(), addr.port()),
            crate::network::ZNetwork::Main,
        )
        .expect("loopback zebra endpoint resolves");
        ChainHub::new(server, Duration::from_secs(5))
    }

    fn call(method: &str, params: Vec<Value>) -> RpcRequest {
        RpcRequest::positional(method, params)
    }

    #[tokio::test]
    async fn the_node_result_is_returned_unchanged() {
        let hub = hub_at(fake_node().await);
        let v = forward_via(Some(&hub), &call("getdifficulty", vec![]), None)
            .await
            .unwrap();
        assert_eq!(v, json!(1.5));
        // Params go out exactly as the caller sent them.
        let v = forward_via(
            Some(&hub),
            &call("getblock", vec![json!("ab"), json!(1)]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(v, json!({ "echo": ["ab", 1] }));
        let v = forward_via(Some(&hub), &call("submitblock", vec![json!("00")]), None)
            .await
            .unwrap();
        assert_eq!(v, json!("duplicate"));
    }

    #[tokio::test]
    async fn getmininginfo_gains_the_difficulty_the_node_leaves_out() {
        let hub = hub_at(fake_node().await);
        let v = forward_via(Some(&hub), &call("getmininginfo", vec![]), None)
            .await
            .unwrap();
        assert_eq!(
            v,
            json!({ "blocks": 10, "networksolps": 3, "difficulty": 1.5 })
        );
        // A node that reports its own difficulty keeps it.
        let v = forward_via(
            Some(&hub),
            &call("getmininginfo", vec![json!("has-difficulty")]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(v["difficulty"], json!(7.0));
    }

    #[tokio::test]
    async fn submitblock_requires_an_explicit_result_even_when_null() {
        let hub = hub_at(fake_node().await);
        let accepted = forward_via(Some(&hub), &call("submitblock", vec![json!("beef")]), None)
            .await
            .unwrap();
        assert!(accepted.is_null());
        let missing = forward_via(Some(&hub), &call("submitblock", vec![json!("abcd")]), None)
            .await
            .unwrap_err();
        assert_eq!(missing.code, -1);
        assert_eq!(missing.message, "submitblock: the node did not answer");
    }

    #[tokio::test]
    async fn a_node_refusal_keeps_the_node_code_and_message() {
        let hub = hub_at(fake_node().await);
        let e = forward_via(Some(&hub), &call("submitblock", vec![]), None)
            .await
            .unwrap_err();
        assert_eq!(e.code, -32602);
        assert_eq!(e.message, "Invalid params: expected a block");
    }

    #[tokio::test]
    async fn an_unreachable_node_is_a_generic_error_that_does_not_name_the_node() {
        // Bind and drop a listener, so the port is known to refuse connections.
        let addr = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap()
        };
        let hub = hub_at(addr);
        let e = forward_via(Some(&hub), &call("getmininginfo", vec![]), None)
            .await
            .unwrap_err();
        assert_eq!(e.code, codes::RPC_MISC_ERROR);
        assert!(!e.message.contains("127.0.0.1"), "{}", e.message);
        assert!(
            !e.message.contains(&addr.port().to_string()),
            "{}",
            e.message
        );
    }

    #[tokio::test]
    async fn no_node_means_method_not_found() {
        let e = forward_via(None, &call("getblocktemplate", vec![]), None)
            .await
            .unwrap_err();
        assert_eq!(e.code, codes::RPC_METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn offline_lightwalletd_answers_method_not_found_without_dialing() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let server =
            crate::backend::resolve(&format!("http://{addr}"), crate::network::ZNetwork::Main)
                .unwrap();
        let hub = ChainHub::new(server, Duration::from_secs(5));
        let e = forward_via(Some(&hub), &call("getblocktemplate", vec![]), None)
            .await
            .unwrap_err();
        assert_eq!(e.code, codes::RPC_METHOD_NOT_FOUND);
    }

    #[test]
    fn a_node_code_outside_the_i32_range_becomes_a_misc_error() {
        let e = to_rpc_error(
            "getblock",
            PassthroughError::Rpc {
                code: i64::MAX,
                message: "odd".into(),
            },
        );
        assert_eq!(e.code, codes::RPC_MISC_ERROR);
        assert_eq!(e.message, "odd");
    }
}
