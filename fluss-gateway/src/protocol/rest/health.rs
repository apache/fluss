// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Health endpoints.
//!
//! `GET /health` returns `{status, uptime_ms}` and answers from the event loop
//! without a backend RPC; deeper diagnostics live in the Prometheus metrics, not in this payload.
//! `GET /ready` is the readiness counterpart: 200 only while the gateway accepts traffic.

use crate::error::ErrorEnvelope;
use crate::protocol::rest::{RestState, error_response, json_response, request_id};
use axum::extract::{Request, State};
use axum::response::Response;
use serde::Serialize;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Health routes merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new()
        .routes(routes!(health))
        .routes(routes!(ready))
}

/// Response of `GET /health`: liveness plus process uptime.
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: &'static str,
    /// Milliseconds since the gateway process started.
    pub uptime_ms: u64,
}

/// Returns process liveness and uptime without a backend RPC.
#[utoipa::path(
    get,
    path = "/health",
    operation_id = "getHealth",
    tag = "health",
    responses(
        (status = 200, description = "Gateway liveness and uptime", body = HealthResponse),
        (status = 400, description = "This operation accepts no query parameters", body = ErrorEnvelope),
        (status = 405, description = "Wrong method for this route", body = ErrorEnvelope),
    )
)]
pub(crate) async fn health(State(state): State<RestState>) -> Response {
    // The response type is the documented schema, so the payload cannot drift from the contract.
    json_response(&HealthResponse {
        status: "ok",
        uptime_ms: u64::try_from(state.started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
    })
    .expect("the health response is serializable")
}

/// Response of `GET /ready`: the gateway accepts application traffic.
#[derive(Debug, Serialize, ToSchema)]
pub struct ReadyResponse {
    pub status: &'static str,
}

/// Readiness for load balancers and readiness probes.
///
/// `/health` is liveness and stays 200 while the process answers, so a liveness probe never
/// restarts a draining process; this route reports acceptance instead. Readiness covers the process
/// lifecycle only: Fluss cluster availability surfaces through request errors and metrics, not by
/// unloading every gateway instance at once.
#[utoipa::path(
    get,
    path = "/ready",
    operation_id = "getReady",
    tag = "health",
    responses(
        (status = 200, description = "The gateway accepts application traffic", body = ReadyResponse),
        (status = 400, description = "This operation accepts no query parameters", body = ErrorEnvelope),
        (status = 405, description = "Wrong method for this route", body = ErrorEnvelope),
        (status = 503, description = "The gateway is starting or shutting down", body = ErrorEnvelope),
    )
)]
pub(crate) async fn ready(State(state): State<RestState>, request: Request) -> Response {
    match state.readiness.ensure_accepting() {
        Ok(()) => json_response(&ReadyResponse { status: "ready" })
            .expect("the ready response is serializable"),
        Err(error) => error_response(&error, &request_id(&request)),
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// Builds the production router over serving test state.
    fn app() -> axum::Router {
        let state = test_support::test_state();
        state.readiness.set_serving();
        crate::protocol::rest::build_router(state, &test_support::test_options())
    }

    async fn get(app: axum::Router, path: &str) -> Response {
        app.oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("json body")
    }

    /// `/health` answers `{status, uptime_ms}` and nothing else.
    #[tokio::test]
    async fn health_answers_status_and_uptime_only() {
        let response = get(app(), "/health").await;
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["status"], "ok");
        assert!(json["uptime_ms"].is_u64(), "{json}");
        assert_eq!(
            json.as_object().expect("object").len(),
            2,
            "no diagnostic fields beyond status and uptime_ms: {json}"
        );
    }

    /// `/health` answers before startup completes: it sits outside the acceptance guard, so it
    /// never depends on the process having reached the serving state.
    #[tokio::test]
    async fn health_answers_before_startup_completes() {
        let state = test_support::test_state();
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());
        let response = get(app, "/health").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "ok");
    }

    /// The health endpoint stays reachable while the process is draining.
    #[tokio::test]
    async fn health_stays_200_during_shutdown() {
        let state = test_support::test_state();
        state.readiness.set_serving();
        state.readiness.begin_quiescing();
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());
        let response = get(app, "/health").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "ok");
    }

    /// Endpoints without declared query parameters reject them consistently.
    #[tokio::test]
    async fn health_endpoints_reject_query_parameters() {
        for path in ["/health?probe=deep", "/ready?probe=deep"] {
            let response = get(app(), path).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
            assert_eq!(
                body_json(response).await["error"]["code"],
                "invalid_argument"
            );
        }
    }

    /// `/ready` is 200 only while the gateway accepts traffic, so load balancers stop sending
    /// traffic the moment shutdown starts instead of feeding requests the acceptance guard rejects.
    #[tokio::test]
    async fn ready_reflects_acceptance_across_the_lifecycle() {
        let state = test_support::test_state();

        let app = crate::protocol::rest::build_router(state.clone(), &test_support::test_options());
        let response = get(app, "/ready").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let json = body_json(response).await;
        assert_eq!(json["error"]["code"], "unavailable");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("starting"),
            "the envelope names the state: {json}"
        );

        state.readiness.set_serving();
        let app = crate::protocol::rest::build_router(state.clone(), &test_support::test_options());
        let response = get(app, "/ready").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "ready");

        state.readiness.begin_quiescing();
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());
        let response = get(app, "/ready").await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body_json(response).await["error"]["code"], "unavailable");
    }
}
