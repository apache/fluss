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

//! Cluster discovery: `GET /v1/clusters`.

use crate::error::ErrorEnvelope;
use crate::protocol::rest::{RestState, json_response};
use axum::extract::State;
use axum::response::Response;
use serde::Serialize;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Response of `GET /v1/clusters`: the IDs used in the cluster-scoped paths of the other APIs.
#[derive(Debug, Serialize, ToSchema)]
pub struct ClustersResponse {
    pub clusters: Vec<String>,
}

/// Cluster discovery routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new().routes(routes!(list_clusters))
}

/// Lists the configured clusters.
///
/// This is a configuration echo: it touches no connection, so it answers at the same speed whether
/// Fluss is up or down, and a caller polling it cannot turn into a burst of connection attempts. It
/// deliberately carries **no** reachability field — the gateway has no background probe, so any such
/// field would report the outcome of some earlier request rather than the current state. Whether a
/// cluster can be served shows up in the errors of its own requests and in the `connections_*` metrics.
#[utoipa::path(
    get,
    path = "/v1/clusters",
    operation_id = "listClusters",
    tag = "metadata",
    responses(
        (status = 200, description = "Configured cluster IDs in lexical order", body = ClustersResponse),
        (status = 400, description = "This operation accepts no query parameters", body = ErrorEnvelope),
        (status = 405, description = "Wrong method for this route", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured limit", body = ErrorEnvelope),
        (status = 503, description = "Gateway starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn list_clusters(State(state): State<RestState>) -> Response {
    let clusters = state
        .backend
        .clusters()
        .iter()
        .map(|id| id.as_str().to_string())
        .collect();
    json_response(&ClustersResponse { clusters }).expect("cluster discovery is serializable")
}

#[cfg(test)]
mod tests {
    use crate::backend::fake::FakeFlussBackend;
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    /// A serving router over two configured clusters.
    fn app() -> axum::Router {
        let state = test_support::state_with_backend(Arc::new(FakeFlussBackend::with_clusters(&[
            "zeta", "alpha",
        ])));
        state.readiness.set_serving();
        crate::protocol::rest::build_router(state, &test_support::test_options())
    }

    async fn request(
        app: axum::Router,
        method: Method,
        path: &str,
    ) -> (StatusCode, serde_json::Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    /// Discovery lists every configured cluster ID in lexical order, and repeating the request returns
    /// the identical body: the answer is a function of configuration, never of the caller or of Fluss.
    #[tokio::test]
    async fn discovery_is_ordered_and_repeatable() {
        let app = app();
        let (status, body) = request(app.clone(), Method::GET, "/v1/clusters").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"clusters": ["alpha", "zeta"]}));

        let (_, again) = request(app, Method::GET, "/v1/clusters").await;
        assert_eq!(body, again);
    }

    /// The route defines no query parameters, so one is a bad request rather than being ignored.
    #[tokio::test]
    async fn discovery_rejects_query_parameters_and_other_methods() {
        let (status, body) = request(app(), Method::GET, "/v1/clusters?max_results=1").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");

        let (status, body) = request(app(), Method::POST, "/v1/clusters").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(body["error"]["code"], "method_not_allowed");
    }

    /// Discovery sits behind the acceptance guard: a draining gateway stops answering it, so a caller
    /// cannot pick a cluster from an instance that is on its way out.
    #[tokio::test]
    async fn discovery_is_unavailable_while_draining() {
        let state = test_support::test_state();
        state.readiness.set_serving();
        state.readiness.begin_quiescing();
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());

        let (status, body) = request(app, Method::GET, "/v1/clusters").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["code"], "unavailable");
    }
}
