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

//! Generated OpenAPI 3.1 document served at `GET /v1/openapi.json`.
//!
//! The document is derived from the routers themselves by
//! [`utoipa_axum::router::OpenApiRouter::split_for_parts`] — there is no hand-maintained list of paths or
//! schemas anywhere in the crate, so the served contract cannot drift from the mounted routes. The error
//! schemas are the live wire types from [`crate::error`], and the `ErrorCode` vocabulary is generated from
//! the taxonomy, so the contract cannot drift from the implementation either.

use crate::error::{ErrorCode, ErrorEnvelope};
use crate::protocol::rest::{RestState, json_response};
use axum::extract::State;
use axum::response::Response;
use serde_json::Value;
use utoipa::{OpenApi, openapi::OpenApiBuilder};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Seeds the generated document with the shared error schemas, which no single handler owns.
#[derive(OpenApi)]
#[openapi(components(schemas(ErrorCode, ErrorEnvelope)))]
struct SharedSchemas;

/// OpenAPI routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::with_openapi(SharedSchemas::openapi()).routes(routes!(serve))
}

/// Applies the gateway's own metadata to the router-generated document.
///
/// Called once by [`crate::protocol::rest::build_router`].
pub(crate) fn finalize(api: utoipa::openapi::OpenApi) -> Value {
    let api = OpenApiBuilder::from(api)
        .info(
            utoipa::openapi::InfoBuilder::new()
                .title("fluss-gateway")
                .description(Some("Stateless REST gateway for Apache Fluss"))
                .version(env!("CARGO_PKG_VERSION"))
                .license(Some(
                    utoipa::openapi::LicenseBuilder::new()
                        .name("Apache-2.0")
                        .url(Some("https://www.apache.org/licenses/LICENSE-2.0"))
                        .build(),
                ))
                .build(),
        )
        // The gateway serves the API at the listener root; a relative server keeps the document
        // host-agnostic.
        .servers(Some([utoipa::openapi::ServerBuilder::new()
            .url("/")
            .build()]))
        // An explicit empty root security array: honest for this PR — no authentication exists yet. The
        // authentication capability adds securitySchemes and per-operation requirements.
        .security(Some(Vec::new()))
        .build();
    serde_json::to_value(api).expect("generated OpenAPI is serializable")
}

/// Serves the generated OpenAPI 3.1 document as JSON.
#[utoipa::path(
    get,
    path = "/v1/openapi.json",
    operation_id = "getOpenApi",
    tag = "metadata",
    responses(
        (status = 200, description = "OpenAPI 3.1 document"),
        (status = 405, description = "Wrong method for this route", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured limit", body = ErrorEnvelope),
        (status = 503, description = "Gateway starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn serve(State(state): State<RestState>) -> Response {
    let document = state
        .openapi
        .get()
        .expect("build_router fills the document before the router serves");
    json_response(document).expect("OpenAPI JSON is serializable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// Fetches the document exactly as the gateway serves it.
    async fn served_document() -> Value {
        let state = test_support::test_state();
        state.readiness.set_serving();
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// The checked-in `openapi.yaml` next to this crate's `Cargo.toml` (FIP-49).
    fn checked_in_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi.yaml")
    }

    /// Regenerates the checked-in `openapi.yaml` from the typed contract: `just openapi`.
    #[tokio::test]
    #[ignore = "rewrites openapi.yaml in the working tree; run via `just openapi`"]
    async fn export_checked_in_document() {
        let yaml =
            serde_yaml_ng::to_string(&served_document().await).expect("the document serializes");
        std::fs::write(checked_in_path(), yaml).expect("openapi.yaml is writable");
    }

    /// The checked-in document always matches the served one, so the published specification
    /// cannot drift from the implementation (FIP-49 schema-validation contract).
    #[tokio::test]
    async fn the_checked_in_document_matches_the_served_one() {
        let checked_in = std::fs::read_to_string(checked_in_path())
            .expect("openapi.yaml is checked in; regenerate it with `just openapi`");
        let checked_in: Value =
            serde_yaml_ng::from_str(&checked_in).expect("openapi.yaml parses as YAML");
        assert_eq!(
            checked_in,
            served_document().await,
            "openapi.yaml is stale; regenerate it with `just openapi`"
        );
    }

    #[tokio::test]
    async fn served_document_is_generated_from_the_mounted_routes() {
        let document = served_document().await;

        assert_eq!(document["openapi"], "3.1.0");
        assert_eq!(document["info"]["title"], "fluss-gateway");
        assert_eq!(document["info"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(document["info"]["license"]["name"], "Apache-2.0");
        assert!(
            document["info"].get("contact").is_none(),
            "the library-default contact must not leak"
        );
        assert!(
            !document["servers"]
                .as_array()
                .expect("servers array")
                .is_empty(),
            "a relative root server is declared"
        );
        assert!(
            document["security"]
                .as_array()
                .expect("security array")
                .is_empty(),
            "root security is explicitly empty until authentication lands"
        );
        assert_eq!(
            document["paths"]["/v1/openapi.json"]["get"]["operationId"],
            "getOpenApi"
        );
        assert!(
            document["components"]["schemas"]["ErrorEnvelope"].is_object(),
            "the shared error envelope is registered"
        );
        assert_eq!(
            document["components"]["schemas"]["ErrorBody"]["properties"]["code"]["$ref"],
            "#/components/schemas/ErrorCode",
            "the envelope code refers to the generated vocabulary: {}",
            document["components"]["schemas"]["ErrorBody"]
        );
    }

    /// The published `ErrorCode` vocabulary is generated from the taxonomy, so adding an [`ErrorKind`]
    /// without regenerating the document fails here rather than shipping a stale contract.
    #[tokio::test]
    async fn the_published_vocabulary_is_the_taxonomy() {
        let document = served_document().await;
        let published: Vec<&str> = document["components"]["schemas"]["ErrorCode"]["enum"]
            .as_array()
            .expect("ErrorCode enum values")
            .iter()
            .map(|value| value.as_str().expect("code is a string"))
            .collect();
        assert_eq!(published, crate::error::wire_codes());
    }

    #[tokio::test]
    async fn the_document_declares_no_scan_or_cursor_path() {
        let document = served_document().await;
        let paths = document["paths"].as_object().expect("paths object");
        for path in paths.keys() {
            assert!(!path.contains("/scan"), "stateless gateway exposes {path}");
            assert!(!path.contains("cursor"), "stateless gateway exposes {path}");
            assert!(
                !path.contains("offsets"),
                "stateless gateway exposes {path}"
            );
        }
    }
}
