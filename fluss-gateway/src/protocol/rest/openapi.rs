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
use crate::protocol::rest::datatype::{ColumnDataType, WireDataType, WireRowField};
use crate::protocol::rest::{RestState, json_response};
use axum::extract::State;
use axum::response::Response;
use serde_json::Value;
use utoipa::{OpenApi, ToSchema, openapi::OpenApiBuilder};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Registers shared schemas not collected from one handler.
#[derive(OpenApi)]
#[openapi(components(schemas(ErrorCode, ErrorEnvelope, WireDataType, WireRowField)))]
struct SharedSchemas;

/// OpenAPI routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::with_openapi(SharedSchemas::openapi()).routes(routes!(serve))
}

/// Applies gateway metadata and schema constraints to the router-generated document.
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
    let mut document = serde_json::to_value(api).expect("generated OpenAPI is serializable");
    // utoipa does not propagate deny_unknown_fields to internally tagged enum variants.
    for name in [ColumnDataType::name(), WireDataType::name()] {
        for variant in document["components"]["schemas"][name.as_ref()]["oneOf"]
            .as_array_mut()
            .expect("data type variants are generated")
        {
            variant["additionalProperties"] = Value::Bool(false);
        }
    }
    document
}

/// Serves the generated OpenAPI 3.1 document as JSON.
#[utoipa::path(
    get,
    path = "/v1/openapi.json",
    operation_id = "getOpenApi",
    tag = "metadata",
    responses(
        (status = 200, description = "OpenAPI 3.1 document"),
        (status = 400, description = "This operation accepts no query parameters", body = ErrorEnvelope),
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

    /// The checked-in `openapi.yaml` next to this crate's `Cargo.toml`.
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
    /// cannot drift from the implementation.
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

        let schemas = &document["components"]["schemas"];
        assert_eq!(
            schemas["CreateDatabaseBody"]["required"],
            serde_json::json!(["database"])
        );
        assert_eq!(
            schemas["CreateTableBody"]["required"],
            serde_json::json!(["table_name", "columns"])
        );
        assert!(
            document["paths"]["/v1/clusters/{cluster}/databases/{database}"]
                .get("get")
                .is_none(),
            "the API does not expose describeDatabase"
        );
        assert_eq!(
            schemas["TableResponse"]["required"],
            serde_json::json!(["database", "table", "columns"])
        );
        for (path, method, status) in [
            (
                "/v1/clusters/{cluster}/databases/{database}/tables",
                "post",
                "201",
            ),
            (
                "/v1/clusters/{cluster}/databases/{database}/tables/{table}",
                "patch",
                "204",
            ),
        ] {
            let response = &document["paths"][path][method]["responses"][status];
            assert!(response.is_object(), "{method} {path}: {status}");
            assert!(response.get("content").is_none(), "{method} {path}");
        }
        let table =
            &document["paths"]["/v1/clusters/{cluster}/databases/{database}/tables/{table}"];
        assert!(table["patch"]["responses"].get("200").is_none());
        assert_eq!(
            schemas["PartitionEntry"]["required"],
            serde_json::json!(["name", "partition"])
        );
        assert_eq!(
            schemas["PartitionsResponse"]["properties"]["partitions"]["items"]["$ref"],
            "#/components/schemas/PartitionEntry"
        );
        for field in [
            "primary_key",
            "partitioned_by",
            "distribution",
            "configs",
            "custom_properties",
            "comment",
        ] {
            assert!(
                !schemas["TableResponse"]["properties"][field]
                    .to_string()
                    .contains("\"null\""),
                "an absent table field is omitted, not nullable: {field}"
            );
        }
        for schema in ["CreateTableBody", "TableResponse"] {
            for field in ["configs", "custom_properties"] {
                let property = &schemas[schema]["properties"][field];
                assert_eq!(property["type"], "object", "{schema}.{field}");
                assert_eq!(
                    property["additionalProperties"]["type"], "string",
                    "{schema}.{field}"
                );
            }
        }
        for (name, has_nullable) in [("ColumnDataType", false), ("WireDataType", true)] {
            for variant in schemas[name]["oneOf"].as_array().expect("type variants") {
                assert_eq!(
                    variant["properties"].get("nullable").is_some(),
                    has_nullable,
                    "{name}: {variant}"
                );
                assert_eq!(
                    variant["additionalProperties"], false,
                    "type variants must reject undeclared fields: {name}: {variant}"
                );
            }
        }

        let mut pending = vec![&document];
        while let Some(value) = pending.pop() {
            match value {
                Value::Object(object) => {
                    if let Some(reference) = object.get("$ref").and_then(Value::as_str)
                        && let Some(pointer) = reference.strip_prefix('#')
                    {
                        assert!(
                            document.pointer(pointer).is_some(),
                            "unresolved reference: {reference}"
                        );
                    }
                    pending.extend(object.values());
                }
                Value::Array(array) => pending.extend(array),
                _ => {}
            }
        }

        for (path, item) in document["paths"]
            .as_object()
            .expect("paths object")
            .iter()
            .filter(|(path, _)| path.contains("/databases"))
        {
            for method in ["get", "post", "patch", "delete"] {
                let Some(operation) = item.get(method) else {
                    continue;
                };
                for status in ["400", "404", "413", "429", "500", "501", "503", "504"] {
                    assert!(
                        operation["responses"].get(status).is_some(),
                        "{method} {path} must declare {status}"
                    );
                }
                assert!(
                    operation["responses"].get("403").is_none(),
                    "{method} {path} must not declare caller authorization failures"
                );
            }
        }

        for (path, method) in [
            ("/v1/clusters/{cluster}/databases", "post"),
            ("/v1/clusters/{cluster}/databases/{database}/tables", "post"),
            (
                "/v1/clusters/{cluster}/databases/{database}/tables/{table}/partitions",
                "post",
            ),
        ] {
            assert!(
                document["paths"][path][method]["responses"]["201"]["headers"]["Location"]
                    .is_object(),
                "{method} {path} must declare its Location header"
            );
        }
    }

    /// The document route follows the same strict query policy as every other endpoint.
    #[tokio::test]
    async fn document_route_rejects_query_parameters() {
        let state = test_support::test_state();
        state.readiness.set_serving();
        let app = crate::protocol::rest::build_router(state, &test_support::test_options());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/openapi.json?format=yaml")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "invalid_argument");
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
