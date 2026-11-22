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

//! Catalog metadata endpoints.

use crate::backend::FlussBackend;
use crate::backend::context::RequestContext;
use crate::error::{ErrorEnvelope, GatewayResult};
use crate::protocol::rest::datatype::ColumnDataType;
use crate::protocol::rest::pagination::{Collection, Page};
use crate::protocol::rest::{
    RestState, error_response, json_response, reject_query_parameters, request_id, resolve_cluster,
};
use axum::extract::{Path, Request, State};
use axum::middleware;
use axum::response::Response;
use fluss::metadata::{PartitionInfo, TableInfo, TablePath};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Response of `GET /v1/clusters/{cluster}/databases`.
#[derive(Debug, Serialize, ToSchema)]
pub struct DatabasesResponse {
    pub databases: Vec<String>,
    /// Present only while more entries follow.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub next_page_token: Option<String>,
}

/// Response of `GET /v1/clusters/{cluster}/databases/{database}/tables`.
#[derive(Debug, Serialize, ToSchema)]
pub struct TablesResponse {
    pub tables: Vec<String>,
    /// Present only while more entries follow.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub next_page_token: Option<String>,
}

/// Response of a database creation.
#[derive(Debug, Serialize, ToSchema)]
pub struct DatabaseResponse {
    pub database: String,
}

/// One column of a table schema.
#[derive(Debug, Serialize, ToSchema)]
pub struct ColumnResponse {
    pub name: String,
    pub data_type: ColumnDataType,
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub comment: Option<String>,
}

/// The bucket distribution of a table.
#[derive(Debug, Serialize, ToSchema)]
pub struct DistributionResponse {
    #[schema(minimum = 1)]
    pub bucket_count: i32,
    pub bucket_keys: Vec<String>,
}

/// Response of `GET /v1/clusters/{cluster}/databases/{database}/tables/{table}`.
#[derive(Debug, Serialize, ToSchema)]
pub struct TableResponse {
    pub database: String,
    pub table: String,
    pub columns: Vec<ColumnResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub primary_key: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub partitioned_by: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub distribution: Option<DistributionResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub comment: Option<String>,
    /// Fluss table properties.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub configs: Option<HashMap<String, String>>,
    /// Custom table metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub custom_properties: Option<HashMap<String, String>>,
}

impl From<&TableInfo> for TableResponse {
    fn from(info: &TableInfo) -> Self {
        Self {
            database: info.table_path.database().to_string(),
            table: info.table_path.table().to_string(),
            columns: info
                .schema
                .columns()
                .iter()
                .map(|column| {
                    let data_type = column.data_type();
                    ColumnResponse {
                        name: column.name().to_string(),
                        data_type: ColumnDataType::from(data_type),
                        nullable: data_type.is_nullable(),
                        comment: column.comment().map(str::to_string),
                    }
                })
                .collect(),
            primary_key: (!info.primary_keys.is_empty()).then(|| info.primary_keys.clone()),
            partitioned_by: (!info.partition_keys.is_empty()).then(|| info.partition_keys.to_vec()),
            distribution: (info.num_buckets > 0).then(|| DistributionResponse {
                bucket_count: info.num_buckets,
                bucket_keys: info.bucket_keys.clone(),
            }),
            comment: info.comment.clone(),
            configs: (!info.properties.is_empty()).then(|| info.properties.clone()),
            custom_properties: (!info.custom_properties.is_empty())
                .then(|| info.custom_properties.clone()),
        }
    }
}

/// Response of a partition creation.
#[derive(Debug, Serialize, ToSchema)]
pub struct PartitionResponse {
    pub database: String,
    pub table: String,
    pub partition: HashMap<String, String>,
}

/// One partition in a listing.
#[derive(Debug, Serialize, ToSchema)]
pub struct PartitionEntry {
    /// Native partition name; percent-encode it as a path segment when deleting.
    pub name: String,
    pub partition: HashMap<String, String>,
}

impl From<&PartitionInfo> for PartitionEntry {
    fn from(info: &PartitionInfo) -> Self {
        let spec = info.get_resolved_partition_spec();
        Self {
            name: info.get_partition_name(),
            partition: spec
                .get_partition_keys()
                .iter()
                .cloned()
                .zip(spec.get_partition_values().iter().cloned())
                .collect(),
        }
    }
}

/// Response of `GET /v1/clusters/{cluster}/databases/{database}/tables/{table}/partitions`.
#[derive(Debug, Serialize, ToSchema)]
pub struct PartitionsResponse {
    pub partitions: Vec<PartitionEntry>,
    /// Present only while more entries follow.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    pub next_page_token: Option<String>,
}

/// Metadata routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new()
        .routes(routes!(describe_table))
        .route_layer(middleware::from_fn(reject_query_parameters))
        .routes(routes!(list_databases))
        .routes(routes!(list_tables))
        .routes(routes!(list_partitions))
}

/// Lists the databases of one configured cluster.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases",
    operation_id = "listDatabases",
    tag = "metadata",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("max_results" = Option<usize>, Query,
            description = "Maximum entries to return. Defaults to 100, capped at 1000.",
            minimum = 1, maximum = 1000),
        ("page_token" = Option<String>, Query,
            description = "Opaque token from the `next_page_token` of the previous response."),
    ),
    responses(
        (status = 200, description = "Databases in lexical order", body = DatabasesResponse),
        (status = 400, description = "Invalid page parameter or page token", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured limit", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 501, description = "Fluss does not support the operation or API version", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn list_databases(
    State(state): State<RestState>,
    Path(cluster): Path<String>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    let prepared = prepare_page(&state, &request, &cluster, Collection::Databases, None);
    let result = async {
        let (backend, page, ctx) = prepared?;
        let (databases, next_page_token) = page.apply(backend.list_databases(&ctx).await?);
        json_response(&DatabasesResponse {
            databases,
            next_page_token,
        })
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Lists the tables of one database.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases/{database}/tables",
    operation_id = "listTables",
    tag = "metadata",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Database name"),
        ("max_results" = Option<usize>, Query,
            description = "Maximum entries to return. Defaults to 100, capped at 1000.",
            minimum = 1, maximum = 1000),
        ("page_token" = Option<String>, Query,
            description = "Opaque token from the `next_page_token` of the previous response."),
    ),
    responses(
        (status = 200, description = "Tables in lexical order", body = TablesResponse),
        (status = 400, description = "Invalid page parameter or page token", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster or database", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured limit", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 501, description = "Fluss does not support the operation or API version", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn list_tables(
    State(state): State<RestState>,
    Path((cluster, database)): Path<(String, String)>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    let prepared = prepare_page(
        &state,
        &request,
        &cluster,
        Collection::Tables,
        Some(&database),
    );
    let result = async {
        let (backend, page, ctx) = prepared?;
        let (tables, next_page_token) = page.apply(backend.list_tables(&ctx, &database).await?);
        json_response(&TablesResponse {
            tables,
            next_page_token,
        })
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Describes one table: its schema, keys, distribution, partitioning, and properties.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}",
    operation_id = "describeTable",
    tag = "metadata",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Database name"),
        ("table" = String, Path, description = "Table name"),
    ),
    responses(
        (status = 200, description = "The table metadata", body = TableResponse),
        (status = 400, description = "Unsupported query parameter", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster, database, or table", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured limit", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 501, description = "Fluss does not support the operation or API version", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn describe_table(
    State(state): State<RestState>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    let prepared = resolve_cluster(&state, &request, &cluster);
    let result = async {
        let (backend, ctx) = prepared?;
        let description = backend
            .describe_table(&ctx, &TablePath::new(database, table))
            .await?;
        json_response(&TableResponse::from(&description))
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Lists the partitions of one partitioned table.
#[utoipa::path(
    get,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/partitions",
    operation_id = "listPartitions",
    tag = "metadata",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Database name"),
        ("table" = String, Path, description = "Table name"),
        ("max_results" = Option<usize>, Query,
            description = "Maximum entries to return. Defaults to 100, capped at 1000.",
            minimum = 1, maximum = 1000),
        ("page_token" = Option<String>, Query,
            description = "Opaque token from the `next_page_token` of the previous response."),
    ),
    responses(
        (status = 200, description = "Partitions in partition-name order", body = PartitionsResponse),
        (status = 400, description = "Invalid page parameter or page token", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster, database, or table", body = ErrorEnvelope),
        (status = 413, description = "Request body above the configured limit", body = ErrorEnvelope),
        (status = 429, description = "Metadata concurrency limit exceeded", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 501, description = "Fluss does not support the operation or API version", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, or the gateway is starting or shutting down", body = ErrorEnvelope),
        (status = 504, description = "Request deadline exceeded", body = ErrorEnvelope),
    )
)]
pub(crate) async fn list_partitions(
    State(state): State<RestState>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    let table = TablePath::new(database, table);
    // The page token is scoped to the qualified table, so a token minted for one table cannot page
    // another.
    let prepared = prepare_page(
        &state,
        &request,
        &cluster,
        Collection::Partitions,
        Some(&table.to_string()),
    );
    let result = async {
        let (backend, page, ctx) = prepared?;
        let (partitions, next_page_token) = page
            .apply_by(backend.list_partitions(&ctx, &table).await?, |partition| {
                partition.get_partition_name().into()
            });
        json_response(&PartitionsResponse {
            partitions: partitions.iter().map(PartitionEntry::from).collect(),
            next_page_token,
        })
    }
    .await;
    result.unwrap_or_else(|error| error_response(&error, &request_id))
}

/// Validates the cluster and page request before calling the backend.
fn prepare_page(
    state: &RestState,
    request: &Request,
    cluster: &str,
    collection: Collection,
    scope: Option<&str>,
) -> GatewayResult<(Arc<dyn FlussBackend>, Page, RequestContext)> {
    let (backend, ctx) = resolve_cluster(state, request, cluster)?;
    let page = Page::parse(request.uri(), cluster, collection, scope)?;
    Ok((backend, page, ctx))
}

#[cfg(test)]
mod tests {
    use crate::backend::FlussBackend;
    use crate::backend::fake::{FakeFlussBackend, Operation};
    use crate::error::GatewayError;
    use crate::protocol::rest::test_support;
    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use fluss::metadata::{
        BigIntType, DataType, DecimalType, PartitionInfo, ResolvedPartitionSpec, Schema,
        StringType, TableDescriptor, TableInfo, TablePath,
    };
    use http_body_util::BodyExt;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn app(backend: Arc<FakeFlussBackend>) -> axum::Router {
        let state = test_support::state_with_backend(backend as Arc<dyn FlussBackend>);
        state.readiness.set_serving();
        crate::protocol::rest::build_router(state, &test_support::test_options())
    }

    fn catalog() -> Arc<FakeFlussBackend> {
        Arc::new(FakeFlussBackend::with_catalog(&[
            ("sales", &["orders", "customers"]),
            ("ops", &[]),
        ]))
    }

    async fn get(app: &axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
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

    #[tokio::test]
    async fn the_collections_answer_in_order_without_a_token() {
        let app = app(catalog());

        let (status, body) = get(&app, "/v1/clusters/default/databases").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"databases": ["ops", "sales"]}));

        let (status, body) = get(
            &app,
            "/v1/clusters/default/databases/sales/tables?max_results=2",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"tables": ["customers", "orders"]}));

        let (status, body) = get(&app, "/v1/clusters/default/databases/ops/tables").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"tables": []}));
    }

    #[tokio::test]
    async fn a_partial_page_carries_a_token_scoped_to_its_endpoint() {
        let app = app(catalog());

        let (_, body) = get(&app, "/v1/clusters/default/databases?max_results=1").await;
        assert_eq!(body["databases"], serde_json::json!(["ops"]));
        let token = body["next_page_token"]
            .as_str()
            .expect("a partial page carries a token")
            .to_string();

        let (status, body) = get(
            &app,
            &format!("/v1/clusters/default/databases?page_token={token}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"databases": ["sales"]}));

        let (status, body) = get(
            &app,
            &format!("/v1/clusters/default/databases/sales/tables?page_token={token}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn metadata_errors_keep_their_own_status() {
        let backend = catalog();
        let app = app(Arc::clone(&backend));

        for path in [
            "/v1/clusters/other/databases",
            "/v1/clusters/other/databases?max_results=0",
            "/v1/clusters/Not%20A%20Cluster/databases",
            "/v1/clusters/other/databases/sales/tables",
        ] {
            let (status, body) = get(&app, path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            assert_eq!(body["error"]["code"], "cluster_not_found", "{path}");
        }

        for path in [
            "/v1/clusters/default/databases?max_results=0",
            "/v1/clusters/default/databases?max_results=1&max_results=2",
            "/v1/clusters/%FF/databases",
            "/v1/clusters/default/databases?page_token=nope!",
            "/v1/clusters/default/databases/missing/tables?max_results=99999",
            "/v1/clusters/default/databases/sales/tables/orders?page_token=x",
            "/v1/clusters/other/databases/sales/tables/orders?page_token=x",
        ] {
            let (status, body) = get(&app, path).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
            assert_eq!(body["error"]["code"], "invalid_argument", "{path}");
        }

        let (status, body) = get(&app, "/v1/clusters/default/databases/missing/tables").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "database_not_found");

        for (operation, path) in [
            (Operation::ListDatabases, "/v1/clusters/default/databases"),
            (
                Operation::ListTables,
                "/v1/clusters/default/databases/sales/tables",
            ),
            (
                Operation::DescribeTable,
                "/v1/clusters/default/databases/sales/tables/orders",
            ),
            (
                Operation::ListPartitions,
                "/v1/clusters/default/databases/sales/tables/orders/partitions",
            ),
        ] {
            backend.fail_once(operation, GatewayError::unavailable("backend unavailable"));
            let (status, body) = get(&app, &format!("{path}?unsupported=true")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{operation:?}");
            assert_eq!(body["error"]["code"], "invalid_argument");
            let (status, body) = get(&app, path).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{operation:?}");
            assert_eq!(body["error"]["code"], "unavailable");
            assert_eq!(body["error"]["message"], "backend unavailable");
            assert_eq!(get(&app, path).await.0, StatusCode::OK, "{operation:?}");
        }
    }

    #[tokio::test]
    async fn a_table_is_described_in_the_shape_that_recreates_it() {
        let backend = Arc::new(FakeFlussBackend::new());
        let mut table = described_table();
        backend.define_table(table.clone());
        let app = app(Arc::clone(&backend));

        let (status, body) = get(&app, "/v1/clusters/default/databases/sales/tables/orders").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            serde_json::json!({
                "database": "sales",
                "table": "orders",
                "columns": [
                    {"name": "id", "data_type": {"type": "BIGINT"}, "nullable": false},
                    {
                        "name": "amount",
                        "data_type": {"type": "DECIMAL", "precision": 18, "scale": 2},
                        "nullable": true,
                        "comment": "the order total",
                    },
                    {"name": "dt", "data_type": {"type": "STRING"}, "nullable": false},
                ],
                "primary_key": ["id", "dt"],
                "partitioned_by": ["dt"],
                "distribution": {"bucket_count": 4, "bucket_keys": ["id"]},
                "comment": "the orders table",
                "configs": {"table.log.ttl": "7d"},
                "custom_properties": {"app.owner": "sales", "table.log.ttl": "custom value"},
            })
        );

        table.properties.clear();
        backend.define_table(table);
        let (status, custom_only) =
            get(&app, "/v1/clusters/default/databases/sales/tables/orders").await;
        assert_eq!(status, StatusCode::OK);
        assert!(custom_only.get("configs").is_none(), "{custom_only}");
        assert_eq!(custom_only["custom_properties"], body["custom_properties"]);
    }

    #[tokio::test]
    async fn a_log_table_omits_the_primary_key_fields() {
        let backend = catalog();
        let (status, body) = get(
            &app(backend),
            "/v1/clusters/default/databases/sales/tables/orders",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("primary_key").is_none(), "{body}");
        assert!(body.get("partitioned_by").is_none(), "{body}");
        assert!(body.get("configs").is_none(), "{body}");
        assert!(body.get("custom_properties").is_none(), "{body}");
    }

    #[tokio::test]
    async fn describing_what_does_not_exist_is_a_not_found() {
        let app = app(catalog());

        for (path, code) in [
            (
                "/v1/clusters/default/databases/absent/tables/orders",
                "database_not_found",
            ),
            (
                "/v1/clusters/default/databases/sales/tables/absent",
                "table_not_found",
            ),
            (
                "/v1/clusters/other/databases/sales/tables/orders",
                "cluster_not_found",
            ),
        ] {
            let (status, body) = get(&app, path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            assert_eq!(body["error"]["code"], code, "{path}");
        }
    }

    #[tokio::test]
    async fn partitions_page_in_name_order_under_a_table_scoped_token() {
        let backend = Arc::new(FakeFlussBackend::new());
        let table = TablePath::new("sales", "orders");
        backend.define_table(described_table());
        for name in ["2026-08-26", "2026-08-24", "2026-08-25"] {
            let spec =
                ResolvedPartitionSpec::new(Arc::from(["dt".to_string()]), vec![name.to_string()])
                    .expect("a fixture partition");
            backend.define_partition(&table, PartitionInfo::new(1, spec));
        }
        backend.define_database("sales");
        let mut updated = described_table();
        updated.comment = Some("updated table".to_string());
        backend.define_table(updated);
        let app = app(backend);

        let (status, body) = get(&app, "/v1/clusters/default/databases/sales/tables/orders").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["comment"], "updated table");

        let (status, body) = get(
            &app,
            "/v1/clusters/default/databases/sales/tables/orders/partitions?max_results=2",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["partitions"],
            serde_json::json!([
                {"name": "2026-08-24", "partition": {"dt": "2026-08-24"}},
                {"name": "2026-08-25", "partition": {"dt": "2026-08-25"}},
            ])
        );

        let token = body["next_page_token"].as_str().expect("a page token");
        let (status, body) = get(
            &app,
            &format!(
                "/v1/clusters/default/databases/sales/tables/orders/partitions?page_token={token}"
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("next_page_token").is_none(), "{body}");
        assert_eq!(
            body["partitions"],
            serde_json::json!([{"name": "2026-08-26", "partition": {"dt": "2026-08-26"}}])
        );

        let (status, _) = get(
            &app,
            &format!("/v1/clusters/default/databases?page_token={token}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    fn described_table() -> TableInfo {
        let schema = Schema::builder()
            .column("id", DataType::BigInt(BigIntType::with_nullable(false)))
            .column(
                "amount",
                DataType::Decimal(DecimalType::with_nullable(true, 18, 2).unwrap()),
            )
            .with_comment("the order total")
            .column("dt", DataType::String(StringType::with_nullable(false)))
            .primary_key(["id", "dt"])
            .unwrap()
            .build()
            .expect("the described schema is valid");
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .partitioned_by(vec!["dt".to_string()])
            .distributed_by(Some(4), vec!["id".to_string()])
            .comment("the orders table")
            .properties(HashMap::from([(
                "table.log.ttl".to_string(),
                "7d".to_string(),
            )]))
            .custom_properties(HashMap::from([
                ("app.owner".to_string(), "sales".to_string()),
                ("table.log.ttl".to_string(), "custom value".to_string()),
            ]))
            .build()
            .expect("the described descriptor is valid");
        TableInfo::of(
            TablePath::new("sales", "orders"),
            77,
            1,
            descriptor,
            1_700_000_000_000,
            1_700_000_000_000,
        )
    }
}
