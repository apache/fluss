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

//! Batch writes: validation failures reject the request before submission; completed writes report
//! per-entry outcomes. A request timeout cannot currently cancel native writes.

use crate::backend::context::RequestContext;
use crate::backend::types::ClusterId;
use crate::backend::unknown_cluster;
use crate::backend::{FlussBackend, WriteRequest, WriteResult};
use crate::error::{ErrorEnvelope, ErrorKind, GatewayError, GatewayResult};
use crate::observability;
use crate::protocol::rest::codec::{RowDecodeError, RowShape, SchemaDecoder};
use crate::protocol::rest::{
    RestState, error_response, json_response, request_context, request_id,
    validate_json_content_type,
};
use axum::body::Bytes;
use axum::extract::{FromRequest, Path, Request, State};
use axum::http::StatusCode;
use axum::response::Response;
use fluss::TableId;
use fluss::metadata::{TableInfo, TablePath};
use fluss::record::ChangeType;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Records routes, merged into the main router by [`crate::protocol::rest::build_router`].
pub fn routes() -> OpenApiRouter<RestState> {
    OpenApiRouter::new().routes(routes!(write_records))
}

struct Bucket {
    available: f64,
    per_second: f64,
    refilled_at: Instant,
}

impl Bucket {
    fn new(per_second: f64, now: Instant) -> Self {
        Self {
            available: per_second,
            per_second,
            refilled_at: now,
        }
    }

    fn take(&mut self, cost: f64, now: Instant) -> bool {
        let elapsed = now
            .saturating_duration_since(self.refilled_at)
            .as_secs_f64();
        self.available = (self.available + elapsed * self.per_second).min(self.per_second);
        self.refilled_at = now;
        if self.available >= cost || self.available >= self.per_second {
            self.available -= cost;
            true
        } else {
            false
        }
    }
}

pub(crate) struct RateLimit {
    buckets: Option<Mutex<(Bucket, Bucket)>>,
}

impl RateLimit {
    pub(crate) fn from_config(
        enabled: bool,
        requests_per_second: u32,
        bytes_per_second: u64,
    ) -> Self {
        let buckets = enabled.then(|| {
            let now = Instant::now();
            Mutex::new((
                Bucket::new(requests_per_second as f64, now),
                Bucket::new(bytes_per_second as f64, now),
            ))
        });
        Self { buckets }
    }

    pub(crate) fn admit(&self, body_bytes: usize) -> GatewayResult<()> {
        let Some(buckets) = &self.buckets else {
            return Ok(());
        };
        let now = Instant::now();
        let mut buckets = buckets.lock().expect("the rate limit lock is not poisoned");
        if !buckets.0.take(1.0, now) {
            return Err(GatewayError::resource_exhausted(
                "the write request rate limit is exhausted",
            ));
        }
        if !buckets.1.take(body_bytes as f64, now) {
            return Err(GatewayError::resource_exhausted(
                "the write byte rate limit is exhausted",
            ));
        }
        Ok(())
    }
}

struct CachedDecoder {
    identity: (TableId, i32),
    decoder: Arc<SchemaDecoder>,
}

pub(crate) struct SchemaCache {
    entries: RwLock<HashMap<(ClusterId, TablePath), CachedDecoder>>,
}

impl SchemaCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    fn get(&self, cluster: &ClusterId, table: &TableInfo) -> GatewayResult<Arc<SchemaDecoder>> {
        let key = (cluster.clone(), table.table_path.clone());
        let identity = (table.table_id, table.schema_id);
        if let Some(cached) = self
            .entries
            .read()
            .expect("the schema cache lock is not poisoned")
            .get(&key)
            .filter(|cached| cached.identity == identity)
        {
            return Ok(cached.decoder.clone());
        }

        let decoder = Arc::new(SchemaDecoder::new(table.row_type().clone())?);
        let mut entries = self
            .entries
            .write()
            .expect("the schema cache lock is not poisoned");
        if let Some(cached) = entries
            .get(&key)
            .filter(|cached| cached.identity == identity)
        {
            return Ok(cached.decoder.clone());
        }
        if entries.len() >= 1024 && !entries.contains_key(&key) {
            entries.clear();
        }
        entries.insert(
            key,
            CachedDecoder {
                identity,
                decoder: decoder.clone(),
            },
        );
        Ok(decoder)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Append,
    Upsert,
    Delete,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Upsert => "upsert",
            Self::Delete => "delete",
        }
    }
}

struct PreparedEntry {
    id: String,
    operation: Operation,
    row_json: Vec<u8>,
}

fn validate_operations(
    table: &TableInfo,
    entries: &[PreparedEntry],
    partial_update_columns: Option<&[String]>,
) -> Result<(), RowDecodeError> {
    if table.has_primary_key() {
        if let Some(entry) = entries
            .iter()
            .find(|entry| entry.operation == Operation::Append)
        {
            return Err(RowDecodeError::invalid(GatewayError::invalid_argument(
                format!(
                    "primary-key tables accept only upsert and delete operations, but entry `{}` is an append",
                    entry.id
                ),
            )));
        }
    } else {
        if let Some(entry) = entries
            .iter()
            .find(|entry| entry.operation != Operation::Append)
        {
            return Err(RowDecodeError::invalid(GatewayError::invalid_argument(
                format!(
                    "log tables accept only append operations, but entry `{}` uses the `{}` operation",
                    entry.id,
                    entry.operation.name()
                ),
            )));
        }
        if partial_update_columns.is_some() {
            return Err(RowDecodeError::invalid(GatewayError::invalid_argument(
                "partial updates are not supported for log tables",
            )));
        }
    }
    Ok(())
}

fn sparse_targets(
    table: &TableInfo,
    partial_update_columns: Option<&[String]>,
) -> Result<Option<Vec<String>>, RowDecodeError> {
    let auto_increment = table.get_schema().auto_increment_col_names();
    if !auto_increment.is_empty() && partial_update_columns.is_none() {
        return Err(RowDecodeError::schema_mismatch(
            GatewayError::invalid_argument(
                "this table has auto-increment columns, so partial_update_columns is required",
            ),
        ));
    }
    let Some(columns) = partial_update_columns else {
        return Ok(None);
    };
    if columns.is_empty() {
        return Err(
            GatewayError::invalid_argument("partial_update_columns must not be empty").into(),
        );
    }

    let fields = table.row_type().fields();
    let known: HashSet<&str> = fields.iter().map(|field| field.name()).collect();
    let mut selected = HashSet::with_capacity(columns.len());
    for column in columns {
        if !known.contains(column.as_str()) {
            return Err(RowDecodeError::schema_mismatch(
                GatewayError::invalid_argument(format!(
                    "partial-update column `{column}` is not in the table schema"
                )),
            ));
        }
        if !selected.insert(column.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "duplicate partial-update column `{column}`"
            ))
            .into());
        }
        if auto_increment.iter().any(|name| name == column) {
            return Err(RowDecodeError::schema_mismatch(
                GatewayError::invalid_argument(format!(
                    "auto-increment column `{column}` cannot be targeted"
                )),
            ));
        }
    }
    for key in table.get_primary_keys() {
        if !selected.contains(key.as_str()) {
            return Err(RowDecodeError::schema_mismatch(
                GatewayError::invalid_argument(format!(
                    "partial_update_columns must include primary-key column `{key}`"
                )),
            ));
        }
    }
    for field in fields {
        if table
            .get_primary_keys()
            .iter()
            .any(|key| key == field.name())
            || auto_increment.iter().any(|name| name == field.name())
        {
            continue;
        }
        if !field.data_type().is_nullable() {
            return Err(RowDecodeError::schema_mismatch(
                GatewayError::invalid_argument(format!(
                    "a partial update requires every non-primary-key, non-auto-increment column to be nullable, but column `{}` is NOT NULL",
                    field.name()
                )),
            ));
        }
    }
    Ok(Some(columns.to_vec()))
}

async fn decode_batch(
    ctx: &RequestContext,
    backend: &dyn FlussBackend,
    cache: &SchemaCache,
    table: &TablePath,
    entries: &[PreparedEntry],
    partial_update_columns: Option<&[String]>,
) -> Result<WriteRequest, GatewayError> {
    let snapshot = backend.table_info(ctx, table).await?;
    let decoder = cache.get(ctx.cluster_id(), &snapshot)?;
    let identity = (snapshot.table_id, snapshot.schema_id);
    match preflight(snapshot, &decoder, entries, partial_update_columns) {
        Ok(request) => Ok(request),
        Err(error) if !error.is_schema_mismatch() => Err(error.into_gateway_error()),
        Err(error) => {
            let refreshed = backend.describe_table(ctx, table).await?;
            if (refreshed.table_id, refreshed.schema_id) == identity {
                return Err(error.into_gateway_error());
            }
            let decoder = cache.get(ctx.cluster_id(), &refreshed)?;
            preflight(refreshed, &decoder, entries, partial_update_columns)
                .map_err(RowDecodeError::into_gateway_error)
        }
    }
}

fn preflight(
    table: TableInfo,
    decoder: &SchemaDecoder,
    entries: &[PreparedEntry],
    partial_update_columns: Option<&[String]>,
) -> Result<WriteRequest, RowDecodeError> {
    validate_operations(&table, entries, partial_update_columns)?;
    let targets = sparse_targets(&table, partial_update_columns)?;
    let primary_keys = table.get_primary_keys().clone();
    let mut rows = Vec::with_capacity(entries.len());
    let mut change_types = Vec::with_capacity(entries.len());
    for entry in entries {
        let shape = match entry.operation {
            Operation::Append => RowShape::Complete,
            Operation::Upsert => targets
                .as_deref()
                .map_or(RowShape::Complete, RowShape::Sparse),
            Operation::Delete => RowShape::Sparse(&primary_keys),
        };
        rows.push(decoder.decode_row(&format!("entry `{}`", entry.id), &entry.row_json, shape)?);
        change_types.push(match entry.operation {
            Operation::Append => ChangeType::AppendOnly,
            Operation::Upsert => ChangeType::Insert,
            Operation::Delete => ChangeType::Delete,
        });
    }
    WriteRequest::new(table, rows, change_types, targets).map_err(RowDecodeError::from)
}

fn ensure_json_acceptable(headers: &axum::http::HeaderMap) -> GatewayResult<()> {
    let Some(accept) = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(());
    };
    if accept.split(',').any(|entry| {
        matches!(
            entry.split(';').next().unwrap_or_default().trim(),
            "application/json" | "application/*" | "*/*"
        )
    }) {
        return Ok(());
    }
    Err(GatewayError::new(
        ErrorKind::NotAcceptable,
        "this operation answers `application/json` only",
    ))
}

/// The write request body.
///
/// The row objects stay as raw JSON so that number lexemes survive to schema-aware decoding: a
/// BIGINT or DECIMAL sent as a base-10 string must not pass through an `f64`.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteBody<T> {
    /// Columns targeted by every entry in this batch. KV tables only.
    ///
    /// Every primary-key column must be included, and every non-primary-key,
    /// non-auto-increment column in the table must be nullable. Missing or explicit-null nullable
    /// targets are written as null; untargeted columns are preserved. Deletes require only
    /// primary-key values and clear the targeted non-key columns; the row is removed when all
    /// non-key columns become null.
    #[serde(default)]
    #[schema(min_items = 1)]
    pub partial_update_columns: Option<Vec<String>>,
    #[schema(min_items = 1)]
    pub entries: Vec<WriteBodyEntry<T>>,
}

/// One request entry. Exactly one of `append`, `upsert`, or `delete` is required, and its value is
/// the row object.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub struct WriteBodyEntry<T> {
    /// Opaque caller correlation value, unique within the request, echoed by every outcome.
    pub id: String,
    #[serde(default, deserialize_with = "deserialize_operation")]
    #[schema(value_type = Object)]
    pub append: Option<T>,
    #[serde(default, deserialize_with = "deserialize_operation")]
    #[schema(value_type = Object)]
    pub upsert: Option<T>,
    #[serde(default, deserialize_with = "deserialize_operation")]
    #[schema(value_type = Object)]
    pub delete: Option<T>,
}

// Preserve explicit null so it cannot hide a second operation in an entry.
fn deserialize_operation<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WriteSuccessResponse {
    pub id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WriteFailureResponse {
    pub id: String,
    /// A stable error code, plus `storage_backpressure` — a KV write the store refused under
    /// backpressure. `timeout` marks an indeterminate outcome: the row may already be applied.
    pub error_code: String,
    pub message: String,
}

/// Ordered, entry-correlated outcomes of one batch.
#[derive(Debug, Serialize, ToSchema)]
pub struct WriteResponse {
    pub row_count: u64,
    pub success_count: u64,
    pub error_count: u64,
    /// Successes in input order.
    pub successes: Vec<WriteSuccessResponse>,
    /// Failures in input order. A verdict can be shared by entries that landed in one accumulator
    /// batch.
    pub failures: Vec<WriteFailureResponse>,
}

#[utoipa::path(
    post,
    path = "/v1/clusters/{cluster}/databases/{database}/tables/{table}/records",
    operation_id = "writeRecords",
    summary = "Writes a batch of records to a table",
    tag = "records",
    description = "Writes a batch in input order after validating every row against the table \
                   schema. A validation failure rejects the whole batch with 400 before anything \
                   is submitted. Delivery is at least once from the caller's perspective: the \
                   gateway never resubmits after submission, but client retries and caller \
                   retries can duplicate log appends. An entry whose code is `timeout` may have \
                   been applied, and entries sharing one accumulator batch can share a verdict.",
    params(
        ("cluster" = String, Path, description = "Configured cluster ID"),
        ("database" = String, Path, description = "Exact database name"),
        ("table" = String, Path, description = "Exact table name")
    ),
    request_body(content = WriteBody<serde_json::Value>, content_type = "application/json"),
    responses(
        (status = 200, description = "Ordered entry outcomes; completion can be indeterminate after submission", body = WriteResponse),
        (status = 400, description = "Malformed request, or preflight rejected the whole batch", body = ErrorEnvelope),
        (status = 404, description = "Unknown cluster or table", body = ErrorEnvelope),
        (status = 406, description = "A JSON response is not acceptable", body = ErrorEnvelope),
        (status = 413, description = "Body or row limit exceeded", body = ErrorEnvelope),
        (status = 415, description = "Unsupported request media type", body = ErrorEnvelope),
        (status = 429, description = "The write concurrency gate or rate limit is exhausted; retry after the `Retry-After` pause", body = ErrorEnvelope),
        (status = 500, description = "Fluss backend failure", body = ErrorEnvelope),
        (status = 501, description = "Fluss does not support the operation or API version", body = ErrorEnvelope),
        (status = 503, description = "Fluss is unavailable, the gateway is starting or shutting down, or the table/schema changed before submission", body = ErrorEnvelope),
        (status = 504, description = "The request deadline passed; writes may still complete after submission", body = ErrorEnvelope)
    )
)]
pub(crate) async fn write_records(
    State(state): State<RestState>,
    Path((cluster, database, table)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    let request_id = request_id(&request);
    run_write(&state, &cluster, database, table, request)
        .await
        .unwrap_or_else(|error| error_response(&error, &request_id))
}

async fn run_write(
    state: &RestState,
    cluster: &str,
    database: String,
    table: String,
    request: Request,
) -> Result<Response, GatewayError> {
    validate_json_content_type(request.headers())?;
    ensure_json_acceptable(request.headers())?;

    let cluster_id = ClusterId::try_from(cluster).map_err(|_| unknown_cluster(cluster))?;
    if !state.backend.has_cluster(cluster_id.as_str()) {
        return Err(unknown_cluster(cluster));
    }
    let ctx = request_context(cluster_id, &request);

    let body = collect_body(request).await?;
    state.write_rate.admit(body.len())?;

    let parsed: WriteBody<&RawValue> = serde_json::from_slice(&body).map_err(|error| {
        GatewayError::invalid_argument(format!(
            "the request body is not a valid write body: {error}"
        ))
    })?;
    if parsed.entries.len() > state.write_max_rows as usize {
        return Err(GatewayError::limit_exceeded(format!(
            "this write request carries {} rows but the limit is {}",
            parsed.entries.len(),
            state.write_max_rows
        )));
    }
    let partial_update_columns = parsed.partial_update_columns.as_deref();
    let entries = prepared_entries(&parsed)?;

    let path = TablePath::new(database, table);
    let write_request = decode_batch(
        &ctx,
        state.backend.as_ref(),
        state.schemas.as_ref(),
        &path,
        &entries,
        partial_update_columns,
    )
    .await?;

    let result = state.backend.write(&ctx, write_request).await?;

    observability::write_bytes(ctx.cluster_id().as_str(), body.len() as u64);
    if !result.failures.is_empty() {
        // One line per request rather than per row: a 10 000-row batch that fails wholesale must not
        // produce 10 000 log lines. The first code is enough to classify the cause, and the per-row
        // detail is already in the response.
        log::warn!(
            "request_id={} cluster={} table={}.{} wrote {} of {} rows; first failure was {}",
            ctx.request_id(),
            ctx.cluster_id().as_str(),
            path.database(),
            path.table(),
            result.success_count(),
            result.row_count,
            result.failures[0].error.code()
        );
    }
    json_response(&shape_response(&entries, result))
}

/// Applies the router's byte cap while reading, including bodies without Content-Length.
async fn collect_body(request: Request) -> Result<Bytes, GatewayError> {
    Bytes::from_request(request, &()).await.map_err(|error| {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            GatewayError::limit_exceeded("the request body exceeds the byte limit")
        } else {
            GatewayError::invalid_argument(format!("the request body is unreadable: {error}"))
        }
    })
}

/// Validates the entry envelope and lifts each row object out as raw bytes.
fn prepared_entries(body: &WriteBody<&RawValue>) -> Result<Vec<PreparedEntry>, GatewayError> {
    if body.entries.is_empty() {
        return Err(GatewayError::invalid_argument(
            "a write request must carry at least one entry",
        ));
    }
    let mut ids = HashSet::with_capacity(body.entries.len());
    let mut entries = Vec::with_capacity(body.entries.len());
    for entry in &body.entries {
        if !ids.insert(entry.id.as_str()) {
            return Err(GatewayError::invalid_argument(format!(
                "duplicate write entry ID `{}`",
                entry.id
            )));
        }
        let (operation, row) = match (entry.append, entry.upsert, entry.delete) {
            (Some(row), None, None) => (Operation::Append, row),
            (None, Some(row), None) => (Operation::Upsert, row),
            (None, None, Some(row)) => (Operation::Delete, row),
            _ => {
                return Err(GatewayError::invalid_argument(format!(
                    "entry `{}` must carry exactly one of append, upsert, or delete",
                    entry.id
                )));
            }
        };
        entries.push(PreparedEntry {
            id: entry.id.clone(),
            operation,
            row_json: row.get().as_bytes().to_vec(),
        });
    }
    Ok(entries)
}

/// Correlates the backend's index-keyed verdicts back to the caller's entry IDs.
fn shape_response(entries: &[PreparedEntry], result: WriteResult) -> WriteResponse {
    let failed: HashSet<usize> = result
        .failures
        .iter()
        .map(|failure| failure.index)
        .collect();
    let successes = entries
        .iter()
        .enumerate()
        .filter(|(index, _)| !failed.contains(index))
        .map(|(_, entry)| WriteSuccessResponse {
            id: entry.id.clone(),
        })
        .collect();
    let failures = result
        .failures
        .iter()
        .map(|failure| WriteFailureResponse {
            id: entries[failure.index].id.clone(),
            error_code: failure.error.code().to_string(),
            message: failure.error.message().to_string(),
        })
        .collect();
    WriteResponse {
        row_count: result.row_count,
        success_count: result.success_count(),
        error_count: result.error_count(),
        successes,
        failures,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::FlussBackend;
    use crate::backend::fake::FakeFlussBackend;
    use crate::backend::fake::{log_table_info, users_table_info};
    use crate::protocol::rest::{RestOptions, test_support};
    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use fluss::metadata::{DataType, JsonSerde, Schema, TableDescriptor};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn app(backend: Arc<FakeFlussBackend>) -> axum::Router {
        app_with(backend, test_support::test_options())
    }

    /// Builds the state from the same options as the router, so a test cannot enable a rate limit on
    /// one and not the other.
    fn app_with(backend: Arc<FakeFlussBackend>, options: RestOptions) -> axum::Router {
        let state = test_support::state_with_backend_and_options(
            backend as Arc<dyn FlussBackend>,
            &options,
        );
        state.readiness.set_serving();
        crate::protocol::rest::build_router(state, &options)
    }

    fn catalog() -> Arc<FakeFlussBackend> {
        Arc::new(
            FakeFlussBackend::with_catalog(&[("fluss", &["users", "applog"])])
                .with_table(users_table_info(1))
                .with_table(log_table_info(1)),
        )
    }

    const USERS: &str = "/v1/clusters/default/databases/fluss/tables/users/records";
    const APPLOG: &str = "/v1/clusters/default/databases/fluss/tables/applog/records";

    #[test]
    fn decoder_cache_tracks_table_identity() {
        let cache = SchemaCache::new();
        let cluster = ClusterId::try_from("default").unwrap();
        let table = users_table_info(1);
        let first = cache.get(&cluster, &table).unwrap();
        assert!(Arc::ptr_eq(&first, &cache.get(&cluster, &table).unwrap()));

        let mut recreated = users_table_info(1);
        recreated.table_id += 1;
        assert!(!Arc::ptr_eq(
            &first,
            &cache.get(&cluster, &recreated).unwrap()
        ));
    }

    #[test]
    fn token_buckets_enforce_and_refill_both_write_budgets() {
        let now = Instant::now();
        for mut bucket in [Bucket::new(1.0, now), Bucket::new(10.0, now)] {
            let capacity = bucket.per_second;
            assert!(bucket.take(capacity, now));
            assert!(!bucket.take(1.0, now));
            assert!(bucket.take(capacity, now + std::time::Duration::from_secs(1)));
        }
    }

    #[test]
    fn sparse_target_errors_refresh_only_when_metadata_may_be_stale() {
        let table = users_table_info(1);
        for (columns, stale) in [
            (vec!["id".to_string(), "unknown".to_string()], true),
            (vec!["name".to_string()], true),
            (Vec::new(), false),
            (vec!["id".to_string(), "id".to_string()], false),
        ] {
            assert_eq!(
                sparse_targets(&table, Some(&columns))
                    .unwrap_err()
                    .is_schema_mismatch(),
                stale
            );
        }
        assert_eq!(
            sparse_targets(&table, Some(&["id".to_string(), "name".to_string()])).unwrap(),
            Some(vec!["id".to_string(), "name".to_string()])
        );
        assert!(sparse_targets(&table, None).unwrap().is_none());
    }

    #[test]
    fn partial_updates_require_nullable_non_key_columns() {
        let schema = Schema::builder()
            .column(
                "id",
                DataType::Int(fluss::metadata::IntType::with_nullable(false)),
            )
            .column(
                "name",
                DataType::String(fluss::metadata::StringType::with_nullable(false)),
            )
            .primary_key(["id"])
            .unwrap()
            .build()
            .unwrap();
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), Vec::new())
            .build()
            .unwrap();
        let table = TableInfo::of(TablePath::new("fluss", "strict"), 1, 1, descriptor, 0, 0);

        let error =
            sparse_targets(&table, Some(&["id".to_string(), "name".to_string()])).unwrap_err();
        assert!(error.message().contains("column `name` is NOT NULL"));
    }

    /// The gateway only ever sees a table decoded from the server's JSON, so its auto-increment
    /// guards are reachable only if the column survives that round trip.
    #[test]
    fn auto_increment_guards_fire_on_a_table_loaded_from_json() {
        let schema = Schema::builder()
            .column(
                "uid",
                DataType::String(fluss::metadata::StringType::with_nullable(false)),
            )
            .column(
                "region",
                DataType::String(fluss::metadata::StringType::with_nullable(true)),
            )
            .column(
                "uid_int",
                DataType::BigInt(fluss::metadata::BigIntType::with_nullable(true)),
            )
            .primary_key(["uid"])
            .unwrap()
            .enable_auto_increment("uid_int")
            .unwrap()
            .build()
            .unwrap();
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), Vec::new())
            .build()
            .unwrap();

        let json = descriptor.serialize_json().unwrap();
        let decoded = TableDescriptor::deserialize_json(&json).unwrap();
        let table = TableInfo::of(TablePath::new("fluss", "autoinc"), 1, 1, decoded, 0, 0);
        assert_eq!(
            table.get_schema().auto_increment_col_names(),
            &vec!["uid_int".to_string()]
        );

        let missing = sparse_targets(&table, None).unwrap_err();
        assert!(
            missing
                .message()
                .contains("partial_update_columns is required"),
            "unexpected error: {}",
            missing.message()
        );

        let targeted =
            sparse_targets(&table, Some(&["uid".to_string(), "uid_int".to_string()])).unwrap_err();
        assert!(
            targeted
                .message()
                .contains("auto-increment column `uid_int` cannot be targeted"),
            "unexpected error: {}",
            targeted.message()
        );
    }

    async fn post(app: &axum::Router, path: &str, body: &str) -> (StatusCode, serde_json::Value) {
        post_with(app, path, body, &[("content-type", "application/json")]).await
    }

    async fn post_with(
        app: &axum::Router,
        path: &str,
        body: &str,
        headers: &[(&str, &str)],
    ) -> (StatusCode, serde_json::Value) {
        let mut builder = HttpRequest::builder().method("POST").uri(path);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn a_full_batch_succeeds_and_echoes_every_entry_id_in_order() {
        let app = app(catalog());
        let body = r#"{"entries":[
            {"id":"e1","upsert":{"id":1,"name":"ada"}},
            {"id":"e2","delete":{"id":2}},
            {"id":"e3","upsert":{"id":3,"name":"bob"}}
        ]}"#;

        let (status, body) = post(&app, USERS, body).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["row_count"], 3);
        assert_eq!(body["success_count"], 3);
        assert_eq!(body["error_count"], 0);
        assert_eq!(
            body["successes"],
            serde_json::json!([{"id":"e1"},{"id":"e2"},{"id":"e3"}])
        );
        assert_eq!(body["failures"], serde_json::json!([]));
    }

    /// A partial failure is a 200: the successful entries are real, and hiding them behind an error
    /// status would make the caller retry rows that are already durable.
    #[tokio::test]
    async fn a_partial_failure_is_a_200_that_names_both_sides_in_input_order() {
        let backend = catalog();
        backend.fail_rows(vec![(
            2,
            GatewayError::unavailable("no leader for bucket 3"),
        )]);
        let app = app(backend);

        let (status, body) = post(
            &app,
            USERS,
            r#"{"entries":[
                {"id":"e1","upsert":{"id":1,"name":"ada"}},
                {"id":"e2","upsert":{"id":2,"name":"bob"}},
                {"id":"e3","upsert":{"id":3,"name":"cyd"}}
            ]}"#,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["row_count"], 3);
        assert_eq!(body["success_count"], 2);
        assert_eq!(body["error_count"], 1);
        assert_eq!(
            body["successes"],
            serde_json::json!([{"id":"e1"},{"id":"e2"}])
        );
        assert_eq!(body["failures"][0]["id"], "e3");
        assert_eq!(body["failures"][0]["error_code"], "unavailable");
    }

    #[tokio::test]
    async fn the_envelope_rejections_are_all_400() {
        let backend = catalog();
        let app = app(Arc::clone(&backend));
        for (body, expected_in_message) in [
            (r#"{"entries":[]}"#, "at least one"),
            (
                r#"{"entries":[{"id":"e1","upsert":{"id":1},"delete":{"id":1}}]}"#,
                "exactly one",
            ),
            (r#"{"entries":[{"id":"e1"}]}"#, "exactly one"),
            (
                r#"{"entries":[{"id":"e1","upsert":{"id":1},"delete":null}]}"#,
                "exactly one",
            ),
            (
                r#"{"entries":[{"id":"e1","upsert":null}]}"#,
                "row must be a JSON object",
            ),
            (
                r#"{"entries":[{"id":"e1","upsert":null,"upsert":{"id":1}}]}"#,
                "duplicate field",
            ),
            (
                r#"{"entries":[{"id":"dup","upsert":{"id":1}},{"id":"dup","upsert":{"id":2}}]}"#,
                "duplicate write entry ID",
            ),
            (
                r#"{"partial_update_columns":"id","entries":[{"id":"e1","upsert":{"id":1}}]}"#,
                "expected a sequence",
            ),
            (
                r#"{"partial_update_columns":[],"entries":[{"id":"e1","upsert":{"id":1}}]}"#,
                "must not be empty",
            ),
            (
                r#"{"partial_update_columns":["id","id"],"entries":[{"id":"e1","upsert":{"id":1}}]}"#,
                "duplicate partial-update column",
            ),
            (
                r#"{"entries":[{"id":"e1","upsert":{"id":1,"unknown":2}}]}"#,
                "entry `e1`: unknown column",
            ),
            (
                r#"{"entries":[{"id":"e1","upsert":{"id":1}}],"nope":1}"#,
                "write body",
            ),
            (r#"not json"#, "write body"),
        ] {
            let (status, response) = post(&app, USERS, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
            assert_eq!(response["error"]["code"], "invalid_argument");
            let message = response["error"]["message"].as_str().unwrap();
            assert!(
                message.contains(expected_in_message),
                "body {body} gave message {message}"
            );
        }
        assert!(backend.writes().is_empty());
    }

    #[tokio::test]
    async fn streamed_bodies_are_bounded_without_content_length() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let backend = catalog();
        let mut options = test_support::test_options();
        options.max_body_bytes = 256;
        let app = app_with(Arc::clone(&backend), options);
        for (size, status) in [
            (256, StatusCode::OK),
            (257, StatusCode::PAYLOAD_TOO_LARGE),
            (4096, StatusCode::PAYLOAD_TOO_LARGE),
        ] {
            let mut body = br#"{"entries":[{"id":"e1","upsert":{"id":1}}]}"#.to_vec();
            body.resize(size, b' ');
            let chunks: Vec<_> = body.chunks(128).map(Bytes::copy_from_slice).collect();
            let read = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&read);
            let stream = futures_util::stream::iter(chunks.into_iter().map(move |chunk| {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok::<_, std::convert::Infallible>(chunk)
            }));
            let response = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri(USERS)
                        .header("content-type", "application/json")
                        .body(Body::from_stream(stream))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status, "body size {size}");
            assert!(
                read.load(Ordering::Relaxed) <= 3,
                "stop reading at the byte cap"
            );
            if status == StatusCode::PAYLOAD_TOO_LARGE {
                let bytes = response.into_body().collect().await.unwrap().to_bytes();
                let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(body["error"]["code"], "limit_exceeded");
            }
            assert_eq!(
                backend.writes().len(),
                1,
                "only the boundary-sized body is written"
            );
        }
    }

    #[tokio::test]
    async fn the_table_kind_decides_which_operations_are_accepted() {
        let app = app(catalog());

        let (status, body) = post(
            &app,
            APPLOG,
            r#"{"entries":[{"id":"e1","upsert":{"ts":"1","message":"hi"}}]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("log tables accept only append"),
            "{body}"
        );

        let (status, body) = post(
            &app,
            USERS,
            r#"{"entries":[{"id":"e1","append":{"id":1,"name":"ada"}}]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("primary-key tables accept only upsert and delete"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn an_unknown_cluster_and_an_unknown_table_are_both_404() {
        let app = app(catalog());

        let (status, body) = post(
            &app,
            "/v1/clusters/nope/databases/fluss/tables/users/records",
            r#"{"entries":[{"id":"e1","upsert":{"id":1}}]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "cluster_not_found");

        let (status, body) = post(
            &app,
            "/v1/clusters/default/databases/fluss/tables/gone/records",
            r#"{"entries":[{"id":"e1","upsert":{"id":1}}]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "table_not_found");
    }

    #[tokio::test]
    async fn the_media_type_and_row_limits_answer_415_406_and_413() {
        let app = app(catalog());
        let one = r#"{"entries":[{"id":"e1","upsert":{"id":1,"name":"ada"}}]}"#;

        let (status, _) = post_with(&app, USERS, one, &[("content-type", "text/plain")]).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let (status, _) = post_with(
            &app,
            USERS,
            one,
            &[
                ("content-type", "application/json"),
                ("accept", "text/html"),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::NOT_ACCEPTABLE);

        let mut options = test_support::test_options();
        options.write_max_rows = 1;
        let app = app_with(catalog(), options);
        let (status, body) = post(
            &app,
            USERS,
            r#"{"entries":[
                {"id":"e1","upsert":{"id":1,"name":"a"}},
                {"id":"e2","upsert":{"id":2,"name":"b"}}
            ]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body["error"]["code"], "limit_exceeded");
    }

    #[tokio::test]
    async fn exhausted_write_rate_limits_answer_429_with_retry_after() {
        let one = r#"{"entries":[{"id":"e1","upsert":{"id":1,"name":"ada"}}]}"#;
        for (requests_per_second, bytes_per_second) in [(1, u64::MAX), (u32::MAX, 1)] {
            let mut options = test_support::test_options();
            options.write_rate_limit_enabled = true;
            options.write_rate_limit_requests_per_second = requests_per_second;
            options.write_rate_limit_bytes_per_second = bytes_per_second;
            let app = app_with(catalog(), options);

            assert_eq!(post(&app, USERS, one).await.0, StatusCode::OK);
            let response = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri(USERS)
                        .header("content-type", "application/json")
                        .body(Body::from(one.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            assert!(response.headers().contains_key("retry-after"));
        }
    }

    #[tokio::test]
    async fn a_backend_outage_answers_503() {
        let backend = catalog();
        backend.fail_once(
            crate::backend::fake::Operation::DescribeTable,
            GatewayError::unavailable("Fluss is unreachable"),
        );
        let app = app(backend);
        let (status, body) = post(
            &app,
            USERS,
            r#"{"entries":[{"id":"e1","upsert":{"id":1,"name":"ada"}}]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["code"], "unavailable");
    }

    #[tokio::test]
    async fn a_query_string_and_a_wrong_method_are_refused() {
        let app = app(catalog());
        let (status, _) = post(
            &app,
            &format!("{USERS}?partial_update=true"),
            r#"{"entries":[{"id":"e1","upsert":{"id":1}}]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri(USERS)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    /// A partial update targets the writer, so the whole batch carries one column set — and the
    /// backend must receive exactly the columns the caller declared.
    #[tokio::test]
    async fn a_partial_update_passes_the_declared_columns_to_the_backend() {
        let backend = catalog();
        let app = app(Arc::clone(&backend));

        let (status, _) = post(
            &app,
            USERS,
            r#"{"partial_update_columns":["id","name"],
                "entries":[{"id":"e1","upsert":{"id":1,"name":"ada"}},
                           {"id":"e2","upsert":{"id":2}},
                           {"id":"e3","upsert":{"id":3,"name":null}}]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let recorded = backend.writes();
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0].as_deref(),
            Some(&["id".to_string(), "name".to_string()][..])
        );
    }
}
