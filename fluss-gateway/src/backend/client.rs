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

//! The production [`FlussBackend`] over one connection cache per configured cluster.

use crate::backend::connection::{ConnectionCache, NativeConnector};
use crate::backend::context::RequestContext;
use crate::backend::errors::{classify_fluss_error, map_fluss_error};
use crate::backend::types::ClusterId;
use crate::backend::unknown_cluster;
use crate::backend::{FlussBackend, RowWriteError, WriteRequest, WriteResult};
use crate::config::GatewayConfig;
use crate::error::{ErrorKind, GatewayError, GatewayResult, Resource};
use crate::observability;
use async_trait::async_trait;
use fluss::client::{
    AppendWriter, FlussAdmin, FlussConnection, FlussTable, UpsertWriter, WriteResultFuture,
};
use fluss::error::{Error as FlussClientError, FlussError};
use fluss::metadata::{
    AlterTableChanges, PartitionInfo, PartitionSpec, PhysicalTablePath, TableDescriptor, TableInfo,
    TablePath,
};
use fluss::record::ChangeType;
use fluss::row::GenericRow;
use futures_util::future::join_all;
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

const UNKNOWN_COMPLETION: &str = "write completion is unknown; a retry may duplicate this row";

pub struct NativeFlussBackend {
    caches: BTreeMap<ClusterId, ConnectionCache<NativeConnector>>,
}

impl NativeFlussBackend {
    /// Builds the routing table without connecting.
    pub fn from_config(config: &GatewayConfig) -> Self {
        let caches = config
            .clusters
            .iter()
            .map(|(id, cluster)| {
                let id = ClusterId::try_from(id.as_str())
                    .expect("configuration validation accepted every cluster ID");
                let cache =
                    ConnectionCache::new(id.clone(), cluster, NativeConnector::new(cluster));
                (id, cache)
            })
            .collect();
        Self { caches }
    }

    /// Closes all clusters concurrently within one timeout.
    pub(crate) async fn close(&self, timeout: Duration) -> GatewayResult<()> {
        let closes = join_all(self.caches.values().map(|cache| cache.close(timeout))).await;
        let mut first_failure = None;
        for (id, result) in self.caches.keys().zip(closes) {
            if let Err(error) = result {
                log::warn!("failed to close the connections of cluster `{id}`: {error}");
                first_failure = first_failure.or(Some(error));
            }
        }
        first_failure.map_or(Ok(()), Err)
    }

    /// Runs one idle scan for every configured cluster.
    pub(crate) async fn clean_expired_connections(&self) {
        join_all(self.caches.values().map(ConnectionCache::clean_expired)).await;
    }

    fn cache_for(&self, ctx: &RequestContext) -> GatewayResult<&ConnectionCache<NativeConnector>> {
        self.caches
            .get(ctx.cluster_id())
            .ok_or_else(|| unknown_cluster(ctx.cluster_id().as_str()))
    }

    /// Routes and bounds one native admin call, then classifies its failure.
    async fn admin_call<T, F, Fut>(
        &self,
        ctx: &RequestContext,
        what: &'static str,
        operation: F,
    ) -> GatewayResult<T>
    where
        F: FnOnce(Arc<FlussAdmin>) -> Fut,
        Fut: Future<Output = Result<T, FlussClientError>>,
    {
        let cache = self.cache_for(ctx)?;
        ctx.run(async {
            let connection = cache.connection(ctx).await?;
            let result = match connection.get_admin() {
                Ok(admin) => operation(admin).await,
                Err(error) => Err(error),
            };
            result.map_err(|native| map_fluss_error(what, native, Some(ctx)))
        })
        .await
    }

    async fn submit_write(
        connection: &Arc<FlussConnection>,
        ctx: &RequestContext,
        request: WriteRequest,
    ) -> GatewayResult<WriteResult> {
        let metadata = connection.get_metadata();
        let is_append = request.is_append();
        let (table_info, rows, change_types, partial_update_columns) = request.into_parts();
        let table = FlussTable::new(connection.as_ref(), metadata, table_info);
        let row_count = rows.len() as u64;
        let writer_error = |error| map_fluss_error("prepare the table writer", error, Some(ctx));
        let writer = if is_append {
            Writer::Append(
                table
                    .new_append()
                    .and_then(|append| append.create_writer())
                    .map_err(&writer_error)?,
            )
        } else {
            let upsert = table.new_upsert().map_err(&writer_error)?;
            let upsert = match &partial_update_columns {
                Some(columns) => {
                    // TODO: Roll fluss-rust batches when partial-update columns change.
                    let names: Vec<&str> = columns.iter().map(String::as_str).collect();
                    upsert
                        .partial_update_with_column_names(&names)
                        .map_err(&writer_error)?
                }
                None => upsert,
            };
            Writer::Upsert(upsert.create_writer().map_err(&writer_error)?)
        };

        let (mut failures, pending) =
            tokio::task::spawn_blocking(move || submit_in_order(&writer, rows, change_types))
                .await
                .map_err(map_submit_join_error)?;

        let deadline = ctx.deadline();
        for (index, future) in pending {
            let error = match tokio::time::timeout_at(deadline.into(), future).await {
                Ok(Ok(())) => continue,
                Ok(Err(error)) => classify_unknown(error),
                Err(_) => GatewayError::new(ErrorKind::DeadlineExceeded, UNKNOWN_COMPLETION),
            };
            failures.push(RowWriteError { index, error });
        }
        failures.sort_by_key(|failure| failure.index);

        Ok(WriteResult {
            row_count,
            failures,
        })
    }
}

#[async_trait]
impl FlussBackend for NativeFlussBackend {
    fn clusters(&self) -> Vec<ClusterId> {
        self.caches.keys().cloned().collect()
    }

    fn has_cluster(&self, id: &str) -> bool {
        ClusterId::try_from(id).is_ok_and(|id| self.caches.contains_key(&id))
    }

    async fn list_databases(&self, ctx: &RequestContext) -> GatewayResult<Vec<String>> {
        self.admin_call(ctx, "list the databases", |admin| async move {
            admin.list_databases().await
        })
        .await
    }

    async fn list_tables(
        &self,
        ctx: &RequestContext,
        database: &str,
    ) -> GatewayResult<Vec<String>> {
        self.admin_call(ctx, "list the tables", |admin| async move {
            admin.list_tables(database).await
        })
        .await
    }

    async fn create_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()> {
        self.admin_call(ctx, "create the database", |admin| async move {
            admin.create_database(database, None, false).await
        })
        .await
    }

    async fn drop_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()> {
        self.admin_call(ctx, "drop the database", |admin| async move {
            admin.drop_database(database, false, false).await
        })
        .await
    }

    async fn table_info(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<TableInfo> {
        let connection = ctx.run(self.cache_for(ctx)?.connection(ctx)).await?;
        if let Some(table) = connection
            .get_metadata()
            .get_cluster()
            .opt_get_table(table)
            .cloned()
        {
            return Ok(table);
        }
        self.describe_table(ctx, table).await
    }

    async fn describe_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<TableInfo> {
        self.admin_call(ctx, "describe the table", |admin| async move {
            admin.get_table_info(table).await
        })
        .await
    }

    async fn create_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        descriptor: &TableDescriptor,
    ) -> GatewayResult<()> {
        self.admin_call(ctx, "create the table", |admin| async move {
            admin.create_table(table, descriptor, false).await
        })
        .await
    }

    async fn alter_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        changes: AlterTableChanges,
    ) -> GatewayResult<()> {
        self.admin_call(ctx, "alter the table", |admin| async move {
            admin.alter_table(table, false, changes).await
        })
        .await
    }

    async fn drop_table(&self, ctx: &RequestContext, table: &TablePath) -> GatewayResult<()> {
        self.admin_call(ctx, "drop the table", |admin| async move {
            admin.drop_table(table, false).await
        })
        .await
    }

    async fn list_partitions(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<Vec<PartitionInfo>> {
        self.admin_call(ctx, "list the partitions", |admin| async move {
            admin.list_partition_infos(table).await
        })
        .await
    }

    async fn create_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()> {
        self.admin_call(ctx, "create the partition", |admin| async move {
            admin.create_partition(table, spec, false).await
        })
        .await
    }

    async fn drop_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()> {
        self.admin_call(ctx, "drop the partition", |admin| async move {
            admin.drop_partition(table, spec, false).await
        })
        .await
    }

    async fn write(
        &self,
        ctx: &RequestContext,
        request: WriteRequest,
    ) -> GatewayResult<WriteResult> {
        let connection = ctx.run(self.cache_for(ctx)?.connection(ctx)).await?;
        let row_count = request.rows().len() as u64;
        let table = request.table().clone();
        let current = connection
            .get_metadata()
            .get_cluster()
            .opt_get_table(&table)
            .map(|table| (table.table_id, table.schema_id))
            .ok_or_else(|| {
                GatewayError::unavailable(
                    "table metadata changed before submission; retry the request",
                )
                .with_resource(Resource::Table)
            })?;
        // TODO: Remove this check after fluss-rust pins log batches to table/schema identity.
        if (request.table.table_id, request.table.schema_id) != current {
            return Err(GatewayError::unavailable(
                "the table or schema changed before submission; retry the request",
            )
            .with_resource(Resource::Table));
        }

        let result = Self::submit_write(&connection, ctx, request).await;
        let stale = match &result {
            Err(error) => error.resource() == Some(Resource::Table),
            Ok(result) => result
                .failures
                .iter()
                .any(|failure| failure.error.resource() == Some(Resource::Table)),
        };
        if stale {
            invalidate_table_locations(&connection, &table);
        }
        let result = result?;
        observability::write_rows(ctx.cluster_id().as_str(), row_count);
        Ok(result)
    }
}

enum Writer {
    Append(AppendWriter),
    Upsert(UpsertWriter),
}

fn submit_in_order(
    writer: &Writer,
    rows: Vec<GenericRow<'static>>,
    change_types: Vec<ChangeType>,
) -> (Vec<RowWriteError>, Vec<(usize, WriteResultFuture)>) {
    let mut failures = Vec::new();
    let mut pending = Vec::with_capacity(rows.len());
    for (index, (row, change_type)) in rows.iter().zip(change_types).enumerate() {
        let submitted = match (writer, change_type) {
            (Writer::Append(writer), ChangeType::AppendOnly) => writer.append(row),
            (Writer::Upsert(writer), ChangeType::Insert) => writer.upsert(row),
            (Writer::Upsert(writer), ChangeType::Delete) => writer.delete(row),
            _ => unreachable!(
                "WriteRequest::new rejects a mixed batch and any non-write change type"
            ),
        };
        match submitted {
            Ok(future) => pending.push((index, future)),
            Err(error) => failures.push(RowWriteError {
                index,
                error: classify_rejected(error),
            }),
        }
    }
    (failures, pending)
}

fn classify_rejected(error: FlussClientError) -> GatewayError {
    let failure = classify_fluss_error("write the row", &error);
    log::debug!("a write row was refused before submission: {error}");
    failure
}

fn classify_unknown(error: FlussClientError) -> GatewayError {
    if matches!(
        error.api_error(),
        Some(
            FlussError::StorageBackpressureException
                | FlussError::AuthorizationException
                | FlussError::TableNotExist
        )
    ) {
        return classify_rejected(error);
    }
    let classified = classify_fluss_error("write the row", &error);
    log::debug!("a submitted write row ended indeterminately: {error}");
    let failure = GatewayError::new(classified.kind(), UNKNOWN_COMPLETION);
    match classified.resource() {
        Some(resource) => failure.with_resource(resource),
        None => failure,
    }
}

fn map_submit_join_error(error: tokio::task::JoinError) -> GatewayError {
    if error.is_cancelled() {
        GatewayError::unavailable("the write submission stopped during gateway shutdown")
    } else {
        log::error!("the write submission task failed unexpectedly: {error}");
        GatewayError::internal("the write submission failed unexpectedly")
    }
}

fn invalidate_table_locations(connection: &FlussConnection, table: &TablePath) {
    let metadata = connection.get_metadata();
    let cluster = metadata.get_cluster();
    let mut paths: HashSet<PhysicalTablePath> = cluster
        .get_bucket_locations_by_path()
        .keys()
        .filter(|path| path.get_table_path() == table)
        .map(|path| path.as_ref().clone())
        .collect();
    if paths.is_empty() {
        paths.insert(PhysicalTablePath::of(Arc::new(table.clone())));
    }
    drop(cluster);
    metadata.invalidate_physical_table_meta(&paths);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::errors::tests::api_failure;
    use crate::config::{ClusterConfig, ConfigDuration};
    use crate::error::ErrorKind;

    fn backend(clusters: &[(&str, ClusterConfig)]) -> NativeFlussBackend {
        NativeFlussBackend::from_config(&GatewayConfig {
            clusters: clusters
                .iter()
                .map(|(id, config)| ((*id).to_string(), config.clone()))
                .collect(),
            ..GatewayConfig::default()
        })
    }

    fn service_cluster() -> ClusterConfig {
        ClusterConfig::default()
    }

    #[test]
    fn failures_after_submission_report_an_unknown_outcome() {
        let rejected = classify_rejected(api_failure(FlussError::RequestTimeOut));
        let unknown = classify_unknown(api_failure(FlussError::RequestTimeOut));

        assert_eq!(rejected.kind(), ErrorKind::DeadlineExceeded);
        assert!(!rejected.message().contains("unknown"));
        assert_eq!(unknown.kind(), ErrorKind::DeadlineExceeded);
        assert!(unknown.message().contains("completion is unknown"));
    }

    #[test]
    fn definite_server_rejections_remain_definite_after_submission() {
        for error in [
            FlussError::StorageBackpressureException,
            FlussError::AuthorizationException,
            FlussError::TableNotExist,
        ] {
            assert!(
                !classify_unknown(api_failure(error))
                    .message()
                    .contains("unknown")
            );
        }
    }

    #[tokio::test]
    async fn routing_answers_only_from_configuration() {
        let backend = backend(&[("zeta", service_cluster()), ("alpha", service_cluster())]);

        assert_eq!(
            backend
                .clusters()
                .iter()
                .map(ClusterId::as_str)
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
        assert!(backend.has_cluster("alpha"));
        for unknown in ["beta", "Not A Cluster", ""] {
            assert!(!backend.has_cluster(unknown), "{unknown:?}");
        }

        let ctx = RequestContext::for_test("beta", Duration::from_secs(5));
        assert_eq!(
            failure(backend.list_databases(&ctx).await),
            (ErrorKind::NotFound, "cluster_not_found")
        );
    }

    #[tokio::test]
    async fn a_dial_failure_reaches_the_caller_classified() {
        let unreachable = ClusterConfig {
            bootstrap_servers: "127.0.0.1:1".to_string(),
            connect_timeout: ConfigDuration::from_millis(200),
            ..service_cluster()
        };
        let illegal = ClusterConfig {
            bootstrap_servers: "not-a-host-port".to_string(),
            ..service_cluster()
        };
        let backend = backend(&[("down", unreachable), ("bad_address", illegal)]);

        for (cluster, expected) in [
            ("down", ErrorKind::Unavailable),
            ("bad_address", ErrorKind::InvalidArgument),
        ] {
            let ctx = RequestContext::for_test(cluster, Duration::from_secs(5));
            assert_eq!(
                failure(backend.list_databases(&ctx).await).0,
                expected,
                "{cluster}"
            );
        }
        backend.close(Duration::from_secs(1)).await.unwrap();
    }

    fn failure<T>(result: GatewayResult<T>) -> (ErrorKind, &'static str) {
        let error = result.err().expect("the call fails");
        (error.kind(), error.code())
    }

    #[tokio::test]
    async fn a_server_that_never_answers_ends_at_the_request_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a bound listener");
        let address = listener.local_addr().expect("a bound address");
        tokio::spawn(async move {
            let mut accepted = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                accepted.push(stream);
            }
        });

        let backend = backend(&[(
            "default",
            ClusterConfig {
                bootstrap_servers: address.to_string(),
                ..service_cluster()
            },
        )]);
        let ctx = RequestContext::for_test("default", Duration::from_millis(300));

        assert_eq!(
            failure(backend.list_databases(&ctx).await).0,
            ErrorKind::DeadlineExceeded
        );
    }
}
