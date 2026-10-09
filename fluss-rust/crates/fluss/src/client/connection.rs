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

use crate::client::WriterClient;
use crate::client::admin::FlussAdmin;
use crate::client::lookup::LookupClient;
use crate::client::metadata::Metadata;
use crate::client::table::FlussTable;
use crate::config::Config;
use crate::error::{Error, FlussError, Result};
use crate::metadata::{PhysicalTablePath, TableInfo, TableOrPartition, TablePath};
use crate::rpc::RpcClient;
use crate::{PartitionId, TableId};
use log::warn;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct FlussConnection {
    metadata: Arc<Metadata>,
    network_connects: Arc<RpcClient>,
    args: Config,
    writer_client: RwLock<Option<Arc<WriterClient>>>,
    admin_client: RwLock<Option<Arc<FlussAdmin>>>,
    lookup_client: RwLock<Option<Arc<LookupClient>>>,
    /// Per table, the bucket-count epoch and the rescaled partitions `get_table` cached.
    rescaled_partitions: Mutex<HashMap<TableId, (i64, Vec<PartitionId>)>>,
}

impl FlussConnection {
    pub async fn new(arg: Config) -> Result<Self> {
        arg.validate_security()
            .map_err(|msg| Error::IllegalArgument { message: msg })?;
        arg.validate_scanner()
            .map_err(|msg| Error::IllegalArgument { message: msg })?;
        arg.validate_writer()
            .map_err(|msg| Error::IllegalArgument { message: msg })?;

        let timeout = Duration::from_millis(arg.connect_timeout_ms);
        // connect_timeout_ms: no lower-bound validation to match Java behavior.
        // Java allows 0 — tracked in https://github.com/apache/fluss/issues/3068
        let connections = if arg.is_sasl_enabled() {
            Arc::new(
                RpcClient::new()
                    .with_sasl(
                        arg.security_sasl_username.clone(),
                        arg.security_sasl_password.clone(),
                    )
                    .with_timeout(timeout),
            )
        } else {
            Arc::new(RpcClient::new().with_timeout(timeout))
        };
        let metadata = Metadata::new(arg.bootstrap_servers.as_str(), connections.clone()).await?;

        Ok(FlussConnection {
            metadata: Arc::new(metadata),
            network_connects: connections.clone(),
            args: arg.clone(),
            writer_client: Default::default(),
            admin_client: RwLock::new(None),
            lookup_client: Default::default(),
            rescaled_partitions: Default::default(),
        })
    }

    /// Gracefully shut down the connection, draining any pending write batches.
    ///
    /// If a writer client has been created, this method will signal it to drain
    /// its buffers and wait for the background sender task to complete, bounded
    /// by the provided timeout.
    pub async fn close(&self, timeout: Duration) -> Result<()> {
        let writer_client = self.writer_client.write().take();
        if let Some(client) = writer_client {
            client.close(timeout).await?;
        }
        Ok(())
    }

    pub fn get_metadata(&self) -> Arc<Metadata> {
        self.metadata.clone()
    }

    pub fn get_connections(&self) -> Arc<RpcClient> {
        self.network_connects.clone()
    }

    pub fn config(&self) -> &Config {
        &self.args
    }

    pub fn get_admin(&self) -> Result<Arc<FlussAdmin>> {
        // 1. Fast path: return cached instance if already initialized.
        if let Some(admin) = self.admin_client.read().as_ref() {
            return Ok(admin.clone());
        }

        // 2. Slow path: acquire write lock.
        let mut admin_guard = self.admin_client.write();

        // 3. Double-check: another thread may have initialized while we waited.
        if let Some(admin) = admin_guard.as_ref() {
            return Ok(admin.clone());
        }

        // 4. Initialize and cache.
        let admin = Arc::new(FlussAdmin::new(
            self.network_connects.clone(),
            self.metadata.clone(),
        ));
        *admin_guard = Some(admin.clone());
        Ok(admin)
    }

    pub fn get_or_create_writer_client(&self) -> Result<Arc<WriterClient>> {
        // 1. Fast path: Attempt to acquire a read lock to check if the client already exists.
        if let Some(client) = self.writer_client.read().as_ref() {
            return Ok(client.clone());
        }

        // 2. Slow path: Acquire the write lock.
        let mut writer_guard = self.writer_client.write();

        // 3. Double-check: Another thread might have initialized the client
        // while this thread was waiting for the write lock.
        if let Some(client) = writer_guard.as_ref() {
            return Ok(client.clone());
        }

        // 4. Initialize the client since we are certain it doesn't exist yet.
        let new_client = Arc::new(WriterClient::new(self.args.clone(), self.metadata.clone())?);

        // 5. Store and return the newly created client.
        *writer_guard = Some(new_client.clone());
        Ok(new_client)
    }

    #[cfg(feature = "integration_tests")]
    pub fn estimated_batch_size_for_table(
        &self,
        table_path: &Arc<PhysicalTablePath>,
    ) -> Option<usize> {
        self.writer_client
            .read()
            .as_ref()
            .and_then(|c| c.estimated_batch_size_for_table(table_path))
    }

    /// Gets or creates a lookup client for batched lookup operations.
    pub fn get_or_create_lookup_client(&self) -> Result<Arc<LookupClient>> {
        // 1. Fast path: Attempt to acquire a read lock to check if the client already exists.
        if let Some(client) = self.lookup_client.read().as_ref() {
            return Ok(client.clone());
        }

        // 2. Slow path: Acquire the write lock.
        let mut lookup_guard = self.lookup_client.write();

        // 3. Double-check: Another thread might have initialized the client
        // while this thread was waiting for the write lock.
        if let Some(client) = lookup_guard.as_ref() {
            return Ok(client.clone());
        }

        // 4. Initialize the client since we are certain it doesn't exist yet.
        let new_client = Arc::new(LookupClient::new(&self.args, self.metadata.clone()));

        // 5. Store and return the newly created client.
        *lookup_guard = Some(new_client.clone());
        Ok(new_client)
    }

    pub async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>> {
        self.metadata.update_table_metadata(table_path).await?;
        let table_info = self
            .metadata
            .get_cluster()
            .get_table(table_path)
            .map_err(|e| {
                if e.api_error() == Some(FlussError::InvalidTableException) {
                    Error::table_not_exist(format!("Table not found: {table_path}"))
                } else {
                    e
                }
            })?
            .clone();
        if table_info.is_partitioned() && table_info.get_bucket_count_epoch() > 0 {
            self.cache_rescaled_partitions(&table_info).await;
        }
        Ok(FlussTable::new(self, self.metadata.clone(), table_info))
    }

    /// Caches partitions whose bucket count differs from the table's, so a writer's first keyed
    /// write to one doesn't fail with `InvalidBucketRouting`. Runs once per `bucket.num` change.
    async fn cache_rescaled_partitions(&self, table_info: &TableInfo) {
        let table_id = table_info.table_id;
        let epoch = table_info.get_bucket_count_epoch();
        let is_cached = |partition_id: &PartitionId| {
            self.metadata
                .get_cluster()
                .bucket_count(TableOrPartition::Partition(*partition_id))
                .is_some()
        };
        if let Some((cached_epoch, partition_ids)) = self.rescaled_partitions.lock().get(&table_id)
        {
            if *cached_epoch == epoch && partition_ids.iter().all(is_cached) {
                return;
            }
        }
        let result = async {
            let table_path = Arc::new(table_info.table_path.clone());
            let rescaled: Vec<(PartitionId, String)> = self
                .get_admin()?
                .list_partition_infos(&table_info.table_path)
                .await?
                .into_iter()
                .filter(|info| info.get_bucket_count() != table_info.num_buckets)
                .map(|info| (info.get_partition_id(), info.get_partition_name()))
                .collect();
            let uncached: Vec<Arc<PhysicalTablePath>> = rescaled
                .iter()
                .filter(|(partition_id, _)| !is_cached(partition_id))
                .map(|(_, partition_name)| {
                    Arc::new(PhysicalTablePath::of_partitioned(
                        table_path.clone(),
                        Some(partition_name.clone()),
                    ))
                })
                .collect();
            if !uncached.is_empty() {
                self.metadata
                    .update_physical_table_metadata(&uncached)
                    .await?;
            }
            Ok::<_, Error>(rescaled.into_iter().map(|(id, _)| id).collect())
        }
        .await;
        match result {
            Ok(partition_ids) => {
                self.rescaled_partitions
                    .lock()
                    .insert(table_id, (epoch, partition_ids));
            }
            Err(e) => warn!(
                "Failed to cache the rescaled partitions of {}: {e}",
                table_info.table_path
            ),
        }
    }
}
