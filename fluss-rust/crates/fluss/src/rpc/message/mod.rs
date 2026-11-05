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

use crate::error::Error;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};
use bytes::{Buf, BufMut};

mod acquire_kv_snapshot_lease;
mod add_server_tag;
mod alter_cluster_configs;
mod alter_database;
mod alter_table;
mod api_versions;
mod authenticate;
mod cancel_rebalance;
mod create_acls;
mod create_database;
mod create_partition;
mod create_table;
mod database_exists;
mod delete_producer_offsets;
mod describe_cluster_configs;
mod drop_acls;
mod drop_database;
mod drop_kv_snapshot_lease;
mod drop_partition;
mod drop_table;
mod fetch;
mod get_cluster_health;
mod get_database_info;
mod get_kv_snapshot_metadata;
mod get_lake_snapshot;
mod get_latest_kv_snapshots;
mod get_latest_lake_snapshot;
mod get_producer_offsets;
mod get_security_token;
mod get_table;
mod get_table_schema;
mod get_table_stats;
mod header;
mod init_writer;
mod limit_scan;
mod list_acls;
mod list_database_summaries;
mod list_databases;
mod list_kv_snapshots;
mod list_offsets;
mod list_partition_infos;
mod list_rebalance_progress;
mod list_remote_log_manifests;
mod list_tables;
mod lookup;
mod prefix_lookup;
mod produce_log;
mod put_kv;
mod rebalance;
mod register_producer_offsets;
mod release_kv_snapshot_lease;
mod remove_server_tag;
mod scan_kv;
mod table_exists;
mod update_metadata;

pub(crate) use crate::rpc::RpcError;
pub(crate) use acquire_kv_snapshot_lease::*;
pub(crate) use add_server_tag::*;
pub(crate) use alter_cluster_configs::*;
pub(crate) use alter_database::*;
pub(crate) use alter_table::*;
pub(crate) use api_versions::*;
pub(crate) use authenticate::*;
pub(crate) use cancel_rebalance::*;
pub(crate) use create_acls::*;
pub(crate) use create_database::*;
pub(crate) use create_partition::*;
pub(crate) use create_table::*;
pub(crate) use database_exists::*;
pub(crate) use delete_producer_offsets::*;
pub(crate) use describe_cluster_configs::*;
pub(crate) use drop_acls::*;
pub(crate) use drop_database::*;
pub(crate) use drop_kv_snapshot_lease::*;
pub(crate) use drop_partition::*;
pub(crate) use drop_table::*;
pub(crate) use fetch::*;
pub(crate) use get_cluster_health::*;
pub(crate) use get_database_info::*;
pub(crate) use get_kv_snapshot_metadata::*;
pub(crate) use get_lake_snapshot::*;
pub(crate) use get_latest_kv_snapshots::*;
pub(crate) use get_latest_lake_snapshot::*;
pub(crate) use get_producer_offsets::*;
pub(crate) use get_security_token::*;
pub(crate) use get_table::*;
pub(crate) use get_table_schema::*;
pub(crate) use get_table_stats::*;
pub(crate) use header::*;
pub(crate) use init_writer::*;
pub(crate) use limit_scan::*;
pub(crate) use list_acls::*;
pub(crate) use list_database_summaries::*;
pub(crate) use list_databases::*;
pub(crate) use list_kv_snapshots::*;
pub use list_offsets::OffsetSpec;
pub(crate) use list_offsets::*;
pub(crate) use list_partition_infos::*;
pub(crate) use list_rebalance_progress::*;
pub(crate) use list_remote_log_manifests::*;
pub(crate) use list_tables::*;
pub(crate) use lookup::*;
pub(crate) use prefix_lookup::*;
pub(crate) use produce_log::*;
pub(crate) use put_kv::*;
pub(crate) use rebalance::*;
pub(crate) use register_producer_offsets::*;
pub(crate) use release_kv_snapshot_lease::*;
pub(crate) use remove_server_tag::*;
pub(crate) use scan_kv::*;
pub(crate) use table_exists::*;
pub(crate) use update_metadata::*;

pub(crate) trait RequestBody {
    type ResponseBody;

    const API_KEY: ApiKey;

    /// Fails when this request needs something the negotiated `version` lacks.
    fn check_version(&self, _version: ApiVersion) -> Result<(), Error> {
        Ok(())
    }
}

impl<T: RequestBody> RequestBody for &T {
    type ResponseBody = T::ResponseBody;

    const API_KEY: ApiKey = T::API_KEY;

    fn check_version(&self, version: ApiVersion) -> Result<(), Error> {
        (**self).check_version(version)
    }
}

pub(crate) trait WriteType<W>: Sized
where
    W: BufMut,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError>;
}

pub(crate) trait ReadType<R>: Sized
where
    R: Buf,
{
    fn read(reader: &mut R) -> Result<Self, ReadError>;
}

#[macro_export]
macro_rules! impl_write_type {
    ($type:ty) => {
        impl<W> WriteType<W> for $type
        where
            W: BufMut,
        {
            fn write(&self, writer: &mut W) -> Result<(), WriteError> {
                Ok(self.inner_request.encode(writer).unwrap())
            }
        }
    };
}

#[macro_export]
macro_rules! impl_read_type {
    ($type:ty) => {
        impl<R> ReadType<R> for $type
        where
            R: Buf,
        {
            fn read(reader: &mut R) -> Result<Self, ReadError> {
                Ok(<$type>::decode(reader).unwrap())
            }
        }
    };
}
