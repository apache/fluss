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

use fluss::error::{Error as CoreError, FlussError};
use rustler::{Atom, NifStruct};

rustler::atoms! {
    ok,
    error,
    nil,

    // Change types
    append_only,
    insert,
    update_before,
    update_after,
    delete,

    // Poll result message tags
    fluss_records,
    fluss_poll_error,

    // Record map keys
    offset,
    timestamp,
    change_type,
    row,

    // Error code atoms (mirror of fluss::error::FlussError).
    none,
    unknown_server_error,
    network_exception,
    unsupported_version,
    corrupt_message,
    database_not_exist,
    database_not_empty,
    database_already_exist,
    table_not_exist,
    table_already_exist,
    schema_not_exist,
    log_storage_exception,
    kv_storage_exception,
    not_leader_or_follower,
    record_too_large_exception,
    corrupt_record_exception,
    invalid_table_exception,
    invalid_database_exception,
    invalid_replication_factor,
    invalid_required_acks,
    log_offset_out_of_range_exception,
    non_primary_key_table_exception,
    unknown_table_or_bucket_exception,
    invalid_update_version_exception,
    invalid_coordinator_exception,
    fenced_leader_epoch_exception,
    request_time_out,
    storage_exception,
    operation_not_attempted_exception,
    not_enough_replicas_after_append_exception,
    not_enough_replicas_exception,
    security_token_exception,
    out_of_order_sequence_exception,
    duplicate_sequence_exception,
    unknown_writer_id_exception,
    invalid_column_projection,
    invalid_target_column,
    partition_not_exists,
    table_not_partitioned_exception,
    invalid_timestamp_exception,
    invalid_config_exception,
    lake_storage_not_configured_exception,
    kv_snapshot_not_exist,
    partition_already_exists,
    partition_spec_invalid_exception,
    leader_not_available_exception,
    partition_max_num_exception,
    authenticate_exception,
    security_disabled_exception,
    authorization_exception,
    bucket_max_num_exception,
    fenced_tiering_epoch_exception,
    retriable_authenticate_exception,
    invalid_server_rack_info_exception,
    lake_snapshot_not_exist,
    lake_table_already_exist,
    ineligible_replica_exception,
    invalid_alter_table_exception,
    deletion_disabled_exception,
    client_error,
}

pub const CLIENT_ERROR_CODE: i32 = -2;

// `__exception__` is the marker `defexception` sets. Rustler bypasses the
// Elixir constructor, so we must serialize it explicitly or `raise err`
// rejects the struct at the Elixir side.
#[derive(NifStruct)]
#[module = "Fluss.Error"]
pub struct NifFlussError {
    pub code: Atom,
    pub error_code: i32,
    pub message: String,
    #[allow(non_snake_case)]
    pub __exception__: bool,
}

impl NifFlussError {
    pub fn from_core(error: &CoreError) -> Self {
        // Transport failures map to `:network_exception` (Java parity,
        // retriable).
        let (code, error_code) = match error {
            CoreError::FlussAPIError { api_error } => {
                (api_error_atom(api_error.code), api_error.code)
            }
            CoreError::RpcError { .. } => {
                (network_exception(), FlussError::NetworkException.code())
            }
            _ => (client_error(), CLIENT_ERROR_CODE),
        };
        Self {
            code,
            error_code,
            message: error.to_string(),
            __exception__: true,
        }
    }

    pub fn client(message: String) -> Self {
        Self {
            code: client_error(),
            error_code: CLIENT_ERROR_CODE,
            message,
            __exception__: true,
        }
    }
}

fn api_error_atom(code: i32) -> Atom {
    match FlussError::for_code(code) {
        FlussError::UnknownServerError => unknown_server_error(),
        FlussError::None => none(),
        FlussError::NetworkException => network_exception(),
        FlussError::UnsupportedVersion => unsupported_version(),
        FlussError::CorruptMessage => corrupt_message(),
        FlussError::DatabaseNotExist => database_not_exist(),
        FlussError::DatabaseNotEmpty => database_not_empty(),
        FlussError::DatabaseAlreadyExist => database_already_exist(),
        FlussError::TableNotExist => table_not_exist(),
        FlussError::TableAlreadyExist => table_already_exist(),
        FlussError::SchemaNotExist => schema_not_exist(),
        FlussError::LogStorageException => log_storage_exception(),
        FlussError::KvStorageException => kv_storage_exception(),
        FlussError::NotLeaderOrFollower => not_leader_or_follower(),
        FlussError::RecordTooLargeException => record_too_large_exception(),
        FlussError::CorruptRecordException => corrupt_record_exception(),
        FlussError::InvalidTableException => invalid_table_exception(),
        FlussError::InvalidDatabaseException => invalid_database_exception(),
        FlussError::InvalidReplicationFactor => invalid_replication_factor(),
        FlussError::InvalidRequiredAcks => invalid_required_acks(),
        FlussError::LogOffsetOutOfRangeException => log_offset_out_of_range_exception(),
        FlussError::NonPrimaryKeyTableException => non_primary_key_table_exception(),
        FlussError::UnknownTableOrBucketException => unknown_table_or_bucket_exception(),
        FlussError::InvalidUpdateVersionException => invalid_update_version_exception(),
        FlussError::InvalidCoordinatorException => invalid_coordinator_exception(),
        FlussError::FencedLeaderEpochException => fenced_leader_epoch_exception(),
        FlussError::RequestTimeOut => request_time_out(),
        FlussError::StorageException => storage_exception(),
        FlussError::OperationNotAttemptedException => operation_not_attempted_exception(),
        FlussError::NotEnoughReplicasAfterAppendException => {
            not_enough_replicas_after_append_exception()
        }
        FlussError::NotEnoughReplicasException => not_enough_replicas_exception(),
        FlussError::SecurityTokenException => security_token_exception(),
        FlussError::OutOfOrderSequenceException => out_of_order_sequence_exception(),
        FlussError::DuplicateSequenceException => duplicate_sequence_exception(),
        FlussError::UnknownWriterIdException => unknown_writer_id_exception(),
        FlussError::InvalidColumnProjection => invalid_column_projection(),
        FlussError::InvalidTargetColumn => invalid_target_column(),
        FlussError::PartitionNotExists => partition_not_exists(),
        FlussError::TableNotPartitionedException => table_not_partitioned_exception(),
        FlussError::InvalidTimestampException => invalid_timestamp_exception(),
        FlussError::InvalidConfigException => invalid_config_exception(),
        FlussError::LakeStorageNotConfiguredException => lake_storage_not_configured_exception(),
        FlussError::KvSnapshotNotExist => kv_snapshot_not_exist(),
        FlussError::PartitionAlreadyExists => partition_already_exists(),
        FlussError::PartitionSpecInvalidException => partition_spec_invalid_exception(),
        FlussError::LeaderNotAvailableException => leader_not_available_exception(),
        FlussError::PartitionMaxNumException => partition_max_num_exception(),
        FlussError::AuthenticateException => authenticate_exception(),
        FlussError::SecurityDisabledException => security_disabled_exception(),
        FlussError::AuthorizationException => authorization_exception(),
        FlussError::BucketMaxNumException => bucket_max_num_exception(),
        FlussError::FencedTieringEpochException => fenced_tiering_epoch_exception(),
        FlussError::RetriableAuthenticateException => retriable_authenticate_exception(),
        FlussError::InvalidServerRackInfoException => invalid_server_rack_info_exception(),
        FlussError::LakeSnapshotNotExist => lake_snapshot_not_exist(),
        FlussError::LakeTableAlreadyExist => lake_table_already_exist(),
        FlussError::IneligibleReplicaException => ineligible_replica_exception(),
        FlussError::InvalidAlterTableException => invalid_alter_table_exception(),
        FlussError::DeletionDisabledException => deletion_disabled_exception(),
    }
}

pub fn to_nif_err(e: CoreError) -> rustler::Error {
    rustler::Error::Term(Box::new(NifFlussError::from_core(&e)))
}

pub fn client_err(msg: impl Into<String>) -> rustler::Error {
    rustler::Error::Term(Box::new(NifFlussError::client(msg.into())))
}
