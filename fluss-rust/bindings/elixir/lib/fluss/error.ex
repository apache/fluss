# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

defmodule Fluss.Error do
  @moduledoc """
  Structured error returned from Fluss operations.

  Fields:

    * `:code` — stable atom for pattern matching.
    * `:error_code` — raw integer code. Protocol codes `0..57`, `-1` for
      `:unknown_server_error`, `-2` for `:client_error`.
    * `:message` — human-readable description.

  Also an exception, so `raise err` works.

  `:client_error` covers any failure that didn't come from the server API
  (bad input, transport, I/O, decode, consumed write handle, etc.) and is
  not retriable, matching the Python and C++ bindings.
  """

  defexception [:code, :error_code, :message]

  @typedoc "Error code atom."
  @type code ::
          :none
          | :unknown_server_error
          | :network_exception
          | :unsupported_version
          | :corrupt_message
          | :database_not_exist
          | :database_not_empty
          | :database_already_exist
          | :table_not_exist
          | :table_already_exist
          | :schema_not_exist
          | :log_storage_exception
          | :kv_storage_exception
          | :not_leader_or_follower
          | :record_too_large_exception
          | :corrupt_record_exception
          | :invalid_table_exception
          | :invalid_database_exception
          | :invalid_replication_factor
          | :invalid_required_acks
          | :log_offset_out_of_range_exception
          | :non_primary_key_table_exception
          | :unknown_table_or_bucket_exception
          | :invalid_update_version_exception
          | :invalid_coordinator_exception
          | :fenced_leader_epoch_exception
          | :request_time_out
          | :storage_exception
          | :operation_not_attempted_exception
          | :not_enough_replicas_after_append_exception
          | :not_enough_replicas_exception
          | :security_token_exception
          | :out_of_order_sequence_exception
          | :duplicate_sequence_exception
          | :unknown_writer_id_exception
          | :invalid_column_projection
          | :invalid_target_column
          | :partition_not_exists
          | :table_not_partitioned_exception
          | :invalid_timestamp_exception
          | :invalid_config_exception
          | :lake_storage_not_configured_exception
          | :kv_snapshot_not_exist
          | :partition_already_exists
          | :partition_spec_invalid_exception
          | :leader_not_available_exception
          | :partition_max_num_exception
          | :authenticate_exception
          | :security_disabled_exception
          | :authorization_exception
          | :bucket_max_num_exception
          | :fenced_tiering_epoch_exception
          | :retriable_authenticate_exception
          | :invalid_server_rack_info_exception
          | :lake_snapshot_not_exist
          | :lake_table_already_exist
          | :ineligible_replica_exception
          | :invalid_alter_table_exception
          | :deletion_disabled_exception
          | :client_error

  @type t :: %__MODULE__{code: code(), error_code: integer(), message: String.t()}

  @retriable_codes [
    :network_exception,
    :corrupt_message,
    :schema_not_exist,
    :log_storage_exception,
    :kv_storage_exception,
    :not_leader_or_follower,
    :corrupt_record_exception,
    :unknown_table_or_bucket_exception,
    :request_time_out,
    :storage_exception,
    :not_enough_replicas_after_append_exception,
    :not_enough_replicas_exception,
    :leader_not_available_exception
  ]

  @impl true
  def message(%__MODULE__{code: code, message: msg}) do
    "Fluss error [#{code}]: #{msg}"
  end

  @doc "Returns `true` if retrying the operation may succeed."
  @spec retriable?(t()) :: boolean()
  def retriable?(%__MODULE__{code: code}), do: code in @retriable_codes
end
