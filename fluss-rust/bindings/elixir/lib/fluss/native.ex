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

defmodule Fluss.Native do
  @moduledoc false
  # Release only when packaging for prod; debug keeps dev/test compiles fast.
  use Rustler,
    otp_app: :fluss,
    crate: "fluss_nif",
    mode: if(Mix.env() == :prod, do: :release, else: :debug)

  # Connection
  def connection_new(_config), do: :erlang.nif_error(:nif_not_loaded)

  # Admin
  def admin_new(_conn), do: :erlang.nif_error(:nif_not_loaded)

  def admin_get_server_nodes(_admin), do: :erlang.nif_error(:nif_not_loaded)

  def admin_create_database(_admin, _name, _descriptor, _ignore_if_exists),
    do: :erlang.nif_error(:nif_not_loaded)

  def admin_get_database_info(_admin, _database_name), do: :erlang.nif_error(:nif_not_loaded)

  def admin_drop_database(_admin, _name, _ignore_if_not_exists),
    do: :erlang.nif_error(:nif_not_loaded)

  def admin_list_databases(_admin), do: :erlang.nif_error(:nif_not_loaded)

  def admin_database_exists(_admin, _database_name), do: :erlang.nif_error(:nif_not_loaded)

  def admin_create_table(_admin, _db, _table, _descriptor, _ignore_if_exists),
    do: :erlang.nif_error(:nif_not_loaded)

  def admin_drop_table(_admin, _db, _table, _ignore_if_not_exists),
    do: :erlang.nif_error(:nif_not_loaded)

  def admin_list_tables(_admin, _database), do: :erlang.nif_error(:nif_not_loaded)

  def admin_table_exists(_admin, _database_name, _table_name),
    do: :erlang.nif_error(:nif_not_loaded)

  def admin_get_table_info(_admin, _database_name, _table_name),
    do: :erlang.nif_error(:nif_not_loaded)

  def admin_get_table_schema(_admin, _database_name, _table_name, _schema_id),
    do: :erlang.nif_error(:nif_not_loaded)

  # Schema / TableDescriptor
  def table_descriptor_new(_schema, _options),
    do: :erlang.nif_error(:nif_not_loaded)

  # Table
  def table_get(_conn, _database, _table), do: :erlang.nif_error(:nif_not_loaded)
  def table_has_primary_key(_table), do: :erlang.nif_error(:nif_not_loaded)
  def table_column_names(_table), do: :erlang.nif_error(:nif_not_loaded)

  # AppendWriter
  def append_writer_new(_table), do: :erlang.nif_error(:nif_not_loaded)
  def append_writer_append(_writer, _values), do: :erlang.nif_error(:nif_not_loaded)
  def append_writer_flush(_writer), do: :erlang.nif_error(:nif_not_loaded)

  # TableUpsert
  def table_upsert_new(_table), do: :erlang.nif_error(:nif_not_loaded)

  def table_upsert_partial_update_by_name(_table_upsert, _column_names),
    do: :erlang.nif_error(:nif_not_loaded)

  def table_upsert_partial_update_by_index(_table_upsert, _column_indices),
    do: :erlang.nif_error(:nif_not_loaded)

  def table_upsert_create_writer(_table_upsert), do: :erlang.nif_error(:nif_not_loaded)

  # UpsertWriter
  def upsert_writer_upsert(_writer, _values), do: :erlang.nif_error(:nif_not_loaded)
  def upsert_writer_delete(_writer, _primary_key), do: :erlang.nif_error(:nif_not_loaded)
  def upsert_writer_flush(_writer), do: :erlang.nif_error(:nif_not_loaded)

  # Lookuper
  def lookuper_new(_table), do: :erlang.nif_error(:nif_not_loaded)
  def lookuper_lookup(_lookuper, _key), do: :erlang.nif_error(:nif_not_loaded)

  # PrefixLookuper
  def prefix_lookuper_new(_table, _columns), do: :erlang.nif_error(:nif_not_loaded)
  def prefix_lookuper_lookup(_lookuper, _prefix), do: :erlang.nif_error(:nif_not_loaded)

  # LogScanner
  def log_scanner_new(_table), do: :erlang.nif_error(:nif_not_loaded)
  def log_scanner_subscribe(_scanner, _bucket, _offset), do: :erlang.nif_error(:nif_not_loaded)

  def log_scanner_subscribe_buckets(_scanner, _bucket_offsets),
    do: :erlang.nif_error(:nif_not_loaded)

  def log_scanner_unsubscribe(_scanner, _bucket), do: :erlang.nif_error(:nif_not_loaded)
  def log_scanner_poll(_scanner, _timeout_ms), do: :erlang.nif_error(:nif_not_loaded)

  # WriteHandle
  def write_handle_wait(_handle), do: :erlang.nif_error(:nif_not_loaded)

  # Cancellable tasks
  def task_cancel(_task), do: :erlang.nif_error(:nif_not_loaded)

  # Constants
  def earliest_offset, do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  def await_nif(ref) do
    receive do
      {^ref, result} -> result
    end
  end

  # With the write buffer full, a write waits for room off the scheduler and its handle
  # arrives as a message.
  @doc false
  def await_queued({:pending, ref}), do: await_nif(ref)
  def await_queued({:error, _} = err), do: err
  def await_queued(handle), do: {:ok, handle}

  # Waits for a cancellable task's reply. On timeout it cancels the task, but a reply that
  # raced in still wins, as with `Task.shutdown/1`.
  @doc false
  def await_task({:error, _} = err, _timeout), do: err

  def await_task({ref, task}, timeout) do
    receive do
      {^ref, result} -> result
    after
      timeout ->
        case task_cancel(task) do
          :cancelled ->
            {:error, timeout_error(timeout)}

          :replying ->
            await_nif(ref)
        end
    end
  end

  @doc false
  def validate_timeout!(opts) do
    # `receive ... after` accepts at most 2^32 - 1 ms.
    case Keyword.validate!(opts, timeout: 5_000)[:timeout] do
      t when t in 0..0xFFFFFFFF or t == :infinity ->
        t

      t ->
        raise ArgumentError,
              "expected :timeout to be 0..4294967295 ms or :infinity, got: #{inspect(t)}"
    end
  end

  # 25 is core's RequestTimeOut code.
  defp timeout_error(timeout) do
    %Fluss.Error{
      code: :request_time_out,
      error_code: 25,
      message: "no reply within #{timeout} ms"
    }
  end
end
