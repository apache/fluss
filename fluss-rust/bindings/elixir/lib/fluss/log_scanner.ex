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

defmodule Fluss.LogScanner do
  @moduledoc """
  Scanner for reading records from a log table.

  `poll/2` is non-blocking — it returns `:ok` immediately and sends results
  as `{:fluss_records, records}` or `{:fluss_poll_error, %Fluss.Error{}}` to
  the calling process. No dirty scheduler threads are held during the wait.

  Each record is an atom-keyed map: `:offset`, `:timestamp`, `:change_type`, `:row`.
  Row values are also atom-keyed (column names interned as atoms).

  ## Examples

      scanner = Fluss.LogScanner.new!(table)
      :ok = Fluss.LogScanner.subscribe(scanner, 0, Fluss.earliest_offset())
      :ok = Fluss.LogScanner.poll(scanner, 5_000)

      receive do
        {:fluss_records, records} ->
          for record <- records, do: IO.inspect(record[:row])
        {:fluss_poll_error, %Fluss.Error{code: code, message: msg}} ->
          IO.puts("poll error [\#{code}]: \#{msg}")
      end

  """

  alias Fluss.Native

  @type t :: reference() | {:consumer, reference()}
  @type bucket :: {integer(), integer() | nil, non_neg_integer()}
  @type saved_offset :: {non_neg_integer(), String.t() | nil}
  @type record :: %{atom() => term()}

  @spec new(Fluss.Table.t()) :: {:ok, t()} | {:error, Fluss.Error.t()}
  def new(table) do
    case Native.log_scanner_new(table) do
      {:error, _} = err -> err
      s -> {:ok, s}
    end
  end

  @doc """
  Creates a consumer scanner. Options: required `:group_id`, `:timeout_ms` (30_000),
  and optional `:auto_commit_interval_ms`. Auto-commit runs before the next poll;
  process the previous result completely first. Drop never commits.
  """
  def new(table, opts) do
    opts = Keyword.validate!(opts, [:group_id, :timeout_ms, :auto_commit_interval_ms])

    Native.consumer_scanner_new(
      table,
      Keyword.fetch!(opts, :group_id),
      Keyword.get(opts, :timeout_ms, 30_000),
      Keyword.get(opts, :auto_commit_interval_ms)
    )
    |> Native.await_nif()
    |> wrap_consumer()
  end

  @doc "Creates a scanner over `{database, table}` pairs, sharing one group and commit queue."
  def new_multi(conn, tables, opts) do
    opts = Keyword.validate!(opts, [:group_id, :timeout_ms, :auto_commit_interval_ms])

    Native.consumer_scanner_multi_new(
      conn,
      tables,
      Keyword.fetch!(opts, :group_id),
      Keyword.get(opts, :timeout_ms, 30_000),
      Keyword.get(opts, :auto_commit_interval_ms)
    )
    |> Native.await_nif()
    |> wrap_consumer()
  end

  defp wrap_consumer({:ok, ref}), do: {:ok, {:consumer, ref}}
  defp wrap_consumer(error), do: error

  @doc """
  Buckets are `{table_id, partition_id_or_nil, bucket_id}` tuples. Initializers are
  `:earliest`, `:latest`, `:fail`, `{:offset, n}`, `{:timestamp, ms}`,
  `{:committed, group}` or `{:committed, group, fallback}`. Fallbacks only resolve
  missing commits; authorization errors never select a fallback.
  """
  def subscribe_with_initializer({:consumer, scanner}, buckets, initializer) do
    Native.consumer_scanner_subscribe(scanner, buckets, initializer_steps(initializer))
    |> Native.await_nif()
  end

  defp initializer_steps({:committed, group}), do: [{:committed, group}, :earliest]

  defp initializer_steps({:committed, group, fallback}),
    do: [{:committed, group} | initializer_steps(fallback)]

  defp initializer_steps(value), do: [value]

  @doc """
  Captures offsets immediately and returns a reference; receive `{ref, {:ok, result}}`.
  Inspect `result.request.status` and every `result.buckets` entry. Status is
  `:success`, `:rejected`, `:unknown`, or `:needs_recovery`. Offsets are a list of
  `{bucket, {next_offset, metadata_or_nil}}`, or nil for all delivered positions.
  Not receiving the reply does not cancel the ordered commit.
  """
  def commit_async({:consumer, scanner}, offsets \\ nil),
    do: Native.consumer_scanner_commit(scanner, offsets)

  @doc "Waits for the ordered commit. After processing the last batch, call this before shutdown."
  def commit_sync(scanner, offsets \\ nil),
    do: commit_async(scanner, offsets) |> Native.await_nif()

  @spec new!(Fluss.Table.t()) :: t()
  def new!(table) do
    case new(table) do
      {:ok, s} -> s
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec subscribe(reference(), integer(), integer()) :: :ok | {:error, Fluss.Error.t()}
  def subscribe(scanner, bucket, offset) do
    scanner
    |> Native.log_scanner_subscribe(bucket, offset)
    |> Native.await_nif()
  end

  @doc """
  Subscribes to multiple buckets. Takes a list of `{bucket_id, offset}` tuples.
  """
  @spec subscribe_buckets(reference(), [{integer(), integer()}]) ::
          :ok | {:error, Fluss.Error.t()}
  def subscribe_buckets(scanner, bucket_offsets) when is_list(bucket_offsets) do
    scanner
    |> Native.log_scanner_subscribe_buckets(bucket_offsets)
    |> Native.await_nif()
  end

  @spec unsubscribe(t(), integer() | bucket()) :: :ok | {:error, Fluss.Error.t()}
  def unsubscribe({:consumer, scanner}, bucket),
    do: Native.consumer_scanner_unsubscribe(scanner, bucket) |> Native.await_nif()

  def unsubscribe(scanner, bucket) do
    scanner
    |> Native.log_scanner_unsubscribe(bucket)
    |> Native.await_nif()
  end

  @doc """
  Starts a non-blocking poll. Returns `:ok` immediately.
  Results arrive as `{:fluss_records, [record]}` or
  `{:fluss_poll_error, %Fluss.Error{}}`.
  """
  @spec poll(t(), non_neg_integer()) :: :ok
  def poll({:consumer, scanner}, timeout_ms),
    do: Native.consumer_scanner_poll(scanner, timeout_ms)

  def poll(scanner, timeout_ms),
    do: Native.log_scanner_poll(scanner, timeout_ms)
end
