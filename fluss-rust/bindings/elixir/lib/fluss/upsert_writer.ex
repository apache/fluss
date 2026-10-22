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

defmodule Fluss.UpsertWriter do
  @moduledoc """
  Writer for inserting, updating and deleting rows in a primary key (KV) table.

  Built with `Fluss.TableUpsert`, which fixes the written columns for the writer's
  lifetime — so `upsert/2` takes every column for a plain writer, and only the target
  columns for a partial-update writer.

  Values are given as a map keyed by column name (atoms or strings), or as a
  positional list.

      Fluss.UpsertWriter.upsert(writer, %{id: 1, email: "alice@example.com"})
      Fluss.UpsertWriter.upsert(writer, [1, "alice@example.com"])

  In a map, a key that is not one of the writer's columns is an error, and so is naming a
  column twice as an atom and a string (`%{id: 1, "id" => 2}`). An omitted column is
  written as null. A `NOT NULL` column must carry a value — leaving it out and passing
  `nil` are both rejected. Primary keys are always `NOT NULL`.

  A list is positional — table column order, or the order the targets were named for a
  partial-update writer — and `nil` means null, rejected in a `NOT NULL` column as in a
  map. Prefer maps: a list in the wrong order writes to the wrong columns without error.

  `delete/2` takes only the primary key, not a full row. As a list, the key values follow
  the order the primary key was declared in, which can differ from table column order:
  with `primary_key(["b", "a"])`, `delete(writer, [b, a])`.

      Fluss.UpsertWriter.delete(writer, %{id: 1})
      Fluss.UpsertWriter.delete(writer, [1])

  Writes are queued and return a `Fluss.WriteHandle` — drop it for fire-and-forget, or
  `Fluss.WriteHandle.wait/1` for a per-record ack. `flush/1` waits for all queued writes.
  When the write buffer is full, a write waits for room, up to the writer buffer wait
  timeout in `Fluss.Config` (no limit by default).
  """

  alias Fluss.Native

  @type t :: reference()

  @typedoc "Column values as a positional list, or keyed by column name."
  @type values :: [term()] | %{Fluss.TableUpsert.column() => term()}

  @spec upsert(t(), values()) :: {:ok, Fluss.WriteHandle.t()} | {:error, Fluss.Error.t()}
  def upsert(writer, values) when is_list(values) or is_map(values) do
    writer |> Native.upsert_writer_upsert(values) |> Native.await_queued()
  end

  @spec delete(t(), values()) :: {:ok, Fluss.WriteHandle.t()} | {:error, Fluss.Error.t()}
  def delete(writer, primary_key) when is_list(primary_key) or is_map(primary_key) do
    writer |> Native.upsert_writer_delete(primary_key) |> Native.await_queued()
  end

  @spec flush(t()) :: :ok | {:error, Fluss.Error.t()}
  def flush(writer) do
    writer
    |> Native.upsert_writer_flush()
    |> Native.await_nif()
  end
end
