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

defmodule Fluss.Lookuper do
  @moduledoc """
  Reads a row by primary key from a primary key (KV) table.

      lookuper = Fluss.Lookuper.new!(table)

      {:ok, %{id: 1, name: "alice"}} = Fluss.Lookuper.lookup(lookuper, %{id: 1})
      {:ok, %{id: 1, name: "alice"}} = Fluss.Lookuper.lookup(lookuper, [1])
      {:ok, nil} = Fluss.Lookuper.lookup(lookuper, [404])

  The key is a map keyed by column name (atoms or strings), or a list in primary key
  declaration order, as in `Fluss.UpsertWriter.delete/2`. `nil` and non-key map keys are errors.

  Rows are maps keyed by column-name atoms, with the columns `table` had when it was fetched
  with `Fluss.Table.get/3`. `lookup!/3` returns the row or raises `Fluss.NoResultsError`,
  like `Repo.get!/2`.

  Share one lookuper per table across processes. Lookups are batched, so a lone one waits up
  to the lookup batch timeout in `Fluss.Config` (100 ms by default). On a partitioned table
  the key includes the partition columns; a missing partition returns `{:ok, nil}`.

  `lookup/3` waits up to `timeout:` ms (default `5_000`, or `:infinity`), like `Task.await/2`.
  On this client-side timeout the lookup is cancelled and
  `{:error, %Fluss.Error{code: :request_time_out}}` returned; a lookup whose caller exits is
  cancelled too.
  """

  alias Fluss.Native

  @type t :: reference()

  @typedoc "Primary key values as a list in key order, or keyed by column name."
  @type key :: [term()] | %{(atom() | String.t()) => term()}

  @typedoc "A row keyed by column-name atoms."
  @type row :: %{atom() => term()}

  @spec new(Fluss.Table.t()) :: {:ok, t()} | {:error, Fluss.Error.t()}
  def new(table) do
    case Native.lookuper_new(table) do
      {:error, _} = err -> err
      lookuper -> {:ok, lookuper}
    end
  end

  @spec new!(Fluss.Table.t()) :: t()
  def new!(table) do
    case new(table) do
      {:ok, lookuper} -> lookuper
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec lookup(t(), key(), keyword()) :: {:ok, row() | nil} | {:error, Fluss.Error.t()}
  def lookup(lookuper, key, opts \\ []) when is_list(key) or is_map(key) do
    timeout = Native.validate_timeout!(opts)
    lookuper |> Native.lookuper_lookup(key) |> Native.await_task(timeout)
  end

  @spec lookup!(t(), key(), keyword()) :: row()
  def lookup!(lookuper, key, opts \\ []) do
    case lookup(lookuper, key, opts) do
      {:ok, nil} -> raise Fluss.NoResultsError, key: key
      {:ok, row} -> row
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end
end
