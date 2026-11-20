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

defmodule Fluss.PrefixLookuper do
  @moduledoc """
  Reads every row whose primary key starts with a given prefix, from a primary key (KV) table.

      # primary key (tenant_id, id), bucket key tenant_id
      lookuper = Fluss.PrefixLookuper.new!(table, [:tenant_id])

      {:ok, [%{tenant_id: 7, id: 1, ...}, %{tenant_id: 7, id: 2, ...}]} =
        Fluss.PrefixLookuper.lookup(lookuper, %{tenant_id: 7})

  Prefix columns (atoms or strings) are the partition columns (if any), then the bucket key,
  which must be a leading part of the primary key (minus partition columns) but not all of
  it; `new/2` returns an error otherwise. The prefix is a map, or a list in `new/2` order;
  `nil` is an error.

  Rows, batching and timeouts are as in `Fluss.Lookuper`; no match is `[]`, from `lookup!/3`
  too.
  """

  alias Fluss.Native

  @type t :: reference()

  @type column :: atom() | String.t()

  @typedoc "Prefix values as a list in prefix-column order, or keyed by column name."
  @type prefix :: [term()] | %{column() => term()}

  @spec new(Fluss.Table.t(), [column()]) :: {:ok, t()} | {:error, Fluss.Error.t()}
  def new(table, columns) when is_list(columns) do
    case Native.prefix_lookuper_new(table, columns) do
      {:error, _} = err -> err
      lookuper -> {:ok, lookuper}
    end
  end

  @spec new!(Fluss.Table.t(), [column()]) :: t()
  def new!(table, columns) do
    case new(table, columns) do
      {:ok, lookuper} -> lookuper
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec lookup(t(), prefix(), keyword()) ::
          {:ok, [Fluss.Lookuper.row()]} | {:error, Fluss.Error.t()}
  def lookup(lookuper, prefix, opts \\ []) when is_list(prefix) or is_map(prefix) do
    timeout = Native.validate_timeout!(opts)
    lookuper |> Native.prefix_lookuper_lookup(prefix) |> Native.await_task(timeout)
  end

  @spec lookup!(t(), prefix(), keyword()) :: [Fluss.Lookuper.row()]
  def lookup!(lookuper, prefix, opts \\ []) do
    case lookup(lookuper, prefix, opts) do
      {:ok, rows} -> rows
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end
end
