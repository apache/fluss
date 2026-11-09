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

defmodule Fluss.TableUpsert do
  @moduledoc """
  Builder for a `Fluss.UpsertWriter` on a primary key (KV) table.

      table |> Fluss.TableUpsert.new!() |> Fluss.TableUpsert.create_writer!()

  `new!/1` fails on a table without a primary key.

  ## Partial update

  Naming target columns restricts the writer to those columns; the rest keep their
  current value. Columns may be named by name or by zero-based index — names may be
  atoms or strings.

      table
      |> Fluss.TableUpsert.new!()
      |> Fluss.TableUpsert.partial_update_by_name!([:id, :email])
      |> Fluss.TableUpsert.create_writer!()

  The targets must include the primary key, and every non-primary-key column in the
  table must be nullable — including ones the writer does not touch.

  Naming a column twice is an error, including the same column as an atom and a string
  (`[:id, "id"]`).

  The primary key and nullability rules are checked in `create_writer/1`, not in the
  `partial_update_*` call that caused the violation.

  Each step returns a new builder, so one builder can produce several writers.
  """

  alias Fluss.Native

  @type t :: reference()

  @type column :: atom() | String.t()

  @spec new(Fluss.Table.t()) :: {:ok, t()} | {:error, Fluss.Error.t()}
  def new(table) do
    case Native.table_upsert_new(table) do
      {:error, _} = err -> err
      builder -> {:ok, builder}
    end
  end

  @spec new!(Fluss.Table.t()) :: t()
  def new!(table) do
    case new(table) do
      {:ok, builder} -> builder
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec partial_update_by_name(t(), [column()]) :: {:ok, t()} | {:error, Fluss.Error.t()}
  def partial_update_by_name(table_upsert, column_names) when is_list(column_names) do
    case Native.table_upsert_partial_update_by_name(table_upsert, column_names) do
      {:error, _} = err -> err
      builder -> {:ok, builder}
    end
  end

  @spec partial_update_by_name!(t(), [column()]) :: t()
  def partial_update_by_name!(table_upsert, column_names) when is_list(column_names) do
    case partial_update_by_name(table_upsert, column_names) do
      {:ok, builder} -> builder
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec partial_update_by_index(t(), [non_neg_integer()]) ::
          {:ok, t()} | {:error, Fluss.Error.t()}
  def partial_update_by_index(table_upsert, column_indices) when is_list(column_indices) do
    case Native.table_upsert_partial_update_by_index(table_upsert, column_indices) do
      {:error, _} = err -> err
      builder -> {:ok, builder}
    end
  end

  @spec partial_update_by_index!(t(), [non_neg_integer()]) :: t()
  def partial_update_by_index!(table_upsert, column_indices) when is_list(column_indices) do
    case partial_update_by_index(table_upsert, column_indices) do
      {:ok, builder} -> builder
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec create_writer(t()) :: {:ok, Fluss.UpsertWriter.t()} | {:error, Fluss.Error.t()}
  def create_writer(table_upsert) do
    case Native.table_upsert_create_writer(table_upsert) do
      {:error, _} = err -> err
      writer -> {:ok, writer}
    end
  end

  @spec create_writer!(t()) :: Fluss.UpsertWriter.t()
  def create_writer!(table_upsert) do
    case create_writer(table_upsert) do
      {:ok, writer} -> writer
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end
end
