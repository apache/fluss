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

defmodule Fluss.Schema do
  @moduledoc """
  Schema definition for a Fluss table.

  Simple types: `:boolean`, `:tinyint`, `:smallint`, `:int`, `:bigint`,
  `:float`, `:double`, `:string`, `:bytes`, `:date`, `:time`, `:timestamp`, `:timestamp_ltz`

  Parameterized types: `{:decimal, precision, scale}`, `{:char, length}`, `{:binary, length}`

  Complex types: `{:array, dt}`, `{:map, key_dt, value_dt}`, `{:row, [{name, dt}]}`
  (e.g. `{:row, [{"x", :int}, {"y", :string}]}`).

  Nullability: types are nullable by default; wrap as `{:not_null, dt}` to mark
  non-null at any depth — e.g. `{:array, {:not_null, :int}}`.

  ## Examples

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)
        |> Fluss.Schema.column("amount", {:decimal, 10, 2}, comment: "Order total")

  """

  defstruct columns: [], primary_key: []

  @type data_type ::
          :boolean
          | :tinyint
          | :smallint
          | :int
          | :bigint
          | :float
          | :double
          | :string
          | :bytes
          | :date
          | :time
          | :timestamp
          | :timestamp_ltz
          | {:decimal, non_neg_integer(), non_neg_integer()}
          | {:char, non_neg_integer()}
          | {:binary, non_neg_integer()}
          | {:array, data_type()}
          | {:map, data_type(), data_type()}
          | {:row, [{String.t(), data_type()}]}
          | {:not_null, data_type()}

  @type t :: %__MODULE__{
          columns: [Fluss.Column.t()],
          primary_key: [String.t()]
        }

  @type column_opts :: [comment: String.t() | nil]

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Appends a column.

  ## Options

    * `:comment` - the column's comment.
  """
  @spec column(t(), String.t(), data_type(), column_opts()) :: t()
  def column(%__MODULE__{} = schema, name, data_type, opts \\ []) do
    opts = validate_opts!(opts)

    column =
      validate_column!(%Fluss.Column{name: name, data_type: data_type, comment: opts[:comment]})

    %{schema | columns: schema.columns ++ [column]}
  end

  @spec primary_key(t(), [String.t()]) :: t()
  def primary_key(%__MODULE__{} = schema, keys) do
    %{schema | primary_key: validate_primary_key!(keys)}
  end

  # A hand-built schema skips `column/4`; checked before the NIF, which would otherwise
  # raise an opaque decode error, or panic on an improper list.
  @doc false
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{columns: columns, primary_key: primary_key} = schema) do
    validate_items!(columns, "columns", &validate_column!/1)
    validate_primary_key!(primary_key)
    schema
  end

  defp validate_primary_key!(keys),
    do: validate_items!(keys, "primary_key", &validate_string!(&1, "primary key column"))

  defp validate_items!(list, field, validate_item) when is_list(list) do
    if List.improper?(list) do
      raise ArgumentError, "expected #{field} to be a proper list, got: #{inspect(list)}"
    end

    Enum.each(list, validate_item)
    list
  end

  defp validate_items!(other, field, _validate_item) do
    raise ArgumentError, "expected #{field} to be a list, got: #{inspect(other)}"
  end

  defp validate_column!(%Fluss.Column{name: name, comment: comment} = column) do
    validate_string!(name, "column name")
    unless is_nil(comment), do: validate_string!(comment, "column comment")
    column
  end

  defp validate_column!(other) do
    raise ArgumentError,
          "expected a %Fluss.Column{} (see Fluss.Schema.column/4), got: #{inspect(other)}"
  end

  defp validate_string!(value, _what) when is_binary(value), do: value

  defp validate_string!(value, what) do
    raise ArgumentError, "expected the #{what} to be a string, got: #{inspect(value)}"
  end

  defp validate_opts!(opts) when is_list(opts), do: Keyword.validate!(opts, comment: nil)

  defp validate_opts!(opts) do
    raise ArgumentError, "expected options to be a keyword list, got: #{inspect(opts)}"
  end
end
