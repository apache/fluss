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

  Parameterized types: `{:decimal, precision, scale}` (precision 1..38, scale 0..precision),
  `{:char, length}`, `{:binary, length}` (length at least 1)

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

  # Keep in sync with `data_type()` below.
  @simple_types [
    :boolean,
    :tinyint,
    :smallint,
    :int,
    :bigint,
    :float,
    :double,
    :string,
    :bytes,
    :date,
    :time,
    :timestamp,
    :timestamp_ltz
  ]

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
          | {:decimal, pos_integer(), non_neg_integer()}
          | {:char, pos_integer()}
          | {:binary, pos_integer()}
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

  defp validate_column!(
         %Fluss.Column{name: name, data_type: data_type, comment: comment} = column
       ) do
    validate_string!(name, "column name")
    validate_data_type!(data_type, name)
    unless is_nil(comment), do: validate_string!(comment, "column comment")
    column
  end

  defp validate_column!(other) do
    raise ArgumentError,
          "expected a %Fluss.Column{} (see Fluss.Schema.column/4), got: #{inspect(other)}"
  end

  defp validate_data_type!(data_type, column_name) do
    unless valid_data_type?(data_type) do
      raise ArgumentError,
            "invalid data type for column `#{column_name}` (see Fluss.Schema), " <>
              "got: #{inspect(data_type, limit: :infinity)}"
    end

    data_type
  end

  # Mirrors the NIF's `DataType`.
  defp valid_data_type?(type) when type in @simple_types, do: true

  defp valid_data_type?({:decimal, precision, scale}) when precision in 1..38,
    do: scale in 0..precision

  defp valid_data_type?({:char, length}) when length in 1..0xFFFFFFFF, do: true
  defp valid_data_type?({:binary, length}) when length in 1..0xFFFFFFFFFFFFFFFF, do: true
  defp valid_data_type?({:array, item}), do: valid_data_type?(item)
  defp valid_data_type?({:map, key, value}), do: valid_data_type?(key) and valid_data_type?(value)
  defp valid_data_type?({:not_null, type}), do: valid_data_type?(type)

  defp valid_data_type?({:row, fields}) when is_list(fields),
    do: not List.improper?(fields) and Enum.all?(fields, &valid_row_field?/1)

  defp valid_data_type?(_type), do: false

  defp valid_row_field?({name, type}) when is_binary(name), do: valid_data_type?(type)
  defp valid_row_field?(_field), do: false

  defp validate_string!(value, _what) when is_binary(value), do: value

  defp validate_string!(value, what) do
    raise ArgumentError, "expected the #{what} to be a string, got: #{inspect(value)}"
  end

  defp validate_opts!(opts) when is_list(opts), do: Keyword.validate!(opts, comment: nil)

  defp validate_opts!(opts) do
    raise ArgumentError, "expected options to be a keyword list, got: #{inspect(opts)}"
  end
end
