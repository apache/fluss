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

  ## Examples

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)
        |> Fluss.Schema.column("amount", {:decimal, 10, 2})

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

  @type t :: %__MODULE__{
          columns: [{String.t(), data_type()}],
          primary_key: [String.t()]
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec column(t(), String.t(), data_type()) :: t()
  def column(%__MODULE__{} = schema, name, data_type) when is_binary(name) do
    %{schema | columns: schema.columns ++ [{name, data_type}]}
  end

  @spec primary_key(t(), [String.t()]) :: t()
  def primary_key(%__MODULE__{} = schema, keys) when is_list(keys) do
    %{schema | primary_key: keys}
  end
end
