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

defmodule Fluss.DatabaseDescriptor do
  @moduledoc """
  User-supplied configuration of a Fluss database — its comment and any custom
  properties.

  Constructed via the builder helpers (or as a struct directly) and passed to
  `Fluss.Admin.create_database/3`. Also returned as the `:descriptor` field of
  `Fluss.DatabaseInfo` from `Fluss.Admin.get_database_info/2`.

  ## Examples

      descriptor =
        Fluss.DatabaseDescriptor.new()
        |> Fluss.DatabaseDescriptor.comment("My database")
        |> Fluss.DatabaseDescriptor.put_custom_property("owner", "team-x")

      :ok = Fluss.Admin.create_database(admin, "my_db", descriptor)

  """

  @enforce_keys [:comment, :custom_properties]
  defstruct [:comment, :custom_properties]

  @type t :: %__MODULE__{
          comment: String.t() | nil,
          custom_properties: %{optional(String.t()) => String.t()}
        }

  @spec new() :: t()
  def new, do: %__MODULE__{comment: nil, custom_properties: %{}}

  @spec comment(t(), String.t()) :: t()
  def comment(%__MODULE__{} = desc, c) when is_binary(c), do: %{desc | comment: c}

  @spec put_custom_property(t(), String.t(), String.t()) :: t()
  def put_custom_property(%__MODULE__{} = desc, k, v) when is_binary(k) and is_binary(v),
    do: %{desc | custom_properties: Map.put(desc.custom_properties, k, v)}

  @spec put_custom_properties(t(), %{optional(String.t()) => String.t()}) :: t()
  def put_custom_properties(%__MODULE__{} = desc, props) when is_map(props) do
    Enum.each(props, fn {k, v} ->
      unless is_binary(k) and is_binary(v) do
        raise ArgumentError,
              "put_custom_properties/2 requires String keys and values, got #{inspect({k, v})}"
      end
    end)

    %{desc | custom_properties: Map.merge(desc.custom_properties, props)}
  end
end
