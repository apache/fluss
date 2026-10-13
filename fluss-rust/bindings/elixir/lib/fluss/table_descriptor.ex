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

defmodule Fluss.TableDescriptor do
  @moduledoc """
  Descriptor for creating a Fluss table.

  Bundles a `Fluss.Schema` with optional table settings; pass the result to
  `Fluss.Admin.create_table/5`.

  ## Options

  `new!/2` takes a keyword list:

    * `:bucket_count` - number of buckets to distribute the table into
      (non-negative integer). Omitted means the server decides.
    * `:bucket_keys` - column names to hash for bucketing. For primary-key
      tables these must be a subset of the primary key (excluding partition
      keys), defaulting to the primary key columns when omitted. For log
      tables they are unconstrained.
    * `:partition_keys` - column names to partition the table by.
    * `:properties` - map of recognized Fluss table properties that the engine
      interprets, e.g. `%{"table.log.format" => "ARROW"}`.
    * `:custom_properties` - map of arbitrary string metadata (e.g. ownership,
      team). Stored verbatim and never interpreted by Fluss.
    * `:comment` - table comment.

  ## Examples

      Fluss.TableDescriptor.new!(schema)
      Fluss.TableDescriptor.new!(schema, bucket_count: 3)

      Fluss.TableDescriptor.new!(schema,
        bucket_count: 4,
        bucket_keys: ["id"],
        partition_keys: ["dt"],
        custom_properties: %{"owner" => "data-platform"},
        comment: "events table"
      )

  """

  defmodule Options do
    @moduledoc false
    # Internal boundary struct decoded by the `table_descriptor_new` NIF.
    # Users supply these as a keyword list to `Fluss.TableDescriptor.new!/2`;
    # `new!/2` normalizes that into this struct before crossing into Rust.

    @type t :: %__MODULE__{
            bucket_count: non_neg_integer() | nil,
            bucket_keys: list(),
            partition_keys: list(),
            properties: map(),
            custom_properties: map(),
            comment: String.t() | nil
          }

    defstruct bucket_count: nil,
              bucket_keys: [],
              partition_keys: [],
              properties: %{},
              custom_properties: %{},
              comment: nil
  end

  alias Fluss.Native

  @type t :: reference()

  @doc """
  Builds a table descriptor from a `Fluss.Schema` and a keyword list of options.

  See the module documentation for the available options. Raises `Fluss.Error`
  if the descriptor cannot be built (e.g. an invalid bucket-key/partition-key
  combination), or `KeyError` if an unknown option key is given.
  """
  @spec new!(Fluss.Schema.t(), keyword()) :: t()
  def new!(%Fluss.Schema{} = schema, opts \\ []) do
    opts_struct = struct!(Fluss.TableDescriptor.Options, opts)

    case Native.table_descriptor_new(schema, opts_struct) do
      {:error, %Fluss.Error{} = err} -> raise err
      ref -> ref
    end
  end
end
