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

defmodule Fluss.TableInfo do
  @moduledoc """
  Metadata about a Fluss table: identity, schema id, key/partition layout,
  bucketing, properties, and creation/modification timestamps as tracked by the
  cluster.

  Returned by `Fluss.Admin.get_table_info/3`. This carries metadata only — for a
  handle to read from or write to the table, use `Fluss.Table.get/3`.
  """

  @enforce_keys [
    :database_name,
    :table_name,
    :table_id,
    :schema_id,
    :schema,
    :num_buckets,
    :has_primary_key,
    :primary_keys,
    :physical_primary_keys,
    :bucket_keys,
    :has_bucket_key,
    :is_default_bucket_key,
    :is_partitioned,
    :is_auto_partitioned,
    :partition_keys,
    :comment,
    :properties,
    :custom_properties,
    :created_time,
    :modified_time
  ]

  defstruct [
    :database_name,
    :table_name,
    :table_id,
    :schema_id,
    :schema,
    :num_buckets,
    :has_primary_key,
    :primary_keys,
    :physical_primary_keys,
    :bucket_keys,
    :has_bucket_key,
    :is_default_bucket_key,
    :is_partitioned,
    :is_auto_partitioned,
    :partition_keys,
    :comment,
    :properties,
    :custom_properties,
    :created_time,
    :modified_time
  ]

  @type t :: %__MODULE__{
          database_name: String.t(),
          table_name: String.t(),
          table_id: integer(),
          schema_id: integer(),
          schema: Fluss.Schema.t(),
          num_buckets: integer(),
          has_primary_key: boolean(),
          primary_keys: [String.t()],
          physical_primary_keys: [String.t()],
          bucket_keys: [String.t()],
          has_bucket_key: boolean(),
          is_default_bucket_key: boolean(),
          is_partitioned: boolean(),
          is_auto_partitioned: boolean(),
          partition_keys: [String.t()],
          comment: String.t() | nil,
          properties: %{String.t() => String.t()},
          custom_properties: %{String.t() => String.t()},
          created_time: integer(),
          modified_time: integer()
        }
end
