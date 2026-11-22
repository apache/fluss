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

defmodule Fluss.PartitionInfo do
  @moduledoc """
  A partition of a partitioned table. Returned by `Fluss.Admin.list_partition_infos/4`.

  `partition_name` is the partition values joined by `$`, e.g. `"EU"` or `"2026$EU"`.
  """

  @enforce_keys [:partition_id, :partition_name, :partition_spec, :bucket_count]
  defstruct [:partition_id, :partition_name, :partition_spec, :bucket_count]

  @type t :: %__MODULE__{
          partition_id: integer(),
          partition_name: String.t(),
          partition_spec: %{String.t() => String.t()},
          bucket_count: pos_integer()
        }
end
