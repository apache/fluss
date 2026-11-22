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

defmodule Fluss.DatabaseInfo do
  @moduledoc """
  Metadata about a Fluss database: name, descriptor, and creation/modification
  timestamps as tracked by the cluster.

  Returned by `Fluss.Admin.get_database_info/2`. The `:descriptor` field
  holds a `Fluss.DatabaseDescriptor.t()` carrying the user-supplied comment and
  custom properties.
  """

  @enforce_keys [:database_name, :descriptor, :created_time, :modified_time]
  defstruct [:database_name, :descriptor, :created_time, :modified_time]

  @type t :: %__MODULE__{
          database_name: String.t(),
          descriptor: Fluss.DatabaseDescriptor.t(),
          created_time: integer(),
          modified_time: integer()
        }
end
