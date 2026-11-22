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

defmodule Fluss.SchemaInfo do
  @moduledoc """
  A versioned schema: a table's `Fluss.Schema` together with its schema id.

  Returned by `Fluss.Admin.get_table_schema/4`. Fluss versions schemas, so the
  `:schema_id` identifies which version this is.
  """

  @enforce_keys [:schema, :schema_id]
  defstruct [:schema, :schema_id]

  @type t :: %__MODULE__{
          schema: Fluss.Schema.t(),
          schema_id: integer()
        }
end
