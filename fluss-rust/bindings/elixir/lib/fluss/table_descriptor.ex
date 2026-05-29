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

  Options: `:bucket_count`, `:properties` (list of `{key, value}` string tuples).

  ## Examples

      Fluss.TableDescriptor.new!(schema)
      Fluss.TableDescriptor.new!(schema, bucket_count: 3)

  """

  alias Fluss.Native

  @type t :: reference()

  @spec new!(Fluss.Schema.t(), keyword()) :: t()
  def new!(%Fluss.Schema{} = schema, opts \\ []) do
    bucket_count = Keyword.get(opts, :bucket_count)
    properties = Keyword.get(opts, :properties, [])

    case Native.table_descriptor_new(schema, bucket_count, properties) do
      {:error, %Fluss.Error{} = err} -> raise err
      ref -> ref
    end
  end
end
