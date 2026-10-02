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

defmodule Fluss.Connection do
  @moduledoc """
  A connection to a Fluss cluster.

  Errors are per-operation, not per-connection. If the server becomes
  unreachable, individual calls fail but the connection recovers
  transparently — there is no need to recreate it.

  ## Examples

      config = Fluss.Config.new("localhost:9123")
      {:ok, conn} = Fluss.Connection.new(config)

  """

  alias Fluss.Native

  @type t :: reference()

  @spec new(Fluss.Config.t()) :: {:ok, t()} | {:error, Fluss.Error.t()}
  def new(%Fluss.Config{} = config) do
    config
    |> Native.connection_new()
    |> Native.await_nif()
  end

  @spec new!(Fluss.Config.t()) :: t()
  def new!(%Fluss.Config{} = config) do
    case new(config) do
      {:ok, conn} -> conn
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end
end
