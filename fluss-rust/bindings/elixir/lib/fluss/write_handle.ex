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

defmodule Fluss.WriteHandle do
  @moduledoc """
  Handle for a pending write operation.

  Returned by `Fluss.AppendWriter.append/2`. Drop for fire-and-forget,
  or call `wait/1` for per-record server acknowledgment.
  """

  alias Fluss.Native

  @type t :: reference()

  @spec wait(t()) :: :ok | {:error, Fluss.Error.t()}
  def wait(handle) do
    handle
    |> Native.write_handle_wait()
    |> Native.await_nif()
  end

  @spec wait!(t()) :: :ok
  def wait!(handle) do
    case wait(handle) do
      :ok -> :ok
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end
end
