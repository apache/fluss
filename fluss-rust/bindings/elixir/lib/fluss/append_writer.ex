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

defmodule Fluss.AppendWriter do
  @moduledoc """
  Writer for appending records to a log table.

  Values are passed as a list in column order. Use `nil` for null values.
  `append/2` returns a `Fluss.WriteHandle` — drop it for fire-and-forget,
  or call `Fluss.WriteHandle.wait/1` for per-record acknowledgment.

  ## Examples

      writer = Fluss.AppendWriter.new!(table)

      # Fire-and-forget
      Fluss.AppendWriter.append(writer, [1_700_000_000, "hello"])
      Fluss.AppendWriter.append(writer, [1_700_000_001, "world"])
      :ok = Fluss.AppendWriter.flush(writer)

      # Per-record ack
      {:ok, handle} = Fluss.AppendWriter.append(writer, [1_700_000_002, "critical"])
      :ok = Fluss.WriteHandle.wait(handle)

  """

  alias Fluss.Native

  @type t :: reference()

  @spec new(Fluss.Table.t()) :: {:ok, t()} | {:error, Fluss.Error.t()}
  def new(table) do
    case Native.append_writer_new(table) do
      {:error, _} = err -> err
      w -> {:ok, w}
    end
  end

  @spec new!(Fluss.Table.t()) :: t()
  def new!(table) do
    case new(table) do
      {:ok, w} -> w
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec append(t(), list()) :: {:ok, Fluss.WriteHandle.t()} | {:error, Fluss.Error.t()}
  def append(writer, values) when is_list(values) do
    case Native.append_writer_append(writer, values) do
      {:error, _} = err -> err
      handle -> {:ok, handle}
    end
  end

  @spec flush(t()) :: :ok | {:error, Fluss.Error.t()}
  def flush(writer) do
    writer
    |> Native.append_writer_flush()
    |> Native.await_nif()
  end
end
