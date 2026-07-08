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

defmodule Fluss do
  @moduledoc """
  Elixir client for Apache Fluss (Incubating).

  ## Examples

      config = Fluss.Config.new("localhost:9123")
      conn = Fluss.Connection.new!(config)
      admin = Fluss.Admin.new!(conn)

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("ts", :bigint)
        |> Fluss.Schema.column("message", :string)

      :ok = Fluss.Admin.create_table(admin, "my_db", "events", Fluss.TableDescriptor.new!(schema))

      table = Fluss.Table.get!(conn, "my_db", "events")
      writer = Fluss.AppendWriter.new!(table)
      Fluss.AppendWriter.append(writer, [1_700_000_000, "hello"])
      :ok = Fluss.AppendWriter.flush(writer)

      scanner = Fluss.LogScanner.new!(table)
      :ok = Fluss.LogScanner.subscribe(scanner, 0, Fluss.earliest_offset())
      :ok = Fluss.LogScanner.poll(scanner, 5_000)
      receive do
        {:fluss_records, records} -> records
      end

  """

  alias Fluss.Native

  def earliest_offset, do: Native.earliest_offset()
end
