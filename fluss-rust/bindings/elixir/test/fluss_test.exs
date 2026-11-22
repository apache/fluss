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

defmodule FlussTest do
  use ExUnit.Case

  describe "TableDescriptor" do
    test "creates descriptor from schema" do
      Fluss.Schema.new()
      |> Fluss.Schema.column("id", :int)
      |> Fluss.TableDescriptor.new!()
    end

    test "creates descriptor with bucket count" do
      Fluss.Schema.new()
      |> Fluss.Schema.column("id", :int)
      |> Fluss.TableDescriptor.new!(bucket_count: 3)
    end

    test "accepts all simple data types" do
      Fluss.Schema.new()
      |> Fluss.Schema.column("a", :boolean)
      |> Fluss.Schema.column("b", :tinyint)
      |> Fluss.Schema.column("c", :smallint)
      |> Fluss.Schema.column("d", :int)
      |> Fluss.Schema.column("e", :bigint)
      |> Fluss.Schema.column("f", :float)
      |> Fluss.Schema.column("g", :double)
      |> Fluss.Schema.column("h", :string)
      |> Fluss.Schema.column("i", :bytes)
      |> Fluss.Schema.column("j", :date)
      |> Fluss.Schema.column("k", :time)
      |> Fluss.Schema.column("l", :timestamp)
      |> Fluss.Schema.column("m", :timestamp_ltz)
      |> Fluss.TableDescriptor.new!()
    end

    test "accepts parameterized data types" do
      Fluss.Schema.new()
      |> Fluss.Schema.column("amount", {:decimal, 10, 2})
      |> Fluss.Schema.column("code", {:char, 5})
      |> Fluss.Schema.column("data", {:binary, 16})
      |> Fluss.TableDescriptor.new!()
    end
  end

  describe "earliest_offset/0" do
    test "returns -2" do
      assert Fluss.earliest_offset() == -2
    end
  end
end
