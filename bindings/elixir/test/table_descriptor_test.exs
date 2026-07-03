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

defmodule Fluss.TableDescriptorTest do
  use ExUnit.Case, async: true

  alias Fluss.Schema
  alias Fluss.TableDescriptor

  describe "new!/2" do
    test "returns a descriptor reference for a minimal schema" do
      schema =
        Schema.new()
        |> Schema.column("id", :int)
        |> Schema.column("name", :string)

      assert is_reference(TableDescriptor.new!(schema))
    end

    test "returns a descriptor reference with all options set" do
      schema =
        Schema.new()
        |> Schema.column("id", :int)
        |> Schema.column("dt", :string)
        |> Schema.primary_key(["id", "dt"])

      descriptor =
        TableDescriptor.new!(schema,
          bucket_count: 3,
          bucket_keys: ["id"],
          partition_keys: ["dt"],
          properties: %{"table.replication.factor" => "1"},
          custom_properties: %{"owner" => "data-platform"},
          comment: "events table"
        )

      assert is_reference(descriptor)
    end

    test "raises Fluss.Error when bucket keys are not a subset of the primary key" do
      schema =
        Schema.new()
        |> Schema.column("id", :int)
        |> Schema.column("region", :string)
        |> Schema.primary_key(["id"])

      assert_raise Fluss.Error, fn ->
        TableDescriptor.new!(schema, bucket_keys: ["region"])
      end
    end

    test "raises Fluss.Error when bucket keys overlap partition keys" do
      schema =
        Schema.new()
        |> Schema.column("id", :int)
        |> Schema.column("dt", :string)

      assert_raise Fluss.Error, fn ->
        TableDescriptor.new!(schema, partition_keys: ["dt"], bucket_keys: ["dt"])
      end
    end

    # `new!/2` rejects unknown keys rather than silently dropping them.
    test "raises KeyError on an unknown option" do
      schema = Schema.new() |> Schema.column("id", :int)

      assert_raise KeyError, fn ->
        TableDescriptor.new!(schema, bogus: 1)
      end
    end
  end
end
