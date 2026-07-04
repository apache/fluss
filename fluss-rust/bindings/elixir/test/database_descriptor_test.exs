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

defmodule Fluss.DatabaseDescriptorTest do
  use ExUnit.Case, async: true

  alias Fluss.DatabaseDescriptor

  describe "new/0" do
    test "returns a descriptor with no comment and no custom properties" do
      assert DatabaseDescriptor.new() ==
               %DatabaseDescriptor{comment: nil, custom_properties: %{}}
    end
  end

  describe "comment/2" do
    test "sets the comment field" do
      desc = DatabaseDescriptor.new() |> DatabaseDescriptor.comment("hello")
      assert desc.comment == "hello"
    end

    test "overwrites a previous comment" do
      desc =
        DatabaseDescriptor.new()
        |> DatabaseDescriptor.comment("first")
        |> DatabaseDescriptor.comment("second")

      assert desc.comment == "second"
    end

    test "preserves existing custom_properties" do
      desc =
        DatabaseDescriptor.new()
        |> DatabaseDescriptor.put_custom_property("k", "v")
        |> DatabaseDescriptor.comment("c")

      assert desc.comment == "c"
      assert desc.custom_properties == %{"k" => "v"}
    end

    test "raises on non-binary input" do
      assert_raise FunctionClauseError, fn ->
        DatabaseDescriptor.comment(DatabaseDescriptor.new(), 42)
      end
    end
  end

  describe "put_custom_property/3" do
    test "adds a property" do
      desc = DatabaseDescriptor.new() |> DatabaseDescriptor.put_custom_property("k", "v")
      assert desc.custom_properties == %{"k" => "v"}
    end

    test "accumulates multiple properties" do
      desc =
        DatabaseDescriptor.new()
        |> DatabaseDescriptor.put_custom_property("k1", "v1")
        |> DatabaseDescriptor.put_custom_property("k2", "v2")

      assert desc.custom_properties == %{"k1" => "v1", "k2" => "v2"}
    end

    test "overwrites a previous value for the same key" do
      desc =
        DatabaseDescriptor.new()
        |> DatabaseDescriptor.put_custom_property("k", "v1")
        |> DatabaseDescriptor.put_custom_property("k", "v2")

      assert desc.custom_properties == %{"k" => "v2"}
    end

    test "preserves existing comment" do
      desc =
        DatabaseDescriptor.new()
        |> DatabaseDescriptor.comment("c")
        |> DatabaseDescriptor.put_custom_property("k", "v")

      assert desc.comment == "c"
      assert desc.custom_properties == %{"k" => "v"}
    end

    test "raises on non-binary key" do
      assert_raise FunctionClauseError, fn ->
        DatabaseDescriptor.put_custom_property(DatabaseDescriptor.new(), :atom_key, "v")
      end
    end

    test "raises on non-binary value" do
      assert_raise FunctionClauseError, fn ->
        DatabaseDescriptor.put_custom_property(DatabaseDescriptor.new(), "k", 42)
      end
    end
  end

  describe "put_custom_properties/2" do
    test "adds entries from a map" do
      desc =
        DatabaseDescriptor.new()
        |> DatabaseDescriptor.put_custom_properties(%{"k1" => "v1", "k2" => "v2"})

      assert desc.custom_properties == %{"k1" => "v1", "k2" => "v2"}
    end

    test "merges with existing entries" do
      desc =
        DatabaseDescriptor.new()
        |> DatabaseDescriptor.put_custom_property("k0", "v0")
        |> DatabaseDescriptor.put_custom_properties(%{"k1" => "v1"})

      assert desc.custom_properties == %{"k0" => "v0", "k1" => "v1"}
    end

    test "overwrites on key collision" do
      desc =
        DatabaseDescriptor.new()
        |> DatabaseDescriptor.put_custom_property("k", "v1")
        |> DatabaseDescriptor.put_custom_properties(%{"k" => "v2"})

      assert desc.custom_properties == %{"k" => "v2"}
    end

    test "accepts an empty map" do
      desc = DatabaseDescriptor.new() |> DatabaseDescriptor.put_custom_properties(%{})
      assert desc.custom_properties == %{}
    end

    test "preserves existing comment" do
      desc =
        DatabaseDescriptor.new()
        |> DatabaseDescriptor.comment("c")
        |> DatabaseDescriptor.put_custom_properties(%{"k" => "v"})

      assert desc.comment == "c"
      assert desc.custom_properties == %{"k" => "v"}
    end

    test "raises FunctionClauseError on non-map input" do
      assert_raise FunctionClauseError, fn ->
        DatabaseDescriptor.put_custom_properties(DatabaseDescriptor.new(), [])
      end
    end

    test "raises ArgumentError on non-binary key in the map" do
      assert_raise ArgumentError, fn ->
        DatabaseDescriptor.put_custom_properties(DatabaseDescriptor.new(), %{:atom_key => "v"})
      end
    end

    test "raises ArgumentError on non-binary value in the map" do
      assert_raise ArgumentError, fn ->
        DatabaseDescriptor.put_custom_properties(DatabaseDescriptor.new(), %{"k" => 42})
      end
    end
  end
end
