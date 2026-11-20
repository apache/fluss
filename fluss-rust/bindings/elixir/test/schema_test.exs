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

defmodule Fluss.SchemaTest do
  use ExUnit.Case, async: true

  alias Fluss.Column
  alias Fluss.Schema

  describe "column/4" do
    test "appends columns in order, with an optional comment" do
      schema =
        Schema.new()
        |> Schema.column("id", :int, comment: "The order id")
        |> Schema.column("note", :string)

      assert schema.columns == [
               %Column{name: "id", data_type: :int, comment: "The order id"},
               %Column{name: "note", data_type: :string, comment: nil}
             ]
    end

    test "a schema with comments builds a table descriptor" do
      schema = Schema.column(Schema.new(), "id", :int, comment: "The order id")
      assert is_reference(Fluss.TableDescriptor.new!(schema))
    end

    test "raises ArgumentError on malformed arguments" do
      for {name, opts, message} <- [
            {:id, [], "expected the column name to be a string"},
            {"id", [comment: 1], "expected the column comment to be a string"},
            {"id", [unknown: "x"], "unknown keys [:unknown]"},
            {"id", "not opts", "expected options to be a keyword list"}
          ] do
        assert_raise ArgumentError, ~r/#{Regex.escape(message)}/, fn ->
          Schema.column(Schema.new(), name, :int, opts)
        end
      end
    end
  end

  describe "data types" do
    test "accepts every simple type, the parameterized bounds and nested types" do
      simple_types =
        ~w(boolean tinyint smallint int bigint float double string bytes date time timestamp
           timestamp_ltz)a

      types =
        simple_types ++
          [
            {:decimal, 1, 0},
            {:decimal, 38, 38},
            {:char, 1},
            {:char, 0xFFFFFFFF},
            {:binary, 0xFFFFFFFFFFFFFFFF},
            {:row, []},
            {:not_null, {:not_null, :int}},
            {:row, [{"a", {:array, {:map, {:char, 1}, {:binary, 16}}}}]}
          ]

      for type <- types do
        schema = Schema.column(Schema.new(), "c", type)
        assert is_reference(Fluss.TableDescriptor.new!(schema)), inspect(type)
      end
    end

    test "raises ArgumentError for an invalid type, in column/4 and in a hand-built schema" do
      for type <- [
            :bogus,
            nil,
            "int",
            {:row, [{"a", :int} | :oops]},
            {:row, [{"b", {:row, [{"x", :int} | :oops]}}]},
            {:row, [{:a, :int}]},
            {:array, :bogus},
            {:map, :int},
            {:not_null, :bogus},
            {:decimal, 0, 0},
            {:decimal, 39, 2},
            {:decimal, 10, 11},
            {:decimal, 10.0, 2},
            {:char, 0},
            {:char, 0x100000000},
            {:binary, -1},
            {:binary, 0x10000000000000000}
          ] do
        message = ~r/invalid data type for column `c`.*#{Regex.escape(inspect(type))}/

        assert_raise ArgumentError, message, fn -> Schema.column(Schema.new(), "c", type) end

        assert_raise ArgumentError, message, fn ->
          Fluss.TableDescriptor.new!(%Schema{columns: [%Column{name: "c", data_type: type}]})
        end
      end
    end
  end

  describe "primary_key/2" do
    test "takes a list of column names, even for one column" do
      assert Schema.primary_key(Schema.new(), ["id"]).primary_key == ["id"]
    end

    test "raises ArgumentError on malformed keys" do
      for {keys, message} <- [
            {"id", "expected primary_key to be a list"},
            {[:id], "expected the primary key column to be a string"},
            {["id" | :tail], "expected primary_key to be a proper list"}
          ] do
        assert_raise ArgumentError, ~r/#{Regex.escape(message)}/, fn ->
          Schema.primary_key(Schema.new(), keys)
        end
      end
    end
  end

  describe "a hand-built schema" do
    test "raises ArgumentError in TableDescriptor.new!/2 for malformed columns" do
      for {columns, message} <- [
            {[{"id", :int}], "expected a %Fluss.Column{} (see Fluss.Schema.column/4)"},
            {[%Column{name: :id, data_type: :int}], "expected the column name to be a string"},
            {[%Column{name: "id", data_type: :int, comment: 1}],
             "expected the column comment to be a string"},
            {[%Column{name: "id", data_type: :int} | :tail],
             "expected columns to be a proper list"},
            {:not_a_list, "expected columns to be a list"}
          ] do
        assert_raise ArgumentError, ~r/#{Regex.escape(message)}/, fn ->
          Fluss.TableDescriptor.new!(%Schema{columns: columns})
        end
      end
    end

    test "raises ArgumentError in TableDescriptor.new!/2 for a malformed primary key" do
      schema = Schema.column(Schema.new(), "id", :int)

      for {primary_key, message} <- [
            {[:id], "expected the primary key column to be a string"},
            {["id" | :tail], "expected primary_key to be a proper list"}
          ] do
        assert_raise ArgumentError, ~r/#{Regex.escape(message)}/, fn ->
          Fluss.TableDescriptor.new!(%{schema | primary_key: primary_key})
        end
      end
    end
  end
end
