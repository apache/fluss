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

defmodule Fluss.Integration.LookupTest do
  use Fluss.Test.IntegrationCase, async: false

  alias Fluss.Lookuper
  alias Fluss.PrefixLookuper
  alias Fluss.TableUpsert
  alias Fluss.UpsertWriter

  @database "fluss"

  describe "primary key lookup" do
    setup ctx do
      table = create_kv_table(ctx, "lookup", id_name_email_schema())
      writer = writer!(table)
      upsert!(writer, [1, "alice", "alice@example.com"])
      upsert!(writer, [2, "bob", nil])
      %{table: table, writer: writer, lookuper: Lookuper.new!(table)}
    end

    test "finds a row by list or map key", %{lookuper: lookuper} do
      expected = %{id: 1, name: "alice", email: "alice@example.com"}

      assert Lookuper.lookup(lookuper, [1]) == {:ok, expected}
      assert Lookuper.lookup(lookuper, %{id: 1}) == {:ok, expected}
      assert Lookuper.lookup(lookuper, %{"id" => 1}) == {:ok, expected}
    end

    test "returns nil for a null column", %{lookuper: lookuper} do
      assert Lookuper.lookup(lookuper, [2]) == {:ok, %{id: 2, name: "bob", email: nil}}
    end

    test "returns {:ok, nil} for a missing key", %{lookuper: lookuper} do
      assert Lookuper.lookup(lookuper, [404]) == {:ok, nil}
    end

    test "lookup!/3 returns the row or raises Fluss.NoResultsError", %{lookuper: lookuper} do
      assert Lookuper.lookup!(lookuper, [1])[:name] == "alice"

      error = assert_raise Fluss.NoResultsError, fn -> Lookuper.lookup!(lookuper, [404]) end
      assert error.key == [404]
      assert error.message == "expected a row for key [404], got none"
    end

    test "sees overwrites and deletes", %{writer: writer, lookuper: lookuper} do
      upsert!(writer, [1, "alice2", "alice@example.com"])
      assert {:ok, %{name: "alice2"}} = Lookuper.lookup(lookuper, [1])

      delete!(writer, [1])
      assert Lookuper.lookup(lookuper, [1]) == {:ok, nil}
    end

    test "sees a partial update merged into the row", %{table: table, lookuper: lookuper} do
      partial =
        table
        |> TableUpsert.new!()
        |> TableUpsert.partial_update_by_name!([:id, :email])
        |> TableUpsert.create_writer!()

      upsert!(partial, %{id: 1, email: "new@example.com"})

      assert Lookuper.lookup(lookuper, [1]) ==
               {:ok, %{id: 1, name: "alice", email: "new@example.com"}}
    end

    test "can be shared by many processes", %{lookuper: lookuper} do
      results =
        1..50
        |> Enum.map(fn i -> Task.async(fn -> Lookuper.lookup(lookuper, [rem(i, 3)]) end) end)
        |> Task.await_many()

      assert Enum.count(results, &match?({:ok, %{id: 1}}, &1)) == 17
      assert Enum.count(results, &match?({:ok, %{id: 2}}, &1)) == 17
      assert Enum.count(results, &(&1 == {:ok, nil})) == 16
    end
  end

  describe "key validation" do
    setup ctx do
      %{
        lookuper: ctx |> create_kv_table("lookup_validation", id_name_schema()) |> Lookuper.new!()
      }
    end

    test "rejects a list of the wrong length", %{lookuper: lookuper} do
      assert {:error, %Fluss.Error{} = err} = Lookuper.lookup(lookuper, [1, "alice"])
      assert err.message =~ "expected 1 values, got 2"
    end

    test "rejects a non-key column in a map", %{lookuper: lookuper} do
      assert {:error, %Fluss.Error{} = err} = Lookuper.lookup(lookuper, %{id: 1, name: "x"})
      assert err.message =~ "column `name` is not accepted here; expected one of: id"
    end

    test "rejects a nil or missing key value", %{lookuper: lookuper} do
      for key <- [[nil], %{id: nil}, %{}] do
        assert {:error, %Fluss.Error{} = err} = Lookuper.lookup(lookuper, key)
        assert err.message =~ "NOT NULL column `id` must not be nil or omitted"
      end
    end

    test "rejects an invalid timeout option", %{lookuper: lookuper} do
      for opts <- [[timeout: -1], [timeout: 2 ** 32], [timeout: "1"], [nope: 1]] do
        assert_raise ArgumentError, fn -> Lookuper.lookup(lookuper, [1], opts) end
      end
    end
  end

  describe "composite primary key" do
    test "a list key follows primary key declaration order", ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("a", :int)
        |> Fluss.Schema.column("b", :int)
        |> Fluss.Schema.column("v", :string)
        |> Fluss.Schema.primary_key(["b", "a"])

      table = create_kv_table(ctx, "lookup_composite_key", schema, bucket_keys: ["b"])
      writer = writer!(table)
      upsert!(writer, %{a: 1, b: 2, v: "x"})
      upsert!(writer, %{a: 2, b: 1, v: "y"})
      lookuper = Lookuper.new!(table)

      # [b, a]: b=2, a=1. Column order would have found the other row.
      assert {:ok, %{v: "x"}} = Lookuper.lookup(lookuper, [2, 1])

      # The same list deletes the same row.
      delete!(writer, [2, 1])
      assert Lookuper.lookup(lookuper, [2, 1]) == {:ok, nil}
      assert {:ok, %{v: "y"}} = Lookuper.lookup(lookuper, [1, 2])
    end
  end

  describe "table kind and types" do
    test "new/1 rejects a log table", %{conn: conn, admin: admin} do
      table_name = unique("lookup_log")
      on_exit(fn -> cleanup_table(admin, table_name) end)

      schema = Fluss.Schema.new() |> Fluss.Schema.column("id", :int)
      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)
      table = Fluss.Table.get!(conn, @database, table_name)

      assert {:error, %Fluss.Error{} = err} = Lookuper.new(table)
      assert err.message =~ "Lookup is only supported for primary key tables"
    end

    # Core retries an unknown bucket leader forever: https://github.com/apache/fluss/issues/4706
    @tag :skip
    test "returns an error after the table is dropped", %{conn: conn, admin: admin} do
      table_name = unique("lookup_dropped_table")
      descriptor = Fluss.TableDescriptor.new!(id_name_schema(), bucket_count: 1)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)
      table = Fluss.Table.get!(conn, @database, table_name)
      upsert!(writer!(table), [1, "alice"])
      lookuper = Lookuper.new!(table)

      assert {:ok, %{id: 1, name: "alice"}} = Lookuper.lookup(lookuper, [1])

      :ok = Fluss.Admin.drop_table(admin, @database, table_name, false)

      assert {:error, %Fluss.Error{code: :table_not_exist}} = Lookuper.lookup(lookuper, [1])
    end

    test "returns array, map and row values", ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("tags", {:array, :string})
        |> Fluss.Schema.column("attrs", {:map, :string, :int})
        |> Fluss.Schema.column("point", {:row, [{"x", :int}, {"y", :int}]})
        |> Fluss.Schema.primary_key(["id"])

      table = create_kv_table(ctx, "lookup_complex", schema)
      row = %{id: 1, tags: ["a", "b"], attrs: %{"x" => 1}, point: %{x: 10, y: 20}}
      table |> writer!() |> upsert!(row)

      assert Lookuper.lookup(Lookuper.new!(table), [1]) == {:ok, row}
    end
  end

  describe "timeout" do
    # A 2 s batch timeout makes every lone lookup take ~2 s.
    setup ctx do
      conn =
        ctx.config |> Fluss.Config.set_lookup_batch_timeout_ms(2_000) |> Fluss.Connection.new!()

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("tenant_id", :int)
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.primary_key(["tenant_id", "id"])

      table =
        create_kv_table(%{ctx | conn: conn}, "lookup_slow", schema, bucket_keys: ["tenant_id"])

      upsert!(writer!(table), [7, 1])
      %{table: table, lookuper: Lookuper.new!(table)}
    end

    test "returns request_time_out and the cancelled lookup never replies", %{lookuper: lookuper} do
      assert {:error, %Fluss.Error{code: :request_time_out}} =
               Lookuper.lookup(lookuper, [7, 1], timeout: 200)

      refute_receive _, 3_000
    end

    test "waits for the reply with :infinity", %{lookuper: lookuper} do
      assert Lookuper.lookup(lookuper, [7, 1], timeout: :infinity) ==
               {:ok, %{tenant_id: 7, id: 1}}
    end

    test "lookup!/3 raises on timeout", %{lookuper: lookuper} do
      assert_raise Fluss.Error, ~r/no reply within 100 ms/, fn ->
        Lookuper.lookup!(lookuper, [7, 1], timeout: 100)
      end
    end

    test "applies to prefix lookups", %{table: table} do
      lookuper = PrefixLookuper.new!(table, [:tenant_id])

      assert {:error, %Fluss.Error{code: :request_time_out}} =
               PrefixLookuper.lookup(lookuper, [7], timeout: 100)

      refute_receive _, 3_000
    end

    test "a caller that exits doesn't affect other lookups", %{lookuper: lookuper} do
      caller = spawn(fn -> Lookuper.lookup(lookuper, [7, 1], timeout: :infinity) end)
      other = Task.async(fn -> Lookuper.lookup(lookuper, [7, 1]) end)
      # Let both lookups reach the same batch before the caller dies.
      Process.sleep(100)
      Process.exit(caller, :kill)

      assert Task.await(other, 5_000) == {:ok, %{tenant_id: 7, id: 1}}
    end
  end

  describe "partitioned table" do
    test "looks up within an auto-created partition", ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("year", :string)
        |> Fluss.Schema.column("name", :string)
        |> Fluss.Schema.primary_key(["id", "year"])

      # YEAR keeps the partition value stable for the length of the test.
      table =
        create_kv_table(ctx, "lookup_partitioned", schema,
          partition_keys: ["year"],
          properties: %{
            "table.auto-partition.enabled" => "true",
            "table.auto-partition.time-unit" => "YEAR"
          }
        )

      year = Integer.to_string(Date.utc_today().year)
      table |> writer!() |> upsert!([1, year, "alice"])
      lookuper = Lookuper.new!(table)

      assert Lookuper.lookup(lookuper, [1, year]) == {:ok, %{id: 1, year: year, name: "alice"}}
      assert Lookuper.lookup(lookuper, %{id: 1, year: "1999"}) == {:ok, nil}
    end
  end

  describe "prefix lookup" do
    setup ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("tenant_id", :int)
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)
        |> Fluss.Schema.primary_key(["tenant_id", "id"])

      table = create_kv_table(ctx, "lookup_prefix", schema, bucket_keys: ["tenant_id"])
      writer = writer!(table)
      upsert!(writer, [7, 1, "a"])
      upsert!(writer, [7, 2, "b"])
      upsert!(writer, [8, 1, "c"])
      %{table: table}
    end

    test "returns every row with the prefix", %{table: table} do
      lookuper = PrefixLookuper.new!(table, [:tenant_id])

      assert {:ok, rows} = PrefixLookuper.lookup(lookuper, [7])

      assert Enum.sort_by(rows, & &1[:id]) == [
               %{tenant_id: 7, id: 1, name: "a"},
               %{tenant_id: 7, id: 2, name: "b"}
             ]

      assert PrefixLookuper.lookup(lookuper, %{"tenant_id" => 8}) ==
               {:ok, [%{tenant_id: 8, id: 1, name: "c"}]}
    end

    test "returns an empty list for no match, from lookup!/3 too", %{table: table} do
      lookuper = PrefixLookuper.new!(table, ["tenant_id"])

      assert PrefixLookuper.lookup(lookuper, [9]) == {:ok, []}
      assert PrefixLookuper.lookup!(lookuper, [9]) == []
    end

    test "rejects a nil prefix value", %{table: table} do
      lookuper = PrefixLookuper.new!(table, [:tenant_id])

      assert {:error, %Fluss.Error{} = err} = PrefixLookuper.lookup(lookuper, [nil])
      assert err.message =~ "NOT NULL column `tenant_id` must not be nil or omitted"
    end

    test "new/2 rejects an improper list of columns", %{table: table} do
      assert {:error, %Fluss.Error{} = err} = PrefixLookuper.new(table, [:tenant_id | :id])
      assert err.message =~ "expected a list of column names"
    end

    test "new/2 rejects columns that are not the bucket key", %{table: table} do
      assert {:error, %Fluss.Error{} = err} = PrefixLookuper.new(table, [:id])
      assert err.message =~ "must contain all bucket keys"
    end

    test "looks up a prefix within a partition", ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("tenant_id", :int)
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("year", :string)
        |> Fluss.Schema.primary_key(["tenant_id", "id", "year"])

      table =
        create_kv_table(ctx, "lookup_prefix_partitioned", schema,
          bucket_keys: ["tenant_id"],
          partition_keys: ["year"],
          properties: %{
            "table.auto-partition.enabled" => "true",
            "table.auto-partition.time-unit" => "YEAR"
          }
        )

      year = Integer.to_string(Date.utc_today().year)
      writer = writer!(table)
      upsert!(writer, [7, 1, year])
      upsert!(writer, [7, 2, year])
      upsert!(writer, [8, 1, year])
      lookuper = PrefixLookuper.new!(table, [:year, :tenant_id])

      assert {:ok, rows} = PrefixLookuper.lookup(lookuper, [year, 7])
      assert rows |> Enum.map(& &1[:id]) |> Enum.sort() == [1, 2]
      assert PrefixLookuper.lookup(lookuper, %{year: "1999", tenant_id: 7}) == {:ok, []}
    end

    test "new/2 rejects a repeated partition column", ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("tenant_id", :int)
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("year", :string)
        |> Fluss.Schema.primary_key(["tenant_id", "id", "year"])

      table =
        create_kv_table(ctx, "lookup_prefix_dup", schema,
          bucket_keys: ["tenant_id"],
          partition_keys: ["year"]
        )

      assert {:error, %Fluss.Error{} = err} =
               PrefixLookuper.new(table, [:year, "year", :tenant_id])

      assert err.message =~ "duplicate target column `year`"
    end
  end

  # -- helpers ---------------------------------------------------------------

  defp id_name_schema do
    Fluss.Schema.new()
    |> Fluss.Schema.column("id", :int)
    |> Fluss.Schema.column("name", :string)
    |> Fluss.Schema.primary_key(["id"])
  end

  defp id_name_email_schema do
    Fluss.Schema.new()
    |> Fluss.Schema.column("id", :int)
    |> Fluss.Schema.column("name", :string)
    |> Fluss.Schema.column("email", :string)
    |> Fluss.Schema.primary_key(["id"])
  end

  defp create_kv_table(%{conn: conn, admin: admin}, label, schema, opts \\ []) do
    table_name = unique(label)
    cleanup_table(admin, table_name)
    on_exit(fn -> cleanup_table(admin, table_name) end)

    descriptor =
      Fluss.TableDescriptor.new!(
        schema,
        Keyword.merge([bucket_count: 1, bucket_keys: ["id"]], opts)
      )

    :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)
    Fluss.Table.get!(conn, @database, table_name)
  end

  defp writer!(table), do: table |> TableUpsert.new!() |> TableUpsert.create_writer!()

  defp upsert!(writer, values) do
    {:ok, handle} = UpsertWriter.upsert(writer, values)
    :ok = Fluss.WriteHandle.wait(handle)
  end

  defp delete!(writer, primary_key) do
    {:ok, handle} = UpsertWriter.delete(writer, primary_key)
    :ok = Fluss.WriteHandle.wait(handle)
  end

  defp unique(label), do: "ex_test_#{label}_#{:rand.uniform(100_000)}"

  defp cleanup_table(admin, table_name) do
    Fluss.Admin.drop_table(admin, @database, table_name, true)
  end
end
