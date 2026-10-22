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

defmodule Fluss.Integration.KvTableTest do
  use Fluss.Test.IntegrationCase, async: false

  alias Fluss.Native
  alias Fluss.TableUpsert
  alias Fluss.UpsertWriter

  @database "fluss"

  describe "changelog" do
    test "upsert, overwrite and delete produce the expected changelog", ctx do
      table = create_kv_table(ctx, "changelog", id_name_schema())
      writer = table |> TableUpsert.new!() |> TableUpsert.create_writer!()

      upsert!(writer, [1, "alice"])
      upsert!(writer, [2, "bob"])
      upsert!(writer, [1, "alice2"])
      delete!(writer, [2])

      records = changelog(table, 5)

      assert Enum.map(records, & &1[:change_type]) == [
               :insert,
               :insert,
               :update_before,
               :update_after,
               :delete
             ]

      assert Enum.map(records, & &1[:offset]) == [0, 1, 2, 3, 4]

      # -U and -D carry the old image, +U the new one.
      assert Enum.map(records, &{&1[:row][:id], &1[:row][:name]}) == [
               {1, "alice"},
               {2, "bob"},
               {1, "alice"},
               {1, "alice2"},
               {2, "bob"}
             ]
    end

    test "map keys may be atoms or strings", ctx do
      table = create_kv_table(ctx, "map_form", id_name_schema())
      writer = table |> TableUpsert.new!() |> TableUpsert.create_writer!()

      upsert!(writer, %{id: 1, name: "alice"})
      upsert!(writer, %{"id" => 2, "name" => "bob"})
      delete!(writer, %{id: 1})

      assert :ok = UpsertWriter.flush(writer)

      records = changelog(table, 3)

      assert Enum.map(records, & &1[:change_type]) == [:insert, :insert, :delete]

      assert Enum.map(records, &{&1[:row][:id], &1[:row][:name]}) == [
               {1, "alice"},
               {2, "bob"},
               {1, "alice"}
             ]
    end

    test "nil writes a null into a nullable column", ctx do
      table = create_kv_table(ctx, "null_value", id_name_schema())
      writer = table |> TableUpsert.new!() |> TableUpsert.create_writer!()

      upsert!(writer, [1, nil])
      upsert!(writer, %{id: 2, name: nil})

      records = changelog(table, 2)
      assert Enum.map(records, &{&1[:row][:id], &1[:row][:name]}) == [{1, nil}, {2, nil}]
    end
  end

  describe "partial update" do
    test "leaves untargeted columns untouched", ctx do
      table = create_kv_table(ctx, "partial_name", id_name_email_schema())

      full = table |> TableUpsert.new!() |> TableUpsert.create_writer!()
      upsert!(full, [1, "alice", "alice@example.com"])

      partial =
        table
        |> TableUpsert.new!()
        |> TableUpsert.partial_update_by_name!([:id, :email])
        |> TableUpsert.create_writer!()

      upsert!(partial, %{id: 1, email: "new@example.com"})

      records = changelog(table, 3)
      assert Enum.map(records, & &1[:change_type]) == [:insert, :update_before, :update_after]

      after_image = records |> Enum.at(2) |> Map.fetch!(:row)
      assert after_image[:email] == "new@example.com"
      assert after_image[:name] == "alice"
    end

    test "by index behaves like by name", ctx do
      table = create_kv_table(ctx, "partial_index", id_name_email_schema())

      full = table |> TableUpsert.new!() |> TableUpsert.create_writer!()
      upsert!(full, [1, "alice", "alice@example.com"])

      partial =
        table
        |> TableUpsert.new!()
        |> TableUpsert.partial_update_by_index!([0, 2])
        |> TableUpsert.create_writer!()

      # Positional list follows the target order given above: id, then email.
      upsert!(partial, [1, "new@example.com"])

      after_image = table |> changelog(3) |> Enum.at(2) |> Map.fetch!(:row)
      assert after_image[:email] == "new@example.com"
      assert after_image[:name] == "alice"
    end

    test "column names may be atoms or strings", ctx do
      table = create_kv_table(ctx, "partial_strings", id_name_email_schema())
      builder = TableUpsert.new!(table)

      assert {:ok, _} = TableUpsert.partial_update_by_name(builder, [:id, :email])
      assert {:ok, _} = TableUpsert.partial_update_by_name(builder, ["id", "email"])
    end

    test "is rejected when the primary key is not a target", ctx do
      table = create_kv_table(ctx, "partial_no_pk", id_name_email_schema())

      assert {:error, %Fluss.Error{} = err} =
               table
               |> TableUpsert.new!()
               |> TableUpsert.partial_update_by_name!([:email])
               |> TableUpsert.create_writer()

      assert err.message =~ "primary key"
    end

    test "is rejected when a non-primary-key column is NOT NULL", ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", {:not_null, :string})
        |> Fluss.Schema.primary_key(["id"])

      table = create_kv_table(ctx, "partial_not_null", schema)

      assert {:error, %Fluss.Error{} = err} =
               table
               |> TableUpsert.new!()
               |> TableUpsert.partial_update_by_name!([:id])
               |> TableUpsert.create_writer()

      assert err.message =~ "nullable"
    end

    test "reports an unknown column name", ctx do
      table = create_kv_table(ctx, "partial_unknown", id_name_schema())

      assert {:error, %Fluss.Error{} = err} =
               table |> TableUpsert.new!() |> TableUpsert.partial_update_by_name([:nope])

      assert err.message =~ "nope"
    end

    test "reports an out-of-range column index", ctx do
      table = create_kv_table(ctx, "partial_range", id_name_schema())

      assert {:error, %Fluss.Error{} = err} =
               table |> TableUpsert.new!() |> TableUpsert.partial_update_by_index([99])

      assert err.message =~ "99"
    end

    test "rejects a column index given twice", ctx do
      table = create_kv_table(ctx, "partial_dup_index", id_name_schema())

      assert {:error, %Fluss.Error{} = err} =
               table |> TableUpsert.new!() |> TableUpsert.partial_update_by_index([0, 0])

      assert err.message =~ "duplicate target column `id`"
    end

    test "reports an invalid column index", ctx do
      builder =
        ctx |> create_kv_table("partial_bad_index", id_name_schema()) |> TableUpsert.new!()

      for index <- [-1, 2 ** 70, 1.5, :a] do
        assert {:error, %Fluss.Error{} = err} =
                 TableUpsert.partial_update_by_index(builder, [index])

        assert err.message =~ "invalid target column index"
      end
    end

    test "rejects an improper list of targets", ctx do
      builder =
        ctx |> create_kv_table("partial_improper", id_name_schema()) |> TableUpsert.new!()

      assert {:error, %Fluss.Error{} = err} =
               TableUpsert.partial_update_by_index(builder, [0 | 1])

      assert err.message =~ "expected a list of column indices"

      assert {:error, %Fluss.Error{} = err} =
               TableUpsert.partial_update_by_name(builder, [:id | :name])

      assert err.message =~ "expected a list of column names"
    end

    test "rejects a nil primary key in a partial-update list", ctx do
      table = create_kv_table(ctx, "partial_nil_pk", id_name_email_schema())

      writer =
        table
        |> TableUpsert.new!()
        |> TableUpsert.partial_update_by_name!([:id, :email])
        |> TableUpsert.create_writer!()

      assert {:error, %Fluss.Error{} = err} = UpsertWriter.upsert(writer, [nil, "a@b.c"])
      assert err.message =~ "NOT NULL column `id` must not be nil or omitted"
    end

    test "rejects a column name given twice as atom and string", ctx do
      table = create_kv_table(ctx, "partial_dup_name", id_name_schema())

      assert {:error, %Fluss.Error{} = err} =
               table |> TableUpsert.new!() |> TableUpsert.partial_update_by_name(["id", :id])

      assert err.message =~ "duplicate target column `id`"
    end

    test "rejects a list that does not match the targets", ctx do
      table = create_kv_table(ctx, "partial_list_len", id_name_email_schema())

      writer =
        table
        |> TableUpsert.new!()
        |> TableUpsert.partial_update_by_name!([:id, :email])
        |> TableUpsert.create_writer!()

      assert {:error, %Fluss.Error{} = err} =
               UpsertWriter.upsert(writer, [1, "alice", "alice@example.com"])

      assert err.message =~ "expected 2 values for the target columns, got 3"
    end

    test "rejects a map key for a column that is not a target", ctx do
      table = create_kv_table(ctx, "partial_non_target", id_name_email_schema())

      writer =
        table
        |> TableUpsert.new!()
        |> TableUpsert.partial_update_by_name!(["id", "email"])
        |> TableUpsert.create_writer!()

      assert {:error, %Fluss.Error{} = err} =
               UpsertWriter.upsert(writer, %{"id" => 1, "name" => "alice"})

      assert err.message =~ "column `name` is not accepted here; expected one of: id, email"
    end
  end

  describe "input validation" do
    setup ctx do
      table = create_kv_table(ctx, "validation", id_name_schema())
      %{writer: table |> TableUpsert.new!() |> TableUpsert.create_writer!()}
    end

    test "rejects a list of the wrong length", %{writer: writer} do
      assert {:error, %Fluss.Error{} = err} = UpsertWriter.upsert(writer, [1])
      assert err.message =~ "expected 2 values"
    end

    test "rejects an unknown map key", %{writer: writer} do
      assert {:error, %Fluss.Error{} = err} =
               UpsertWriter.upsert(writer, %{id: 1, nope: "x"})

      assert err.message =~ "column `nope` is unknown"
    end

    test "rejects a missing primary key", %{writer: writer} do
      assert {:error, %Fluss.Error{} = err} = UpsertWriter.upsert(writer, %{name: "alice"})
      assert err.message =~ "NOT NULL column `id` must not be nil or omitted"
    end

    test "rejects an explicit nil primary key", %{writer: writer} do
      assert {:error, %Fluss.Error{} = err} =
               UpsertWriter.upsert(writer, %{id: nil, name: "alice"})

      assert err.message =~ "NOT NULL column `id` must not be nil or omitted"
    end

    test "rejects a nil primary key in a list", %{writer: writer} do
      assert {:error, %Fluss.Error{} = err} = UpsertWriter.upsert(writer, [nil, "alice"])
      assert err.message =~ "NOT NULL column `id` must not be nil or omitted"

      assert {:error, %Fluss.Error{} = err} = UpsertWriter.delete(writer, [nil])
      assert err.message =~ "NOT NULL column `id` must not be nil or omitted"
    end

    test "rejects an improper list", %{writer: writer} do
      assert {:error, %Fluss.Error{} = err} = UpsertWriter.upsert(writer, [1 | "alice"])
      assert err.message =~ "expected a list of values"

      assert {:error, %Fluss.Error{} = err} = UpsertWriter.delete(writer, [1 | 2])
      assert err.message =~ "expected a list of values"
    end

    test "rejects nil in a NOT NULL column in a list", ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", {:not_null, :string})
        |> Fluss.Schema.primary_key(["id"])

      table = create_kv_table(ctx, "not_null_list", schema)
      writer = table |> TableUpsert.new!() |> TableUpsert.create_writer!()

      assert {:error, %Fluss.Error{} = err} = UpsertWriter.upsert(writer, [1, nil])
      assert err.message =~ "NOT NULL column `name` must not be nil or omitted"
    end

    test "rejects a delete whose key is not the primary key", %{writer: writer} do
      assert {:error, %Fluss.Error{} = err} = UpsertWriter.delete(writer, %{name: "alice"})
      assert err.message =~ "column `name` is not accepted here; expected one of: id"
    end

    test "rejects a delete list of the wrong length", %{writer: writer} do
      assert {:error, %Fluss.Error{} = err} = UpsertWriter.delete(writer, [1, "alice"])
      assert err.message =~ "expected 1 values for the target columns, got 2"
    end

    test "rejects a map that names a column twice", %{writer: writer} do
      assert {:error, %Fluss.Error{} = err} =
               UpsertWriter.upsert(writer, %{"id" => 1, :id => 2, :name => "alice"})

      assert err.message =~ "column `id` given more than once"
    end
  end

  describe "full write buffer" do
    # A buffer of one batch fills every few writes, which then take the waiting path.
    setup ctx do
      config =
        ctx.config
        |> Fluss.Config.set_writer_dynamic_batch_size_min(1024)
        |> Fluss.Config.set_writer_batch_size(1024)
        |> Fluss.Config.set_writer_buffer_memory_size(1024)

      %{config: config}
    end

    test "writes wait for room instead of failing", ctx do
      conn = Fluss.Connection.new!(ctx.config)
      table = create_kv_table(%{ctx | conn: conn}, "full_buffer", id_name_schema())
      writer = table |> TableUpsert.new!() |> TableUpsert.create_writer!()

      for id <- 1..200 do
        assert {:ok, _handle} = UpsertWriter.upsert(writer, [id, String.duplicate("x", 50)])
      end

      assert {:ok, _handle} = UpsertWriter.delete(writer, [1])
      assert :ok = UpsertWriter.flush(writer)
      assert length(changelog(table, 201)) == 201
    end

    test "a write into a full buffer waits off the scheduler", ctx do
      conn = Fluss.Connection.new!(ctx.config)
      table = create_kv_table(%{ctx | conn: conn}, "full_buffer_pending", id_name_schema())
      writer = table |> TableUpsert.new!() |> TableUpsert.create_writer!()

      results =
        for id <- 1..200,
            do: Native.upsert_writer_upsert(writer, [id, String.duplicate("x", 50)])

      assert Enum.any?(results, &match?({:pending, _ref}, &1))

      for result <- results do
        assert {:ok, _handle} = Native.await_queued(result)
      end
    end

    # Only the retry can return this error; a full buffer on the first try is retried.
    test "a write that waits past the timeout returns an error", ctx do
      conn =
        ctx.config |> Fluss.Config.set_writer_buffer_wait_timeout_ms(1) |> Fluss.Connection.new!()

      table = create_kv_table(%{ctx | conn: conn}, "buffer_timeout", id_name_schema())
      writer = table |> TableUpsert.new!() |> TableUpsert.create_writer!()

      results = for id <- 1..200, do: UpsertWriter.upsert(writer, [id, String.duplicate("x", 50)])

      assert Enum.any?(results, fn
               {:error, %Fluss.Error{message: message}} -> message =~ "buffer exhausted"
               _ -> false
             end)
    end
  end

  describe "composite primary key" do
    test "a list key follows primary key declaration order, not column order", ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("a", :int)
        |> Fluss.Schema.column("b", :int)
        |> Fluss.Schema.column("v", :string)
        |> Fluss.Schema.primary_key(["b", "a"])

      table = create_kv_table(ctx, "composite", schema, ["b"])
      writer = table |> TableUpsert.new!() |> TableUpsert.create_writer!()

      upsert!(writer, %{a: 1, b: 2, v: "x"})
      upsert!(writer, %{a: 2, b: 1, v: "y"})

      # [b, a]: deletes a=1, b=2. Column order would have deleted the other row.
      delete!(writer, [2, 1])

      records = changelog(table, 3)
      assert Enum.map(records, & &1[:change_type]) == [:insert, :insert, :delete]

      deleted = records |> Enum.at(2) |> Map.fetch!(:row)
      assert {deleted[:a], deleted[:b], deleted[:v]} == {1, 2, "x"}
    end
  end

  describe "table kind" do
    test "new/1 rejects a log table", %{conn: conn, admin: admin} do
      table_name = unique("log_table")
      cleanup_table(admin, table_name)
      on_exit(fn -> cleanup_table(admin, table_name) end)

      schema = Fluss.Schema.new() |> Fluss.Schema.column("id", :int)
      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)

      table = Fluss.Table.get!(conn, @database, table_name)

      assert {:error, %Fluss.Error{} = err} = TableUpsert.new(table)
      assert err.message =~ "primary key"
    end
  end

  describe "complex types" do
    test "round-trips array, map and row values through upsert", ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("tags", {:array, :string})
        |> Fluss.Schema.column("attrs", {:map, :string, :int})
        |> Fluss.Schema.column("point", {:row, [{"x", :int}, {"y", :int}]})
        |> Fluss.Schema.primary_key(["id"])

      table = create_kv_table(ctx, "complex", schema)
      writer = table |> TableUpsert.new!() |> TableUpsert.create_writer!()

      upsert!(writer, %{
        id: 1,
        tags: ["a", "b"],
        attrs: %{"x" => 1},
        point: %{x: 10, y: 20}
      })

      row = table |> changelog(1) |> Enum.at(0) |> Map.fetch!(:row)
      assert row[:tags] == ["a", "b"]
      assert row[:attrs] == %{"x" => 1}
      assert row[:point] == %{x: 10, y: 20}
    end

    test "rejects an improper list for an array", ctx do
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("tags", {:array, :string})
        |> Fluss.Schema.primary_key(["id"])

      table = create_kv_table(ctx, "improper_array", schema)
      writer = table |> TableUpsert.new!() |> TableUpsert.create_writer!()

      assert {:error, %Fluss.Error{} = err} = UpsertWriter.upsert(writer, [1, ["a" | "b"]])
      assert err.message =~ "expected a list for array"
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

  # One bucket so changelog offsets stay contiguous and ordered.
  defp create_kv_table(%{conn: conn, admin: admin}, label, schema, bucket_keys \\ ["id"]) do
    table_name = unique(label)
    cleanup_table(admin, table_name)
    on_exit(fn -> cleanup_table(admin, table_name) end)

    descriptor = Fluss.TableDescriptor.new!(schema, bucket_count: 1, bucket_keys: bucket_keys)
    :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)

    Fluss.Table.get!(conn, @database, table_name)
  end

  # Awaiting every write is what makes the changelog order deterministic.
  defp upsert!(writer, values) do
    {:ok, handle} = UpsertWriter.upsert(writer, values)
    :ok = Fluss.WriteHandle.wait(handle)
  end

  defp delete!(writer, primary_key) do
    {:ok, handle} = UpsertWriter.delete(writer, primary_key)
    :ok = Fluss.WriteHandle.wait(handle)
  end

  defp changelog(table, expected_count) do
    scanner = Fluss.LogScanner.new!(table)
    :ok = Fluss.LogScanner.subscribe(scanner, 0, Fluss.earliest_offset())
    records = poll_records(scanner, expected_count)
    assert length(records) == expected_count
    records
  end

  defp unique(label), do: "ex_test_kv_#{label}_#{:rand.uniform(100_000)}"

  defp cleanup_table(admin, table_name) do
    Fluss.Admin.drop_table(admin, @database, table_name, true)
  end

  defp poll_records(scanner, expected_count, timeout_ms \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_poll(scanner, expected_count, deadline, [])
  end

  defp do_poll(_scanner, expected_count, _deadline, acc) when length(acc) >= expected_count do
    acc
  end

  defp do_poll(scanner, expected_count, deadline, acc) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      acc
    else
      :ok = Fluss.LogScanner.poll(scanner, min(5_000, remaining))

      receive do
        {:fluss_records, records} ->
          do_poll(scanner, expected_count, deadline, acc ++ records)

        {:fluss_poll_error, reason} ->
          IO.warn("poll error during test: #{inspect(reason)}")
          do_poll(scanner, expected_count, deadline, acc)
      after
        min(6_000, remaining) ->
          acc
      end
    end
  end
end
