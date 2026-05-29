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

defmodule Fluss.Integration.LogTableTest do
  use ExUnit.Case, async: false

  alias Fluss.Test.Cluster

  @moduletag :integration

  @database "fluss"

  setup_all do
    case Cluster.ensure_started() do
      {:ok, servers} ->
        config = Fluss.Config.new(servers)

        # Wait for cluster to be fully ready (connection + admin working)
        {conn, admin} = connect_with_retry(config, 90)

        %{conn: conn, admin: admin, config: config}

      {:error, reason} ->
        raise "Failed to start Fluss cluster: #{reason}"
    end
  end

  describe "append and scan" do
    test "append rows and scan with log scanner", %{conn: conn, admin: admin} do
      table_name = "ex_test_append_and_scan_#{:rand.uniform(100_000)}"
      cleanup_table(admin, table_name)

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("c1", :int)
        |> Fluss.Schema.column("c2", :string)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)

      table = Fluss.Table.get!(conn, @database, table_name)
      writer = Fluss.AppendWriter.new!(table)

      # Append 6 rows
      for {c1, c2} <- [{1, "a1"}, {2, "a2"}, {3, "a3"}, {4, "a4"}, {5, "a5"}, {6, "a6"}] do
        {:ok, _} = Fluss.AppendWriter.append(writer, [c1, c2])
      end

      :ok = Fluss.AppendWriter.flush(writer)

      # Scan all records
      scanner = Fluss.LogScanner.new!(table)
      :ok = Fluss.LogScanner.subscribe(scanner, 0, Fluss.earliest_offset())

      records = poll_records(scanner, 6)

      assert length(records) == 6

      sorted = Enum.sort_by(records, fn r -> r[:row][:c1] end)

      for {record, i} <- Enum.with_index(sorted, 1) do
        assert record[:row][:c1] == i
        assert record[:row][:c2] == "a#{i}"
        assert record[:change_type] == :append_only
      end

      # Unsubscribe should not error
      :ok = Fluss.LogScanner.unsubscribe(scanner, 0)

      cleanup_table(admin, table_name)
    end

    test "append with nil values", %{conn: conn, admin: admin} do
      table_name = "ex_test_append_nil_#{:rand.uniform(100_000)}"
      cleanup_table(admin, table_name)

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)

      table = Fluss.Table.get!(conn, @database, table_name)
      writer = Fluss.AppendWriter.new!(table)

      {:ok, _} = Fluss.AppendWriter.append(writer, [1, nil])
      {:ok, _} = Fluss.AppendWriter.append(writer, [2, "present"])
      :ok = Fluss.AppendWriter.flush(writer)

      scanner = Fluss.LogScanner.new!(table)
      :ok = Fluss.LogScanner.subscribe(scanner, 0, Fluss.earliest_offset())

      records = poll_records(scanner, 2)
      assert length(records) == 2

      sorted = Enum.sort_by(records, fn r -> r[:row][:id] end)
      assert Enum.at(sorted, 0)[:row][:name] == nil
      assert Enum.at(sorted, 1)[:row][:name] == "present"

      cleanup_table(admin, table_name)
    end
  end

  describe "multiple data types" do
    test "tinyint, smallint, int, bigint, float, double, string, boolean", %{
      conn: conn,
      admin: admin
    } do
      table_name = "ex_test_data_types_#{:rand.uniform(100_000)}"
      cleanup_table(admin, table_name)

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("a_tinyint", :tinyint)
        |> Fluss.Schema.column("b_smallint", :smallint)
        |> Fluss.Schema.column("c_int", :int)
        |> Fluss.Schema.column("d_bigint", :bigint)
        |> Fluss.Schema.column("e_float", :float)
        |> Fluss.Schema.column("f_double", :double)
        |> Fluss.Schema.column("g_string", :string)
        |> Fluss.Schema.column("h_bool", :boolean)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)

      table = Fluss.Table.get!(conn, @database, table_name)
      writer = Fluss.AppendWriter.new!(table)

      {:ok, _} =
        Fluss.AppendWriter.append(writer, [
          127,
          32_000,
          42,
          1_000_000_000_000,
          3.14,
          2.718281828,
          "hello",
          true
        ])

      {:ok, _} =
        Fluss.AppendWriter.append(writer, [-128, -32_000, -1, -999, 0.0, -1.5, "", false])

      :ok = Fluss.AppendWriter.flush(writer)

      scanner = Fluss.LogScanner.new!(table)
      :ok = Fluss.LogScanner.subscribe(scanner, 0, Fluss.earliest_offset())

      records = poll_records(scanner, 2)
      assert length(records) == 2

      sorted = Enum.sort_by(records, fn r -> r[:row][:c_int] end)
      row1 = Enum.at(sorted, 0)[:row]
      row2 = Enum.at(sorted, 1)[:row]

      assert row1[:a_tinyint] == -128
      assert row1[:b_smallint] == -32_000
      assert row1[:c_int] == -1
      assert row1[:d_bigint] == -999
      assert row1[:g_string] == ""
      assert row1[:h_bool] == false

      assert row2[:a_tinyint] == 127
      assert row2[:b_smallint] == 32_000
      assert row2[:c_int] == 42
      assert row2[:d_bigint] == 1_000_000_000_000
      assert row2[:g_string] == "hello"
      assert row2[:h_bool] == true

      cleanup_table(admin, table_name)
    end
  end

  describe "subscribe_buckets" do
    test "subscribe to multiple buckets at once", %{conn: conn, admin: admin} do
      table_name = "ex_test_subscribe_buckets_#{:rand.uniform(100_000)}"
      cleanup_table(admin, table_name)

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("val", :string)

      descriptor = Fluss.TableDescriptor.new!(schema, bucket_count: 3)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)

      table = Fluss.Table.get!(conn, @database, table_name)
      writer = Fluss.AppendWriter.new!(table)

      for i <- 1..9 do
        {:ok, _} = Fluss.AppendWriter.append(writer, [i, "v#{i}"])
      end

      :ok = Fluss.AppendWriter.flush(writer)

      scanner = Fluss.LogScanner.new!(table)
      earliest = Fluss.earliest_offset()

      :ok =
        Fluss.LogScanner.subscribe_buckets(scanner, [
          {0, earliest},
          {1, earliest},
          {2, earliest}
        ])

      records = poll_records(scanner, 9)
      assert length(records) == 9

      ids = records |> Enum.map(fn r -> r[:row][:id] end) |> Enum.sort()
      assert ids == Enum.to_list(1..9)

      cleanup_table(admin, table_name)
    end
  end

  describe "admin operations" do
    test "create and drop database", %{admin: admin} do
      db_name = "ex_test_db_#{:rand.uniform(100_000)}"
      :ok = Fluss.Admin.create_database(admin, db_name, true)

      {:ok, databases} = Fluss.Admin.list_databases(admin)
      assert db_name in databases

      :ok = Fluss.Admin.drop_database(admin, db_name, true)
    end

    test "list tables", %{admin: admin} do
      table_name = "ex_test_list_tables_#{:rand.uniform(100_000)}"
      cleanup_table(admin, table_name)

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)

      {:ok, tables} = Fluss.Admin.list_tables(admin, @database)
      assert table_name in tables

      cleanup_table(admin, table_name)
    end

    test "table metadata", %{conn: conn, admin: admin} do
      table_name = "ex_test_table_meta_#{:rand.uniform(100_000)}"
      cleanup_table(admin, table_name)

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)

      table = Fluss.Table.get!(conn, @database, table_name)
      assert Fluss.Table.has_primary_key?(table) == false
      assert Fluss.Table.column_names(table) == ["id", "name"]

      cleanup_table(admin, table_name)
    end
  end

  describe "scan from offset" do
    test "subscribe from specific offset skips earlier records", %{conn: conn, admin: admin} do
      table_name = "ex_test_scan_offset_#{:rand.uniform(100_000)}"
      cleanup_table(admin, table_name)

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)

      table = Fluss.Table.get!(conn, @database, table_name)
      writer = Fluss.AppendWriter.new!(table)

      for i <- 1..5 do
        {:ok, _} = Fluss.AppendWriter.append(writer, [i])
      end

      :ok = Fluss.AppendWriter.flush(writer)

      # Subscribe from offset 3, should skip first 3 records
      scanner = Fluss.LogScanner.new!(table)
      :ok = Fluss.LogScanner.subscribe(scanner, 0, 3)

      records = poll_records(scanner, 2)
      assert length(records) == 2

      ids = records |> Enum.map(fn r -> r[:row][:id] end) |> Enum.sort()
      assert ids == [4, 5]

      cleanup_table(admin, table_name)
    end
  end

  describe "multiple flushes" do
    test "append, flush, append more, flush, scan all", %{conn: conn, admin: admin} do
      table_name = "ex_test_multi_flush_#{:rand.uniform(100_000)}"
      cleanup_table(admin, table_name)

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("batch", :string)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table_name, descriptor, false)

      table = Fluss.Table.get!(conn, @database, table_name)
      writer = Fluss.AppendWriter.new!(table)

      # First batch
      {:ok, _} = Fluss.AppendWriter.append(writer, [1, "first"])
      {:ok, _} = Fluss.AppendWriter.append(writer, [2, "first"])
      :ok = Fluss.AppendWriter.flush(writer)

      # Second batch
      {:ok, _} = Fluss.AppendWriter.append(writer, [3, "second"])
      {:ok, _} = Fluss.AppendWriter.append(writer, [4, "second"])
      :ok = Fluss.AppendWriter.flush(writer)

      scanner = Fluss.LogScanner.new!(table)
      :ok = Fluss.LogScanner.subscribe(scanner, 0, Fluss.earliest_offset())

      records = poll_records(scanner, 4)
      assert length(records) == 4

      sorted = Enum.sort_by(records, fn r -> r[:row][:id] end)
      assert Enum.at(sorted, 0)[:row][:batch] == "first"
      assert Enum.at(sorted, 1)[:row][:batch] == "first"
      assert Enum.at(sorted, 2)[:row][:batch] == "second"
      assert Enum.at(sorted, 3)[:row][:batch] == "second"

      cleanup_table(admin, table_name)
    end
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

  defp cleanup_table(admin, table_name) do
    Fluss.Admin.drop_table(admin, @database, table_name, true)
  end

  defp connect_with_retry(config, timeout_s) do
    deadline = System.monotonic_time(:second) + timeout_s
    do_connect_retry(config, deadline, nil)
  end

  defp do_connect_retry(config, deadline, last_error) do
    if System.monotonic_time(:second) >= deadline do
      raise "Could not connect to Fluss cluster: #{inspect(last_error)}"
    end

    try do
      conn = Fluss.Connection.new!(config)
      admin = Fluss.Admin.new!(conn)
      {:ok, _databases} = Fluss.Admin.list_databases(admin)
      {conn, admin}
    rescue
      e ->
        Process.sleep(2_000)
        do_connect_retry(config, deadline, e)
    end
  end
end
