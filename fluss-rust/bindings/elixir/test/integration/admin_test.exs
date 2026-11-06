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

defmodule Fluss.Integration.AdminTest do
  use Fluss.Test.IntegrationCase, async: false

  @database "fluss"

  describe "get_server_nodes/1" do
    test "returns a non-empty list of %Fluss.ServerNode{} structs", %{admin: admin} do
      assert {:ok, nodes} = Fluss.Admin.get_server_nodes(admin)
      assert is_list(nodes)
      assert nodes != []

      for node <- nodes do
        assert %Fluss.ServerNode{} = node
        assert is_integer(node.id)
        assert is_binary(node.uid) and node.uid != ""
        assert is_binary(node.host) and node.host != ""
        assert is_integer(node.port) and node.port > 0
        assert node.server_type in [:coordinator_server, :tablet_server]
      end
    end

    test "cluster has exactly one coordinator", %{admin: admin} do
      {:ok, nodes} = Fluss.Admin.get_server_nodes(admin)
      coordinators = Enum.filter(nodes, &(&1.server_type == :coordinator_server))

      assert [_coordinator] = coordinators
    end
  end

  describe "get_server_nodes!/1" do
    test "returns the list directly without an :ok tuple", %{admin: admin} do
      nodes = Fluss.Admin.get_server_nodes!(admin)
      assert is_list(nodes)
      assert nodes != []
      assert %Fluss.ServerNode{} = hd(nodes)
    end
  end

  describe "create_database/3 with a descriptor" do
    test "accepts a comment-only descriptor", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      descriptor = Fluss.DatabaseDescriptor.new() |> Fluss.DatabaseDescriptor.comment("hello")
      :ok = Fluss.Admin.create_database(admin, db, descriptor)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true) end)

      assert {:ok, info} = Fluss.Admin.get_database_info(admin, db)
      assert info.descriptor.comment == "hello"
      assert info.descriptor.custom_properties == %{}
    end

    test "accepts a properties-only descriptor", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"

      descriptor =
        Fluss.DatabaseDescriptor.new()
        |> Fluss.DatabaseDescriptor.put_custom_property("region", "eu-west-1")

      :ok = Fluss.Admin.create_database(admin, db, descriptor)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true) end)

      assert {:ok, info} = Fluss.Admin.get_database_info(admin, db)
      assert info.descriptor.custom_properties == %{"region" => "eu-west-1"}
    end

    test "accepts a struct constructed directly without builders", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      descriptor = %Fluss.DatabaseDescriptor{comment: "direct", custom_properties: %{"k" => "v"}}
      :ok = Fluss.Admin.create_database(admin, db, descriptor)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true) end)

      assert {:ok, info} = Fluss.Admin.get_database_info(admin, db)
      assert info.descriptor.comment == "direct"
      assert info.descriptor.custom_properties == %{"k" => "v"}
    end
  end

  describe "create_database/4 with a descriptor" do
    test "accepts a descriptor and returns :ok if the database exists when ignore_if_exists is true",
         %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      descriptor = %Fluss.DatabaseDescriptor{comment: "direct", custom_properties: %{"k" => "v"}}
      :ok = Fluss.Admin.create_database(admin, db, descriptor, ignore_if_exists: true)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true) end)

      descriptor = %Fluss.DatabaseDescriptor{comment: "updated", custom_properties: %{}}

      :ok = Fluss.Admin.create_database(admin, db, descriptor, ignore_if_exists: true)
      assert {:ok, info} = Fluss.Admin.get_database_info(admin, db)

      # the 2nd create was skipped due to `ignore_if_exists: true`
      assert info.descriptor.comment == "direct"
      assert info.descriptor.custom_properties == %{"k" => "v"}
    end

    test "fails if the database exists and ignore_if_exists is false", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      descriptor = %Fluss.DatabaseDescriptor{comment: "direct", custom_properties: %{"k" => "v"}}
      :ok = Fluss.Admin.create_database(admin, db, descriptor, ignore_if_exists: true)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true) end)

      assert {:error, %Fluss.Error{}} = Fluss.Admin.create_database(admin, db, descriptor)
    end
  end

  describe "options" do
    test "an existing or missing table is an error unless ignored", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      descriptor =
        Fluss.TableDescriptor.new!(Fluss.Schema.new() |> Fluss.Schema.column("id", :int))

      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor)

      assert {:error, %Fluss.Error{code: :table_already_exist}} =
               Fluss.Admin.create_table(admin, @database, table, descriptor)

      assert :ok =
               Fluss.Admin.create_table(admin, @database, table, descriptor,
                 ignore_if_exists: true
               )

      :ok = Fluss.Admin.drop_table(admin, @database, table)

      assert {:error, %Fluss.Error{code: :table_not_exist}} =
               Fluss.Admin.drop_table(admin, @database, table)

      assert :ok = Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
    end

    test "a missing database is an error unless ignored", %{admin: admin} do
      db = "fluss_missing_#{:rand.uniform(100_000)}"

      assert {:error, %Fluss.Error{}} = Fluss.Admin.drop_database(admin, db)
      assert :ok = Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true)
    end

    test "drop_database cascades by default", %{admin: admin} do
      schema = Fluss.Schema.new() |> Fluss.Schema.column("id", :int)

      for cascade <- [true, false] do
        db = "fluss_cascade_#{:rand.uniform(100_000)}"
        :ok = Fluss.Admin.create_database(admin, db, Fluss.DatabaseDescriptor.new())
        on_exit(fn -> Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true) end)
        :ok = Fluss.Admin.create_table(admin, db, "t", Fluss.TableDescriptor.new!(schema))

        if cascade do
          assert :ok = Fluss.Admin.drop_database(admin, db)
          refute Fluss.Admin.database_exists!(admin, db)
        else
          assert {:error, %Fluss.Error{}} = Fluss.Admin.drop_database(admin, db, cascade: false)
          assert Fluss.Admin.database_exists!(admin, db)
        end
      end
    end

    test "raises on malformed options", %{admin: admin} do
      assert_raise ArgumentError, fn ->
        Fluss.Admin.drop_table(admin, @database, "t", ignre_if_not_exists: true)
      end

      assert_raise ArgumentError, ~r/expected :cascade to be a boolean/, fn ->
        Fluss.Admin.drop_database(admin, "db", cascade: "yes")
      end

      assert_raise ArgumentError, ~r/expected options to be a keyword list/, fn ->
        Fluss.Admin.drop_table(admin, @database, "t", %{ignore_if_not_exists: true})
      end

      assert_raise ArgumentError, ~r/expected a %Fluss.DatabaseDescriptor\{\}, got/, fn ->
        Fluss.Admin.create_database(admin, "db", ignore_if_exists: true)
      end
    end

    test "raises on a non-string name", %{admin: admin} do
      assert_raise ArgumentError, ~r/expected the table name to be a string, got: nil/, fn ->
        Fluss.Admin.table_exists(admin, @database, nil)
      end

      assert_raise ArgumentError, ~r/expected the database name to be a string, got: :db/, fn ->
        Fluss.Admin.list_tables(admin, :db)
      end
    end
  end

  describe "get_database_info/2" do
    test "returns DatabaseInfo with the descriptor used at creation", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"

      descriptor =
        Fluss.DatabaseDescriptor.new()
        |> Fluss.DatabaseDescriptor.comment("Integration test database")
        |> Fluss.DatabaseDescriptor.put_custom_property("owner", "ci")
        |> Fluss.DatabaseDescriptor.put_custom_property("env", "test")

      :ok = Fluss.Admin.create_database(admin, db, descriptor)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true) end)

      assert {:ok, %Fluss.DatabaseInfo{} = info} = Fluss.Admin.get_database_info(admin, db)
      assert info.database_name == db
      assert info.descriptor.comment == "Integration test database"
      assert info.descriptor.custom_properties == %{"owner" => "ci", "env" => "test"}
      assert is_integer(info.created_time)
      assert is_integer(info.modified_time)
    end

    test "returns DatabaseInfo for a database created with an empty descriptor", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      :ok = Fluss.Admin.create_database(admin, db, Fluss.DatabaseDescriptor.new())
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true) end)

      assert {:ok, %Fluss.DatabaseInfo{} = info} = Fluss.Admin.get_database_info(admin, db)
      assert info.database_name == db
      assert %Fluss.DatabaseDescriptor{} = info.descriptor
      assert is_integer(info.created_time)
      assert is_integer(info.modified_time)
    end

    test "returns error for a non-existent database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      assert {:error, %Fluss.Error{}} = Fluss.Admin.get_database_info(admin, db)
    end
  end

  describe "database_exists/2" do
    test "returns {:ok, true} for an existing database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"

      :ok = Fluss.Admin.create_database(admin, db, Fluss.DatabaseDescriptor.new())
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true) end)

      assert {:ok, true} = Fluss.Admin.database_exists(admin, db)
    end

    test "returns {:ok, false} for a non-existent database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"

      assert Fluss.Admin.database_exists(admin, db) == {:ok, false}
    end
  end

  describe "database_exists!/2" do
    test "returns true for an existing database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"

      :ok = Fluss.Admin.create_database(admin, db, Fluss.DatabaseDescriptor.new())
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, ignore_if_not_exists: true) end)

      assert Fluss.Admin.database_exists!(admin, db)
    end

    test "returns false for a non-existent database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"

      refute Fluss.Admin.database_exists!(admin, db)
    end
  end

  describe "table_exists/3" do
    test "returns {:ok, true} for an existing table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("c1", :int)
        |> Fluss.Schema.column("c2", :string)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      assert {:ok, true} = Fluss.Admin.table_exists(admin, @database, table)
    end

    test "returns {:ok, false} for a non-existent table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"
      assert {:ok, false} = Fluss.Admin.table_exists(admin, @database, table)
    end
  end

  describe "table_exists!/3" do
    test "returns true for an existing table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("c1", :int)
        |> Fluss.Schema.column("c2", :string)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      assert Fluss.Admin.table_exists!(admin, @database, table)
    end

    test "returns false for a non-existent table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"
      refute Fluss.Admin.table_exists!(admin, @database, table)
    end
  end

  describe "get_table_info/3" do
    test "returns a %Fluss.TableInfo{} with the table's metadata", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)
        |> Fluss.Schema.primary_key(["id"])

      descriptor = Fluss.TableDescriptor.new!(schema, bucket_count: 4)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      assert {:ok, info} = Fluss.Admin.get_table_info(admin, @database, table)
      assert %Fluss.TableInfo{} = info

      # Fields we set explicitly — exact assertions.
      assert info.database_name == @database
      assert info.table_name == table
      assert info.num_buckets == 4
      assert info.has_primary_key == true
      assert info.primary_keys == ["id"]

      # Schema round-trips: columns in definition order, PK preserved.
      assert info.schema == %Fluss.Schema{
               columns: [{"id", {:not_null, :int}}, {"name", :string}],
               primary_key: ["id"]
             }

      assert info.is_partitioned == false
      assert info.is_auto_partitioned == false
      assert info.partition_keys == []
      assert info.comment == nil

      assert info.physical_primary_keys == ["id"]
      assert info.bucket_keys == ["id"]
      assert info.has_bucket_key == true
      assert info.is_default_bucket_key == true

      # Server-assigned — sanity only, values aren't predictable.
      assert is_integer(info.table_id) and info.table_id > 0
      assert is_integer(info.schema_id) and info.schema_id >= 1
      assert is_integer(info.created_time) and info.created_time > 0
      assert is_integer(info.modified_time) and info.modified_time > 0

      assert is_map(info.properties)
      assert is_map(info.custom_properties)
    end

    test "exposes a composite primary key", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("region", :string)
        |> Fluss.Schema.column("name", :string)
        |> Fluss.Schema.primary_key(["id", "region"])

      descriptor = Fluss.TableDescriptor.new!(schema, bucket_count: 4)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      assert {:ok, info} = Fluss.Admin.get_table_info(admin, @database, table)

      assert info.has_primary_key == true
      assert info.primary_keys == ["id", "region"]
      assert info.physical_primary_keys == ["id", "region"]

      assert info.schema == %Fluss.Schema{
               columns: [
                 {"id", {:not_null, :int}},
                 {"region", {:not_null, :string}},
                 {"name", :string}
               ],
               primary_key: ["id", "region"]
             }
    end

    test "returns {:error, %Fluss.Error{}} for a non-existent table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"
      assert {:error, %Fluss.Error{}} = Fluss.Admin.get_table_info(admin, @database, table)
    end
  end

  describe "get_table_info!/3" do
    test "returns the %Fluss.TableInfo{} directly without an :ok tuple", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      info = Fluss.Admin.get_table_info!(admin, @database, table)
      assert %Fluss.TableInfo{} = info
      assert info.table_name == table
    end

    test "raises Fluss.Error for a non-existent table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      assert_raise Fluss.Error, fn ->
        Fluss.Admin.get_table_info!(admin, @database, table)
      end
    end
  end

  describe "get_table_schema/4" do
    test "returns the current schema as %Fluss.SchemaInfo{}", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)
        |> Fluss.Schema.primary_key(["id"])

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      assert {:ok, %Fluss.SchemaInfo{} = info} =
               Fluss.Admin.get_table_schema(admin, @database, table)

      assert info.schema == %Fluss.Schema{
               columns: [{"id", {:not_null, :int}}, {"name", :string}],
               primary_key: ["id"]
             }

      assert is_integer(info.schema_id) and info.schema_id >= 1
    end

    test "round-trips complex types and nullability", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("tags", {:array, :string})
        |> Fluss.Schema.column("scores", {:array, {:not_null, :int}})
        |> Fluss.Schema.column("attrs", {:map, :string, :int})
        |> Fluss.Schema.column("point", {:row, [{"x", :int}, {"y", :int}]})
        |> Fluss.Schema.primary_key(["id"])

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      assert {:ok, info} = Fluss.Admin.get_table_schema(admin, @database, table)

      assert info.schema == %Fluss.Schema{
               columns: [
                 {"id", {:not_null, :int}},
                 {"tags", {:array, :string}},
                 {"scores", {:array, {:not_null, :int}}},
                 # map keys are non-null by Fluss spec, so the `:string`
                 # key reads back wrapped even though it was written bare.
                 {"attrs", {:map, {:not_null, :string}, :int}},
                 {"point", {:row, [{"x", :int}, {"y", :int}]}}
               ],
               primary_key: ["id"]
             }
    end

    test "fetches a specific schema version by id", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)
        |> Fluss.Schema.primary_key(["id"])

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      # Without alter_table there's only one version, so requesting its id
      # explicitly must return the same SchemaInfo as the latest (nil) lookup.
      {:ok, current} = Fluss.Admin.get_table_schema(admin, @database, table)

      assert {:ok, by_id} =
               Fluss.Admin.get_table_schema(admin, @database, table, current.schema_id)

      assert by_id == current
    end

    test "returns {:error, %Fluss.Error{}} for a non-existent table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      assert {:error, %Fluss.Error{}} =
               Fluss.Admin.get_table_schema(admin, @database, table)
    end
  end

  describe "create_table/5 with descriptor options" do
    test "round-trips comment and custom_properties", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)

      descriptor =
        Fluss.TableDescriptor.new!(schema,
          comment: "events table",
          custom_properties: %{"owner" => "data-platform"}
        )

      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      assert {:ok, info} = Fluss.Admin.get_table_info(admin, @database, table)
      assert info.comment == "events table"
      assert info.custom_properties == %{"owner" => "data-platform"}
    end

    test "round-trips explicit bucket_keys on a primary-key table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("region", :string)
        |> Fluss.Schema.primary_key(["id", "region"])

      descriptor = Fluss.TableDescriptor.new!(schema, bucket_count: 3, bucket_keys: ["id"])

      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      assert {:ok, info} = Fluss.Admin.get_table_info(admin, @database, table)
      assert info.num_buckets == 3
      assert info.bucket_keys == ["id"]
      # An explicit, non-default bucket key (the default would be the full PK).
      assert info.is_default_bucket_key == false
    end

    test "round-trips partition_keys and reports the table as partitioned", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      # Partition keys must be a subset of the primary key for PK tables.
      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("dt", :string)
        |> Fluss.Schema.primary_key(["id", "dt"])

      descriptor = Fluss.TableDescriptor.new!(schema, partition_keys: ["dt"], bucket_count: 2)

      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, ignore_if_exists: true)

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      assert {:ok, info} = Fluss.Admin.get_table_info(admin, @database, table)
      assert info.is_partitioned == true
      assert info.partition_keys == ["dt"]
    end
  end

  describe "partitions" do
    test "create, list and drop a partition", %{admin: admin} do
      table = partitioned_table(admin, ["region"])

      assert :ok = Fluss.Admin.create_partition(admin, @database, table, %{region: "EU"})

      assert {:ok, [%Fluss.PartitionInfo{} = info]} =
               Fluss.Admin.list_partition_infos(admin, @database, table)

      assert is_integer(info.partition_id)
      assert info.partition_name == "EU"
      assert info.partition_spec == %{"region" => "EU"}
      assert info.bucket_count == 2

      assert :ok = Fluss.Admin.drop_partition(admin, @database, table, %{"region" => "EU"})
      assert Fluss.Admin.list_partition_infos!(admin, @database, table) == []
    end

    test "an existing or missing partition is an error unless ignored", %{admin: admin} do
      table = partitioned_table(admin, ["region"])
      spec = %{region: "EU"}
      :ok = Fluss.Admin.create_partition(admin, @database, table, spec)

      assert {:error, %Fluss.Error{code: :partition_already_exists}} =
               Fluss.Admin.create_partition(admin, @database, table, spec)

      assert :ok =
               Fluss.Admin.create_partition(admin, @database, table, spec, ignore_if_exists: true)

      missing = %{region: "US"}

      assert {:error, %Fluss.Error{code: :partition_not_exists}} =
               Fluss.Admin.drop_partition(admin, @database, table, missing)

      assert :ok =
               Fluss.Admin.drop_partition(admin, @database, table, missing,
                 ignore_if_not_exists: true
               )
    end

    test "a partial spec filters the list", %{admin: admin} do
      table = partitioned_table(admin, ["region", "dt"])

      for region <- ["EU", "US"] do
        :ok =
          Fluss.Admin.create_partition(admin, @database, table, %{region: region, dt: "2026"})
      end

      assert [%Fluss.PartitionInfo{partition_name: "EU$2026"}] =
               Fluss.Admin.list_partition_infos!(admin, @database, table, %{region: "EU"})

      assert length(Fluss.Admin.list_partition_infos!(admin, @database, table)) == 2
    end

    test "a created partition accepts writes", %{conn: conn, admin: admin} do
      table_name = partitioned_table(admin, ["region"])
      :ok = Fluss.Admin.create_partition(admin, @database, table_name, %{region: "EU"})
      table = Fluss.Table.get!(conn, @database, table_name)
      writer = table |> Fluss.TableUpsert.new!() |> Fluss.TableUpsert.create_writer!()

      {:ok, handle} = Fluss.UpsertWriter.upsert(writer, %{id: 1, region: "EU"})
      assert :ok = Fluss.WriteHandle.wait(handle)

      assert Fluss.Lookuper.lookup(Fluss.Lookuper.new!(table), %{id: 1, region: "EU"}) ==
               {:ok, %{id: 1, region: "EU"}}
    end

    test "a write to a missing partition creates it without blocking other writes", %{
      conn: conn,
      admin: admin
    } do
      table_name = partitioned_table(admin, ["region"])
      :ok = Fluss.Admin.create_partition(admin, @database, table_name, %{region: "EU"})
      table = Fluss.Table.get!(conn, @database, table_name)
      writer = table |> Fluss.TableUpsert.new!() |> Fluss.TableUpsert.create_writer!()

      {:ok, missing} = Fluss.UpsertWriter.upsert(writer, %{id: 1, region: "US"})
      {:ok, existing} = Fluss.UpsertWriter.upsert(writer, %{id: 2, region: "EU"})

      assert :ok = Fluss.WriteHandle.wait(missing)
      assert :ok = Fluss.WriteHandle.wait(existing)

      assert ["EU", "US"] =
               Fluss.Admin.list_partition_infos!(admin, @database, table_name)
               |> Enum.map(& &1.partition_name)
               |> Enum.sort()
    end

    test "raises on a malformed spec", %{admin: admin} do
      table = partitioned_table(admin, ["region"])

      for {spec, message} <- [
            {%{region: 2026}, "partition value for `region` must be a string"},
            {%{:region => "EU", "region" => "US"},
             "partition column `region` given more than once"},
            {[region: "EU"], "expected a map of partition column to value"}
          ] do
        assert_raise ArgumentError, ~r/#{Regex.escape(message)}/, fn ->
          Fluss.Admin.create_partition(admin, @database, table, spec)
        end
      end

      assert_raise ArgumentError, fn ->
        Fluss.Admin.list_partition_infos(admin, @database, table, %{region: 1})
      end
    end

    test "a non-partitioned table returns core's error", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"
      schema = Fluss.Schema.new() |> Fluss.Schema.column("id", :int)
      :ok = Fluss.Admin.create_table(admin, @database, table, Fluss.TableDescriptor.new!(schema))

      on_exit(fn ->
        Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true)
      end)

      assert {:error, %Fluss.Error{}} =
               Fluss.Admin.create_partition(admin, @database, table, %{region: "EU"})
    end
  end

  describe "offsets" do
    test "earliest and latest offsets of a bucket", %{conn: conn, admin: admin} do
      table = log_table(admin)
      append!(conn, table, [[1], [2], [3]])

      assert Fluss.Admin.list_offsets(admin, @database, table, [0], :earliest) == {:ok, %{0 => 0}}
      assert Fluss.Admin.list_offsets(admin, @database, table, [0], :latest) == {:ok, %{0 => 3}}
    end

    test "a timestamp finds the first record at or after it", %{conn: conn, admin: admin} do
      table = log_table(admin)
      append!(conn, table, [[1]])
      Process.sleep(100)
      timestamp = System.system_time(:millisecond)
      Process.sleep(100)
      append!(conn, table, [[2]])

      assert Fluss.Admin.list_offsets!(admin, @database, table, [0], {:timestamp, timestamp}) ==
               %{0 => 1}
    end

    test "a timestamp after the last record gives the latest offset", %{conn: conn, admin: admin} do
      table = log_table(admin)
      append!(conn, table, [[1], [2]])
      Process.sleep(100)
      timestamp = System.system_time(:millisecond)

      assert Fluss.Admin.list_offsets!(admin, @database, table, [0], {:timestamp, timestamp}) ==
               %{0 => 2}
    end

    test "a timestamp in the future returns an error", %{conn: conn, admin: admin} do
      table = log_table(admin)
      append!(conn, table, [[1]])
      timestamp = System.system_time(:millisecond) + :timer.hours(1)

      assert {:error, %Fluss.Error{code: :invalid_timestamp_exception}} =
               Fluss.Admin.list_offsets(admin, @database, table, [0], {:timestamp, timestamp})
    end

    test "returns one offset per bucket", %{conn: conn, admin: admin} do
      table = log_table(admin, bucket_count: 3)
      # The writer waits for bucket leaders; querying a brand-new table can race them.
      append!(conn, table, [[1]])

      offsets = Fluss.Admin.list_offsets!(admin, @database, table, [0, 1, 2], :latest)
      assert offsets |> Map.keys() |> Enum.sort() == [0, 1, 2]
      assert offsets |> Map.values() |> Enum.sum() == 1
    end

    test "offsets of a partition", %{conn: conn, admin: admin} do
      year = Integer.to_string(Date.utc_today().year)
      table = partitioned_log_table(admin)
      append!(conn, table, [[1, year], [2, year]])

      assert Fluss.Admin.list_partition_offsets!(admin, @database, table, year, [0], :latest) ==
               %{0 => 2}

      assert Fluss.Admin.list_partition_offsets!(
               admin,
               @database,
               table,
               %{year: year},
               [0],
               :latest
             ) ==
               %{0 => 2}

      assert {:error, %Fluss.Error{code: :partition_not_exists}} =
               Fluss.Admin.list_partition_offsets(admin, @database, table, "1999", [0], :latest)

      assert {:error, %Fluss.Error{code: :partition_not_exists}} =
               Fluss.Admin.list_partition_offsets(
                 admin,
                 @database,
                 table,
                 %{year: "1999"},
                 [0],
                 :latest
               )
    end

    test "a spec is resolved in partition-key order", %{conn: conn, admin: admin} do
      table = partitioned_table(admin, ["region", "year"])
      :ok = Fluss.Admin.create_partition(admin, @database, table, %{region: "EU", year: "2026"})

      # The writer waits for bucket leaders; querying a brand-new partition can race them.
      writer =
        conn
        |> Fluss.Table.get!(@database, table)
        |> Fluss.TableUpsert.new!()
        |> Fluss.TableUpsert.create_writer!()

      {:ok, handle} = Fluss.UpsertWriter.upsert(writer, %{id: 1, region: "EU", year: "2026"})
      :ok = Fluss.WriteHandle.wait(handle)

      for partition <- [%{"year" => "2026", "region" => "EU"}, "EU$2026"] do
        offsets =
          Fluss.Admin.list_partition_offsets!(admin, @database, table, partition, [0, 1], :latest)

        assert offsets |> Map.values() |> Enum.sum() == 1
      end
    end

    test "a spec must name exactly the partition keys", %{admin: admin} do
      table = partitioned_table(admin, ["region", "year"])

      for partition <- [%{region: "EU"}, %{region: "EU", year: "2026", id: "1"}, %{}] do
        assert {:error, %Fluss.Error{code: :partition_spec_invalid_exception}} =
                 Fluss.Admin.list_partition_offsets(
                   admin,
                   @database,
                   table,
                   partition,
                   [0],
                   :latest
                 )
      end
    end

    test "a spec on a non-partitioned table returns an error", %{admin: admin} do
      table = log_table(admin)

      assert {:error, %Fluss.Error{code: :partition_spec_invalid_exception}} =
               Fluss.Admin.list_partition_offsets(
                 admin,
                 @database,
                 table,
                 %{year: "2026"},
                 [0],
                 :latest
               )
    end

    test "raises on malformed arguments", %{admin: admin} do
      table = log_table(admin)

      for {bucket_ids, spec, message} <- [
            {[-1], :latest, "expected a list of bucket ids"},
            {[2 ** 31], :latest, "expected a list of bucket ids"},
            {[:a], :latest, "expected a list of bucket ids"},
            {[0 | 1], :latest, "expected a list of bucket ids"},
            {nil, :latest, "expected a list of bucket ids"},
            {[0], :newest, "expected :earliest, :latest or {:timestamp, milliseconds}"},
            {[0], {:timestamp, "now"},
             "expected :earliest, :latest or {:timestamp, milliseconds}"}
          ] do
        assert_raise ArgumentError, ~r/#{Regex.escape(message)}/, fn ->
          Fluss.Admin.list_offsets(admin, @database, table, bucket_ids, spec)
        end
      end

      assert_raise ArgumentError, ~r/expected the table name to be a string/, fn ->
        Fluss.Admin.list_offsets(admin, @database, nil, [0], :latest)
      end

      assert_raise ArgumentError, ~r/expected a partition spec map or a partition name/, fn ->
        Fluss.Admin.list_partition_offsets(admin, @database, table, nil, [0], :latest)
      end

      assert_raise ArgumentError, ~r/partition value for `year` must be a string/, fn ->
        Fluss.Admin.list_partition_offsets(admin, @database, table, %{year: 2026}, [0], :latest)
      end
    end

    test "returns core's error for an empty bucket list", %{admin: admin} do
      table = log_table(admin)

      assert {:error, %Fluss.Error{} = error} =
               Fluss.Admin.list_offsets(admin, @database, table, [], :latest)

      assert error.message =~ "Buckets are empty"

      assert_raise Fluss.Error, fn ->
        Fluss.Admin.list_offsets!(admin, @database, table, [], :latest)
      end
    end

    test "a bucket that doesn't exist returns core's error", %{admin: admin} do
      table = log_table(admin)

      assert {:error, %Fluss.Error{}} =
               Fluss.Admin.list_offsets(admin, @database, table, [99], :latest)
    end

    test "list_offsets on a partitioned table returns an error", %{admin: admin} do
      table = partitioned_log_table(admin)

      assert {:error, %Fluss.Error{}} =
               Fluss.Admin.list_offsets(admin, @database, table, [0], :latest)
    end
  end

  # A primary key table partitioned by `keys` (string columns), with two buckets.
  defp partitioned_table(admin, keys) do
    table = "fluss_table_#{:rand.uniform(100_000)}"

    schema =
      keys
      |> Enum.reduce(Fluss.Schema.new() |> Fluss.Schema.column("id", :int), fn key, schema ->
        Fluss.Schema.column(schema, key, :string)
      end)
      |> Fluss.Schema.primary_key(["id" | keys])

    descriptor =
      Fluss.TableDescriptor.new!(schema,
        partition_keys: keys,
        bucket_count: 2,
        bucket_keys: ["id"]
      )

    :ok = Fluss.Admin.create_table(admin, @database, table, descriptor)
    on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true) end)
    table
  end

  defp log_table(admin, opts \\ []) do
    table = "fluss_table_#{:rand.uniform(100_000)}"
    schema = Fluss.Schema.new() |> Fluss.Schema.column("id", :int)
    descriptor = Fluss.TableDescriptor.new!(schema, Keyword.merge([bucket_count: 1], opts))
    :ok = Fluss.Admin.create_table(admin, @database, table, descriptor)
    on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true) end)
    table
  end

  # Auto-partitioned by year in UTC, so this `Date.utc_today()` year's partition exists.
  defp partitioned_log_table(admin) do
    table = "fluss_table_#{:rand.uniform(100_000)}"

    schema =
      Fluss.Schema.new()
      |> Fluss.Schema.column("id", :int)
      |> Fluss.Schema.column("year", :string)

    descriptor =
      Fluss.TableDescriptor.new!(schema,
        bucket_count: 1,
        partition_keys: ["year"],
        properties: %{
          "table.auto-partition.enabled" => "true",
          "table.auto-partition.time-unit" => "YEAR",
          "table.auto-partition.time-zone" => "UTC"
        }
      )

    :ok = Fluss.Admin.create_table(admin, @database, table, descriptor)
    on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, ignore_if_not_exists: true) end)
    table
  end

  defp append!(conn, table, rows) do
    writer = conn |> Fluss.Table.get!(@database, table) |> Fluss.AppendWriter.new!()

    for row <- rows, do: {:ok, _} = Fluss.AppendWriter.append(writer, row)
    :ok = Fluss.AppendWriter.flush(writer)
  end
end
