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
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

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
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

      assert {:ok, info} = Fluss.Admin.get_database_info(admin, db)
      assert info.descriptor.custom_properties == %{"region" => "eu-west-1"}
    end

    test "accepts a struct constructed directly without builders", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      descriptor = %Fluss.DatabaseDescriptor{comment: "direct", custom_properties: %{"k" => "v"}}
      :ok = Fluss.Admin.create_database(admin, db, descriptor)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

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
      :ok = Fluss.Admin.create_database(admin, db, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

      descriptor = %Fluss.DatabaseDescriptor{comment: "updated", custom_properties: %{}}

      :ok = Fluss.Admin.create_database(admin, db, descriptor)
      assert {:ok, info} = Fluss.Admin.get_database_info(admin, db)

      # the 2nd create was skipped due to `ignore_if_exists: true`
      assert info.descriptor.comment == "direct"
      assert info.descriptor.custom_properties == %{"k" => "v"}
    end

    test "fails if the database exists and ignore_if_exists is false", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      descriptor = %Fluss.DatabaseDescriptor{comment: "direct", custom_properties: %{"k" => "v"}}
      :ok = Fluss.Admin.create_database(admin, db, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

      assert {:error, %Fluss.Error{}} = Fluss.Admin.create_database(admin, db, descriptor, false)
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
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

      assert {:ok, %Fluss.DatabaseInfo{} = info} = Fluss.Admin.get_database_info(admin, db)
      assert info.database_name == db
      assert info.descriptor.comment == "Integration test database"
      assert info.descriptor.custom_properties == %{"owner" => "ci", "env" => "test"}
      assert is_integer(info.created_time)
      assert is_integer(info.modified_time)
    end

    test "returns DatabaseInfo for a database created with no descriptor", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      :ok = Fluss.Admin.create_database(admin, db)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

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

      :ok = Fluss.Admin.create_database(admin, db)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

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

      :ok = Fluss.Admin.create_database(admin, db)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

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
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

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
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

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
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

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
               columns: [{"id", :int}, {"name", :string}],
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
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

      assert {:ok, info} = Fluss.Admin.get_table_info(admin, @database, table)

      assert info.has_primary_key == true
      assert info.primary_keys == ["id", "region"]
      assert info.physical_primary_keys == ["id", "region"]

      assert info.schema == %Fluss.Schema{
               columns: [{"id", :int}, {"region", :string}, {"name", :string}],
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
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

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
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

      assert {:ok, %Fluss.SchemaInfo{} = info} =
               Fluss.Admin.get_table_schema(admin, @database, table)

      assert info.schema == %Fluss.Schema{
               columns: [{"id", :int}, {"name", :string}],
               primary_key: ["id"]
             }

      assert is_integer(info.schema_id) and info.schema_id >= 1
    end

    test "fetches a specific schema version by id", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)
        |> Fluss.Schema.primary_key(["id"])

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

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

      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

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

      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

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

      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

      assert {:ok, info} = Fluss.Admin.get_table_info(admin, @database, table)
      assert info.is_partitioned == true
      assert info.partition_keys == ["dt"]
    end
  end
end
