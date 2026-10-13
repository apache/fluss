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

defmodule Fluss.Admin do
  @moduledoc """
  Admin client for DDL operations (create/drop databases and tables).

  ## Examples

      admin = Fluss.Admin.new!(conn)
      :ok = Fluss.Admin.create_database(admin, "my_db")

      database_descriptor =
        Fluss.DatabaseDescriptor.new()
        |> Fluss.DatabaseDescriptor.comment("App data")

      :ok = Fluss.Admin.create_database(admin, "another_db", database_descriptor)

      schema = Fluss.Schema.new() |> Fluss.Schema.column("ts", :bigint)
      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, "my_db", "events", descriptor)

  """

  alias Fluss.Native

  @type t :: reference()

  @spec new(Fluss.Connection.t()) :: {:ok, t()} | {:error, Fluss.Error.t()}
  def new(conn) do
    case Native.admin_new(conn) do
      {:error, _} = err -> err
      admin -> {:ok, admin}
    end
  end

  @spec new!(Fluss.Connection.t()) :: t()
  def new!(conn) do
    case new(conn) do
      {:ok, admin} -> admin
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec get_server_nodes(t()) :: {:ok, [Fluss.ServerNode.t()]} | {:error, Fluss.Error.t()}
  def get_server_nodes(admin) do
    admin
    |> Native.admin_get_server_nodes()
    |> Native.await_nif()
  end

  @spec get_server_nodes!(t()) :: [Fluss.ServerNode.t()]
  def get_server_nodes!(admin) do
    case get_server_nodes(admin) do
      {:ok, nodes} -> nodes
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec create_database(t(), String.t(), Fluss.DatabaseDescriptor.t() | nil, boolean()) ::
          :ok | {:error, Fluss.Error.t()}
  def create_database(admin, name, descriptor \\ nil, ignore_if_exists \\ true) do
    admin
    |> Native.admin_create_database(name, descriptor, ignore_if_exists)
    |> Native.await_nif()
  end

  @spec get_database_info(t(), String.t()) ::
          {:ok, Fluss.DatabaseInfo.t()} | {:error, Fluss.Error.t()}
  def get_database_info(admin, database_name) do
    admin
    |> Native.admin_get_database_info(database_name)
    |> Native.await_nif()
  end

  @spec get_database_info!(t(), String.t()) :: Fluss.DatabaseInfo.t()
  def get_database_info!(admin, database_name) do
    case get_database_info(admin, database_name) do
      {:ok, info} -> info
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec drop_database(t(), String.t(), boolean()) :: :ok | {:error, Fluss.Error.t()}
  def drop_database(admin, name, ignore_if_not_exists \\ true) do
    admin
    |> Native.admin_drop_database(name, ignore_if_not_exists)
    |> Native.await_nif()
  end

  @spec list_databases(t()) :: {:ok, [String.t()]} | {:error, Fluss.Error.t()}
  def list_databases(admin) do
    admin
    |> Native.admin_list_databases()
    |> Native.await_nif()
  end

  @spec list_databases!(t()) :: [String.t()]
  def list_databases!(admin) do
    case list_databases(admin) do
      {:ok, dbs} -> dbs
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec database_exists(t(), String.t()) :: {:ok, boolean()} | {:error, Fluss.Error.t()}
  def database_exists(admin, database_name) do
    admin
    |> Native.admin_database_exists(database_name)
    |> Native.await_nif()
  end

  @spec database_exists!(t(), String.t()) :: boolean()
  def database_exists!(admin, database_name) do
    case database_exists(admin, database_name) do
      {:ok, exists} -> exists
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec create_table(t(), String.t(), String.t(), Fluss.TableDescriptor.t(), boolean()) ::
          :ok | {:error, Fluss.Error.t()}
  def create_table(admin, database, table, descriptor, ignore_if_exists \\ true) do
    admin
    |> Native.admin_create_table(database, table, descriptor, ignore_if_exists)
    |> Native.await_nif()
  end

  @spec drop_table(t(), String.t(), String.t(), boolean()) :: :ok | {:error, Fluss.Error.t()}
  def drop_table(admin, database, table, ignore_if_not_exists \\ true) do
    admin
    |> Native.admin_drop_table(database, table, ignore_if_not_exists)
    |> Native.await_nif()
  end

  @spec list_tables(t(), String.t()) :: {:ok, [String.t()]} | {:error, Fluss.Error.t()}
  def list_tables(admin, database) do
    admin
    |> Native.admin_list_tables(database)
    |> Native.await_nif()
  end

  @spec list_tables!(t(), String.t()) :: [String.t()]
  def list_tables!(admin, database) do
    case list_tables(admin, database) do
      {:ok, tables} -> tables
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec table_exists(t(), String.t(), String.t()) :: {:ok, boolean()} | {:error, Fluss.Error.t()}
  def table_exists(admin, database_name, table_name) do
    admin
    |> Native.admin_table_exists(database_name, table_name)
    |> Native.await_nif()
  end

  @spec table_exists!(t(), String.t(), String.t()) :: boolean()
  def table_exists!(admin, database_name, table_name) do
    case table_exists(admin, database_name, table_name) do
      {:ok, exists} -> exists
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec get_table_info(t(), String.t(), String.t()) ::
          {:ok, Fluss.TableInfo.t()} | {:error, Fluss.Error.t()}
  def get_table_info(admin, database_name, table_name) do
    admin
    |> Native.admin_get_table_info(database_name, table_name)
    |> Native.await_nif()
  end

  @spec get_table_info!(t(), String.t(), String.t()) :: Fluss.TableInfo.t()
  def get_table_info!(admin, database_name, table_name) do
    case get_table_info(admin, database_name, table_name) do
      {:ok, table_info} -> table_info
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  @spec get_table_schema(t(), String.t(), String.t(), integer() | nil) ::
          {:ok, Fluss.SchemaInfo.t()} | {:error, Fluss.Error.t()}
  def get_table_schema(admin, database_name, table_name, schema_id \\ nil) do
    admin
    |> Native.admin_get_table_schema(database_name, table_name, schema_id)
    |> Native.await_nif()
  end

  @spec get_table_schema!(t(), String.t(), String.t(), integer() | nil) :: Fluss.SchemaInfo.t()
  def get_table_schema!(admin, database_name, table_name, schema_id \\ nil) do
    case get_table_schema(admin, database_name, table_name, schema_id) do
      {:ok, schema_info} -> schema_info
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end
end
