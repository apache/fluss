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
  Admin client: databases, tables and partitions.

  `create_*` and `drop_*` take keyword options, listed on each function.

  A partition spec maps partition columns (atoms or strings) to string values, e.g.
  `%{region: "EU"}`. A write to a partition that doesn't exist creates it, unless
  `Fluss.Config.set_writer_dynamic_create_partition_enabled/2` turns that off, in which case the
  write fails with `:partition_not_exists`.

  Malformed arguments (a non-string name, an unknown option, a bad partition spec) raise
  `ArgumentError`; errors from the server are returned as `{:error, %Fluss.Error{}}`.

  ## Examples

      admin = Fluss.Admin.new!(conn)
      :ok = Fluss.Admin.create_database(admin, "my_db", Fluss.DatabaseDescriptor.new())

      database_descriptor =
        Fluss.DatabaseDescriptor.new()
        |> Fluss.DatabaseDescriptor.comment("App data")

      :ok = Fluss.Admin.create_database(admin, "another_db", database_descriptor)

      schema = Fluss.Schema.new() |> Fluss.Schema.column("ts", :bigint)
      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, "my_db", "events", descriptor)
      :ok = Fluss.Admin.create_table(admin, "my_db", "events", descriptor, ignore_if_exists: true)

  """

  alias Fluss.Native

  @type t :: reference()

  @typedoc "Partition column (atom or string) to value."
  @type partition_spec :: %{(atom() | String.t()) => String.t()}

  @type create_database_opts :: [ignore_if_exists: boolean()]
  @type drop_database_opts :: [ignore_if_not_exists: boolean(), cascade: boolean()]
  @type create_table_opts :: [ignore_if_exists: boolean()]
  @type drop_table_opts :: [ignore_if_not_exists: boolean()]
  @type create_partition_opts :: [ignore_if_exists: boolean()]
  @type drop_partition_opts :: [ignore_if_not_exists: boolean()]

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

  @doc """
  Creates a database.

  ## Options

    * `:ignore_if_exists` - `:ok` if it already exists. Default `false`.
  """
  @spec create_database(
          t(),
          String.t(),
          Fluss.DatabaseDescriptor.t(),
          create_database_opts()
        ) ::
          :ok | {:error, Fluss.Error.t()}
  def create_database(admin, name, descriptor, opts \\ []) do
    name = validate_name!(name, "database")
    descriptor = validate_database_descriptor!(descriptor)
    opts = validate_opts!(opts, ignore_if_exists: false)

    admin
    |> Native.admin_create_database(name, descriptor, Map.new(opts))
    |> Native.await_nif()
  end

  @spec get_database_info(t(), String.t()) ::
          {:ok, Fluss.DatabaseInfo.t()} | {:error, Fluss.Error.t()}
  def get_database_info(admin, database_name) do
    database_name = validate_name!(database_name, "database")

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

  @doc """
  Drops a database.

  ## Options

    * `:ignore_if_not_exists` - `:ok` if it doesn't exist. Default `false`.
    * `:cascade` - also drop its tables. Default `true`.
  """
  @spec drop_database(t(), String.t(), drop_database_opts()) :: :ok | {:error, Fluss.Error.t()}
  def drop_database(admin, name, opts \\ []) do
    name = validate_name!(name, "database")
    opts = validate_opts!(opts, ignore_if_not_exists: false, cascade: true)

    admin
    |> Native.admin_drop_database(name, Map.new(opts))
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
    database_name = validate_name!(database_name, "database")

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

  @doc """
  Creates a table.

  ## Options

    * `:ignore_if_exists` - `:ok` if it already exists. Default `false`.
  """
  @spec create_table(
          t(),
          String.t(),
          String.t(),
          Fluss.TableDescriptor.t(),
          create_table_opts()
        ) ::
          :ok | {:error, Fluss.Error.t()}
  def create_table(admin, database, table, descriptor, opts \\ []) do
    database = validate_name!(database, "database")
    table = validate_name!(table, "table")
    opts = validate_opts!(opts, ignore_if_exists: false)

    admin
    |> Native.admin_create_table(database, table, descriptor, Map.new(opts))
    |> Native.await_nif()
  end

  @doc """
  Drops a table.

  ## Options

    * `:ignore_if_not_exists` - `:ok` if it doesn't exist. Default `false`.
  """
  @spec drop_table(t(), String.t(), String.t(), drop_table_opts()) ::
          :ok | {:error, Fluss.Error.t()}
  def drop_table(admin, database, table, opts \\ []) do
    database = validate_name!(database, "database")
    table = validate_name!(table, "table")
    opts = validate_opts!(opts, ignore_if_not_exists: false)

    admin
    |> Native.admin_drop_table(database, table, Map.new(opts))
    |> Native.await_nif()
  end

  @spec list_tables(t(), String.t()) :: {:ok, [String.t()]} | {:error, Fluss.Error.t()}
  def list_tables(admin, database) do
    database = validate_name!(database, "database")

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
    database_name = validate_name!(database_name, "database")
    table_name = validate_name!(table_name, "table")

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
    database_name = validate_name!(database_name, "database")
    table_name = validate_name!(table_name, "table")

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
    database_name = validate_name!(database_name, "database")
    table_name = validate_name!(table_name, "table")

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

  @doc """
  Creates a partition. A missing table is an error, whatever the options.

  ## Options

    * `:ignore_if_exists` - `:ok` if it already exists. Default `false`.
  """
  @spec create_partition(t(), String.t(), String.t(), partition_spec(), create_partition_opts()) ::
          :ok | {:error, Fluss.Error.t()}
  def create_partition(admin, database, table, spec, opts \\ []) do
    database = validate_name!(database, "database")
    table = validate_name!(table, "table")
    spec = validate_spec!(spec)
    opts = validate_opts!(opts, ignore_if_exists: false)

    admin
    |> Native.admin_create_partition(database, table, spec, Map.new(opts))
    |> Native.await_nif()
  end

  @doc """
  Drops a partition. A missing table is an error, whatever the options.

  ## Options

    * `:ignore_if_not_exists` - `:ok` if it doesn't exist. Default `false`.
  """
  @spec drop_partition(t(), String.t(), String.t(), partition_spec(), drop_partition_opts()) ::
          :ok | {:error, Fluss.Error.t()}
  def drop_partition(admin, database, table, spec, opts \\ []) do
    database = validate_name!(database, "database")
    table = validate_name!(table, "table")
    spec = validate_spec!(spec)
    opts = validate_opts!(opts, ignore_if_not_exists: false)

    admin
    |> Native.admin_drop_partition(database, table, spec, Map.new(opts))
    |> Native.await_nif()
  end

  @doc "Lists the table's partitions, or only those matching a partial `spec`."
  @spec list_partition_infos(t(), String.t(), String.t(), partition_spec() | nil) ::
          {:ok, [Fluss.PartitionInfo.t()]} | {:error, Fluss.Error.t()}
  def list_partition_infos(admin, database, table, spec \\ nil) do
    database = validate_name!(database, "database")
    table = validate_name!(table, "table")
    spec = spec && validate_spec!(spec)

    admin
    |> Native.admin_list_partition_infos(database, table, spec)
    |> Native.await_nif()
  end

  @spec list_partition_infos!(t(), String.t(), String.t(), partition_spec() | nil) ::
          [Fluss.PartitionInfo.t()]
  def list_partition_infos!(admin, database, table, spec \\ nil) do
    case list_partition_infos(admin, database, table, spec) do
      {:ok, infos} -> infos
      {:error, %Fluss.Error{} = err} -> raise err
    end
  end

  defp validate_name!(name, _what) when is_binary(name), do: name

  defp validate_name!(name, what) do
    raise ArgumentError, "expected the #{what} name to be a string, got: #{inspect(name)}"
  end

  # Every option is a boolean. Returns the options with defaults filled in.
  defp validate_opts!(opts, defaults) when is_list(opts) do
    opts |> Keyword.validate!(defaults) |> Enum.map(&validate_boolean!/1)
  end

  defp validate_opts!(opts, _defaults) do
    raise ArgumentError, "expected options to be a keyword list, got: #{inspect(opts)}"
  end

  defp validate_database_descriptor!(%Fluss.DatabaseDescriptor{} = descriptor), do: descriptor

  defp validate_database_descriptor!(descriptor) do
    raise ArgumentError, "expected a %Fluss.DatabaseDescriptor{}, got: #{inspect(descriptor)}"
  end

  defp validate_boolean!({_key, value} = opt) when is_boolean(value), do: opt

  defp validate_boolean!({key, value}) do
    raise ArgumentError, "expected #{inspect(key)} to be a boolean, got: #{inspect(value)}"
  end

  # String keys, so an atom and a string naming the same column are caught.
  defp validate_spec!(spec) when is_map(spec) do
    Enum.reduce(spec, %{}, fn {column, value}, acc ->
      column = spec_column!(column)

      unless is_binary(value) do
        raise ArgumentError,
              "partition value for `#{column}` must be a string, got: #{inspect(value)}"
      end

      if Map.has_key?(acc, column) do
        raise ArgumentError, "partition column `#{column}` given more than once"
      end

      Map.put(acc, column, value)
    end)
  end

  defp validate_spec!(spec) do
    raise ArgumentError,
          "expected a map of partition column to value, got: #{inspect(spec)}"
  end

  defp spec_column!(column) when is_atom(column), do: Atom.to_string(column)
  defp spec_column!(column) when is_binary(column), do: column

  defp spec_column!(column) do
    raise ArgumentError,
          "expected partition columns to be atoms or strings, got: #{inspect(column)}"
  end
end
