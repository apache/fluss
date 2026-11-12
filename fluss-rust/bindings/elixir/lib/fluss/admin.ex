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
  Admin client: databases, tables, partitions and bucket offsets.

  `create_*`, `drop_*` and `alter_*` take keyword options, listed on each function.

  A partition spec maps partition columns (atoms or strings) to string values, e.g.
  `%{region: "EU"}`. A write to a partition that doesn't exist creates it, unless
  `Fluss.Config.set_writer_dynamic_create_partition_enabled/2` turns that off, in which case the
  write fails with `:partition_not_exists`.

  `list_offsets/5` returns `%{bucket_id => offset}` for `:earliest`, `:latest` or
  `{:timestamp, milliseconds}`, the first offset at or after that time: the latest offset if
  no record is that recent, `:invalid_timestamp_exception` if the time is in the future.
  Partitioned tables use `list_partition_offsets/6`.

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

  import Fluss.ArgumentValidation

  alias Fluss.Native

  @type t :: reference()

  @typedoc "Partition column (atom or string) to value."
  @type partition_spec :: %{(atom() | String.t()) => String.t()}

  @type create_database_opts :: [ignore_if_exists: boolean()]
  @type drop_database_opts :: [ignore_if_not_exists: boolean(), cascade: boolean()]
  @type alter_database_opts :: [ignore_if_not_exists: boolean()]
  @type create_table_opts :: [ignore_if_exists: boolean()]
  @type drop_table_opts :: [ignore_if_not_exists: boolean()]
  @type alter_table_opts :: [ignore_if_not_exists: boolean()]
  @type create_partition_opts :: [ignore_if_exists: boolean()]
  @type drop_partition_opts :: [ignore_if_not_exists: boolean()]
  @type offset_spec :: :earliest | :latest | {:timestamp, integer()}

  @type database_change ::
          {:set_property, String.t(), String.t()}
          | {:reset_property, String.t()}
          | {:set_comment, String.t() | nil}

  @type table_change ::
          {:add_column, String.t(), Fluss.Schema.data_type()}
          | {:add_column, String.t(), Fluss.Schema.data_type(), [comment: String.t() | nil]}
          | {:drop_column, String.t()}
          | {:rename_column, String.t(), String.t()}
          | {:modify_column, String.t(),
             [type: Fluss.Schema.data_type(), comment: String.t() | nil]}
          | {:set_property, String.t(), String.t()}
          | {:reset_property, String.t()}
          | {:set_bucket_count, pos_integer()}

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

  @doc """
  Alters a database. An empty list of changes is a no-op.

  ## Changes

    * `{:set_property, key, value}`, `{:reset_property, key}`.
    * `{:set_comment, comment}` - `nil` removes it. At most one.

  ## Options

    * `:ignore_if_not_exists` - `:ok` if it doesn't exist. Default `false`.
  """
  @spec alter_database(t(), String.t(), [database_change()], alter_database_opts()) ::
          :ok | {:error, Fluss.Error.t()}
  def alter_database(admin, name, changes, opts \\ []) do
    name = validate_name!(name, "database")
    changes = validate_database_changes!(changes)
    opts = validate_opts!(opts, ignore_if_not_exists: false)

    admin
    |> Native.admin_alter_database(name, changes, Map.new(opts))
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

  @doc """
  Alters a table. The changes apply all together or not at all; an empty list is a no-op.

  ## Changes

    * `{:add_column, name, type}`, `{:add_column, name, type, comment: comment}` - appends a
      column. The type must be nullable (no top-level `{:not_null, _}`).
    * `{:drop_column, name}`, `{:rename_column, from, to}`.
    * `{:modify_column, name, type: type, comment: comment}` - both options are optional; a
      missing one is left unchanged, `comment: nil` sends an empty comment.
    * `{:set_property, key, value}`, `{:reset_property, key}` - a reset property falls back to
      its default.
    * `{:set_bucket_count, count}` - the bucket count for new partitions of a partitioned
      table; existing partitions keep theirs. At most one.

  The server only supports adding columns for now. It applies column changes in the order add,
  drop, rename, modify, and rejects a request that mixes properties, column changes and the
  bucket count (`:invalid_alter_table_exception`).

  ## Options

    * `:ignore_if_not_exists` - `:ok` if it doesn't exist. Default `false`.
  """
  @spec alter_table(t(), String.t(), String.t(), [table_change()], alter_table_opts()) ::
          :ok | {:error, Fluss.Error.t()}
  def alter_table(admin, database, table, changes, opts \\ []) do
    database = validate_name!(database, "database")
    table = validate_name!(table, "table")
    changes = validate_table_changes!(changes)
    opts = validate_opts!(opts, ignore_if_not_exists: false)

    admin
    |> Native.admin_alter_table(database, table, changes, Map.new(opts))
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

  @spec list_offsets(t(), String.t(), String.t(), [non_neg_integer()], offset_spec()) ::
          {:ok, %{non_neg_integer() => non_neg_integer()}} | {:error, Fluss.Error.t()}
  def list_offsets(admin, database, table, bucket_ids, spec) do
    database = validate_name!(database, "database")
    table = validate_name!(table, "table")
    bucket_ids = validate_bucket_ids!(bucket_ids)
    spec = validate_offset_spec!(spec)

    admin
    |> Native.admin_list_offsets(database, table, bucket_ids, spec)
    |> Native.await_nif()
  end

  @spec list_offsets!(t(), String.t(), String.t(), [non_neg_integer()], offset_spec()) ::
          %{non_neg_integer() => non_neg_integer()}
  def list_offsets!(admin, database, table, bucket_ids, spec) do
    case list_offsets(admin, database, table, bucket_ids, spec) do
      {:ok, offsets} -> offsets
      {:error, %Fluss.Error{} = error} -> raise error
    end
  end

  @doc "Offsets of a partition, given as a full spec map or its name (`\"EU$2026\"`)."
  @spec list_partition_offsets(
          t(),
          String.t(),
          String.t(),
          partition_spec() | String.t(),
          [non_neg_integer()],
          offset_spec()
        ) :: {:ok, %{non_neg_integer() => non_neg_integer()}} | {:error, Fluss.Error.t()}
  def list_partition_offsets(admin, database, table, partition, bucket_ids, spec) do
    database = validate_name!(database, "database")
    table = validate_name!(table, "table")
    partition = validate_partition!(partition)
    bucket_ids = validate_bucket_ids!(bucket_ids)
    spec = validate_offset_spec!(spec)

    admin
    |> Native.admin_list_partition_offsets(database, table, partition, bucket_ids, spec)
    |> Native.await_nif()
  end

  @spec list_partition_offsets!(
          t(),
          String.t(),
          String.t(),
          partition_spec() | String.t(),
          [non_neg_integer()],
          offset_spec()
        ) :: %{non_neg_integer() => non_neg_integer()}
  def list_partition_offsets!(admin, database, table, partition, bucket_ids, spec) do
    case list_partition_offsets(admin, database, table, partition, bucket_ids, spec) do
      {:ok, offsets} -> offsets
      {:error, %Fluss.Error{} = error} -> raise error
    end
  end

  # Every option is a boolean. Returns the options with defaults filled in.
  defp validate_opts!(opts, defaults) do
    opts |> validate_keyword!(defaults) |> Enum.map(&validate_boolean!/1)
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

  defp validate_partition!(name) when is_binary(name), do: {:name, name}
  defp validate_partition!(spec) when is_map(spec), do: {:spec, validate_spec!(spec)}

  defp validate_partition!(partition) do
    raise ArgumentError,
          "expected a partition spec map or a partition name, got: #{inspect(partition)}"
  end

  # Bucket ids are i32 in core.
  defp validate_bucket_ids!(bucket_ids) do
    if is_list(bucket_ids) and not List.improper?(bucket_ids) and
         Enum.all?(bucket_ids, &(is_integer(&1) and &1 in 0..0x7FFFFFFF)) do
      bucket_ids
    else
      raise ArgumentError,
            "expected a list of bucket ids (0..2147483647), got: #{inspect(bucket_ids)}"
    end
  end

  defp validate_offset_spec!(spec) when spec in [:earliest, :latest], do: spec

  defp validate_offset_spec!({:timestamp, milliseconds} = spec)
       when milliseconds in -0x8000000000000000..0x7FFFFFFFFFFFFFFF,
       do: spec

  defp validate_offset_spec!(spec) do
    raise ArgumentError,
          "expected :earliest, :latest or {:timestamp, milliseconds}, got: #{inspect(spec)}"
  end

  defp validate_table_changes!(changes) do
    changes
    |> validate_items!("changes", &validate_table_change!/1)
    |> validate_at_most_one!(:set_bucket_count)
  end

  defp validate_table_change!({:add_column, name, type}),
    do: validate_table_change!({:add_column, name, type, []})

  defp validate_table_change!({:add_column, name, type, opts}) do
    name = validate_name!(name, "column")
    type = Fluss.Schema.validate_data_type!(type, name)
    opts = validate_keyword!(opts, comment: nil)

    comment =
      unless is_nil(opts[:comment]), do: validate_string!(opts[:comment], "column comment")

    # The server always rejects it, with an unregistered error code and a Java stack trace.
    if match?({:not_null, _}, type) do
      raise ArgumentError, "an added column must be nullable, got: #{inspect(type)} for `#{name}`"
    end

    {:add_column, name, type, comment}
  end

  defp validate_table_change!({:drop_column, name}),
    do: {:drop_column, validate_name!(name, "column")}

  defp validate_table_change!({:rename_column, from, to}),
    do: {:rename_column, validate_name!(from, "column"), validate_name!(to, "new column")}

  defp validate_table_change!({:modify_column, name, opts}) do
    name = validate_name!(name, "column")
    opts = validate_keyword!(opts, [:type, :comment])

    type =
      if Keyword.has_key?(opts, :type), do: Fluss.Schema.validate_data_type!(opts[:type], name)

    # The wire has no "remove the comment"; `""` is the closest.
    comment =
      case Keyword.fetch(opts, :comment) do
        :error -> nil
        {:ok, nil} -> ""
        {:ok, comment} -> validate_string!(comment, "column comment")
      end

    {:modify_column, name, type, comment}
  end

  defp validate_table_change!({:set_bucket_count, count} = change) do
    unless is_integer(count) and count in 1..0x7FFFFFFF do
      raise ArgumentError,
            "expected the bucket count to be an integer in 1..2147483647, got: #{inspect(count)}"
    end

    change
  end

  defp validate_table_change!(change) do
    case validate_property_change!(change) do
      nil ->
        raise ArgumentError,
              "expected a table change (see Fluss.Admin.alter_table/5), got: #{inspect(change)}"

      # As a property the server would store it and keep the bucket count.
      property_change when elem(property_change, 1) == "bucket.num" ->
        raise ArgumentError, ~s("bucket.num" is not a property, use {:set_bucket_count, count})

      property_change ->
        property_change
    end
  end

  defp validate_database_changes!(changes) do
    changes
    |> validate_items!("changes", &validate_database_change!/1)
    |> validate_at_most_one!(:set_comment)
  end

  # The server resets a `""` comment to none.
  defp validate_database_change!({:set_comment, nil}), do: {:set_comment, ""}

  defp validate_database_change!({:set_comment, comment}),
    do: {:set_comment, validate_string!(comment, "database comment")}

  defp validate_database_change!(change) do
    validate_property_change!(change) ||
      raise ArgumentError,
            "expected a database change (see Fluss.Admin.alter_database/4), got: #{inspect(change)}"
  end

  # Returns `nil` for a change that isn't a property change.
  defp validate_property_change!({:set_property, key, value}),
    do:
      {:set_property, validate_string!(key, "property key"),
       validate_string!(value, "property value")}

  defp validate_property_change!({:reset_property, key}),
    do: {:reset_property, validate_string!(key, "property key")}

  defp validate_property_change!(_change), do: nil

  # Core holds at most one of these.
  defp validate_at_most_one!(changes, tag) do
    if Enum.count(changes, &(elem(&1, 0) == tag)) > 1 do
      raise ArgumentError, "expected at most one #{inspect(tag)} change"
    end

    changes
  end
end
