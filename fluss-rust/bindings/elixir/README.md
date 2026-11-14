# Fluss Elixir Client

Elixir client for [Apache Fluss](https://fluss.apache.org/), built on the official Rust client via [Rustler](https://github.com/rusterlium/rustler) NIFs.

Supports **log tables** (append + scan) and **primary key (KV) tables** (upsert, delete,
partial update, primary key and prefix lookup, with the changelog readable through the log
scanner).

## Requirements

- Elixir >= 1.15
- Rust stable toolchain (for compiling the NIF)

## Quick Start

```elixir
config = Fluss.Config.new("localhost:9123")
conn = Fluss.Connection.new!(config)
admin = Fluss.Admin.new!(conn)

schema =
  Fluss.Schema.new()
  |> Fluss.Schema.column("ts", :bigint)
  |> Fluss.Schema.column("message", :string)

:ok = Fluss.Admin.create_table(admin, "my_db", "events", Fluss.TableDescriptor.new!(schema))

table = Fluss.Table.get!(conn, "my_db", "events")
writer = Fluss.AppendWriter.new!(table)
Fluss.AppendWriter.append(writer, [1_700_000_000, "hello"])
:ok = Fluss.AppendWriter.flush(writer)

scanner = Fluss.LogScanner.new!(table)
:ok = Fluss.LogScanner.subscribe(scanner, 0, Fluss.earliest_offset())
:ok = Fluss.LogScanner.poll(scanner, 5_000)

receive do
  {:fluss_records, records} ->
    for record <- records, do: IO.inspect(record[:row])
end
```

### Primary key tables

Give the schema a primary key, then write through `Fluss.TableUpsert`. Values are a map
keyed by column name, or a positional list.

```elixir
schema =
  Fluss.Schema.new()
  |> Fluss.Schema.column("id", :int)
  |> Fluss.Schema.column("name", :string)
  |> Fluss.Schema.column("email", :string)
  |> Fluss.Schema.primary_key(["id"])

:ok = Fluss.Admin.create_table(admin, "my_db", "users", Fluss.TableDescriptor.new!(schema))

table = Fluss.Table.get!(conn, "my_db", "users")
writer = table |> Fluss.TableUpsert.new!() |> Fluss.TableUpsert.create_writer!()

Fluss.UpsertWriter.upsert(writer, %{id: 1, name: "alice"})
Fluss.UpsertWriter.delete(writer, %{id: 1})
:ok = Fluss.UpsertWriter.flush(writer)
```

Naming target columns restricts the writer to those columns, leaving the rest untouched:

```elixir
writer =
  table
  |> Fluss.TableUpsert.new!()
  |> Fluss.TableUpsert.partial_update_by_name!([:id, :email])
  |> Fluss.TableUpsert.create_writer!()
```

Rows are read back by primary key with `Fluss.Lookuper`, or by key prefix with
`Fluss.PrefixLookuper`. A lookuper can be shared by many processes:

```elixir
Fluss.UpsertWriter.upsert(writer, %{id: 2, name: "bob"})
:ok = Fluss.UpsertWriter.flush(writer)

lookuper = Fluss.Lookuper.new!(table)

{:ok, %{id: 2, name: "bob", email: nil}} = Fluss.Lookuper.lookup(lookuper, %{id: 2})
{:ok, nil} = Fluss.Lookuper.lookup(lookuper, [1])  # deleted above
```

A primary key table's changelog is read with `Fluss.LogScanner`, exactly as above; each
record carries a `:change_type` of `:insert`, `:update_before`, `:update_after` or
`:delete`.

## Data Types

Simple: `:boolean`, `:tinyint`, `:smallint`, `:int`, `:bigint`, `:float`, `:double`, `:string`, `:bytes`, `:date`, `:time`, `:timestamp`, `:timestamp_ltz`

Parameterized: `{:decimal, precision, scale}`, `{:char, length}`, `{:binary, length}`

## Development

```bash
cd bindings/elixir
mix test                        # unit tests
mix test --include integration  # starts Docker cluster
```

Set `FLUSS_BOOTSTRAP_SERVERS` to use an existing cluster.

## License

Apache License 2.0
