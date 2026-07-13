# Fluss Elixir Client

Elixir client for [Apache Fluss (Incubating)](https://fluss.apache.org/), built on the official Rust client via [Rustler](https://github.com/rusterlium/rustler) NIFs.

Currently supports **log tables** (append + scan). Primary key (KV) table support is planned.

## Requirements

- Elixir >= 1.15
- Rust stable toolchain (for compiling the NIF)

## Quick Start

```elixir
config = Fluss.Config.new("localhost:9123")
conn = Fluss.Connection.new!(config)
admin = Fluss.Admin.new!(conn)

schema =
  Fluss.Schema.build()
  |> Fluss.Schema.column("ts", :bigint)
  |> Fluss.Schema.column("message", :string)
  |> Fluss.Schema.build!()

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
