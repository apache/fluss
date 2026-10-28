# Log tables

A log table has no primary key: records are appended and never change.

## Create

```elixir
schema =
  Fluss.Schema.new()
  |> Fluss.Schema.column("ts", :bigint)
  |> Fluss.Schema.column("message", :string)

descriptor = Fluss.TableDescriptor.new!(schema, bucket_count: 3)
:ok = Fluss.Admin.create_table(admin, "my_db", "events", descriptor)

table = Fluss.Table.get!(conn, "my_db", "events")
```

## Write

Values are a list in column order; `nil` is null. See `Fluss.AppendWriter` for per-record acks.

```elixir
writer = Fluss.AppendWriter.new!(table)
Fluss.AppendWriter.append(writer, [1_700_000_000, "hello"])
:ok = Fluss.AppendWriter.flush(writer)
```

## Read

A scanner subscribes to buckets, each from a starting offset; `Fluss.LogScanner.poll/2`
delivers records as messages. `{:fluss_records, []}` means nothing arrived before the timeout,
and `:change_type` is always `:append_only` for log tables.

A GenServer that reads every bucket from the start:

```elixir
defmodule MyApp.EventsConsumer do
  use GenServer

  def start_link({admin, table}), do: GenServer.start_link(__MODULE__, {admin, table})

  @impl true
  def init({admin, table}) do
    info = Fluss.Admin.get_table_info!(admin, "my_db", "events")
    scanner = Fluss.LogScanner.new!(table)
    buckets = for b <- 0..(info.num_buckets - 1), do: {b, Fluss.earliest_offset()}
    :ok = Fluss.LogScanner.subscribe_buckets(scanner, buckets)
    send(self(), :poll)
    {:ok, scanner}
  end

  @impl true
  def handle_info(:poll, scanner) do
    :ok = Fluss.LogScanner.poll(scanner, 1_000)
    {:noreply, scanner}
  end

  def handle_info({:fluss_records, records}, scanner) do
    Enum.each(records, &IO.inspect(&1.row))
    send(self(), :poll)
    {:noreply, scanner}
  end

  def handle_info({:fluss_poll_error, error}, scanner) do
    IO.warn("poll failed: #{Exception.message(error)}")
    send(self(), :poll)
    {:noreply, scanner}
  end
end
```

Poll messages don't say which scanner they came from, so give each scanner its own process,
and poll again only after the previous result arrives.

Partitioned log tables can't be scanned yet.
