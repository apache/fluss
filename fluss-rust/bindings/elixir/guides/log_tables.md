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

## Consumer groups and saved offsets

```elixir
{:ok, scanner} = Fluss.LogScanner.new(table, group_id: "orders")
bucket = {table_info.table_id, nil, 0}
:ok = Fluss.LogScanner.subscribe_with_initializer(scanner, [bucket], {:committed, "orders"})
:ok = Fluss.LogScanner.poll(scanner, 1_000)
receive do
  {:fluss_records, records} ->
    Enum.each(records, &process/1)
    {:ok, result} = Fluss.LogScanner.commit_sync(scanner)
    true = result.request.status == :success
    true = Enum.all?(result.buckets, fn {_, outcome} -> outcome.status == :success end)
  {:fluss_poll_error, error} -> raise error
end
```

For multiple tables, call `Fluss.LogScanner.new_multi(conn, [{"db", "table_a"}, {"db", "table_b"}], group_id: "orders")`. Records include `:table_bucket` and retain their source table's schema. Consumer scanners use table-qualified `{table_id, partition_id_or_nil, bucket_id}` tuples with `subscribe_with_initializer/3` and `unsubscribe/2`. Drive polling, assignment and commits from one process; receive and process one poll result before issuing another.

`{:committed, group}` defaults missing commits to `:earliest`. A third element selects `:latest`, `{:timestamp, epoch_ms}`, `{:offset, next_offset}`, or `:fail`. Authorization and transport failures never select a fallback. Assignment is manual, without membership or rebalance.

Explicit commits take `[{bucket, {next_offset, metadata_or_nil}}]`; nil and empty metadata remain distinct. `commit_async/2` enqueues immediately and returns a reference for `{ref, {:ok, result}}`. Not receiving the result does not cancel the ordered operation. Inspect `result.request` and every `result.buckets` outcome: statuses are `:success`, `:rejected`, `:unknown`, or `:needs_recovery`, with `:error_code` and `:message`. An unknown write blocks subsequent commits on the scanner; settle the old writer before recovering.

The optional `:auto_commit_interval_ms` enables commits before a subsequent poll when due. Finish processing the previous poll first. There is no background commit during processing or final commit on garbage collection; call `commit_sync/1` after the last processed batch.

`Fluss.Admin` provides `get_consumer_offsets/3`, `list_consumer_offsets/2`, `list_consumer_groups/1`, `alter_consumer_offsets/3`, `delete_consumer_offsets/3`, and `delete_consumer_groups/2`. Selected lookup returns a map of buckets to `{next_offset, metadata_or_nil}` or nil; all-offset listing omits unauthorized tables. Queries require GROUP DESCRIBE and, for selected lookup, TABLE DESCRIBE on each source. Reset requires GROUP READ plus TABLE READ; selected deletion GROUP WRITE; group deletion GROUP DROP. Missing deletion targets succeed. Inspect per-bucket/group mutation outcomes, and stop consumers before resetting or deleting: the server does not fence competing writers, and later commits recreate a deleted group.
