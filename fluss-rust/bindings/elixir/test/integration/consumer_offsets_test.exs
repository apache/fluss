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

defmodule Fluss.Integration.ConsumerOffsetsTest do
  use Fluss.Test.IntegrationCase, async: false

  test "cross-table schemas, ordered commits and management", %{conn: conn, admin: admin} do
    suffix = System.unique_integer([:positive])
    group = "elixir-#{suffix}"
    tables = [{"fluss", "offset_a_#{suffix}"}, {"fluss", "offset_b_#{suffix}"}]

    buckets =
      for {{database, name}, index} <- Enum.with_index(tables) do
        schema =
          Fluss.Schema.column(
            Fluss.Schema.new(),
            if(index == 0, do: "id", else: "name"),
            if(index == 0, do: :int, else: :string)
          )

        descriptor = Fluss.TableDescriptor.new!(schema, bucket_count: 1)
        :ok = Fluss.Admin.create_table(admin, database, name, descriptor)
        table = Fluss.Table.get!(conn, database, name)
        writer = Fluss.AppendWriter.new!(table)

        for value <- 0..2 do
          {:ok, _} =
            Fluss.AppendWriter.append(writer, [if(index == 0, do: value, else: to_string(value))])
        end

        :ok = Fluss.AppendWriter.flush(writer)
        info = Fluss.Admin.get_table_info!(admin, database, name)
        {info.table_id, nil, 0}
      end

    # If the polling process exits without processing its mailbox, another process must
    # restore, not commit the dead consumer's native cursor.
    orphan_group = group <> "-orphan"
    {:ok, orphan} = Fluss.LogScanner.new_multi(conn, tables, group_id: orphan_group)
    :ok = Fluss.LogScanner.subscribe_from_committed(orphan, buckets)
    assert :ok = Task.async(fn -> Fluss.LogScanner.poll(orphan, 1000) end) |> Task.await()
    assert {:ok, %{request: %{status: :rejected}}} = Fluss.LogScanner.commit_sync(orphan)
    assert {:ok, orphan_offsets} = Fluss.Admin.get_consumer_offsets(admin, orphan_group, buckets)
    assert Enum.all?(orphan_offsets, fn {_bucket, saved} -> is_nil(saved) end)

    [a, b] = buckets
    {:ok, scanner} = Fluss.LogScanner.new_multi(conn, tables, group_id: group)
    :ok = Fluss.LogScanner.subscribe_from_committed(scanner, buckets)
    records = poll(scanner) ++ poll(scanner)
    assert Enum.map(Enum.filter(records, &(&1.table_bucket == a)), & &1.row.id) == [0, 1, 2]

    assert Enum.map(Enum.filter(records, &(&1.table_bucket == b)), & &1.row.name) == [
             "0",
             "1",
             "2"
           ]

    # A different process cannot acknowledge a delivery made to this mailbox.
    assert {:ok, %{request: %{status: :rejected}}} =
             Task.async(fn -> Fluss.LogScanner.commit_sync(scanner) end) |> Task.await()

    {:ok, partial} =
      Fluss.LogScanner.commit_sync(scanner, [{a, {1, nil}}, {b, {1, String.duplicate("x", 5000)}}])

    assert partial.request.status == :success
    assert partial.buckets[a].status == :success
    assert partial.buckets[b].status == :rejected
    assert partial.buckets[b].error_code == 80
    first = Fluss.LogScanner.commit_async(scanner, [{a, {2, ""}}])
    second = Fluss.LogScanner.commit_async(scanner, [{a, {1, nil}}])
    assert_receive {^second, {:ok, second_result}}, 30_000
    successful(second_result)
    assert_receive {^first, {:ok, first_result}}, 30_000
    successful(first_result)

    assert {:ok, %{^a => {1, nil}, ^b => nil}} =
             Fluss.Admin.get_consumer_offsets(admin, group, buckets)

    {:ok, reset} =
      Fluss.Admin.alter_consumer_offsets(admin, group, [{a, {1, ""}}, {b, {2, "checkpoint"}}])

    successful(reset)

    assert {:ok, %{^a => {1, ""}, ^b => {2, "checkpoint"}}} =
             Fluss.Admin.list_consumer_offsets(admin, group)

    {:ok, groups} = Fluss.Admin.list_consumer_groups(admin)
    assert group in groups
    {:ok, restored} = Fluss.LogScanner.new_multi(conn, tables, group_id: group)

    :ok =
      Fluss.LogScanner.subscribe_from_committed(restored, buckets)

    remaining = poll(restored) ++ poll(restored)

    assert Enum.sort(Enum.map(remaining, &{&1.table_bucket, &1.offset})) ==
             Enum.sort([{a, 1}, {a, 2}, {b, 2}])

    {:ok, committed} = Fluss.LogScanner.commit_sync(restored)
    successful(committed)

    {:ok, single} =
      Fluss.LogScanner.new(Fluss.Table.get!(conn, "fluss", elem(hd(tables), 1)),
        group_id: group,
        auto_commit_interval_ms: 10
      )

    :ok = Fluss.LogScanner.subscribe(single, a, 1)
    assert length(poll(single)) == 2
    {:ok, single_commit} = Fluss.LogScanner.commit_sync(single)
    successful(single_commit)
    {:ok, deleted} = Fluss.Admin.delete_consumer_offsets(admin, group, [a])
    successful(deleted)

    assert {:ok, %{^group => %{status: :success}}} =
             Fluss.Admin.delete_consumer_groups(admin, [group])

    assert {:ok, offsets} = Fluss.Admin.list_consumer_offsets(admin, group)
    assert offsets == %{}
    for {db, name} <- tables, do: Fluss.Admin.drop_table(admin, db, name)
  end

  defp poll(scanner) do
    :ok = Fluss.LogScanner.poll(scanner, 10_000)

    receive do
      {:fluss_records, records} -> records
      {:fluss_poll_error, error} -> flunk(inspect(error))
    after
      15_000 -> flunk("poll did not complete")
    end
  end

  defp successful(result) do
    assert result.request.status == :success
    assert Enum.all?(result.buckets, fn {_, value} -> value.status == :success end)
  end
end
