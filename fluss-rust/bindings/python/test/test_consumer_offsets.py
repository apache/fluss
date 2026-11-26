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

"""Consumer offsets through Python objects, real RPC, and both scan formats."""

import asyncio
import uuid

import pyarrow as pa
import pytest

import fluss


def successful(result):
    assert result.request.status == "success", result.request.message
    assert all(value.status == "success" for value in result.buckets.values())


@pytest.mark.asyncio
async def test_multi_table_restore_and_management(connection, admin):
    suffix = uuid.uuid4().hex
    group = "python-" + suffix
    paths = [
        fluss.TablePath("fluss", "offset_a_" + suffix),
        fluss.TablePath("fluss", "offset_b_" + suffix),
    ]
    buckets = []
    for index, path in enumerate(paths):
        columns = [("id", pa.int32())] if index == 0 else [("name", pa.string())]
        await admin.create_table(
            path,
            fluss.TableDescriptor(fluss.Schema(pa.schema(columns)), bucket_count=1),
        )
        table = await connection.get_table(path)
        info = await admin.get_table_info(path)
        buckets.append(fluss.TableBucket(info.table_id, 0))
        writer = table.new_append().create_writer()
        for value in range(3):
            writer.append([value] if index == 0 else [str(value)])
        await writer.flush()

    scanner = await connection.create_multi_table_scanner(paths, group)
    scanner.subscribe_from_committed(buckets)
    received = {}
    for _ in range(2):
        records = await scanner.poll(10000)
        for bucket in records.buckets():
            received[bucket] = [record.row for record in records.records(bucket)]
    assert received[buckets[0]] == [{"id": n} for n in range(3)]
    assert received[buckets[1]] == [{"name": str(n)} for n in range(3)]
    assert all(
        value is None
        for value in (await admin.get_consumer_offsets(group, buckets)).values()
    )
    results = []
    scanner.commit_async(
        results.append,
        [
            (buckets[0], fluss.OffsetAndMetadata(1)),
            (buckets[1], fluss.OffsetAndMetadata(1, "x" * 5000)),
        ],
    )

    async def poll_until_callback():
        while not results:
            await scanner.poll(20)

    await asyncio.wait_for(poll_until_callback(), timeout=10)
    (partial,) = results
    assert partial.request.status == "success"
    assert partial.buckets[buckets[0]].status == "success"
    assert partial.buckets[buckets[1]].status == "rejected"
    assert partial.buckets[buckets[1]].error_code == 80
    calls = []
    scanner.commit_async(
        lambda r: calls.append((1, r)), [(buckets[0], fluss.OffsetAndMetadata(2, ""))]
    )
    scanner.commit_async(
        lambda r: calls.append((2, r)), [(buckets[0], fluss.OffsetAndMetadata(1))]
    )
    successful(scanner.commit_sync([]))
    assert [index for index, _ in calls] == [1, 2]
    for _, result in calls:
        successful(result)
    saved = await admin.get_consumer_offsets(group, buckets)
    assert saved[buckets[0]].offset == 1
    assert saved[buckets[0]].metadata is None
    assert saved[buckets[1]] is None
    successful(
        await admin.alter_consumer_offsets(
            group,
            [
                (buckets[0], fluss.OffsetAndMetadata(1, "")),
                (buckets[1], fluss.OffsetAndMetadata(2, "checkpoint")),
            ],
        )
    )
    saved = await admin.list_consumer_offsets(group)
    assert saved[buckets[0]].metadata == ""
    assert saved[buckets[1]].metadata == "checkpoint"
    assert group in await admin.list_consumer_groups()
    restored = await connection.create_multi_table_scanner(paths, group, arrow=True)
    with pytest.raises(fluss.FlussError, match="poll_record_batch"):
        await restored.poll(0)
    restored.subscribe_from_committed(buckets)
    starts = {}
    for _ in range(2):
        for batch in await restored.poll_record_batch(10000):
            starts[batch.bucket] = batch.base_offset
    assert starts == {buckets[0]: 1, buckets[1]: 2}
    successful(restored.commit_sync())
    single = (
        await (await connection.get_table(paths[0]))
        .new_scan()
        .with_consumer_group(group)
        .with_auto_commit(10)
        .create_log_scanner()
    )
    with pytest.raises(fluss.FlussError, match="batch-based"):
        await single.poll_record_batch(0)
    single.subscribe(buckets[0].bucket_id, 1)
    assert (await single.poll(10000)).count() == 2
    successful(single.commit_sync())
    successful(await admin.delete_consumer_offsets(group, [buckets[0]]))
    assert (await admin.get_consumer_offsets(group, buckets))[buckets[0]] is None
    deleted = await admin.delete_consumer_groups([group])
    assert deleted[group].status == "success"
    assert not await admin.list_consumer_offsets(group)
    for path in paths:
        await admin.drop_table(path)


@pytest.mark.asyncio
async def test_undelivered_poll_cannot_advance_committed_offset(connection, admin):
    group = "delivery-" + uuid.uuid4().hex
    path = fluss.TablePath("fluss", "delivery_" + uuid.uuid4().hex)
    schema = pa.schema([("d", pa.date32())])
    await admin.create_table(
        path, fluss.TableDescriptor(fluss.Schema(schema), bucket_count=1)
    )
    table = await connection.get_table(path)
    bucket = fluss.TableBucket((await admin.get_table_info(path)).table_id, 0)
    writer = table.new_append().create_writer()
    # Arrow supports year zero; conversion to datetime.date fails after the native poll.
    dates = pa.array([-719528], type=pa.int32()).cast(pa.date32())
    writer.write_arrow_batch(pa.record_batch([dates], schema=schema))
    await writer.flush()

    async def restore(**options):
        scanner = await connection.create_multi_table_scanner([path], group, **options)
        scanner.subscribe_from_committed([bucket])
        return scanner

    scanner = await restore(auto_commit_interval_ms=1)
    with pytest.raises(ValueError, match="year 0"):
        await scanner.poll(5000)
    with pytest.raises(fluss.FlussError, match="recreate"):
        scanner.commit_async(successful)
    with pytest.raises(fluss.FlussError, match="recreate"):
        await scanner.poll(100)
    assert (await admin.get_consumer_offsets(group, [bucket]))[bucket] is None

    # Cancel after starting the native poll, before its result reaches the task.
    abandoned = await restore(arrow=True)
    task = asyncio.create_task(abandoned.poll_record_batch(5000))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    with pytest.raises(fluss.FlussError, match="recreate"):
        abandoned.commit_sync()

    scanner = await restore(arrow=True)
    assert [b.base_offset for b in await scanner.poll_record_batch(5000)] == [0]
    successful(scanner.commit_sync())
    await admin.drop_table(path)
