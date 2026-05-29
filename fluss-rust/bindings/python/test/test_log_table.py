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

"""Integration tests for log (append-only) table operations.

Mirrors the Rust integration tests in crates/fluss/tests/integration/log_table.rs.
"""

import asyncio
import time

import pyarrow as pa
import pytest

import fluss


async def test_append_and_scan(connection, admin):
    """Test appending record batches and scanning with a record-based scanner."""
    table_path = fluss.TablePath("fluss", "py_test_append_and_scan")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("c1", pa.int32()), pa.field("c2", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(
        schema, bucket_count=3, bucket_keys=["c1"]
    )
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()

    batch1 = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3], type=pa.int32()), pa.array(["a1", "a2", "a3"])],
        schema=pa.schema([pa.field("c1", pa.int32()), pa.field("c2", pa.string())]),
    )
    append_writer.write_arrow_batch(batch1)

    batch2 = pa.RecordBatch.from_arrays(
        [pa.array([4, 5, 6], type=pa.int32()), pa.array(["a4", "a5", "a6"])],
        schema=pa.schema([pa.field("c1", pa.int32()), pa.field("c2", pa.string())]),
    )
    append_writer.write_arrow_batch(batch2)

    await append_writer.flush()

    # Scan with record-based scanner
    scanner = await table.new_scan().create_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

    records = await _poll_records(scanner, expected_count=6)

    assert len(records) == 6, f"Expected 6 records, got {len(records)}"

    records.sort(key=lambda r: r.row["c1"])

    expected_c1 = [1, 2, 3, 4, 5, 6]
    expected_c2 = ["a1", "a2", "a3", "a4", "a5", "a6"]
    for i, record in enumerate(records):
        assert record.row["c1"] == expected_c1[i], f"c1 mismatch at row {i}"
        assert record.row["c2"] == expected_c2[i], f"c2 mismatch at row {i}"

    # Test unsubscribe
    scanner.unsubscribe(bucket_id=0)

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_append_dict_rows(connection, admin):
    """Test appending rows as dicts and scanning."""
    table_path = fluss.TablePath("fluss", "py_test_append_dict_rows")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()

    # Append using dicts
    append_writer.append({"id": 1, "name": "Alice"})
    append_writer.append({"id": 2, "name": "Bob"})
    # Append using lists
    append_writer.append([3, "Charlie"])
    await append_writer.flush()

    scanner = await table.new_scan().create_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

    records = await _poll_records(scanner, expected_count=3)
    assert len(records) == 3

    rows = sorted([r.row for r in records], key=lambda r: r["id"])
    assert rows[0] == {"id": 1, "name": "Alice"}
    assert rows[1] == {"id": 2, "name": "Bob"}
    assert rows[2] == {"id": 3, "name": "Charlie"}

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_list_offsets(connection, admin, wait_for_table_ready):
    """Test listing earliest, latest, and timestamp-based offsets."""
    table_path = fluss.TablePath("fluss", "py_test_list_offsets")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    await wait_for_table_ready(table_path)

    # Earliest offset should be 0 for empty table
    earliest = await admin.list_offsets(
        table_path, bucket_ids=[0], offset_spec=fluss.OffsetSpec.earliest()
    )
    assert earliest[0] == 0

    # Latest offset should be 0 for empty table
    latest = await admin.list_offsets(
        table_path, bucket_ids=[0], offset_spec=fluss.OffsetSpec.latest()
    )
    assert latest[0] == 0

    before_append_ms = int(time.time() * 1000)

    # Append some records
    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(["alice", "bob", "charlie"]),
        ],
        schema=pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())]),
    )
    append_writer.write_arrow_batch(batch)
    await append_writer.flush()

    await asyncio.sleep(1)

    after_append_ms = int(time.time() * 1000)

    # Latest offset should be 3 after appending 3 records
    latest_after = await admin.list_offsets(
        table_path, bucket_ids=[0], offset_spec=fluss.OffsetSpec.latest()
    )
    assert latest_after[0] == 3

    # Earliest offset should still be 0
    earliest_after = await admin.list_offsets(
        table_path, bucket_ids=[0], offset_spec=fluss.OffsetSpec.earliest()
    )
    assert earliest_after[0] == 0

    # Timestamp before append should resolve to offset 0
    ts_before = await admin.list_offsets(
        table_path,
        bucket_ids=[0],
        offset_spec=fluss.OffsetSpec.timestamp(before_append_ms),
    )
    assert ts_before[0] == 0

    # Intentional sleep to avoid race condition FlussError(code=38) The timestamp is invalid
    await asyncio.sleep(1)

    # Timestamp after append should resolve to offset 3
    ts_after = await admin.list_offsets(
        table_path,
        bucket_ids=[0],
        offset_spec=fluss.OffsetSpec.timestamp(after_append_ms),
    )
    assert ts_after[0] == 3

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_project(connection, admin):
    """Test column projection by name and by index."""
    table_path = fluss.TablePath("fluss", "py_test_project")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("col_a", pa.int32()),
                pa.field("col_b", pa.string()),
                pa.field("col_c", pa.int32()),
            ]
        )
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(["x", "y", "z"]),
            pa.array([10, 20, 30], type=pa.int32()),
        ],
        schema=pa.schema(
            [
                pa.field("col_a", pa.int32()),
                pa.field("col_b", pa.string()),
                pa.field("col_c", pa.int32()),
            ]
        ),
    )
    append_writer.write_arrow_batch(batch)
    await append_writer.flush()

    # Test project_by_name: select col_b and col_c only
    scan = table.new_scan().project_by_name(["col_b", "col_c"])
    scanner = await scan.create_log_scanner()
    scanner.subscribe_buckets({0: 0})

    records = await _poll_records(scanner, expected_count=3)
    assert len(records) == 3

    records.sort(key=lambda r: r.row["col_c"])
    expected_col_b = ["x", "y", "z"]
    expected_col_c = [10, 20, 30]
    for i, record in enumerate(records):
        assert record.row["col_b"] == expected_col_b[i]
        assert record.row["col_c"] == expected_col_c[i]
        # col_a should not be present in projected results
        assert "col_a" not in record.row

    # Test project by indices [1, 0] -> (col_b, col_a)
    scanner2 = await table.new_scan().project([1, 0]).create_log_scanner()
    scanner2.subscribe_buckets({0: 0})

    records2 = await _poll_records(scanner2, expected_count=3)
    assert len(records2) == 3

    records2.sort(key=lambda r: r.row["col_a"])
    for i, record in enumerate(records2):
        assert record.row["col_b"] == expected_col_b[i]
        assert record.row["col_a"] == [1, 2, 3][i]
        assert "col_c" not in record.row

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_poll_batches(connection, admin, wait_for_table_ready):
    """Test batch-based scanning with poll_arrow and poll_record_batch."""
    table_path = fluss.TablePath("fluss", "py_test_poll_batches")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    await wait_for_table_ready(table_path)

    table = await connection.get_table(table_path)
    scanner = await table.new_scan().create_record_batch_log_scanner()
    scanner.subscribe(bucket_id=0, start_offset=0)

    # Empty table should return empty result
    result = await scanner.poll_arrow(500)
    assert result.num_rows == 0

    writer = table.new_append().create_writer()
    pa_schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [pa.array([1, 2], type=pa.int32()), pa.array(["a", "b"])],
            schema=pa_schema,
        )
    )
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [pa.array([3, 4], type=pa.int32()), pa.array(["c", "d"])],
            schema=pa_schema,
        )
    )
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [pa.array([5, 6], type=pa.int32()), pa.array(["e", "f"])],
            schema=pa_schema,
        )
    )
    await writer.flush()

    # Poll until we get all 6 records
    all_ids = await _poll_arrow_ids(scanner, expected_count=6)
    assert all_ids == [1, 2, 3, 4, 5, 6]

    # Append more and verify offset continuation (no duplicates)
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [pa.array([7, 8], type=pa.int32()), pa.array(["g", "h"])],
            schema=pa_schema,
        )
    )
    await writer.flush()

    new_ids = await _poll_arrow_ids(scanner, expected_count=2)
    assert new_ids == [7, 8]

    # Subscribe from mid-offset should truncate (skip earlier records)
    trunc_scanner = await table.new_scan().create_record_batch_log_scanner()
    trunc_scanner.subscribe(bucket_id=0, start_offset=3)

    trunc_ids = await _poll_arrow_ids(trunc_scanner, expected_count=5)
    assert trunc_ids == [4, 5, 6, 7, 8]

    # Projection with batch scanner
    proj_scanner = (
        await table.new_scan()
        .project_by_name(["id"])
        .create_record_batch_log_scanner()
    )
    proj_scanner.subscribe(bucket_id=0, start_offset=0)
    batches = await proj_scanner.poll_record_batch(10000)
    assert len(batches) > 0
    assert batches[0].batch.num_columns == 1

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_to_arrow_and_to_pandas(connection, admin):
    """Test to_arrow() and to_pandas() convenience methods."""
    table_path = fluss.TablePath("fluss", "py_test_to_arrow_pandas")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()

    pa_schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3], type=pa.int32()), pa.array(["a", "b", "c"])],
            schema=pa_schema,
        )
    )
    await writer.flush()

    num_buckets = (await admin.get_table_info(table_path)).num_buckets

    # to_arrow()
    scanner = await table.new_scan().create_record_batch_log_scanner()
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
    arrow_table = await scanner.to_arrow()
    assert arrow_table.num_rows == 3
    assert arrow_table.schema.names == ["id", "name"]

    # to_pandas()
    scanner2 = await table.new_scan().create_record_batch_log_scanner()
    scanner2.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
    df = await scanner2.to_pandas()
    assert len(df) == 3
    assert list(df.columns) == ["id", "name"]

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_to_arrow_batch_reader(connection, admin):
    """Test to_arrow_batch_reader() returns a lazy PyArrow RecordBatchReader."""
    table_path = fluss.TablePath("fluss", "py_test_to_arrow_batch_reader")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()

    pa_schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [pa.array([10, 20, 30], type=pa.int32()), pa.array(["x", "y", "z"])],
            schema=pa_schema,
        )
    )
    await writer.flush()

    num_buckets = (await admin.get_table_info(table_path)).num_buckets

    scanner = await table.new_scan().create_record_batch_log_scanner()
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

    # to_arrow_batch_reader() is a blocking/sync API; run in a thread to
    # avoid starving the asyncio event loop (see docstring warning).
    def _read_all():
        reader = scanner.to_arrow_batch_reader()
        assert isinstance(reader, pa.RecordBatchReader)
        assert reader.schema == pa_schema

        batches = list(reader)
        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == 3

        result_table = pa.Table.from_batches(batches, schema=pa_schema)
        assert result_table.column("id").to_pylist() == [10, 20, 30]
        assert result_table.column("name").to_pylist() == ["x", "y", "z"]

    await asyncio.to_thread(_read_all)

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_to_arrow_batch_reader_drop_and_guard(connection, admin):
    """Test reader-active guard and Drop cleanup on mid-iteration drop."""
    table_path = fluss.TablePath("fluss", "py_test_batch_reader_drop_guard")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()

    pa_schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    # Write multiple separate flushes so the server stores multiple log
    # batches per bucket. This makes it likely that the reader's first poll
    # only drains a subset, leaving real work for the Drop cleanup loop.
    num_flushes = 10
    rows_per_flush = 200
    total_rows = num_flushes * rows_per_flush
    for f in range(num_flushes):
        start = f * rows_per_flush
        writer.write_arrow_batch(
            pa.RecordBatch.from_arrays(
                [
                    pa.array(
                        list(range(start, start + rows_per_flush)), type=pa.int32()
                    ),
                    pa.array(
                        [f"row_{i}" for i in range(start, start + rows_per_flush)]
                    ),
                ],
                schema=pa_schema,
            )
        )
        await writer.flush()

    num_buckets = (await admin.get_table_info(table_path)).num_buckets

    scanner = await table.new_scan().create_record_batch_log_scanner()
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

    # to_arrow_batch_reader() is a blocking/sync API; run all blocking
    # interactions in a thread to avoid starving the asyncio event loop.
    def _test_guard_and_drop():
        # --- Guard blocks subscribe / unsubscribe while reader is active ---
        reader = scanner.to_arrow_batch_reader()
        with pytest.raises(fluss.FlussError, match="RecordBatchLogReader is active"):
            scanner.subscribe_buckets({0: fluss.EARLIEST_OFFSET})
        with pytest.raises(fluss.FlussError, match="RecordBatchLogReader is active"):
            scanner.unsubscribe(0)

        # --- Drop mid-iteration: read one batch, then discard ---
        first_batch = next(reader)
        assert first_batch.num_rows > 0
        del reader

        # --- Drop unsubscribed leftover buckets: creating a reader without
        #     re-subscribing must fail with "No buckets subscribed" ---
        with pytest.raises(fluss.FlussError, match="No buckets subscribed"):
            scanner.to_arrow_batch_reader()

        # --- Guard cleared after drop: scanner is reusable from a fresh subscribe ---
        scanner.subscribe_buckets(
            {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
        )
        reader2 = scanner.to_arrow_batch_reader()
        batches = list(reader2)
        assert sum(b.num_rows for b in batches) == total_rows

    await asyncio.to_thread(_test_guard_and_drop)

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_partitioned_table_append_scan(connection, admin, wait_for_table_ready):
    """Test append and scan on a partitioned log table."""
    table_path = fluss.TablePath("fluss", "py_test_partitioned_log_append")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("region", pa.string()),
                pa.field("value", pa.int64()),
            ]
        )
    )
    table_descriptor = fluss.TableDescriptor(
        schema,
        partition_keys=["region"],
    )
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    # Create partitions
    for region in ["US", "EU"]:
        await admin.create_partition(
            table_path, {"region": region}, ignore_if_exists=True
        )
        await wait_for_table_ready(table_path, partition_name=region)
    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()

    # Append rows
    test_data = [
        (1, "US", 100),
        (2, "US", 200),
        (3, "EU", 300),
        (4, "EU", 400),
    ]
    for id_, region, value in test_data:
        append_writer.append({"id": id_, "region": region, "value": value})
    await append_writer.flush()

    # Append arrow batches per partition
    pa_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("region", pa.string()),
            pa.field("value", pa.int64()),
        ]
    )
    us_batch = pa.RecordBatch.from_arrays(
        [
            pa.array([5, 6], type=pa.int32()),
            pa.array(["US", "US"]),
            pa.array([500, 600], type=pa.int64()),
        ],
        schema=pa_schema,
    )
    append_writer.write_arrow_batch(us_batch)

    eu_batch = pa.RecordBatch.from_arrays(
        [
            pa.array([7, 8], type=pa.int32()),
            pa.array(["EU", "EU"]),
            pa.array([700, 800], type=pa.int64()),
        ],
        schema=pa_schema,
    )
    append_writer.write_arrow_batch(eu_batch)
    await append_writer.flush()

    # Verify partition offsets
    us_offsets = await admin.list_partition_offsets(
        table_path,
        partition_name="US",
        bucket_ids=[0],
        offset_spec=fluss.OffsetSpec.latest(),
    )
    assert us_offsets[0] == 4, "US partition should have 4 records"

    eu_offsets = await admin.list_partition_offsets(
        table_path,
        partition_name="EU",
        bucket_ids=[0],
        offset_spec=fluss.OffsetSpec.latest(),
    )
    assert eu_offsets[0] == 4, "EU partition should have 4 records"

    # Scan all partitions
    scanner = await table.new_scan().create_log_scanner()
    partition_infos = await admin.list_partition_infos(table_path)
    for p in partition_infos:
        scanner.subscribe_partition(
            partition_id=p.partition_id, bucket_id=0, start_offset=0
        )

    expected = [
        (1, "US", 100),
        (2, "US", 200),
        (3, "EU", 300),
        (4, "EU", 400),
        (5, "US", 500),
        (6, "US", 600),
        (7, "EU", 700),
        (8, "EU", 800),
    ]

    # Poll and verify per-bucket grouping
    all_records = []
    deadline = time.monotonic() + 10
    while len(all_records) < 8 and time.monotonic() < deadline:
        scan_records = await scanner.poll(5000)
        for bucket, bucket_records in scan_records.items():
            assert bucket.partition_id is not None, "Partitioned table should have partition_id"
            # All records in a bucket should belong to the same partition
            regions = {r.row["region"] for r in bucket_records}
            assert len(regions) == 1, f"Bucket has mixed regions: {regions}"
            all_records.extend(bucket_records)

    assert len(all_records) == 8

    collected = sorted(
        [(r.row["id"], r.row["region"], r.row["value"]) for r in all_records],
        key=lambda x: x[0],
    )
    assert collected == expected

    # Test unsubscribe_partition: unsubscribe from EU, only US data should remain
    unsub_scanner = await table.new_scan().create_log_scanner()
    eu_partition_id = next(
        p.partition_id for p in partition_infos if p.partition_name == "EU"
    )
    for p in partition_infos:
        unsub_scanner.subscribe_partition(p.partition_id, 0, 0)
    unsub_scanner.unsubscribe_partition(eu_partition_id, 0)

    remaining = await _poll_records(unsub_scanner, expected_count=4, timeout_s=5)
    assert len(remaining) == 4
    assert all(r.row["region"] == "US" for r in remaining)

    # Test subscribe_partition_buckets (batch subscribe)
    batch_scanner = await table.new_scan().create_log_scanner()
    partition_bucket_offsets = {
        (p.partition_id, 0): fluss.EARLIEST_OFFSET for p in partition_infos
    }
    batch_scanner.subscribe_partition_buckets(partition_bucket_offsets)

    batch_records = await _poll_records(batch_scanner, expected_count=8)
    assert len(batch_records) == 8
    batch_collected = sorted(
        [(r.row["id"], r.row["region"], r.row["value"]) for r in batch_records],
        key=lambda x: x[0],
    )
    assert batch_collected == expected

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_write_arrow(connection, admin):
    """Test writing a full PyArrow Table via write_arrow()."""
    table_path = fluss.TablePath("fluss", "py_test_write_arrow")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()

    pa_schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    arrow_table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
            "name": pa.array(["alice", "bob", "charlie", "dave", "eve"]),
        },
        schema=pa_schema,
    )
    writer.write_arrow(arrow_table)
    await writer.flush()

    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    scanner = await table.new_scan().create_record_batch_log_scanner()
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

    result = await scanner.to_arrow()
    assert result.num_rows == 5

    ids = sorted(result.column("id").to_pylist())
    names = [
        n
        for _, n in sorted(
            zip(result.column("id").to_pylist(), result.column("name").to_pylist())
        )
    ]
    assert ids == [1, 2, 3, 4, 5]
    assert names == ["alice", "bob", "charlie", "dave", "eve"]

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_write_pandas(connection, admin):
    """Test writing a Pandas DataFrame via write_pandas()."""
    import pandas as pd

    table_path = fluss.TablePath("fluss", "py_test_write_pandas")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()

    df = pd.DataFrame({"id": [10, 20, 30], "name": ["x", "y", "z"]})
    writer.write_pandas(df)
    await writer.flush()

    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    scanner = await table.new_scan().create_record_batch_log_scanner()
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

    result = await scanner.to_pandas()
    assert len(result) == 3

    result_sorted = result.sort_values("id").reset_index(drop=True)
    assert result_sorted["id"].tolist() == [10, 20, 30]
    assert result_sorted["name"].tolist() == ["x", "y", "z"]

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_partitioned_table_to_arrow(connection, admin, wait_for_table_ready):
    """Test to_arrow() on partitioned tables."""
    table_path = fluss.TablePath("fluss", "py_test_partitioned_to_arrow")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("region", pa.string()),
                pa.field("value", pa.int64()),
            ]
        )
    )
    table_descriptor = fluss.TableDescriptor(schema, partition_keys=["region"])
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    for region in ["US", "EU"]:
        await admin.create_partition(
            table_path, {"region": region}, ignore_if_exists=True
        )
        await wait_for_table_ready(table_path, partition_name=region)

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()
    writer.append({"id": 1, "region": "US", "value": 100})
    writer.append({"id": 2, "region": "EU", "value": 200})
    await writer.flush()

    scanner = await table.new_scan().create_record_batch_log_scanner()
    partition_infos = await admin.list_partition_infos(table_path)
    for p in partition_infos:
        scanner.subscribe_partition(p.partition_id, 0, fluss.EARLIEST_OFFSET)

    arrow_table = await scanner.to_arrow()
    assert arrow_table.num_rows == 2

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_scan_records_indexing_and_slicing(connection, admin):
    """Test ScanRecords indexing, slicing (incl. negative steps), and iteration consistency."""
    table_path = fluss.TablePath("fluss", "py_test_scan_records_indexing")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.string())])
    )
    await admin.create_table(table_path, fluss.TableDescriptor(schema))

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [pa.array(list(range(1, 9)), type=pa.int32()),
             pa.array([f"v{i}" for i in range(1, 9)])],
            schema=pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.string())]),
        )
    )
    await writer.flush()

    scanner = await table.new_scan().create_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

    # Poll until we get a non-empty ScanRecords (need ≥2 records for slice tests)
    sr = None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        sr = await scanner.poll(5000)
        if len(sr) >= 2:
            break
    assert sr is not None and len(sr) >= 2, "Expected at least 2 records"
    n = len(sr)
    offsets = [sr[i].offset for i in range(n)]

    # Iteration and indexing must produce the same order
    assert [r.offset for r in sr] == offsets

    # Negative indexing
    assert sr[-1].offset == offsets[-1]
    assert sr[-n].offset == offsets[0]

    # Verify slices match the same operation on the offsets reference list
    test_slices = [
        slice(1, n - 1),          # forward subrange
        slice(None, None, -1),    # [::-1] full reverse
        slice(n - 2, 0, -1),      # reverse with bounds
        slice(n - 1, 0, -2),      # reverse with step
        slice(None, None, 2),     # [::2]
        slice(1, None, 3),        # [1::3]
        slice(2, 2),              # empty
    ]
    for s in test_slices:
        result = [r.offset for r in sr[s]]
        assert result == offsets[s], f"slice {s}: got {result}, expected {offsets[s]}"

    # Bucket-based indexing
    for bucket in sr.buckets():
        assert len(sr[bucket]) > 0

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_async_iterator(connection, admin):
    """Test the Python asynchronous iterator loop (`async for`) on LogScanner."""
    table_path = fluss.TablePath("fluss", "py_test_async_iterator")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.string())])
    )
    await admin.create_table(table_path, fluss.TableDescriptor(schema))

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()
    
    # Write 5 records
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [pa.array(list(range(1, 6)), type=pa.int32()),
             pa.array([f"async{i}" for i in range(1, 6)])],
            schema=pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.string())]),
        )
    )
    await writer.flush()

    scanner = await table.new_scan().create_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

    collected = []
    
    # Here is the magical Issue #424 async iterator logic at work:
    async def consume_scanner():
        async for record in scanner:
            collected.append(record)
            if len(collected) == 5:
                break
                
    await consume_scanner()
    
    assert len(collected) == 5, f"Expected 5 records, got {len(collected)}"
    
    collected.sort(key=lambda r: r.row["id"])
    for i, record in enumerate(collected):
        assert record.row["id"] == i + 1
        assert record.row["val"] == f"async{i + 1}"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_async_iterator_break_no_leak(connection, admin):
    """Verify that breaking out of `async for` does not leak resources.

    After breaking, the scanner must still be usable for synchronous
    `poll()` calls.  If the old implementation's tokio::spawn'd task
    were still alive, it would hold the Mutex and cause `poll()` to
    deadlock or error.
    """
    table_path = fluss.TablePath("fluss", "py_test_async_break_leak")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.string())])
    )
    await admin.create_table(table_path, fluss.TableDescriptor(schema))

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [
                pa.array(list(range(1, 11)), type=pa.int32()),
                pa.array([f"v{i}" for i in range(1, 11)]),
            ],
            schema=pa.schema(
                [pa.field("id", pa.int32()), pa.field("val", pa.string())]
            ),
        )
    )
    await writer.flush()

    scanner = await table.new_scan().create_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    scanner.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )

    # Phase 1: async for with early break (collect only 3 of 10)
    collected_async = []

    async def consume_and_break():
        async for record in scanner:
            collected_async.append(record)
            if len(collected_async) >= 3:
                break

    await consume_and_break()
    assert len(collected_async) == 3, (
        f"Expected 3 records from async for, got {len(collected_async)}"
    )

    # Phase 2: sync poll() must still work — proves no leaked task / lock.
    # With small data and few buckets, _async_poll may have fetched all
    # records in one batch. After break, the un-yielded records from that
    # batch are lost. So sync poll may return 0 records — the key assertion
    # is that poll() completes without deadlock (returns within timeout).
    remaining = await scanner.poll(2000)
    assert remaining is not None, "poll() should return (not deadlock)"

    # If we got records, verify no duplicates
    async_ids = {r.row["id"] for r in collected_async}
    sync_ids = {r.row["id"] for r in remaining}
    assert async_ids.isdisjoint(sync_ids), (
        f"Duplicate IDs between async and sync: {async_ids & sync_ids}"
    )

    # All IDs must be from the original 1-10 range
    all_ids = async_ids | sync_ids
    assert all_ids.issubset(set(range(1, 11))), (
        f"Unexpected IDs: {all_ids - set(range(1, 11))}"
    )

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_async_iterator_multiple_batches(connection, admin):
    """Verify async iteration works across multiple network poll cycles.

    _async_poll does a single bounded poll per call.  Writing 20 records
    to multiple buckets ensures the Python generator must loop through
    several _async_poll calls to collect them all.
    """
    table_path = fluss.TablePath("fluss", "py_test_async_multi_batch")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(
        schema, bucket_count=3, bucket_keys=["id"]
    )
    await admin.create_table(
        table_path, table_descriptor, ignore_if_exists=False
    )

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()

    num_records = 20
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [
                pa.array(list(range(1, num_records + 1)), type=pa.int32()),
                pa.array([f"multi{i}" for i in range(1, num_records + 1)]),
            ],
            schema=pa.schema(
                [pa.field("id", pa.int32()), pa.field("val", pa.string())]
            ),
        )
    )
    await writer.flush()

    scanner = await table.new_scan().create_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    scanner.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )

    collected = []

    async def consume_all():
        async for record in scanner:
            collected.append(record)
            if len(collected) >= num_records:
                break

    await consume_all()
    assert len(collected) == num_records, (
        f"Expected {num_records} records, got {len(collected)}"
    )

    # Verify all IDs are present (order may vary due to bucketing)
    ids = sorted(r.row["id"] for r in collected)
    assert ids == list(range(1, num_records + 1))

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_batch_async_iterator(connection, admin):
    """Test the Python asynchronous iterator loop (`async for`) on a batch LogScanner.

    With our __aiter__ dispatch, a batch-based scanner should yield RecordBatch
    objects (not ScanRecord). Each yielded item has .batch (PyArrow RecordBatch),
    .bucket, .base_offset, .last_offset.
    """
    table_path = fluss.TablePath("fluss", "py_test_batch_async_iter")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.string())])
    )
    await admin.create_table(table_path, fluss.TableDescriptor(schema))

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [
                pa.array(list(range(1, 7)), type=pa.int32()),
                pa.array([f"bv{i}" for i in range(1, 7)]),
            ],
            schema=pa.schema(
                [pa.field("id", pa.int32()), pa.field("val", pa.string())]
            ),
        )
    )
    await writer.flush()

    batch_scanner = await table.new_scan().create_record_batch_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    batch_scanner.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )

    collected_batches = []
    total_rows = 0

    async def consume_batches():
        nonlocal total_rows
        async for rb in batch_scanner:
            collected_batches.append(rb)
            total_rows += rb.batch.num_rows
            if total_rows >= 6:
                break

    await consume_batches()

    assert total_rows >= 6, f"Expected >=6 total rows, got {total_rows}"
    assert len(collected_batches) > 0

    # Verify each yielded item is a RecordBatch with expected attributes
    for rb in collected_batches:
        assert hasattr(rb, "batch"), "RecordBatch should have .batch"
        assert hasattr(rb, "bucket"), "RecordBatch should have .bucket"
        assert hasattr(rb, "base_offset"), "RecordBatch should have .base_offset"
        assert hasattr(rb, "last_offset"), "RecordBatch should have .last_offset"
        # .batch should be a PyArrow RecordBatch
        arrow_batch = rb.batch
        assert isinstance(arrow_batch, pa.RecordBatch), (
            f"Expected PyArrow RecordBatch, got {type(arrow_batch).__name__}"
        )
        assert arrow_batch.num_columns == 2
        assert set(arrow_batch.schema.names) == {"id", "val"}

    # Verify all 6 IDs are present
    all_ids = []
    for rb in collected_batches:
        all_ids.extend(rb.batch.column("id").to_pylist())
    assert sorted(all_ids[:6]) == [1, 2, 3, 4, 5, 6]

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_batch_async_iterator_break_no_leak(connection, admin):
    """Verify that breaking out of batch `async for` does not leak resources.

    After breaking, the scanner must still be usable for synchronous
    poll_record_batch() calls, proving no leaked task or lock.
    """
    table_path = fluss.TablePath("fluss", "py_test_batch_async_break")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.string())])
    )
    await admin.create_table(table_path, fluss.TableDescriptor(schema))

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [
                pa.array(list(range(1, 11)), type=pa.int32()),
                pa.array([f"bl{i}" for i in range(1, 11)]),
            ],
            schema=pa.schema(
                [pa.field("id", pa.int32()), pa.field("val", pa.string())]
            ),
        )
    )
    await writer.flush()

    batch_scanner = await table.new_scan().create_record_batch_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    batch_scanner.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )

    # Phase 1: async for with early break (collect just 1 batch)
    first_batch = None

    async def consume_and_break():
        nonlocal first_batch
        async for rb in batch_scanner:
            first_batch = rb
            break

    await consume_and_break()
    assert first_batch is not None, "Should have received at least 1 batch"
    assert first_batch.batch.num_rows > 0

    # Phase 2: sync poll_record_batch() must still work — proves no leak
    remaining = await batch_scanner.poll_record_batch(2000)
    assert remaining is not None, "poll_record_batch() should return (not deadlock)"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_batch_async_iterator_multiple_batches(connection, admin):
    """Verify batch async iteration works across multiple network poll cycles.

    Writing 20 records to 3 buckets ensures the generator must loop through
    several _async_poll_batches calls to collect them all.
    """
    table_path = fluss.TablePath("fluss", "py_test_batch_async_multi")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(
        schema, bucket_count=3, bucket_keys=["id"]
    )
    await admin.create_table(
        table_path, table_descriptor, ignore_if_exists=False
    )

    table = await connection.get_table(table_path)
    writer = table.new_append().create_writer()

    num_records = 20
    writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [
                pa.array(list(range(1, num_records + 1)), type=pa.int32()),
                pa.array([f"bm{i}" for i in range(1, num_records + 1)]),
            ],
            schema=pa.schema(
                [pa.field("id", pa.int32()), pa.field("val", pa.string())]
            ),
        )
    )
    await writer.flush()

    batch_scanner = await table.new_scan().create_record_batch_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    batch_scanner.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )

    all_ids = []

    async def consume_all():
        async for rb in batch_scanner:
            all_ids.extend(rb.batch.column("id").to_pylist())
            if len(all_ids) >= num_records:
                break

    await consume_all()
    assert len(all_ids) >= num_records, (
        f"Expected >={num_records} IDs, got {len(all_ids)}"
    )
    assert sorted(all_ids[:num_records]) == list(range(1, num_records + 1))

    await admin.drop_table(table_path, ignore_if_not_exists=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _poll_records(scanner, expected_count, timeout_s=10):
    """Poll a record-based scanner until expected_count records are collected."""
    collected = []
    deadline = time.monotonic() + timeout_s
    while len(collected) < expected_count and time.monotonic() < deadline:
        records = await scanner.poll(5000)
        collected.extend(records)
    return collected


async def _poll_arrow_ids(scanner, expected_count, timeout_s=10):
    """Poll a batch scanner and extract 'id' column values."""
    all_ids = []
    deadline = time.monotonic() + timeout_s
    while len(all_ids) < expected_count and time.monotonic() < deadline:
        arrow_table = await scanner.poll_arrow(5000)
        if arrow_table.num_rows > 0:
            all_ids.extend(arrow_table.column("id").to_pylist())
    return all_ids


async def test_append_and_scan_with_array(connection, admin):
    """Test appending and scanning with array columns."""
    table_path = fluss.TablePath("fluss", "py_test_append_and_scan_with_array")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    pa_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("scores", pa.list_(pa.int32())),
        ]
    )
    schema = fluss.Schema(pa_schema)
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()

    # Batch 1: Testing standard lists
    batch1 = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2], type=pa.int32()),
            pa.array([["a", "b"], ["c"]], type=pa.list_(pa.string())),
            pa.array([[10, 20], [30]], type=pa.list_(pa.int32())),
        ],
        schema=pa_schema,
    )
    append_writer.write_arrow_batch(batch1)

    # Batch 2: Testing null values inside arrays and null arrays
    batch2 = pa.RecordBatch.from_arrays(
        [
            pa.array([3, 4, 5, 6], type=pa.int32()),
            pa.array([["d", None], None, [], [None]], type=pa.list_(pa.string())),
            pa.array([[40, 50], [60], None, []], type=pa.list_(pa.int32())),
        ],
        schema=pa_schema,
    )
    append_writer.write_arrow_batch(batch2)
    await append_writer.flush()

    # Verify via LogScanner (record-by-record)
    scanner = await table.new_scan().create_log_scanner()
    scanner.subscribe_buckets({0: fluss.EARLIEST_OFFSET})
    records = await _poll_records(scanner, expected_count=6)

    assert len(records) == 6
    records.sort(key=lambda r: r.row["id"])

    # Verify Batch 1
    assert records[0].row["tags"] == ["a", "b"]
    assert records[0].row["scores"] == [10, 20]
    assert records[1].row["tags"] == ["c"]
    assert records[1].row["scores"] == [30]

    # Verify Batch 2
    assert records[2].row["tags"] == ["d", None]
    assert records[2].row["scores"] == [40, 50]
    assert records[3].row["tags"] is None
    assert records[3].row["scores"] == [60]
    assert records[4].row["tags"] == []
    assert records[4].row["scores"] is None
    assert records[5].row["tags"] == [None]
    assert records[5].row["scores"] == []

    # Verify via to_arrow (batch-based)
    scanner2 = await table.new_scan().create_record_batch_log_scanner()
    scanner2.subscribe_buckets({0: fluss.EARLIEST_OFFSET})
    result_table = await scanner2.to_arrow()

    assert result_table.num_rows == 6
    assert result_table.column("tags").to_pylist() == [
        ["a", "b"],
        ["c"],
        ["d", None],
        None,
        [],
        [None],
    ]
    assert result_table.column("scores").to_pylist() == [
        [10, 20],
        [30],
        [40, 50],
        [60],
        None,
        [],
    ]




async def test_append_rows_with_array(connection, admin):
    """Test appending rows with array data as Python lists and scanning."""
    table_path = fluss.TablePath("fluss", "py_test_append_rows_with_array")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    pa_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("scores", pa.list_(pa.int32())),
        ]
    )
    schema = fluss.Schema(pa_schema)
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()

    # Append rows using dicts with lists
    append_writer.append({"id": 1, "tags": ["a", "b"], "scores": [10, 20]})
    append_writer.append({"id": 2, "tags": ["c"], "scores": [30]})
    # Append row using list with nested list (null handling)
    append_writer.append([3, None, [40, None, 60]])
    
    await append_writer.flush()

    scanner = await table.new_scan().create_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

    records = await _poll_records(scanner, expected_count=3)
    assert len(records) == 3

    rows = sorted([r.row for r in records], key=lambda r: r["id"])
    assert rows[0] == {"id": 1, "tags": ["a", "b"], "scores": [10, 20]}
    assert rows[1] == {"id": 2, "tags": ["c"], "scores": [30]}
    # Note: records[2].row["tags"] will be None, records[2].row["scores"] will be [40, None, 60]
    assert rows[2]["id"] == 3
    assert rows[2]["tags"] is None
    assert rows[2]["scores"] == [40, None, 60]

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_append_rows_with_nested_array(connection, admin):
    """Test appending rows with nested array data (ARRAY<ARRAY<INT>>) and scanning."""
    table_path = fluss.TablePath("fluss", "py_test_append_rows_with_nested_array")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    pa_schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("matrix", pa.list_(pa.list_(pa.int32()))),
    ])
    schema = fluss.Schema(pa_schema)
    await admin.create_table(table_path, fluss.TableDescriptor(schema), ignore_if_exists=False)

    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()

    # Append nested lists
    append_writer.append({"id": 1, "matrix": [[1, 2], [3, 4]]})
    append_writer.append({"id": 2, "matrix": [[], [5], [6, 7, 8]]})
    append_writer.append({"id": 3, "matrix": None})
    append_writer.append({"id": 4, "matrix": [[1, None], None, []]})
    append_writer.append({"id": 5, "matrix": [[None, None]]})
    
    await append_writer.flush()

    scanner = await table.new_scan().create_log_scanner()
    num_buckets = (await admin.get_table_info(table_path)).num_buckets
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

    records = await _poll_records(scanner, expected_count=5)
    assert len(records) == 5

    rows = sorted([r.row for r in records], key=lambda r: r["id"])
    assert rows[0] == {"id": 1, "matrix": [[1, 2], [3, 4]]}
    assert rows[1] == {"id": 2, "matrix": [[], [5], [6, 7, 8]]}
    assert rows[2] == {"id": 3, "matrix": None}
    assert rows[3] == {"id": 4, "matrix": [[1, None], None, []]}
    assert rows[4] == {"id": 5, "matrix": [[None, None]]}

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_append_rows_with_invalid_array(connection, admin):
    """Test that appending invalid data to an array column raises an error."""
    table_path = fluss.TablePath("fluss", "py_test_append_rows_with_invalid_array")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    pa_schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("tags", pa.list_(pa.string())),
    ])
    schema = fluss.Schema(pa_schema)
    await admin.create_table(table_path, fluss.TableDescriptor(schema), ignore_if_exists=False)

    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()

    # Appending a string instead of a list should raise an error
    with pytest.raises(Exception, match="Expected sequence for Array column"):
        append_writer.append({"id": 4, "tags": "not_a_list"})
    
    await admin.drop_table(table_path, ignore_if_not_exists=False)
