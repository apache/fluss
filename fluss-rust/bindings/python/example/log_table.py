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

"""Log table example: append-only writes and log scanning.

Covers the append writer (Arrow Table/RecordBatch, dict, list, pandas inputs),
flushing, offset queries, batch and record scanners, column projection, and the
async context-manager API.

Run standalone against a local cluster:

    python example/log_table.py

Or point it at a specific cluster:

    FLUSS_BOOTSTRAP_SERVERS=host:port python example/log_table.py
"""

import asyncio
import os
from datetime import date, datetime
from datetime import time as dt_time
from decimal import Decimal
from typing import Optional

import pandas as pd
import pyarrow as pa

import fluss

DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9123"

# Total rows written before the scans run (3 + 2 + 1 + 1 + 2). The
# context-manager demo writes one more row, but only after the scans.
EXPECTED_ROWS = 9


async def main(bootstrap_servers: Optional[str] = None):
    bootstrap_servers = bootstrap_servers or os.environ.get(
        "FLUSS_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
    )

    config = fluss.Config(
        {
            "bootstrap.servers": bootstrap_servers,
            "writer.request-max-size": "10485760",  # 10 MB
            "writer.acks": "all",  # Wait for all replicas to acknowledge
            "writer.retries": "3",  # Retry up to 3 times on failure
            "writer.batch-size": "2097152",  # 2 MB batch size (in bytes)
        }
    )
    conn = await fluss.FlussConnection.create(config)
    try:
        await _run(conn)
    finally:
        await conn.close()
        print("\nConnection closed")


async def _run(conn):
    fields = [
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float32()),
        pa.field("age", pa.int32()),
        pa.field("birth_date", pa.date32()),
        pa.field("check_in_time", pa.time32("ms")),
        pa.field("created_at", pa.timestamp("us")),  # TIMESTAMP (NTZ)
        pa.field("updated_at", pa.timestamp("us", tz="UTC")),  # TIMESTAMP_LTZ
        pa.field("salary", pa.decimal128(10, 2)),
    ]
    schema = pa.schema(fields)
    table_descriptor = fluss.TableDescriptor(fluss.Schema(schema))

    admin = conn.get_admin()
    table_path = fluss.TablePath("fluss", "example_log_table")

    # Drop-then-create keeps the example rerunnable on a shared cluster.
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=True)
    print(f"Created table: {table_path}")

    # A fresh table briefly reports "not leader" until bucket leaders are elected.
    await _await_bucket_leader(admin, table_path)

    table_info = await admin.get_table_info(table_path)
    print(f"Table info: {table_info}")
    print(f"Table ID: {table_info.table_id}")
    print(f"Primary keys: {table_info.get_primary_keys()}")
    num_buckets = table_info.num_buckets

    print("\n--- list_offsets() before writes ---")
    offsets = await admin.list_offsets(
        table_path, bucket_ids=[0], offset_spec=fluss.OffsetSpec.latest()
    )
    print(f"Latest offsets (before writes): {offsets}")

    table = await conn.get_table(table_path)
    append_writer = table.new_append().create_writer()

    print("\n--- Writing a PyArrow Table ---")
    pa_table = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            pa.array([95.2, 87.2, 92.1], type=pa.float32()),
            pa.array([25, 30, 35], type=pa.int32()),
            pa.array(
                [date(1999, 5, 15), date(1994, 3, 20), date(1989, 11, 8)],
                type=pa.date32(),
            ),
            pa.array(
                [dt_time(9, 0, 0), dt_time(9, 30, 0), dt_time(10, 0, 0)],
                type=pa.time32("ms"),
            ),
            pa.array(
                [
                    datetime(2024, 1, 15, 10, 30),
                    datetime(2024, 1, 15, 11, 0),
                    datetime(2024, 1, 15, 11, 30),
                ],
                type=pa.timestamp("us"),
            ),
            pa.array(
                [
                    datetime(2024, 1, 15, 10, 30),
                    datetime(2024, 1, 15, 11, 0),
                    datetime(2024, 1, 15, 11, 30),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            pa.array(
                [Decimal("75000.00"), Decimal("82000.50"), Decimal("95000.75")],
                type=pa.decimal128(10, 2),
            ),
        ],
        schema=schema,
    )
    append_writer.write_arrow(pa_table)
    print("Wrote PyArrow Table (3 rows)")

    print("\n--- Writing a PyArrow RecordBatch ---")
    pa_record_batch = pa.RecordBatch.from_arrays(
        [
            pa.array([4, 5], type=pa.int32()),
            pa.array(["David", "Eve"], type=pa.string()),
            pa.array([88.5, 91.0], type=pa.float32()),
            pa.array([28, 32], type=pa.int32()),
            pa.array([date(1996, 7, 22), date(1992, 12, 1)], type=pa.date32()),
            pa.array([dt_time(14, 15, 0), dt_time(8, 45, 0)], type=pa.time32("ms")),
            pa.array(
                [datetime(2024, 1, 16, 9, 0), datetime(2024, 1, 16, 9, 30)],
                type=pa.timestamp("us"),
            ),
            pa.array(
                [datetime(2024, 1, 16, 9, 0), datetime(2024, 1, 16, 9, 30)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            pa.array(
                [Decimal("68000.00"), Decimal("72500.25")],
                type=pa.decimal128(10, 2),
            ),
        ],
        schema=schema,
    )
    append_writer.write_arrow_batch(pa_record_batch)
    print("Wrote PyArrow RecordBatch (2 rows)")

    print("\n--- Appending single rows (dict and list) ---")
    append_writer.append(
        {
            "id": 8,
            "name": "Helen",
            "score": 93.5,
            "age": 26,
            "birth_date": date(1998, 4, 10),
            "check_in_time": dt_time(11, 30, 45),
            "created_at": datetime(2024, 1, 17, 14, 0, 0),
            "updated_at": datetime(2024, 1, 17, 14, 0, 0),
            "salary": Decimal("88000.00"),
        }
    )
    print("Appended row (dict input)")
    append_writer.append(
        [
            9,
            "Ivan",
            90.0,
            31,
            date(1993, 8, 25),
            dt_time(16, 45, 0),
            datetime(2024, 1, 17, 15, 30, 0),
            datetime(2024, 1, 17, 15, 30, 0),
            Decimal("91500.50"),
        ]
    )
    print("Appended row (list input)")

    print("\n--- Writing a Pandas DataFrame ---")
    df = pd.DataFrame(
        {
            "id": [10, 11],
            "name": ["Frank", "Grace"],
            "score": [89.3, 94.7],
            "age": [29, 27],
            "birth_date": [date(1995, 2, 14), date(1997, 9, 30)],
            "check_in_time": [dt_time(10, 0, 0), dt_time(10, 30, 0)],
            "created_at": [
                datetime(2024, 1, 18, 8, 0),
                datetime(2024, 1, 18, 8, 30),
            ],
            "updated_at": [
                datetime(2024, 1, 18, 8, 0),
                datetime(2024, 1, 18, 8, 30),
            ],
            "salary": [Decimal("79000.00"), Decimal("85500.75")],
        }
    )
    append_writer.write_pandas(df)
    print("Wrote Pandas DataFrame (2 rows)")

    print("\n--- Flushing ---")
    await append_writer.flush()
    print("Flushed all pending data")

    offsets = await admin.list_offsets(
        table_path, bucket_ids=[0], offset_spec=fluss.OffsetSpec.latest()
    )
    print(f"Latest offsets (after writes): {offsets}")

    await _scan_batch(table, num_buckets)
    await _scan_records(table, num_buckets)
    await _projection(table, num_buckets)
    await _limit_scan(table, num_buckets)
    await _context_manager_demo(conn, table_path)

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    print(f"\nDropped table: {table_path}")


async def _await_bucket_leader(admin, table_path, *, attempts=60, delay_s=0.5):
    """Poll until the bucket leader is elected, so bucket-level requests on a
    just-created table don't fail with "not leader or follower"."""
    for _ in range(attempts):
        try:
            await admin.list_offsets(
                table_path, bucket_ids=[0], offset_spec=fluss.OffsetSpec.earliest()
            )
            return
        except fluss.FlussError:
            await asyncio.sleep(delay_s)
    # Final attempt (outside the guard) surfaces the real error, not a timeout.
    await admin.list_offsets(
        table_path, bucket_ids=[0], offset_spec=fluss.OffsetSpec.earliest()
    )


async def _scan_batch(table, num_buckets):
    print("\n--- Batch scanner: to_arrow() / to_pandas() ---")
    scanner = await table.new_scan().create_record_batch_log_scanner()
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
    pa_table_result = await scanner.to_arrow()
    assert pa_table_result.num_rows == EXPECTED_ROWS, (
        f"to_arrow() returned {pa_table_result.num_rows}, expected {EXPECTED_ROWS}"
    )
    print(f"to_arrow() returned {pa_table_result.num_rows} rows")

    scanner2 = await table.new_scan().create_record_batch_log_scanner()
    scanner2.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
    df_result = await scanner2.to_pandas()
    assert len(df_result) == EXPECTED_ROWS, (
        f"to_pandas() returned {len(df_result)}, expected {EXPECTED_ROWS}"
    )
    print(f"to_pandas() returned {len(df_result)} rows")

    print("\n--- Batch scanner: to_arrow_batch_reader() (lazy) ---")
    reader_scanner = await table.new_scan().create_record_batch_log_scanner()
    reader_scanner.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )
    arrow_reader = reader_scanner.to_arrow_batch_reader()
    reader_table = pa.Table.from_batches(list(arrow_reader), schema=arrow_reader.schema)
    assert reader_table.num_rows == EXPECTED_ROWS, (
        f"to_arrow_batch_reader() yielded {reader_table.num_rows}, "
        f"expected {EXPECTED_ROWS}"
    )
    print(f"to_arrow_batch_reader() yielded {reader_table.num_rows} rows")

    print("\n--- Batch scanner: poll_arrow() ---")
    poll_scanner = await table.new_scan().create_record_batch_log_scanner()
    poll_scanner.subscribe(bucket_id=0, start_offset=fluss.EARLIEST_OFFSET)
    # poll_arrow() returns an empty (but schema-bearing) table on timeout.
    poll_result = await poll_scanner.poll_arrow(5000)
    print(f"poll_arrow() returned {poll_result.num_rows} rows")

    print("\n--- Batch scanner: poll_record_batch() ---")
    batch_scanner = await table.new_scan().create_record_batch_log_scanner()
    batch_scanner.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )
    batches = await batch_scanner.poll_record_batch(5000)
    print(f"poll_record_batch() returned {len(batches)} batches")
    for i, batch in enumerate(batches):
        print(
            f"  Batch {i}: bucket={batch.bucket}, "
            f"offsets={batch.base_offset}-{batch.last_offset}, "
            f"rows={batch.batch.num_rows}"
        )


async def _scan_records(table, num_buckets):
    print("\n--- Record scanner: poll() ---")
    record_scanner = await table.new_scan().create_log_scanner()
    record_scanner.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )
    scan_records = await record_scanner.poll(5000)
    print(
        f"poll() returned {scan_records.count()} records "
        f"across {len(scan_records.buckets())} bucket(s)"
    )
    for bucket in scan_records.buckets():
        bucket_recs = scan_records.records(bucket)
        print(f"  Bucket {bucket}: {len(bucket_recs)} records")
        for record in bucket_recs[:3]:
            print(
                f"    offset={record.offset}, timestamp={record.timestamp}, "
                f"change_type={record.change_type}"
            )

    print("\n--- unsubscribe() ---")
    unsub_scanner = await table.new_scan().create_record_batch_log_scanner()
    unsub_scanner.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )
    unsub_scanner.unsubscribe(bucket_id=0)
    remaining = await unsub_scanner.poll_arrow(5000)
    print(f"After unsubscribing bucket 0: {remaining.num_rows} rows from the rest")


async def _projection(table, num_buckets):
    print("\n--- Projection by index [0, 1] (id, name) ---")
    scanner_index = (
        await table.new_scan().project([0, 1]).create_record_batch_log_scanner()
    )
    scanner_index.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )
    df_projected = await scanner_index.to_pandas()
    assert list(df_projected.columns) == ["id", "name"], (
        f"Unexpected projected columns: {list(df_projected.columns)}"
    )
    assert len(df_projected) == EXPECTED_ROWS
    print(f"Projected columns: {list(df_projected.columns)}")

    print("\n--- Projection by name ['name', 'score'] ---")
    scanner_names = (
        await table.new_scan()
        .project_by_name(["name", "score"])
        .create_record_batch_log_scanner()
    )
    scanner_names.subscribe_buckets(
        {i: fluss.EARLIEST_OFFSET for i in range(num_buckets)}
    )
    df_named = await scanner_names.to_pandas()
    assert list(df_named.columns) == ["name", "score"], (
        f"Unexpected projected columns: {list(df_named.columns)}"
    )
    assert len(df_named) == EXPECTED_ROWS
    print(f"Projected columns: {list(df_named.columns)}")


async def _limit_scan(table, num_buckets):
    print("\n--- Limit scan: one-shot bounded BatchScanner (per bucket) ---")
    table_id = table.get_table_info().table_id
    total = 0
    for bucket_id in range(num_buckets):
        bucket = fluss.TableBucket(table_id, bucket_id)
        scanner = (
            table.new_scan().limit(EXPECTED_ROWS).create_bucket_batch_scanner(bucket)
        )
        batch = await scanner.next_batch()
        if batch is not None:
            assert batch.bucket == bucket
            total += batch.batch.num_rows
        # One-shot: the scanner is spent after the first batch.
        assert await scanner.next_batch() is None
    assert total == EXPECTED_ROWS, (
        f"Limit scan across buckets returned {total} rows, expected {EXPECTED_ROWS}"
    )
    print(f"Limit scan across {num_buckets} bucket(s) returned {total} rows")


async def _context_manager_demo(conn, table_path):
    print("\n--- Async context manager (auto-flush on exit) ---")
    table = await conn.get_table(table_path)
    async with table.new_append().create_writer() as writer:
        writer.append(
            {
                "id": 100,
                "name": "demo",
                "score": 1.0,
                "age": 25,
                "birth_date": date(2000, 1, 1),
                "check_in_time": dt_time(12, 0, 0),
                "created_at": datetime(2024, 1, 1, 12, 0, 0),
                "updated_at": datetime(2024, 1, 1, 12, 0, 0),
                "salary": Decimal("100.00"),
            }
        )
        # auto-flushes on exit
    print("Wrote one row via context manager")


if __name__ == "__main__":
    asyncio.run(main())
