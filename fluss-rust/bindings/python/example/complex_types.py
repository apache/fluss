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

"""Complex types example: ARRAY, MAP, and ROW (including nesting).

Shows how to define ARRAY / MAP / ROW columns, the write shapes each accepts,
and how they read back through both the record (dict) scan path and the Arrow
scan path on a log table, plus upsert + lookup on a primary-key table.

Write/read shapes:
    ARRAY<T>   write list/tuple                 -> read list
    MAP<K, V>  write dict or [(k, v), ...]       -> read list of (k, v) tuples
    ROW<...>   write dict (by name) or list/tuple -> read dict

Complex types may be nested arbitrarily, but cannot be primary-key or
bucket-key columns.

Run standalone against a local cluster:

    python example/complex_types.py

Or point it at a specific cluster:

    FLUSS_BOOTSTRAP_SERVERS=host:port python example/complex_types.py
"""

import asyncio
import os
import time
from typing import Optional

import pyarrow as pa

import fluss

DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9123"


def _complex_value_fields():
    """The complex (ARRAY / MAP / ROW) value columns, shared by both tables."""
    return [
        pa.field("tags", pa.list_(pa.string())),  # array<string>
        pa.field("scores", pa.list_(pa.int32())),  # array<int> (with nulls)
        pa.field("attrs", pa.map_(pa.string(), pa.int32())),  # map<string, int>
        pa.field(
            "profile",
            pa.struct([("age", pa.int32()), ("city", pa.string())]),  # row<...>
        ),
        pa.field("matrix", pa.list_(pa.list_(pa.int32()))),  # array<array<int>>
        pa.field(
            "arr_of_map", pa.list_(pa.map_(pa.string(), pa.int32()))
        ),  # array<map<string, int>>
    ]


# Row 1 uses the "canonical" write shapes (map as dict, row as dict).
ROW1 = {
    "id": 1,
    "tags": ["a", "b"],
    "scores": [10, 20, 30],
    "attrs": {"x": 1, "y": 2},
    "profile": {"age": 30, "city": "NYC"},
    "matrix": [[1, 2], [3, 4]],
    "arr_of_map": [{"k": 1}],
}

# Row 2 uses the alternative write shapes accepted by the client:
# map as a list of (key, value) pairs, row as a positional list, and a null
# array element.
ROW2 = {
    "id": 2,
    "tags": ["c"],
    "scores": [5, None],
    "attrs": [("p", 7), ("q", 8)],
    "profile": [40, "LA"],
    "matrix": [[9]],
    "arr_of_map": [{"m": 2}, {"n": 3}],
}

# Row 3 omits every nullable complex column from the dict, so each defaults to
# null on write.
ROW3 = {"id": 3}

COMPLEX_COLUMNS = [f.name for f in _complex_value_fields()]


def _assert_rows(rows):
    """Safety net: confirm the rows read back match what we wrote.

    Examples exist to teach the API, so this stays out of the way -- a single
    call per read path. If it ever fails, CI fails too (the example is run as a
    test), which is exactly when we want to know the docs have drifted.
    MAP reads back as a list of (key, value) tuples, so we wrap with dict().
    """
    assert len(rows) == 3, f"expected 3 rows, got {len(rows)}"
    r1, r2, r3 = rows

    assert r1["tags"] == ["a", "b"]
    assert r1["scores"] == [10, 20, 30]
    assert dict(r1["attrs"]) == {"x": 1, "y": 2}
    assert r1["profile"] == {"age": 30, "city": "NYC"}
    assert r1["matrix"] == [[1, 2], [3, 4]]
    assert [dict(m) for m in r1["arr_of_map"]] == [{"k": 1}]

    assert r2["tags"] == ["c"]
    assert r2["scores"] == [5, None]
    assert dict(r2["attrs"]) == {"p": 7, "q": 8}
    assert r2["profile"] == {"age": 40, "city": "LA"}
    assert r2["matrix"] == [[9]]
    assert [dict(m) for m in r2["arr_of_map"]] == [{"m": 2}, {"n": 3}]

    assert all(r3[col] is None for col in COMPLEX_COLUMNS)


async def main(bootstrap_servers: Optional[str] = None):
    bootstrap_servers = bootstrap_servers or os.environ.get(
        "FLUSS_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
    )

    config = fluss.Config({"bootstrap.servers": bootstrap_servers})
    conn = await fluss.FlussConnection.create(config)
    try:
        await _run_log_table(conn)
        await _run_pk_table(conn)
    finally:
        await conn.close()
        print("\nConnection closed")


async def _run_log_table(conn):
    print("\n=== Log table: append + scan complex types ===")
    admin = conn.get_admin()
    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32())] + _complex_value_fields())
    )
    table_path = fluss.TablePath("fluss", "example_complex_log")

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    # bucket_count=1 keeps record-scan ordering deterministic for the example.
    await admin.create_table(
        table_path, fluss.TableDescriptor(schema, bucket_count=1), ignore_if_exists=True
    )
    print(f"Created log table: {table_path}")

    table = await conn.get_table(table_path)
    writer = table.new_append().create_writer()
    writer.append(ROW1)
    writer.append(ROW2)
    writer.append(ROW3)
    await writer.flush()
    print("Appended 3 rows (canonical shapes, alternative shapes, all-null)")

    print("\n--- Record (dict) scan path ---")
    scanner = await table.new_scan().create_log_scanner()
    scanner.subscribe_buckets({0: fluss.EARLIEST_OFFSET})
    records = await _poll_until(scanner, expected=3)
    rows = sorted((r.row for r in records), key=lambda r: r["id"])
    _assert_rows(rows)
    print("Verified ARRAY/MAP/ROW values via record.row dicts")

    print("\n--- Arrow scan path (must agree with the dict path) ---")
    scanner2 = await table.new_scan().create_record_batch_log_scanner()
    scanner2.subscribe_buckets({0: fluss.EARLIEST_OFFSET})
    arrow = await scanner2.to_arrow()
    _assert_rows(arrow.sort_by("id").to_pylist())
    print("Verified the Arrow path returns identical nested values")

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    print(f"Dropped log table: {table_path}")


async def _run_pk_table(conn):
    print("\n=== Primary-key table: upsert + lookup complex types ===")
    admin = conn.get_admin()
    # Complex types are value columns only; the primary/bucket key must be a
    # simple type (the server rejects complex key columns).
    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32())] + _complex_value_fields()),
        primary_keys=["id"],
    )
    table_path = fluss.TablePath("fluss", "example_complex_kv")

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.create_table(
        table_path, fluss.TableDescriptor(schema, bucket_count=1), ignore_if_exists=True
    )
    print(f"Created PK table: {table_path}")

    table = await conn.get_table(table_path)
    writer = table.new_upsert().create_writer()
    writer.upsert(ROW1)
    writer.upsert(ROW2)
    handle = writer.upsert(ROW3)
    await handle.wait()
    print("Upserted 3 rows")

    lookuper = table.new_lookup().create_lookuper()
    rows = []
    for i in (1, 2, 3):
        row = await lookuper.lookup({"id": i})
        assert row is not None, f"expected to find id={i}"
        rows.append(row)
    _assert_rows(rows)
    print("Verified ARRAY/MAP/ROW values via lookup")

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    print(f"Dropped PK table: {table_path}")


async def _poll_until(scanner, expected, timeout_ms=15000):
    """Poll the record scanner until ``expected`` records arrive or we time out.

    A single poll is not guaranteed to drain everything, so accumulate across
    polls rather than asserting on one call.
    """
    deadline = time.monotonic() + timeout_ms / 1000
    collected = []
    while len(collected) < expected and time.monotonic() < deadline:
        collected.extend(list(await scanner.poll(2000)))
    return collected


if __name__ == "__main__":
    asyncio.run(main())
