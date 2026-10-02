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

"""Primary-key table example: upsert, lookup, delete, partial update.

Covers fire-and-forget and per-record-acknowledged upserts, point lookups,
deletes, and partial updates by column name and by column index.

Run standalone against a local cluster:

    python example/pk_table.py

Or point it at a specific cluster:

    FLUSS_BOOTSTRAP_SERVERS=host:port python example/pk_table.py
"""

import asyncio
import os
from datetime import date, datetime
from datetime import time as dt_time
from decimal import Decimal
from typing import Optional

import pyarrow as pa

import fluss

DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9123"


async def main(bootstrap_servers: Optional[str] = None):
    bootstrap_servers = bootstrap_servers or os.environ.get(
        "FLUSS_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
    )

    config = fluss.Config({"bootstrap.servers": bootstrap_servers})
    conn = await fluss.FlussConnection.create(config)
    try:
        await _run(conn)
    finally:
        await conn.close()
        print("\nConnection closed")


async def _run(conn):
    fields = [
        pa.field("user_id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("email", pa.string()),
        pa.field("age", pa.int32()),
        pa.field("birth_date", pa.date32()),
        pa.field("login_time", pa.time32("ms")),
        pa.field("created_at", pa.timestamp("us")),  # TIMESTAMP (NTZ)
        pa.field("updated_at", pa.timestamp("us", tz="UTC")),  # TIMESTAMP_LTZ
        pa.field("balance", pa.decimal128(10, 2)),
    ]
    schema = fluss.Schema(pa.schema(fields), primary_keys=["user_id"])
    table_descriptor = fluss.TableDescriptor(schema, bucket_count=3)

    admin = conn.get_admin()
    table_path = fluss.TablePath("fluss", "example_pk_table")

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=True)
    print(f"Created PK table: {table_path}")

    table = await conn.get_table(table_path)
    print(f"Has primary key: {table.has_primary_key()}")

    await _upsert(table)
    await _lookup(table)
    await _delete(table)
    await _partial_update(table)
    await _limit_scan(table)

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    print(f"\nDropped PK table: {table_path}")


async def _upsert(table):
    print("\n--- Upsert (fire-and-forget) ---")
    upsert_writer = table.new_upsert().create_writer()

    rows = [
        {
            "user_id": 1,
            "name": "Alice",
            "email": "alice@example.com",
            "age": 25,
            "birth_date": date(1999, 5, 15),
            "login_time": dt_time(9, 30, 45, 123000),
            "created_at": datetime(2024, 1, 15, 10, 30, 45, 123456),
            "updated_at": datetime(2024, 1, 15, 10, 30, 45, 123456),
            "balance": Decimal("1234.56"),
        },
        {
            "user_id": 2,
            "name": "Bob",
            "email": "bob@example.com",
            "age": 30,
            "birth_date": date(1994, 3, 20),
            "login_time": dt_time(14, 15, 30, 500000),
            "created_at": datetime(2024, 1, 16, 11, 22, 33, 444555),
            "updated_at": datetime(2024, 1, 16, 11, 22, 33, 444555),
            "balance": Decimal("5678.91"),
        },
        {
            "user_id": 3,
            "name": "Charlie",
            "email": "charlie@example.com",
            "age": 35,
            "birth_date": date(1989, 11, 8),
            "login_time": dt_time(16, 45, 59, 999000),
            "created_at": datetime(2024, 1, 17, 23, 59, 59, 999999),
            "updated_at": datetime(2024, 1, 17, 23, 59, 59, 999999),
            "balance": Decimal("9876.54"),
        },
    ]
    for row in rows:
        upsert_writer.upsert(row)
    await upsert_writer.flush()
    print(f"Upserted {len(rows)} rows (flush waits for server acknowledgment)")

    print("\n--- Upsert (per-record acknowledgment) ---")
    handle = upsert_writer.upsert(
        {
            "user_id": 1,
            "name": "Alice Updated",
            "email": "alice.new@example.com",
            "age": 26,
            "birth_date": date(1999, 5, 15),
            "login_time": dt_time(10, 11, 12, 345000),
            "created_at": datetime(2024, 1, 15, 10, 30, 45, 123456),
            "updated_at": datetime(2024, 1, 20, 15, 45, 30, 678901),
            "balance": Decimal("2345.67"),
        }
    )
    await handle.wait()
    print("Updated user_id=1 (Alice -> Alice Updated), server acknowledged")


async def _lookup(table):
    print("\n--- Lookup ---")
    lookuper = table.new_lookup().create_lookuper()

    result = await lookuper.lookup({"user_id": 1})
    assert result is not None, "Expected to find user_id=1"
    assert result["name"] == "Alice Updated", f"Unexpected name: {result['name']}"
    print(f"Lookup user_id=1: name={result['name']}, balance={result['balance']}")

    result = await lookuper.lookup({"user_id": 2})
    assert result is not None, "Expected to find user_id=2"
    print(f"Lookup user_id=2: name={result['name']}")

    # A missing key returns None (normal control flow, not an error).
    result = await lookuper.lookup({"user_id": 999})
    assert result is None, "Expected user_id=999 to be absent"
    print("Lookup user_id=999: not found (expected)")


async def _delete(table):
    print("\n--- Delete ---")
    upsert_writer = table.new_upsert().create_writer()
    handle = upsert_writer.delete({"user_id": 3})
    await handle.wait()
    print("Deleted user_id=3, server acknowledged")

    lookuper = table.new_lookup().create_lookuper()
    result = await lookuper.lookup({"user_id": 3})
    assert result is None, "Expected user_id=3 to be deleted"
    print("Lookup user_id=3 after delete: not found (deletion confirmed)")


async def _partial_update(table):
    print("\n--- Partial update by column names ---")
    by_name = (
        table.new_upsert()
        .partial_update_by_name(["user_id", "balance"])
        .create_writer()
    )
    handle = by_name.upsert({"user_id": 1, "balance": Decimal("9999.99")})
    await handle.wait()

    lookuper = table.new_lookup().create_lookuper()
    result = await lookuper.lookup({"user_id": 1})
    assert result is not None, "Expected to find user_id=1"
    assert result["balance"] == Decimal("9999.99")
    assert result["name"] == "Alice Updated", "name should be unchanged"
    print(
        f"By name: balance={result['balance']} (updated), "
        f"name={result['name']} (unchanged)"
    )

    print("\n--- Partial update by column indices ---")
    # Columns: 0=user_id (PK), 1=name. Update name only.
    by_index = table.new_upsert().partial_update_by_index([0, 1]).create_writer()
    handle = by_index.upsert([1, "Alice Renamed"])
    await handle.wait()

    result = await lookuper.lookup({"user_id": 1})
    assert result is not None, "Expected to find user_id=1"
    assert result["name"] == "Alice Renamed", "name should be updated"
    assert result["balance"] == Decimal("9999.99"), "balance should be unchanged"
    print(
        f"By index: name={result['name']} (updated), "
        f"balance={result['balance']} (unchanged)"
    )


async def _limit_scan(table):
    print("\n--- Limit scan: bounded BatchScanner over current rows (per bucket) ---")
    table_info = table.get_table_info()
    total = 0
    for bucket_id in range(table_info.num_buckets):
        bucket = fluss.TableBucket(table_info.table_id, bucket_id)
        scanner = table.new_scan().limit(100).create_bucket_batch_scanner(bucket)
        arrow_table = await scanner.to_arrow()
        total += arrow_table.num_rows
    # Users 1 and 2 remain (user 3 was deleted; user 1 was updated in place).
    assert total == 2, f"Limit scan returned {total} current rows, expected 2"
    print(f"Limit scan across {table_info.num_buckets} bucket(s) returned {total} rows")


if __name__ == "__main__":
    asyncio.run(main())
