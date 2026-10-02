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

"""Partitioned primary-key (KV) table example.

Covers upsert, lookup, update, and delete on a primary-key table whose primary
key includes the partition key, across multiple partitions.

Run standalone against a local cluster:

    python example/partitioned_kv_table.py

Or point it at a specific cluster:

    FLUSS_BOOTSTRAP_SERVERS=host:port python example/partitioned_kv_table.py
"""

import asyncio
import os
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
        pa.field("region", pa.string()),  # partition key + part of PK
        pa.field("user_id", pa.int32()),  # part of PK
        pa.field("name", pa.string()),
        pa.field("score", pa.int64()),
    ]
    schema = fluss.Schema(pa.schema(fields), primary_keys=["region", "user_id"])
    table_descriptor = fluss.TableDescriptor(schema, partition_keys=["region"])

    admin = conn.get_admin()
    table_path = fluss.TablePath("fluss", "example_partitioned_kv")

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=True)
    print(f"Created partitioned KV table: {table_path}")

    for region in ("US", "EU", "APAC"):
        await admin.create_partition(
            table_path, {"region": region}, ignore_if_exists=True
        )
    print("Created partitions: US, EU, APAC")

    table = await conn.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()
    lookuper = table.new_lookup().create_lookuper()

    print("\n--- Upsert across partitions ---")
    test_data = [
        ("US", 1, "Gustave", 100),
        ("US", 2, "Lune", 200),
        ("EU", 1, "Sciel", 150),
        ("EU", 2, "Maelle", 250),
        ("APAC", 1, "Noco", 300),
    ]
    for region, user_id, name, score in test_data:
        upsert_writer.upsert(
            {"region": region, "user_id": user_id, "name": name, "score": score}
        )
    await upsert_writer.flush()
    print(f"Upserted {len(test_data)} rows across 3 partitions")

    print("\n--- Lookup across partitions ---")
    for region, user_id, name, score in test_data:
        result = await lookuper.lookup({"region": region, "user_id": user_id})
        assert result is not None, f"Expected region={region} user_id={user_id}"
        assert result["name"] == name, f"Name mismatch: {result['name']} != {name}"
        assert result["score"] == score, f"Score mismatch: {result['score']} != {score}"
    print(f"All {len(test_data)} rows verified")

    print("\n--- Update within a partition ---")
    handle = upsert_writer.upsert(
        {"region": "US", "user_id": 1, "name": "Gustave Updated", "score": 999}
    )
    await handle.wait()
    result = await lookuper.lookup({"region": "US", "user_id": 1})
    assert result is not None and result["name"] == "Gustave Updated"
    assert result["score"] == 999
    print(f"Updated US/1: name={result['name']}, score={result['score']}")

    print("\n--- Lookup in a non-existent partition ---")
    result = await lookuper.lookup({"region": "UNKNOWN", "user_id": 1})
    assert result is None, "Expected UNKNOWN partition lookup to return None"
    print("UNKNOWN partition lookup: not found (expected)")

    print("\n--- Delete within a partition ---")
    handle = upsert_writer.delete({"region": "EU", "user_id": 1})
    await handle.wait()
    result = await lookuper.lookup({"region": "EU", "user_id": 1})
    assert result is None, "Expected EU/1 to be deleted"
    print("Deleted EU/1, confirmed absent")

    result = await lookuper.lookup({"region": "EU", "user_id": 2})
    assert result is not None and result["name"] == "Maelle"
    print(f"Sibling EU/2 still present: name={result['name']}")

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    print(f"\nDropped partitioned KV table: {table_path}")


if __name__ == "__main__":
    asyncio.run(main())
