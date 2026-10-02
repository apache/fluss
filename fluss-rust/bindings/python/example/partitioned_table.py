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

"""Partitioned log table example.

Covers creating and listing partitions, writing across partitions, querying
per-partition offsets, and the partition-aware scanner subscription APIs
(subscribe_partition, subscribe_partition_buckets, unsubscribe_partition).

Run standalone against a local cluster:

    python example/partitioned_table.py

Or point it at a specific cluster:

    FLUSS_BOOTSTRAP_SERVERS=host:port python example/partitioned_table.py
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
        pa.field("id", pa.int32()),
        pa.field("region", pa.string()),  # partition key
        pa.field("value", pa.int64()),
    ]
    schema = fluss.Schema(pa.schema(fields))
    table_descriptor = fluss.TableDescriptor(
        schema, partition_keys=["region"], bucket_count=1
    )

    admin = conn.get_admin()
    table_path = fluss.TablePath("fluss", "example_partitioned_log")

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=True)
    print(f"Created partitioned table: {table_path}")

    print("\n--- Creating partitions ---")
    await admin.create_partition(table_path, {"region": "US"}, ignore_if_exists=True)
    await admin.create_partition(table_path, {"region": "EU"}, ignore_if_exists=True)
    print("Created partitions: region=US, region=EU")

    partition_infos = await admin.list_partition_infos(table_path)
    assert len(partition_infos) == 2, (
        f"Expected 2 partitions, got {len(partition_infos)}"
    )
    print(f"Partitions: {[p.partition_name for p in partition_infos]}")

    print("\n--- Writing across partitions ---")
    table = await conn.get_table(table_path)
    writer = table.new_append().create_writer()
    writer.append({"id": 1, "region": "US", "value": 100})
    writer.append({"id": 2, "region": "US", "value": 200})
    writer.append({"id": 3, "region": "EU", "value": 300})
    writer.append({"id": 4, "region": "EU", "value": 400})
    await writer.flush()
    print("Wrote 4 records (2 to US, 2 to EU)")

    print("\n--- list_partition_infos() with a partition_spec filter ---")
    us_partitions = await admin.list_partition_infos(
        table_path, partition_spec={"region": "US"}
    )
    assert len(us_partitions) == 1, (
        f"Expected 1 partition for region=US, got {len(us_partitions)}"
    )
    print(f"Filtered partitions (region=US): {us_partitions}")

    print("\n--- list_partition_offsets() ---")
    # partition_name is the value (e.g. "US"), not "region=US".
    us_offsets = await admin.list_partition_offsets(
        table_path,
        partition_name="US",
        bucket_ids=[0],
        offset_spec=fluss.OffsetSpec.latest(),
    )
    print(f"US partition latest offsets: {us_offsets}")
    eu_offsets = await admin.list_partition_offsets(
        table_path,
        partition_name="EU",
        bucket_ids=[0],
        offset_spec=fluss.OffsetSpec.latest(),
    )
    print(f"EU partition latest offsets: {eu_offsets}")

    await _scan_partitions(table, partition_infos)

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    print(f"\nDropped partitioned table: {table_path}")


async def _scan_partitions(table, partition_infos):
    print("\n--- subscribe_partition() + to_arrow() ---")
    scanner = await table.new_scan().create_record_batch_log_scanner()
    for p in partition_infos:
        scanner.subscribe_partition(
            partition_id=p.partition_id,
            bucket_id=0,
            start_offset=fluss.EARLIEST_OFFSET,
        )
    arrow = await scanner.to_arrow()
    assert arrow.num_rows == 4, f"Expected 4 records, got {arrow.num_rows}"
    print(f"to_arrow() returned {arrow.num_rows} records across all partitions")

    print("\n--- subscribe_partition_buckets() + to_arrow() ---")
    batch_scanner = await table.new_scan().create_record_batch_log_scanner()
    partition_bucket_offsets = {
        (p.partition_id, 0): fluss.EARLIEST_OFFSET for p in partition_infos
    }
    batch_scanner.subscribe_partition_buckets(partition_bucket_offsets)
    batch_arrow = await batch_scanner.to_arrow()
    assert batch_arrow.num_rows == 4, f"Expected 4 records, got {batch_arrow.num_rows}"
    print(f"to_arrow() returned {batch_arrow.num_rows} records")

    print("\n--- unsubscribe_partition() ---")
    scanner3 = await table.new_scan().create_record_batch_log_scanner()
    for p in partition_infos:
        scanner3.subscribe_partition(p.partition_id, 0, fluss.EARLIEST_OFFSET)
    first = partition_infos[0]
    scanner3.unsubscribe_partition(first.partition_id, 0)
    remaining = await scanner3.to_arrow()
    # Each partition holds 2 records, so dropping one leaves 2.
    assert remaining.num_rows == 2, f"Expected 2 records, got {remaining.num_rows}"
    print(
        f"After unsubscribing partition {first.partition_name}: "
        f"{remaining.num_rows} records from the rest"
    )

    print("\n--- to_pandas() on a partitioned table ---")
    scanner4 = await table.new_scan().create_record_batch_log_scanner()
    for p in partition_infos:
        scanner4.subscribe_partition(p.partition_id, 0, fluss.EARLIEST_OFFSET)
    df = await scanner4.to_pandas()
    assert len(df) == 4, f"Expected 4 records, got {len(df)}"
    print(f"to_pandas() returned {len(df)} records")


if __name__ == "__main__":
    asyncio.run(main())
