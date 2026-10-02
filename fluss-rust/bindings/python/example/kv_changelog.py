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

"""Primary-key table changelog (CDC) example.

Subscribe to a primary-key table's changelog and print each row-level change as
a ``+I`` / ``-U`` / ``+U`` / ``-D`` event.

Run standalone against a local cluster:

    python example/kv_changelog.py

Or point it at a specific cluster:

    FLUSS_BOOTSTRAP_SERVERS=host:port python example/kv_changelog.py
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
    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())]),
        primary_keys=["id"],
    )
    # A single bucket keeps the changelog on one bucket and in order, which makes
    # the CDC output easy to follow.
    table_descriptor = fluss.TableDescriptor(schema, bucket_count=1)

    admin = conn.get_admin()
    table_path = fluss.TablePath("fluss", "example_kv_changelog")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=True)
    print(f"Created PK table: {table_path}")

    table = await conn.get_table(table_path)
    writer = table.new_upsert().create_writer()

    # Insert three keys (+I), update one (-U / +U) and delete one (-D).
    for id_, name in [(1, "alice"), (2, "bob"), (3, "carol")]:
        writer.upsert({"id": id_, "name": name})
    writer.upsert({"id": 2, "name": "bob-v2"})
    writer.delete({"id": 3})
    await writer.flush()

    # Subscribe from the start of the changelog and print each CDC event until we
    # reach the end of the log.
    scanner = await table.new_scan().create_log_scanner()
    scanner.subscribe(bucket_id=0, start_offset=fluss.EARLIEST_OFFSET)

    print("Changelog (change_type id name):")
    while True:
        records = await scanner.poll(3000)
        if records.is_empty():
            break
        for record in records:
            print(
                f"  {record.change_type.short_string()} "
                f"{record.row['id']} {record.row['name']}"
            )

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    print(f"\nDropped table: {table_path}")


if __name__ == "__main__":
    asyncio.run(main())
