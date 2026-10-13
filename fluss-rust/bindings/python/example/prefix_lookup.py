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

"""Prefix lookup example: fetch all orders for a customer without scanning.

Run standalone against a local cluster:

    python example/prefix_lookup.py

Or point it at a specific cluster:

    FLUSS_BOOTSTRAP_SERVERS=host:port python example/prefix_lookup.py
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
        pa.schema(
            [
                pa.field("customer_id", pa.int32()),
                pa.field("order_id", pa.int64()),
                pa.field("item", pa.string()),
            ]
        ),
        primary_keys=["customer_id", "order_id"],
    )
    # The bucket key must be a strict prefix of the primary key.
    descriptor = fluss.TableDescriptor(
        schema, bucket_count=3, bucket_keys=["customer_id"]
    )
    admin = conn.get_admin()
    table_path = fluss.TablePath("fluss", "example_prefix_lookup")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.create_table(table_path, descriptor, ignore_if_exists=True)
    print(f"Created orders table: {table_path}")

    try:
        table = await conn.get_table(table_path)
        orders = [
            {"customer_id": 1, "order_id": 102, "item": "Notebook"},
            {"customer_id": 1, "order_id": 101, "item": "Keyboard"},
            {"customer_id": 2, "order_id": 101, "item": "Mouse"},
        ]
        writer = table.new_upsert().create_writer()
        for order in orders:
            writer.upsert(order)
        # Wait for acknowledgment before reading the orders back.
        await writer.flush()

        lookuper = table.new_lookup().lookup_by(["customer_id"]).create_lookuper()
        result = await lookuper.lookup({"customer_id": 1})
        expected = sorted(orders[:2], key=lambda row: row["order_id"])
        assert sorted(result, key=lambda row: row["order_id"]) == expected
        print(f"Orders for customer_id=1: {result}")

        # List and tuple keys follow the column order supplied to lookup_by().
        result = await lookuper.lookup([2])
        assert result == [orders[2]]
        print(f"Orders for customer_id=2: {result}")

        result = await lookuper.lookup((999,))
        assert result == [], "Expected no orders for customer_id=999"
        print("Orders for customer_id=999: [] (no matches)")
    finally:
        await admin.drop_table(table_path, ignore_if_not_exists=True)
        print(f"\nDropped orders table: {table_path}")


if __name__ == "__main__":
    asyncio.run(main())
