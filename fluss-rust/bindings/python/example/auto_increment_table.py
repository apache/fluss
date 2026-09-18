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

"""Auto-increment column example.

The server assigns the auto-increment column, so writes must be partial updates
that omit it. A full-row write is rejected.

Run standalone against a local cluster:

    python example/auto_increment_table.py

Or point it at a specific cluster:

    FLUSS_BOOTSTRAP_SERVERS=host:port python example/auto_increment_table.py
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
                pa.field("uid", pa.string()),
                pa.field("region", pa.string()),
                pa.field("uid_int", pa.int64()),
            ]
        ),
        primary_keys=["uid"],
        auto_increment_column="uid_int",
    )
    assert schema.get_auto_increment_columns() == ["uid_int"]

    admin = conn.get_admin()
    table_path = fluss.TablePath("fluss", "example_auto_increment_table")

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.create_table(
        table_path, fluss.TableDescriptor(schema, bucket_count=1), ignore_if_exists=True
    )
    print(f"Created table with auto-increment column: {table_path}")

    table_info = await admin.get_table_info(table_path)
    print(
        "Auto-increment columns: "
        f"{table_info.get_schema().get_auto_increment_columns()}"
    )

    table = await conn.get_table(table_path)
    await _full_row_write_is_rejected(table)
    await _partial_update_assigns_the_value(table)

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    print(f"\nDropped table: {table_path}")


async def _full_row_write_is_rejected(table):
    print("\n--- Full-row write ---")
    try:
        table.new_upsert().create_writer()
    except fluss.FlussError as error:
        print(f"Rejected: {error}")
    else:
        raise AssertionError("expected a full-row writer to be rejected")


async def _partial_update_assigns_the_value(table):
    print("\n--- Partial update, omitting the auto-increment column ---")
    writer = (
        table.new_upsert().partial_update_by_name(["uid", "region"]).create_writer()
    )
    for uid, region in [("alice", "eu"), ("bob", "us"), ("carol", "apac")]:
        await writer.upsert({"uid": uid, "region": region}).wait()
        print(f"Upserted uid={uid}")

    lookuper = table.new_lookup().create_lookuper()
    assigned = []
    for uid in ["alice", "bob", "carol"]:
        row = await lookuper.lookup({"uid": uid})
        assert row is not None, f"expected to find uid={uid}"
        assigned.append(row["uid_int"])
        print(f"uid={uid} region={row['region']} uid_int={row['uid_int']}")

    assert all(value is not None for value in assigned), "server did not assign values"
    assert len(set(assigned)) == len(assigned), f"values not distinct: {assigned}"


if __name__ == "__main__":
    asyncio.run(main())
