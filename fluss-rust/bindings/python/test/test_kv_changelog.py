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

"""Integration tests for primary-key (KV) table changelog (CDC) scanning.

Mirrors crates/fluss/tests/integration/kv_changelog.rs.
"""

import time

import pyarrow as pa
import pytest

import fluss


async def test_subscribe_kv_table_changelog(connection, admin, wait_for_table_ready):
    """A record-mode scanner over a PK table yields its CDC changelog.

    With the default FULL changelog image: inserting a new key emits +I,
    overwriting an existing key emits -U (old image) then +U (new image), and a
    delete emits -D (old image). A single bucket keeps the offsets contiguous.
    """
    table_path = fluss.TablePath("fluss", "py_test_kv_changelog")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())]),
        primary_keys=["id"],
    )
    table_descriptor = fluss.TableDescriptor(schema, bucket_count=1)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)
    await wait_for_table_ready(table_path)

    table = await connection.get_table(table_path)
    writer = table.new_upsert().create_writer()

    # Await each write so the changelog offsets are produced in a fixed order.
    await writer.upsert({"id": 1, "name": "alice"}).wait()  # +I
    await writer.upsert({"id": 2, "name": "bob"}).wait()  # +I
    await writer.upsert({"id": 1, "name": "alice2"}).wait()  # -U / +U
    await writer.delete({"id": 2}).wait()  # -D

    scanner = await table.new_scan().create_log_scanner()
    scanner.subscribe(bucket_id=0, start_offset=fluss.EARLIEST_OFFSET)

    records = await _poll_records(scanner, expected_count=5)
    assert len(records) == 5

    records.sort(key=lambda r: r.offset)
    assert [r.offset for r in records] == [0, 1, 2, 3, 4]
    assert [r.change_type for r in records] == [
        fluss.ChangeType.Insert,
        fluss.ChangeType.Insert,
        fluss.ChangeType.UpdateBefore,
        fluss.ChangeType.UpdateAfter,
        fluss.ChangeType.Delete,
    ]
    assert [(r.row["id"], r.row["name"]) for r in records] == [
        (1, "alice"),  # +I
        (2, "bob"),  # +I
        (1, "alice"),  # -U (old image)
        (1, "alice2"),  # +U (new image)
        (2, "bob"),  # -D (old image)
    ]

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_record_batch_scanner_rejects_primary_key(connection, admin):
    """The Arrow batch scanner carries no per-record change types, so it rejects
    primary-key tables (mirrors the core / Java restriction)."""
    table_path = fluss.TablePath("fluss", "py_test_kv_changelog_batch_reject")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())]),
        primary_keys=["id"],
    )
    await admin.create_table(
        table_path, fluss.TableDescriptor(schema), ignore_if_exists=False
    )

    table = await connection.get_table(table_path)
    with pytest.raises(fluss.FlussError):
        await table.new_scan().create_record_batch_log_scanner()

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def _poll_records(scanner, expected_count, timeout_s=10):
    """Poll a record-based scanner until expected_count records are collected."""
    collected = []
    deadline = time.monotonic() + timeout_s
    while len(collected) < expected_count and time.monotonic() < deadline:
        records = await scanner.poll(5000)
        collected.extend(records)
    return collected
