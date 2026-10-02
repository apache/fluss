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

"""Integration tests for the one-shot limit-based BatchScanner.

Mirrors crates/fluss/tests/integration/batch_scanner.rs.
"""

import pyarrow as pa
import pytest

import fluss


async def test_returns_appended_rows_then_none(connection, admin):
    table_path = fluss.TablePath("fluss", "py_test_bs_log")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    schema = pa.schema([pa.field("c1", pa.int32()), pa.field("c2", pa.string())])
    table_descriptor = fluss.TableDescriptor(
        fluss.Schema(schema), bucket_count=1, bucket_keys=["c1"]
    )
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()
    append_writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2, 3, 4, 5], pa.int32()),
                pa.array(["a", "b", "c", "d", "e"]),
            ],
            schema=schema,
        )
    )
    await append_writer.flush()

    bucket = fluss.TableBucket(table.get_table_info().table_id, 0)
    scanner = table.new_scan().limit(3).create_bucket_batch_scanner(bucket)
    assert scanner.bucket == bucket

    first = await scanner.next_batch()
    assert first is not None
    assert first.bucket == bucket
    # The server may return fewer rows than the limit, but never more.
    assert 0 < first.batch.num_rows <= 3
    assert await scanner.next_batch() is None

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_reads_primary_key_table(connection, admin):
    table_path = fluss.TablePath("fluss", "py_test_bs_pk")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())]),
        primary_keys=["id"],
    )
    table_descriptor = fluss.TableDescriptor(schema, bucket_count=1)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()
    expected = {1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}
    for id_, name in expected.items():
        upsert_writer.upsert({"id": id_, "name": name})
    await upsert_writer.flush()

    bucket = fluss.TableBucket(table.get_table_info().table_id, 0)
    scanner = table.new_scan().limit(3).create_bucket_batch_scanner(bucket)
    first = await scanner.next_batch()
    assert first is not None

    rows = first.batch.to_pydict()
    assert 0 < len(rows["id"]) <= 3
    assert all(expected[i] == name for i, name in zip(rows["id"], rows["name"]))
    assert await scanner.next_batch() is None

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_to_arrow_and_collect(connection, admin):
    table_path = fluss.TablePath("fluss", "py_test_bs_to_arrow")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    schema = pa.schema([pa.field("c1", pa.int32()), pa.field("c2", pa.string())])
    table_descriptor = fluss.TableDescriptor(
        fluss.Schema(schema), bucket_count=1, bucket_keys=["c1"]
    )
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()
    append_writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [pa.array([10, 20], pa.int32()), pa.array(["x", "y"])], schema=schema
        )
    )
    await append_writer.flush()
    table_id = table.get_table_info().table_id

    batches = (
        await table.new_scan()
        .limit(10)
        .create_bucket_batch_scanner(fluss.TableBucket(table_id, 0))
        .collect_all_batches()
    )
    assert [b.batch.num_rows for b in batches] == [2]

    arrow = (
        await table.new_scan()
        .limit(10)
        .create_bucket_batch_scanner(fluss.TableBucket(table_id, 0))
        .to_arrow()
    )
    assert arrow.to_pydict() == {"c1": [10, 20], "c2": ["x", "y"]}

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_projection_skips_middle_column(connection, admin):
    table_path = fluss.TablePath("fluss", "py_test_bs_projection")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    schema = pa.schema(
        [
            pa.field("c1", pa.int32()),
            pa.field("c2", pa.string()),
            pa.field("c3", pa.int64()),
        ]
    )
    table_descriptor = fluss.TableDescriptor(
        fluss.Schema(schema), bucket_count=1, bucket_keys=["c1"]
    )
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    append_writer = table.new_append().create_writer()
    append_writer.write_arrow_batch(
        pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2], pa.int32()),
                pa.array(["a", "b"]),
                pa.array([100, 200], pa.int64()),
            ],
            schema=schema,
        )
    )
    await append_writer.flush()

    bucket = fluss.TableBucket(table.get_table_info().table_id, 0)
    arrow = (
        await table.new_scan()
        .project_by_name(["c1", "c3"])
        .limit(10)
        .create_bucket_batch_scanner(bucket)
        .to_arrow()
    )
    assert arrow.to_pydict() == {"c1": [1, 2], "c3": [100, 200]}

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_construction_errors(connection, admin):
    table_path = fluss.TablePath("fluss", "py_test_bs_errors")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    schema = fluss.Schema(pa.schema([pa.field("c1", pa.int32())]))
    table_descriptor = fluss.TableDescriptor(schema, bucket_count=1, bucket_keys=["c1"])
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    table_id = table.get_table_info().table_id

    for bad in (0, -5):
        with pytest.raises(fluss.FlussError):
            table.new_scan().limit(bad)

    with pytest.raises(fluss.FlussError):
        table.new_scan().create_bucket_batch_scanner(fluss.TableBucket(table_id, 0))

    for bad_bucket in (
        fluss.TableBucket(table_id + 9999, 0),
        fluss.TableBucket(table_id, 99),
    ):
        with pytest.raises(fluss.FlussError):
            table.new_scan().limit(1).create_bucket_batch_scanner(bad_bucket)

    with pytest.raises(fluss.FlussError):
        await table.new_scan().limit(5).create_log_scanner()
    with pytest.raises(fluss.FlussError):
        await table.new_scan().limit(5).create_record_batch_log_scanner()

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_rejects_non_arrow_log_format(connection, admin):
    table_path = fluss.TablePath("fluss", "py_test_bs_indexed")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    schema = fluss.Schema(pa.schema([pa.field("c1", pa.int32())]))
    table_descriptor = fluss.TableDescriptor(
        schema, bucket_count=1, bucket_keys=["c1"], log_format="INDEXED"
    )
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    bucket = fluss.TableBucket(table.get_table_info().table_id, 0)
    with pytest.raises(fluss.FlussError):
        table.new_scan().limit(1).create_bucket_batch_scanner(bucket)

    await admin.drop_table(table_path, ignore_if_not_exists=False)
