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

"""Integration tests for KV (primary key) table operations.

Mirrors the Rust integration tests in crates/fluss/tests/integration/kv_table.rs.
"""

import math
from datetime import date, datetime, timezone
from datetime import time as dt_time
from decimal import Decimal

import pyarrow as pa
import pytest
from conftest import (
    assert_complex_edge,
    assert_complex_full,
    complex_column_names,
    complex_edge_row,
    complex_full_row,
    complex_null_row,
    complex_schema,
)

import fluss


async def _upsert_and_wait(writer, row):
    handle = writer.upsert(row)
    await handle.wait()


def _assert_float_specials(values):
    assert math.isnan(values[0])
    assert math.isinf(values[1]) and values[1] > 0
    assert math.isinf(values[2]) and values[2] < 0


async def test_upsert_delete_and_lookup(connection, admin):
    """Test upsert, lookup, update, delete, and non-existent key lookup."""
    table_path = fluss.TablePath("fluss", "py_test_upsert_and_lookup")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("age", pa.int64()),
            ]
        ),
        primary_keys=["id"],
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()

    test_data = [(1, "Verso", 32), (2, "Noco", 25), (3, "Esquie", 35)]

    # Upsert rows (fire-and-forget, then flush)
    for id_, name, age in test_data:
        upsert_writer.upsert({"id": id_, "name": name, "age": age})
    await upsert_writer.flush()

    # Lookup and verify
    lookuper = table.new_lookup().create_lookuper()

    for id_, expected_name, expected_age in test_data:
        result = await lookuper.lookup({"id": id_})
        assert result is not None, f"Row with id={id_} should exist"
        assert result["id"] == id_
        assert result["name"] == expected_name
        assert result["age"] == expected_age

    # Update record with id=1 (await acknowledgment)
    handle = upsert_writer.upsert({"id": 1, "name": "Verso", "age": 33})
    await handle.wait()

    result = await lookuper.lookup({"id": 1})
    assert result is not None
    assert result["age"] == 33
    assert result["name"] == "Verso"

    # Delete record with id=1 (await acknowledgment)
    handle = upsert_writer.delete({"id": 1})
    await handle.wait()

    result = await lookuper.lookup({"id": 1})
    assert result is None, "Record 1 should not exist after delete"

    # Verify other records still exist
    for id_ in [2, 3]:
        result = await lookuper.lookup({"id": id_})
        assert result is not None, f"Record {id_} should still exist"

    # Lookup non-existent key
    result = await lookuper.lookup({"id": 999})
    assert result is None, "Non-existent key should return None"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_composite_primary_keys(connection, admin):
    """Test upsert/lookup with composite PKs, including prefix lookup."""
    table_path = fluss.TablePath("fluss", "py_test_composite_pk")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    # PK columns intentionally interleaved with non-PK column to verify
    # that lookup correctly handles non-contiguous primary key indices.
    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("region", pa.string()),
                pa.field("score", pa.int64()),
                pa.field("user_id", pa.int32()),
                pa.field("event_id", pa.int64()),
            ]
        ),
        primary_keys=["region", "user_id", "event_id"],
    )
    table_descriptor = fluss.TableDescriptor(
        schema, bucket_count=3, bucket_keys=["region", "user_id"]
    )
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()

    test_data = [
        ("US", 1, 1, 100),
        ("US", 1, 2, 200),
        ("US", 2, 1, 300),
        ("EU", 1, 1, 150),
        ("EU", 2, 1, 250),
    ]

    for region, user_id, event_id, score in test_data:
        upsert_writer.upsert(
            {
                "region": region,
                "user_id": user_id,
                "event_id": event_id,
                "score": score,
            }
        )
    await upsert_writer.flush()

    lookuper = table.new_lookup().create_lookuper()

    # Lookup (US, 1, 1) -> score 100
    result = await lookuper.lookup({"region": "US", "user_id": 1, "event_id": 1})
    assert result is not None
    assert result["score"] == 100

    # Lookup (EU, 2, 1) -> score 250
    result = await lookuper.lookup({"region": "EU", "user_id": 2, "event_id": 1})
    assert result is not None
    assert result["score"] == 250

    # Update (US, 1, 1) score (await acknowledgment)
    handle = upsert_writer.upsert(
        {"region": "US", "user_id": 1, "event_id": 1, "score": 500}
    )
    await handle.wait()

    result = await lookuper.lookup({"region": "US", "user_id": 1, "event_id": 1})
    assert result is not None
    assert result["score"] == 500

    prefix_lookuper = (
        table.new_lookup().lookup_by(["region", "user_id"]).create_lookuper()
    )

    # Prefix (US, 1) should match 2 rows (event_id 1 and 2)
    rows = await prefix_lookuper.lookup({"region": "US", "user_id": 1})
    assert len(rows) == 2
    event_ids = sorted(row["event_id"] for row in rows)
    assert event_ids == [1, 2]

    # Also validate list/tuple prefix input
    rows = await prefix_lookuper.lookup(["US", 1])
    assert len(rows) == 2
    rows = await prefix_lookuper.lookup(("EU", 2))
    assert len(rows) == 1
    assert rows[0]["event_id"] == 1

    # Validate empty-result case: valid prefix shape but no matching rows.
    rows = await prefix_lookuper.lookup({"region": "APAC", "user_id": 999})
    assert rows == []

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_partial_update(connection, admin):
    """Test partial column update via partial_update_by_name."""
    table_path = fluss.TablePath("fluss", "py_test_partial_update")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("age", pa.int64()),
                pa.field("score", pa.int64()),
            ]
        ),
        primary_keys=["id"],
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)

    # Insert initial record
    upsert_writer = table.new_upsert().create_writer()
    handle = upsert_writer.upsert({"id": 1, "name": "Verso", "age": 32, "score": 6942})
    await handle.wait()

    lookuper = table.new_lookup().create_lookuper()
    result = await lookuper.lookup({"id": 1})
    assert result is not None
    assert result["id"] == 1
    assert result["name"] == "Verso"
    assert result["age"] == 32
    assert result["score"] == 6942

    # Partial update: only update score column
    partial_writer = (
        table.new_upsert().partial_update_by_name(["id", "score"]).create_writer()
    )
    handle = partial_writer.upsert({"id": 1, "score": 420})
    await handle.wait()

    result = await lookuper.lookup({"id": 1})
    assert result is not None
    assert result["id"] == 1
    assert result["name"] == "Verso", "name should remain unchanged"
    assert result["age"] == 32, "age should remain unchanged"
    assert result["score"] == 420, "score should be updated to 420"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_partial_update_by_index(connection, admin):
    """Test partial column update via partial_update_by_index."""
    table_path = fluss.TablePath("fluss", "py_test_partial_update_by_index")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("age", pa.int64()),
                pa.field("score", pa.int64()),
            ]
        ),
        primary_keys=["id"],
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)

    upsert_writer = table.new_upsert().create_writer()
    handle = upsert_writer.upsert({"id": 1, "name": "Verso", "age": 32, "score": 6942})
    await handle.wait()

    # Partial update by indices: columns 0=id (PK), 1=name
    partial_writer = table.new_upsert().partial_update_by_index([0, 1]).create_writer()
    handle = partial_writer.upsert([1, "Verso Renamed"])
    await handle.wait()

    lookuper = table.new_lookup().create_lookuper()
    result = await lookuper.lookup({"id": 1})
    assert result is not None
    assert result["name"] == "Verso Renamed", "name should be updated"
    assert result["score"] == 6942, "score should remain unchanged"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_partial_update_complex(connection, admin):
    """Partial updates preserve non-targeted ROW/MAP/ARRAY columns and update the
    targeted ones (mirrors the Rust partial_update IT)."""
    table_path = fluss.TablePath("fluss", "py_test_partial_update_complex")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field(
                    "nested", pa.struct([("seq", pa.int32()), ("label", pa.string())])
                ),
                pa.field("attrs", pa.map_(pa.string(), pa.int32())),
                pa.field("tags", pa.list_(pa.string())),
            ]
        ),
        primary_keys=["id"],
    )
    await admin.create_table(
        table_path, fluss.TableDescriptor(schema), ignore_if_exists=False
    )
    table = await connection.get_table(table_path)

    await _upsert_and_wait(
        table.new_upsert().create_writer(),
        {
            "id": 1,
            "name": "Verso",
            "nested": {"seq": 10, "label": "alpha"},
            "attrs": {"a": 1, "b": 2},
            "tags": ["alpha-tag", "beta-tag"],
        },
    )

    lookuper = table.new_lookup().create_lookuper()

    # Update only `name`: ROW/MAP/ARRAY columns must be preserved.
    await _upsert_and_wait(
        table.new_upsert().partial_update_by_name(["id", "name"]).create_writer(),
        {"id": 1, "name": "Renamed"},
    )
    r = await lookuper.lookup({"id": 1})
    assert r["name"] == "Renamed"
    assert r["nested"] == {"seq": 10, "label": "alpha"}
    assert dict(r["attrs"]) == {"a": 1, "b": 2}
    assert r["tags"] == ["alpha-tag", "beta-tag"]

    # Update only `nested`: scalar + other complex columns preserved.
    await _upsert_and_wait(
        table.new_upsert().partial_update_by_name(["id", "nested"]).create_writer(),
        {"id": 1, "nested": {"seq": 99, "label": "omega"}},
    )
    r = await lookuper.lookup({"id": 1})
    assert r["nested"] == {"seq": 99, "label": "omega"}
    assert r["name"] == "Renamed"
    assert dict(r["attrs"]) == {"a": 1, "b": 2}
    assert r["tags"] == ["alpha-tag", "beta-tag"]

    # Update `attrs` and `tags`.
    await _upsert_and_wait(
        table.new_upsert()
        .partial_update_by_name(["id", "attrs", "tags"])
        .create_writer(),
        {"id": 1, "attrs": {"z": 99}, "tags": ["gamma"]},
    )
    r = await lookuper.lookup({"id": 1})
    assert dict(r["attrs"]) == {"z": 99}
    assert r["tags"] == ["gamma"]
    assert r["nested"] == {"seq": 99, "label": "omega"}
    assert r["name"] == "Renamed"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_partitioned_table_upsert_and_lookup(connection, admin):
    """Test upsert/lookup/delete on a partitioned KV table."""
    table_path = fluss.TablePath("fluss", "py_test_partitioned_kv_table")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("region", pa.string()),
                pa.field("user_id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("score", pa.int64()),
            ]
        ),
        primary_keys=["region", "user_id"],
    )
    table_descriptor = fluss.TableDescriptor(
        schema,
        partition_keys=["region"],
    )
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    # Create partitions
    for region in ["US", "EU", "APAC"]:
        await admin.create_partition(
            table_path, {"region": region}, ignore_if_exists=True
        )

    table = await connection.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()

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

    lookuper = table.new_lookup().create_lookuper()

    # Verify all rows across partitions
    for region, user_id, expected_name, expected_score in test_data:
        result = await lookuper.lookup({"region": region, "user_id": user_id})
        assert result is not None, f"Row ({region}, {user_id}) should exist"
        assert result["region"] == region
        assert result["user_id"] == user_id
        assert result["name"] == expected_name
        assert result["score"] == expected_score

    # Update within a partition (await acknowledgment)
    handle = upsert_writer.upsert(
        {"region": "US", "user_id": 1, "name": "Gustave Updated", "score": 999}
    )
    await handle.wait()

    result = await lookuper.lookup({"region": "US", "user_id": 1})
    assert result is not None
    assert result["name"] == "Gustave Updated"
    assert result["score"] == 999

    # Lookup in non-existent partition should return None
    result = await lookuper.lookup({"region": "UNKNOWN_REGION", "user_id": 1})
    assert result is None, "Lookup in non-existent partition should return None"

    # Delete within a partition (await acknowledgment)
    handle = upsert_writer.delete({"region": "EU", "user_id": 1})
    await handle.wait()

    result = await lookuper.lookup({"region": "EU", "user_id": 1})
    assert result is None, "Deleted record should not exist"

    # Verify sibling record still exists
    result = await lookuper.lookup({"region": "EU", "user_id": 2})
    assert result is not None
    assert result["name"] == "Maelle"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_omit_nullable_vs_required_fields(connection, admin):
    """Omitting a nullable field defaults it to null; omitting a non-nullable (or
    PK) field is a clear client-side error -- at both top level and inside a ROW."""
    table_path = fluss.TablePath("fluss", "py_test_omit_fields")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    nested_type = pa.struct(
        [
            pa.field("a", pa.int32()),  # nullable
            pa.field("b", pa.int32(), nullable=False),  # NOT NULL
        ]
    )
    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("opt", pa.int32()),  # nullable
                pa.field("req", pa.int32(), nullable=False),  # NOT NULL
                pa.field("nested", nested_type),
            ]
        ),
        primary_keys=["id"],
    )
    await admin.create_table(
        table_path, fluss.TableDescriptor(schema), ignore_if_exists=False
    )
    table = await connection.get_table(table_path)
    writer = table.new_upsert().create_writer()
    lookuper = table.new_lookup().create_lookuper()

    # Omit nullable top-level field `opt` -> Null.
    await _upsert_and_wait(writer, {"id": 1, "req": 5, "nested": {"a": 1, "b": 2}})
    r = await lookuper.lookup({"id": 1})
    assert r["opt"] is None
    assert r["req"] == 5
    assert r["nested"] == {"a": 1, "b": 2}

    # Omit nullable nested field `a` -> Null.
    await _upsert_and_wait(writer, {"id": 2, "req": 9, "nested": {"b": 3}})
    r = await lookuper.lookup({"id": 2})
    assert r["nested"] == {"a": None, "b": 3}

    # Omit non-nullable top-level field `req` -> error.
    with pytest.raises(Exception, match="non-nullable field 'req'"):
        writer.upsert({"id": 3, "opt": 1, "nested": {"a": 1, "b": 2}})

    # Omit non-nullable nested field `b` -> error.
    with pytest.raises(Exception, match="non-nullable row field 'b'"):
        writer.upsert({"id": 4, "req": 5, "nested": {"a": 1}})

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_partitioned_complex(connection, admin):
    """ROW/MAP/ARRAY columns round-trip and update correctly across partitions
    (mirrors the complex columns in the Rust partitioned_table_upsert_and_lookup IT)."""
    table_path = fluss.TablePath("fluss", "py_test_partitioned_complex")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("region", pa.string()),
                pa.field("user_id", pa.int32()),
                pa.field(
                    "nested", pa.struct([("seq", pa.int32()), ("label", pa.string())])
                ),
                pa.field("attrs", pa.map_(pa.string(), pa.int32())),
                pa.field("tags", pa.list_(pa.string())),
            ]
        ),
        primary_keys=["region", "user_id"],
    )
    await admin.create_table(
        table_path,
        fluss.TableDescriptor(schema, partition_keys=["region"]),
        ignore_if_exists=False,
    )
    for region in ["US", "EU"]:
        await admin.create_partition(
            table_path, {"region": region}, ignore_if_exists=True
        )

    table = await connection.get_table(table_path)
    writer = table.new_upsert().create_writer()
    await _upsert_and_wait(
        writer,
        {
            "region": "US",
            "user_id": 1,
            "nested": {"seq": 11, "label": "a"},
            "attrs": {"x": 1},
            "tags": ["alpha"],
        },
    )
    await _upsert_and_wait(
        writer,
        {
            "region": "EU",
            "user_id": 1,
            "nested": {"seq": 22, "label": "b"},
            "attrs": {"y": 2},
            "tags": ["beta"],
        },
    )

    lookuper = table.new_lookup().create_lookuper()
    r = await lookuper.lookup({"region": "US", "user_id": 1})
    assert r["nested"] == {"seq": 11, "label": "a"}
    assert dict(r["attrs"]) == {"x": 1}
    assert r["tags"] == ["alpha"]
    r = await lookuper.lookup({"region": "EU", "user_id": 1})
    assert r["nested"] == {"seq": 22, "label": "b"}
    assert dict(r["attrs"]) == {"y": 2}
    assert r["tags"] == ["beta"]

    # Update complex columns within one partition; sibling partition unaffected.
    await _upsert_and_wait(
        writer,
        {
            "region": "US",
            "user_id": 1,
            "nested": {"seq": 99, "label": "updated"},
            "attrs": {"z": 99},
            "tags": ["renamed"],
        },
    )
    r = await lookuper.lookup({"region": "US", "user_id": 1})
    assert r["nested"] == {"seq": 99, "label": "updated"}
    assert dict(r["attrs"]) == {"z": 99}
    assert r["tags"] == ["renamed"]
    r = await lookuper.lookup({"region": "EU", "user_id": 1})
    assert r["nested"] == {"seq": 22, "label": "b"}

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_all_complex_datatypes(connection, admin):
    """Comprehensive ARRAY/MAP/ROW coverage via upsert+lookup: full, edge, null rows.

    Mirrors the section-based ``all_supported_datatypes`` structure of the Rust
    integration tests: the schema is composed from the shared array/row/map
    sections, and three rows exercise fully-populated, edge-case, and all-null
    values (see complex_types.py).
    """
    table_path = fluss.TablePath("fluss", "py_test_kv_all_complex")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = complex_schema(primary_keys=["id"])
    await admin.create_table(
        table_path, fluss.TableDescriptor(schema), ignore_if_exists=False
    )

    table = await connection.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()
    await _upsert_and_wait(upsert_writer, complex_full_row(1))
    await _upsert_and_wait(upsert_writer, complex_edge_row(2))
    await _upsert_and_wait(upsert_writer, complex_null_row(3))

    lookuper = table.new_lookup().create_lookuper()

    r1 = await lookuper.lookup({"id": 1})
    assert r1 is not None
    assert_complex_full(r1)

    r2 = await lookuper.lookup({"id": 2})
    assert r2 is not None
    assert_complex_edge(r2)

    r3 = await lookuper.lookup({"id": 3})
    assert r3 is not None
    for col in complex_column_names():
        assert r3[col] is None, f"{col} should be null"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_all_supported_datatypes(connection, admin):
    """Test upsert/lookup for all supported data types, including nulls."""
    table_path = fluss.TablePath("fluss", "py_test_kv_all_datatypes")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("pk_int", pa.int32()),
                pa.field("col_boolean", pa.bool_()),
                pa.field("col_tinyint", pa.int8()),
                pa.field("col_smallint", pa.int16()),
                pa.field("col_int", pa.int32()),
                pa.field("col_bigint", pa.int64()),
                pa.field("col_float", pa.float32()),
                pa.field("col_double", pa.float64()),
                pa.field("col_string", pa.string()),
                pa.field("col_decimal", pa.decimal128(10, 2)),
                pa.field("col_date", pa.date32()),
                pa.field("col_time", pa.time32("ms")),
                pa.field("col_timestamp_ntz", pa.timestamp("us")),
                pa.field("col_timestamp_ltz", pa.timestamp("us", tz="UTC")),
                pa.field("col_bytes", pa.binary()),
                pa.field("col_array", pa.list_(pa.string())),
                pa.field("col_binary", pa.binary(16)),
            ]
        ),
        primary_keys=["pk_int"],
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()

    # Test data for all types
    row_data = {
        "pk_int": 1,
        "col_boolean": True,
        "col_tinyint": 127,
        "col_smallint": 32767,
        "col_int": 2147483647,
        "col_bigint": 9223372036854775807,
        "col_float": 3.14,
        "col_double": 2.718281828459045,
        "col_string": "world of fluss python client",
        "col_decimal": Decimal("123.45"),
        "col_date": date(2026, 1, 23),
        "col_time": dt_time(10, 13, 47, 123000),  # millisecond precision
        "col_timestamp_ntz": datetime(2026, 1, 23, 10, 13, 47, 123000),
        "col_timestamp_ltz": datetime(2026, 1, 23, 10, 13, 47, 123000),
        "col_bytes": b"binary data",
        "col_array": ["fluss", "python"],
        "col_binary": b"binary_data_0123",
    }

    await _upsert_and_wait(upsert_writer, row_data)

    lookuper = table.new_lookup().create_lookuper()
    result = await lookuper.lookup({"pk_int": 1})
    assert result is not None, "Row should exist"

    assert result["pk_int"] == 1
    assert result["col_boolean"] is True
    assert result["col_tinyint"] == 127
    assert result["col_smallint"] == 32767
    assert result["col_int"] == 2147483647
    assert result["col_bigint"] == 9223372036854775807
    assert math.isclose(result["col_float"], 3.14, rel_tol=1e-6)
    assert math.isclose(result["col_double"], 2.718281828459045, rel_tol=1e-15)
    assert result["col_string"] == "world of fluss python client"
    assert result["col_decimal"] == Decimal("123.45")
    assert result["col_date"] == date(2026, 1, 23)
    assert result["col_time"] == dt_time(10, 13, 47, 123000)
    assert result["col_timestamp_ntz"] == datetime(2026, 1, 23, 10, 13, 47, 123000)
    assert result["col_timestamp_ltz"] == datetime(
        2026, 1, 23, 10, 13, 47, 123000, tzinfo=timezone.utc
    )
    assert result["col_bytes"] == b"binary data"
    assert result["col_array"] == ["fluss", "python"]
    assert result["col_binary"] == b"binary_data_0123"

    # Test with null values for all nullable columns
    null_row = {"pk_int": 2}
    for col in row_data:
        if col != "pk_int":
            null_row[col] = None
    await _upsert_and_wait(upsert_writer, null_row)

    result = await lookuper.lookup({"pk_int": 2})
    assert result is not None, "Row with nulls should exist"
    assert result["pk_int"] == 2
    for col in row_data:
        if col != "pk_int":
            assert result[col] is None, f"{col} should be null"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_prefix_lookup_validation_errors(connection, admin):
    """Test that prefix lookup raises errors for invalid column configurations."""
    table_path = fluss.TablePath("fluss", "py_test_prefix_lookup_validation")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.string()),
                pa.field("c", pa.int64()),
            ]
        ),
        primary_keys=["a", "b", "c"],
    )
    table_descriptor = fluss.TableDescriptor(
        schema, bucket_count=3, bucket_keys=["a", "b"]
    )
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)

    # lookup_by with columns equal to full PK should error
    with pytest.raises(fluss.FlussError, match="prefix lookup"):
        table.new_lookup().lookup_by(["a", "b", "c"]).create_lookuper()

    # lookup_by with wrong column names should error
    with pytest.raises(fluss.FlussError, match="bucket keys"):
        table.new_lookup().lookup_by(["a", "c"]).create_lookuper()

    # lookup_by with unknown column should error
    with pytest.raises(fluss.FlussError, match="Unknown column name"):
        table.new_lookup().lookup_by(["a", "missing_col"]).create_lookuper()

    await admin.drop_table(table_path, ignore_if_not_exists=False)

    # Partitioned table: lookup columns must include partition keys first,
    # followed by bucket keys.
    partitioned_table_path = fluss.TablePath(
        "fluss", "py_test_prefix_lookup_validation_pt"
    )
    await admin.drop_table(partitioned_table_path, ignore_if_not_exists=True)

    partitioned_schema = fluss.Schema(
        pa.schema(
            [
                pa.field("region", pa.string()),
                pa.field("user_id", pa.int32()),
                pa.field("event_id", pa.int64()),
            ]
        ),
        primary_keys=["region", "user_id", "event_id"],
    )
    partitioned_table_descriptor = fluss.TableDescriptor(
        partitioned_schema,
        partition_keys=["region"],
        bucket_count=3,
        bucket_keys=["user_id"],
    )
    await admin.create_table(
        partitioned_table_path, partitioned_table_descriptor, ignore_if_exists=False
    )

    partitioned_table = await connection.get_table(partitioned_table_path)

    # Missing partition key in lookup columns.
    with pytest.raises(fluss.FlussError, match="partition fields"):
        partitioned_table.new_lookup().lookup_by(["user_id"]).create_lookuper()

    # A non-existent partition returns empty list.
    partitioned_prefix_lookuper = (
        partitioned_table.new_lookup()
        .lookup_by(["region", "user_id"])
        .create_lookuper()
    )
    rows = await partitioned_prefix_lookuper.lookup(
        {"region": "UNKNOWN_REGION", "user_id": 1}
    )
    assert rows == []

    # After partition keys, remaining columns must equal bucket keys.
    with pytest.raises(fluss.FlussError, match="bucket keys"):
        partitioned_table.new_lookup().lookup_by(
            ["region", "event_id"]
        ).create_lookuper()

    await admin.drop_table(partitioned_table_path, ignore_if_not_exists=False)
