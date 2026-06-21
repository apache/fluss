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

"""Integration tests for FlussAdmin operations.

Mirrors the Rust integration tests in crates/fluss/tests/integration/admin.rs.
"""

import pyarrow as pa
import pytest

import fluss


async def test_create_database(admin):
    """Test database create, exists, get_info, and drop lifecycle."""
    db_name = "py_test_create_database"

    # Cleanup in case of prior failed run
    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)

    assert not await admin.database_exists(db_name)

    db_descriptor = fluss.DatabaseDescriptor(
        comment="test_db",
        custom_properties={"k1": "v1", "k2": "v2"},
    )
    await admin.create_database(db_name, db_descriptor, ignore_if_exists=False)

    assert await admin.database_exists(db_name)

    db_info = await admin.get_database_info(db_name)
    assert db_info.database_name == db_name

    descriptor = db_info.get_database_descriptor()
    assert descriptor.comment == "test_db"
    assert descriptor.get_custom_properties() == {"k1": "v1", "k2": "v2"}

    await admin.drop_database(db_name, ignore_if_not_exists=False, cascade=True)

    assert not await admin.database_exists(db_name)


async def test_create_table(admin):
    """Test table create, exists, get_info, list, and drop lifecycle."""
    db_name = "py_test_create_table_db"

    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)

    assert not await admin.database_exists(db_name)
    await admin.create_database(
        db_name,
        fluss.DatabaseDescriptor(comment="Database for test_create_table"),
        ignore_if_exists=False,
    )

    table_name = "test_user_table"
    table_path = fluss.TablePath(db_name, table_name)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("age", pa.int32()),
                pa.field("email", pa.string()),
            ]
        ),
        primary_keys=["id"],
    )
    assert schema.get_primary_keys() == ["id"]

    table_descriptor = fluss.TableDescriptor(
        schema,
        bucket_count=3,
        bucket_keys=["id"],
        comment="Test table for user data (id, name, age, email)",
        log_format="arrow",
        kv_format="indexed",
        properties={"table.replication.factor": "1"},
    )

    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    assert await admin.table_exists(table_path)

    tables = await admin.list_tables(db_name)
    assert len(tables) == 1
    assert table_name in tables

    table_info = await admin.get_table_info(table_path)

    assert table_info.comment == "Test table for user data (id, name, age, email)"
    assert table_info.get_primary_keys() == ["id"]
    assert table_info.num_buckets == 3
    assert table_info.get_bucket_keys() == ["id"]
    assert table_info.get_column_names() == ["id", "name", "age", "email"]

    await admin.drop_table(table_path, ignore_if_not_exists=False)
    assert not await admin.table_exists(table_path)

    await admin.drop_database(db_name, ignore_if_not_exists=False, cascade=True)
    assert not await admin.database_exists(db_name)


async def test_partition_apis(admin):
    """Test partition create, list, and drop lifecycle."""
    db_name = "py_test_partition_apis_db"

    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)
    await admin.create_database(
        db_name,
        fluss.DatabaseDescriptor(comment="Database for test_partition_apis"),
        ignore_if_exists=True,
    )

    table_path = fluss.TablePath(db_name, "partitioned_table")

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("dt", pa.string()),
                pa.field("region", pa.string()),
            ]
        ),
        primary_keys=["id", "dt", "region"],
    )

    table_descriptor = fluss.TableDescriptor(
        schema,
        partition_keys=["dt", "region"],
        bucket_count=3,
        bucket_keys=["id"],
        log_format="arrow",
        kv_format="compacted",
        properties={"table.replication.factor": "1"},
    )

    await admin.create_table(table_path, table_descriptor, ignore_if_exists=True)

    # Initially no partitions
    partitions = await admin.list_partition_infos(table_path)
    assert len(partitions) == 0

    # Create a partition
    await admin.create_partition(
        table_path,
        {"dt": "2024-01-15", "region": "EMEA"},
        ignore_if_exists=False,
    )

    partitions = await admin.list_partition_infos(table_path)
    assert len(partitions) == 1
    assert partitions[0].partition_name == "2024-01-15$EMEA"

    # Drop the partition
    await admin.drop_partition(
        table_path,
        {"dt": "2024-01-15", "region": "EMEA"},
        ignore_if_not_exists=False,
    )

    partitions = await admin.list_partition_infos(table_path)
    assert len(partitions) == 0

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)


async def test_fluss_error_response(admin):
    """Test that API errors are raised as FlussError with correct error codes."""
    table_path = fluss.TablePath("fluss", "py_not_exist")

    with pytest.raises(fluss.FlussError) as exc_info:
        await admin.get_table_info(table_path)

    assert exc_info.value.error_code == fluss.ErrorCode.TABLE_NOT_EXIST


async def test_error_database_not_exist(admin):
    """Test error handling for non-existent database operations."""
    # get_database_info
    with pytest.raises(fluss.FlussError) as exc_info:
        await admin.get_database_info("py_no_such_db")
    assert exc_info.value.error_code == fluss.ErrorCode.DATABASE_NOT_EXIST

    # drop_database without ignore flag
    with pytest.raises(fluss.FlussError) as exc_info:
        await admin.drop_database("py_no_such_db", ignore_if_not_exists=False)
    assert exc_info.value.error_code == fluss.ErrorCode.DATABASE_NOT_EXIST

    # list_tables for non-existent database
    with pytest.raises(fluss.FlussError) as exc_info:
        await admin.list_tables("py_no_such_db")
    assert exc_info.value.error_code == fluss.ErrorCode.DATABASE_NOT_EXIST


async def test_error_database_already_exist(admin):
    """Test error when creating a database that already exists."""
    db_name = "py_test_error_db_already_exist"

    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)
    await admin.create_database(db_name, ignore_if_exists=False)

    # Create same database again without ignore flag
    with pytest.raises(fluss.FlussError) as exc_info:
        await admin.create_database(db_name, ignore_if_exists=False)
    assert exc_info.value.error_code == fluss.ErrorCode.DATABASE_ALREADY_EXIST

    # With ignore flag should succeed
    await admin.create_database(db_name, ignore_if_exists=True)

    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)


async def test_error_table_already_exist(admin):
    """Test error when creating a table that already exists."""
    db_name = "py_test_error_tbl_already_exist_db"

    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)
    await admin.create_database(db_name, ignore_if_exists=True)

    table_path = fluss.TablePath(db_name, "my_table")
    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(
        schema,
        bucket_count=1,
        properties={"table.replication.factor": "1"},
    )

    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    # Create same table again without ignore flag
    with pytest.raises(fluss.FlussError) as exc_info:
        await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)
    assert exc_info.value.error_code == fluss.ErrorCode.TABLE_ALREADY_EXIST

    # With ignore flag should succeed
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=True)

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)


async def test_error_table_not_exist(admin):
    """Test error handling for non-existent table operations."""
    table_path = fluss.TablePath("fluss", "py_no_such_table")

    # drop without ignore flag
    with pytest.raises(fluss.FlussError) as exc_info:
        await admin.drop_table(table_path, ignore_if_not_exists=False)
    assert exc_info.value.error_code == fluss.ErrorCode.TABLE_NOT_EXIST

    # drop with ignore flag should succeed
    await admin.drop_table(table_path, ignore_if_not_exists=True)


async def test_get_server_nodes(admin):
    """Test get_server_nodes returns coordinator and tablet servers."""
    nodes = await admin.get_server_nodes()

    assert len(nodes) > 0, "Expected at least one server node"

    server_types = [n.server_type for n in nodes]
    assert "CoordinatorServer" in server_types, "Expected a coordinator server"
    assert "TabletServer" in server_types, "Expected at least one tablet server"

    for node in nodes:
        assert node.host, "Server node host should not be empty"
        assert node.port > 0, "Server node port should be > 0"
        assert node.uid, "Server node uid should not be empty"
        assert repr(node).startswith("ServerNode(")


async def test_error_table_not_partitioned(admin):
    """Test error when calling partition operations on non-partitioned table."""
    db_name = "py_test_error_not_partitioned_db"

    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)
    await admin.create_database(db_name, ignore_if_exists=True)

    table_path = fluss.TablePath(db_name, "non_partitioned_table")
    schema = fluss.Schema(
        pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    )
    table_descriptor = fluss.TableDescriptor(
        schema,
        bucket_count=1,
        properties={"table.replication.factor": "1"},
    )

    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    with pytest.raises(fluss.FlussError) as exc_info:
        await admin.list_partition_infos(table_path)
    assert (
        exc_info.value.error_code == fluss.ErrorCode.TABLE_NOT_PARTITIONED_EXCEPTION
    )

    await admin.drop_table(table_path, ignore_if_not_exists=True)
    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)
