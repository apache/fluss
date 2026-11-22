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

import pytest
import pyarrow as pa
import time
import fluss

async def _poll_records(scanner, expected_count, timeout_s=10):
    """Poll a record-based scanner until expected_count records are collected."""
    collected = []
    deadline = time.monotonic() + timeout_s
    while len(collected) < expected_count and time.monotonic() < deadline:
        records = await scanner.poll(5000)
        collected.extend(records)
    return collected

@pytest.mark.asyncio
async def test_connection_context_manager(plaintext_bootstrap_servers):
    config = fluss.Config({"bootstrap.servers": plaintext_bootstrap_servers})
    async with await fluss.FlussConnection.create(config) as conn:
        admin = conn.get_admin()
        nodes = await admin.get_server_nodes()
        assert len(nodes) > 0


@pytest.mark.asyncio
async def test_append_writer_success_flush(connection, admin):
    table_path = fluss.TablePath("fluss", "test_append_ctx_success")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    
    schema = fluss.Schema(pa.schema([pa.field("a", pa.int32())]))
    await admin.create_table(table_path, fluss.TableDescriptor(schema))
    
    table = await connection.get_table(table_path)
    
    async with table.new_append().create_writer() as writer:
        writer.append({"a": 1})
        writer.append({"a": 2})
        # No explicit flush here
        
    # After context exit, data should be flushed
    scanner = await table.new_scan().create_log_scanner()
    scanner.subscribe(0, fluss.EARLIEST_OFFSET)
    records = await _poll_records(scanner, expected_count=2)
    assert len(records) == 2
    assert sorted([r.row["a"] for r in records]) == [1, 2]

@pytest.mark.asyncio
async def test_connection_drain_on_close(plaintext_bootstrap_servers, admin):
    table_path = fluss.TablePath("fluss", "test_conn_drain")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    schema = fluss.Schema(pa.schema([pa.field("a", pa.int32())]))
    await admin.create_table(table_path, fluss.TableDescriptor(schema))

    config = fluss.Config({"bootstrap.servers": plaintext_bootstrap_servers})
    async with await fluss.FlussConnection.create(config) as conn:
        table = await conn.get_table(table_path)
        writer = table.new_append().create_writer()
        writer.append({"a": 123})
        # No explicit flush, no writer context exit. 
        # Rely on connection.__aexit__ -> close() to drain.
    
    # Re-connect with a new connection to verify data arrived
    async with await fluss.FlussConnection.create(config) as conn2:
        table2 = await conn2.get_table(table_path)
        scanner = await table2.new_scan().create_log_scanner()
        scanner.subscribe(0, fluss.EARLIEST_OFFSET)
        records = await _poll_records(scanner, expected_count=1)
        assert len(records) == 1
        assert records[0].row["a"] == 123

@pytest.mark.asyncio
async def test_upsert_writer_context_manager(connection, admin):
    table_path = fluss.TablePath("fluss", "test_upsert_ctx")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    
    schema = fluss.Schema(pa.schema([pa.field("id", pa.int32()), pa.field("v", pa.string())]), primary_keys=["id"])
    await admin.create_table(table_path, fluss.TableDescriptor(schema))
    
    table = await connection.get_table(table_path)
    
    # Success path: verify it flushes
    async with table.new_upsert().create_writer() as writer:
        writer.upsert({"id": 1, "v": "a"})
        
    lookuper = table.new_lookup().create_lookuper()
    res = await lookuper.lookup({"id": 1})
    assert res is not None
    assert res["v"] == "a"
    
@pytest.mark.asyncio
async def test_connection_context_manager_exception(plaintext_bootstrap_servers):
    config = fluss.Config({"bootstrap.servers": plaintext_bootstrap_servers})
    class TestException(Exception): pass
    
    try:
        async with await fluss.FlussConnection.create(config) as conn:
            raise TestException("connection error")
    except TestException:
        pass
    # If we reach here without hanging, the connection __aexit__ gracefully handled the error