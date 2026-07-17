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

"""Integration tests for SASL/PLAIN authentication.

Mirrors the Rust integration tests in crates/fluss/tests/integration/sasl_auth.rs.
"""

import pytest

import fluss


async def test_sasl_connect_with_valid_credentials(sasl_bootstrap_servers):
    """Verify that a client with correct SASL credentials can connect and perform operations."""
    config = fluss.Config({
        "bootstrap.servers": sasl_bootstrap_servers,
        "security.protocol": "sasl",
        "security.sasl.mechanism": "PLAIN",
        "security.sasl.username": "admin",
        "security.sasl.password": "admin-secret",
    })
    conn = await fluss.FlussConnection.create(config)
    admin = conn.get_admin()

    db_name = "py_sasl_test_valid_db"
    db_descriptor = fluss.DatabaseDescriptor(comment="created via SASL auth")
    await admin.create_database(db_name, db_descriptor, ignore_if_exists=True)

    assert await admin.database_exists(db_name)

    # Cleanup
    await admin.drop_database(db_name, ignore_if_not_exists=True, cascade=True)
    await conn.close()


async def test_sasl_connect_with_second_user(sasl_bootstrap_servers):
    """Verify that a second user can also authenticate successfully."""
    config = fluss.Config({
        "bootstrap.servers": sasl_bootstrap_servers,
        "security.protocol": "sasl",
        "security.sasl.mechanism": "PLAIN",
        "security.sasl.username": "alice",
        "security.sasl.password": "alice-secret",
    })
    conn = await fluss.FlussConnection.create(config)
    admin = conn.get_admin()

    # Basic operation to confirm functional connection
    assert not await admin.database_exists("some_nonexistent_db_alice")
    await conn.close()


async def test_sasl_connect_with_wrong_password(sasl_bootstrap_servers):
    """Verify that wrong credentials are rejected with AUTHENTICATE_EXCEPTION."""
    config = fluss.Config({
        "bootstrap.servers": sasl_bootstrap_servers,
        "security.protocol": "sasl",
        "security.sasl.mechanism": "PLAIN",
        "security.sasl.username": "admin",
        "security.sasl.password": "wrong-password",
    })
    with pytest.raises(fluss.FlussError) as exc_info:
        await fluss.FlussConnection.create(config)

    assert exc_info.value.error_code == fluss.ErrorCode.AUTHENTICATE_EXCEPTION


async def test_sasl_connect_with_unknown_user(sasl_bootstrap_servers):
    """Verify that a nonexistent user is rejected with AUTHENTICATE_EXCEPTION."""
    config = fluss.Config({
        "bootstrap.servers": sasl_bootstrap_servers,
        "security.protocol": "sasl",
        "security.sasl.mechanism": "PLAIN",
        "security.sasl.username": "nonexistent_user",
        "security.sasl.password": "some-password",
    })
    with pytest.raises(fluss.FlussError) as exc_info:
        await fluss.FlussConnection.create(config)

    assert exc_info.value.error_code == fluss.ErrorCode.AUTHENTICATE_EXCEPTION


async def test_sasl_client_to_plaintext_server(plaintext_bootstrap_servers):
    """Verify that a SASL-configured client fails when connecting to a plaintext server."""
    config = fluss.Config({
        "bootstrap.servers": plaintext_bootstrap_servers,
        "security.protocol": "sasl",
        "security.sasl.mechanism": "PLAIN",
        "security.sasl.username": "admin",
        "security.sasl.password": "admin-secret",
    })
    with pytest.raises(fluss.FlussError):
        await fluss.FlussConnection.create(config)
