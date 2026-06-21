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

import asyncio
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
import pytest_asyncio
from filelock import FileLock

import fluss

CLUSTER_NAME = "shared-test"


def _find_cli_binary():
    env_bin = os.environ.get("FLUSS_TEST_CLUSTER_BIN")
    if env_bin:
        if os.path.isfile(env_bin):
            return env_bin
        raise FileNotFoundError(f"FLUSS_TEST_CLUSTER_BIN={env_bin!r} does not exist")
    result = subprocess.run(
        ["cargo", "locate-project", "--workspace", "--message-format", "plain"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        root = Path(result.stdout.strip()).parent
        for profile in ("debug", "release"):
            bin_path = root / "target" / profile / "fluss-test-cluster"
            if bin_path.is_file():
                return str(bin_path)
    raise FileNotFoundError(
        "fluss-test-cluster not found. Run: cargo build -p fluss-test-cluster"
    )


def _start_cluster():
    lock = Path(tempfile.gettempdir()) / f"fluss-{CLUSTER_NAME}.lock"
    with FileLock(lock):
        cli = _find_cli_binary()
        result = subprocess.run(
            [cli, "start", "--sasl", "--name", CLUSTER_NAME],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"fluss-test-cluster start failed:\n{result.stderr}\n{result.stdout}"
            )
        prefix = "CLUSTER_JSON: "
        for line in result.stdout.strip().split("\n"):
            if line.startswith(prefix):
                info = json.loads(line[len(prefix) :])
                return info["bootstrap_servers"], info.get("sasl_bootstrap_servers")
        raise RuntimeError(
            f"No CLUSTER_JSON token in output:\n{result.stdout}\n{result.stderr}"
        )


def _stop_cluster():
    try:
        cli = _find_cli_binary()
    except FileNotFoundError:
        return
    subprocess.run([cli, "stop", "--name", CLUSTER_NAME], capture_output=True)


async def _connect(bootstrap_servers):
    config = fluss.Config({"bootstrap.servers": bootstrap_servers})
    start = time.time()
    last_err = None
    while time.time() - start < 60:
        try:
            conn = await fluss.FlussConnection.create(config)
            admin = conn.get_admin()
            nodes = await admin.get_server_nodes()
            if any(n.server_type == "TabletServer" for n in nodes):
                return conn
            await conn.close()
            last_err = RuntimeError("No TabletServer available yet")
        except Exception as e:
            last_err = e
        await asyncio.sleep(1)
    raise RuntimeError(f"Could not connect after 60s: {last_err}")


def pytest_unconfigure(config):
    if os.environ.get("FLUSS_BOOTSTRAP_SERVERS"):
        return
    if hasattr(config, "workerinput"):
        return
    if os.environ.get("FLUSS_SKIP_CLUSTER_TEARDOWN"):
        return
    _stop_cluster()


@pytest.fixture(scope="session")
def fluss_cluster():
    env = os.environ.get("FLUSS_BOOTSTRAP_SERVERS")
    if env:
        sasl_env = os.environ.get("FLUSS_SASL_BOOTSTRAP_SERVERS", env)
        yield (env, sasl_env)
        return

    plaintext_addr, sasl_addr = _start_cluster()
    yield (plaintext_addr, sasl_addr or plaintext_addr)


@pytest_asyncio.fixture(scope="session")
async def connection(fluss_cluster):
    plaintext_addr, _sasl_addr = fluss_cluster
    conn = await _connect(plaintext_addr)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def sasl_bootstrap_servers(fluss_cluster):
    _plaintext_addr, sasl_addr = fluss_cluster
    return sasl_addr


@pytest.fixture(scope="session")
def plaintext_bootstrap_servers(fluss_cluster):
    plaintext_addr, _sasl_addr = fluss_cluster
    return plaintext_addr


@pytest_asyncio.fixture(scope="session")
async def admin(connection):
    return connection.get_admin()


@pytest_asyncio.fixture
async def wait_for_table_ready(admin):
    """
    Fixture that returns a helper function to wait for a table or partition to be ready.
    """
    async def _wait(table_path, timeout=15, partition_name=None):
        start_time = time.monotonic()
        while time.monotonic() - start_time < timeout:
            try:
                if partition_name:
                    await admin.list_partition_offsets(
                        table_path, partition_name, [0], fluss.OffsetSpec.earliest()
                    )
                else:
                    await admin.list_offsets(table_path, [0], fluss.OffsetSpec.earliest())
                return
            except (fluss.FlussError, Exception) as e:
                # Catch "No leader found" or other errors that indicate the table/partition is still initializing
                err_msg = str(e)
                if any(msg in err_msg for msg in ["No leader found", "Table not ready", "Metadata not ready", "not leader or follower"]):
                    await asyncio.sleep(1)
                    continue
                raise
        raise TimeoutError(
            f"Table/Partition {table_path} ({partition_name or 'standard'}) "
            f"did not become ready within {timeout}s"
        )

    return _wait
