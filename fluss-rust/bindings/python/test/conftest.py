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
import math
import os
import subprocess
import tempfile
import time
from datetime import date, datetime, timezone
from datetime import time as dt_time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
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
                    await admin.list_offsets(
                        table_path, [0], fluss.OffsetSpec.earliest()
                    )
                return
            except (fluss.FlussError, Exception) as e:
                # Catch "No leader found" or other errors that indicate the table/partition is still initializing
                err_msg = str(e)
                if any(
                    msg in err_msg
                    for msg in [
                        "No leader found",
                        "Table not ready",
                        "Metadata not ready",
                        "not leader or follower",
                    ]
                ):
                    await asyncio.sleep(1)
                    continue
                raise
        raise TimeoutError(
            f"Table/Partition {table_path} ({partition_name or 'standard'}) "
            f"did not become ready within {timeout}s"
        )

    return _wait


# Complex-type (ARRAY/MAP/ROW) helpers shared by the KV and log
# all_complex_datatypes tests: schema sections plus the full/edge/null row matrix.
# Read-back shapes: ARRAY -> list, MAP -> list of (key, value) tuples, ROW -> dict.


def pa_row_seq_label() -> pa.DataType:
    return pa.struct([("seq", pa.int32()), ("label", pa.string())])


def pa_row_deep() -> pa.DataType:
    return pa.struct([("inner", pa.struct([("n", pa.int32())]))])


def pa_row_rich() -> pa.DataType:
    return pa.struct(
        [
            ("f_bool", pa.bool_()),
            ("f_int", pa.int32()),
            ("f_long", pa.int64()),
            ("f_float", pa.float32()),
            ("f_double", pa.float64()),
            ("f_str", pa.string()),
            ("f_bytes", pa.binary()),
            ("f_decimal", pa.decimal128(10, 2)),
            ("f_date", pa.date32()),
            ("f_time", pa.time32("ms")),
            ("f_ts_ntz", pa.timestamp("us")),
            ("f_ts_ltz", pa.timestamp("us", tz="UTC")),
            ("f_binary", pa.binary(4)),
            ("f_array_int", pa.list_(pa.int32())),
        ]
    )


def array_basics_fields() -> list:
    return [
        pa.field("arr_int", pa.list_(pa.int32())),
        pa.field("arr_string", pa.list_(pa.string())),
        pa.field("arr_of_arr", pa.list_(pa.list_(pa.int32()))),
        pa.field("arr_of_row", pa.list_(pa_row_seq_label())),
    ]


def row_basics_fields() -> list:
    return [
        pa.field("row_basic", pa_row_seq_label()),
        pa.field("row_deep", pa_row_deep()),
        pa.field("row_rich", pa_row_rich()),
    ]


def map_basics_fields() -> list:
    return [
        pa.field("map_string_int", pa.map_(pa.string(), pa.int32())),
        pa.field("map_of_row", pa.map_(pa.string(), pa_row_seq_label())),
        pa.field("map_of_map", pa.map_(pa.string(), pa.map_(pa.string(), pa.int32()))),
        pa.field("map_of_array", pa.map_(pa.string(), pa.list_(pa.int32()))),
        pa.field("array_of_map", pa.list_(pa.map_(pa.string(), pa.int32()))),
    ]


def array_rich_fields() -> list:
    return [
        pa.field("arr_bytes", pa.list_(pa.binary())),
        pa.field("arr_date", pa.list_(pa.date32())),
        pa.field("arr_time", pa.list_(pa.time32("ms"))),
        pa.field("arr_ts", pa.list_(pa.timestamp("us"))),
        pa.field("arr_ts_ltz", pa.list_(pa.timestamp("us", tz="UTC"))),
        pa.field("arr_decimal", pa.list_(pa.decimal128(10, 2))),
        pa.field("arr_decimal_big", pa.list_(pa.decimal128(22, 5))),
        pa.field("arr_float", pa.list_(pa.float32())),
        pa.field("arr_double", pa.list_(pa.float64())),
        pa.field("arr_binary", pa.list_(pa.binary(4))),
    ]


def map_rich_fields() -> list:
    return [
        pa.field("map_bytes", pa.map_(pa.string(), pa.binary())),
        pa.field("map_decimal", pa.map_(pa.string(), pa.decimal128(10, 2))),
        pa.field("map_date", pa.map_(pa.string(), pa.date32())),
        pa.field("map_time", pa.map_(pa.string(), pa.time32("ms"))),
        pa.field("map_ts", pa.map_(pa.string(), pa.timestamp("us"))),
        pa.field("map_ts_ltz", pa.map_(pa.string(), pa.timestamp("us", tz="UTC"))),
        pa.field("map_float", pa.map_(pa.string(), pa.float32())),
        pa.field("map_double", pa.map_(pa.string(), pa.float64())),
        pa.field("map_bool", pa.map_(pa.string(), pa.bool_())),
        pa.field("map_binary", pa.map_(pa.string(), pa.binary(4))),
        pa.field("map_int_key", pa.map_(pa.int32(), pa.string())),
    ]


def complex_fields() -> list:
    """`id` + all complex sections, in section order."""
    return (
        [pa.field("id", pa.int32())]
        + array_basics_fields()
        + array_rich_fields()
        + row_basics_fields()
        + map_basics_fields()
        + map_rich_fields()
    )


def complex_column_names() -> list:
    """All complex column names (everything except `id`)."""
    return [f.name for f in complex_fields() if f.name != "id"]


def complex_schema(primary_keys=None) -> "fluss.Schema":
    return fluss.Schema(pa.schema(complex_fields()), primary_keys=primary_keys)


_ROW_RICH_FULL = {
    "f_bool": True,
    "f_int": 100_000,
    "f_long": 9_876_543_210,
    "f_float": float("inf"),
    "f_double": math.pi,
    "f_str": "hello world",
    "f_bytes": b"binary",
    "f_decimal": Decimal("123.45"),
    "f_date": date(2026, 1, 23),
    "f_time": dt_time(10, 13, 47, 123000),
    "f_ts_ntz": datetime(2026, 1, 23, 10, 13, 47, 123456),
    "f_ts_ltz": datetime(2026, 1, 23, 10, 13, 47, 123456, tzinfo=timezone.utc),
    "f_binary": b"\x01\x02\x03\x04",
    "f_array_int": [7, None, 11],
}


def complex_full_row(id_: int) -> dict:
    """Fully-populated row exercising every complex shape (incl. nesting)."""
    return {
        "id": id_,
        "arr_int": [10, 20, 30],
        "arr_string": ["hello", "world"],
        "arr_of_arr": [[1, 2], [3, 4]],
        "arr_of_row": [{"seq": 1, "label": "open"}, {"seq": 2, "label": "close"}],
        "arr_bytes": [b"\x10\x20\x30", None],
        "arr_date": [date(2026, 1, 23), None],
        "arr_time": [dt_time(10, 13, 47, 123000), None],
        "arr_ts": [datetime(2026, 1, 23, 10, 13, 47, 123456)],
        "arr_ts_ltz": [datetime(2026, 1, 23, 10, 13, 47, 123456, tzinfo=timezone.utc)],
        "arr_decimal": [Decimal("123.45"), None],
        "arr_decimal_big": [
            Decimal("12345678901234567.12345"),
            Decimal("-99999999999999999.99999"),
        ],
        "arr_float": [float("nan"), float("inf"), float("-inf")],
        "arr_double": [float("nan"), float("inf"), float("-inf")],
        "arr_binary": [b"\xde\xad\xbe\xef", b"\x00\x01\x02\x03"],
        "row_basic": {"seq": 42, "label": "hello"},
        "row_deep": {"inner": {"n": 99}},
        "row_rich": dict(_ROW_RICH_FULL),
        "map_string_int": {"a": 1, "b": None, "c": 3},
        "map_of_row": {
            "e0": {"seq": 1, "label": "open"},
            "e1": {"seq": 2, "label": "close"},
        },
        "map_of_map": {"g1": {"a": 1, "b": 2}, "g2": {"c": 3}},
        "map_of_array": {"primes": [2, 3, 5], "squares": [1, 4]},
        "array_of_map": [{"x": 1, "y": 2}, {"z": 9}],
        "map_bytes": {"k": b"\x10\x20\x30"},
        "map_decimal": {"p": Decimal("123.45")},
        "map_date": {"d": date(2026, 1, 23)},
        "map_time": {"t": dt_time(10, 13, 47, 123000)},
        "map_ts": {"t": datetime(2026, 1, 23, 10, 13, 47, 123456)},
        "map_ts_ltz": {
            "t": datetime(2026, 1, 23, 10, 13, 47, 123456, tzinfo=timezone.utc)
        },
        "map_float": {"nan": float("nan"), "inf": float("inf"), "ninf": float("-inf")},
        "map_double": {"nan": float("nan"), "inf": float("inf"), "ninf": float("-inf")},
        "map_bool": {"t": True, "f": False},
        "map_binary": {"k": b"\x01\x02\x03\x04"},
        "map_int_key": {1: "one", 2: "two"},
    }


# Rich sections appear only in the full row; edge/null rows leave them NULL.
_RICH_COLUMNS = [f.name for f in array_rich_fields() + map_rich_fields()]


def complex_edge_row(id_: int) -> dict:
    """Edge cases: empty collections, null elements, null nested rows."""
    return {
        "id": id_,
        "arr_int": [],
        "arr_string": [None],
        "arr_of_arr": [[5], None, []],
        "arr_of_row": [{"seq": 7, "label": "x"}, None, {"seq": 8, "label": "y"}],
        "row_basic": None,
        "row_deep": None,
        "row_rich": None,
        "map_string_int": {},
        "map_of_row": {},
        "map_of_map": {},
        "map_of_array": {},
        "array_of_map": [],
        **{name: None for name in _RICH_COLUMNS},
    }


def complex_null_row(id_: int) -> dict:
    """Every complex column set to NULL."""
    return {"id": id_, **{name: None for name in complex_column_names()}}


def _map(value) -> dict:
    """A read-back MAP is a list of (key, value) tuples; turn it into a dict for
    order-independent comparison (test maps use unique scalar keys)."""
    return dict(value)


def _assert_float_triplet(values) -> None:
    """Assert a 3-element sequence holds [NaN, +Inf, -Inf]."""
    assert math.isnan(values[0])
    assert math.isinf(values[1]) and values[1] > 0
    assert math.isinf(values[2]) and values[2] < 0


def assert_complex_full(row: dict) -> None:
    assert row["arr_int"] == [10, 20, 30]
    assert row["arr_string"] == ["hello", "world"]
    assert row["arr_of_arr"] == [[1, 2], [3, 4]]
    assert row["arr_of_row"] == [
        {"seq": 1, "label": "open"},
        {"seq": 2, "label": "close"},
    ]
    assert row["row_basic"] == {"seq": 42, "label": "hello"}
    assert row["row_deep"] == {"inner": {"n": 99}}
    rr = row["row_rich"]
    assert rr["f_bool"] is True
    assert rr["f_int"] == 100_000
    assert rr["f_long"] == 9_876_543_210
    assert math.isinf(rr["f_float"]) and rr["f_float"] > 0
    assert math.isclose(rr["f_double"], math.pi, rel_tol=1e-15)
    assert rr["f_str"] == "hello world"
    assert rr["f_bytes"] == b"binary"
    assert rr["f_decimal"] == Decimal("123.45")
    assert rr["f_date"] == date(2026, 1, 23)
    assert rr["f_time"] == dt_time(10, 13, 47, 123000)
    assert rr["f_ts_ntz"] == datetime(2026, 1, 23, 10, 13, 47, 123456)
    assert rr["f_ts_ltz"] == datetime(
        2026, 1, 23, 10, 13, 47, 123456, tzinfo=timezone.utc
    )
    assert rr["f_binary"] == b"\x01\x02\x03\x04"
    assert rr["f_array_int"] == [7, None, 11]
    assert _map(row["map_string_int"]) == {"a": 1, "b": None, "c": 3}
    assert _map(row["map_of_row"]) == {
        "e0": {"seq": 1, "label": "open"},
        "e1": {"seq": 2, "label": "close"},
    }
    assert {k: _map(v) for k, v in row["map_of_map"]} == {
        "g1": {"a": 1, "b": 2},
        "g2": {"c": 3},
    }
    assert _map(row["map_of_array"]) == {"primes": [2, 3, 5], "squares": [1, 4]}
    assert [_map(m) for m in row["array_of_map"]] == [{"x": 1, "y": 2}, {"z": 9}]
    assert row["arr_bytes"] == [b"\x10\x20\x30", None]
    assert row["arr_date"] == [date(2026, 1, 23), None]
    assert row["arr_time"] == [dt_time(10, 13, 47, 123000), None]
    assert row["arr_ts"] == [datetime(2026, 1, 23, 10, 13, 47, 123456)]
    assert row["arr_ts_ltz"] == [
        datetime(2026, 1, 23, 10, 13, 47, 123456, tzinfo=timezone.utc)
    ]
    assert row["arr_decimal"] == [Decimal("123.45"), None]
    assert row["arr_decimal_big"] == [
        Decimal("12345678901234567.12345"),
        Decimal("-99999999999999999.99999"),
    ]
    _assert_float_triplet(row["arr_float"])
    _assert_float_triplet(row["arr_double"])
    assert row["arr_binary"] == [b"\xde\xad\xbe\xef", b"\x00\x01\x02\x03"]
    assert _map(row["map_bytes"]) == {"k": b"\x10\x20\x30"}
    assert _map(row["map_decimal"]) == {"p": Decimal("123.45")}
    assert _map(row["map_date"]) == {"d": date(2026, 1, 23)}
    assert _map(row["map_time"]) == {"t": dt_time(10, 13, 47, 123000)}
    assert _map(row["map_ts"]) == {"t": datetime(2026, 1, 23, 10, 13, 47, 123456)}
    assert _map(row["map_ts_ltz"]) == {
        "t": datetime(2026, 1, 23, 10, 13, 47, 123456, tzinfo=timezone.utc)
    }
    mf = _map(row["map_float"])
    _assert_float_triplet([mf["nan"], mf["inf"], mf["ninf"]])
    md = _map(row["map_double"])
    _assert_float_triplet([md["nan"], md["inf"], md["ninf"]])
    assert _map(row["map_bool"]) == {"t": True, "f": False}
    assert _map(row["map_binary"]) == {"k": b"\x01\x02\x03\x04"}
    assert _map(row["map_int_key"]) == {1: "one", 2: "two"}


def assert_complex_edge(row: dict) -> None:
    assert row["arr_int"] == []
    assert row["arr_string"] == [None]
    assert row["arr_of_arr"] == [[5], None, []]
    assert row["arr_of_row"] == [
        {"seq": 7, "label": "x"},
        None,
        {"seq": 8, "label": "y"},
    ]
    assert row["row_basic"] is None
    assert row["row_deep"] is None
    assert row["row_rich"] is None
    # empty maps read back as empty lists
    assert row["map_string_int"] == []
    assert row["map_of_row"] == []
    assert row["map_of_map"] == []
    assert row["map_of_array"] == []
    assert row["array_of_map"] == []
    for name in _RICH_COLUMNS:
        assert row[name] is None, f"{name} should be null in the edge row"
