# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Smoke checks for DevKit profiles using the examples in this directory."""

import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
import unittest
from urllib.request import Request, urlopen


DEVKIT_DIR = Path(__file__).resolve().parent
ACTIVE_PROFILE = DEVKIT_DIR / ".deps" / "active-profile"


def run(*args, capture=False, check=True):
    print("+ " + shlex.join(args), flush=True)
    result = subprocess.run(
        args,
        cwd=DEVKIT_DIR,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )
    if capture:
        print(result.stdout, end="", flush=True)
    if check and result.returncode:
        raise AssertionError("Command exited with status %s: %s" % (result.returncode, args))
    return result


class ProfileSmoke(unittest.TestCase):
    profile = None

    @classmethod
    def setUpClass(cls):
        if ACTIVE_PROFILE.exists() and ACTIVE_PROFILE.stat().st_size:
            raise RuntimeError("An active DevKit profile exists. Run 'just clean' first.")
        cls.addClassCleanup(run, "just", "clean")
        run("just", "build", cls.profile)
        run("just", "up", cls.profile)

    def sql(self, path):
        return run("just", "run-sql", path, capture=True).stdout

    def wait_for_sql(self, path, *patterns):
        for attempt in range(30):
            result = run("just", "run-sql", path, capture=True, check=False)
            if result.returncode == 0 and all(
                re.search(pattern, result.stdout) for pattern in patterns
            ):
                return
            if attempt < 29:
                time.sleep(2)
        run("just", "logs", "tiering-submit", "200", check=False)
        self.fail("Lake query did not return the expected row: %s" % path)


class CoreSmoke(ProfileSmoke):
    profile = "core"

    def test_batch_scan(self):
        self.sql("examples/core/setup.sql")
        output = self.sql("examples/core/batch-scan.sql")
        self.assertRegex(
            output,
            r"(?m)^\|[ \t]*1[ \t]*\|[ \t]*10[ \t]*\|"
            r"[ \t]*login[ \t]*\|[ \t]*alpha[ \t]*\|"
            r"[ \t]*2026-01-01 00:00:00\.000[ \t]*\|$",
        )
        self.assertRegex(
            output,
            r"(?m)^\|[ \t]*10[ \t]*\|[ \t]*100[ \t]*\|"
            r"[ \t]*Alice[ \t]*\|[ \t]*gold[ \t]*\|$",
        )


class GatewaySmoke(ProfileSmoke):
    profile = "gateway"

    @staticmethod
    def request(method, path, body=None):
        port = os.environ.get("GATEWAY_REST_PORT", "8080")
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(
            "http://localhost:%s/v1/clusters/default/databases/fluss/%s" % (port, path),
            data=payload,
            headers={"Content-Type": "application/json"},
            method=method,
        )
        with urlopen(request, timeout=10) as response:
            content = response.read()
        return json.loads(content) if content else None

    def test_create_and_append(self):
        self.request(
            "POST",
            "tables",
            {
                "table_name": "gateway_events",
                "columns": [
                    {"name": "event_id", "data_type": {"type": "BIGINT"}, "nullable": False},
                    {"name": "message", "data_type": {"type": "STRING"}, "nullable": False},
                ],
                "distribution": {"bucket_count": 1, "bucket_keys": []},
            },
        )
        table = self.request("GET", "tables/gateway_events")
        self.assertEqual((table["database"], table["table"]), ("fluss", "gateway_events"))
        self.assertEqual(
            [
                (column["name"], column["data_type"]["type"], column["nullable"])
                for column in table["columns"]
            ],
            [("event_id", "BIGINT", False), ("message", "STRING", False)],
        )

        result = self.request(
            "POST",
            "tables/gateway_events/records",
            {
                "entries": [
                    {"id": "event-1", "append": {"event_id": "1", "message": "hello from Gateway"}}
                ]
            },
        )
        self.assertEqual((result["success_count"], result["error_count"]), (1, 0))
        self.assertEqual(len(result["successes"]), 1)
        self.assertEqual(result["successes"][0]["id"], "event-1")


class PaimonLakeSmoke(ProfileSmoke):
    profile = "paimon"

    def test_lake_read(self):
        self.sql("examples/lake/paimon/setup.sql")
        output = self.sql("examples/lake/paimon/union-read.sql")
        self.assertRegex(
            output, r"(?m)^\|[ \t]*1[ \t]*\|[ \t]*login[ \t]*\|[ \t]*alpha[ \t]*\|$"
        )
        self.assertRegex(
            output, r"(?m)^\|[ \t]*1[ \t]*\|[ \t]*Alice[ \t]*\|[ \t]*gold[ \t]*\|$"
        )
        self.wait_for_sql(
            "examples/lake/paimon/lake-only-read.sql",
            r"(?m)^\|[ \t]*1[ \t]*\|[ \t]*Alice[ \t]*\|[ \t]*gold[ \t]*\|$",
            r"(?m)^\|[ \t]*2[ \t]*\|[ \t]*Bob[ \t]*\|[ \t]*silver[ \t]*\|$",
        )


class IcebergLakeSmoke(ProfileSmoke):
    profile = "iceberg"

    def test_lake_read(self):
        self.sql("examples/lake/iceberg/create-table.sql")
        self.sql("examples/lake/iceberg/write-data.sql")
        self.wait_for_sql(
            "examples/lake/iceberg/query-lake.sql",
            r"(?m)^\|[ \t]*3[ \t]*\|[ \t]*6[ \t]*\|[ \t]*3[ \t]*\|"
            r"[ \t]*alpha[ \t]*\|[ \t]*gamma[ \t]*\|$",
        )


class LanceLakeSmoke(ProfileSmoke):
    profile = "lance"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        run(sys.executable, "-m", "pip", "install", "--quiet", "pylance==0.33.0")

    def test_lance_read(self):
        self.sql("examples/lake/lance/create-table.sql")
        self.sql("examples/lake/lance/write-data.sql")
        run(sys.executable, "examples/lake/lance/read-data.py")


if __name__ == "__main__":
    unittest.main()
