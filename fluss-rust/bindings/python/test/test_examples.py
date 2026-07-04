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

"""Executability check for the example scripts.

Auto-discovers every ``example/*.py`` that exposes a callable ``main`` and runs
it against the shared test cluster, so an example that stops working fails CI.
Adding a new example file requires no changes here.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "example"


def _load_module(path: Path):
    spec = importlib.util.spec_from_file_location(f"example_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _discover_examples():
    # Examples import each other's siblings only via the package dir, and may
    # ship a shared "_"-prefixed helper; make the dir importable and skip those.
    if str(EXAMPLES_DIR) not in sys.path:
        sys.path.insert(0, str(EXAMPLES_DIR))
    params = []
    for path in sorted(EXAMPLES_DIR.glob("*.py")):
        if path.name.startswith("_"):
            continue
        module = _load_module(path)
        main = getattr(module, "main", None)
        if callable(main):
            params.append(pytest.param(main, id=path.stem))
    return params


EXAMPLES = _discover_examples()


def test_examples_discovered():
    assert EXAMPLES, f"No runnable examples found in {EXAMPLES_DIR}"


@pytest.mark.parametrize("example_main", EXAMPLES)
async def test_example_runs(example_main, plaintext_bootstrap_servers):
    await example_main(plaintext_bootstrap_servers)
