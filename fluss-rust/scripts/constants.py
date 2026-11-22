#!/usr/bin/env python3
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

import tomllib
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent


def list_packages():
    """Package directories from [workspace].members in root Cargo.toml, plus workspace root.
    Each gets a DEPENDENCIES.rust.tsv. Avoids scanning target/, .git/, etc.
    Requires Python 3.11+ (tomllib).
    """
    root_cargo = ROOT_DIR / "Cargo.toml"
    if not root_cargo.exists():
        return ["."]
    with open(root_cargo, "rb") as f:
        data = tomllib.load(f)
    members = data.get("workspace", {}).get("members", [])
    if not isinstance(members, list):
        return ["."]
    packages = ["."]
    for m in members:
        if isinstance(m, str) and m:
            packages.append(m)
    return packages


PACKAGES = list_packages()
