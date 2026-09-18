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

"""Give each TestPyPI candidate its own version before running maturin."""

import argparse
import re
from pathlib import Path


def set_release_version(pyproject: Path, tag: str) -> str:
    """Set PEP 440 RC metadata; final releases keep the Cargo-derived version."""
    match = re.fullmatch(r"v([0-9]+\.[0-9]+\.[0-9]+)(?:-rc\.?([0-9]+))?", tag)
    if match is None:
        raise ValueError(f"Expected vX.Y.Z or vX.Y.Z-rcN, got {tag!r}")

    version, candidate = match.groups()
    if candidate is None:
        return version

    version = f"{version}rc{int(candidate)}"
    content = pyproject.read_text(encoding="utf-8")
    # Keep the Rust workspace and lockfile at the final version. Static Python
    # metadata takes precedence in maturin and survives rebuilding the sdist.
    content, count = re.subn(
        r'^dynamic[ \t]*=[ \t]*\["version"\][ \t]*$',
        f'version = "{version}"',
        content,
        flags=re.MULTILINE,
    )
    if count != 1:
        raise ValueError(f"Expected exactly one dynamic version field in {pyproject}")
    pyproject.write_text(content, encoding="utf-8")
    return version


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag", help="Release tag already checked against Cargo.toml")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    version = set_release_version(
        root / "fluss-rust/bindings/python/pyproject.toml", args.tag
    )
    print(f"Python distribution version: {version}")


if __name__ == "__main__":
    main()
