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

"""Generate legal files for the Rust code statically linked into Python wheels."""

import argparse
import sys
from pathlib import Path

PYTHON_DIR = Path(__file__).resolve().parent
REPOSITORY_ROOT = PYTHON_DIR.parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT / "tools" / "releasing"))
import generate_rust_license as generator

# Keep this list aligned with the wheel matrix in python-release.yml.
TARGETS = (
    "x86_64-pc-windows-msvc",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail on stale files")
    parser.add_argument(
        "--target",
        action="append",
        choices=TARGETS,
        help="wheel target; defaults to the union of release targets",
    )
    args = parser.parse_args()
    targets = args.target or TARGETS
    packages = generator.runtime_packages(PYTHON_DIR, targets, "fluss_python")
    description = (
        "This LICENSE covers Rust dependencies statically linked into the Python\n"
        "extension for: " + ", ".join(targets) + ".\n"
        "When multiple targets are listed, target-specific crates may only be linked\n"
        "into the corresponding wheels. Dynamically installed Python dependencies\n"
        "such as pandas and pyarrow carry their own licenses."
    )
    generator.write_generated_files(
        REPOSITORY_ROOT,
        {
            PYTHON_DIR / "LICENSE-bin": generator.generate_license(
                REPOSITORY_ROOT, packages, "The Python wheels", description
            ),
            PYTHON_DIR / "NOTICE-bin": generator.generate_notice(
                REPOSITORY_ROOT, packages, (PYTHON_DIR / "NOTICE").read_text()
            ),
        },
        args.check,
    )


if __name__ == "__main__":
    main()
