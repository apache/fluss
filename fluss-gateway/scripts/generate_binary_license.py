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

"""Generate LICENSE-bin and NOTICE-bin for the shipped Linux Gateway binary."""

import argparse
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT / "tools" / "releasing"))
import generate_rust_license as generator

TARGETS = ("x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu")


def generate_license(repository_root, packages):
    return generator.generate_license(
        repository_root,
        packages,
        "The supported Linux Gateway binaries",
        "This LICENSE covers the supported amd64 and arm64 Linux Gateway\n"
        "convenience binaries. Their linked dependency sets are almost identical,\n"
        "but a target-specific crate may appear here even when it is not linked into\n"
        "the other architecture's executable.",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail on stale files")
    args = parser.parse_args()
    gateway_dir = REPOSITORY_ROOT / "fluss-gateway"
    packages = generator.runtime_packages(gateway_dir, TARGETS, "fluss-gateway")
    generator.write_generated_files(
        REPOSITORY_ROOT,
        {
            gateway_dir / "LICENSE-bin": generate_license(REPOSITORY_ROOT, packages),
            gateway_dir / "NOTICE-bin": generator.generate_notice(
                REPOSITORY_ROOT, packages
            ),
        },
        args.check,
    )


if __name__ == "__main__":
    main()
