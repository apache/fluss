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
#
# Release tooling: requires Python 3.11+ (constants.py uses tomllib).

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "This script requires Python 3.11 or newer (uses tomllib). "
        f"Current: {sys.version}. Use python3.11+ or see docs for release requirements."
    )

import subprocess
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

from constants import PACKAGES, ROOT_DIR


def check_single_package(root):
    pkg_dir = ROOT_DIR / root if root != "." else ROOT_DIR
    if (pkg_dir / "Cargo.toml").exists():
        print(f"Checking dependencies of {root}")
        subprocess.run(
            ["cargo", "deny", "check", "license"],
            cwd=pkg_dir,
            check=True,
        )
    else:
        print(f"Skipping {root} as Cargo.toml does not exist")


def check_deps():
    for d in PACKAGES:
        check_single_package(d)


def generate_single_package(root):
    pkg_dir = ROOT_DIR / root if root != "." else ROOT_DIR
    if (pkg_dir / "Cargo.toml").exists():
        print(f"Generating dependencies {root}")
        result = subprocess.run(
            ["cargo", "deny", "list", "-f", "tsv", "-t", "0.6"],
            cwd=pkg_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"cargo deny list failed in {root}: {result.stderr or result.stdout}"
            )
        out_file = pkg_dir / "DEPENDENCIES.rust.tsv"
        out_file.write_text(result.stdout)
    else:
        print(f"Skipping {root} as Cargo.toml does not exist")


def generate_deps():
    for d in PACKAGES:
        generate_single_package(d)


if __name__ == "__main__":
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.set_defaults(func=parser.print_help)
    subparsers = parser.add_subparsers()

    parser_check = subparsers.add_parser(
        "check", description="Check dependencies", help="Check dependencies"
    )
    parser_check.set_defaults(func=check_deps)

    parser_generate = subparsers.add_parser(
        "generate", description="Generate dependencies", help="Generate dependencies"
    )
    parser_generate.set_defaults(func=generate_deps)

    args = parser.parse_args()
    arg_dict = dict(vars(args))
    del arg_dict["func"]
    args.func(**arg_dict)
