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

import difflib
import subprocess
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from functools import cache

from constants import PACKAGES, ROOT_DIR

# Must match the pin in .github/workflows/rust-license-and-format.yml: the
# generated report differs between cargo-deny versions.
CARGO_DENY_VERSION = "0.20.2"


@cache
def verify_cargo_deny():
    output = subprocess.check_output(
        ["cargo", "deny", "--version"], cwd=ROOT_DIR, text=True
    ).strip()
    actual = output.rsplit(" ", 1)[-1]
    if actual != CARGO_DENY_VERSION:
        raise RuntimeError(
            f"cargo-deny {CARGO_DENY_VERSION} is required, found {output!r}"
        )


def package_dir(root):
    return ROOT_DIR / root if root != "." else ROOT_DIR


def cargo_deny(root, *args, capture_output=False):
    verify_cargo_deny()
    return subprocess.run(
        ["cargo", "deny", "--locked", "--all-features", *args],
        cwd=package_dir(root),
        check=not capture_output,
        capture_output=capture_output,
        text=True,
    )


def output_file(root):
    return package_dir(root) / "DEPENDENCIES.rust.tsv"


def dependency_report(root):
    result = cargo_deny(root, "list", "-f", "tsv", "-t", "0.6", capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"cargo deny list failed in {root}: {result.stderr or result.stdout}"
        )
    return result.stdout


def for_each_package(action):
    for root in PACKAGES:
        if (package_dir(root) / "Cargo.toml").exists():
            action(root)
        else:
            print(f"Skipping {root} as Cargo.toml does not exist")


def check_single_package(root):
    print(f"Checking dependencies of {root}")
    cargo_deny(root, "check", "licenses")


def check_deps():
    for_each_package(check_single_package)


def generate_single_package(root):
    print(f"Generating dependencies {root}")
    output_file(root).write_text(dependency_report(root))


def generate_deps():
    for_each_package(generate_single_package)


def verify_deps():
    stale = []

    def verify_single_package(root):
        out_file = output_file(root)
        expected = dependency_report(root)
        if not out_file.exists():
            print(f"Missing dependency report: {out_file.relative_to(ROOT_DIR)}")
            stale.append(root)
            return
        actual = out_file.read_text()
        if actual == expected:
            return
        stale.append(root)
        print(f"Stale dependency report: {out_file.relative_to(ROOT_DIR)}")
        diff = difflib.unified_diff(
            actual.splitlines(),
            expected.splitlines(),
            fromfile=str(out_file.relative_to(ROOT_DIR)),
            tofile="regenerated",
            lineterm="",
        )
        for line in list(diff)[:100]:
            print(line)

    for_each_package(verify_single_package)
    if stale:
        raise SystemExit(
            "dependency reports are missing or stale; "
            "run `python3 scripts/dependencies.py generate` and commit the result"
        )


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

    parser_verify = subparsers.add_parser(
        "verify",
        description="Verify the checked-in dependency reports",
        help="Verify the checked-in dependency reports",
    )
    parser_verify.set_defaults(func=verify_deps)

    args = parser.parse_args()
    arg_dict = dict(vars(args))
    del arg_dict["func"]
    args.func(**arg_dict)
