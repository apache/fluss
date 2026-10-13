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

"""Check workspace root and target selection against real Cargo metadata."""

import subprocess
import tempfile
import unittest
from pathlib import Path

import generate_rust_license as generator


class RuntimeClosureTest(unittest.TestCase):
    def test_only_selected_roots_normal_target_dependencies_are_included(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = [
                "binding",
                "runtime",
                "build_only",
                "dev_only",
                "derive",
                "windows_only",
                "sibling",
            ]
            (root / "Cargo.toml").write_text(
                '[workspace]\nresolver = "2"\nmembers = ['
                + ", ".join('"' + name + '"' for name in names)
                + "]\n"
            )
            for name in names:
                package = root / name
                (package / "src").mkdir(parents=True)
                (package / "src/lib.rs").write_text("")
                manifest = '[package]\nname = "' + name + '"\nversion = "0.1.0"\n'
                if name == "derive":
                    manifest += "[lib]\nproc-macro = true\n"
                if name == "binding":
                    manifest += (
                        "[dependencies]\n"
                        'runtime = { path = "../runtime" }\n'
                        'derive = { path = "../derive" }\n'
                        "[build-dependencies]\n"
                        'build_only = { path = "../build_only" }\n'
                        "[dev-dependencies]\n"
                        'dev_only = { path = "../dev_only" }\n'
                        "[target.'cfg(windows)'.dependencies]\n"
                        'windows_only = { path = "../windows_only" }\n'
                    )
                (package / "Cargo.toml").write_text(manifest)
            # A lockfile for path-only crates requires no registry access.
            subprocess.run(
                [
                    "cargo",
                    "generate-lockfile",
                    "--offline",
                    "--manifest-path",
                    str(root / "Cargo.toml"),
                ],
                check=True,
            )
            linux = generator.runtime_packages(
                root, ["x86_64-unknown-linux-gnu"], "binding"
            )
            self.assertEqual(
                {package["name"] for package in linux}, {"binding", "runtime"}
            )
            windows = generator.runtime_packages(
                root, ["x86_64-pc-windows-msvc"], "binding"
            )
            self.assertEqual(
                {package["name"] for package in windows},
                {"binding", "runtime", "windows_only"},
            )
            with self.assertRaisesRegex(RuntimeError, "Expected one local package"):
                generator.runtime_packages(
                    root, ["x86_64-unknown-linux-gnu"], "missing"
                )


if __name__ == "__main__":
    unittest.main()
