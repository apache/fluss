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

"""Regression checks for licenses of code incorporated into Rust crates."""

import tempfile
import unittest
from pathlib import Path

import generate_binary_license as generator


class IncorporatedLicenseTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "LICENSE").write_text("Apache License\n" + "-" * 80 + "\n")

    def package(self, name, files, license_expression="MIT OR Apache-2.0"):
        directory = self.root / name
        directory.mkdir()
        for filename, contents in files.items():
            path = directory / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents)
        return {
            "name": name,
            "version": "1.0.0",
            "source": "registry",
            "manifest_path": str(directory / "Cargo.toml"),
            "license": license_expression,
        }

    def test_apache_choice_keeps_native_library_and_bindings_licenses(self):
        package = self.package("zstd-sys", {
            "LICENSE.BSD-3-Clause": "Bindings copyright and BSD terms",
            "zstd/LICENSE": "Zstandard copyright and BSD terms",
        })
        result = generator.generate_license(self.root, [package])
        self.assertIn("Bindings copyright and BSD terms", result)
        self.assertIn("Zstandard copyright and BSD terms", result)
        self.assertIn("Selected crate license: Apache-2.0", result)

    def test_apache_choice_keeps_unicode_data_license(self):
        package = self.package("regex-syntax", {
            "src/unicode_tables/LICENSE-UNICODE": "Unicode data copyright and terms",
        })
        result = generator.generate_license(self.root, [package])
        self.assertIn("Unicode data copyright and terms", result)

    def test_dependency_update_cannot_silently_drop_incorporated_license(self):
        package = self.package("zstd-sys", {"zstd/LICENSE": "BSD terms"})
        with self.assertRaisesRegex(RuntimeError, "Missing incorporated license"):
            generator.generate_license(self.root, [package])

    def test_mit_crate_still_requires_and_preserves_its_license(self):
        package = self.package("bytes", {"LICENSE": "Carl Lerche MIT terms"}, "MIT")
        result = generator.generate_license(self.root, [package])
        self.assertIn("Carl Lerche MIT terms", result)
        (Path(package["manifest_path"]).parent / "LICENSE").unlink()
        with self.assertRaisesRegex(RuntimeError, "Cannot locate the MIT text"):
            generator.generate_license(self.root, [package])


if __name__ == "__main__":
    unittest.main()
