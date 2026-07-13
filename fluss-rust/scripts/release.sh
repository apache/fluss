#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Create ASF source release artifacts under dist/ (aligned with Fluss release package format):
#   fluss-rust-{version}-incubating.tgz
#   fluss-rust-{version}-incubating.tgz.asc
#   fluss-rust-{version}-incubating.tgz.sha512
# (Incubator policy requires "incubating" in the artifact name.)
# Run from repo root. Check out the release tag first (e.g. git checkout v0.1.0-rc1).
# Usage: ./scripts/release.sh [version]
#   If version is omitted, it is read from Cargo.toml (workspace.package.version).

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [ -n "$1" ]; then
  VERSION="$1"
else
  VERSION=$(grep -E '^version\s*=' Cargo.toml | head -1 | sed 's/.*"\([^"]*\)".*/\1/')
  if [ -z "$VERSION" ]; then
    echo "Could not read version from Cargo.toml. Pass version as argument: $0 <version>"
    exit 1
  fi
fi

PREFIX="fluss-rust-${VERSION}-incubating"
DIST_DIR="${REPO_ROOT}/dist"
TARBALL="${PREFIX}.tgz"

echo "Creating ASF source release for fluss-rust ${VERSION}"
mkdir -p "$DIST_DIR"

echo "Creating source archive: ${TARBALL}"
git archive --format=tar.gz --prefix="${PREFIX}/" -o "${DIST_DIR}/${TARBALL}" HEAD

echo "Generating SHA-512 checksum: ${TARBALL}.sha512"
if command -v shasum >/dev/null 2>&1; then
  (cd "$DIST_DIR" && shasum -a 512 "$TARBALL" > "${TARBALL}.sha512")
else
  (cd "$DIST_DIR" && sha512sum "$TARBALL" > "${TARBALL}.sha512")
fi

echo "Signing with GPG: ${TARBALL}.asc"
(cd "$DIST_DIR" && gpg --armor --detach-sig "$TARBALL")

echo "Verifying signature"
(cd "$DIST_DIR" && gpg --verify "${TARBALL}.asc" "$TARBALL")

echo "Done. Artifacts in dist/:"
ls -la "${DIST_DIR}/"
echo ""
echo "Next: upload contents of dist/ to SVN (see docs/creating-a-release.md)."
