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
# Vendor the canonical FlussApi.proto into the fluss-rs crate so `cargo publish`
# produces a self-contained crate. build.rs prefers crates/fluss/proto/FlussApi.proto
# when present, otherwise reads it from ../../../fluss-rpc in the monorepo.
#
# Usage:
#   scripts/vendor-proto.sh           # copy canonical proto into the crate
#   scripts/vendor-proto.sh --clean   # remove the vendored copy

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CANONICAL="${REPO_ROOT}/../fluss-rpc/src/main/proto/FlussApi.proto"
DEST_DIR="${REPO_ROOT}/crates/fluss/proto"
DEST="${DEST_DIR}/FlussApi.proto"

if [ "${1:-}" = "--clean" ]; then
  rm -rf "$DEST_DIR"
  echo "Removed vendored proto: ${DEST_DIR}"
  exit 0
fi

if [ ! -f "$CANONICAL" ]; then
  echo "Canonical proto not found: ${CANONICAL}" >&2
  echo "Run from the consolidated repo (fluss-rpc must be a sibling of fluss-rust)." >&2
  exit 1
fi

mkdir -p "$DEST_DIR"
cp "$CANONICAL" "$DEST"
echo "Vendored ${CANONICAL} -> ${DEST}"
