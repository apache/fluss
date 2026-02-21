#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Derives the BINARY distribution LICENSE from the root LICENSE by removing the
# Maven Wrapper section (binary packages do not contain mvnw).
# Single source of truth: edit only the root LICENSE file.
#
# Usage: derive_binary_license.sh [ROOT_LICENSE]
#   ROOT_LICENSE defaults to ${FLUSS_DIR}/LICENSE if FLUSS_DIR is set,
#   otherwise the script dir's ../../LICENSE (project root).
# Output: binary LICENSE to stdout.

set -Eeuo pipefail

DIR=$(dirname "$0")
FLUSS_ROOT="${FLUSS_DIR:-$(cd "${DIR}/../.." && pwd)}"
ROOT_LICENSE="${1:-${FLUSS_ROOT}/LICENSE}"

if [ ! -f "${ROOT_LICENSE}" ]; then
  echo "ERROR: LICENSE file not found: ${ROOT_LICENSE}" >&2
  exit 1
fi

# Remove the Maven Wrapper block (last 3 lines: "Apache Maven Wrapper", "./mvnw", "./mvnw.cmd")
awk '
  /^Apache Maven Wrapper$/ { skip=2; next }
  skip > 0 { skip--; next }
  { print }
' "${ROOT_LICENSE}"
