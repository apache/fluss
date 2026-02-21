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

# Derives the BINARY distribution NOTICE from the root NOTICE by removing the
# Maven Wrapper section (binary packages do not contain mvnw).
# Single source of truth: edit only the root NOTICE file.
#
# Usage: derive_binary_notice.sh [ROOT_NOTICE]
#   ROOT_NOTICE defaults to ${FLUSS_DIR}/NOTICE if FLUSS_DIR is set,
#   otherwise the script dir's ../../NOTICE (project root).
# Output: binary NOTICE to stdout.

set -Eeuo pipefail

DIR=$(dirname "$0")
FLUSS_ROOT="${FLUSS_DIR:-$(cd "${DIR}/../.." && pwd)}"
ROOT_NOTICE="${1:-${FLUSS_ROOT}/NOTICE}"

if [ ! -f "${ROOT_NOTICE}" ]; then
  echo "ERROR: NOTICE file not found: ${ROOT_NOTICE}" >&2
  exit 1
fi

# Skip the block from "This product contains code from the Apache Maven Wrapper"
# through the next line that is exactly "----------------------------------------------------------"
awk '
  /This product contains code from the Apache Maven Wrapper Project:/ { skip=1; next }
  skip && /^----------------------------------------------------------$/ { skip=0; next }
  !skip { print }
' "${ROOT_NOTICE}"
