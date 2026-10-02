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

set -o errexit
set -o nounset

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
FLUSS_HOME="${FLUSS_HOME:-$(cd -- "${SCRIPT_DIR}/.." && pwd -P)}"
FLUSS_CONF_DIR="${FLUSS_CONF_DIR:-${FLUSS_HOME}/conf}"
FLUSS_GATEWAY_CONFIG="${FLUSS_GATEWAY_CONFIG:-${FLUSS_CONF_DIR}/gateway.yaml}"
FLUSS_GATEWAY_BIN="${FLUSS_GATEWAY_BIN:-${FLUSS_HOME}/bin/fluss-gateway}"

has_config=false
for arg in "$@"; do
    if [[ "${arg}" == "--config" || "${arg}" == --config=* ]]; then
        has_config=true
        break
    fi
done

if [[ "${has_config}" == "true" ]]; then
    exec "${FLUSS_GATEWAY_BIN}" "$@"
fi

exec "${FLUSS_GATEWAY_BIN}" --config "${FLUSS_GATEWAY_CONFIG}" "$@"
