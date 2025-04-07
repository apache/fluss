#!/bin/bash
#
# Copyright (c) 2025 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

COMMON_CONF_FILE="${FLUSS_HOME}/conf/common.yaml"

prepare_configuration() {
    additional_conf_file="${FLUSS_HOME}/conf/$1"

    if [ -n "${FLUSS_PROPERTIES}" ]; then
        # copy over all configuration options of FLUSS_PROPERTIES to the common and the additional configuration file
        # since we cannot tell which configuration options are specific and which are not
        echo "#==============================================================================" | tee -a "${COMMON_CONF_FILE}" "$additional_conf_file"
        echo "# Configuration Options from FLUSS_PROPERTIES Environment Variable" | tee -a "${COMMON_CONF_FILE}" "$additional_conf_file"
        echo "#=============================================================================="  | tee -a "${COMMON_CONF_FILE}" "$additional_conf_file"
        echo "${FLUSS_PROPERTIES}" | tee -a "${COMMON_CONF_FILE}" "$additional_conf_file"
    fi
    envsubst < "${COMMON_CONF_FILE}" > "${COMMON_CONF_FILE}.tmp" && mv "${COMMON_CONF_FILE}.tmp" "${COMMON_CONF_FILE}"
    envsubst < "$additional_conf_file" > "$additional_conf_file.tmp" && mv "$additional_conf_file.tmp" "$additional_conf_file"
}

args=("$@")

if [ "$1" = "help" ]; then
  printf "Usage: $(basename "$0") (coordinatorServer|tabletServer)\n"
  printf "    Or $(basename "$0") help\n\n"
  exit 0
elif [ "$1" = "coordinatorServer" ]; then
  prepare_configuration "coordinator-server.yaml"
  args=("${args[@]:1}")
  echo "Starting Coordinator Server"
  exec "$FLUSS_HOME/bin/coordinator-server.sh" start-foreground "${args[@]}"
elif [ "$1" = "tabletServer" ]; then
  prepare_configuration "tablet-server.yaml"
  args=("${args[@]:1}")
  echo "Starting Tablet Server"
  exec "$FLUSS_HOME/bin/tablet-server.sh" start-foreground "${args[@]}"
fi

args=("${args[@]}")

## Running command in pass-through mode
exec "${args[@]}"