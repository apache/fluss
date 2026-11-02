#!/bin/bash
#
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
#

set -o errexit
set -o nounset
set -o pipefail

TIERING_CONFIG_FILE="${TIERING_CONFIG_FILE:-${FLUSS_HOME}/conf/tiering.yaml}"
RUNTIME_CONFIG_FILE="/tmp/fluss-tiering/tiering.yaml"

prepare_configuration() {
    if [ ! -f "${TIERING_CONFIG_FILE}" ]; then
        echo "The tiering configuration file ${TIERING_CONFIG_FILE} does not exist." >&2
        echo "Mount a tiering.yaml at ${TIERING_CONFIG_FILE} or point TIERING_CONFIG_FILE at one." >&2
        exit 1
    fi
    # Stage the configuration in a writable location: the source file can be
    # baked into the image or mounted read-only from a ConfigMap, while
    # FLUSS_TIERING_PROPERTIES lines and ${VAR} substitutions modify the copy.
    mkdir -p "$(dirname "${RUNTIME_CONFIG_FILE}")"
    cp "${TIERING_CONFIG_FILE}" "${RUNTIME_CONFIG_FILE}"
    if [ -n "${FLUSS_TIERING_PROPERTIES:-}" ]; then
        printf '\n%s\n' "${FLUSS_TIERING_PROPERTIES}" >> "${RUNTIME_CONFIG_FILE}"
    fi
    # Expand only ${VAR}, leaving bare $VAR and ${provider:env:KEY} untouched.
    # Concatenation preserves environment values literally, without replacement
    # metacharacters or recursive expansion. GlobalConfiguration treats everything
    # from the first # as a comment, so only scan the preceding text.
    awk '
        {
            comment_start = index($0, "#")
            comment = comment_start ? substr($0, comment_start) : ""
            remaining = comment_start ? substr($0, 1, comment_start - 1) : $0
            expanded = ""
            while (match(remaining, /\$\{[A-Za-z_][A-Za-z0-9_]*\}/)) {
                name = substr(remaining, RSTART + 2, RLENGTH - 3)
                value = ENVIRON[name]
                # Empty required settings must fail before the service starts.
                if (value == "") {
                    printf "The tiering configuration references the environment variable %s, which is not set.\n", name > "/dev/stderr"
                    printf "Set %s or remove the reference from the tiering configuration.\n", name > "/dev/stderr"
                    exit 1
                }
                expanded = expanded substr(remaining, 1, RSTART - 1) value
                remaining = substr(remaining, RSTART + RLENGTH)
            }
            print expanded remaining comment
        }
    ' "${RUNTIME_CONFIG_FILE}" > "${RUNTIME_CONFIG_FILE}.tmp" \
        && mv "${RUNTIME_CONFIG_FILE}.tmp" "${RUNTIME_CONFIG_FILE}"
}

args=("$@")

if [ "${1:-}" = "help" ]; then
  printf "Usage: $(basename "$0") (tieringService)\n"
  printf "    Or $(basename "$0") help\n\n"
  exit 0
elif [ "${1:-}" = "tieringService" ]; then
  args=("${args[@]:1}")
  echo "Starting the embedded Fluss lake tiering service"
  prepare_configuration

  # The embedded Flink runtime runs in this single JVM. Heap and direct memory
  # together must stay below the container memory limit to leave headroom for
  # metaspace and native memory, unless the operator configured the JVM
  # explicitly via JAVA_TOOL_OPTIONS.
  if [ -z "${JAVA_TOOL_OPTIONS:-}" ]; then
      export JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=60.0 -XX:MaxDirectMemorySize=512m"
  fi

  # Arrow (the log record format) requires java.nio to be opened on Java 9+.
  # Fluss's own launch scripts (fluss-daemon.sh and fluss-console.sh) set this
  # flag for the JVMs they start; the embedded runtime bypasses those scripts,
  # so it sets the flag itself. Operators can pass further JVM options via
  # JAVA_TOOL_OPTIONS (picked up by the JVM above).
  exec java \
    -XX:+IgnoreUnrecognizedVMOptions \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    -Dlog4j.configurationFile="${FLINK_HOME}/conf/log4j-console.properties" \
    -cp "${FLINK_HOME}/lib/*" \
    org.apache.fluss.flink.tiering.EmbeddedFlussLakeTiering \
    --config.file "${RUNTIME_CONFIG_FILE}" "${args[@]}"
fi

## Running command in pass-through mode
exec "${args[@]}"
