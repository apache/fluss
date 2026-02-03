#!/usr/bin/env bash

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


USAGE="Usage: fluss-cli.sh [sql|admin] [args]"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

FLUSS_CLI_JAR=$(find "$FLUSS_LIB_DIR" -name 'fluss-cli-*.jar' -type f | head -n 1)

if [ -z "$FLUSS_CLI_JAR" ] || [ ! -f "$FLUSS_CLI_JAR" ]; then
    echo "ERROR: Fluss CLI JAR not found in: $FLUSS_LIB_DIR"
    exit 1
fi

FLUSS_CLASSPATH="${FLUSS_CLI_JAR}:$(constructFlussClassPath)"

if is_jdk_version_ge_17 "$JAVA_RUN" ; then
  JVM_ARGS="${JVM_ARGS} --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
fi

LOG_FILTER='^(SLF4J:|WARNING: sun\.reflect\.Reflection\.getCallerClass)'

filter_cli_output() {
  local suppress_warnings="${FLUSS_CLI_SUPPRESS_WARNINGS:-0}"
  local printed_illegal_access=false
  local printed_token_error=false

  while IFS= read -r line; do
    case "$line" in
      SLF4J:*|*"sun.reflect.Reflection.getCallerClass"*)
        [ "$suppress_warnings" = "1" ] && continue
        ;;
      "WARNING: An illegal reflective access operation has occurred")
        if [ "$suppress_warnings" = "0" ] && [ "$printed_illegal_access" = false ]; then
          echo "WARNING: JVM illegal reflective access detected (Arrow). Consider JDK 17+ or add --add-opens for java.nio." >&2
          printed_illegal_access=true
        fi
        continue
        ;;
      *"Illegal reflective access by org.apache.fluss.shaded.arrow"*|*"Please consider reporting this to the maintainers"*|*"Use --illegal-access=warn"*|*"All illegal access operations will be denied"*)
        continue
        ;;
      *"DefaultSecurityTokenManager"*)
        if [ "$suppress_warnings" = "0" ] && [ "$printed_token_error" = false ]; then
          echo "WARNING: S3 security token failed. Check s3.endpoint/access-key/secret-key." >&2
          printed_token_error=true
        fi
        continue
        ;;
      *SecurityTokenException*|*InvalidClientTokenId*|*AWSSecurityTokenService*)
        continue
        ;;
    esac
    if echo "$line" | grep -E -q "$LOG_FILTER"; then
      [ "$suppress_warnings" = "1" ] && continue
    fi
    echo "$line"
  done
}

exec "$JAVA_RUN" $JVM_ARGS ${FLUSS_ENV_JAVA_OPTS} -classpath "`manglePathList "$FLUSS_CLASSPATH"`" org.apache.fluss.cli.FlussCliMain "$@" \
  > >(filter_cli_output) \
  2> >(filter_cli_output >&2)
