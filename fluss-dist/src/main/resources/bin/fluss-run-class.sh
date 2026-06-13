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

# Launches a Java class with the Fluss classpath.
# Usage: fluss-run-class.sh <classname> [opts]

if [ $# -lt 1 ]; then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

FLUSS_CLASSPATH=`constructFlussClassPath`

CLASS_TO_RUN=$1
shift

CLI_LOG_CONF="${FLUSS_CONF_DIR}/log4j-cli.properties"
if [ -f "$CLI_LOG_CONF" ]; then
  log_setting=("-Dlog4j.configuration=file:${CLI_LOG_CONF}" "-Dlog4j.configurationFile=file:${CLI_LOG_CONF}")
else
  log_setting=("-Dlog4j.configuration=file:${FLUSS_CONF_DIR}/log4j-console.properties" "-Dlog4j.configurationFile=file:${FLUSS_CONF_DIR}/log4j-console.properties")
fi

FLUSS_ENV_JAVA_OPTS=$(eval echo ${FLUSS_ENV_JAVA_OPTS})

exec "$JAVA_RUN" $JVM_ARGS ${FLUSS_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "`manglePathList "$FLUSS_CLASSPATH"`" ${CLASS_TO_RUN} "$@"
