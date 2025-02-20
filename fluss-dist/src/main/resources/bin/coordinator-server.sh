#!/usr/bin/env bash

#
# Copyright (c) 2024 Alibaba Group Holding Ltd.
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


# Start/stop a Fluss Coordinator Server.
USAGE="Usage: $0 ((start|start-foreground) [args])|stop|stop-all"

STARTSTOP=$1

if [ -z $2 ] || [[ $2 == "-D" ]]; then
    # start [-D ...]
    args=("${@:2}")
fi

if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]] && [[ $STARTSTOP != "stop-all" ]]; then
  echo $USAGE
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

SERVICE=coordinator-server

if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then
    # Add coordinator-specific JVM options
    export FLUSS_ENV_JAVA_OPTS="${FLUSS_ENV_JAVA_OPTS} ${FLUSS_ENV_JAVA_OPTS_CS}"

    args=("--configDir" "${FLUSS_CONF_DIR}" "${args[@]}")
    if [ ! -z $HOST ]; then
        args+=("--host")
        args+=("${HOST}")
    fi

    if [ ! -z "${DYNAMIC_PARAMETERS}" ]; then
        args=(${DYNAMIC_PARAMETERS[@]} "${args[@]}")
    fi
fi

if [[ $STARTSTOP == "start-foreground" ]]; then
    exec "${FLUSS_BIN_DIR}"/fluss-console.sh $SERVICE "${args[@]}"
else
    "${FLUSS_BIN_DIR}"/fluss-daemon.sh $STARTSTOP $SERVICE "${args[@]}"
fi