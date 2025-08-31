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


# Start/stop a Fluss tablet Server.
USAGE="Usage: $0 ((start|start-foreground) [args])|stop|stop-all"

STARTSTOP=$1

if [ -z $2 ] || [[ $2 == -D* ]]; then
    # start|start-foreground [-D...]
    args=("${@:2}")
fi

if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]] && [[ $STARTSTOP != "stop-all" ]]; then
  echo $USAGE
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

SERVICE=tablet-server

if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then

    # if no other JVM options are set, set the GC to G1
    if [ -z "${FLUSS_ENV_JAVA_OPTS}" ] && [ -z "${FLUSS_ENV_JAVA_OPTS_TS}" ]; then
        export JVM_ARGS="$JVM_ARGS -XX:+UseG1GC"
    fi

    # Add TabletServer specific JVM options
    export FLUSS_ENV_JAVA_OPTS="${FLUSS_ENV_JAVA_OPTS} ${FLUSS_ENV_JAVA_OPTS_TS}"


    # Startup parameters

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