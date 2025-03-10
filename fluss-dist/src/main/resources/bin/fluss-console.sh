#!/usr/bin/env bash
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


# Start a Fluss service as a console application. Must be stopped with Ctrl-C
# or with SIGTERM by kill or the controlling process.
USAGE="Usage: fluss-console.sh (coordinator-server|tablet-server|zookeeper) [args]"

SERVICE=$1
ARGS=("${@:2}") # get remaining arguments as array

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

case $SERVICE in
    (coordinator-server)
        CLASS_TO_RUN=com.alibaba.fluss.server.coordinator.CoordinatorServer
    ;;

    (tablet-server)
        CLASS_TO_RUN=com.alibaba.fluss.server.tablet.TabletServer
    ;;

    (zookeeper)
        CLASS_TO_RUN=org.apache.zookeeper.server.quorum.QuorumPeerMain
    ;;

    (*)
        echo "Unknown service '${SERVICE}'. $USAGE."
        exit 1
    ;;
esac

FLUSS_CLASSPATH=`constructFlussClassPath`

if [ "$FLUSS_IDENT_STRING" = "" ]; then
    FLUSS_IDENT_STRING="$USER"
fi

pid=$FLUSS_PID_DIR/fluss-$FLUSS_IDENT_STRING-$SERVICE.pid
mkdir -p "$FLUSS_PID_DIR"
# The lock needs to be released after use because this script is started foreground
command -v flock >/dev/null 2>&1
flock_exist=$?
if [[ ${flock_exist} -eq 0 ]]; then
    exec 200<"$FLUSS_PID_DIR"
    flock 200
fi
# Remove the pid file when all the processes are dead
if [ -f "$pid" ]; then
    all_dead=0
    while read each_pid; do
        # Check whether the process is still running
        kill -0 $each_pid > /dev/null 2>&1
        [[ $? -eq 0 ]] && all_dead=1
    done < "$pid"
    [ ${all_dead} -eq 0 ] && rm $pid
fi
id=$([ -f "$pid" ] && echo $(wc -l < "$pid") || echo "0")

FLUSS_LOG_PREFIX="${FLUSS_LOG_DIR}/fluss-${FLUSS_IDENT_STRING}-${SERVICE}-${id}-${HOSTNAME}"
log="${FLUSS_LOG_PREFIX}.log"
out="${FLUSS_LOG_PREFIX}.out"
err="${FLUSS_LOG_PREFIX}.err"

log_setting=("-Dlog.file=${log}" "-Dlog4j.configuration=file:${FLUSS_CONF_DIR}/log4j-console.properties" "-Dlog4j.configurationFile=file:${FLUSS_CONF_DIR}/log4j-console.properties" "-Dlogback.configurationFile=file:${FLUSS_CONF_DIR}/logback-console.xml")

echo "Starting $SERVICE as a console application on host $HOSTNAME."

# Add the current process id to pid file
echo $$ >> "$pid" 2>/dev/null

# Release the lock because the java process runs in the foreground and would block other processes from modifying the pid file
[[ ${flock_exist} -eq 0 ]] &&  flock -u 200

# Evaluate user options for local variable expansion
FLUSS_ENV_JAVA_OPTS=$(eval echo ${FLUSS_ENV_JAVA_OPTS})

if [ "${STD_REDIRECT_TO_FILE}" == "true" ]; then
  # disable console appender to avoid redundant logs in out file
  log_setting=("-Dconsole.log.level=OFF" "${log_setting[@]}")
  exec 1>"${out}"
  exec 2>"${err}"
fi

# when jdk version is 17 or above, need to add following option to make arrow works
# see https://arrow.apache.org/docs/dev/java/install.html#java-compatibility
if is_jdk_version_ge_17 "$JAVA_RUN" ; then
  JVM_ARGS="${JVM_ARGS} --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
fi

exec "$JAVA_RUN" $JVM_ARGS ${FLUSS_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "`manglePathList "$FLUSS_CLASSPATH"`" ${CLASS_TO_RUN} "${ARGS[@]}"
