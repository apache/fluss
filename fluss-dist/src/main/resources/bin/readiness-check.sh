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

# ==============================================================================
# Fluss Readiness Check Script
# ==============================================================================
#
# Two-step readiness probe for Kubernetes StatefulSet rolling upgrades:
#
#   Step 1 — Local TCP port check: verify this TabletServer process is alive
#            and has bound its RPC port. Fast, no external dependency.
#
#   Step 2 — Cluster health check: query the Coordinator's Cluster Health API
#            and pass only if status is GREEN.
#            YELLOW/RED/UNKNOWN means recovery is incomplete — block the upgrade.
#
# Both steps must pass for the pod to be marked Ready.
#
# Exit codes (from ClusterHealthReadinessCheck.java):
#   0 = Ready (status GREEN)
#   1 = Not ready (status YELLOW, RED, UNKNOWN, or Coordinator unreachable)
#   2 = API unsupported → shell handles grace period then TCP fallback
#
# Environment variables (set by helm template or container spec):
#   FLUSS_HOME            - Fluss installation directory
#   FLUSS_CONF_DIR        - Configuration directory (default: $FLUSS_HOME/conf)
#   READINESS_TIMEOUT_MS  - Timeout for Health API call (default: 5000)
#   READINESS_TCP_HOST    - Host for TCP check (default: $POD_IP or 127.0.0.1)
#   READINESS_TCP_PORT    - Port for TCP check (default: 9124)
#   READINESS_BOOTSTRAP_SERVERS - Coordinator bootstrap address
#   READINESS_GRACE_SECS  - Grace period when API is unsupported (default: 60)
# ==============================================================================

set -o pipefail

# ---- Configuration ----

FLUSS_HOME="${FLUSS_HOME:-/opt/fluss}"
FLUSS_CONF_DIR="${FLUSS_CONF_DIR:-${FLUSS_HOME}/conf}"
TIMEOUT_MS="${READINESS_TIMEOUT_MS:-5000}"
TCP_HOST="${READINESS_TCP_HOST:-${POD_IP:-127.0.0.1}}"
TCP_PORT="${READINESS_TCP_PORT:-9124}"
BOOTSTRAP_SERVERS="${READINESS_BOOTSTRAP_SERVERS:-}"
GRACE_SECS="${READINESS_GRACE_SECS:-60}"

# Marker files for tracking state across probe invocations
MARKER_DIR="/tmp/fluss-readiness"
FIRST_READY_MARKER="${MARKER_DIR}/first-ready"
API_UNSUPPORTED_SINCE="${MARKER_DIR}/api-unsupported-since"
# Latched-once marker so the "now in TCP-only fast path" notice is logged
# exactly one time per pod lifetime (avoids flooding kubectl logs every 3s).
FAST_PATH_LOGGED="${MARKER_DIR}/fast-path-logged"

mkdir -p "${MARKER_DIR}"

# ---- Helper Functions ----

# Step 1: TCP port check (local liveness)
check_tcp() {
    local host="${TCP_HOST}"
    local port="${1:-${TCP_PORT}}"
    (echo > /dev/tcp/"${host}"/"${port}") >/dev/null 2>&1
    return $?
}

# Step 2: Run the Java ClusterHealthReadinessCheck CLI tool (cluster health check)
run_recovery_check() {
    local conf_dir="$1"
    local timeout_ms="$2"

    # Construct classpath (same logic as config.sh)
    local classpath=""
    local fluss_server_jar=""
    while IFS= read -r -d '' jarfile; do
        if [[ "$jarfile" =~ .*/fluss-server[^/]*.jar$ ]]; then
            fluss_server_jar="$jarfile"
        elif [[ -z "$classpath" ]]; then
            classpath="$jarfile"
        else
            classpath="${classpath}:${jarfile}"
        fi
    done < <(find "${FLUSS_HOME}/lib" ! -type d -name '*.jar' -print0 | sort -z)

    if [[ -n "$fluss_server_jar" ]]; then
        classpath="${classpath}:${fluss_server_jar}"
    fi

    if [[ -z "$classpath" ]]; then
        echo "[readiness-check] ERROR: No jars found in ${FLUSS_HOME}/lib"
        return 3
    fi

    # Find Java
    local java_cmd="java"
    if [[ -n "${JAVA_HOME}" && -x "${JAVA_HOME}/bin/java" ]]; then
        java_cmd="${JAVA_HOME}/bin/java"
    fi

    # Build optional --bootstrapServers argument
    local bootstrap_arg=""
    if [[ -n "${BOOTSTRAP_SERVERS}" ]]; then
        bootstrap_arg="--bootstrapServers ${BOOTSTRAP_SERVERS}"
    fi

    # Run the check (suppress JVM startup noise, only care about exit code + output)
    local output
    output=$("${java_cmd}" \
        -XX:+IgnoreUnrecognizedVMOptions \
        -Xmx64m \
        -classpath "${classpath}" \
        org.apache.fluss.dist.ClusterHealthReadinessCheck \
        --configDir "${conf_dir}" \
        --timeoutMs "${timeout_ms}" \
        ${bootstrap_arg} 2>&1)
    local exit_code=$?

    # Log output to container's main process stderr (visible in kubectl logs)
    if [[ -n "$output" ]]; then
        echo "$output" >&2
        if [[ -w /proc/1/fd/2 ]]; then
            echo "[readiness-probe] $output" > /proc/1/fd/2
        fi
    fi

    return $exit_code
}

# Record when API unsupported was first detected
mark_api_unsupported() {
    if [[ ! -f "${API_UNSUPPORTED_SINCE}" ]]; then
        date +%s > "${API_UNSUPPORTED_SINCE}"
    fi
}

# Clear API-unsupported marker
clear_api_unsupported() {
    rm -f "${API_UNSUPPORTED_SINCE}"
}

# Check if grace period for API-unsupported has elapsed
is_grace_period_elapsed() {
    if [[ ! -f "${API_UNSUPPORTED_SINCE}" ]]; then
        return 1 # not elapsed (no marker)
    fi
    local since
    since=$(cat "${API_UNSUPPORTED_SINCE}")
    local now
    now=$(date +%s)
    local elapsed=$(( now - since ))
    if [[ $elapsed -ge ${GRACE_SECS} ]]; then
        return 0 # elapsed
    fi
    return 1 # not elapsed yet
}

# ---- Main Logic ----

# Helper: log to container's main process (visible in kubectl logs)
log_to_main() {
    echo "$1" >&2
    if [[ -w /proc/1/fd/2 ]]; then
        echo "$1" > /proc/1/fd/2
    fi
}

# ---- Step 1: Local TCP port check ----
# If the local TS process hasn't bound its port yet, fail immediately.
# No need to query the Coordinator for cluster state.
if ! check_tcp; then
    exit 1
fi

# ---- Steady-state fast path ----
#
# K8s readinessProbe runs every periodSeconds FOREVER, not only during a
# rolling upgrade. The cluster health gate (Step 2 below) is meant to gate
# the FIRST readiness of a freshly created pod so that recovery finishes
# before traffic flows. Running it on every probe cycle is unsafe:
#
#   1. Coordinator pod briefly goes down (e.g. its own rolling restart).
#   2. ALL tablet-server probes call run_recovery_check at the same period,
#      all fail to reach Coordinator → all flip to NotReady simultaneously.
#   3. statefulset then rolls all tablet-servers together → cascade outage:
#         coordinator-server-0   0/2   Init:0/1
#         tablet-server-0/1/2    0/2   Init:0/1
#
# So once the cluster has been GREEN at least once in this pod's lifetime
# (FIRST_READY_MARKER present), we DROP to TCP-only. The marker lives in
# /tmp/fluss-readiness, which is wiped whenever the pod is recreated by an
# upgrade — so each freshly created pod will run the recovery gate exactly
# ONCE before flipping to the fast path.
if [[ -f "${FIRST_READY_MARKER}" ]]; then
    # Log the switch-over notice exactly once per pod lifetime so operators
    # can confirm via `kubectl logs` that this pod has graduated from the
    # cluster health gate to the cheap TCP-only port check.
    if [[ ! -f "${FAST_PATH_LOGGED}" ]]; then
        log_to_main "[readiness-check] Switched to TCP-only port-readiness check; cluster health gate will not run again until this pod is recreated"
        touch "${FAST_PATH_LOGGED}"
    fi
    exit 0
fi

# ---- Step 2: Cluster health check (first boot of this pod only) ----
#
# We reach here only if this pod has never been ready in its lifetime. Block
# traffic until either (a) cluster reports GREEN/YELLOW, or (b) the API is
# unsupported and the grace period has elapsed. Once we latch the marker,
# subsequent probes take the fast path above.
run_recovery_check "${FLUSS_CONF_DIR}" "${TIMEOUT_MS}"
local_exit=$?

case $local_exit in
    0)
        # Recovery completed — latch fast path for the rest of this pod's life.
        log_to_main "[readiness-check] Cluster health GREEN; latching fast path — next probe will switch to TCP-only port check"
        touch "${FIRST_READY_MARKER}"
        clear_api_unsupported
        exit 0
        ;;
    1)
        # Not recovered yet (or Coordinator temporarily unreachable) — stay
        # unready. Next probe cycle will retry. This is the upgrade gate.
        log_to_main "[readiness-check] First boot: BLOCKED (exit 1) — waiting for recovery"
        exit 1
        ;;
    2)
        # API unsupported — after grace period, fall back to TCP-only and
        # latch the fast path so we don't pay this cost forever.
        mark_api_unsupported
        if is_grace_period_elapsed; then
            log_to_main "[readiness-check] API unsupported for >${GRACE_SECS}s, TCP fallback (latched)"
            touch "${FIRST_READY_MARKER}"
            exit 0
        fi
        log_to_main "[readiness-check] API unsupported, waiting (grace period)"
        exit 1
        ;;
    *)
        # Configuration error — fall back to TCP and latch fast path so a
        # broken probe never wedges the rolling upgrade forever.
        log_to_main "[readiness-check] Config error (exit $local_exit), TCP fallback (latched)"
        touch "${FIRST_READY_MARKER}"
        exit 0
        ;;
esac
