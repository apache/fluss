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
#   Step 2 — Cluster health check: call the LOCAL TabletServer's Cluster
#            Health API (which forwards to the Coordinator over the internal
#            listener) and pass only if status is GREEN.
#            YELLOW/RED/UNKNOWN means recovery is incomplete — block the upgrade.
#
# Both steps must pass for the pod to be marked Ready.
#
# Exit codes (from ClusterHealthReadinessCheck.java):
#   0 = Ready (status GREEN)
#   1 = Not ready (status YELLOW, RED, UNKNOWN, or TabletServer unreachable)
#   2 = API unsupported (older server) → immediate TCP fallback (latched)
#
# Environment variables (set by helm template or container spec):
#   FLUSS_HOME                    - Fluss installation directory
#   READINESS_TIMEOUT_MS          - Timeout for Health API call (default: 5000)
#   READINESS_TCP_HOST            - Host for TCP check (default: $POD_IP or 127.0.0.1)
#   READINESS_TCP_PORT            - Port for TCP check (default: 9124)
#   READINESS_ADDRESS             - TabletServer endpoint (host:port) for the
#       probe to talk to. Defaults to ${POD_IP}:${READINESS_TCP_PORT} when POD_IP
#       is set (the typical Kubernetes case where bind.listeners binds the
#       tablet's CLIENT endpoint to the pod IP rather than 0.0.0.0), and falls
#       back to 127.0.0.1:${READINESS_TCP_PORT} otherwise. Either way the probe
#       talks to the local sidecar tablet, which forwards getClusterHealth to
#       the Coordinator over the internal listener — so the probe never needs
#       to know the Coordinator's address.
#   READINESS_HEALTH_CHECK_AUTH   - Optional newline-separated key=value
#       properties (e.g. client.security.protocol=SASL, ...) loaded into the
#       client Configuration. Required when the local listener enforces SASL.
#   READINESS_HEALTH_CHECK_TIMEOUT_SECONDS - Maximum wall-clock seconds the
#       cluster health gate is allowed to keep this pod NotReady on first boot.
#       After this budget is exhausted the probe latches the TCP-only fast path
#       so a permanently-unrecoverable cluster cannot wedge a rolling upgrade.
#       Default: 1200 (20 minutes).
# ==============================================================================

set -o pipefail

# ---- Configuration ----

FLUSS_HOME="${FLUSS_HOME:-/opt/fluss}"
TIMEOUT_MS="${READINESS_TIMEOUT_MS:-5000}"
TCP_HOST="${READINESS_TCP_HOST:-${POD_IP:-127.0.0.1}}"
TCP_PORT="${READINESS_TCP_PORT:-9124}"
# Default the probe to the sidecar tablet inside the same pod so it never has
# to know the Coordinator's pod IP. The tablet forwards the request internally.
# Prefer ${POD_IP} when set (the typical K8s case where bind.listeners binds
# the CLIENT endpoint to the pod IP, not 0.0.0.0); fall back to 127.0.0.1 for
# local/non-K8s invocations.
export READINESS_ADDRESS="${READINESS_ADDRESS:-${POD_IP:-127.0.0.1}:${TCP_PORT}}"
# Wall-clock budget for the first-boot cluster health gate. Once exhausted, the
# probe latches the TCP fast path so a permanently-unhealthy cluster cannot
# wedge a rolling upgrade indefinitely.
HEALTH_CHECK_TIMEOUT_SECONDS="${READINESS_HEALTH_CHECK_TIMEOUT_SECONDS:-1200}"

# Marker files for tracking state across probe invocations
MARKER_DIR="/tmp/fluss-readiness"
FIRST_READY_MARKER="${MARKER_DIR}/first-ready"
# Records the epoch of the first time this pod entered the cluster health gate.
# Used to enforce HEALTH_CHECK_TIMEOUT_SECONDS across probe invocations (the
# Java CLI is forked fresh each cycle, so the budget must live in a marker
# file).
FIRST_PROBE_EPOCH_FILE="${MARKER_DIR}/first-probe-epoch"
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

# Step 2: Run the Java ClusterHealthReadinessCheck CLI tool (cluster health check).
# The tool ships inside fluss-server-${version}.jar; no extra jar (fluss-client,
# fluss-dist) is required.
#
# TabletServer endpoint comes from env var READINESS_ADDRESS (host:port,
# defaults to 127.0.0.1:<tcp-port>). Optional auth properties come from env
# var READINESS_HEALTH_CHECK_AUTH. The Java CLI only takes --timeoutMs.
run_recovery_check() {
    local timeout_ms="$1"

    # Locate fluss-server jar (the only jar this probe needs).
    local fluss_server_jar=""
    while IFS= read -r -d '' jarfile; do
        if [[ "$jarfile" =~ .*/fluss-server[^/]*.jar$ ]]; then
            fluss_server_jar="$jarfile"
            break
        fi
    done < <(find "${FLUSS_HOME}/lib" ! -type d -name '*.jar' -print0 | sort -z)

    if [[ -z "$fluss_server_jar" ]]; then
        echo "[readiness-check] ERROR: fluss-server jar not found in ${FLUSS_HOME}/lib"
        return 3
    fi

    # log4j-api lives next to fluss-server jar in the dist; include the whole lib/
    # on the classpath so SLF4J/log4j wiring resolves correctly.
    local classpath="${FLUSS_HOME}/lib/*"

    # Find Java
    local java_cmd="java"
    if [[ -n "${JAVA_HOME}" && -x "${JAVA_HOME}/bin/java" ]]; then
        java_cmd="${JAVA_HOME}/bin/java"
    fi

    # Run the check — server address (and optional auth) come from env vars
    # directly; --timeoutMs is the only CLI arg needed.
    local output
    output=$("${java_cmd}" \
        -XX:+IgnoreUnrecognizedVMOptions \
        -Xmx64m \
        -classpath "${classpath}" \
        org.apache.fluss.server.tools.ClusterHealthReadinessCheck \
        --timeoutMs "${timeout_ms}" 2>&1)
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
# traffic until the cluster reports GREEN. Once we latch the marker,
# subsequent probes take the fast path above.

# Record the epoch of the first probe attempt so we can enforce the recovery
# budget across subsequent (forked-fresh) Java CLI invocations.
if [[ ! -f "${FIRST_PROBE_EPOCH_FILE}" ]]; then
    date +%s > "${FIRST_PROBE_EPOCH_FILE}"
fi
first_probe_epoch=$(cat "${FIRST_PROBE_EPOCH_FILE}" 2>/dev/null || echo 0)
now_epoch=$(date +%s)
elapsed_seconds=$(( now_epoch - first_probe_epoch ))

# Bail out of the gate if the cluster has stayed unhealthy for too long. We
# explicitly trade off "do not serve traffic from a half-recovered server" for
# "never wedge a rolling upgrade" — operators can lengthen the budget via
# READINESS_HEALTH_CHECK_TIMEOUT_SECONDS if they want stricter behaviour.
if (( elapsed_seconds >= HEALTH_CHECK_TIMEOUT_SECONDS )); then
    log_to_main "[readiness-check] Cluster health gate budget exhausted (${elapsed_seconds}s >= ${HEALTH_CHECK_TIMEOUT_SECONDS}s); latching TCP fast path to unblock rolling upgrade"
    touch "${FIRST_READY_MARKER}"
    exit 0
fi

run_recovery_check "${TIMEOUT_MS}"
local_exit=$?

case $local_exit in
    0)
        # Recovery completed — latch fast path for the rest of this pod's life.
        log_to_main "[readiness-check] Cluster health GREEN; latching fast path — next probe will switch to TCP-only port check"
        touch "${FIRST_READY_MARKER}"
        exit 0
        ;;
    1)
        # Not recovered yet (or Coordinator temporarily unreachable) — stay
        # unready. Next probe cycle will retry. This is the upgrade gate.
        log_to_main "[readiness-check] First boot: BLOCKED (exit 1) — waiting for recovery"
        exit 1
        ;;
    2)
        # API unsupported (older Coordinator that predates the Cluster Health
        # API). Server is up but definitively cannot answer; there is nothing
        # to wait for, so latch the fast path immediately and rely on TCP-only
        # readiness from now on. Waiting here would waste a grace period ×
        # pod_count of dead time on the first upgrade from such a version,
        # with zero protection gained (the API genuinely cannot be available
        # while the entire cluster is on the old Coordinator).
        log_to_main "[readiness-check] Cluster Health API unsupported by Coordinator, TCP fallback (latched)"
        touch "${FIRST_READY_MARKER}"
        exit 0
        ;;
    *)
        # Configuration error — fall back to TCP and latch fast path so a
        # broken probe never wedges the rolling upgrade forever.
        log_to_main "[readiness-check] Config error (exit $local_exit), TCP fallback (latched)"
        touch "${FIRST_READY_MARKER}"
        exit 0
        ;;
esac
