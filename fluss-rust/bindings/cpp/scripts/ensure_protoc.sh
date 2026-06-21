#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

PROTOBUF_BASELINE_VERSION="${PROTOBUF_BASELINE_VERSION:-3.25.5}"
if [[ -n "${XDG_CACHE_HOME:-}" ]]; then
  _PROTOC_DEFAULT_CACHE_BASE="${XDG_CACHE_HOME}"
elif [[ -n "${HOME:-}" ]]; then
  _PROTOC_DEFAULT_CACHE_BASE="${HOME}/.cache"
else
  _PROTOC_DEFAULT_CACHE_BASE="/tmp"
fi

_PROTOC_UNAME_S="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "${_PROTOC_UNAME_S}" in
  linux*)
    _PROTOC_DEFAULT_OS="linux"
    ;;
  darwin*)
    _PROTOC_DEFAULT_OS="osx"
    ;;
  *)
    echo "ERROR: unsupported host OS '${_PROTOC_UNAME_S}'. Please set PROTOC_OS explicitly." >&2
    exit 1
    ;;
esac

_PROTOC_UNAME_M="$(uname -m)"
case "${_PROTOC_UNAME_M}" in
  x86_64|amd64)
    _PROTOC_DEFAULT_ARCH="x86_64"
    ;;
  aarch64|arm64)
    _PROTOC_DEFAULT_ARCH="aarch_64"
    ;;
  *)
    echo "ERROR: unsupported host arch '${_PROTOC_UNAME_M}'. Please set PROTOC_ARCH explicitly." >&2
    exit 1
    ;;
esac

PROTOC_INSTALL_ROOT="${PROTOC_INSTALL_ROOT:-${_PROTOC_DEFAULT_CACHE_BASE}/fluss-cpp-tools}"
PROTOC_OS="${PROTOC_OS:-${_PROTOC_DEFAULT_OS}}"
PROTOC_ARCH="${PROTOC_ARCH:-${_PROTOC_DEFAULT_ARCH}}"
PROTOC_FORCE_INSTALL="${PROTOC_FORCE_INSTALL:-0}"
PROTOC_PRINT_PATH_ONLY="${PROTOC_PRINT_PATH_ONLY:-0}"
PROTOC_ALLOW_INSECURE_DOWNLOAD="${PROTOC_ALLOW_INSECURE_DOWNLOAD:-0}"
PROTOC_SKIP_CHECKSUM_VERIFY="${PROTOC_SKIP_CHECKSUM_VERIFY:-0}"

usage() {
  cat <<'EOF'
Usage: bindings/cpp/scripts/ensure_protoc.sh [--print-path]

Ensures a protoc binary matching the configured protobuf baseline is available.
Installs into a local cache directory (default: \$XDG_CACHE_HOME/fluss-cpp-tools or
\$HOME/.cache/fluss-cpp-tools) and prints
the protoc path on stdout.

Env vars:
  PROTOBUF_BASELINE_VERSION  Baseline protobuf version (default: 3.25.5)
  PROTOC_INSTALL_ROOT        Local cache root (default: XDG/HOME cache dir)
  PROTOC_OS                 protoc package OS (default: auto-detect host: linux/osx)
  PROTOC_ARCH               protoc package arch (default: auto-detect host: x86_64/aarch_64)
  PROTOC_FORCE_INSTALL      1 to force re-download
  PROTOC_ALLOW_INSECURE_DOWNLOAD
                            1 to disable TLS verification (not recommended)
  PROTOC_SKIP_CHECKSUM_VERIFY
                            1 to skip pinned archive checksum verification
  BAZEL_PROXY_URL           Optional proxy (sets curl/wget proxy envs if present)
EOF
}

for arg in "$@"; do
  case "$arg" in
    --print-path)
      PROTOC_PRINT_PATH_ONLY=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      usage >&2
      exit 1
      ;;
  esac
done

setup_proxy_env() {
  if [[ -n "${BAZEL_PROXY_URL:-}" ]]; then
    export http_proxy="${http_proxy:-$BAZEL_PROXY_URL}"
    export https_proxy="${https_proxy:-$BAZEL_PROXY_URL}"
    export HTTP_PROXY="${HTTP_PROXY:-$http_proxy}"
    export HTTPS_PROXY="${HTTPS_PROXY:-$https_proxy}"
  fi
}

normalize_version_for_protoc_release() {
  local v="$1"
  # Protobuf release packaging switched from v3.x.y to vX.Y for newer versions.
  # For our current agreed baseline (3.25.5), the protoc archive/tag is 25.5.
  if [[ "$v" =~ ^3\.([0-9]+\.[0-9]+)$ ]]; then
    local stripped="${BASH_REMATCH[1]}"
    local major="${stripped%%.*}"
    if [[ "$major" -ge 21 ]]; then
      echo "$stripped"
      return 0
    fi
  fi
  echo "$v"
}

version_matches_baseline() {
  local actual="$1"
  local baseline="$2"
  local actual_norm baseline_norm
  actual_norm="$(normalize_version_for_protoc_release "$actual")"
  baseline_norm="$(normalize_version_for_protoc_release "$baseline")"
  [[ "$actual" == "$baseline" || "$actual_norm" == "$baseline_norm" ]]
}

lookup_protoc_archive_sha256() {
  local release_version="$1"
  local os="$2"
  local arch="$3"
  case "${release_version}:${os}:${arch}" in
    25.5:linux:aarch_64)
      echo "dc715bb5aab2ebf9653d7d3efbe55e01a035e45c26f391ff6d9b7923e22914b7"
      ;;
    25.5:linux:x86_64)
      echo "e1ed237a17b2e851cf9662cb5ad02b46e70ff8e060e05984725bc4b4228c6b28"
      ;;
    25.5:osx:aarch_64)
      echo "781a6fc4c265034872cadc65e63dd3c0fc49245b70917821b60e2d457a6876ab"
      ;;
    25.5:osx:x86_64)
      echo "c5447e4f0d5caffb18d9ff21eae7bc7faf2bb2000083d6f49e5b6000b30fceae"
      ;;
    *)
      return 1
      ;;
  esac
}

verify_download_sha256() {
  local file="$1"
  local expected="$2"
  local actual=""
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$file" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "$file" | awk '{print $1}')"
  else
    echo "ERROR: neither sha256sum nor shasum is available for checksum verification." >&2
    return 1
  fi
  if [[ "$actual" != "$expected" ]]; then
    echo "ERROR: protoc archive checksum mismatch." >&2
    echo "  expected: $expected" >&2
    echo "  actual:   $actual" >&2
    return 1
  fi
}

download_file() {
  local url="$1"
  local out="$2"

  if command -v curl >/dev/null 2>&1; then
    local curl_args=(-fL)
    if [[ "${PROTOC_ALLOW_INSECURE_DOWNLOAD}" == "1" ]]; then
      curl_args+=(-k)
    fi
    curl "${curl_args[@]}" "$url" -o "$out"
    return 0
  fi

  if command -v wget >/dev/null 2>&1; then
    local wget_args=()
    if [[ -n "${https_proxy:-}" || -n "${http_proxy:-}" ]]; then
      wget_args+=(-e use_proxy=yes)
      if [[ -n "${https_proxy:-}" ]]; then
        wget_args+=(-e "https_proxy=${https_proxy}")
      fi
      if [[ -n "${http_proxy:-}" ]]; then
        wget_args+=(-e "http_proxy=${http_proxy}")
      fi
    fi
    if [[ "${PROTOC_ALLOW_INSECURE_DOWNLOAD}" == "1" ]]; then
      wget_args+=(--no-check-certificate)
    fi
    wget "${wget_args[@]}" -O "$out" "$url"
    return 0
  fi

  echo "ERROR: neither curl nor wget is available for downloading protoc." >&2
  return 1
}

ensure_zip_tools() {
  command -v unzip >/dev/null 2>&1 || {
    echo "ERROR: unzip not found." >&2
    exit 1
  }
}

setup_proxy_env
ensure_zip_tools

if command -v protoc >/dev/null 2>&1; then
  existing_out="$(protoc --version 2>/dev/null || true)"
  if [[ "$existing_out" =~ ([0-9]+\.[0-9]+\.[0-9]+) ]]; then
    existing_ver="${BASH_REMATCH[1]}"
    if version_matches_baseline "$existing_ver" "$PROTOBUF_BASELINE_VERSION"; then
      command -v protoc
      exit 0
    fi
  fi
fi

PROTOC_RELEASE_VERSION="$(normalize_version_for_protoc_release "$PROTOBUF_BASELINE_VERSION")"
PROTOC_ARCHIVE="protoc-${PROTOC_RELEASE_VERSION}-${PROTOC_OS}-${PROTOC_ARCH}.zip"
PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_RELEASE_VERSION}/${PROTOC_ARCHIVE}"
PROTOC_PREFIX="${PROTOC_INSTALL_ROOT}/protoc-${PROTOC_RELEASE_VERSION}-${PROTOC_OS}-${PROTOC_ARCH}"
PROTOC_BIN="${PROTOC_PREFIX}/bin/protoc"

if [[ "${PROTOC_FORCE_INSTALL}" != "1" && -x "${PROTOC_BIN}" ]]; then
  if [[ "${PROTOC_PRINT_PATH_ONLY}" == "1" ]]; then
    echo "${PROTOC_BIN}"
  else
    echo "${PROTOC_BIN}"
  fi
  exit 0
fi

mkdir -p "${PROTOC_INSTALL_ROOT}"
tmpdir="$(mktemp -d "${PROTOC_INSTALL_ROOT}/.protoc-download.XXXXXX")"
trap 'rm -rf "${tmpdir}"' EXIT

archive_path="${tmpdir}/${PROTOC_ARCHIVE}"
download_file "${PROTOC_URL}" "${archive_path}"
if [[ "${PROTOC_SKIP_CHECKSUM_VERIFY}" != "1" ]]; then
  if expected_sha256="$(lookup_protoc_archive_sha256 "${PROTOC_RELEASE_VERSION}" "${PROTOC_OS}" "${PROTOC_ARCH}")"; then
    verify_download_sha256 "${archive_path}" "${expected_sha256}"
  else
    echo "ERROR: no pinned checksum for protoc archive ${PROTOC_ARCHIVE}. Set PROTOC_SKIP_CHECKSUM_VERIFY=1 to bypass." >&2
    exit 1
  fi
fi

extract_dir="${tmpdir}/extract"
mkdir -p "${extract_dir}"
unzip -q "${archive_path}" -d "${extract_dir}"

rm -rf "${PROTOC_PREFIX}"
mkdir -p "${PROTOC_PREFIX}"
cp -a "${extract_dir}/." "${PROTOC_PREFIX}/"
chmod +x "${PROTOC_BIN}"

echo "${PROTOC_BIN}"
