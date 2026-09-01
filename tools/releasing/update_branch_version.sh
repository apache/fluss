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

##
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}

# fail immediately
set -o errexit
set -o nounset
# print command before executing
set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

###########################

OLD_VERSION=${OLD_VERSION}
NEW_VERSION=${NEW_VERSION}


if [ -z "${OLD_VERSION}" ]; then
	echo "OLD_VERSION is unset"
	exit 1
fi

if [ -z "${NEW_VERSION}" ]; then
	echo "NEW_VERSION is unset"
	exit 1
fi

cd ..

# Keep the independently built Gateway executable on the same release version
# as the Java distribution and shared repository tag.
GATEWAY_CARGO_TOML="fluss-gateway/Cargo.toml"
GATEWAY_CARGO_LOCK="fluss-gateway/Cargo.lock"
GATEWAY_OPENAPI="fluss-gateway/openapi.yaml"
GATEWAY_DEPENDENCIES="fluss-gateway/DEPENDENCIES.rust.tsv"

# Maven development versions omit the patch component (for example,
# 1.1-SNAPSHOT), while Cargo requires SemVer's major.minor.patch form. Preserve
# the prerelease or build suffix when adding that missing component.
gateway_version() {
    local version="$1"
    if [[ "${version}" =~ ^([0-9]+\.[0-9]+)([-+].+)$ ]]; then
        version="${BASH_REMATCH[1]}.0${BASH_REMATCH[2]}"
    elif [[ "${version}" =~ ^[0-9]+\.[0-9]+$ ]]; then
        version="${version}.0"
    fi
    if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?(\+[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$ ]]; then
        return 1
    fi
    printf '%s\n' "${version}"
}

if ! GATEWAY_OLD_VERSION="$(gateway_version "${OLD_VERSION}")"; then
    echo "Cannot map OLD_VERSION ${OLD_VERSION} to a valid Gateway Cargo version." >&2
    exit 1
fi
if ! GATEWAY_NEW_VERSION="$(gateway_version "${NEW_VERSION}")"; then
    echo "Cannot map NEW_VERSION ${NEW_VERSION} to a valid Gateway Cargo version." >&2
    exit 1
fi

CURRENT_GATEWAY_VERSION=$(sed -n '/^\[package\]/,/^\[/p' "${GATEWAY_CARGO_TOML}" \
    | grep '^[[:space:]]*version[[:space:]]*=' \
    | head -1 \
    | sed -E 's/.*"([^"]+)".*/\1/')
if [[ -z "${CURRENT_GATEWAY_VERSION}" ]]; then
    echo "Cannot read the Gateway version from ${GATEWAY_CARGO_TOML}." >&2
    exit 1
fi
if [[ "${CURRENT_GATEWAY_VERSION}" != "${GATEWAY_OLD_VERSION}" ]]; then
    echo "Gateway version ${CURRENT_GATEWAY_VERSION} does not match OLD_VERSION ${OLD_VERSION} (${GATEWAY_OLD_VERSION})." >&2
    exit 1
fi
if ! grep -A2 '^\[\[package\]\]$' "${GATEWAY_CARGO_LOCK}" \
    | grep -A1 '^name = "fluss-gateway"$' \
    | grep -Fq "version = \"${CURRENT_GATEWAY_VERSION}\""; then
    echo "Cannot find Gateway version ${CURRENT_GATEWAY_VERSION} in ${GATEWAY_CARGO_LOCK}." >&2
    exit 1
fi
if ! grep -Fxq "  version: ${CURRENT_GATEWAY_VERSION}" "${GATEWAY_OPENAPI}"; then
    echo "Cannot find Gateway version ${CURRENT_GATEWAY_VERSION} in ${GATEWAY_OPENAPI}." >&2
    exit 1
fi
if ! cut -f1 "${GATEWAY_DEPENDENCIES}" \
    | grep -Fxq "fluss-gateway@${CURRENT_GATEWAY_VERSION}"; then
    echo "Cannot find Gateway version ${CURRENT_GATEWAY_VERSION} in ${GATEWAY_DEPENDENCIES}." >&2
    exit 1
fi

# Change the Java and Gateway versions only after every input has been validated.
find . -name 'pom.xml' -type f -exec perl -pi -e 's#<version>'$OLD_VERSION'</version>#<version>'$NEW_VERSION'</version>#; s#-'$OLD_VERSION'</version>#-'$NEW_VERSION'</version>#' {} \;

export CURRENT_GATEWAY_VERSION GATEWAY_NEW_VERSION
perl -0pi -e 's#(\[\[package\]\]\nname = "fluss-gateway"\nversion = ")\Q$ENV{CURRENT_GATEWAY_VERSION}\E(")#$1$ENV{GATEWAY_NEW_VERSION}$2#' "${GATEWAY_CARGO_LOCK}"
perl -0pi -e 's#(\[package\].*?version = ")\Q$ENV{CURRENT_GATEWAY_VERSION}\E(")#$1$ENV{GATEWAY_NEW_VERSION}$2#s' "${GATEWAY_CARGO_TOML}"
perl -pi -e 's#^(  version: )\Q$ENV{CURRENT_GATEWAY_VERSION}\E$#$1$ENV{GATEWAY_NEW_VERSION}#' "${GATEWAY_OPENAPI}"
perl -pi -e 's#^(fluss-gateway@)\Q$ENV{CURRENT_GATEWAY_VERSION}\E(\t)#$1$ENV{GATEWAY_NEW_VERSION}$2#' "${GATEWAY_DEPENDENCIES}"

if ! grep -A2 '^\[\[package\]\]$' "${GATEWAY_CARGO_LOCK}" \
    | grep -A1 '^name = "fluss-gateway"$' \
    | grep -Fq "version = \"${GATEWAY_NEW_VERSION}\""; then
    echo "Failed to update the Gateway version in ${GATEWAY_CARGO_LOCK}." >&2
    exit 1
fi
if [[ "$(
    sed -n '/^\[package\]/,/^\[/p' "${GATEWAY_CARGO_TOML}" \
        | grep '^[[:space:]]*version[[:space:]]*=' \
        | head -1 \
        | sed -E 's/.*"([^"]+)".*/\1/'
)" != "${GATEWAY_NEW_VERSION}" ]]; then
    echo "Failed to update the Gateway version in ${GATEWAY_CARGO_TOML}." >&2
    exit 1
fi
if ! grep -Fxq "  version: ${GATEWAY_NEW_VERSION}" "${GATEWAY_OPENAPI}"; then
    echo "Failed to update the Gateway version in ${GATEWAY_OPENAPI}." >&2
    exit 1
fi
if ! cut -f1 "${GATEWAY_DEPENDENCIES}" \
    | grep -Fxq "fluss-gateway@${GATEWAY_NEW_VERSION}"; then
    echo "Failed to update the Gateway version in ${GATEWAY_DEPENDENCIES}." >&2
    exit 1
fi

git commit -am "[release] Update version to $NEW_VERSION"

echo "Don't forget to push the change."
