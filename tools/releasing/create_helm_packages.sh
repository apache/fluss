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

if [ -z "${RELEASE_VERSION:-}" ]; then
    echo "RELEASE_VERSION was not set."
    exit 1
fi

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

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
    # turn off xattr headers in the generated archive file on macOS
    TAR_OPTIONS="--no-xattrs"
    # Disable the creation of ._* files on macOS.
    export COPYFILE_DISABLE=1
else
    SHASUM="sha512sum"
    TAR_OPTIONS=""
fi

###########################

cd ..

FLUSS_DIR=`pwd`
HELM_RELEASE_DIR=${FLUSS_DIR}/tools/releasing/release/helm-chart/${RELEASE_VERSION}-rc${RELEASE_CANDIDATE}

echo "Creating helm chart package"

mkdir -p ${HELM_RELEASE_DIR}

cd ${HELM_RELEASE_DIR}

# Package both charts with the requested release version.
for chart_name in fluss fluss-gateway; do
    helm package "${FLUSS_DIR}/helm/${chart_name}" \
        --version "${RELEASE_VERSION}" --app-version "${RELEASE_VERSION}"
    chart_archive="${chart_name}-${RELEASE_VERSION}.tgz"
    helm gpg sign "${chart_archive}"

    $SHASUM "${chart_archive}" > "${chart_archive}.sha512"
    $SHASUM "${chart_archive}.prov" > "${chart_archive}.prov.sha512"
    gpg --armor --detach-sig "${HELM_RELEASE_DIR}/${chart_archive}"
    gpg --armor --detach-sig "${HELM_RELEASE_DIR}/${chart_archive}.prov"
done

# create index.yaml
cd ..
helm repo index .
