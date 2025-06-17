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
SKIP_GPG=${SKIP_GPG:-false}
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
fi

cd ..

FLUSS_DIR=`pwd`
RELEASE_DIR=${FLUSS_DIR}/tools/releasing/release
mkdir -p ${RELEASE_DIR}

###########################

# build maven package, create Fluss distribution, generate signature
make_binary_release() {
  echo "Creating binary release"
  dir_name="fluss-$RELEASE_VERSION-bin"

  # enable release profile here (to check for the maven version)
  $MVN clean package -Prelease -am -Dgpg.skip -Dcheckstyle.skip=true -DskipTests

  cd fluss-dist/target/fluss-${RELEASE_VERSION}-bin
  ${FLUSS_DIR}/tools/releasing/collect_license_files.sh ./fluss-${RELEASE_VERSION} ./fluss-${RELEASE_VERSION}
  tar $TAR_OPTIONS -czf "${dir_name}.tgz" fluss-*

  cp fluss-*.tgz ${RELEASE_DIR}
  cd ${RELEASE_DIR}

  # Sign sha the tgz
  if [ "$SKIP_GPG" == "false" ] ; then
    gpg --armor --detach-sig "${dir_name}.tgz"
  fi
  $SHASUM "${dir_name}.tgz" > "${dir_name}.tgz.sha512"

  cd ${FLUSS_DIR}
}

make_binary_release
