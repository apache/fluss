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
RELEASE_DIR=${FLUSS_DIR}/tools/releasing/release
CLONE_DIR=${RELEASE_DIR}/fluss-tmp-clone

echo "Creating source package"

mkdir -p ${RELEASE_DIR}

# create a temporary git clone to ensure that we have a pristine source release
git clone ${FLUSS_DIR} ${CLONE_DIR}
cd ${CLONE_DIR}

rsync -a \
  --exclude ".git" --exclude ".gitignore" --exclude ".gitattributes" \
  --exclude ".github" --exclude "target" \
  --exclude ".idea" --exclude "*.iml" --exclude ".DS_Store" --exclude "build-target" \
  --exclude "website/" \
  --exclude "*/dependency-reduced-pom.xml" \
  . fluss-$RELEASE_VERSION

tar $TAR_OPTIONS -czf  ${RELEASE_DIR}/fluss-${RELEASE_VERSION}-src.tgz fluss-$RELEASE_VERSION
gpg --armor --detach-sig ${RELEASE_DIR}/fluss-$RELEASE_VERSION-src.tgz
cd ${RELEASE_DIR}
$SHASUM fluss-$RELEASE_VERSION-src.tgz > fluss-$RELEASE_VERSION-src.tgz.sha512

cd ${CURR_DIR}
rm -rf ${CLONE_DIR}
