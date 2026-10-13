#!/bin/bash
#
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
#

# Setup blog content from the external blog repository.
# This script clones the blog repo and places its blog/ directory
# into website/blog/ for integration with the main website build.
#
# Environment variables:
#   BLOG_REPO    - Git repository URL (default: https://github.com/apache/fluss-blog.git)
#   BLOG_BRANCH  - Branch to clone (default: main)

set -euo pipefail

BLOG_REPO="${BLOG_REPO:-https://github.com/apache/fluss-blog.git}"
BLOG_BRANCH="${BLOG_BRANCH:-main}"
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BLOG_TARGET="$SCRIPT_DIR/blog"

if [ -d "$BLOG_TARGET" ]; then
  echo "Blog directory already exists at $BLOG_TARGET, removing for refresh..."
  rm -rf "$BLOG_TARGET"
fi

echo "Cloning blog repository ($BLOG_BRANCH branch)..."
TEMP_DIR=$(mktemp -d)

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

git clone --depth 1 --branch "$BLOG_BRANCH" "$BLOG_REPO" "$TEMP_DIR"

if [ ! -d "$TEMP_DIR/blog" ]; then
  echo "Error: blog/ directory not found in cloned repository"
  exit 1
fi

cp -r "$TEMP_DIR/blog" "$BLOG_TARGET"
echo "Blog content set up at $BLOG_TARGET"
