# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Create ASF source release artifacts under dist/.
# Check out the release tag first (e.g. git checkout v0.1.0-rc1).
# Usage: just release [version]
#   If version is omitted, read from Cargo.toml.

# [version]: optional; if omitted, script reads from Cargo.toml
release [version]:
    ./scripts/release.sh {{version}}

# Bump version on main for next development cycle. Run from main after cutting release branch.
# Usage: just bump-version <current> <next>   e.g. just bump-version 0.1.0 0.1.1
bump-version from to:
    ./scripts/bump-version.sh {{from}} {{to}}
