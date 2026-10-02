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

# Exclude integration tests by default (they need a Docker cluster).
# Run with: mix test --include integration
ExUnit.start(exclude: [:integration])

# Stop Docker containers after all tests finish (matches Python's pytest_unconfigure).
ExUnit.after_suite(fn _ ->
  unless System.get_env("FLUSS_BOOTSTRAP_SERVERS") do
    Fluss.Test.Cluster.stop()
  end
end)
