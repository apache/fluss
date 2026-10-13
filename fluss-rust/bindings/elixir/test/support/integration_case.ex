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

defmodule Fluss.Test.IntegrationCase do
  @moduledoc """
  Testing Template for Fluss integration tests
  """
  use ExUnit.CaseTemplate
  alias Fluss.Test.Cluster

  using do
    quote do
      @moduletag :integration
      alias Fluss.Test.Cluster
    end
  end

  setup_all do
    case Cluster.ensure_started() do
      {:ok, servers} ->
        config = Fluss.Config.new(servers)
        {conn, admin} = Cluster.connect_with_retry(config, 90)
        %{conn: conn, admin: admin, config: config}

      {:error, reason} ->
        raise "Failed to start Fluss cluster: #{reason}"
    end
  end
end
