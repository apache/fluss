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

defmodule Fluss.Test.Cluster do
  @moduledoc false

  # Shells out to the `fluss-test-cluster` CLI (from `crates/fluss-test-cluster`),
  # the same binary used by the Python and C++ integration tests.

  @cluster_name "shared-test"
  @cluster_json_prefix "CLUSTER_JSON: "

  def ensure_started do
    case System.get_env("FLUSS_BOOTSTRAP_SERVERS") do
      nil -> start_cluster()
      servers -> {:ok, servers}
    end
  end

  def stop do
    if System.get_env("FLUSS_BOOTSTRAP_SERVERS") do
      :ok
    else
      case find_cli_binary() do
        {:ok, cli} ->
          System.cmd(cli, ["stop", "--name", @cluster_name], stderr_to_stdout: true)
          :ok

        {:error, _} ->
          :ok
      end
    end
  end

  defp start_cluster do
    with {:ok, cli} <- find_cli_binary(),
         {output, 0} <-
           System.cmd(cli, ["start", "--sasl", "--name", @cluster_name], stderr_to_stdout: true),
         {:ok, bootstrap} <- parse_cluster_json(output) do
      {:ok, bootstrap}
    else
      {output, code} when is_binary(output) ->
        {:error, "fluss-test-cluster start failed (exit #{code}):\n#{output}"}

      {:error, _} = err ->
        err
    end
  end

  defp find_cli_binary do
    case System.get_env("FLUSS_TEST_CLUSTER_BIN") do
      bin when is_binary(bin) and bin != "" ->
        if File.regular?(bin),
          do: {:ok, bin},
          else: {:error, "FLUSS_TEST_CLUSTER_BIN=#{bin} does not exist"}

      _ ->
        locate_via_cargo()
    end
  end

  defp locate_via_cargo do
    case System.cmd("cargo", ["locate-project", "--workspace", "--message-format", "plain"],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        output |> String.trim() |> Path.dirname() |> find_binary_in_target()

      {output, code} ->
        {:error, "cargo locate-project failed (exit #{code}): #{output}"}
    end
  end

  defp find_binary_in_target(root) do
    Enum.find_value(
      ["debug", "release"],
      {:error, "fluss-test-cluster binary not found. Run: cargo build -p fluss-test-cluster"},
      &check_binary(root, &1)
    )
  end

  defp check_binary(root, profile) do
    path = Path.join([root, "target", profile, "fluss-test-cluster"])
    if File.regular?(path), do: {:ok, path}, else: nil
  end

  defp parse_cluster_json(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.find_value(
      {:error, "No #{@cluster_json_prefix} token in output:\n#{output}"},
      &extract_bootstrap/1
    )
  end

  defp extract_bootstrap(line) do
    case String.split(line, @cluster_json_prefix, parts: 2) do
      [_, json] ->
        case decode_bootstrap(json) do
          {:ok, bootstrap} -> {:ok, bootstrap}
          _ -> nil
        end

      _ ->
        nil
    end
  end

  # Minimal JSON extractor for `bootstrap_servers`: avoids adding a JSON dep just for tests.
  defp decode_bootstrap(json) do
    case Regex.run(~r/"bootstrap_servers"\s*:\s*"([^"]+)"/, json) do
      [_, servers] -> {:ok, servers}
      _ -> {:error, "no bootstrap_servers in: #{json}"}
    end
  end
end
