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

defmodule Fluss.ErrorTest do
  use ExUnit.Case, async: true

  @retriable_codes [
    :network_exception,
    :corrupt_message,
    :schema_not_exist,
    :log_storage_exception,
    :kv_storage_exception,
    :not_leader_or_follower,
    :corrupt_record_exception,
    :unknown_table_or_bucket_exception,
    :request_time_out,
    :storage_exception,
    :not_enough_replicas_after_append_exception,
    :not_enough_replicas_exception,
    :leader_not_available_exception
  ]

  @non_retriable_codes [
    :client_error,
    :unknown_server_error,
    :none,
    :table_not_exist,
    :authenticate_exception,
    :authorization_exception,
    :record_too_large_exception,
    :deletion_disabled_exception,
    :invalid_coordinator_exception,
    :fenced_leader_epoch_exception,
    :fenced_tiering_epoch_exception,
    :retriable_authenticate_exception
  ]

  defp err(code), do: %Fluss.Error{code: code, error_code: 0, message: ""}

  test "Exception.message/1 formats '[<code>]: <msg>'" do
    err = %Fluss.Error{code: :network_exception, error_code: 1, message: "disconnected"}
    assert Exception.message(err) == "Fluss error [network_exception]: disconnected"
  end

  test "retriable?/1 returns true for transient protocol codes" do
    for code <- @retriable_codes do
      assert Fluss.Error.retriable?(err(code)), "expected #{code} to be retriable"
    end
  end

  test "retriable?/1 returns false for :client_error and permanent codes" do
    for code <- @non_retriable_codes do
      refute Fluss.Error.retriable?(err(code)), "expected #{code} to not be retriable"
    end
  end

  describe "NIF error surface" do
    test "unreachable server returns %Fluss.Error{code: :network_exception, error_code: 1}" do
      config = Fluss.Config.new("127.0.0.1:1")

      assert {:error, %Fluss.Error{code: :network_exception, error_code: 1}} =
               Fluss.Connection.new(config)
    end

    test "bang variant raises %Fluss.Error{}" do
      config = Fluss.Config.new("127.0.0.1:1")

      assert_raise Fluss.Error, ~r/\[network_exception\]/, fn ->
        Fluss.Connection.new!(config)
      end
    end
  end
end
