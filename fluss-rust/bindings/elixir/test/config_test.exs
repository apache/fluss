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

defmodule Fluss.ConfigTest do
  use ExUnit.Case, async: true

  test "new/1 creates config with bootstrap_servers; all other fields default to nil" do
    config = Fluss.Config.new("localhost:9123")
    assert config == %Fluss.Config{bootstrap_servers: "localhost:9123"}
  end

  test "set_connect_timeout_ms/2 sets the connect timeout" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_connect_timeout_ms(30_000)

    assert config.connect_timeout_ms == 30_000
  end

  test "set_remote_file_download_thread_num/2 sets the download thread num" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_remote_file_download_thread_num(4)

    assert config.remote_file_download_thread_num == 4
  end

  test "set_scanner_log_fetch_max_bytes/2 sets the fetch max bytes" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_scanner_log_fetch_max_bytes(16_777_216)

    assert config.scanner_log_fetch_max_bytes == 16_777_216
  end

  test "set_scanner_log_fetch_max_bytes_for_bucket/2 sets the per-bucket fetch limit" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_scanner_log_fetch_max_bytes_for_bucket(1_048_576)

    assert config.scanner_log_fetch_max_bytes_for_bucket == 1_048_576
  end

  test "set_scanner_log_fetch_min_bytes/2 sets the fetch min bytes" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_scanner_log_fetch_min_bytes(1)

    assert config.scanner_log_fetch_min_bytes == 1
  end

  test "set_scanner_log_fetch_wait_max_time_ms/2 sets the max wait time" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_scanner_log_fetch_wait_max_time_ms(500)

    assert config.scanner_log_fetch_wait_max_time_ms == 500
  end

  test "set_scanner_log_max_poll_records/2 sets the max poll records" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_scanner_log_max_poll_records(1000)

    assert config.scanner_log_max_poll_records == 1000
  end

  test "set_scanner_remote_log_prefetch_num/2 sets the prefetch num" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_scanner_remote_log_prefetch_num(2)

    assert config.scanner_remote_log_prefetch_num == 2
  end

  test "set_scanner_remote_log_read_concurrency/2 sets the read concurrency" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_scanner_remote_log_read_concurrency(4)

    assert config.scanner_remote_log_read_concurrency == 4
  end

  test "set_security_protocol/2 sets the security protocol" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_security_protocol("sasl")

    assert config.security_protocol == "sasl"
  end

  test "set_security_sasl_mechanism/2 sets the SASL mechanism" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_security_sasl_mechanism("PLAIN")

    assert config.security_sasl_mechanism == "PLAIN"
  end

  test "set_security_sasl_username/2 sets the SASL username" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_security_sasl_username("admin")

    assert config.security_sasl_username == "admin"
  end

  test "set_security_sasl_password/2 sets the SASL password" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_security_sasl_password("secret")

    assert config.security_sasl_password == "secret"
  end

  test "inspect/1 redacts security_sasl_password when set" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_security_sasl_password("supersecret")

    output = inspect(config)
    refute output =~ "supersecret"
    assert output =~ "[REDACTED]"
  end

  test "inspect/1 leaves nil security_sasl_password as nil" do
    config = Fluss.Config.new("localhost:9123")
    output = inspect(config)
    assert output =~ "security_sasl_password: nil"
  end

  test "set_writer_acks/2 sets the acks value" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_acks("all")

    assert config.writer_acks == "all"
  end

  test "set_writer_bucket_no_key_assigner/2 sets a valid assigner" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_bucket_no_key_assigner(:sticky)

    assert config.writer_bucket_no_key_assigner == :sticky
  end

  test "set_writer_bucket_no_key_assigner/2 only accepts :sticky or :round_robin" do
    assert_raise FunctionClauseError, fn ->
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_bucket_no_key_assigner(:custom)
    end
  end

  test "set_writer_buffer_memory_size/2 sets the buffer memory size" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_buffer_memory_size(67_108_864)

    assert config.writer_buffer_memory_size == 67_108_864
  end

  test "set_writer_buffer_wait_timeout_ms/2 sets the wait timeout" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_buffer_wait_timeout_ms(5_000)

    assert config.writer_buffer_wait_timeout_ms == 5_000
  end

  test "set_writer_enable_idempotence/2 sets the idempotence flag" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_enable_idempotence(false)

    assert config.writer_enable_idempotence == false
  end

  test "set_writer_max_inflight_requests_per_bucket/2 sets the inflight limit" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_max_inflight_requests_per_bucket(3)

    assert config.writer_max_inflight_requests_per_bucket == 3
  end

  test "set_writer_request_max_size/2 sets the request max size" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_request_max_size(2_097_152)

    assert config.writer_request_max_size == 2_097_152
  end

  test "set_writer_retries/2 sets the retry count" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_retries(5)

    assert config.writer_retries == 5
  end

  test "setters chain correctly" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_acks("all")
      |> Fluss.Config.set_writer_retries(3)
      |> Fluss.Config.set_writer_bucket_no_key_assigner(:round_robin)

    assert config.writer_acks == "all"
    assert config.writer_retries == 3
    assert config.writer_bucket_no_key_assigner == :round_robin
  end
end
