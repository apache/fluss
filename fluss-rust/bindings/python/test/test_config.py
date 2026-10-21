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

import pytest

import fluss


def test_kv_backpressure_configuration():
    config = fluss.Config({"writer.kv-backpressure.max-throttle-ms": "1500"})
    assert config.writer_kv_backpressure_max_throttle_ms == 1500

    config.writer_kv_backpressure_max_throttle_ms = 750
    assert config.writer_kv_backpressure_max_throttle_ms == 750


def test_writer_retry_backoff_configuration():
    config = fluss.Config(
        {
            "writer.retry-backoff-ms": "250",
            "writer.retry-max-backoff-ms": "5000",
        }
    )
    assert config.writer_retry_backoff_ms == 250
    assert config.writer_retry_max_backoff_ms == 5000

    config.writer_retry_backoff_ms = 100
    config.writer_retry_max_backoff_ms = 1000
    assert config.writer_retry_backoff_ms == 100
    assert config.writer_retry_max_backoff_ms == 1000


def test_lookup_configuration():
    config = fluss.Config(
        {
            "lookup.queue-size": "1024",
            "lookup.max-batch-size": "64",
            "lookup.batch-timeout-ms": "5",
            "lookup.max-inflight-requests": "16",
            "lookup.max-retries": "3",
        }
    )
    assert config.lookup_queue_size == 1024
    assert config.lookup_max_batch_size == 64
    assert config.lookup_batch_timeout_ms == 5
    assert config.lookup_max_inflight_requests == 16
    assert config.lookup_max_retries == 3

    config.lookup_batch_timeout_ms = 1
    assert config.lookup_batch_timeout_ms == 1

    with pytest.raises(fluss.FlussError, match="Invalid value 'soon'"):
        fluss.Config({"lookup.batch-timeout-ms": "soon"})


def test_storage_backpressure_error_is_retriable():
    assert fluss.ErrorCode.STORAGE_BACKPRESSURE_EXCEPTION == 72
    assert fluss.ErrorCode.INVALID_BUCKET_ROUTING == 74
    error = fluss.FlussError("backpressure", 72)
    assert error.is_retriable
