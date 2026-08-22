/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Thrown by a tablet server to reject writes when a KV write-pressure source reaches its defensive
 * limit, such as RocksDB write pressure or the TabletServer-wide KV pre-write-buffer memory limit.
 *
 * <p>This is the second tier of the cooperative backpressure model: the first tier is the proactive
 * client-side throttle (driven by pressure piggybacked on responses); once a hard limit is reached,
 * the server short-circuits the write and returns this retriable exception. This prevents request
 * processing threads from blocking on the storage engine and provides a last line of defense
 * against memory exhaustion.
 *
 * <p>Clients should retry after a backoff equal to the configured throttle ceiling.
 *
 * @since 1.0
 */
@PublicEvolving
public class StorageBackpressureException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public StorageBackpressureException(String message) {
        super(message);
    }
}
