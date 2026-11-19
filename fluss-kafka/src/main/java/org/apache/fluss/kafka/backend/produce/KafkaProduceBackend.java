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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.annotation.Internal;

import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Narrow backend used by the Kafka Produce API. */
@Internal
public interface KafkaProduceBackend extends AutoCloseable {
    /**
     * Consumes Kafka records synchronously and writes them through the native Fluss write path.
     *
     * <p>All accesses to borrowed command payloads must finish before this method returns or
     * throws. Record keys and values are borrowed read-only views of storage that may belong to the
     * network request; header values are borrowed byte arrays. The caller keeps these bytes alive
     * and unchanged during this call, and consumers must not modify them. Input storage can be
     * released or reused as soon as request handling returns, without waiting for the returned
     * future. Read-only views do not extend the storage's lifetime.
     *
     * <p>Implementations must decode the payloads into independent objects (for example JSON
     * objects or Fluss rows) before returning. Asynchronous work must use only independently owned
     * data and must not retain the command's records, buffers or header arrays. The returned future
     * represents completion of the write, not permission to continue reading the input payloads.
     *
     * <p>The returned future completes with one {@link TopicProduceResponse} per command topic in
     * command order, each holding one partition response per command partition in command order,
     * under the original request topic names. Every written partition must get a response entry; a
     * client waiting on an omitted entry hangs until its delivery timeout.
     */
    CompletableFuture<List<TopicProduceResponse>> write(KafkaProduceCommand command);

    /** Releases backend resources after request processing stops. */
    @Override
    default void close() throws Exception {}
}
