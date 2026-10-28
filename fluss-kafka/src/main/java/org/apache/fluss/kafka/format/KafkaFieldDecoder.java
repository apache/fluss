/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.format;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

/** Decodes one nullable Kafka record value into its ordered Fluss field projection. */
@Internal
public interface KafkaFieldDecoder {

    /**
     * Decodes Kafka value bytes into internal Fluss field values.
     *
     * <p>The result separates the ordered value projection from optional JSON rescue. Decoder
     * instances are shared by cached write plans and must support concurrent calls without
     * retaining per-record state.
     *
     * <p>The input is borrowed and must not be modified. Returned values may borrow its storage;
     * callers must encode them synchronously before the input can be reused.
     */
    KafkaDecodedValue decode(@Nullable ByteBuffer bytes);
}
