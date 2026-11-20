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

package org.apache.fluss.kafka.api.produce;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.BufferUnderflowException;

import static org.assertj.core.api.Assertions.assertThat;

/** Codec error classification preserves specific protocol failures through nested wrappers. */
class ProduceCompressionErrorTest {
    @Test
    void testCodecIoFailureIsAnInvalidRecord() {
        KafkaException failure =
                new KafkaException("codec wrapper", new IOException("broken codec stream"));
        RuntimeException normalized = ProduceHandler.normalizeRecordCopyFailure(failure);
        assertThat(normalized)
                .isInstanceOf(InvalidRecordException.class)
                .hasMessageContaining("java.io.IOException: broken codec stream")
                .hasCause(failure);
        assertThat(Errors.forException(normalized)).isEqualTo(Errors.INVALID_RECORD);
    }

    @Test
    void testCodecFailureWithoutMessagePreservesExceptionType() {
        KafkaException failure = new KafkaException(new BufferUnderflowException());
        RuntimeException normalized = ProduceHandler.normalizeRecordCopyFailure(failure);
        assertThat(normalized)
                .isInstanceOf(InvalidRecordException.class)
                .hasMessageContaining("java.nio.BufferUnderflowException")
                .hasCause(failure);
        assertThat(Errors.forException(normalized)).isEqualTo(Errors.INVALID_RECORD);
    }

    @Test
    void testSpecificApiFailureSurvivesIoAndCodecWrappers() {
        RecordTooLargeException limit = new RecordTooLargeException("copy limit");
        assertThat(
                        ProduceHandler.normalizeRecordCopyFailure(
                                new KafkaException(new IOException(limit))))
                .isSameAs(limit);
        TimeoutException timeout = new TimeoutException("admission timeout");
        assertThat(ProduceHandler.normalizeRecordCopyFailure(new KafkaException(timeout)))
                .isSameAs(timeout);
    }

    @Test
    void testUnexpectedRuntimeFailureIsNotReportedAsCorruptData() {
        KafkaException failure =
                new KafkaException(new IllegalStateException("unexpected failure"));
        assertThat(ProduceHandler.normalizeRecordCopyFailure(failure)).isSameAs(failure);
    }
}
