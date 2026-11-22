/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.row.BinaryString;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ConsumerOffsetAndMetadata}. */
class ConsumerOffsetAndMetadataTest {

    @Test
    void testOmittedAndEmptyMetadata() {
        ConsumerOffsetAndMetadata omitted = new ConsumerOffsetAndMetadata(10L, null);
        ConsumerOffsetAndMetadata empty = new ConsumerOffsetAndMetadata(10L, "");

        assertThat(omitted.getMetadata()).isNull();
        assertThat(empty.getMetadata()).isEqualTo(BinaryString.EMPTY_UTF8);
        assertThat(omitted).isNotEqualTo(empty);
        ConsumerOffsetAndMetadata otherOmitted = new ConsumerOffsetAndMetadata(10L, null);
        assertThat(omitted).isEqualTo(otherOmitted).hasSameHashCodeAs(otherOmitted);
        assertThat(omitted.isMetadataSizeValid(0)).isTrue();
        assertThat(empty.isMetadataSizeValid(0)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({"abcd,4", "中,3", "😀,4"})
    void testMetadataSizeIsMeasuredInUtf8Bytes(String metadata, int bytes) {
        ConsumerOffsetAndMetadata value = new ConsumerOffsetAndMetadata(10L, metadata);
        assertThat(value.isMetadataSizeValid(bytes)).isTrue();
        assertThat(value.isMetadataSizeValid(bytes - 1)).isFalse();
    }

    @Test
    void testValueEquality() {
        ConsumerOffsetAndMetadata first = new ConsumerOffsetAndMetadata(10L, "metadata");
        ConsumerOffsetAndMetadata second = new ConsumerOffsetAndMetadata(10L, "metadata");

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
        assertThat(first).isNotEqualTo(new ConsumerOffsetAndMetadata(11L, "metadata"));
        assertThat(first).isNotEqualTo(new ConsumerOffsetAndMetadata(10L, "other"));
    }

    @Test
    void testNegativeOffsetIsAllowedForInternalSentinels() {
        ConsumerOffsetAndMetadata offsetAndMetadata = new ConsumerOffsetAndMetadata(-1L, null);

        assertThat(offsetAndMetadata.getOffset()).isEqualTo(-1L);
    }
}
