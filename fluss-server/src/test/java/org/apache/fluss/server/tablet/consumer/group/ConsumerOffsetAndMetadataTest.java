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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ConsumerOffsetAndMetadata}. */
class ConsumerOffsetAndMetadataTest {

    @Test
    void testNullMetadataIsNormalized() {
        ConsumerOffsetAndMetadata offsetAndMetadata = new ConsumerOffsetAndMetadata(10L, null);

        assertThat(offsetAndMetadata.getMetadata())
                .isEqualTo(ConsumerOffsetAndMetadata.EMPTY_METADATA);
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
        ConsumerOffsetAndMetadata offsetAndMetadata =
                new ConsumerOffsetAndMetadata(-1L, ConsumerOffsetAndMetadata.EMPTY_METADATA);

        assertThat(offsetAndMetadata.getOffset()).isEqualTo(-1L);
    }
}
