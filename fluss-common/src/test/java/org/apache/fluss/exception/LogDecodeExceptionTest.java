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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.InstantiationUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogDecodeException}. */
class LogDecodeExceptionTest {

    @ParameterizedTest
    @MethodSource("nextOffsets")
    void testSerializationRoundTrip(OptionalLong nextOffset) throws Exception {
        LogDecodeException original =
                new LogDecodeException(
                        "Failed to decode",
                        new CorruptMessageException("Record batch is corrupt"),
                        new TableBucket(1L, 2L, 3),
                        10L,
                        nextOffset);

        LogDecodeException copy = InstantiationUtils.clone(original);

        assertThat(copy).isNotSameAs(original).hasMessage("Failed to decode");
        assertThat(copy.getTableBucket()).isEqualTo(new TableBucket(1L, 2L, 3));
        assertThat(copy.getOffset()).isEqualTo(10L);
        assertThat(copy.getNextOffset()).isEqualTo(nextOffset);
        assertThat(copy)
                .cause()
                .isInstanceOf(CorruptMessageException.class)
                .hasMessage("Record batch is corrupt");
    }

    private static Collection<OptionalLong> nextOffsets() {
        return Arrays.asList(OptionalLong.of(20L), OptionalLong.empty());
    }
}
