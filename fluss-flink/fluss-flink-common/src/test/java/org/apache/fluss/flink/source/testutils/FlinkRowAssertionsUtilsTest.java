/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.source.testutils;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Test for {@link FlinkRowAssertionsUtils}. */
class FlinkRowAssertionsUtilsTest {

    @Test
    void testCollectRowsWithTimeout() {
        // should throw AssertionError if wait rows timeout
        assertThatThrownBy(
                        () ->
                                collectRowsWithTimeout(
                                        createBlockingHasNextIterator(),
                                        10,
                                        true,
                                        Duration.ofSeconds(1)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Timeout after waiting")
                .hasMessageContaining(
                        "ms for Flink job results. Expected 10 records but only received 0.");
    }

    CloseableIterator<Row> createBlockingHasNextIterator() {
        return new CloseableIterator<Row>() {
            @Override
            public void close() throws Exception {}

            @SuppressWarnings("all")
            @Override
            public boolean hasNext() {
                // to mock blocking
                try {
                    while (true) {
                        Thread.sleep(1_000);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail("Thread sleeping for blocking hasNext() was interrupted.");
                }
                return true;
            }

            @Override
            public Row next() {
                return null;
            }
        };
    }
}
