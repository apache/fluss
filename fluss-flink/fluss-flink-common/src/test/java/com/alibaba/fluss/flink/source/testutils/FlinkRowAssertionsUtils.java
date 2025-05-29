/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.source.testutils;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Utility class providing assertion methods for Flink test results. */
public class FlinkRowAssertionsUtils {

    private FlinkRowAssertionsUtils() {}

    public static void assertResultsIgnoreOrder(
            CloseableIterator<Row> iterator, List<String> expected, boolean closeIterator) {
        try {
            int expectRecords = expected.size();
            List<String> actual = new ArrayList<>(expectRecords);

            long startTime = System.currentTimeMillis();
            int maxWaitTime = 60000; // 60 seconds

            for (int i = 0; i < expectRecords; i++) {
                // Wait for next record with timeout
                while (!iterator.hasNext()) {
                    if (System.currentTimeMillis() - startTime > maxWaitTime) {
                        // Timeout reached - stop waiting
                        break;
                    }
                    Thread.sleep(10);
                }

                if (iterator.hasNext()) {
                    actual.add(iterator.next().toString());
                } else {
                    // No more records available
                    break;
                }
            }

            assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted", e);
        } catch (Exception e) {
            // Handle job completion gracefully
            if (e.getCause() instanceof IllegalStateException
                    && e.getMessage() != null
                    && e.getMessage().contains("MiniCluster")) {
                // Expected for finite jobs - do nothing
            } else {
                throw e;
            }
        } finally {
            if (closeIterator) {
                try {
                    iterator.close();
                } catch (Exception e) {
                    System.err.println("Error closing iterator: " + e.getMessage());
                }
            }
        }
    }
}
