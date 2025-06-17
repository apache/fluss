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

package com.alibaba.fluss.flink.source.testutils;

import org.apache.flink.table.api.TableEnvironment;
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
        List<String> actual = collectRowsWithTimeout(iterator, expected.size(), closeIterator);
        assertThat(actual)
                .as(
                        "Expected %d records but got %d after waiting. Actual results: %s",
                        expected.size(), actual.size(), actual)
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    public static void assertQueryResultExactOrder(
            TableEnvironment env, String query, List<String> expected) throws Exception {
        try (CloseableIterator<Row> rowIter = env.executeSql(query).collect()) {
            List<String> actual = collectRowsWithTimeout(rowIter, expected.size(), true);
            assertThat(actual)
                    .as(
                            "Expected %d records in exact order but got %d after waiting. Query: %s, Actual results: %s",
                            expected.size(), actual.size(), query, actual)
                    .containsExactlyElementsOf(expected);
        }
    }

    private static List<String> collectRowsWithTimeout(
            CloseableIterator<Row> iterator, int expectedCount, boolean closeIterator) {
        List<String> actual = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        int maxWaitTime = 60000; // 60 seconds

        try {
            for (int i = 0; i < expectedCount; i++) {
                // Wait for next record with timeout
                while (!iterator.hasNext()) {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime > maxWaitTime) {
                        // Timeout reached - provide detailed failure info
                        throw new AssertionError(
                                String.format(
                                        "Timeout after waiting %d ms for Flink job results. "
                                                + "Expected %d records but only received %d. "
                                                + "This might indicate a job hang or insufficient data generation.",
                                        elapsedTime, expectedCount, actual.size()));
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

            return actual;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for Flink job results", e);
        } catch (AssertionError e) {
            // Re-throw our timeout assertion errors
            throw e;
        } catch (Exception e) {
            // Handle job completion gracefully
            if (isMiniClusterCompletionException(e)) {
                // Job completed normally - return what we have
                return actual;
            } else {
                long elapsedTime = System.currentTimeMillis() - startTime;
                throw new RuntimeException(
                        String.format(
                                "Unexpected error after waiting %d ms for Flink job results. "
                                        + "Expected %d records but got %d before error occurred.",
                                elapsedTime, expectedCount, actual.size()),
                        e);
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

    private static boolean isMiniClusterCompletionException(Exception e) {
        return e.getCause() instanceof IllegalStateException
                && e.getMessage() != null
                && e.getMessage().contains("MiniCluster");
    }
}
