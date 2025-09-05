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

package org.apache.fluss.flink.source.testutils;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Utility class providing assertion methods for Flink test results. */
public class FlinkRowAssertionsUtils {

    private FlinkRowAssertionsUtils() {}

    public static void assertRowResultsIgnoreOrder(
            CloseableIterator<Row> actual, List<Row> expectedRows, boolean closeIterator) {
        assertResultsIgnoreOrder(
                actual,
                expectedRows.stream().map(Row::toString).collect(Collectors.toList()),
                closeIterator);
    }

    public static void assertResultsIgnoreOrder(
            CloseableIterator<Row> iterator, List<String> expected, boolean closeIterator) {
        List<String> actual = collectRowsWithTimeout(iterator, expected.size(), closeIterator);
        assertThat(actual)
                .as(
                        "Expected %d records but got %d after waiting. Actual results: %s",
                        expected.size(), actual.size(), actual)
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    public static void assertResultsExactOrder(
            CloseableIterator<Row> iterator, List<Row> expected, boolean closeIterator) {
        List<String> actual = collectRowsWithTimeout(iterator, expected.size(), closeIterator);
        assertThat(actual)
                .as(
                        "Expected %d records but got %d after waiting. Actual results: %s",
                        expected.size(), actual.size(), actual)
                .containsExactlyElementsOf(
                        expected.stream().map(Row::toString).collect(Collectors.toList()));
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

    public static List<String> collectRowsWithTimeout(
            CloseableIterator<Row> iterator, int expectedCount) {
        return collectRowsWithTimeout(iterator, expectedCount, true);
    }

    public static List<String> collectRowsWithTimeout(
            CloseableIterator<Row> iterator, int expectedCount, boolean closeIterator) {
        if (expectedCount < 0) {
            throw new IllegalArgumentException(
                    "Expected count must be non-negative: " + expectedCount);
        }
        if (iterator == null) {
            throw new IllegalArgumentException("Iterator cannot be null");
        }
        return collectRowsWithTimeout(
                iterator,
                expectedCount,
                closeIterator,
                // max wait 1 minute
                Duration.ofMinutes(1));
    }

    protected static List<String> collectRowsWithTimeout(
            CloseableIterator<Row> iterator,
            int expectedCount,
            boolean closeIterator,
            Duration maxWaitTime) {
        List<String> actual = new ArrayList<>();
        long startTimeMs = System.currentTimeMillis();
        long deadlineTimeMs = startTimeMs + maxWaitTime.toMillis();
        try {
            for (int i = 0; i < expectedCount; i++) {
                // Wait for next record with timeout
                if (!waitForNextWithTimeout(
                        iterator, Math.max(deadlineTimeMs - System.currentTimeMillis(), 1_000))) {
                    throw timeoutError(
                            System.currentTimeMillis() - startTimeMs, expectedCount, actual.size());
                }
                if (iterator.hasNext()) {
                    actual.add(iterator.next().toString());
                } else {
                    // No more records available
                    break;
                }
            }
            return actual;
        } catch (AssertionError e) {
            // Re-throw our timeout assertion errors
            throw e;
        } catch (Exception e) {
            // Handle job completion gracefully
            if (isMiniClusterCompletionException(e)) {
                // Job completed normally - return what we have
                return actual;
            } else {
                long elapsedTime = System.currentTimeMillis() - startTimeMs;
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

    private static AssertionError timeoutError(
            long elapsedTime, int expectedCount, int actualCount) {
        return new AssertionError(
                String.format(
                        "Timeout after waiting %d ms for Flink job results. "
                                + "Expected %d records but only received %d. "
                                + "This might indicate a job hang or insufficient data generation.",
                        elapsedTime, expectedCount, actualCount));
    }

    private static boolean waitForNextWithTimeout(
            CloseableIterator<Row> iterator, long maxWaitTime) {
        CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(iterator::hasNext);
        System.out.println("Waiting for " + maxWaitTime + " ms to finish.");
        try {
            return future.get(maxWaitTime, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            System.err.println("Timeout waiting for " + maxWaitTime + " ms to finish.");
            future.cancel(true);
            return false;
        } catch (Exception e) {
            throw new RuntimeException("Error checking iterator.hasNext()", e);
        }
    }

    private static boolean isMiniClusterCompletionException(Exception e) {
        return e.getCause() instanceof IllegalStateException
                && e.getMessage() != null
                && e.getMessage().contains("MiniCluster");
    }
}
