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

package org.apache.fluss.utils;

import org.apache.fluss.utils.function.ThrowingSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.function.Predicate;

/**
 * Utility class for executing operations with retry logic.
 *
 * <p>This class provides methods to execute operations that may fail transiently, with configurable
 * retry count and exponential backoff.
 */
public final class RetryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RetryUtils.class);

    private RetryUtils() {}

    /**
     * Executes an operation with retry logic using exponential backoff.
     *
     * @param operation the operation to execute
     * @param operationName a descriptive name for logging
     * @param maxRetries maximum number of retry attempts
     * @param initialBackoffMs initial backoff delay in milliseconds
     * @param maxBackoffMs maximum backoff delay in milliseconds
     * @param retryPredicate predicate to determine if an exception should trigger a retry
     * @param <T> the return type of the operation
     * @return the result of the operation
     * @throws IOException if all retries are exhausted or a non-retryable exception occurs
     * @throws InterruptedIOException if the thread is interrupted during retry backoff
     */
    public static <T> T executeWithRetry(
            ThrowingSupplier<T, ? extends Exception> operation,
            String operationName,
            int maxRetries,
            long initialBackoffMs,
            long maxBackoffMs,
            Predicate<Exception> retryPredicate)
            throws IOException {

        Exception lastException = null;
        long backoffMs = initialBackoffMs;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return operation.get();
            } catch (Exception e) {
                // Check if the exception is an InterruptedException or wraps one
                if (isInterruptedException(e)) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException(
                            operationName + " was interrupted on attempt " + attempt);
                }

                lastException = e;

                if (!retryPredicate.test(e)) {
                    // Non-retryable exception
                    throw wrapAsIOException(e, operationName);
                }

                if (attempt < maxRetries) {
                    LOG.warn(
                            "{} failed (attempt {}/{}), retrying in {} ms",
                            operationName,
                            attempt,
                            maxRetries,
                            backoffMs,
                            e);
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException(
                                operationName
                                        + " was interrupted while waiting for retry after attempt "
                                        + attempt);
                    }
                    backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
                }
            }
        }

        throw new IOException(
                String.format("%s failed after %d attempts", operationName, maxRetries),
                lastException);
    }

    /**
     * Executes an IO operation with retry logic. Retries on any IOException.
     *
     * @param operation the operation to execute
     * @param operationName a descriptive name for logging
     * @param maxRetries maximum number of retry attempts
     * @param initialBackoffMs initial backoff delay in milliseconds
     * @param maxBackoffMs maximum backoff delay in milliseconds
     * @param <T> the return type of the operation
     * @return the result of the operation
     * @throws IOException if all retries are exhausted or a non-retryable exception occurs
     * @throws InterruptedIOException if the thread is interrupted during retry backoff
     */
    public static <T> T executeIOWithRetry(
            ThrowingSupplier<T, ? extends Exception> operation,
            String operationName,
            int maxRetries,
            long initialBackoffMs,
            long maxBackoffMs)
            throws IOException {
        return executeWithRetry(
                operation,
                operationName,
                maxRetries,
                initialBackoffMs,
                maxBackoffMs,
                e -> e instanceof IOException);
    }

    /**
     * Checks if the exception is an InterruptedException or wraps one.
     *
     * @param e the exception to check
     * @return true if the exception is or wraps an InterruptedException
     */
    private static boolean isInterruptedException(Exception e) {
        Throwable current = e;
        while (current != null) {
            if (current instanceof InterruptedException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static IOException wrapAsIOException(Exception e, String operationName) {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        return new IOException(operationName + " failed", e);
    }
}
