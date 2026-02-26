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

package org.apache.fluss.record;

import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.util.CommonUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom rounding policy that reduces Arrow's chunk size from 16MB to 4MB to align with Netty
 * 4.1+ memory allocation behavior.
 *
 * <p>Arrow's default maxOrder=11 (16MB chunks) can cause memory inefficiency when used with Netty's
 * maxOrder=9 (4MB chunks). This class patches the default by using maxOrder=9.
 *
 * <p>TODO: Remove this once fixed upstream in <a
 * href="https://github.com/apache/arrow-java/issues/1040">apache/arrow-java#1040</a>.
 */
public class FlussRoundingPolicy implements RoundingPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(FlussRoundingPolicy.class);
    public final long chunkSize;
    private static final long MIN_PAGE_SIZE = 4096;
    private static final long MAX_CHUNK_SIZE = ((long) Integer.MAX_VALUE + 1) / 2;
    private static final long DEFAULT_CHUNK_SIZE;

    static {
        long defaultPageSize =
                Long.parseLong(System.getProperty("org.apache.memory.allocator.pageSize", "8192"));
        try {
            validateAndCalculatePageShifts(defaultPageSize);
        } catch (Throwable t) {
            defaultPageSize = 8192;
        }

        int defaultMaxOrder =
                Integer.parseInt(System.getProperty("org.apache.memory.allocator.maxOrder", "9"));
        try {
            validateAndCalculateChunkSize(defaultPageSize, defaultMaxOrder);
        } catch (Throwable t) {
            defaultMaxOrder = 11;
        }
        DEFAULT_CHUNK_SIZE = validateAndCalculateChunkSize(defaultPageSize, defaultMaxOrder);
        if (LOG.isDebugEnabled()) {
            LOG.debug("-Dorg.apache.memory.allocator.pageSize: {}", defaultPageSize);
            LOG.debug("-Dorg.apache.memory.allocator.maxOrder: {}", defaultMaxOrder);
        }
    }

    /**
     * Validates page size (must be >= 4096 and power of 2) and returns log2(pageSize).
     *
     * @throws IllegalArgumentException if validation fails
     */
    private static long validateAndCalculatePageShifts(long pageSize) {
        if (pageSize < MIN_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "pageSize: " + pageSize + " (expected: " + MIN_PAGE_SIZE + ")");
        }

        if ((pageSize & pageSize - 1) != 0) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
        }

        // Logarithm base 2. At this point we know that pageSize is a power of two.
        return Long.SIZE - 1L - Long.numberOfLeadingZeros(pageSize);
    }

    private static long validateAndCalculateChunkSize(long pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        long chunkSize = pageSize;
        for (long i = maxOrder; i > 0; i--) {
            if (chunkSize > MAX_CHUNK_SIZE / 2) {
                throw new IllegalArgumentException(
                        String.format(
                                "pageSize (%d) << maxOrder (%d) must not exceed %d",
                                pageSize, maxOrder, MAX_CHUNK_SIZE));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }

    /** The singleton instance with default chunk size. */
    public static final FlussRoundingPolicy DEFAULT_ROUNDING_POLICY =
            new FlussRoundingPolicy(DEFAULT_CHUNK_SIZE);

    private FlussRoundingPolicy(long chunkSize) {
        this.chunkSize = chunkSize;
    }

    /** Rounds request size to next power of 2 if less than chunk size, otherwise returns as-is. */
    @Override
    public long getRoundedSize(long requestSize) {
        return requestSize < chunkSize ? CommonUtil.nextPowerOfTwo(requestSize) : requestSize;
    }
}
