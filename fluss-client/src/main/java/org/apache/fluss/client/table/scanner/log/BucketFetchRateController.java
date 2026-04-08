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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.TableBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Controls the fetch rate for individual buckets based on the amount of data returned in recent
 * fetches.
 *
 * <p>For buckets that consistently return data, they are fetched every round. For buckets that
 * return no data for consecutive fetches, the fetch frequency is progressively reduced using
 * exponential backoff. This is particularly useful for partitioned tables where only the latest
 * partitions have active data, avoiding wasted CPU and network resources on empty partitions.
 *
 * <p>The backoff schedule is: after 1 empty fetch, skip 1 round; after 2 empty fetches, skip 2
 * rounds; after 3, skip 4 rounds; and so on (powers of 2), up to {@code maxSkipRounds}. Any fetch
 * that returns data immediately resets the backoff to zero.
 *
 * <p>This class is NOT thread-safe. Callers must ensure proper synchronization.
 */
@Internal
class BucketFetchRateController {

    private static final Logger LOG = LoggerFactory.getLogger(BucketFetchRateController.class);

    /** Maximum exponent for the exponential backoff (2^5 = 32). */
    private static final int MAX_BACKOFF_SHIFT = 5;

    private final int maxSkipRounds;
    private final Map<TableBucket, BucketFetchState> bucketStates;

    BucketFetchRateController(int maxSkipRounds) {
        this.maxSkipRounds = maxSkipRounds;
        this.bucketStates = new HashMap<>();
    }

    /**
     * Determines whether the given bucket should be included in the current fetch round.
     *
     * <p>If the bucket is in a cool down period (i.e., it has been returning empty results), this
     * method decrements the remaining skip count and returns {@code false}. Otherwise, it returns
     * {@code true} indicating the bucket should be fetched.
     *
     * @param tableBucket the bucket to check
     * @return {@code true} if the bucket should be fetched in this round
     */
    boolean shouldFetch(TableBucket tableBucket) {
        BucketFetchState state = bucketStates.get(tableBucket);
        if (state == null) {
            return true;
        }
        if (state.remainingSkipRounds > 0) {
            state.remainingSkipRounds--;
            LOG.trace(
                    "Adaptive fetch: skipping bucket {} (consecutive empty fetches: {}, "
                            + "remaining skip rounds: {})",
                    tableBucket,
                    state.consecutiveEmptyFetches,
                    state.remainingSkipRounds);
            return false;
        }
        return true;
    }

    /**
     * Records the result of a fetch for the given bucket, adjusting future fetch frequency.
     *
     * <p>If the fetch returned data, the backoff is immediately reset to zero. If the fetch
     * returned no data, the consecutive empty count is incremented and a new skip interval is
     * calculated using exponential backoff.
     *
     * @param tableBucket the bucket that was fetched
     * @param hasRecords {@code true} if the fetch returned any records
     */
    void recordFetchResult(TableBucket tableBucket, boolean hasRecords) {
        BucketFetchState state =
                bucketStates.computeIfAbsent(tableBucket, k -> new BucketFetchState());
        if (hasRecords) {
            if (state.consecutiveEmptyFetches > 0) {
                LOG.info(
                        "Adaptive fetch: bucket {} recovered from cool down "
                                + "(was throttled after {} consecutive empty fetches)",
                        tableBucket,
                        state.consecutiveEmptyFetches);
            }
            state.consecutiveEmptyFetches = 0;
            state.remainingSkipRounds = 0;
        } else {
            state.consecutiveEmptyFetches++;
            int shift = Math.min(state.consecutiveEmptyFetches - 1, MAX_BACKOFF_SHIFT);
            state.remainingSkipRounds = Math.min(1 << shift, maxSkipRounds);
            LOG.info(
                    "Adaptive fetch: bucket {} has no data, entering cool down "
                            + "(consecutive empty fetches: {}, will skip {} rounds)",
                    tableBucket,
                    state.consecutiveEmptyFetches,
                    state.remainingSkipRounds);
        }
    }

    /** Removes the tracking state for the given bucket. */
    void removeBucket(TableBucket tableBucket) {
        bucketStates.remove(tableBucket);
    }

    /** Resets all tracking state. */
    void reset() {
        bucketStates.clear();
    }

    /** Returns the current number of remaining skip rounds for the given bucket, for testing. */
    @VisibleForTesting
    int getRemainingSkipRounds(TableBucket tableBucket) {
        BucketFetchState state = bucketStates.get(tableBucket);
        return state == null ? 0 : state.remainingSkipRounds;
    }

    /** Returns the number of consecutive empty fetches for the given bucket, for testing. */
    @VisibleForTesting
    int getConsecutiveEmptyFetches(TableBucket tableBucket) {
        BucketFetchState state = bucketStates.get(tableBucket);
        return state == null ? 0 : state.consecutiveEmptyFetches;
    }

    /** Per-bucket fetch state tracking consecutive empty fetches and cool down. */
    private static class BucketFetchState {
        int consecutiveEmptyFetches = 0;
        int remainingSkipRounds = 0;
    }
}
