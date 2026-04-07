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

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BucketFetchRateController}. */
class BucketFetchRateControllerTest {

    private static final int MAX_SKIP_ROUNDS = 32;
    private BucketFetchRateController controller;

    @BeforeEach
    void setup() {
        controller = new BucketFetchRateController(MAX_SKIP_ROUNDS);
    }

    @Test
    void testNewBucketShouldAlwaysFetch() {
        TableBucket tb = new TableBucket(1L, 0L, 0);
        assertThat(controller.shouldFetch(tb)).isTrue();
    }

    @Test
    void testBucketWithDataResetsCoolDown() {
        TableBucket tb = new TableBucket(1L, 0L, 0);

        // Record multiple empty fetches to build up cool down
        controller.recordFetchResult(tb, false);
        controller.recordFetchResult(tb, false);
        controller.recordFetchResult(tb, false);
        assertThat(controller.getConsecutiveEmptyFetches(tb)).isEqualTo(3);
        assertThat(controller.getRemainingSkipRounds(tb)).isGreaterThan(0);

        // Now record a fetch with data
        controller.recordFetchResult(tb, true);
        assertThat(controller.getConsecutiveEmptyFetches(tb)).isEqualTo(0);
        assertThat(controller.getRemainingSkipRounds(tb)).isEqualTo(0);
        assertThat(controller.shouldFetch(tb)).isTrue();
    }

    @Test
    void testExponentialBackoff() {
        TableBucket tb = new TableBucket(1L, 0L, 0);

        // 1st empty fetch: skip 1 round (2^0 = 1)
        controller.recordFetchResult(tb, false);
        assertThat(controller.getRemainingSkipRounds(tb)).isEqualTo(1);

        // Consume the skip round
        assertThat(controller.shouldFetch(tb)).isFalse();
        assertThat(controller.shouldFetch(tb)).isTrue();

        // 2nd empty fetch: skip 2 rounds (2^1 = 2)
        controller.recordFetchResult(tb, false);
        assertThat(controller.getRemainingSkipRounds(tb)).isEqualTo(2);

        // Consume the 2 skip rounds
        assertThat(controller.shouldFetch(tb)).isFalse();
        assertThat(controller.shouldFetch(tb)).isFalse();
        assertThat(controller.shouldFetch(tb)).isTrue();

        // 3rd empty fetch: skip 4 rounds (2^2 = 4)
        controller.recordFetchResult(tb, false);
        assertThat(controller.getRemainingSkipRounds(tb)).isEqualTo(4);

        for (int i = 0; i < 4; i++) {
            assertThat(controller.shouldFetch(tb)).isFalse();
        }
        assertThat(controller.shouldFetch(tb)).isTrue();
    }

    @Test
    void testMaxSkipRoundsCap() {
        TableBucket tb = new TableBucket(1L, 0L, 0);

        // Record many empty fetches to exceed the max backoff shift
        for (int i = 0; i < 20; i++) {
            controller.recordFetchResult(tb, false);
            // Consume all skip rounds
            while (!controller.shouldFetch(tb)) {
                // draining
            }
        }

        // After 20 empty fetches, skip rounds should be capped at MAX_SKIP_ROUNDS
        controller.recordFetchResult(tb, false);
        assertThat(controller.getRemainingSkipRounds(tb)).isLessThanOrEqualTo(MAX_SKIP_ROUNDS);
    }

    @Test
    void testMultipleBucketsIndependent() {
        TableBucket tb1 = new TableBucket(1L, 0L, 0);
        TableBucket tb2 = new TableBucket(1L, 1L, 0);

        // tb1 gets empty fetches, tb2 gets data
        controller.recordFetchResult(tb1, false);
        controller.recordFetchResult(tb1, false);
        controller.recordFetchResult(tb2, true);

        // tb1 should be in cool down
        assertThat(controller.getConsecutiveEmptyFetches(tb1)).isEqualTo(2);
        assertThat(controller.getRemainingSkipRounds(tb1)).isGreaterThan(0);

        // tb2 should be fetch able immediately
        assertThat(controller.getConsecutiveEmptyFetches(tb2)).isEqualTo(0);
        assertThat(controller.shouldFetch(tb2)).isTrue();
    }

    @Test
    void testRemoveBucket() {
        TableBucket tb = new TableBucket(1L, 0L, 0);

        controller.recordFetchResult(tb, false);
        assertThat(controller.getConsecutiveEmptyFetches(tb)).isEqualTo(1);

        controller.removeBucket(tb);
        assertThat(controller.getConsecutiveEmptyFetches(tb)).isEqualTo(0);
        assertThat(controller.shouldFetch(tb)).isTrue();
    }

    @Test
    void testReset() {
        TableBucket tb1 = new TableBucket(1L, 0L, 0);
        TableBucket tb2 = new TableBucket(1L, 1L, 0);

        controller.recordFetchResult(tb1, false);
        controller.recordFetchResult(tb2, false);

        controller.reset();

        assertThat(controller.getConsecutiveEmptyFetches(tb1)).isEqualTo(0);
        assertThat(controller.getConsecutiveEmptyFetches(tb2)).isEqualTo(0);
        assertThat(controller.shouldFetch(tb1)).isTrue();
        assertThat(controller.shouldFetch(tb2)).isTrue();
    }

    @Test
    void testShouldFetchDecrementsCounter() {
        TableBucket tb = new TableBucket(1L, 0L, 0);

        // 1 empty fetch -> 1 skip round
        controller.recordFetchResult(tb, false);
        assertThat(controller.getRemainingSkipRounds(tb)).isEqualTo(1);

        // shouldFetch returns false and decrements to 0
        assertThat(controller.shouldFetch(tb)).isFalse();
        assertThat(controller.getRemainingSkipRounds(tb)).isEqualTo(0);

        // Now it should be fetch able
        assertThat(controller.shouldFetch(tb)).isTrue();
    }

    @Test
    void testSmallMaxSkipRounds() {
        BucketFetchRateController smallController = new BucketFetchRateController(4);
        TableBucket tb = new TableBucket(1L, 0L, 0);

        // Record many empty fetches
        for (int i = 0; i < 10; i++) {
            smallController.recordFetchResult(tb, false);
            // Drain skip rounds
            while (!smallController.shouldFetch(tb)) {
                // draining
            }
        }

        // Even after many empty fetches, skip rounds should be capped at 4
        smallController.recordFetchResult(tb, false);
        assertThat(smallController.getRemainingSkipRounds(tb)).isLessThanOrEqualTo(4);
    }
}
