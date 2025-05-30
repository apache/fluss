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

package com.alibaba.fluss.client.write;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.alibaba.fluss.client.write.DynamicWriteBatchSizeEstimator.INCREASE_RATIO;
import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DynamicWriteBatchSizeEstimator}. */
public class DynamicWriteBatchSizeEstimatorTest {

    private DynamicWriteBatchSizeEstimator estimator;

    @BeforeEach
    public void setup() {
        estimator = new DynamicWriteBatchSizeEstimator(1024, 1024 * 10, 128, 0);
    }

    @Test
    void testEstimator() {
        assertThat(estimator.batchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(1024);

        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 500);
        assertThat(estimator.batchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(1024);

        estimator.dynamicEstimateBatchSize();
        assertThat(estimator.batchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(1024 / 2);
    }

    @Test
    void testZeroProtected() {
        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 0);
        estimator.dynamicEstimateBatchSize();
        assertThat(estimator.batchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(512);

        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 0);
        estimator.dynamicEstimateBatchSize();
        assertThat(estimator.batchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(256);

        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 0);
        estimator.dynamicEstimateBatchSize();
        assertThat(estimator.batchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(128);

        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 0);
        estimator.dynamicEstimateBatchSize();
        assertThat(estimator.batchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(128);
    }

    @Test
    void testDynamicAdjustmentWithinThreshold() {
        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 950);
        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 960);
        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 940);

        // Buffer Should increase by INCREASE_RATIO.
        estimator.dynamicEstimateBatchSize();
        assertThat(estimator.batchSize(DATA1_PHYSICAL_TABLE_PATH))
                .isEqualTo((int) Math.round(1024 * (1 + INCREASE_RATIO)));
    }

    @Test
    void testDynamicAdjustmentBeyondThreshold() {
        // Setup test data with difference > DIFF_RATIO_THRESHOLD.
        DynamicWriteBatchSizeEstimator estimator =
                new DynamicWriteBatchSizeEstimator(1024, 1024 * 10, 128, 0);

        // Setup test data with difference >10%
        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 500);
        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 550);
        estimator.recordNewBatchSize(DATA1_PHYSICAL_TABLE_PATH, 450);

        // Buffer should decrease to max(current/2, 2*pageSize).
        estimator.dynamicEstimateBatchSize();
        assertThat(estimator.batchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(1024 / 2);
    }
}
