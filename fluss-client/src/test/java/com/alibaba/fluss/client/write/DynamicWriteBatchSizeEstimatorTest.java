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

import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DynamicWriteBatchSizeEstimator}. */
public class DynamicWriteBatchSizeEstimatorTest {

    private DynamicWriteBatchSizeEstimator estimator;

    @BeforeEach
    public void setup() {
        estimator = new DynamicWriteBatchSizeEstimator(true, 1000, 100);
    }

    @Test
    void testEstimator() {
        assertThat(estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(1000);
        estimator = new DynamicWriteBatchSizeEstimator(false, 1000, 100);
        assertThat(estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(1000);

        estimator = new DynamicWriteBatchSizeEstimator(true, 1000, 100);
        // test decrease 5%
        estimator.updateEstimation(DATA1_PHYSICAL_TABLE_PATH, 450);
        int expectedSize = 950;
        assertThat(estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH))
                .isEqualTo(expectedSize);

        // test increase 5%
        estimator.updateEstimation(DATA1_PHYSICAL_TABLE_PATH, 350);
        expectedSize = (int) (950 * 0.95);
        assertThat(estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH))
                .isEqualTo(expectedSize);

        // test increase 10%
        estimator.updateEstimation(DATA1_PHYSICAL_TABLE_PATH, 930);
        assertThat(estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH))
                .isEqualTo((int) (expectedSize * 1.1));
    }

    @Test
    void testMinDecreaseToPageSize() {
        int estimatedSize = estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH);
        estimator.updateEstimation(DATA1_PHYSICAL_TABLE_PATH, 1000);
        while (estimatedSize > 2 * 100) {
            estimator.updateEstimation(DATA1_PHYSICAL_TABLE_PATH, (int) (estimatedSize * 0.5) - 10);
            estimatedSize = estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH);
        }

        assertThat(estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(200);
        estimator.updateEstimation(DATA1_PHYSICAL_TABLE_PATH, 0);
        assertThat(estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(200);
    }

    @Test
    void testMaxIncreaseToMaxBatchSize() {
        assertThat(estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(1000);
        estimator.updateEstimation(DATA1_PHYSICAL_TABLE_PATH, 2000);
        assertThat(estimator.getEstimatedBatchSize(DATA1_PHYSICAL_TABLE_PATH)).isEqualTo(1000);
    }
}
