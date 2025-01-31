/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.compression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ArrowCompressionRatioEstimator}. */
public class ArrowCompressionRatioEstimatorTest {

    private ArrowCompressionRatioEstimator compressionRatioEstimator;

    @BeforeEach
    public void setup() {
        compressionRatioEstimator = new ArrowCompressionRatioEstimator();
    }

    @Test
    void testUpdateEstimation() {
        class EstimationsObservedRatios {
            final float currentEstimation;
            final float observedRatio;

            EstimationsObservedRatios(float currentEstimation, float observedRatio) {
                this.currentEstimation = currentEstimation;
                this.observedRatio = observedRatio;
            }
        }

        // If currentEstimation is smaller than observedRatio, the updatedCompressionRatio is
        // currentEstimation plus COMPRESSION_RATIO_DETERIORATE_STEP(0.05), otherwise
        // currentEstimation minus COMPRESSION_RATIO_IMPROVING_STEP(0.005). There are four cases,
        // and updatedCompressionRatio shouldn't smaller than observedRatio in all cases.
        List<EstimationsObservedRatios> estimationsObservedRatios =
                Arrays.asList(
                        new EstimationsObservedRatios(0.8f, 0.84f),
                        new EstimationsObservedRatios(0.6f, 0.7f),
                        new EstimationsObservedRatios(0.6f, 0.4f),
                        new EstimationsObservedRatios(0.004f, 0.001f));
        for (EstimationsObservedRatios estimationObservedRatio : estimationsObservedRatios) {
            compressionRatioEstimator.updateEstimation(estimationObservedRatio.currentEstimation);
            float updatedCompressionRatio = compressionRatioEstimator.estimation();
            assertThat(updatedCompressionRatio)
                    .isGreaterThanOrEqualTo(estimationObservedRatio.observedRatio);
        }
    }
}
