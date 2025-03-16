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

package com.alibaba.fluss.compression;

import com.alibaba.fluss.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

/** This class helps estimate the compression ratio for a table. */
@Internal
@ThreadSafe
public class ArrowCompressionRatioEstimator {
    /**
     * The constant speed to increase compression ratio when a batch compresses better than
     * expected.
     */
    private static final float COMPRESSION_RATIO_IMPROVING_STEP = 0.005f;

    /**
     * The minimum speed to decrease compression ratio when a batch compresses worse than expected.
     */
    private static final float COMPRESSION_RATIO_DETERIORATE_STEP = 0.05f;

    /**
     * The default compression ratio when initialize a new {@link ArrowCompressionRatioEstimator}.
     */
    private static final float DEFAULT_COMPRESSION_RATIO = 1.0f;

    /** The current compression ratio, use volatile for lock-free. */
    private volatile float compressionRatio;

    public ArrowCompressionRatioEstimator() {
        compressionRatio = DEFAULT_COMPRESSION_RATIO;
    }

    /** Update the compression ratio estimation with the observed compression ratio. */
    public void updateEstimation(float observedRatio) {
        float currentEstimation = compressionRatio;
        // it is possible it can't guarantee atomic and isolation update,
        // but it's fine as it's just an estimation
        if (observedRatio > currentEstimation) {
            compressionRatio =
                    Math.max(currentEstimation + COMPRESSION_RATIO_DETERIORATE_STEP, observedRatio);
        } else if (observedRatio < currentEstimation) {
            compressionRatio =
                    Math.max(currentEstimation - COMPRESSION_RATIO_IMPROVING_STEP, observedRatio);
        }
    }

    /** Get current compression ratio estimation. */
    public float estimation() {
        return compressionRatio;
    }
}
