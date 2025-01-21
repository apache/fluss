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

import com.alibaba.fluss.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/**
 * This class help estimate the compression ratio for each table and each arrow compression type
 * combination.
 */
@Internal
@ThreadSafe
public class ArrowCompressionRatioEstimator {
    /**
     * The constant speed to increase compression ratio when a batch compresses better than
     * expected.
     */
    public static final float COMPRESSION_RATIO_IMPROVING_STEP = 0.005f;

    /**
     * The minimum speed to decrease compression ratio when a batch compresses worse than expected.
     */
    public static final float COMPRESSION_RATIO_DETERIORATE_STEP = 0.05f;

    private final Map<Long, Map<String, Float>> compressionRatio;
    private final Map<Long, Lock> tableLocks;

    public ArrowCompressionRatioEstimator() {
        compressionRatio = new ConcurrentHashMap<>();
        tableLocks = new ConcurrentHashMap<>();
    }

    /**
     * Update the compression ratio estimation for a table and related compression info with the
     * observed compression ratio.
     */
    public void updateEstimation(
            long tableId, ArrowCompressionInfo compressionInfo, float observedRatio) {
        Lock lock = tableLocks.computeIfAbsent(tableId, k -> new ReentrantLock());
        inLock(
                lock,
                () -> {
                    Map<String, Float> compressionRatioMap =
                            compressionRatio.computeIfAbsent(
                                    tableId, k -> new ConcurrentHashMap<>());
                    String compressionKey = compressionInfo.toString();
                    float currentEstimation =
                            compressionRatioMap.getOrDefault(compressionKey, 1.0f);
                    if (observedRatio > currentEstimation) {
                        compressionRatioMap.put(
                                compressionKey,
                                Math.max(
                                        currentEstimation + COMPRESSION_RATIO_DETERIORATE_STEP,
                                        observedRatio));
                    } else if (observedRatio < currentEstimation) {
                        compressionRatioMap.put(
                                compressionKey,
                                Math.max(
                                        currentEstimation - COMPRESSION_RATIO_IMPROVING_STEP,
                                        observedRatio));
                    }
                });
    }

    /** Get the compression ratio estimation for a table and related compression info. */
    public float estimation(long tableId, ArrowCompressionInfo compressionInfo) {
        Lock lock = tableLocks.computeIfAbsent(tableId, k -> new ReentrantLock());
        return inLock(
                lock,
                () -> {
                    Map<String, Float> compressionRatioMap =
                            compressionRatio.computeIfAbsent(
                                    tableId, k -> new ConcurrentHashMap<>());
                    String compressionKey = compressionInfo.toString();

                    if (!compressionRatioMap.containsKey(compressionKey)) {
                        compressionRatioMap.put(compressionKey, 1.0f);
                    }

                    return compressionRatioMap.get(compressionKey);
                });
    }
}
