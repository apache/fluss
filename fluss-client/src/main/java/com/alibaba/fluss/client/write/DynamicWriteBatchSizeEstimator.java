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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ConcurrentHashMap;

/** An estimator to estimate the buffer usage of a writeBatch. */
@Internal
@ThreadSafe
public class DynamicWriteBatchSizeEstimator {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicWriteBatchSizeEstimator.class);

    private static final double RATIO_TO_INCREASE_BATCH_SIZE = 0.8d;
    private static final double RATIO_TO_DECREASE_BATCH_SIZE = 0.5d;
    private final int maxBatchSize;
    private final int pageSize;
    private final boolean dynamicBatchSizeEnabled;

    private final ConcurrentHashMap<PhysicalTablePath, Integer> estimatedBatchSizeMap;

    public DynamicWriteBatchSizeEstimator(
            boolean dynamicBatchSizeEnabled, int maxBatchSize, int pageSize) {
        this.dynamicBatchSizeEnabled = dynamicBatchSizeEnabled;

        if (dynamicBatchSizeEnabled) {
            this.estimatedBatchSizeMap = MapUtils.newConcurrentHashMap();
        } else {
            this.estimatedBatchSizeMap = null;
        }

        this.maxBatchSize = maxBatchSize;
        this.pageSize = pageSize;
    }

    public void updateEstimation(PhysicalTablePath physicalTablePath, int observedBatchSize) {
        if (!dynamicBatchSizeEnabled) {
            return;
        }

        int estimatedBatchSize =
                estimatedBatchSizeMap.getOrDefault(physicalTablePath, maxBatchSize);
        int newEstimatedBatchSize = estimatedBatchSize;
        if (observedBatchSize >= estimatedBatchSize
                || observedBatchSize > estimatedBatchSize * RATIO_TO_INCREASE_BATCH_SIZE) {
            // To increase 10%
            newEstimatedBatchSize = Math.min((int) (estimatedBatchSize * 1.1), maxBatchSize);
        } else if (observedBatchSize < estimatedBatchSize * RATIO_TO_DECREASE_BATCH_SIZE) {
            // To decrease 5%
            newEstimatedBatchSize = Math.max((int) (estimatedBatchSize * 0.95), 2 * pageSize);
        }

        estimatedBatchSizeMap.put(physicalTablePath, newEstimatedBatchSize);
        LOG.debug(
                "Set estimated batch size for {} from {} to {}",
                physicalTablePath,
                estimatedBatchSize,
                newEstimatedBatchSize);
    }

    public int getEstimatedBatchSize(PhysicalTablePath physicalTablePath) {
        return dynamicBatchSizeEnabled
                ? estimatedBatchSizeMap.getOrDefault(physicalTablePath, maxBatchSize)
                : maxBatchSize;
    }
}
