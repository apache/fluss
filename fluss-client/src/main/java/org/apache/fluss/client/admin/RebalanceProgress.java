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

package org.apache.fluss.client.admin;

import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.metadata.TableBucket;

import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The rebalance progress.
 *
 * @since 0.9
 */
public class RebalanceProgress {

    /** The rebalance status for the overall rebalance. */
    private final RebalanceStatus rebalanceStatus;

    /** The rebalance progress for the overall rebalance. Between 0.0d to 1.0d */
    private final double progress;

    /** The rebalance progress for each tabletBucket. */
    private final Map<TableBucket, RebalanceResultForBucket> progressForBucketMap;

    public RebalanceProgress(
            RebalanceStatus rebalanceStatus,
            double progress,
            Map<TableBucket, RebalanceResultForBucket> progressForBucketMap) {
        // TODO: we may derive the overall progress and status from progressForBucketMap
        this.rebalanceStatus = checkNotNull(rebalanceStatus);
        this.progress = progress;
        this.progressForBucketMap = checkNotNull(progressForBucketMap);
    }

    public RebalanceStatus status() {
        return rebalanceStatus;
    }

    public double progress() {
        return progress;
    }

    public Map<TableBucket, RebalanceResultForBucket> progressForBucketMap() {
        return progressForBucketMap;
    }
}
