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

package org.apache.fluss.lake.paimon.tiering.markdone;

import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Trigger to mark partitions done, copied from Paimon's {@code
 * org.apache.paimon.flink.sink.listener.PartitionMarkDoneTrigger} (release 1.3) with the following
 * modifications:
 *
 * <ul>
 *   <li>the restored state carries the last update time per partition instead of resetting it to
 *       the current time, since the tiering service recreates the trigger for every round;
 *   <li>the caller provides partition end times through {@link PartitionEndTimeExtractor};
 *   <li>Flink operator state, end-input and watermark related code is removed.
 * </ul>
 */
class PartitionMarkDoneTrigger {

    private final PartitionEndTimeExtractor endTimeExtractor;
    private final long idleTime;
    private final Map<String, Long> trackedPartitionLastUpdateTimes;

    public PartitionMarkDoneTrigger(
            Map<String, Long> restoredPartitionLastUpdateTimes,
            PartitionEndTimeExtractor endTimeExtractor,
            long idleTime) {
        this.trackedPartitionLastUpdateTimes = new HashMap<>(restoredPartitionLastUpdateTimes);
        this.endTimeExtractor = endTimeExtractor;
        this.idleTime = idleTime;
    }

    public void notifyPartition(String partition, long currentTimeMillis) {
        if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
            this.trackedPartitionLastUpdateTimes.put(partition, currentTimeMillis);
        }
    }

    public List<String> donePartitions(long currentTimeMillis) {
        List<String> needDone = new ArrayList<>();
        Iterator<Map.Entry<String, Long>> iter =
                trackedPartitionLastUpdateTimes.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Long> entry = iter.next();
            String partition = entry.getKey();
            long lastUpdateTime = entry.getValue();

            Long partitionEndTime = endTimeExtractor.extract(partition);
            // skip illegal partition
            if (partitionEndTime == null) {
                iter.remove();
                continue;
            }
            lastUpdateTime = Math.max(lastUpdateTime, partitionEndTime);

            if (currentTimeMillis - lastUpdateTime > idleTime) {
                needDone.add(partition);
                iter.remove();
            }
        }
        return needDone;
    }

    /** Returns the pending (not yet done) partitions to be persisted as state. */
    public Map<String, Long> trackedPartitionLastUpdateTimes() {
        return trackedPartitionLastUpdateTimes;
    }

    /** Extracts the partition end time in epoch millis, null if it cannot be derived. */
    public interface PartitionEndTimeExtractor {
        @Nullable
        Long extract(String partition);
    }
}
