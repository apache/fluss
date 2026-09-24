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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.paimon.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *   <li>the partition end time is extracted by an injected {@link PartitionEndTimeExtractor} to
 *       also support Fluss auto-partitioned tables besides Paimon's timestamp-pattern/formatter
 *       plus time-interval rule;
 *   <li>Flink operator state, end-input and watermark related code is removed.
 * </ul>
 */
public class PartitionMarkDoneTrigger {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionMarkDoneTrigger.class);

    private final PartitionEndTimeExtractor endTimeExtractor;
    private final long idleTime;
    private final Map<String, Long> pendingPartitions;

    public PartitionMarkDoneTrigger(
            Map<String, Long> restoredPendingPartitions,
            PartitionEndTimeExtractor endTimeExtractor,
            long idleTime) {
        this.pendingPartitions = new HashMap<>(restoredPendingPartitions);
        this.endTimeExtractor = endTimeExtractor;
        this.idleTime = idleTime;
    }

    public void notifyPartition(String partition, long currentTimeMillis) {
        if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
            this.pendingPartitions.put(partition, currentTimeMillis);
        }
    }

    public List<String> donePartitions(long currentTimeMillis) {
        List<String> needDone = new ArrayList<>();
        Iterator<Map.Entry<String, Long>> iter = pendingPartitions.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Long> entry = iter.next();
            String partition = entry.getKey();
            long lastUpdateTime = entry.getValue();

            Long partitionEndTime = endTimeExtractor.extract(partition);
            // skip illegal partition
            if (partitionEndTime == null) {
                LOG.warn("Can't extract partition end time from partition {}, skip it.", partition);
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
    public Map<String, Long> pendingPartitions() {
        return pendingPartitions;
    }

    /** Extracts the partition end time in epoch millis, null if it cannot be derived. */
    public interface PartitionEndTimeExtractor {
        @Nullable
        Long extract(String partition);
    }
}
