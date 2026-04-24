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

package org.apache.fluss.flink.source.enumerator;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.utils.AutoPartitionStrategy;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.utils.PartitionUtils.generateAutoPartitionTime;

/** Filters auto partitions whose time range ends before the startup timestamp. */
final class StartupTimestampPartitionFilter {

    private StartupTimestampPartitionFilter() {}

    static List<PartitionInfo> filter(
            List<PartitionInfo> partitionInfos,
            long startupTimestampMs,
            List<String> partitionKeys,
            AutoPartitionStrategy autoPartitionStrategy) {
        if (!autoPartitionStrategy.isAutoPartitionEnabled()) {
            return partitionInfos;
        }

        String autoPartitionKey = autoPartitionStrategy.key();
        if (autoPartitionKey == null) {
            autoPartitionKey = partitionKeys.get(0);
        }

        int autoPartitionKeyIndex = partitionKeys.indexOf(autoPartitionKey);
        if (autoPartitionKeyIndex < 0) {
            return partitionInfos;
        }

        AutoPartitionTimeUnit timeUnit = autoPartitionStrategy.timeUnit();
        String startupPartitionValue =
                generateAutoPartitionTime(
                        ZonedDateTime.ofInstant(
                                Instant.ofEpochMilli(startupTimestampMs),
                                autoPartitionStrategy.timeZone().toZoneId()),
                        0,
                        timeUnit);
        List<PartitionInfo> out = new ArrayList<>(partitionInfos.size());
        for (PartitionInfo partitionInfo : partitionInfos) {
            if (includePartition(
                    partitionInfo, autoPartitionKeyIndex, startupPartitionValue)) {
                out.add(partitionInfo);
            }
        }
        return out;
    }

    private static boolean includePartition(
            PartitionInfo partitionInfo,
            int autoPartitionKeyIndex,
            String startupPartitionValue) {
        List<String> values = partitionInfo.getResolvedPartitionSpec().getPartitionValues();
        if (autoPartitionKeyIndex >= values.size()) {
            return true;
        }
        String partitionValue = values.get(autoPartitionKeyIndex);
        return partitionValue.compareTo(startupPartitionValue) >= 0;
    }
}
