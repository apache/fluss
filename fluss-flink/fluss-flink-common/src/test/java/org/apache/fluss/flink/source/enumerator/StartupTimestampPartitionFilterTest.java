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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.utils.AutoPartitionStrategy;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StartupTimestampPartitionFilterTest {

    @Test
    void filtersDayPartitionsByAutoPartitionTimezone() {
        long startup = Instant.parse("2026-04-20T00:00:00Z").toEpochMilli();
        List<PartitionInfo> partitions =
                Arrays.asList(part("dt", "20260418"), part("dt", "20260419"));

        List<PartitionInfo> out =
                StartupTimestampPartitionFilter.filter(
                        partitions,
                        startup,
                        Collections.singletonList("dt"),
                        strategy(AutoPartitionTimeUnit.DAY, "America/Los_Angeles", null));

        assertThat(out).extracting(PartitionInfo::getPartitionName).containsExactly("20260419");
    }

    @Test
    void filtersHourPartitions() {
        long startup = Instant.parse("2026-04-20T10:30:00Z").toEpochMilli();
        List<PartitionInfo> partitions =
                Arrays.asList(
                        part("dt", "2026042009"),
                        part("dt", "2026042010"),
                        part("dt", "2026042011"));

        List<PartitionInfo> out =
                StartupTimestampPartitionFilter.filter(
                        partitions,
                        startup,
                        Collections.singletonList("dt"),
                        strategy(AutoPartitionTimeUnit.HOUR, "UTC", null));

        assertThat(out)
                .extracting(PartitionInfo::getPartitionName)
                .containsExactly("2026042010", "2026042011");
    }

    @Test
    void filtersMonthQuarterAndYearPartitions() {
        long startup = Instant.parse("2026-04-01T00:00:00Z").toEpochMilli();

        assertThat(
                        StartupTimestampPartitionFilter.filter(
                                Arrays.asList(part("dt", "202603"), part("dt", "202604")),
                                startup,
                                Collections.singletonList("dt"),
                                strategy(AutoPartitionTimeUnit.MONTH, "UTC", null)))
                .extracting(PartitionInfo::getPartitionName)
                .containsExactly("202604");

        assertThat(
                        StartupTimestampPartitionFilter.filter(
                                Arrays.asList(part("dt", "20261"), part("dt", "20262")),
                                startup,
                                Collections.singletonList("dt"),
                                strategy(AutoPartitionTimeUnit.QUARTER, "UTC", null)))
                .extracting(PartitionInfo::getPartitionName)
                .containsExactly("20262");

        assertThat(
                        StartupTimestampPartitionFilter.filter(
                                Arrays.asList(part("dt", "2025"), part("dt", "2026")),
                                startup,
                                Collections.singletonList("dt"),
                                strategy(AutoPartitionTimeUnit.YEAR, "UTC", null)))
                .extracting(PartitionInfo::getPartitionName)
                .containsExactly("2026");
    }

    @Test
    void usesConfiguredAutoPartitionKeyForCompositePartitions() {
        long startup = Instant.parse("2026-04-20T00:00:00Z").toEpochMilli();
        List<String> partitionKeys = Arrays.asList("region", "dt");
        List<PartitionInfo> partitions =
                Arrays.asList(
                        part(partitionKeys, Arrays.asList("cn", "20260419")),
                        part(partitionKeys, Arrays.asList("us", "20260420")));

        List<PartitionInfo> out =
                StartupTimestampPartitionFilter.filter(
                        partitions,
                        startup,
                        partitionKeys,
                        strategy(AutoPartitionTimeUnit.DAY, "UTC", "dt"));

        assertThat(out).extracting(PartitionInfo::getPartitionName).containsExactly("us$20260420");
    }

    @Test
    void keepsPartitionsWhenAutoPartitionIsDisabledOrValueIsUnrecognized() {
        long startup = Instant.parse("2026-04-20T00:00:00Z").toEpochMilli();
        List<PartitionInfo> partitions =
                Arrays.asList(part("dt", "20260419"), part("dt", "not-a-day"));

        assertThat(
                        StartupTimestampPartitionFilter.filter(
                                partitions,
                                startup,
                                Collections.singletonList("dt"),
                                disabledStrategy()))
                .containsExactlyElementsOf(partitions);

        assertThat(
                        StartupTimestampPartitionFilter.filter(
                                partitions,
                                startup,
                                Collections.singletonList("dt"),
                                strategy(AutoPartitionTimeUnit.DAY, "UTC", null)))
                .extracting(PartitionInfo::getPartitionName)
                .containsExactly("not-a-day");
    }

    private static AutoPartitionStrategy strategy(
            AutoPartitionTimeUnit timeUnit, String timeZone, String autoPartitionKey) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
        conf.set(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, timeUnit);
        conf.set(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, timeZone);
        if (autoPartitionKey != null) {
            conf.set(ConfigOptions.TABLE_AUTO_PARTITION_KEY, autoPartitionKey);
        }
        return AutoPartitionStrategy.from(conf);
    }

    private static AutoPartitionStrategy disabledStrategy() {
        return AutoPartitionStrategy.from(new Configuration());
    }

    private static PartitionInfo part(String partitionKey, String partitionValue) {
        return new PartitionInfo(
                1L, ResolvedPartitionSpec.fromPartitionValue(partitionKey, partitionValue), null);
    }

    private static PartitionInfo part(List<String> partitionKeys, List<String> partitionValues) {
        return new PartitionInfo(
                1L, new ResolvedPartitionSpec(partitionKeys, partitionValues), null);
    }
}
