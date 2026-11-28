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

package org.apache.fluss.flink.action.bulkload;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for load validation and snapshot partition planning. */
class LoadLakeDataToFlussActionTest {

    @Test
    void testPreserveExecutionParallelism() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(3);

        assertThat(LoadLakeToFluss.createEnvironment(environment).getParallelism()).isEqualTo(3);
    }

    @Test
    void testDefaultRestartLimit() {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        ReadableConfig loadConfig = loadConfiguration(config);

        assertThat(loadConfig.get(RestartStrategyOptions.RESTART_STRATEGY))
                .isEqualTo("fixed-delay");
        assertThat(loadConfig.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS))
                .isEqualTo(3);
        assertThat(loadConfig.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY))
                .isEqualTo(Duration.ofSeconds(10));
        assertThat(config.contains(RestartStrategyOptions.RESTART_STRATEGY)).isFalse();
    }

    @Test
    void testPreserveExplicitRestartStrategy() {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        ReadableConfig loadConfig = loadConfiguration(config);

        assertThat(loadConfig.get(RestartStrategyOptions.RESTART_STRATEGY)).isEqualTo("none");
    }

    @Test
    void testPreserveRestartLimitWithoutExplicitStrategy() {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(20));
        ReadableConfig loadConfig = loadConfiguration(config);

        assertThat(loadConfig.get(RestartStrategyOptions.RESTART_STRATEGY))
                .isEqualTo("fixed-delay");
        assertThat(loadConfig.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS))
                .isEqualTo(2);
        assertThat(loadConfig.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY))
                .isEqualTo(Duration.ofMillis(20));
    }

    @ParameterizedTest
    @CsvSource({", existing-load", "42, "})
    void testRejectIncompleteLoadIdentity(Long sourceSnapshotId, String bulkLoadId) {
        assertThatThrownBy(
                        () ->
                                new LoadLakeDataToFlussAction(
                                        new Configuration(),
                                        new Configuration(),
                                        TablePath.of("db", "table"),
                                        sourceSnapshotId,
                                        bulkLoadId,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be supplied together");
    }

    @Test
    void testMultiColumnPartitionFilter() {
        TableInfo tableInfo =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("dt", DataTypes.STRING())
                                                .column("region", DataTypes.INT())
                                                .primaryKey("id", "dt", "region")
                                                .build())
                                .partitionedBy("dt", "region")
                                .distributedBy(2)
                                .build());
        Predicate filter =
                LoadLakeToFluss.buildPartitionFilter(
                        tableInfo,
                        new HashSet<>(Arrays.asList("20261108$1", "20261110$2")),
                        Instant.parse("2026-11-10T01:00:00Z"));
        BinaryString first = BinaryString.fromString("20261108");
        BinaryString second = BinaryString.fromString("20261110");
        assertThat(filter.test(GenericRow.of(1, first, 1))).isTrue();
        assertThat(filter.test(GenericRow.of(1, second, 2))).isTrue();
        // A multi-column selection must preserve each pair of partition values.
        assertThat(filter.test(GenericRow.of(1, first, 2))).isFalse();
        assertThat(filter.test(GenericRow.of(1, second, 1))).isFalse();
    }

    @Test
    void testAutoPartitionSelectionUsesTableTimeZone() {
        TableInfo tableInfo = partitionedTable(true);
        Set<String> partitions =
                new HashSet<>(
                        Arrays.asList(
                                "20261102",
                                "20261103",
                                "20261108",
                                "20261109",
                                "20261110",
                                "20261111"));
        Instant now = Instant.parse("2026-11-09T16:00:00Z");
        // Shanghai is already on November 10. Retention includes November 3 and later,
        // including source partitions in the future; gaps do not extend the retained range.
        Predicate filter = LoadLakeToFluss.buildPartitionFilter(tableInfo, null, now);
        assertThat(matchingPartitions(filter, partitions))
                .containsExactlyInAnyOrder(
                        "20261103", "20261108", "20261109", "20261110", "20261111");
        assertThat(matchingPartitions(filter, Collections.singleton("20261102"))).isEmpty();
        // A resumed transaction retains its original target even after retention advances.
        assertThat(
                        matchingPartitions(
                                LoadLakeToFluss.buildPartitionFilter(
                                        tableInfo, Collections.singleton("20261102"), now),
                                partitions))
                .containsExactly("20261102");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testAutoPartitionSelectionRetentionBoundary(int retention) {
        TableInfo tableInfo = partitionedTable(true, retention);
        Set<String> partitions = new HashSet<>(Arrays.asList("20261102", "20261110", "20261111"));
        Instant now = Instant.parse("2026-11-09T16:00:00Z");
        Predicate filter = LoadLakeToFluss.buildPartitionFilter(tableInfo, null, now);
        if (retention < 0) {
            assertThat(filter).isNull();
            return;
        }
        assertThat(matchingPartitions(filter, partitions))
                .containsExactlyInAnyOrderElementsOf(
                        new HashSet<>(Arrays.asList("20261110", "20261111")));
    }

    private static ReadableConfig loadConfiguration(
            org.apache.flink.configuration.Configuration config) {
        return LoadLakeToFluss.createEnvironment(
                        StreamExecutionEnvironment.createLocalEnvironment(config))
                .getConfiguration();
    }

    private static Set<String> matchingPartitions(Predicate filter, Set<String> partitions) {
        return partitions.stream()
                .filter(
                        partition ->
                                filter.test(GenericRow.of(1, BinaryString.fromString(partition))))
                .collect(Collectors.toSet());
    }

    private static TableInfo tableInfo(TableDescriptor descriptor) {
        return TableInfo.of(
                TablePath.of("db", "table"), 1L, 1, descriptor, "file:///tmp/fluss", 0L, 0L);
    }

    private static TableInfo partitionedTable(boolean autoPartitioned) {
        return partitionedTable(autoPartitioned, 7);
    }

    private static TableInfo partitionedTable(boolean autoPartitioned, int retention) {
        return tableInfo(
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("dt", DataTypes.STRING())
                                        .primaryKey("id", "dt")
                                        .build())
                        .partitionedBy("dt")
                        .distributedBy(2)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, autoPartitioned)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, retention)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "Asia/Shanghai")
                        .build());
    }
}
