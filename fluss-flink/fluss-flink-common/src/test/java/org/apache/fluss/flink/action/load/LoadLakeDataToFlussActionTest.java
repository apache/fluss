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

package org.apache.fluss.flink.action.load;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for load validation and snapshot partition planning. */
class LoadLakeDataToFlussActionTest {

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

    @ParameterizedTest
    @ValueSource(strings = {"none", "fixed-delay", "failure-rate", "exponential-delay"})
    void testPreserveExplicitRestartStrategy(String strategy) {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, strategy);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 7);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(20));
        ReadableConfig loadConfig = loadConfiguration(config);

        assertThat(loadConfig.get(RestartStrategyOptions.RESTART_STRATEGY)).isEqualTo(strategy);
        assertThat(loadConfig.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS))
                .isEqualTo(7);
        assertThat(loadConfig.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY))
                .isEqualTo(Duration.ofMillis(20));
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
    @ValueSource(booleans = {true, false})
    void testRejectUnsupportedBuild(boolean ttl) {
        TableInfo table =
                tableInfo(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .primaryKey("id")
                                                .build())
                                .distributedBy(2)
                                .property(
                                        ttl
                                                ? ConfigOptions.TABLE_KV_TTL.key()
                                                : ConfigOptions.TABLE_MERGE_ENGINE.key(),
                                        ttl ? "1 h" : "first_row")
                                .build());
        assertThatThrownBy(() -> LoadLakeDataToFlussAction.validateBuildSupport(table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Bulk Load does not support");
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
                                        null,
                                        bulkLoadId,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be supplied together");
    }

    @Test
    void testRejectUnsupportedPartitionPushdown() {
        TestingLakeSource source =
                new TestingLakeSource() {
                    @Override
                    public FilterPushDownResult withFilters(List<Predicate> predicates) {
                        return FilterPushDownResult.of(Collections.emptyList(), predicates);
                    }
                };
        // Loading all splits would also load expired partitions, so a rejected filter must fail.
        assertThatThrownBy(
                        () ->
                                LoadLakeToFluss.planSplits(
                                        partitionedTable(true),
                                        source,
                                        42L,
                                        null,
                                        Instant.parse("2026-11-10T01:00:00Z")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot push down the load partition filter");
    }

    @Test
    void testPartitionFilterPushdown() throws Exception {
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
        TestingLakeSource source =
                new TestingLakeSource() {
                    private List<Predicate> filters;

                    @Override
                    public FilterPushDownResult withFilters(List<Predicate> predicates) {
                        filters = predicates;
                        return FilterPushDownResult.of(predicates, Collections.emptyList());
                    }

                    @Override
                    public Planner<LakeSplit> createPlanner(PlannerContext context) {
                        assertThat(context.snapshotId()).isEqualTo(42L);
                        assertThat(filters).hasSize(1);
                        Predicate filter = filters.get(0);
                        BinaryString first = BinaryString.fromString("20261108");
                        BinaryString second = BinaryString.fromString("20261110");
                        assertThat(filter.test(GenericRow.of(1, first, 1))).isTrue();
                        assertThat(filter.test(GenericRow.of(1, second, 2))).isTrue();
                        // A multi-column selection must preserve each pair of partition values.
                        assertThat(filter.test(GenericRow.of(1, first, 2))).isFalse();
                        assertThat(filter.test(GenericRow.of(1, second, 1))).isFalse();
                        return () ->
                                Arrays.asList(
                                        new TestingLakeSplit(0, Arrays.asList("20261108", "1")),
                                        new TestingLakeSplit(0, Arrays.asList("20261110", "2")));
                    }
                };
        assertThat(
                        LoadLakeToFluss.planSplits(
                                tableInfo,
                                source,
                                42L,
                                new HashSet<>(Arrays.asList("20261108$1", "20261110$2")),
                                Instant.parse("2026-11-10T01:00:00Z")))
                .extracting(LakeSplit::partition)
                .containsExactly(Arrays.asList("20261108", "1"), Arrays.asList("20261110", "2"));
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
        Predicate filter = LoadLakeToFluss.partitionFilter(tableInfo, null, now);
        assertThat(matchingPartitions(filter, partitions))
                .containsExactlyInAnyOrder(
                        "20261103", "20261108", "20261109", "20261110", "20261111");
        assertThat(matchingPartitions(filter, Collections.singleton("20261102"))).isEmpty();
        // A resumed transaction retains its original target even after retention advances.
        assertThat(
                        matchingPartitions(
                                LoadLakeToFluss.partitionFilter(
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
        Predicate filter = LoadLakeToFluss.partitionFilter(tableInfo, null, now);
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
