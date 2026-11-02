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

package org.apache.fluss.client.metadata;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the cross-language boundary encoder. */
class UnionReadContextEncoderTest {

    @Test
    void testEncodePartitionedBoundary() throws Exception {
        TableInfo table = table(true);
        List<PartitionInfo> partitions =
                Arrays.asList(partition(9, "US", 2), partition(10, "欧洲\"\n", 4));
        Map<TableBucket, Long> earliest = offsets(partitions, 8);
        Map<TableBucket, Long> stops = offsets(partitions, 9007199254740993L);
        Map<TableBucket, Long> seams = offsets(partitions, 12);
        seams.remove(new TableBucket(7, 10L, 3));
        byte[] json =
                UnionReadContextEncoder.encode(
                        table, new LakeSnapshot(42, seams), partitions, earliest, stops);
        JsonNode node = new ObjectMapper().readTree(json);
        JsonNode ranges = node.get("log_ranges");
        assertThat(ranges).hasSize(6);
        assertThat(ranges.get(0).get("bucket_count").intValue()).isEqualTo(2);
        assertThat(ranges.get(2).get("bucket_count").intValue()).isEqualTo(4);
        assertThat(ranges.get(5).get("start_offset").longValue()).isEqualTo(8L);
        assertThat(ranges.get(0).at("/partition/kind").asText()).isEqualTo("ordinary");
        assertThat(ranges.get(0).at("/baseline/kind").asText()).isEqualTo("lake");
        assertThat(ranges.get(5).at("/baseline/kind").asText()).isEqualTo("none");
        assertThat(node.at("/lake/table_path/database").asText()).isEqualTo("lake_db");
        assertThat(node.at("/lake/table_path/table").asText()).isEqualTo("orders_lake");
        assertThat(node.at("/lake/snapshot_id").longValue()).isEqualTo(42L);
        assertThat(node.get("context_version").intValue()).isEqualTo(1);
        assertThat(ranges.get(2).at("/partition/identity/key_values/0/1").asText())
                .isEqualTo("欧洲\"\n");
        assertThat(ranges.get(0).get("stop_offset").longValue()).isEqualTo(9007199254740993L);
        assertThat(node.get("schema"))
                .isEqualTo(new ObjectMapper().readTree(table.getSchema().toJsonBytes()));
        assertThat(new String(json, StandardCharsets.UTF_8))
                .doesNotContain("SECRET", "warehouse", "properties", "projection");
    }

    @Test
    void testLogOnlyAndLakeOnlyBoundaries() throws Exception {
        TableInfo table = table(false);
        Map<TableBucket, Long> earliest = new HashMap<>();
        Map<TableBucket, Long> stops = new HashMap<>();
        for (int bucket = 0; bucket < 2; bucket++) {
            earliest.put(new TableBucket(7, bucket), 0L);
            stops.put(new TableBucket(7, bucket), 20L);
        }
        JsonNode node =
                new ObjectMapper()
                        .readTree(
                                UnionReadContextEncoder.encode(
                                        table, null, Collections.emptyList(), earliest, stops));
        assertThat(node.at("/lake/snapshot_id").isNull()).isTrue();
        assertThat(node.at("/log_ranges/0/partition/identity").asText()).isEqualTo("unpartitioned");
        assertThat(node.at("/log_ranges/0/start_offset").longValue()).isZero();
        assertThat(node.at("/log_ranges/0/baseline/kind").asText()).isEqualTo("none");
        JsonNode zeroSeam =
                new ObjectMapper()
                        .readTree(
                                UnionReadContextEncoder.encode(
                                        table,
                                        new LakeSnapshot(
                                                42,
                                                Collections.singletonMap(
                                                        new TableBucket(7, 0), 0L)),
                                        Collections.emptyList(),
                                        earliest,
                                        stops));
        assertThat(zeroSeam.at("/log_ranges/0/start_offset").longValue()).isZero();
        assertThat(zeroSeam.at("/log_ranges/0/baseline/kind").asText()).isEqualTo("lake");
        assertThat(zeroSeam.at("/log_ranges/1/baseline/kind").asText()).isEqualTo("none");
        JsonNode lakeOnly =
                new ObjectMapper()
                        .readTree(
                                UnionReadContextEncoder.encode(
                                        table(true),
                                        new LakeSnapshot(42, Collections.emptyMap()),
                                        Collections.emptyList(),
                                        Collections.emptyMap(),
                                        Collections.emptyMap()));
        assertThat(lakeOnly.get("log_ranges")).isEmpty();
        assertThat(lakeOnly.at("/lake/snapshot_id").longValue()).isEqualTo(42L);
    }

    @Test
    void testFlussOnlyBoundaryEncodesEarliest() throws Exception {
        TableInfo table = table(false);
        table.getProperties().setString("table.datalake.enabled", "false");
        Map<TableBucket, Long> earliest = new HashMap<>();
        Map<TableBucket, Long> stops = new HashMap<>();
        for (int bucket = 0; bucket < 2; bucket++) {
            earliest.put(new TableBucket(7, bucket), 8L);
            stops.put(new TableBucket(7, bucket), 20L);
        }
        JsonNode node =
                new ObjectMapper()
                        .readTree(
                                UnionReadContextEncoder.encode(
                                        table, null, Collections.emptyList(), earliest, stops));
        assertThat(node.get("lake").isNull()).isTrue();
        for (JsonNode range : node.get("log_ranges")) {
            assertThat(range.get("start_offset").longValue()).isEqualTo(8L);
        }
    }

    @Test
    void testRejectMismatchedBoundaries() {
        TableInfo table = table(true);
        List<PartitionInfo> partitions = Collections.singletonList(partition(9, "US", 2));
        Map<TableBucket, Long> earliest = offsets(partitions, 0);
        Map<TableBucket, Long> stops = offsets(partitions, 20);
        Map<TableBucket, Long> seams = offsets(partitions, 12);
        LakeSnapshot snapshot = new LakeSnapshot(42, seams);
        stops.remove(new TableBucket(7, 9L, 1));
        assertThatThrownBy(
                        () ->
                                UnionReadContextEncoder.encode(
                                        table, snapshot, partitions, earliest, stops))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing log offsets");
        stops.put(new TableBucket(7, 9L, 1), 20L);
        assertThatThrownBy(
                        () ->
                                UnionReadContextEncoder.encode(
                                        table,
                                        snapshot,
                                        Arrays.asList(partitions.get(0), partitions.get(0)),
                                        earliest,
                                        stops))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate or mismatched partition");
        assertThatThrownBy(
                        () ->
                                UnionReadContextEncoder.encode(
                                        table,
                                        new LakeSnapshot(
                                                42,
                                                Collections.singletonMap(
                                                        new TableBucket(8, 0), 1L)),
                                        partitions,
                                        earliest,
                                        stops))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid bucket seam");
    }

    @Test
    void testPartitionScopeIncludesLakeOnlyPartitions() throws Exception {
        List<PartitionInfo> partitions =
                Arrays.asList(partition(9, "US", 2), partition(10, "EU", 4));
        PartitionSpec filter = new PartitionSpec(Collections.singletonMap("region", "US"));
        byte[] json =
                UnionReadContextEncoder.encode(
                        table(true),
                        new LakeSnapshot(42, Collections.emptyMap()),
                        partitions,
                        offsets(Collections.singletonList(partitions.get(0)), 0),
                        offsets(Collections.singletonList(partitions.get(0)), 20),
                        filter);
        assertThat(new ObjectMapper().readTree(json).get("log_ranges")).hasSize(2);
        assertThat(new ObjectMapper().readTree(json).get("partition_filter").get("region").asText())
                .isEqualTo("US");
        byte[] lakeOnly =
                UnionReadContextEncoder.encode(
                        table(true),
                        new LakeSnapshot(42, Collections.emptyMap()),
                        partitions,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        new PartitionSpec(Collections.singletonMap("region", "expired")));
        JsonNode lakeNode = new ObjectMapper().readTree(lakeOnly);
        assertThat(lakeNode.get("log_ranges")).isEmpty();
        assertThat(lakeNode.at("/partition_filter/region").asText()).isEqualTo("expired");
        assertThatThrownBy(
                        () ->
                                UnionReadContextEncoder.encode(
                                        table(true),
                                        null,
                                        partitions,
                                        Collections.emptyMap(),
                                        Collections.emptyMap(),
                                        new PartitionSpec(Collections.singletonMap("amount", "1"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition keys");
    }

    static TableInfo table(boolean partitioned) {
        return table(partitioned, 7, 2);
    }

    static TableInfo table(boolean partitioned, long tableId, int schemaId) {
        Map<String, String> properties = new HashMap<>();
        properties.put("table.datalake.enabled", "true");
        properties.put("table.datalake.format", "paimon");
        properties.put("table.datalake.database-name", "lake_db");
        properties.put("table.datalake.table-name", "orders_lake");
        properties.put("table.datalake.paimon.s3.secret-key", "SECRET");
        properties.put("table.datalake.paimon.warehouse", "s3://private/warehouse");
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("region", DataTypes.STRING())
                        .column("amount", DataTypes.BIGINT());
        if (schemaId > 2) {
            schema.column("added", DataTypes.STRING());
        }
        if (partitioned) {
            schema.primaryKey("id", "region");
        }
        return new TableInfo(
                TablePath.of("fluss", "orders"),
                tableId,
                schemaId,
                schema.build(),
                partitioned ? new ArrayList<>(Collections.singletonList("id")) : new ArrayList<>(),
                partitioned
                        ? new ArrayList<>(Collections.singletonList("region"))
                        : new ArrayList<>(),
                2,
                Configuration.fromMap(properties),
                new Configuration(),
                null,
                null,
                0,
                0);
    }

    static PartitionInfo partition(long id, String value, int bucketCount) {
        return new PartitionInfo(
                id, ResolvedPartitionSpec.fromPartitionValue("region", value), null, bucketCount);
    }

    private static Map<TableBucket, Long> offsets(List<PartitionInfo> partitions, long offset) {
        Map<TableBucket, Long> result = new HashMap<>();
        for (PartitionInfo partition : partitions) {
            for (int bucket = 0; bucket < partition.getBucketCount(); bucket++) {
                result.put(new TableBucket(7, partition.getPartitionId(), bucket), offset);
            }
        }
        return result;
    }
}
