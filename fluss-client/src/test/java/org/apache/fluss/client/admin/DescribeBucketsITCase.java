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

import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.metadata.BucketInfo;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration test for describing table buckets through {@link Admin}. */
class DescribeBucketsITCase extends ClientToServerITCaseBase {

    private static final TablePath NON_PARTITIONED_TABLE_PATH =
            TablePath.of("test_db", "non_partitioned_table");
    private static final TablePath PARTITIONED_TABLE_PATH =
            TablePath.of("test_db", "partitioned_table");

    @Test
    void testDescribeBucketsForNonPartitionedTable() throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(3, "id")
                        .build();
        long tableId = createTable(NON_PARTITIONED_TABLE_PATH, tableDescriptor, false);

        List<BucketInfo> bucketInfos = waitAndDescribeBuckets(NON_PARTITIONED_TABLE_PATH, null, 3);
        assertThat(bucketInfos).extracting(BucketInfo::getBucketId).containsExactly(0, 1, 2);
        bucketInfos.forEach(
                bucketInfo -> {
                    assertBucketInfo(bucketInfo, NON_PARTITIONED_TABLE_PATH, tableId, null);
                    assertThat(bucketInfo.getPartitionName()).isNull();
                });

        assertThatThrownBy(
                        () ->
                                admin.describeBuckets(
                                                NON_PARTITIONED_TABLE_PATH,
                                                newPartitionSpec("pt", "2025"))
                                        .get())
                .cause()
                .isInstanceOf(TableNotPartitionedException.class);
        assertThatThrownBy(
                        () -> admin.describeBuckets(TablePath.of("test_db", "missing_table")).get())
                .cause()
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testDescribeBucketsForPartitionedTable() throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("pt", DataTypes.STRING())
                                        .column("region", DataTypes.STRING())
                                        .build())
                        .distributedBy(2, "id")
                        .partitionedBy("pt", "region")
                        .build();
        long tableId = createTable(PARTITIONED_TABLE_PATH, tableDescriptor, false);
        PartitionSpec p2025Cn =
                newPartitionSpec(Arrays.asList("pt", "region"), Arrays.asList("2025", "cn"));
        PartitionSpec p2025Us =
                newPartitionSpec(Arrays.asList("pt", "region"), Arrays.asList("2025", "us"));
        PartitionSpec p2026Cn =
                newPartitionSpec(Arrays.asList("pt", "region"), Arrays.asList("2026", "cn"));
        admin.createPartition(PARTITIONED_TABLE_PATH, p2025Cn, false).get();
        admin.createPartition(PARTITIONED_TABLE_PATH, p2025Us, false).get();
        admin.createPartition(PARTITIONED_TABLE_PATH, p2026Cn, false).get();

        Map<String, Long> partitionIds =
                admin.listPartitionInfos(PARTITIONED_TABLE_PATH).get().stream()
                        .collect(
                                Collectors.toMap(
                                        PartitionInfo::getPartitionName,
                                        PartitionInfo::getPartitionId));

        List<BucketInfo> allPartitionBuckets =
                waitAndDescribeBuckets(PARTITIONED_TABLE_PATH, null, 6);
        assertThat(allPartitionBuckets)
                .extracting(
                        bucketInfo ->
                                bucketInfo.getPartitionName() + ":" + bucketInfo.getBucketId())
                .containsExactly(
                        "2025$cn:0",
                        "2025$cn:1",
                        "2025$us:0",
                        "2025$us:1",
                        "2026$cn:0",
                        "2026$cn:1");
        allPartitionBuckets.forEach(
                bucketInfo ->
                        assertBucketInfo(
                                bucketInfo,
                                PARTITIONED_TABLE_PATH,
                                tableId,
                                partitionIds.get(bucketInfo.getPartitionName())));

        List<BucketInfo> partialPartitionBuckets =
                waitAndDescribeBuckets(PARTITIONED_TABLE_PATH, newPartitionSpec("pt", "2025"), 4);
        assertThat(partialPartitionBuckets)
                .extracting(BucketInfo::getPartitionName)
                .containsExactly("2025$cn", "2025$cn", "2025$us", "2025$us");

        List<BucketInfo> exactPartitionBuckets =
                waitAndDescribeBuckets(PARTITIONED_TABLE_PATH, p2025Cn, 2);
        assertThat(exactPartitionBuckets)
                .extracting(BucketInfo::getPartitionName)
                .containsOnly("2025$cn");
        assertThat(exactPartitionBuckets).extracting(BucketInfo::getBucketId).containsExactly(0, 1);

        assertThat(
                        admin.describeBuckets(
                                        PARTITIONED_TABLE_PATH, newPartitionSpec("pt", "missing"))
                                .get())
                .isEmpty();
        assertThatThrownBy(
                        () ->
                                admin.describeBuckets(
                                                PARTITIONED_TABLE_PATH,
                                                newPartitionSpec("unknown", "2025"))
                                        .get())
                .cause()
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining("unknown");
    }

    private List<BucketInfo> waitAndDescribeBuckets(
            TablePath tablePath, @Nullable PartitionSpec partitionSpec, int expectedBucketCount)
            throws Exception {
        waitUntil(
                () -> {
                    List<BucketInfo> bucketInfos = describeBuckets(tablePath, partitionSpec);
                    return bucketInfos.size() == expectedBucketCount
                            && bucketInfos.stream()
                                    .allMatch(
                                            bucketInfo ->
                                                    bucketInfo.getLeaderId().isPresent()
                                                            && bucketInfo
                                                                    .getLeaderEpoch()
                                                                    .isPresent()
                                                            && !bucketInfo.getIsr().isEmpty());
                },
                Duration.ofMinutes(1),
                "Waiting for bucket metadata");
        return describeBuckets(tablePath, partitionSpec);
    }

    private List<BucketInfo> describeBuckets(
            TablePath tablePath, @Nullable PartitionSpec partitionSpec) throws Exception {
        return partitionSpec == null
                ? admin.describeBuckets(tablePath).get()
                : admin.describeBuckets(tablePath, partitionSpec).get();
    }

    private static void assertBucketInfo(
            BucketInfo bucketInfo,
            TablePath tablePath,
            long tableId,
            @Nullable Long expectedPartitionId) {
        assertThat(bucketInfo.getTablePath()).isEqualTo(tablePath);
        assertThat(bucketInfo.getTableId()).isEqualTo(tableId);
        if (expectedPartitionId == null) {
            assertThat(bucketInfo.getPartitionId()).isEmpty();
        } else {
            assertThat(bucketInfo.getPartitionId()).hasValue(expectedPartitionId);
        }
        assertThat(bucketInfo.getReplicas()).hasSize(3);
        assertThat(bucketInfo.getIsr()).isNotEmpty();
        assertThat(bucketInfo.getReplicas()).containsAll(bucketInfo.getIsr());
        assertThat(bucketInfo.getIsr()).contains(bucketInfo.getLeaderId().getAsInt());
    }
}
