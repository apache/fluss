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

package org.apache.fluss.server.zk.data.lake;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LakeTableJsonSerde}. */
class LakeTableJsonSerdeTest extends JsonSerdeTestBase<LakeTable> {

    LakeTableJsonSerdeTest() {
        super(LakeTableJsonSerde.INSTANCE);
    }

    @Override
    protected LakeTable[] createObjects() {
        // Test case 1: Empty lake snapshots list (v2)
        LakeTable lakeTable1 = new LakeTable(Collections.emptyList());

        // Test case 2: Single snapshot metadata with readable offsets (v2)
        LakeTable.LakeSnapshotMetadata metadata1 =
                new LakeTable.LakeSnapshotMetadata(
                        1L, new FsPath("/path/to/tiered1"), new FsPath("/path/to/readable1"));
        LakeTable lakeTable2 = new LakeTable(Collections.singletonList(metadata1));

        // Test case 3: Single snapshot metadata without readable offsets (v2)
        LakeTable.LakeSnapshotMetadata metadata2 =
                new LakeTable.LakeSnapshotMetadata(2L, new FsPath("/path/to/tiered2"), null);
        LakeTable lakeTable3 = new LakeTable(Collections.singletonList(metadata2));

        // Test case 4: Multiple snapshot metadata (v2)
        List<LakeTable.LakeSnapshotMetadata> metadatas = new ArrayList<>();
        metadatas.add(
                new LakeTable.LakeSnapshotMetadata(
                        3L, new FsPath("/path/to/tiered3"), new FsPath("/path/to/readable3")));
        metadatas.add(
                new LakeTable.LakeSnapshotMetadata(
                        4L, new FsPath("/path/to/tiered4"), new FsPath("/path/to/readable4")));
        metadatas.add(new LakeTable.LakeSnapshotMetadata(5L, new FsPath("/path/to/tiered5"), null));
        LakeTable lakeTable4 = new LakeTable(metadatas);

        // Test case 5: Version 1 format - non-partition table
        long tableId = 4;
        Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 1), 200L);
        LakeTableSnapshot snapshot1 = new LakeTableSnapshot(10L, bucketLogEndOffset);
        LakeTable lakeTable5 = new LakeTable(snapshot1);

        // Test case 6: Version 1 format - partition table
        tableId = 5;
        bucketLogEndOffset = new HashMap<>();
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 0), 100L);
        bucketLogEndOffset.put(new TableBucket(tableId, 1L, 1), 200L);
        bucketLogEndOffset.put(new TableBucket(tableId, 2L, 0), 300L);
        LakeTableSnapshot snapshot2 = new LakeTableSnapshot(11L, bucketLogEndOffset);
        LakeTable lakeTable6 = new LakeTable(snapshot2);

        return new LakeTable[] {
            lakeTable1, lakeTable2, lakeTable3, lakeTable4, lakeTable5, lakeTable6
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            // Test case 1: Empty lake snapshots list (v2)
            "{\"version\":2,\"lake_snapshots\":[]}",
            // Test case 2: Single snapshot metadata with readable offsets (v2)
            "{\"version\":2,\"lake_snapshots\":[{\"snapshot_id\":1,\"tiered_offsets\":\"/path/to/tiered1\",\"readable_offsets\":\"/path/to/readable1\"}]}",
            // Test case 3: Single snapshot metadata without readable offsets (v2)
            "{\"version\":2,\"lake_snapshots\":[{\"snapshot_id\":2,\"tiered_offsets\":\"/path/to/tiered2\"}]}",
            // Test case 4: Multiple snapshot metadata (v2)
            "{\"version\":2,\"lake_snapshots\":[{\"snapshot_id\":3,\"tiered_offsets\":\"/path/to/tiered3\",\"readable_offsets\":\"/path/to/readable3\"},{\"snapshot_id\":4,\"tiered_offsets\":\"/path/to/tiered4\",\"readable_offsets\":\"/path/to/readable4\"},{\"snapshot_id\":5,\"tiered_offsets\":\"/path/to/tiered5\"}]}",
            // Test case 5: Version 1 format - non-partition table
            "{\"version\":1,\"snapshot_id\":10,\"table_id\":4,\"buckets\":[{\"bucket_id\":1,\"log_end_offset\":200},{\"bucket_id\":0,\"log_end_offset\":100}]}",
            // Test case 6: Version 1 format - partition table
            "{\"version\":1,\"snapshot_id\":11,\"table_id\":5,\"buckets\":[{\"partition_id\":1,\"bucket_id\":1,\"log_end_offset\":200},{\"partition_id\":1,\"bucket_id\":0,\"log_end_offset\":100},{\"partition_id\":2,\"bucket_id\":0,\"log_end_offset\":300}]}",
        };
    }

    @Test
    void testVersion1Compatibility() throws IOException {
        // Test that Version 1 format can be deserialized correctly
        // Test case 1: Non-partition table
        String version1Json1 =
                "{\"version\":1,\"snapshot_id\":10,\"table_id\":4,"
                        + "\"buckets\":[{\"bucket_id\":0,\"log_end_offset\":100},{\"bucket_id\":1,\"log_end_offset\":200}]}";
        LakeTable actual1 =
                JsonSerdeUtils.readValue(
                        version1Json1.getBytes(StandardCharsets.UTF_8),
                        LakeTableJsonSerde.INSTANCE);

        // Create expected LakeTableSnapshot
        Map<TableBucket, Long> expectedBuckets1 = new HashMap<>();
        expectedBuckets1.put(new TableBucket(4L, 0), 100L);
        expectedBuckets1.put(new TableBucket(4L, 1), 200L);
        LakeTableSnapshot expectedSnapshot1 = new LakeTableSnapshot(10L, expectedBuckets1);
        assertThat(actual1.getOrReadLatestTableSnapshot()).isEqualTo(expectedSnapshot1);

        // Test case 2: Partition table
        String version1Json2 =
                "{\"version\":1,\"snapshot_id\":11,\"table_id\":5,"
                        + "\"buckets\":[{\"partition_id\":1,\"bucket_id\":0,\"log_end_offset\":100},"
                        + "{\"partition_id\":1,\"bucket_id\":1,\"log_end_offset\":200},"
                        + "{\"partition_id\":2,\"bucket_id\":0,\"log_end_offset\":300}]}";
        LakeTable actual2 =
                JsonSerdeUtils.readValue(
                        version1Json2.getBytes(StandardCharsets.UTF_8),
                        LakeTableJsonSerde.INSTANCE);

        // Create expected LakeTableSnapshot
        Map<TableBucket, Long> expectedBuckets2 = new HashMap<>();
        expectedBuckets2.put(new TableBucket(5L, 1L, 0), 100L);
        expectedBuckets2.put(new TableBucket(5L, 1L, 1), 200L);
        expectedBuckets2.put(new TableBucket(5L, 2L, 0), 300L);
        LakeTableSnapshot expectedSnapshot2 = new LakeTableSnapshot(11L, expectedBuckets2);
        assertThat(actual2.getOrReadLatestTableSnapshot()).isEqualTo(expectedSnapshot2);
    }

    /**
     * Verifies forward / backward compatibility of the optional {@code commit_timestamp} field
     * introduced in #2625.
     *
     * <ul>
     *   <li>Legacy V2 JSON without {@code commit_timestamp} must deserialize successfully and the
     *       in-memory entry should report {@link
     *       LakeTable.LakeSnapshotMetadata#UNKNOWN_COMMIT_TIMESTAMP}.
     *   <li>V2 JSON containing {@code commit_timestamp} must round-trip and the value must be
     *       preserved.
     *   <li>An entry stamped with a non-zero timestamp must serialize the field; an entry whose
     *       timestamp is {@code UNKNOWN_COMMIT_TIMESTAMP} must omit the field (keeping output
     *       byte-compatible with legacy znodes).
     * </ul>
     */
    @Test
    void testCommitTimestampJsonCompatibility() throws IOException {
        // 1. Legacy V2 JSON (no commit_timestamp) -> fallback to UNKNOWN_COMMIT_TIMESTAMP
        String legacyV2 =
                "{\"version\":2,\"lake_snapshots\":["
                        + "{\"snapshot_id\":7,\"tiered_offsets\":\"/p/t7\",\"readable_offsets\":\"/p/r7\"}"
                        + "]}";
        LakeTable parsedLegacy =
                JsonSerdeUtils.readValue(
                        legacyV2.getBytes(StandardCharsets.UTF_8), LakeTableJsonSerde.INSTANCE);
        assertThat(parsedLegacy.getLakeSnapshotMetadatas()).hasSize(1);
        assertThat(parsedLegacy.getLakeSnapshotMetadatas().get(0).getCommitTimestamp())
                .isEqualTo(LakeTable.LakeSnapshotMetadata.UNKNOWN_COMMIT_TIMESTAMP);

        // 2. New V2 JSON with commit_timestamp -> value preserved
        String newV2 =
                "{\"version\":2,\"lake_snapshots\":["
                        + "{\"snapshot_id\":8,\"tiered_offsets\":\"/p/t8\","
                        + "\"readable_offsets\":\"/p/r8\",\"commit_timestamp\":12345678}"
                        + "]}";
        LakeTable parsedNew =
                JsonSerdeUtils.readValue(
                        newV2.getBytes(StandardCharsets.UTF_8), LakeTableJsonSerde.INSTANCE);
        assertThat(parsedNew.getLakeSnapshotMetadatas()).hasSize(1);
        assertThat(parsedNew.getLakeSnapshotMetadatas().get(0).getCommitTimestamp())
                .isEqualTo(12345678L);

        // 3. Round-trip: stamped entry serializes the field, unknown entry omits it.
        LakeTable.LakeSnapshotMetadata stamped =
                new LakeTable.LakeSnapshotMetadata(
                        9L, new FsPath("/p/t9"), new FsPath("/p/r9"), 99999L);
        LakeTable.LakeSnapshotMetadata unstamped =
                new LakeTable.LakeSnapshotMetadata(10L, new FsPath("/p/t10"), null);
        List<LakeTable.LakeSnapshotMetadata> mixed = new ArrayList<>();
        mixed.add(stamped);
        mixed.add(unstamped);
        LakeTable mixedTable = new LakeTable(mixed);

        byte[] serialized =
                JsonSerdeUtils.writeValueAsBytes(mixedTable, LakeTableJsonSerde.INSTANCE);
        String serializedStr = new String(serialized, StandardCharsets.UTF_8);
        assertThat(serializedStr).contains("\"commit_timestamp\":99999");
        // entry #2 is unstamped, the field should be absent
        assertThat(serializedStr).doesNotContain("\"commit_timestamp\":0");

        LakeTable roundTripped = JsonSerdeUtils.readValue(serialized, LakeTableJsonSerde.INSTANCE);
        assertThat(roundTripped).isEqualTo(mixedTable);
    }
}
