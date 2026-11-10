/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.decode.FixedSchemaDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the sys.consumer_offsets schema and key/value encode/decode. */
class ConsumerOffsetSchemaTest {

    private static final short SCHEMA_ID = (short) 1;

    private final FixedSchemaDecoder decoder =
            new FixedSchemaDecoder(KvFormat.COMPACTED, ConsumerOffsetSchema.getTableSchema());
    private final ConsumerOffsetSchema offsetSchema = new ConsumerOffsetSchema();

    @Test
    void testSchemaMatchesFip() {
        // PK: (record_type, group_id, table_id, partition_id, bucket_id)
        assertThat(ConsumerOffsetSchema.getPrimaryKeys())
                .containsExactly(
                        "record_type", "group_id", "table_id", "partition_id", "bucket_id");
        // Value: (offset, metadata) — FIP-53 has no commit_timestamp.
        assertThat(ConsumerOffsetSchema.getTableSchema().getColumnNames())
                .containsExactly(
                        "group_id",
                        "record_type",
                        "table_id",
                        "partition_id",
                        "bucket_id",
                        "offset",
                        "metadata");
    }

    @Test
    void testOffsetCommitValueRoundTrip() {
        CompactedRow row =
                offsetSchema.encodeOffsetRow(
                        "my-group",
                        ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                        42L,
                        7L,
                        3,
                        new ConsumerOffsetAndMetadata(100L, "wm=5"));

        InternalRow decoded = decode(row);
        assertThat(decoded.getString(0).toString()).isEqualTo("my-group");
        assertThat(decoded.getInt(1)).isEqualTo(ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE);
        assertThat(decoded.getLong(2)).isEqualTo(42L);
        assertThat(decoded.getLong(3)).isEqualTo(7L);
        assertThat(decoded.getInt(4)).isEqualTo(3);
        assertThat(decoded.getLong(5)).isEqualTo(100L);
        assertThat(decoded.getString(6).toString()).isEqualTo("wm=5");
    }

    @Test
    void testOmittedAndEmptyMetadataHaveDistinctEncodings() {
        CompactedRow absent =
                offsetSchema.encodeOffsetRow(
                        "g",
                        ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                        1L,
                        null,
                        0,
                        new ConsumerOffsetAndMetadata(1L, null));
        int absentSize = absent.getSizeInBytes();
        assertThat(decode(absent).isNullAt(6)).isTrue();

        CompactedRow empty =
                offsetSchema.encodeOffsetRow(
                        "g",
                        ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                        1L,
                        null,
                        0,
                        new ConsumerOffsetAndMetadata(1L, ""));
        assertThat(decode(empty).getString(6).toString()).isEmpty();
        assertThat(empty.getSizeInBytes()).isEqualTo(absentSize + 1);
    }

    @Test
    void testNullPartitionIdEncodesAsMinusOneSentinel() {
        // Non-partitioned table: partitionId == null materializes as -1 (PK is NOT NULL).
        CompactedRow row =
                offsetSchema.encodeOffsetRow(
                        "g",
                        ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                        1L,
                        null,
                        0,
                        new ConsumerOffsetAndMetadata(5L, ""));
        assertThat(decode(row).getLong(3)).isEqualTo(-1L);

        // Partition id 0 is a valid partition id and must survive the round-trip unchanged.
        CompactedRow zeroPartition =
                offsetSchema.encodeOffsetRow(
                        "g",
                        ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                        1L,
                        0L,
                        0,
                        new ConsumerOffsetAndMetadata(5L, ""));
        assertThat(decode(zeroPartition).getLong(3)).isEqualTo(0L);
    }

    @Test
    void testEncodeOffsetKeyMatchesRowKeyEncoding() {
        CompactedRow row =
                offsetSchema.encodeOffsetRow(
                        "my-group",
                        ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                        42L,
                        7L,
                        3,
                        new ConsumerOffsetAndMetadata(100L, ""));

        byte[] keyFromRow = offsetSchema.encodeOffsetKey(row);
        byte[] keyFromFields = offsetSchema.encodeOffsetKey("my-group", 42L, 7L, 3);
        assertThat(keyFromFields).isEqualTo(keyFromRow);
    }

    @Test
    void testOffsetCommitPrefixIsBytePrefixOfFullKey() {
        // The prefix scan correctness of the zero-memory ConsumerGroupManager depends on this
        // property:
        // encodeOffsetCommitPrefix(g) must be a strict byte prefix of every offset-commit key.
        for (String group : new String[] {"a", "my-group", "group-with-ünicode", "x"}) {
            byte[] prefix = offsetSchema.encodeOffsetCommitPrefix(group);
            assertThat(prefix).isNotEmpty();

            byte[] keyNonPartitioned = offsetSchema.encodeOffsetKey(group, 1L, null, 0);
            byte[] keyPartitioned = offsetSchema.encodeOffsetKey(group, 2L, 99L, 5);

            assertThat(isPrefix(prefix, keyNonPartitioned))
                    .as("prefix of group %s must be a byte prefix of its full keys", group)
                    .isTrue();
            assertThat(isPrefix(prefix, keyPartitioned)).isTrue();
        }
    }

    @Test
    void testDifferentGroupsPrefixesDoNotCrossMatch() {
        byte[] prefixA = offsetSchema.encodeOffsetCommitPrefix("group-a");
        byte[] keyB = offsetSchema.encodeOffsetKey("group-b", 1L, null, 0);
        // "group-a" and "group-b" share a common byte prefix ("group-"), but the full encoded
        // group prefixes must diverge before the end — otherwise a prefix scan for group-a
        // would also see group-b's rows.
        assertThat(isPrefix(prefixA, keyB))
                .as("group-a's prefix must not match group-b's key")
                .isFalse();
    }

    @Test
    void testGroupBucketKeyMatchesStandardBucketKeyEncoder() {
        // The standard KV path creates a bucket key encoder from the table's schema and
        // the distributedBy column ("group_id"). ConsumerOffsetSchema.encodeGroupBucketKey must
        // produce
        // identical bytes — this is the guarantee that FindConsumerGroupOwner routes to the
        // same bucket where the data actually resides.
        KeyEncoder standardEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        ConsumerOffsetSchema.getTableSchema().getRowType(),
                        Collections.singletonList("group_id"),
                        null);

        for (String group :
                new String[] {
                    "a",
                    "my-group",
                    "test-group-123",
                    "group-with-\u00fcnicode",
                    "x",
                    "g" + repeat('y', 200)
                }) {
            CompactedRow row =
                    offsetSchema.encodeOffsetRow(
                            group,
                            ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                            -1L,
                            -1L,
                            -1,
                            new ConsumerOffsetAndMetadata(-1L, ""));
            byte[] standardBytes = standardEncoder.encodeKey(row);
            byte[] consumerOffsetSchemaBytes = offsetSchema.encodeGroupBucketKey(group);

            assertThat(consumerOffsetSchemaBytes)
                    .as(
                            "encodeGroupBucketKey(%s) must match the standard bucket key encoder",
                            group)
                    .isEqualTo(standardBytes);
        }
    }

    @Test
    void testOffsetCommitPrefixMatchesRecordTypeAndGroupEncoder() {
        KeyEncoder standardEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        ConsumerOffsetSchema.getTableSchema().getRowType(),
                        Arrays.asList("record_type", "group_id"),
                        null);

        for (String group : new String[] {"a", "my-group", "group-with-ünicode", "x"}) {
            CompactedRow row =
                    offsetSchema.encodeOffsetRow(
                            group,
                            ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                            -1L,
                            -1L,
                            -1,
                            new ConsumerOffsetAndMetadata(-1L, ""));
            byte[] standardBytes = standardEncoder.encodeKey(row);
            assertThat(offsetSchema.encodeOffsetCommitPrefix(group))
                    .as("offset-commit prefix for %s must encode record_type then group_id", group)
                    .isEqualTo(standardBytes);
        }
    }

    @Test
    void testBucketForIsDeterministicAndInRange() {
        int bucketCount = 50;
        for (String group : new String[] {"a", "my-group", "test-group-123", "another-group"}) {
            int bucket1 = offsetSchema.bucketFor(group, bucketCount);
            int bucket2 = offsetSchema.bucketFor(group, bucketCount);

            assertThat(bucket1)
                    .as("bucket for group %s must be in [0, %d)", group, bucketCount)
                    .isBetween(0, bucketCount - 1);
            assertThat(bucket1)
                    .as("bucketing must be deterministic for group %s", group)
                    .isEqualTo(bucket2);
        }
    }

    @Test
    void testSharedSchemaEncodesRowsAndKeysAcrossThreads() throws Exception {
        int threads = 8;
        int iterations = 128;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch rowsReady = new CountDownLatch(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                String groupId = "group-" + i;
                byte[] expectedKey = offsetSchema.encodeOffsetKey(groupId, 42L, 7L, 3);
                byte[] expectedPrefix = offsetSchema.encodeOffsetCommitPrefix(groupId);
                futures.add(
                        executor.submit(
                                () -> {
                                    for (int j = 0; j < iterations; j++) {
                                        CompactedRow row =
                                                offsetSchema.encodeOffsetRow(
                                                        groupId,
                                                        ConsumerOffsetSchema
                                                                .OFFSET_COMMIT_RECORD_TYPE,
                                                        42L,
                                                        7L,
                                                        3,
                                                        new ConsumerOffsetAndMetadata(j, ""));
                                        if (j == 0) {
                                            rowsReady.countDown();
                                            assertThat(rowsReady.await(30, TimeUnit.SECONDS))
                                                    .isTrue();
                                        }
                                        assertThat(offsetSchema.encodeOffsetCommitPrefix(groupId))
                                                .isEqualTo(expectedPrefix);
                                        assertThat(offsetSchema.encodeOffsetKey(row))
                                                .isEqualTo(expectedKey);
                                        assertThat(row.getLong(5)).isEqualTo(j);
                                    }
                                    return null;
                                }));
            }
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    // ------------------------------------------------------------------

    /** Decodes a row the same way the zero-memory ConsumerGroupManager decodes RocksDB values. */
    private InternalRow decode(CompactedRow row) {
        byte[] stored = ValueEncoder.encodeValue(SCHEMA_ID, row);
        return decoder.decode(MemorySegment.wrap(stored));
    }

    private static boolean isPrefix(byte[] prefix, byte[] bytes) {
        if (bytes.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (prefix[i] != bytes[i]) {
                return false;
            }
        }
        return true;
    }

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }
}
