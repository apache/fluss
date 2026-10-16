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

import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.decode.FixedSchemaDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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
    private final KeyEncoder offsetKeyEncoder = ConsumerOffsetSchema.createOffsetKeyEncoder();

    @Test
    void testSchemaMatchesFip() {
        // PK: (group_id, record_type, table_id, partition_id, bucket_id)
        assertThat(ConsumerOffsetSchema.getPrimaryKeys())
                .containsExactly(
                        "group_id", "record_type", "table_id", "partition_id", "bucket_id");
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
                ConsumerOffsetSchema.encodeOffsetRow(
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
    void testNullPartitionIdEncodesAsMinusOneSentinel() {
        // Non-partitioned table: partitionId == null materializes as -1 (PK is NOT NULL).
        CompactedRow row =
                ConsumerOffsetSchema.encodeOffsetRow(
                        "g",
                        ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                        1L,
                        null,
                        0,
                        new ConsumerOffsetAndMetadata(5L, ""));
        assertThat(decode(row).getLong(3)).isEqualTo(-1L);

        // Partition id 0 is a valid partition id and must survive the round-trip unchanged.
        CompactedRow zeroPartition =
                ConsumerOffsetSchema.encodeOffsetRow(
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
                ConsumerOffsetSchema.encodeOffsetRow(
                        "my-group",
                        ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                        42L,
                        7L,
                        3,
                        new ConsumerOffsetAndMetadata(100L, ""));

        byte[] keyFromRow = ConsumerOffsetSchema.encodeOffsetKey(offsetKeyEncoder, row);
        byte[] keyFromFields =
                ConsumerOffsetSchema.encodeOffsetKey(offsetKeyEncoder, "my-group", 42L, 7L, 3);
        assertThat(keyFromFields).isEqualTo(keyFromRow);
    }

    @Test
    void testGroupPrefixIsBytePrefixOfFullKey() {
        // The prefix scan correctness of the zero-memory GroupManager depends on this property:
        // encodeGroupPrefix(g) must be a strict byte prefix of every encodeOffsetKey of group g.
        for (String group : new String[] {"a", "my-group", "group-with-ünicode", "x"}) {
            byte[] prefix = ConsumerOffsetSchema.encodeGroupPrefix(group);
            assertThat(prefix).isNotEmpty();

            byte[] keyNonPartitioned =
                    ConsumerOffsetSchema.encodeOffsetKey(offsetKeyEncoder, group, 1L, null, 0);
            byte[] keyPartitioned =
                    ConsumerOffsetSchema.encodeOffsetKey(offsetKeyEncoder, group, 2L, 99L, 5);

            assertThat(isPrefix(prefix, keyNonPartitioned))
                    .as("prefix of group %s must be a byte prefix of its full keys", group)
                    .isTrue();
            assertThat(isPrefix(prefix, keyPartitioned)).isTrue();
        }
    }

    @Test
    void testDifferentGroupsPrefixesDoNotCrossMatch() {
        byte[] prefixA = ConsumerOffsetSchema.encodeGroupPrefix("group-a");
        byte[] keyB =
                ConsumerOffsetSchema.encodeOffsetKey(offsetKeyEncoder, "group-b", 1L, null, 0);
        // "group-a" and "group-b" share a common byte prefix ("group-"), but the full encoded
        // group prefixes must diverge before the end — otherwise a prefix scan for group-a
        // would also see group-b's rows.
        assertThat(isPrefix(prefixA, keyB))
                .as("group-a's prefix must not match group-b's key")
                .isFalse();
    }

    @Test
    void testEncodeGroupPrefixMatchesStandardBucketKeyEncoder() {
        // The standard KV path creates a bucket key encoder from the table's schema and
        // the distributedBy column ("group_id"). ConsumerOffsetSchema.encodeGroupPrefix must
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
                    ConsumerOffsetSchema.encodeOffsetRow(
                            group,
                            ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                            -1L,
                            -1L,
                            -1,
                            new ConsumerOffsetAndMetadata(-1L, ""));
            byte[] standardBytes = standardEncoder.encodeKey(row);
            byte[] consumerOffsetSchemaBytes = ConsumerOffsetSchema.encodeGroupPrefix(group);

            assertThat(consumerOffsetSchemaBytes)
                    .as("encodeGroupPrefix(%s) must match the standard bucket key encoder", group)
                    .isEqualTo(standardBytes);
        }
    }

    @Test
    void testBucketForIsDeterministicAndInRange() {
        // GroupManager.bucketFor uses FlussBucketingFunction on the encoded group_id;
        // verify the result is deterministic and always within [0, bucketCount).
        int bucketCount = 50;
        for (String group : new String[] {"a", "my-group", "test-group-123", "another-group"}) {
            byte[] bucketKey = ConsumerOffsetSchema.encodeGroupPrefix(group);
            int bucket1 = FlussBucketingFunction.bucketForRowKey(bucketKey, bucketCount);
            int bucket2 = FlussBucketingFunction.bucketForRowKey(bucketKey, bucketCount);

            assertThat(bucket1)
                    .as("bucket for group %s must be in [0, %d)", group, bucketCount)
                    .isBetween(0, bucketCount - 1);
            assertThat(bucket1)
                    .as("bucketing must be deterministic for group %s", group)
                    .isEqualTo(bucket2);
        }
    }

    @Test
    void testPerThreadKeyEncoderCanBeReused() throws Exception {
        int threads = 8;
        int iterations = 128;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch startLatch = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                String groupId = "group-" + i;
                KeyEncoder keyEncoder = ConsumerOffsetSchema.createOffsetKeyEncoder();
                byte[] expected =
                        ConsumerOffsetSchema.encodeOffsetKey(keyEncoder, groupId, 42L, 7L, 3);
                futures.add(
                        executor.submit(
                                () -> {
                                    startLatch.await();
                                    for (int j = 0; j < iterations; j++) {
                                        assertThat(
                                                        ConsumerOffsetSchema.encodeOffsetKey(
                                                                keyEncoder, groupId, 42L, 7L, 3))
                                                .isEqualTo(expected);
                                    }
                                    return null;
                                }));
            }
            startLatch.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    // ------------------------------------------------------------------

    /** Decodes a row the same way the zero-memory GroupManager decodes RocksDB values. */
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
