/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

import org.apache.fluss.metadata.GroupId;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.utils.ByteArraySlice;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for consumer-group management helpers. */
class GroupManagerManagementTest {

    private static final short SCHEMA_ID = 1;

    @Test
    void testPopulateGroupIdsDeduplicatesAndSorts() throws Exception {
        Set<String> groupIds = new TreeSet<>();
        ConsumerGroupManager.populateGroupIds(
                Arrays.asList(
                        encodedGroupMetadata(GroupId.of("group-b")),
                        encodedGroupMetadata(GroupId.of("group-a")),
                        encodedGroupMetadata(GroupId.of("group-b")),
                        encodedOffset(GroupId.of("group-c"), new TableBucket(1L, 0))),
                valueDecoder(),
                groupIds);

        assertThat(groupIds).containsExactly("group-a", "group-b");
    }

    @Test
    void testEncodeGroupRowKeysUsesMetadataRows() throws Exception {
        GroupId groupId = GroupId.of("group-1");
        TableBucket offsetBucket = new TableBucket(1L, 0);
        List<byte[]> keys =
                ConsumerGroupManager.encodeGroupRowKeys(
                        groupId,
                        Arrays.asList(
                                encodedGroupMetadata(groupId),
                                encodedOffset(groupId, offsetBucket)),
                        valueDecoder());

        assertThat(keys)
                .containsExactly(
                        new ConsumerOffsetSchema().encodeGroupMetadataKey(groupId, -1L, null, -1));
    }

    @Test
    void testExistingKeysFiltersPresentValues() {
        List<byte[]> keys = Arrays.asList(new byte[] {1}, new byte[] {2}, new byte[] {3});
        List<ByteArraySlice> values =
                Arrays.asList(
                        ByteArraySlice.wrap(new byte[] {10}),
                        null,
                        ByteArraySlice.wrap(new byte[] {30}));

        assertThat(ConsumerGroupManager.existingKeys(keys, values))
                .containsExactly(keys.get(0), keys.get(2));
    }

    private static ValueDecoder valueDecoder() {
        return ConsumerOffsetSchema.createValueDecoder(
                new TestingSchemaGetter(SCHEMA_ID, ConsumerOffsetSchema.getTableSchema()));
    }

    private static ByteArraySlice encodedGroupMetadata(GroupId groupId) {
        return encodedRecord(
                groupId, ConsumerOffsetSchema.GROUP_METADATA_RECORD_TYPE, new TableBucket(-1L, -1));
    }

    private static ByteArraySlice encodedOffset(GroupId groupId, TableBucket tableBucket) {
        return encodedRecord(groupId, ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE, tableBucket);
    }

    private static ByteArraySlice encodedRecord(
            GroupId groupId, int recordType, TableBucket tableBucket) {
        CompactedRow row =
                new ConsumerOffsetSchema()
                        .encodeOffsetRow(
                                recordType,
                                groupId,
                                tableBucket.getTableId(),
                                tableBucket.getPartitionId(),
                                tableBucket.getBucket(),
                                new ConsumerOffsetAndMetadata(-1L, ""));
        return ByteArraySlice.wrap(ValueEncoder.encodeValue(SCHEMA_ID, row));
    }
}
