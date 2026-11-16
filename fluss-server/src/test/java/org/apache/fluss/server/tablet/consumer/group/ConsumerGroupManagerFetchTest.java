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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.ByteArraySlice;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the consumer-offset fetch path. */
class ConsumerGroupManagerFetchTest {

    private static final short OLD_SCHEMA_ID = 1;
    private static final short NEW_SCHEMA_ID = 2;

    @Test
    void testReadSelectedOffsetsPreservesOrderAndReturnsNoOffset() throws Exception {
        String groupId = "group-1";
        TableBucket committedBucket = new TableBucket(10L, 3);
        TableBucket missingBucket = new TableBucket(10L, 1);
        Map<TableBucket, ConsumerOffsetAndMetadata> offsets = new LinkedHashMap<>();
        ConsumerGroupManager.populateSelectedOffsets(
                Arrays.asList(committedBucket, missingBucket),
                Arrays.asList(
                        ByteArraySlice.wrap(
                                encodedOffset(groupId, committedBucket, 42L, "metadata")),
                        null),
                valueDecoder(),
                offsets);

        assertThat(offsets.keySet()).containsExactly(committedBucket, missingBucket);
        assertThat(offsets.get(committedBucket).getOffset()).isEqualTo(42L);
        assertThat(offsets.get(committedBucket).getMetadata().toString()).isEqualTo("metadata");
        assertThat(offsets.get(missingBucket)).isNull();
    }

    @Test
    void testReadAllOffsetsRestoresPartitionedAndNonPartitionedBuckets() throws Exception {
        String groupId = "group-1";
        TableBucket nonPartitioned = new TableBucket(10L, 0);
        TableBucket partitioned = new TableBucket(11L, 7L, 1);
        Map<TableBucket, ConsumerOffsetAndMetadata> offsets = new LinkedHashMap<>();
        ConsumerGroupManager.populateAllOffsets(
                Arrays.asList(
                        ByteArraySlice.wrap(encodedOffset(groupId, nonPartitioned, 10L, "")),
                        ByteArraySlice.wrap(encodedOffset(groupId, partitioned, 20L, "p"))),
                valueDecoder(),
                offsets);

        assertThat(new ArrayList<>(offsets.keySet())).containsExactly(nonPartitioned, partitioned);
        assertThat(offsets.get(nonPartitioned).getOffset()).isEqualTo(10L);
        assertThat(offsets.get(partitioned).getOffset()).isEqualTo(20L);
    }

    @Test
    void testReadAllOffsetsSupportsMixedSchemaVersions() {
        String groupId = "group-1";
        TableBucket oldBucket = new TableBucket(10L, 0);
        TableBucket newBucket = new TableBucket(11L, 1);
        Map<TableBucket, ConsumerOffsetAndMetadata> offsets = new LinkedHashMap<>();
        ConsumerGroupManager.populateAllOffsets(
                Arrays.asList(
                        ByteArraySlice.wrap(encodedOffset(groupId, oldBucket, 10L, "old")),
                        ByteArraySlice.wrap(
                                encodedEvolvedOffset(groupId, newBucket, 20L, "new", "extra"))),
                valueDecoder(),
                offsets);

        assertThat(offsets.keySet()).containsExactly(oldBucket, newBucket);
        assertThat(offsets.get(oldBucket).getOffset()).isEqualTo(10L);
        assertThat(offsets.get(oldBucket).getMetadata().toString()).isEqualTo("old");
        assertThat(offsets.get(newBucket).getOffset()).isEqualTo(20L);
        assertThat(offsets.get(newBucket).getMetadata().toString()).isEqualTo("new");
    }

    private static ValueDecoder valueDecoder() {
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(OLD_SCHEMA_ID, ConsumerOffsetSchema.getTableSchema());
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(evolvedSchema(), NEW_SCHEMA_ID));
        return ConsumerOffsetSchema.createValueDecoder(schemaGetter);
    }

    private static Schema evolvedSchema() {
        return Schema.newBuilder()
                .fromSchema(ConsumerOffsetSchema.getTableSchema())
                .column("extra", DataTypes.STRING())
                .build();
    }

    private static byte[] encodedOffset(
            String groupId, TableBucket tableBucket, long offset, String metadata) {
        CompactedRow row =
                new ConsumerOffsetSchema()
                        .encodeOffsetRow(
                                ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE,
                                GroupId.of(groupId),
                                tableBucket.getTableId(),
                                tableBucket.getPartitionId(),
                                tableBucket.getBucket(),
                                new ConsumerOffsetAndMetadata(offset, metadata));
        return ValueEncoder.encodeValue(OLD_SCHEMA_ID, row);
    }

    private static byte[] encodedEvolvedOffset(
            String groupId, TableBucket tableBucket, long offset, String metadata, String extra) {
        Schema schema = evolvedSchema();
        DataType[] fieldTypes = schema.getRowType().getChildren().toArray(new DataType[0]);
        CompactedRowEncoder encoder = new CompactedRowEncoder(fieldTypes);
        encoder.startNewRow();
        encoder.encodeField(0, ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE);
        encoder.encodeField(1, BinaryString.fromString(groupId));
        encoder.encodeField(2, tableBucket.getTableId());
        encoder.encodeField(
                3,
                tableBucket.getPartitionId() == null
                        ? ConsumerOffsetSchema.NON_PARTITIONED_PARTITION_ID
                        : tableBucket.getPartitionId());
        encoder.encodeField(4, tableBucket.getBucket());
        encoder.encodeField(5, offset);
        encoder.encodeField(6, BinaryString.fromString(metadata));
        encoder.encodeField(7, BinaryString.fromString(extra));
        CompactedRow row = encoder.finishRow();
        return ValueEncoder.encodeValue(NEW_SCHEMA_ID, row);
    }
}
