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

package org.apache.fluss.record;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultKvRecord}. */
class DefaultKvRecordTest extends KvTestBase {

    @Test
    void testBase() throws Exception {
        KvRecordReadContext kvRecordReadContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));
        // create row.

        CompactedRow row;
        try (CompactedRowEncoder writer = new CompactedRowEncoder(baseRowFieldTypes)) {
            writer.startNewRow();
            writer.encodeField(0, 10);
            writer.encodeField(1, BinaryString.fromString("abc"));
            row = writer.finishRow();
        }

        byte[] key = new byte[] {1, 2};
        DefaultKvRecord.writeTo(outputView, key, row);

        // Test read from.
        KvRecord kvRecord =
                DefaultKvRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        schemaId,
                        kvRecordReadContext);

        // four byte for length + bytes for key length  + bytes for key +
        // bytes for row
        // 4 + 1 + 2 + 6 = 13
        assertThat(kvRecord.getSizeInBytes()).isEqualTo(13);
        // check key
        assertThat(keyToBytes(kvRecord)).isEqualTo(key);
        // check value
        assertThat(kvRecord.getRow()).isEqualTo(row);

        // now, check write record with value is null
        key = new byte[] {2, 3};
        outputView = new MemorySegmentOutputView(100);
        DefaultKvRecord.writeTo(outputView, key, null);
        // Test read from.
        kvRecord =
                DefaultKvRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        schemaId,
                        kvRecordReadContext);
        // four byte for length + bytes for key length  + bytes for key
        // 4 + 1 + 2  = 7
        assertThat(kvRecord.getSizeInBytes()).isEqualTo(7);
        // check key
        assertThat(keyToBytes(kvRecord)).isEqualTo(key);
        // check value
        assertThat(kvRecord.getRow()).isNull();
    }

    @Test
    void testWriteToAndReadFromWithRandomData() throws Exception {
        // Test write to.
        CompactedRow row = TestInternalRowGenerator.genCompactedRowForAllType();
        Random rnd = new Random();
        byte[] key = TestInternalRowGenerator.generateRandomBytes(rnd);
        DefaultKvRecord.writeTo(outputView, key, row);

        RowType rowType = TestInternalRowGenerator.createAllRowType();
        DataType[] colTypes = rowType.getChildren().toArray(new DataType[0]);

        // Test read from.
        KvRecord kvRecord =
                DefaultKvRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        schemaId,
                        KvRecordReadContext.createReadContext(
                                KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA)));

        // check key
        assertThat(keyToBytes(kvRecord)).isEqualTo(key);
        // check value
        assertThat(kvRecord.getRow()).isEqualTo(row);
    }

    @Test
    void testV0RecordDefaultMutationType() throws Exception {
        KvRecordReadContext readContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));

        // V0 record with non-null row should return UPSERT.
        CompactedRow row;
        try (CompactedRowEncoder writer = new CompactedRowEncoder(baseRowFieldTypes)) {
            writer.startNewRow();
            writer.encodeField(0, 42);
            writer.encodeField(1, BinaryString.fromString("v0"));
            row = writer.finishRow();
        }
        byte[] key = new byte[] {1, 2};
        DefaultKvRecord.writeTo(outputView, key, row);
        KvRecord kvRecord =
                DefaultKvRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()), 0, schemaId, readContext);
        assertThat(kvRecord.getMutationType()).isEqualTo(MutationType.UPSERT);

        // V0 record with null row should return DELETE.
        outputView = new MemorySegmentOutputView(100);
        DefaultKvRecord.writeTo(outputView, key, null);
        kvRecord =
                DefaultKvRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()), 0, schemaId, readContext);
        assertThat(kvRecord.getMutationType()).isEqualTo(MutationType.DELETE);
    }

    @Test
    void testV2UpsertRoundTrip() throws Exception {
        KvRecordReadContext readContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));

        CompactedRow row;
        try (CompactedRowEncoder writer = new CompactedRowEncoder(baseRowFieldTypes)) {
            writer.startNewRow();
            writer.encodeField(0, 10);
            writer.encodeField(1, BinaryString.fromString("abc"));
            row = writer.finishRow();
        }

        byte[] key = new byte[] {1, 2};
        DefaultKvRecord.writeToV2(outputView, MutationType.UPSERT, key, row);

        KvRecord kvRecord =
                DefaultKvRecord.readFromV2(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()), 0, schemaId, readContext);

        // V2 size = V0 size + 1 byte for MutationType
        // 4 (length) + 1 (mutationType) + 1 (keyLen varint) + 2 (key) + 6 (row) = 14
        assertThat(kvRecord.getSizeInBytes()).isEqualTo(14);
        assertThat(keyToBytes(kvRecord)).isEqualTo(key);
        assertThat(kvRecord.getRow()).isEqualTo(row);
        assertThat(kvRecord.getMutationType()).isEqualTo(MutationType.UPSERT);
    }

    @Test
    void testV2DeleteRoundTrip() throws Exception {
        KvRecordReadContext readContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));

        byte[] key = new byte[] {3, 4};
        DefaultKvRecord.writeToV2(outputView, MutationType.DELETE, key, null);

        KvRecord kvRecord =
                DefaultKvRecord.readFromV2(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()), 0, schemaId, readContext);

        // 4 (length) + 1 (mutationType) + 1 (keyLen varint) + 2 (key) = 8
        assertThat(kvRecord.getSizeInBytes()).isEqualTo(8);
        assertThat(keyToBytes(kvRecord)).isEqualTo(key);
        assertThat(kvRecord.getRow()).isNull();
        assertThat(kvRecord.getMutationType()).isEqualTo(MutationType.DELETE);
    }

    @Test
    void testV2RetractRoundTrip() throws Exception {
        KvRecordReadContext readContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));

        CompactedRow row;
        try (CompactedRowEncoder writer = new CompactedRowEncoder(baseRowFieldTypes)) {
            writer.startNewRow();
            writer.encodeField(0, 99);
            writer.encodeField(1, BinaryString.fromString("old"));
            row = writer.finishRow();
        }

        byte[] key = new byte[] {5, 6};
        DefaultKvRecord.writeToV2(outputView, MutationType.RETRACT, key, row);

        KvRecord kvRecord =
                DefaultKvRecord.readFromV2(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()), 0, schemaId, readContext);

        assertThat(keyToBytes(kvRecord)).isEqualTo(key);
        assertThat(kvRecord.getRow()).isEqualTo(row);
        assertThat(kvRecord.getMutationType()).isEqualTo(MutationType.RETRACT);
    }

    @Test
    void testV2SizeOf() {
        byte[] key = new byte[] {1, 2};
        // V2 size should be V0 size + 1 (MutationType byte)
        assertThat(DefaultKvRecord.sizeOfV2(key, null))
                .isEqualTo(DefaultKvRecord.sizeOf(key, null) + 1);
    }
}
