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

import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultKvRecordBatch}. */
class DefaultKvRecordBatchTest extends KvTestBase {

    @Test
    void writeAndReadBatch() throws Exception {
        int recordNumber = 100;
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);

        List<byte[]> keys = new ArrayList<>();
        List<CompactedRow> rows = new ArrayList<>();
        for (int i = 0; i < recordNumber; i++) {
            byte[] key = new byte[] {(byte) i, (byte) i};
            CompactedRow row =
                    i % 2 == 1 ? null : TestInternalRowGenerator.genCompactedRowForAllType();
            builder.append(key, row);
            keys.add(key);
            rows.add(row);
        }

        KvRecordBatch kvRecords = DefaultKvRecordBatch.pointToBytesView(builder.build());
        kvRecords.ensureValid();

        // verify the header info
        assertThat(kvRecords.getRecordCount()).isEqualTo(recordNumber);
        assertThat(kvRecords.magic()).isEqualTo(magic);
        assertThat(kvRecords.isValid()).isTrue();
        assertThat(kvRecords.schemaId()).isEqualTo(schemaId);

        // verify record.
        int i = 0;
        for (KvRecord record :
                kvRecords.records(
                        KvRecordReadContext.createReadContext(
                                KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA)))) {
            assertThat(keyToBytes(record)).isEqualTo(keys.get(i));
            assertThat(record.getRow()).isEqualTo(rows.get(i));
            i++;
        }

        builder.close();
    }

    @Test
    void testNullRowIsNormalizedAsDelete() throws Exception {
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);
        byte[] key = new byte[] {1, 2};
        builder.append(key, null);

        DefaultKvRecordBatch batch = DefaultKvRecordBatch.pointToBytesView(builder.build());
        batch.ensureValid();

        KvRecord record =
                batch.records(
                                KvRecordReadContext.createReadContext(
                                        KvFormat.COMPACTED,
                                        new TestingSchemaGetter(1, DATA1_SCHEMA)))
                        .iterator()
                        .next();
        assertThat(record.getRow()).isNull();

        builder.close();
    }

    @Test
    void testV0BatchDefaultMutationType() throws Exception {
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);

        CompactedRow row;
        try (CompactedRowEncoder writer = new CompactedRowEncoder(baseRowFieldTypes)) {
            writer.startNewRow();
            writer.encodeField(0, 1);
            writer.encodeField(1, BinaryString.fromString("v0"));
            row = writer.finishRow();
        }

        builder.append(new byte[] {1}, row);
        builder.append(new byte[] {2}, null);

        DefaultKvRecordBatch batch = DefaultKvRecordBatch.pointToBytesView(builder.build());
        batch.ensureValid();

        // V0 read context (v2Format=false)
        KvRecordReadContext readContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));

        List<KvRecord> records = new ArrayList<>();
        for (KvRecord record : batch.records(readContext)) {
            records.add(record);
        }

        assertThat(records).hasSize(2);
        // Non-null row -> UPSERT
        assertThat(records.get(0).getMutationType()).isEqualTo(MutationType.UPSERT);
        assertThat(records.get(0).getRow()).isNotNull();
        // Null row -> DELETE
        assertThat(records.get(1).getMutationType()).isEqualTo(MutationType.DELETE);
        assertThat(records.get(1).getRow()).isNull();

        builder.close();
    }

    @Test
    void testV2BatchMixedMutationTypes() throws Exception {
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED,
                        true);

        CompactedRow upsertRow;
        try (CompactedRowEncoder writer = new CompactedRowEncoder(baseRowFieldTypes)) {
            writer.startNewRow();
            writer.encodeField(0, 10);
            writer.encodeField(1, BinaryString.fromString("upsert"));
            upsertRow = writer.finishRow();
        }

        CompactedRow retractRow;
        try (CompactedRowEncoder writer = new CompactedRowEncoder(baseRowFieldTypes)) {
            writer.startNewRow();
            writer.encodeField(0, 20);
            writer.encodeField(1, BinaryString.fromString("retract"));
            retractRow = writer.finishRow();
        }

        byte[] key1 = new byte[] {1};
        byte[] key2 = new byte[] {2};
        byte[] key3 = new byte[] {3};

        builder.appendV2(MutationType.UPSERT, key1, upsertRow);
        builder.appendV2(MutationType.DELETE, key2, null);
        builder.appendV2(MutationType.RETRACT, key3, retractRow);

        DefaultKvRecordBatch batch = DefaultKvRecordBatch.pointToBytesView(builder.build());
        batch.ensureValid();

        assertThat(batch.getRecordCount()).isEqualTo(3);

        // V2 read context
        KvRecordReadContext readContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));

        List<KvRecord> records = new ArrayList<>();
        for (KvRecord record : batch.records(readContext)) {
            records.add(record);
        }

        assertThat(records).hasSize(3);

        // Record 0: UPSERT
        assertThat(keyToBytes(records.get(0))).isEqualTo(key1);
        assertThat(records.get(0).getRow()).isEqualTo(upsertRow);
        assertThat(records.get(0).getMutationType()).isEqualTo(MutationType.UPSERT);

        // Record 1: DELETE
        assertThat(keyToBytes(records.get(1))).isEqualTo(key2);
        assertThat(records.get(1).getRow()).isNull();
        assertThat(records.get(1).getMutationType()).isEqualTo(MutationType.DELETE);

        // Record 2: RETRACT
        assertThat(keyToBytes(records.get(2))).isEqualTo(key3);
        assertThat(records.get(2).getRow()).isEqualTo(retractRow);
        assertThat(records.get(2).getMutationType()).isEqualTo(MutationType.RETRACT);

        builder.close();
    }

    @Test
    void testV2FormatAttributeRoundTrip() throws Exception {
        // V2 batch: isV2Format() should be true
        KvRecordBatchBuilder v2Builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED,
                        true);

        CompactedRow row;
        try (CompactedRowEncoder writer = new CompactedRowEncoder(baseRowFieldTypes)) {
            writer.startNewRow();
            writer.encodeField(0, 42);
            writer.encodeField(1, BinaryString.fromString("v2test"));
            row = writer.finishRow();
        }

        v2Builder.appendV2(MutationType.UPSERT, new byte[] {1}, row);

        DefaultKvRecordBatch v2Batch = DefaultKvRecordBatch.pointToBytesView(v2Builder.build());
        v2Batch.ensureValid();
        assertThat(v2Batch.isV2Format()).isTrue();
        assertThat(v2Batch.getRecordCount()).isEqualTo(1);

        v2Builder.close();

        // V0 batch: isV2Format() should be false
        KvRecordBatchBuilder v0Builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);

        v0Builder.append(new byte[] {2}, row);

        DefaultKvRecordBatch v0Batch = DefaultKvRecordBatch.pointToBytesView(v0Builder.build());
        v0Batch.ensureValid();
        assertThat(v0Batch.isV2Format()).isFalse();
        assertThat(v0Batch.getRecordCount()).isEqualTo(1);

        v0Builder.close();
    }
}
