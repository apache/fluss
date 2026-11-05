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

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
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
        assertThat(kvRecords.hasMutationType()).isFalse();

        // verify record.
        int i = 0;
        for (KvRecord record :
                kvRecords.records(
                        KvRecordReadContext.createReadContext(
                                KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA)))) {
            assertThat(keyToBytes(record)).isEqualTo(keys.get(i));
            assertThat(record.getRow()).isEqualTo(rows.get(i));
            assertThat(record.getMutationType())
                    .isEqualTo(rows.get(i) == null ? MutationType.DELETE : MutationType.UPSERT);
            i++;
        }
        assertThat(i).isEqualTo(recordNumber);

        builder.close();
    }

    @Test
    void testBatchWithMixedMutationTypes() throws Exception {
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

        builder.append(MutationType.UPSERT, key1, upsertRow);
        builder.append(MutationType.DELETE, key2, null);
        builder.append(MutationType.RETRACT, key3, retractRow);

        DefaultKvRecordBatch batch = DefaultKvRecordBatch.pointToBytesView(builder.build());
        batch.ensureValid();

        assertThat(batch.getRecordCount()).isEqualTo(3);

        // Read context for records with explicit mutation types
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
    void testMutationTypeAttributeRoundTrip() throws Exception {
        // Batches carrying mutation types set the corresponding attribute.
        KvRecordBatchBuilder builderWithMutationType =
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

        builderWithMutationType.append(MutationType.UPSERT, new byte[] {1}, row);

        DefaultKvRecordBatch batchWithMutationType =
                DefaultKvRecordBatch.pointToBytesView(builderWithMutationType.build());
        batchWithMutationType.ensureValid();
        assertThat(batchWithMutationType.hasMutationType()).isTrue();
        assertThat(batchWithMutationType.getRecordCount()).isEqualTo(1);

        builderWithMutationType.close();

        // Batches without mutation types leave the attribute unset.
        KvRecordBatchBuilder builderWithoutMutationType =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);

        builderWithoutMutationType.append(new byte[] {2}, row);

        DefaultKvRecordBatch batchWithoutMutationType =
                DefaultKvRecordBatch.pointToBytesView(builderWithoutMutationType.build());
        batchWithoutMutationType.ensureValid();
        assertThat(batchWithoutMutationType.hasMutationType()).isFalse();
        assertThat(batchWithoutMutationType.getRecordCount()).isEqualTo(1);

        builderWithoutMutationType.close();
    }

    @Test
    void testEqualsAndHashCode() throws Exception {
        DefaultKvRecordBatch batch1 = buildBatch();
        DefaultKvRecordBatch batch2 = buildBatch();

        assertThat(batch1).isEqualTo(batch2);
        assertThat(batch1).hasSameHashCodeAs(batch2);
    }

    @Test
    void testHashCodeDiffersForDifferentContents() throws Exception {
        DefaultKvRecordBatch batch1 = buildBatch();
        DefaultKvRecordBatch batch2 = buildBatch(new byte[] {(byte) 9, (byte) 9});

        assertThat(batch1).isNotEqualTo(batch2);
        assertThat(batch1.hashCode()).isNotEqualTo(batch2.hashCode());
    }

    private DefaultKvRecordBatch buildBatch() throws Exception {
        return buildBatch(new byte[] {(byte) 1, (byte) 1});
    }

    private DefaultKvRecordBatch buildBatch(byte[] key) throws Exception {
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(100),
                        KvFormat.COMPACTED);
        builder.append(key, compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a1"}));
        DefaultKvRecordBatch batch = DefaultKvRecordBatch.pointToBytesView(builder.build());
        builder.close();
        return batch;
    }
}
