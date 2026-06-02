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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.MultiTable;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.MultiTableRecord;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.MultiTableLogScanner;
import org.apache.fluss.client.table.scanner.log.MultiTableRecords;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link MultiTableWriter} / {@link MultiTableWriterImpl}. */
class MultiTableWriterITCase extends ClientToServerITCaseBase {

    private static final Schema LOG_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    private static final Schema LOG_SCHEMA_2 =
            Schema.newBuilder()
                    .column("key", DataTypes.INT())
                    .column("value", DataTypes.STRING())
                    .column("score", DataTypes.BIGINT())
                    .build();

    private static final Schema PK_SCHEMA =
            Schema.newBuilder()
                    .primaryKey("id")
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    // ---------------------------------------------------------------- log

    @Test
    void testWriteSingleLogTable() throws Exception {
        TablePath tp = TablePath.of("test_db", "mt_w_single_log");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build();
        long tableId = createTable(tp, descriptor, false);
        waitAllReplicasReady(tableId, 1);
        int sid = currentSchemaId(tp);

        int recordCount = 5;
        List<GenericRow> expected = new ArrayList<>();
        MultiTable multiTable = conn.getMultiTable();
        try (MultiTableWriter writer = multiTable.newMultiTableWrite().createWriter()) {
            for (int i = 0; i < recordCount; i++) {
                GenericRow r = row(i, "v-" + i);
                expected.add(r);
                writer.write(MultiTableWriteRecord.of(tp, ChangeType.INSERT, r, sid)).get();
            }
            writer.flush();
        }

        // Verify through a log scanner bound to the table.
        List<GenericRow> actual = pollAllAppendRows(tp, recordCount);
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testWriteMultipleLogTables() throws Exception {
        TablePath tp1 = TablePath.of("test_db", "mt_w_multi_log1");
        TablePath tp2 = TablePath.of("test_db", "mt_w_multi_log2");
        long id1 =
                createTable(
                        tp1,
                        TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build(),
                        false);
        long id2 =
                createTable(
                        tp2,
                        TableDescriptor.builder().schema(LOG_SCHEMA_2).distributedBy(1).build(),
                        true);
        waitAllReplicasReady(id1, 1);
        waitAllReplicasReady(id2, 1);
        int sid1 = currentSchemaId(tp1);
        int sid2 = currentSchemaId(tp2);

        int n1 = 3;
        int n2 = 4;
        List<GenericRow> expected1 = new ArrayList<>();
        List<GenericRow> expected2 = new ArrayList<>();

        try (MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter()) {
            for (int i = 0; i < n1; i++) {
                GenericRow r = row(i, "t1-" + i);
                expected1.add(r);
                writer.write(MultiTableWriteRecord.of(tp1, ChangeType.INSERT, r, sid1)).get();
            }
            for (int i = 0; i < n2; i++) {
                GenericRow r = row(100 + i, "t2-" + i, (long) (i * 10));
                expected2.add(r);
                writer.write(MultiTableWriteRecord.of(tp2, ChangeType.APPEND_ONLY, r, sid2)).get();
            }
            writer.flush();
        }

        assertThat(pollAllAppendRows(tp1, n1)).containsExactlyInAnyOrderElementsOf(expected1);
        assertThat(pollAllAppendRows(tp2, n2)).containsExactlyInAnyOrderElementsOf(expected2);
    }

    // ---------------------------------------------------------------- pk

    @Test
    void testWriteSinglePkTableUpsertAndDelete() throws Exception {
        TablePath tp = TablePath.of("test_db", "mt_w_pk");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(PK_SCHEMA).distributedBy(1).build();
        long tableId = createTable(tp, descriptor, false);
        waitAllReplicasReady(tableId, 1);
        int sid = currentSchemaId(tp);

        try (MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter()) {
            // INSERT (upsert semantic)
            writer.write(MultiTableWriteRecord.of(tp, ChangeType.INSERT, row(1, "a"), sid)).get();

            // UPDATE_AFTER (upsert semantic)
            writer.write(
                            MultiTableWriteRecord.of(
                                    tp, ChangeType.UPDATE_AFTER, row(1, "a-updated"), sid))
                    .get();

            // UPDATE_BEFORE is skipped entirely (returns sentinel).
            writer.write(MultiTableWriteRecord.of(tp, ChangeType.UPDATE_BEFORE, row(1, "a"), sid))
                    .get();

            writer.flush();
        }

        // Verify: pk=1 resolves to ("a-updated").
        try (Table table = conn.getTable(tp)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow found = lookupRow(lookuper, row(1));
            assertThat(found).isNotNull();
            assertThat(found.getInt(0)).isEqualTo(1);
            assertThat(found.getString(1).toString()).isEqualTo("a-updated");
        }

        // DELETE: pk=1 disappears.
        try (MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter()) {
            writer.write(MultiTableWriteRecord.of(tp, ChangeType.DELETE, row(1, "a-updated"), sid))
                    .get();
            writer.flush();
        }
        try (Table table = conn.getTable(tp)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            assertThat(lookupRow(lookuper, row(1))).isNull();
        }
    }

    // ---------------------------------------------------------------- mix

    @Test
    void testWriteMixedLogAndPkTables() throws Exception {
        TablePath logTp = TablePath.of("test_db", "mt_w_mix_log");
        TablePath pkTp = TablePath.of("test_db", "mt_w_mix_pk");
        long logId =
                createTable(
                        logTp,
                        TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build(),
                        false);
        long pkId =
                createTable(
                        pkTp,
                        TableDescriptor.builder().schema(PK_SCHEMA).distributedBy(1).build(),
                        true);
        waitAllReplicasReady(logId, 1);
        waitAllReplicasReady(pkId, 1);
        int logSid = currentSchemaId(logTp);
        int pkSid = currentSchemaId(pkTp);

        try (MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter()) {
            writer.write(
                            MultiTableWriteRecord.of(
                                    logTp, ChangeType.INSERT, row(1, "log-a"), logSid))
                    .get();
            writer.write(MultiTableWriteRecord.of(pkTp, ChangeType.INSERT, row(42, "pk-a"), pkSid))
                    .get();
            writer.write(
                            MultiTableWriteRecord.of(
                                    pkTp, ChangeType.UPDATE_AFTER, row(42, "pk-b"), pkSid))
                    .get();
            writer.flush();
        }

        // Log table: one appended row.
        List<GenericRow> logRows = pollAllAppendRows(logTp, 1);
        assertThat(logRows).hasSize(1);

        // PK table: key=42 has value "pk-b".
        try (Table table = conn.getTable(pkTp)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow found = lookupRow(lookuper, row(42));
            assertThat(found).isNotNull();
            assertThat(found.getString(1).toString()).isEqualTo("pk-b");
        }
    }

    // ---------------------------------------------------------------- partitioned

    @Test
    void testWritePartitionedTable() throws Exception {
        TablePath tp = TablePath.of("test_db", "mt_w_part");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .partitionedBy("pt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        long tableId = createTable(tp, descriptor, false);

        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tp);
        assertThat(partitionIdByNames).isNotEmpty();

        int perPartition = 2;
        int partSid = currentSchemaId(tp);
        try (MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter()) {
            for (String partitionName : partitionIdByNames.keySet()) {
                for (int i = 0; i < perPartition; i++) {
                    writer.write(
                                    MultiTableWriteRecord.of(
                                            tp,
                                            ChangeType.INSERT,
                                            row(i, "v-" + i, partitionName),
                                            partSid))
                            .get();
                }
            }
            writer.flush();
        }

        int total = perPartition * partitionIdByNames.size();
        // Scan across all partitions via a MultiTableLogScanner.
        try (MultiTableLogScanner scanner =
                conn.getMultiTable().newMultiTableScan().createLogScanner()) {
            for (Long partitionId : partitionIdByNames.values()) {
                scanner.subscribeFromBeginning(tp, partitionId, 0);
            }
            int polled = 0;
            long deadline = System.currentTimeMillis() + 30_000;
            while (polled < total && System.currentTimeMillis() < deadline) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                polled += records.count();
            }
            assertThat(polled).isEqualTo(total);
        }
    }

    // ---------------------------------------------------------------- cdc mirror

    @Test
    void testCdcMirrorScanThenWrite() throws Exception {
        TablePath src = TablePath.of("test_db", "mt_w_cdc_src");
        TablePath dst = TablePath.of("test_db", "mt_w_cdc_dst");
        long srcId =
                createTable(
                        src,
                        TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build(),
                        false);
        long dstId =
                createTable(
                        dst,
                        TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build(),
                        true);
        waitAllReplicasReady(srcId, 1);
        waitAllReplicasReady(dstId, 1);

        // 1. Seed src via the single-table AppendWriter.
        int recordCount = 6;
        List<GenericRow> seeded = new ArrayList<>();
        try (Table table = conn.getTable(src)) {
            AppendWriter append = table.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                GenericRow r = row(i, "src-" + i);
                seeded.add(r);
                append.append(r).get();
            }
        }

        // 2. Mirror src -> dst via MultiTable scanner + writer.
        MultiTable multiTable = conn.getMultiTable();
        try (MultiTableLogScanner scanner = multiTable.newMultiTableScan().createLogScanner();
                MultiTableWriter writer = multiTable.newMultiTableWrite().createWriter()) {
            scanner.subscribeFromBeginning(src, 0);
            int mirrored = 0;
            long deadline = System.currentTimeMillis() + 30_000;
            while (mirrored < recordCount && System.currentTimeMillis() < deadline) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (MultiTableRecord scanned : records) {
                    // Retarget to dst; forward original ChangeType / row / schemaId so the writer
                    // refreshes metadata if needed.
                    MultiTableWriteRecord retarget =
                            MultiTableWriteRecord.of(
                                    dst,
                                    scanned.getChangeType(),
                                    scanned.getRow(),
                                    scanned.getSchemaId());
                    writer.write(retarget).get();
                    mirrored++;
                }
            }
            writer.flush();
            assertThat(mirrored).isEqualTo(recordCount);
        }

        // 3. Verify dst has the same rows as src.
        List<GenericRow> mirroredRows = pollAllAppendRows(dst, recordCount);
        assertThat(mirroredRows).containsExactlyInAnyOrderElementsOf(seeded);
    }

    // ---------------------------------------------------------------- errors

    @Test
    void testWriteUnknownTableFailsFuture() throws Exception {
        TablePath unknown = TablePath.of("test_db", "mt_w_unknown_table");
        try (MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter()) {
            assertThatThrownBy(
                            () ->
                                    writer.write(
                                                    MultiTableWriteRecord.of(
                                                            unknown,
                                                            ChangeType.INSERT,
                                                            row(1, "v"),
                                                            1))
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .rootCause()
                    .hasMessageContaining("Table 'test_db.mt_w_unknown_table' does not exist.");
        }
    }

    @Test
    void testChangeTypeIncompatibleWithTableKind() throws Exception {
        // DELETE on a log table must fail (log tables have no PK).
        TablePath logTp = TablePath.of("test_db", "mt_w_bad_ct_log");
        long logId =
                createTable(
                        logTp,
                        TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build(),
                        false);
        waitAllReplicasReady(logId, 1);
        int logSid = currentSchemaId(logTp);

        try (MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter()) {
            assertThatThrownBy(
                            () ->
                                    writer.write(
                                                    MultiTableWriteRecord.of(
                                                            logTp,
                                                            ChangeType.DELETE,
                                                            row(1, "v"),
                                                            logSid))
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("log table");
        }
    }

    @Test
    void testFlushAndClose() throws Exception {
        TablePath tp = TablePath.of("test_db", "mt_w_flush_close");
        long tableId =
                createTable(
                        tp,
                        TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build(),
                        false);
        waitAllReplicasReady(tableId, 1);
        int sid = currentSchemaId(tp);

        MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter();
        writer.write(MultiTableWriteRecord.of(tp, ChangeType.INSERT, row(1, "x"), sid)).get();
        writer.flush();
        writer.close();

        // Further writes after close must fail synchronously.
        assertThatThrownBy(
                        () ->
                                writer.write(
                                        MultiTableWriteRecord.of(
                                                tp, ChangeType.INSERT, row(2, "y"), sid)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");

        // Close is idempotent.
        writer.close();
    }

    @Test
    void testWriteAfterAddColumn() throws Exception {
        // Verify the writer transparently picks up a new schema after an ALTER TABLE ADD
        // COLUMN: an old-schema record tagged with the original schemaId uses the cached
        // encoder, and a record tagged with the post-alter schemaId triggers a metadata
        // refresh + encoder rebuild so both batches end up readable with their original
        // shapes.
        TablePath tp = TablePath.of("test_db", "mt_w_add_column");
        long tableId =
                createTable(
                        tp,
                        TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build(),
                        false);
        waitAllReplicasReady(tableId, 1);
        int oldSid = currentSchemaId(tp);

        int preCount = 3;
        int postCount = 3;
        List<GenericRow> expectedPre = new ArrayList<>();
        List<GenericRow> expectedPost = new ArrayList<>();

        try (MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter()) {
            // Phase 1: write with the original 2-column schema.
            for (int i = 0; i < preCount; i++) {
                GenericRow r = row(i, "before-" + i);
                expectedPre.add(r);
                writer.write(MultiTableWriteRecord.of(tp, ChangeType.INSERT, r, oldSid)).get();
            }

            // ALTER TABLE: append a new column "score BIGINT".
            admin.alterTable(
                            tp,
                            Collections.singletonList(
                                    TableChange.addColumn(
                                            "score",
                                            DataTypes.BIGINT(),
                                            null,
                                            TableChange.ColumnPosition.last())),
                            false)
                    .get();
            FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
            int newSid = waitForSchemaIdBump(tp, oldSid);

            // Phase 2: write the new 3-column rows tagged with the bumped schemaId &mdash;
            // the writer must flush old-schema buffered records, refresh metadata and rebuild
            // the encoder against the new schema before encoding the wider row.
            for (int i = 0; i < postCount; i++) {
                GenericRow r = row(100 + i, "after-" + i, (long) (i * 10));
                expectedPost.add(r);
                writer.write(MultiTableWriteRecord.of(tp, ChangeType.INSERT, r, newSid)).get();
            }
            writer.flush();
        }

        // Read back using the multi-table scanner so each record keeps its write-time schema,
        // and split rows by their RowType field count.
        int total = preCount + postCount;
        List<GenericRow> actualPre = new ArrayList<>();
        List<GenericRow> actualPost = new ArrayList<>();
        try (MultiTableLogScanner scanner =
                conn.getMultiTable().newMultiTableScan().createLogScanner()) {
            scanner.subscribeFromBeginning(tp, 0);
            long deadline = System.currentTimeMillis() + 30_000;
            int polled = 0;
            while (polled < total && System.currentTimeMillis() < deadline) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (MultiTableRecord rec : records) {
                    assertThat(rec.getTableId()).isEqualTo(tableId);
                    InternalRow r = rec.getRow();
                    int fc = rec.getSchema().getRowType().getFieldCount();
                    if (fc == 2) {
                        actualPre.add(row(r.getInt(0), r.getString(1)));
                    } else {
                        assertThat(fc).isEqualTo(3);
                        actualPost.add(row(r.getInt(0), r.getString(1), r.getLong(2)));
                    }
                    polled++;
                }
            }
        }

        assertThat(actualPre).containsExactlyInAnyOrderElementsOf(expectedPre);
        assertThat(actualPost).containsExactlyInAnyOrderElementsOf(expectedPost);
    }

    @Test
    void testWriteWithFutureSchemaIdFailsAfterRefresh() throws Exception {
        // When the record's schemaId differs from the cached state, the writer refreshes
        // table metadata and rebuilds encoders against the latest schema. If after the
        // refresh the record's schemaId is still ahead of the server's schemaId (i.e., the
        // caller is referencing a future / unknown schema), the write must fail.
        TablePath tp = TablePath.of("test_db", "mt_w_bad_schema_id");
        long tableId =
                createTable(
                        tp,
                        TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build(),
                        false);
        waitAllReplicasReady(tableId, 1);
        int sid = currentSchemaId(tp);
        int futureSid = sid + 99;

        try (MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter()) {
            assertThatThrownBy(
                            () ->
                                    writer.write(
                                                    MultiTableWriteRecord.of(
                                                            tp,
                                                            ChangeType.INSERT,
                                                            row(1, "v"),
                                                            futureSid))
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("SchemaId mismatch")
                    .hasMessageContaining("record schemaId=" + futureSid)
                    .hasMessageContaining("current table schemaId=" + sid)
                    .hasMessageContaining("after metadata refresh");
        }
    }

    @Test
    void testWriteHistoricalSchemaAfterSchemaEvolves() throws Exception {
        // After the table has evolved to a new schema, a record still tagged with the OLD schemaId
        // (record.schemaId < latest schemaId) must be encoded against its historical schema and
        // sent successfully &mdash; exercising the writer's historical-schema build path rather
        // than
        // the cached-latest fast path used by testWriteAfterAddColumn (which writes old rows BEFORE
        // the alter, while the cached latest still IS the old schema).
        TablePath tp = TablePath.of("test_db", "mt_w_historical_schema");
        long tableId =
                createTable(
                        tp,
                        TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1).build(),
                        false);
        waitAllReplicasReady(tableId, 1);
        int oldSid = currentSchemaId(tp);

        // Evolve the schema first: append "score BIGINT" so the table's latest schema is the
        // 3-column shape before any write happens.
        admin.alterTable(
                        tp,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "score",
                                        DataTypes.BIGINT(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        int newSid = waitForSchemaIdBump(tp, oldSid);

        List<GenericRow> expectedNew = new ArrayList<>();
        List<GenericRow> expectedOld = new ArrayList<>();
        try (MultiTableWriter writer = conn.getMultiTable().newMultiTableWrite().createWriter()) {
            // Prime the writer with the latest (3-column) schema so its cached "latest" is newSid.
            for (int i = 0; i < 2; i++) {
                GenericRow r = row(100 + i, "new-" + i, (long) (i * 10));
                expectedNew.add(r);
                writer.write(MultiTableWriteRecord.of(tp, ChangeType.INSERT, r, newSid)).get();
            }

            // Now write 2-column rows tagged with the historical oldSid. Because oldSid is behind
            // the cached latest newSid, the writer must fetch the historical schema and build a
            // dedicated encoder so the narrow row is encoded & sent against schemaId=oldSid.
            for (int i = 0; i < 3; i++) {
                GenericRow r = row(i, "old-" + i);
                expectedOld.add(r);
                writer.write(MultiTableWriteRecord.of(tp, ChangeType.INSERT, r, oldSid)).get();
            }
            writer.flush();
        }

        // Read back via the multi-table scanner so each record keeps its write-time schema, then
        // split rows by their RowType field count.
        int total = expectedNew.size() + expectedOld.size();
        List<GenericRow> actualNew = new ArrayList<>();
        List<GenericRow> actualOld = new ArrayList<>();
        try (MultiTableLogScanner scanner =
                conn.getMultiTable().newMultiTableScan().createLogScanner()) {
            scanner.subscribeFromBeginning(tp, 0);
            long deadline = System.currentTimeMillis() + 30_000;
            int polled = 0;
            while (polled < total && System.currentTimeMillis() < deadline) {
                MultiTableRecords records = scanner.poll(Duration.ofSeconds(1));
                for (MultiTableRecord rec : records) {
                    assertThat(rec.getTableId()).isEqualTo(tableId);
                    InternalRow r = rec.getRow();
                    int fc = rec.getSchema().getRowType().getFieldCount();
                    if (fc == 2) {
                        actualOld.add(row(r.getInt(0), r.getString(1)));
                    } else {
                        assertThat(fc).isEqualTo(3);
                        actualNew.add(row(r.getInt(0), r.getString(1), r.getLong(2)));
                    }
                    polled++;
                }
            }
        }

        assertThat(actualOld).containsExactlyInAnyOrderElementsOf(expectedOld);
        assertThat(actualNew).containsExactlyInAnyOrderElementsOf(expectedNew);
    }

    // ---------------------------------------------------------------- helpers

    private int currentSchemaId(TablePath tp) throws Exception {
        return admin.getTableInfo(tp).get().getSchemaId();
    }

    /**
     * Polls {@link #currentSchemaId(TablePath)} until it advances past {@code oldSid} or the
     * deadline expires; returns the new schemaId.
     */
    private int waitForSchemaIdBump(TablePath tp, int oldSid) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        int sid = currentSchemaId(tp);
        while (sid == oldSid && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
            sid = currentSchemaId(tp);
        }
        assertThat(sid).isGreaterThan(oldSid);

        return sid;
    }

    private List<GenericRow> pollAllAppendRows(TablePath tp, int expected) throws Exception {
        List<GenericRow> result = new ArrayList<>();
        try (Table table = conn.getTable(tp);
                LogScanner scanner = table.newScan().createLogScanner()) {
            subscribeFromBeginning(scanner, table);
            int fieldCount = table.getTableInfo().getRowType().getFieldCount();
            long deadline = System.currentTimeMillis() + 30_000;
            while (result.size() < expected && System.currentTimeMillis() < deadline) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : records.buckets()) {
                    for (ScanRecord r : records.records(bucket)) {
                        result.add(copyRow(r.getRow(), fieldCount));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Copies a scanner-produced row field-by-field into a detached {@link GenericRow} using the two
     * row shapes produced by this test: {@code (int, string)} or {@code (int, string, long)}.
     */
    private static GenericRow copyRow(InternalRow row, int fieldCount) {
        GenericRow out = new GenericRow(fieldCount);
        out.setField(0, row.getInt(0));
        out.setField(1, row.getString(1));
        if (fieldCount == 3) {
            out.setField(2, row.getLong(2));
        }
        return out;
    }
}
