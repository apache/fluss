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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.encode.paimon.PaimonKeyEncoder;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.utils.UnsafeUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonLakeTableLookuper}. */
class PaimonLakeTableLookuperTest {

    private static final String TEST_DB = "test_db";

    private @TempDir File tempWarehouseDir;
    private Configuration paimonConfig;
    private Catalog paimonCatalog;

    @BeforeEach
    void setUp() {
        paimonConfig = new Configuration();
        paimonConfig.setString("type", "paimon");
        paimonConfig.setString("warehouse", tempWarehouseDir.toString());
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
    }

    @AfterEach
    void tearDown() throws Exception {
        paimonCatalog.close();
    }

    @Test
    void testPartitionedTableLookup() throws Exception {
        TablePath tablePath = TablePath.of(TEST_DB, "partitioned_pk_table");
        createPartitionedPkTable(tablePath);

        // Write records to two partitions
        writeRecords(
                tablePath,
                Arrays.asList(
                        GenericRow.of(
                                BinaryString.fromString("p1"),
                                1,
                                BinaryString.fromString("aaa"),
                                0,
                                0L,
                                Timestamp.fromEpochMillis(0)),
                        GenericRow.of(
                                BinaryString.fromString("p2"),
                                2,
                                BinaryString.fromString("bbb"),
                                0,
                                0L,
                                Timestamp.fromEpochMillis(0))));

        RowType flussRowType = buildPartitionedFlussRowType();
        PaimonKeyEncoder keyEncoder = new PaimonKeyEncoder(flussRowType, Arrays.asList("pk", "pt"));
        org.apache.fluss.types.DataType[] flussTypes =
                new org.apache.fluss.types.DataType[] {
                    new StringType(), new IntType(), new StringType()
                };

        try (PaimonLakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(paimonConfig, tablePath)) {
            // Lookup in partition "p1"
            ResolvedPartitionSpec partitionP1 =
                    new ResolvedPartitionSpec(
                            Collections.singletonList("pt"), Collections.singletonList("p1"));
            LakeTableLookuper.LookupContext contextP1 =
                    new LakeTableLookuper.LookupContext(partitionP1, 0, 1);

            byte[] keyBytesP1 =
                    keyEncoder.encodeKey(
                            org.apache.fluss.row.GenericRow.of(
                                    org.apache.fluss.row.BinaryString.fromString("p1"),
                                    1,
                                    org.apache.fluss.row.BinaryString.fromString("aaa")));
            byte[] result = lookuper.lookup(keyBytesP1, contextP1);

            assertThat(result).isNotNull();
            short schemaId = UnsafeUtils.getShort(result, 0);
            assertThat(schemaId).isEqualTo((short) 1);

            CompactedRow row = decodeValueRow(result, flussTypes);
            assertThat(row.getString(0).toString()).isEqualTo("p1");
            assertThat(row.getInt(1)).isEqualTo(1);
            assertThat(row.getString(2).toString()).isEqualTo("aaa");

            // Lookup in partition "p2"
            ResolvedPartitionSpec partitionP2 =
                    new ResolvedPartitionSpec(
                            Collections.singletonList("pt"), Collections.singletonList("p2"));
            LakeTableLookuper.LookupContext contextP2 =
                    new LakeTableLookuper.LookupContext(partitionP2, 0, 1);

            byte[] keyBytesP2 =
                    keyEncoder.encodeKey(
                            org.apache.fluss.row.GenericRow.of(
                                    org.apache.fluss.row.BinaryString.fromString("p2"),
                                    2,
                                    org.apache.fluss.row.BinaryString.fromString("bbb")));
            byte[] result2 = lookuper.lookup(keyBytesP2, contextP2);

            assertThat(result2).isNotNull();
            CompactedRow row2 = decodeValueRow(result2, flussTypes);
            assertThat(row2.getString(0).toString()).isEqualTo("p2");
            assertThat(row2.getInt(1)).isEqualTo(2);
            assertThat(row2.getString(2).toString()).isEqualTo("bbb");

            // Lookup missing key should return null
            byte[] missingKeyBytes =
                    keyEncoder.encodeKey(
                            org.apache.fluss.row.GenericRow.of(
                                    org.apache.fluss.row.BinaryString.fromString("p1"),
                                    999,
                                    org.apache.fluss.row.BinaryString.fromString("nonexistent")));
            assertThat(lookuper.lookup(missingKeyBytes, contextP1)).isNull();

            // Write updated data to p1 (new snapshot) and verify refresh
            writeRecords(
                    tablePath,
                    Collections.singletonList(
                            GenericRow.of(
                                    BinaryString.fromString("p1"),
                                    1,
                                    BinaryString.fromString("aaa_v2"),
                                    0,
                                    1L,
                                    Timestamp.fromEpochMillis(1))));

            byte[] refreshedResult = lookuper.lookup(keyBytesP1, contextP1);
            assertThat(refreshedResult).isNotNull();
            assertThat(decodeValueRow(refreshedResult, flussTypes).getString(2).toString())
                    .isEqualTo("aaa_v2");
        }
    }

    @Test
    void testSchemaEvolution() throws Exception {
        TablePath tablePath = TablePath.of(TEST_DB, "schema_evo_table");
        createPartitionedPkTable(tablePath);

        // Write with schema v1: user columns = (pt, pk, val)
        writeRecords(
                tablePath,
                Collections.singletonList(
                        GenericRow.of(
                                BinaryString.fromString("p1"),
                                1,
                                BinaryString.fromString("v1"),
                                0,
                                0L,
                                Timestamp.fromEpochMillis(0))));

        RowType flussRowTypeV1 = buildPartitionedFlussRowType();
        PaimonKeyEncoder keyEncoder =
                new PaimonKeyEncoder(flussRowTypeV1, Arrays.asList("pk", "pt"));

        try (PaimonLakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(paimonConfig, tablePath)) {
            ResolvedPartitionSpec partP1 =
                    new ResolvedPartitionSpec(
                            Collections.singletonList("pt"), Collections.singletonList("p1"));

            byte[] keyBytesK1 =
                    keyEncoder.encodeKey(
                            org.apache.fluss.row.GenericRow.of(
                                    org.apache.fluss.row.BinaryString.fromString("p1"),
                                    1,
                                    org.apache.fluss.row.BinaryString.fromString("v1")));

            // Lookup with schemaId=1 → 3 user columns
            LakeTableLookuper.LookupContext ctxV1 =
                    new LakeTableLookuper.LookupContext(partP1, 0, 1);
            byte[] resultV1 = lookuper.lookup(keyBytesK1, ctxV1);
            assertThat(resultV1).isNotNull();

            org.apache.fluss.types.DataType[] typesV1 =
                    new org.apache.fluss.types.DataType[] {
                        new StringType(), new IntType(), new StringType()
                    };
            CompactedRow rowV1 = decodeValueRow(resultV1, typesV1);
            assertThat(rowV1.getString(0).toString()).isEqualTo("p1");
            assertThat(rowV1.getInt(1)).isEqualTo(1);
            assertThat(rowV1.getString(2).toString()).isEqualTo("v1");

            // Evolve schema: add "extra" column after "val" (before system columns)
            Identifier tableId =
                    Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
            paimonCatalog.alterTable(
                    tableId,
                    Collections.singletonList(
                            SchemaChange.addColumn(
                                    "extra",
                                    DataTypes.STRING(),
                                    null,
                                    SchemaChange.Move.after("extra", "val"))),
                    false);

            // Write with schema v2: user columns = (pt, pk, val, extra)
            writeRecords(
                    tablePath,
                    Collections.singletonList(
                            GenericRow.of(
                                    BinaryString.fromString("p1"),
                                    2,
                                    BinaryString.fromString("v2"),
                                    BinaryString.fromString("extra_val"),
                                    0,
                                    1L,
                                    Timestamp.fromEpochMillis(1))));

            org.apache.fluss.types.DataType[] typesV2 =
                    new org.apache.fluss.types.DataType[] {
                        new StringType(), new IntType(), new StringType(), new StringType()
                    };

            // Lookup old key k1 with schemaId=2 → lookuper reinitializes with new schema
            LakeTableLookuper.LookupContext ctxV2 =
                    new LakeTableLookuper.LookupContext(partP1, 0, 2);
            byte[] resultK1V2 = lookuper.lookup(keyBytesK1, ctxV2);
            assertThat(resultK1V2).isNotNull();

            CompactedRow rowK1V2 = decodeValueRow(resultK1V2, typesV2);
            assertThat(rowK1V2.getString(0).toString()).isEqualTo("p1");
            assertThat(rowK1V2.getInt(1)).isEqualTo(1);
            assertThat(rowK1V2.getString(2).toString()).isEqualTo("v1");
            assertThat(rowK1V2.isNullAt(3)).isTrue(); // new column is null for old data

            // Lookup new key k2 with schemaId=2 → full data
            byte[] keyBytesK2 =
                    keyEncoder.encodeKey(
                            org.apache.fluss.row.GenericRow.of(
                                    org.apache.fluss.row.BinaryString.fromString("p1"),
                                    2,
                                    org.apache.fluss.row.BinaryString.fromString("v2")));
            byte[] resultK2V2 = lookuper.lookup(keyBytesK2, ctxV2);
            assertThat(resultK2V2).isNotNull();

            CompactedRow rowK2V2 = decodeValueRow(resultK2V2, typesV2);
            assertThat(rowK2V2.getString(0).toString()).isEqualTo("p1");
            assertThat(rowK2V2.getInt(1)).isEqualTo(2);
            assertThat(rowK2V2.getString(2).toString()).isEqualTo("v2");
            assertThat(rowK2V2.getString(3).toString()).isEqualTo("extra_val");
        }
    }

    // -------------------------------------------------------------------------
    //  Helper methods
    // -------------------------------------------------------------------------

    private void createPartitionedPkTable(TablePath tablePath) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.STRING())
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .column("__bucket", DataTypes.INT())
                        .column("__offset", DataTypes.BIGINT())
                        .column("__timestamp", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .primaryKey("pk", "pt")
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(
                                CoreOptions.CHANGELOG_PRODUCER.key(),
                                CoreOptions.ChangelogProducer.INPUT.toString())
                        .build();
        createTable(tablePath, schema);
    }

    private void createTable(TablePath tablePath, Schema schema) throws Exception {
        paimonCatalog.createDatabase(tablePath.getDatabaseName(), true);
        paimonCatalog.createTable(
                Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName()),
                schema,
                true);
    }

    private void writeRecords(TablePath tablePath, List<org.apache.paimon.data.InternalRow> records)
            throws Exception {
        Table table =
                paimonCatalog.getTable(
                        Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName()));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer = writeBuilder.newWrite()) {
            for (org.apache.paimon.data.InternalRow record : records) {
                writer.write(record);
            }
            List<CommitMessage> messages = writer.prepareCommit();
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(messages);
            }
        }
    }

    private static RowType buildPartitionedFlussRowType() {
        return new RowType(
                Arrays.asList(
                        new DataField("pt", new StringType()),
                        new DataField("pk", new IntType()),
                        new DataField("val", new StringType())));
    }

    private static CompactedRow decodeValueRow(
            byte[] valueBytes, org.apache.fluss.types.DataType[] types) {
        byte[] rowBytes =
                Arrays.copyOfRange(valueBytes, ValueEncoder.SCHEMA_ID_LENGTH, valueBytes.length);
        CompactedRow row = new CompactedRow(types);
        row.pointTo(org.apache.fluss.memory.MemorySegment.wrap(rowBytes), 0, rowBytes.length);
        return row;
    }
}
