/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.paimon.utils.PaimonConversions;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests conversion of Paimon table metadata to a Fluss table descriptor. */
class PaimonTableToFlussTableDescriptorTest {

    private static final String DATABASE = "converter_db";

    @TempDir private File tempWarehouseDir;

    private PaimonLakeCatalog lakeCatalog;

    @BeforeEach
    void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());
        lakeCatalog = new PaimonLakeCatalog(configuration);
    }

    @AfterEach
    void tearDown() {
        lakeCatalog.close();
    }

    @Test
    void testConvertSchemaAndSupportedDistributions() throws Exception {
        TableDescriptor hashFixedDescriptor =
                convert(
                        "hash_fixed_table",
                        Schema.newBuilder()
                                .column(
                                        "id",
                                        org.apache.paimon.types.DataTypes.BIGINT().notNull(),
                                        "identifier")
                                .column(
                                        "name",
                                        org.apache.paimon.types.DataTypes.VARCHAR(20),
                                        "display name")
                                .column("payload", org.apache.paimon.types.DataTypes.VARBINARY(128))
                                .column("pt", org.apache.paimon.types.DataTypes.STRING().notNull())
                                .partitionKeys("pt")
                                .option(CoreOptions.BUCKET.key(), "4")
                                .option(CoreOptions.BUCKET_KEY.key(), "id")
                                .option("future.compaction.option", "ignored")
                                .comment("existing table")
                                .build());

        assertThat(hashFixedDescriptor.getSchema().getColumnNames())
                .containsExactly("id", "name", "payload", "pt");
        assertThat(hashFixedDescriptor.getSchema().getColumns().get(0).getDataType().isNullable())
                .isFalse();
        assertThat(hashFixedDescriptor.getSchema().getColumns().get(0).getComment())
                .contains("identifier");
        assertThat(hashFixedDescriptor.getSchema().getColumns().get(1).getComment())
                .contains("display name");
        assertThat(hashFixedDescriptor.getPartitionKeys()).containsExactly("pt");
        assertThat(hashFixedDescriptor.getBucketKeys()).containsExactly("id");
        assertThat(hashFixedDescriptor.getTableDistribution())
                .get()
                .satisfies(distribution -> assertThat(distribution.getBucketCount()).contains(4));
        assertThat(hashFixedDescriptor.getProperties()).isEmpty();
        assertThat(hashFixedDescriptor.getCustomProperties()).isEmpty();
        assertThat(hashFixedDescriptor.getComment()).contains("existing table");

        TableDescriptor bucketUnawareDescriptor =
                convert(
                        "bucket_unaware_table",
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .build());

        assertThat(bucketUnawareDescriptor.getSchema().getColumnNames()).containsExactly("id");
        assertThat(bucketUnawareDescriptor.getBucketKeys()).isEmpty();
        assertThat(bucketUnawareDescriptor.getTableDistribution())
                .get()
                .satisfies(distribution -> assertThat(distribution.getBucketCount()).isEmpty());
    }

    @Test
    void testRejectUnsupportedTableDefinitions() throws Exception {
        assertInvalidTableDescriptor(
                "primary_key_table",
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("name", org.apache.paimon.types.DataTypes.STRING())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "4")
                        .build(),
                "primary-key table");

        assertInvalidTableDescriptor(
                "mod_bucket_table",
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "4")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option(CoreOptions.BUCKET_FUNCTION_TYPE.key(), "mod")
                        .build(),
                "bucket function");

        assertInvalidTableDescriptor(
                "unsupported_type_table",
                Schema.newBuilder()
                        .column("payload", org.apache.paimon.types.DataTypes.VARIANT())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build(),
                "column 'payload' uses unsupported type");

        assertInvalidTableDescriptor(
                "rowkind_field_table",
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .column("op", org.apache.paimon.types.DataTypes.STRING().notNull())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROWKIND_FIELD.key(), "op")
                        .build(),
                CoreOptions.ROWKIND_FIELD.key());

        assertInvalidTableDescriptor(
                "data_evolution_table",
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build(),
                CoreOptions.DATA_EVOLUTION_ENABLED.key());

        assertInvalidTableDescriptor(
                "row_tracking_table",
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .build(),
                CoreOptions.ROW_TRACKING_ENABLED.key());

        assertInvalidTableDescriptor(
                "deletion_vectors_table",
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build(),
                CoreOptions.DELETION_VECTORS_ENABLED.key());

        assertInvalidTableDescriptor(
                "default_value_table",
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.INT(), null, "1")
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .build(),
                "column 'id' has a default value");
    }

    @Test
    void testConvertSingleStringTimePartition() throws Exception {
        TableDescriptor descriptor =
                convert(
                        "single_time_partition",
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                                .column("dt", org.apache.paimon.types.DataTypes.STRING())
                                .partitionKeys("dt")
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd")
                                .option("sink.process-time-zone", "UTC")
                                .build());

        assertThat(descriptor.getPartitionKeys()).containsExactly("dt");
        assertThat(descriptor.getProperties())
                .hasSize(5)
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_KEY.key(), "dt")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "DAY")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "yyyyMMdd")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE.key(), "UTC")
                .doesNotContainKeys(
                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key(),
                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key());
    }

    @Test
    void testConvertMultiColumnTimePartition() throws Exception {
        TableDescriptor descriptor =
                convert(
                        "multi_column_time_partition",
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                                .column("region", org.apache.paimon.types.DataTypes.STRING())
                                .column("dt", org.apache.paimon.types.DataTypes.STRING())
                                .partitionKeys("region", "dt")
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$dt")
                                .option(
                                        CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(),
                                        "yyyy-MM-dd")
                                .option(CoreOptions.PARTITION_EXPIRATION_TIME.key(), "90 d")
                                .option(
                                        CoreOptions.PARTITION_EXPIRATION_STRATEGY.key(),
                                        "values-time")
                                .option("sink.process-time-zone", "Asia/Shanghai")
                                .build());

        assertThat(descriptor.getPartitionKeys()).containsExactly("region", "dt");
        assertThat(descriptor.getProperties())
                .hasSize(6)
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_KEY.key(), "dt")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "DAY")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "yyyy-MM-dd")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE.key(), "Asia/Shanghai")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key(), "0")
                .doesNotContainKey(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key());
    }

    @Test
    void testConvertDateTimePartition() throws Exception {
        TableDescriptor isoDateDescriptor =
                convert(
                        "iso_date_partition",
                        Schema.newBuilder()
                                .column("dt", org.apache.paimon.types.DataTypes.DATE())
                                .partitionKeys("dt")
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(
                                        CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(),
                                        "yyyy-MM-dd")
                                .build());

        assertThat(isoDateDescriptor.getPartitionKeys()).containsExactly("dt");
        assertThat(isoDateDescriptor.getProperties())
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "DAY")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), "yyyy-MM-dd");

        TableDescriptor compactDateDescriptor =
                convert(
                        "compact_date_partition",
                        Schema.newBuilder()
                                .column("dt", org.apache.paimon.types.DataTypes.DATE())
                                .partitionKeys("dt")
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd")
                                .build());

        assertThat(compactDateDescriptor.getPartitionKeys()).containsExactly("dt");
        assertThat(compactDateDescriptor.getProperties()).isEmpty();
    }

    @Test
    void testKeepUnmappableTimePartitionsRegular() throws Exception {
        TableDescriptor missingFormatterDescriptor =
                convert(
                        "missing_formatter_partition",
                        Schema.newBuilder()
                                .column("dt", org.apache.paimon.types.DataTypes.STRING())
                                .partitionKeys("dt")
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .build());
        assertThat(missingFormatterDescriptor.getPartitionKeys()).containsExactly("dt");
        assertThat(missingFormatterDescriptor.getProperties()).isEmpty();

        TableDescriptor compositePatternDescriptor =
                convert(
                        "composite_pattern_partition",
                        Schema.newBuilder()
                                .column("year", org.apache.paimon.types.DataTypes.STRING())
                                .column("month", org.apache.paimon.types.DataTypes.STRING())
                                .partitionKeys("year", "month")
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(
                                        CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(),
                                        "$year-$month")
                                .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyy-MM")
                                .build());
        assertThat(compositePatternDescriptor.getPartitionKeys()).containsExactly("year", "month");
        assertThat(compositePatternDescriptor.getProperties()).isEmpty();
    }

    private TableDescriptor convert(String tableName, Schema paimonSchema) throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, tableName);
        FileStoreTable table = createPaimonTable(tablePath, paimonSchema);
        return PaimonConversions.toFlussTableDescriptor(tablePath, table);
    }

    private FileStoreTable createPaimonTable(TablePath tablePath, Schema paimonSchema)
            throws Exception {
        lakeCatalog.getPaimonCatalog().createDatabase(tablePath.getDatabaseName(), true);
        lakeCatalog.getPaimonCatalog().createTable(toPaimon(tablePath), paimonSchema, false);
        return (FileStoreTable) lakeCatalog.getPaimonCatalog().getTable(toPaimon(tablePath));
    }

    private void assertInvalidTableDescriptor(
            String tableName, Schema paimonSchema, String expectedMessage) throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, tableName);
        FileStoreTable table = createPaimonTable(tablePath, paimonSchema);
        assertThatThrownBy(() -> PaimonConversions.toFlussTableDescriptor(tablePath, table))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(expectedMessage)
                .hasMessageContaining(tablePath.toString());
    }
}
