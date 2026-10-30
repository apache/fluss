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
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests conversion of Paimon table metadata to a Fluss table descriptor. */
class PaimonTableToFlussTableDescriptorTest {

    private static final String DATABASE = "converter_db";

    @TempDir private File tempWarehouseDir;

    private PaimonLakeCatalog lakeCatalog;
    private int primaryKeyTableCount;

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

    @ParameterizedTest
    @MethodSource({"unsupportedAppendOnlySchemas", "unsupportedPrimaryKeySchemas"})
    void testRejectUnsupportedTableDefinitions(
            String tableName, Schema schema, String expectedMessage) throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, tableName);
        FileStoreTable table = createPaimonTable(tablePath, schema);
        assertThatThrownBy(() -> PaimonConversions.toFlussTableDescriptor(tablePath, table))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(expectedMessage)
                .hasMessageContaining(tablePath.toString());
    }

    private static Stream<Arguments> unsupportedAppendOnlySchemas() {
        return Stream.of(
                Arguments.of(
                        "mod_bucket_table",
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                                .option(CoreOptions.BUCKET.key(), "4")
                                .option(CoreOptions.BUCKET_KEY.key(), "id")
                                .option(CoreOptions.BUCKET_FUNCTION_TYPE.key(), "mod")
                                .build(),
                        "bucket function"),
                Arguments.of(
                        "unsupported_type_table",
                        Schema.newBuilder()
                                .column("payload", org.apache.paimon.types.DataTypes.VARIANT())
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .build(),
                        "column 'payload' uses unsupported type"),
                Arguments.of(
                        "rowkind_field_table",
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                                .column("op", org.apache.paimon.types.DataTypes.STRING().notNull())
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(CoreOptions.ROWKIND_FIELD.key(), "op")
                                .build(),
                        CoreOptions.ROWKIND_FIELD.key()),
                Arguments.of(
                        "data_evolution_table",
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                                .build(),
                        CoreOptions.DATA_EVOLUTION_ENABLED.key()),
                Arguments.of(
                        "row_tracking_table",
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                                .build(),
                        CoreOptions.ROW_TRACKING_ENABLED.key()),
                Arguments.of(
                        "deletion_vectors_table",
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT())
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                                .build(),
                        CoreOptions.DELETION_VECTORS_ENABLED.key()),
                Arguments.of(
                        "default_value_table",
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.INT(), null, "1")
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .build(),
                        "column 'id' has a default value"));
    }

    @ParameterizedTest
    @CsvSource({"false, yyyyMMdd", "false, yyyy-MM-dd", "true, yyyyMMdd", "true, yyyy-MM-dd"})
    void testConvertSingleStringTimePartition(boolean primaryKey, String formatter)
            throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                        .column("dt", org.apache.paimon.types.DataTypes.STRING().notNull())
                        .partitionKeys("dt")
                        .option(CoreOptions.BUCKET.key(), primaryKey ? "4" : "-1")
                        .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), formatter)
                        .option(CoreOptions.PARTITION_EXPIRATION_TIME.key(), "7 d")
                        .option("sink.process-time-zone", "UTC");
        if (primaryKey) {
            schema.primaryKey("id", "dt");
        }
        TableDescriptor descriptor = convert("single_time_partition", schema.build());

        assertThat(descriptor.getPartitionKeys()).containsExactly("dt");
        assertThat(descriptor.hasPrimaryKey()).isEqualTo(primaryKey);
        if (primaryKey) {
            assertThat(descriptor.getSchema().getPrimaryKeyColumnNames())
                    .containsExactly("id", "dt");
            assertThat(descriptor.getBucketKeys()).containsExactly("id");
        } else {
            assertThat(descriptor.getBucketKeys()).isEmpty();
        }
        assertThat(descriptor.getProperties())
                .hasSize(5)
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_KEY.key(), "dt")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), "DAY")
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), formatter)
                .containsEntry(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE.key(), "UTC")
                .doesNotContainKeys(
                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key(),
                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key(),
                        ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED.key());
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
    }

    @ParameterizedTest
    @MethodSource("unmappableTimePartitionSchemas")
    void testKeepUnmappableTimePartitionsRegular(String tableName, Schema schema) throws Exception {
        TableDescriptor descriptor = convert(tableName, schema);
        assertThat(descriptor.getPartitionKeys()).containsExactlyElementsOf(schema.partitionKeys());
        assertThat(descriptor.getProperties()).isEmpty();
    }

    private static Stream<Arguments> unmappableTimePartitionSchemas() {
        return Stream.of(
                Arguments.of(
                        "compact_date_partition",
                        Schema.newBuilder()
                                .column("dt", org.apache.paimon.types.DataTypes.DATE())
                                .partitionKeys("dt")
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd")
                                .build()),
                Arguments.of(
                        "missing_formatter_partition",
                        Schema.newBuilder()
                                .column("dt", org.apache.paimon.types.DataTypes.STRING())
                                .partitionKeys("dt")
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .build()),
                Arguments.of(
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
                                .build()));
    }

    @Test
    void testPreservePrimaryKeyAndPartitionOrder() throws Exception {
        TableDescriptor descriptor =
                convert(
                        "primary_key_table",
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column(
                                        "region",
                                        org.apache.paimon.types.DataTypes.STRING().notNull())
                                .column("dt", org.apache.paimon.types.DataTypes.STRING().notNull())
                                .column("payload", org.apache.paimon.types.DataTypes.STRING())
                                .primaryKey("region", "id", "dt")
                                .partitionKeys("dt", "region")
                                .option(CoreOptions.BUCKET.key(), "4")
                                .build());

        assertThat(descriptor.getSchema().getPrimaryKeyColumnNames())
                .containsExactly("region", "id", "dt");
        assertThat(descriptor.getPartitionKeys()).containsExactly("dt", "region");
        assertThat(descriptor.getBucketKeys()).containsExactly("id");
    }

    @ParameterizedTest
    @CsvSource({
        "deduplicate, false, false, true, , allow",
        "deduplicate, true, false, true, , ignore",
        "first-row, false, false, true, first_row, disable",
        "first-row, true, false, true, first_row, ignore",
        "partial-update, true, false, false, aggregation, ignore",
        "partial-update, false, true, false, aggregation, allow",
        "partial-update, false, true, true, aggregation, allow",
        "aggregation, false, true, true, aggregation, allow",
        "aggregation, true, true, false, aggregation, ignore",
        "aggregation, true, false, false, aggregation, ignore"
    })
    void testConvertMergeAndDeleteSemantics(
            String engine,
            boolean ignoreDelete,
            boolean removeOnDelete,
            boolean ignoreUpdateBefore,
            String flussEngine,
            String deleteBehavior)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.MERGE_ENGINE.key(), engine);
        options.put(CoreOptions.IGNORE_DELETE.key(), Boolean.toString(ignoreDelete));
        options.put(CoreOptions.IGNORE_UPDATE_BEFORE.key(), Boolean.toString(ignoreUpdateBefore));
        options.put(
                CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE.key(),
                Boolean.toString(removeOnDelete));
        options.put(
                CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE.key(),
                Boolean.toString(removeOnDelete));
        TableDescriptor descriptor = convertPrimaryKeyTable(options);
        Configuration configuration = Configuration.fromMap(descriptor.getProperties());
        DeleteBehavior expectedDeleteBehavior = DeleteBehavior.fromString(deleteBehavior);
        if (flussEngine == null && expectedDeleteBehavior == DeleteBehavior.ALLOW) {
            assertThat(descriptor.getProperties())
                    .doesNotContainKey(ConfigOptions.TABLE_DELETE_BEHAVIOR.key());
        } else {
            assertThat(configuration.getOptional(ConfigOptions.TABLE_DELETE_BEHAVIOR))
                    .contains(expectedDeleteBehavior);
        }
        if (flussEngine == null) {
            assertThat(descriptor.getProperties())
                    .doesNotContainKey(ConfigOptions.TABLE_MERGE_ENGINE.key());
        } else {
            assertThat(configuration.get(ConfigOptions.TABLE_MERGE_ENGINE))
                    .isEqualTo(MergeEngineType.fromString(flussEngine));
        }
        if ("aggregation".equals(flussEngine)) {
            assertThat(descriptor.getSchema().getAggFunction("payload"))
                    .contains(AggFunctions.LAST_VALUE_IGNORE_NULLS());
        }
    }

    @ParameterizedTest
    @CsvSource({
        "sum,sum,false",
        "min,min,true",
        "max,max,true",
        "first_value,first_value,true",
        "first_non_null_value,first_value_ignore_nulls,true",
        "last_value,last_value,false",
        "last_value,last_value,true",
        "last_non_null_value,last_value_ignore_nulls,false",
        "last_non_null_value,last_value_ignore_nulls,true",
        "bool_and,bool_and,true",
        "bool_or,bool_or,true",
        "rbm32,rbm32,true",
        "rbm64,rbm64,true"
    })
    void testConvertFieldAggregations(
            String paimonFunction, String flussFunction, boolean ignoreRetract) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.MERGE_ENGINE.key(), "aggregation");
        options.put(CoreOptions.IGNORE_DELETE.key(), "true");
        options.put(CoreOptions.FIELDS_DEFAULT_AGG_FUNC.key(), "last_value");
        options.put("fields.payload.aggregate-function", paimonFunction);
        options.put("fields.payload.ignore-retract", Boolean.toString(ignoreRetract));
        options.put("fields.version.aggregate-function", "last_non_null_value");
        DataType payloadType = DataTypes.BIGINT();
        if (paimonFunction.startsWith("bool_")) {
            payloadType = DataTypes.BOOLEAN();
        } else if (paimonFunction.startsWith("rbm")) {
            payloadType = DataTypes.BYTES();
        } else if ("first_value".equals(paimonFunction)) {
            payloadType = payloadType.notNull();
        }
        TableDescriptor descriptor = convertPrimaryKeyTable(options, payloadType);

        assertThat(descriptor.getSchema().getAggFunction("payload"))
                .get()
                .satisfies(
                        function ->
                                assertThat(function.getType().toString()).isEqualTo(flussFunction));
        assertThat(descriptor.getSchema().getAggFunction("version"))
                .contains(AggFunctions.LAST_VALUE_IGNORE_NULLS());
        assertThat(descriptor.getSchema().getAggFunction("event_time"))
                .contains(AggFunctions.LAST_VALUE());
        assertThat(descriptor.getSchema().getAggFunction("id")).isEmpty();
    }

    @Test
    void testConvertVersionedTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SEQUENCE_FIELD.key(), "version");
        options.put(CoreOptions.IGNORE_DELETE.key(), "true");
        TableDescriptor descriptor = convertPrimaryKeyTable(options);
        Configuration configuration = Configuration.fromMap(descriptor.getProperties());
        assertThat(configuration.get(ConfigOptions.TABLE_MERGE_ENGINE))
                .isEqualTo(MergeEngineType.VERSIONED);
        assertThat(configuration.get(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN))
                .isEqualTo("version");
        assertThat(configuration.getOptional(ConfigOptions.TABLE_DELETE_BEHAVIOR))
                .contains(DeleteBehavior.IGNORE);
    }

    @ParameterizedTest
    @CsvSource({"7 d, 604800000", "1 ms, 1"})
    void testConvertRecordExpiration(String ttl, long expectedMillis) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.RECORD_LEVEL_EXPIRE_TIME.key(), ttl);
        options.put(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "event_time");
        TableDescriptor descriptor = convertPrimaryKeyTable(options);
        assertThat(descriptor.getProperties())
                .containsEntry(ConfigOptions.TABLE_KV_TTL_TIME_COLUMN.key(), "event_time");
        assertThat(
                        Configuration.fromMap(descriptor.getProperties())
                                .get(ConfigOptions.TABLE_KV_TTL))
                .isEqualTo(Duration.ofMillis(expectedMillis));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0 ms", "1 ns", "999999 ns"})
    void testRejectInvalidRecordExpirationDuration(String ttl) {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.RECORD_LEVEL_EXPIRE_TIME.key(), ttl);
        options.put(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "event_time");
        assertInvalidPrimaryKeyOptions(
                options,
                "Invalid value '" + ttl + "'",
                CoreOptions.RECORD_LEVEL_EXPIRE_TIME.key(),
                "at least 1 millisecond");
    }

    @ParameterizedTest
    @CsvSource({
        "bucket,-1,bucket mode",
        "bucket,-2,bucket mode",
        "bucket-function.type,mod,bucket function",
        "rowkind.field,payload,rowkind.field",
        "sequence.snapshot-ordering,true,sequence.snapshot-ordering",
        "fields.version.sequence-group,payload,sequence-group",
        "partial-update.remove-record-on-sequence-group,version,remove-record-on-sequence-group"
    })
    void testRejectUnsupportedPrimaryKeyDefinition(String key, String value, String message)
            throws Exception {
        assertInvalidPrimaryKeyOptions(Collections.singletonMap(key, value), message);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "listagg",
                "collect",
                "nested_update",
                "merge_map",
                "hll_sketch",
                "first_non_null_value_legacy"
            })
    void testRejectUnsupportedAggregations(String function) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.MERGE_ENGINE.key(), "aggregation");
        options.put(CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE.key(), "true");
        options.put(CoreOptions.IGNORE_UPDATE_BEFORE.key(), "true");
        options.put(CoreOptions.FIELDS_DEFAULT_AGG_FUNC.key(), "last_value");
        options.put("fields.payload.aggregate-function", function);
        assertInvalidPrimaryKeyOptions(options, "fields.payload.aggregate-function");
    }

    @ParameterizedTest
    @CsvSource({
        // engine, function, ignoreRetract, expectedOption, expectedMessage
        "partial-update, , false, partial-update.remove-record-on-delete, UPDATE_BEFORE",
        "aggregation, sum, true, fields.version.ignore-retract, ignore-retract=false",
        "aggregation, product, false, fields.version.aggregate-function, division by zero",
        "aggregation, product, true, fields.version.aggregate-function, division by zero",
        "aggregation, min, false, fields.version.ignore-retract, ignore-retract=true",
        "aggregation, max, false, fields.version.ignore-retract, ignore-retract=true",
        "aggregation, first_value, false, fields.version.ignore-retract, ignore-retract=true",
        "aggregation, first_non_null_value, false, fields.version.ignore-retract, ignore-retract=true"
    })
    void testRejectIncompatibleMergeRetraction(
            String engine,
            String function,
            boolean ignoreRetract,
            String expectedOption,
            String expectedMessage) {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.MERGE_ENGINE.key(), engine);
        // Tiering bypasses the table-level filter, so ignoring UPDATE_BEFORE there is insufficient.
        options.put(CoreOptions.IGNORE_UPDATE_BEFORE.key(), "true");
        if (function != null) {
            options.put(CoreOptions.IGNORE_DELETE.key(), "true");
            options.put("fields.version.aggregate-function", function);
            options.put("fields.version.ignore-retract", Boolean.toString(ignoreRetract));
        }
        assertInvalidPrimaryKeyOptions(options, expectedOption, "Fluss tiering", expectedMessage);
    }

    @Test
    void testConvertMixedAggregations() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.MERGE_ENGINE.key(), "aggregation");
        options.put(CoreOptions.IGNORE_DELETE.key(), "true");
        options.put("fields.version.aggregate-function", "sum");
        options.put("fields.payload.aggregate-function", "max");
        options.put("fields.payload.ignore-retract", "true");
        TableDescriptor descriptor = convertPrimaryKeyTable(options);
        assertThat(descriptor.getSchema().getAggFunction("version")).contains(AggFunctions.SUM());
        assertThat(descriptor.getSchema().getAggFunction("payload")).contains(AggFunctions.MAX());

        // Whole-row deletion cannot be combined with field-level ignore-retract in Paimon.
        // Without that field policy, the current tiering path cannot write this mixed changelog.
        options.put(CoreOptions.IGNORE_DELETE.key(), "false");
        options.put(CoreOptions.IGNORE_UPDATE_BEFORE.key(), "true");
        options.put(CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE.key(), "true");
        options.remove("fields.payload.ignore-retract");
        assertInvalidPrimaryKeyOptions(
                options,
                "fields.payload.ignore-retract",
                "aggregation.remove-record-on-delete=true");
    }

    @ParameterizedTest
    @CsvSource({
        "sequence.field,version,sequence.field",
        "sequence.field,payload,nullable sequence.field",
        "sequence.field.sort-order,descending,sequence.field",
        "sequence.field,'version,id',sequence.field"
    })
    void testRejectUnsupportedSequenceRules(String key, String value, String message)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SEQUENCE_FIELD.key(), "version");
        options.put(CoreOptions.IGNORE_DELETE.key(), "true");
        if ("version".equals(value)) {
            options.put(CoreOptions.IGNORE_DELETE.key(), "false");
        }
        options.put(key, value);
        assertInvalidPrimaryKeyOptions(options, message);
    }

    @Test
    void testRejectUnsupportedAggregationRetraction() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.MERGE_ENGINE.key(), "aggregation");
        options.put("fields.version.aggregate-function", "sum");
        assertInvalidPrimaryKeyOptions(
                options,
                "merge-engine=aggregation",
                "ignore-delete=false",
                "aggregation.remove-record-on-delete=true",
                "Fluss aggregation supports whole-row DELETE semantics only",
                "whole-row deletion");
        options.put(CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE.key(), "true");
        assertInvalidPrimaryKeyOptions(
                options,
                "merge-engine=aggregation",
                "ignore-delete=false",
                "ignore-update-before=true",
                "Fluss aggregation requires UPDATE_BEFORE records to be ignored",
                "UPDATE_BEFORE");
    }

    private static Stream<Arguments> unsupportedPrimaryKeySchemas() {
        return Stream.of(
                Arguments.of(
                        "nullable_primary_key",
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .primaryKey("id")
                                .option(CoreOptions.BUCKET.key(), "4")
                                .option("primary-key.nullable", "true")
                                .build(),
                        "nullable primary key"),
                // Paimon accepts INT seconds, while Fluss TTL requires milliseconds or a timestamp.
                Arguments.of(
                        "expiration_int_time",
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column("event_time", DataTypes.INT())
                                .primaryKey("id")
                                .option(CoreOptions.BUCKET.key(), "4")
                                .option(CoreOptions.RECORD_LEVEL_EXPIRE_TIME.key(), "1 d")
                                .option(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "event_time")
                                .build(),
                        "Fluss row-level TTL supports only BIGINT, TIMESTAMP and TIMESTAMP_LTZ"),
                Arguments.of(
                        "expiration_bigint_time",
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column("event_time", DataTypes.BIGINT())
                                .primaryKey("id")
                                .option(CoreOptions.BUCKET.key(), "4")
                                .option(CoreOptions.RECORD_LEVEL_EXPIRE_TIME.key(), "7 d")
                                .option(CoreOptions.RECORD_LEVEL_TIME_FIELD.key(), "event_time")
                                .build(),
                        "Paimon accepts both epoch seconds and milliseconds"),
                // Paimon checks the missing time field when writing, after accepting the schema.
                Arguments.of(
                        "expiration_missing_time_field",
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .primaryKey("id")
                                .option(CoreOptions.BUCKET.key(), "4")
                                .option(CoreOptions.RECORD_LEVEL_EXPIRE_TIME.key(), "1 d")
                                .build(),
                        "Fluss event-time TTL requires 'record-level.time-field' to be specified"));
    }

    private TableDescriptor convertPrimaryKeyTable(Map<String, String> options) throws Exception {
        return convertPrimaryKeyTable(options, DataTypes.STRING());
    }

    private TableDescriptor convertPrimaryKeyTable(
            Map<String, String> options, DataType payloadType) throws Exception {
        TablePath path = TablePath.of(DATABASE, "primary_key_options_" + primaryKeyTableCount++);
        FileStoreTable table =
                createPaimonTable(
                        path,
                        Schema.newBuilder()
                                .column("id", org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column(
                                        "version",
                                        org.apache.paimon.types.DataTypes.BIGINT().notNull())
                                .column("payload", payloadType)
                                .column("event_time", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                                .primaryKey("id")
                                .option(CoreOptions.BUCKET.key(), "4")
                                .build());
        // Build a schema copy to exercise our validation independently of Paimon's DDL checks.
        Map<String, String> sourceOptions = new HashMap<>(table.options());
        sourceOptions.putAll(options);
        return PaimonConversions.toFlussTableDescriptor(
                path, table.copy(table.schema().copy(sourceOptions)));
    }

    private void assertInvalidPrimaryKeyOptions(Map<String, String> options, String... messages) {
        assertThatThrownBy(() -> convertPrimaryKeyTable(options))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContainingAll(messages);
    }

    @ParameterizedTest
    @ValueSource(strings = {"row", "array", "map_key", "map_value", "deep_nested"})
    void testRejectNestedColumnDefaultValues(String container) throws Exception {
        DataType rowWithDefault =
                DataTypes.ROW(new DataField(0, "count", DataTypes.INT(), null, "1"));
        DataType payloadType;
        String defaultColumn;
        switch (container) {
            case "array":
                payloadType = DataTypes.ARRAY(rowWithDefault);
                defaultColumn = "payload.element.count";
                break;
            case "map_key":
                payloadType = DataTypes.MAP(rowWithDefault, DataTypes.STRING());
                defaultColumn = "payload.key.count";
                break;
            case "map_value":
                payloadType = DataTypes.MAP(DataTypes.STRING(), rowWithDefault);
                defaultColumn = "payload.value.count";
                break;
            case "deep_nested":
                payloadType =
                        DataTypes.ARRAY(
                                DataTypes.MAP(
                                        DataTypes.STRING(),
                                        DataTypes.ROW(new DataField(0, "inner", rowWithDefault))));
                defaultColumn = "payload.element.value.inner.count";
                break;
            default:
                payloadType = rowWithDefault;
                defaultColumn = "payload.count";
        }
        TablePath tablePath = TablePath.of(DATABASE, "nested_default_" + container);
        FileStoreTable table =
                createPaimonTable(
                        tablePath,
                        Schema.newBuilder()
                                .column("payload", payloadType)
                                .option(CoreOptions.BUCKET.key(), "-1")
                                .build());

        // Verify Paimon persists the nested default before checking our rejection.
        assertThat(table.rowType().getField("payload").type().asSQLString()).contains("DEFAULT 1");
        assertThatThrownBy(() -> PaimonConversions.toFlussTableDescriptor(tablePath, table))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("column '" + defaultColumn + "' has a default value");
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
}
