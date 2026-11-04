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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.paimon.source.FlussRowAsPaimonRow;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.PartitionUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.BucketSpec;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;
import static org.apache.fluss.lake.paimon.PaimonLakeCatalog.LEGACY_SYSTEM_COLUMNS;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Utils for conversion between Paimon and Fluss. */
public class PaimonConversions {

    // use literal directly, a future paimon version will rename
    // the variable PARTITION_GENERATE_LEGCY_NAME to PARTITION_GENERATE_LEGACY_NAME, use literal
    // can help avoid NoSuchField error
    // todo: after upgrade paimon version, we call fall back to use PARTITION_GENERATE_LEGACY_NAME
    // again
    /** Option controlling whether Paimon uses legacy partition value encoding. */
    public static final String PARTITION_GENERATE_LEGACY_NAME_OPTION_KEY = "partition.legacy-name";

    /**
     * Native Paimon table option maintained by Fluss to mark whether the (clean-layout) Paimon
     * table is currently accelerated by Fluss LakeStream. Managed only for new-layout tables that
     * do not carry the Fluss system columns; legacy tables are left untouched. Disabling lake
     * acceleration removes the option instead of persisting {@code false}.
     */
    public static final String LAKESTREAM_ENABLED_OPTION_KEY = "lakestream.enabled";

    // for fluss config
    public static final String FLUSS_CONF_PREFIX = "fluss.";
    public static final String TABLE_DATALAKE_PAIMON_PREFIX = "table.datalake.paimon.";
    // for paimon config
    private static final String PAIMON_CONF_PREFIX = "paimon.";

    /** Paimon config options set by Fluss should not be set by users. */
    @VisibleForTesting public static final Set<String> PAIMON_UNSETTABLE_OPTIONS = new HashSet<>();

    static {
        PAIMON_UNSETTABLE_OPTIONS.add(CoreOptions.BUCKET.key());
        PAIMON_UNSETTABLE_OPTIONS.add(CoreOptions.BUCKET_KEY.key());
        PAIMON_UNSETTABLE_OPTIONS.add(PARTITION_GENERATE_LEGACY_NAME_OPTION_KEY);
    }

    public static RowKind toRowKind(ChangeType changeType) {
        switch (changeType) {
            case APPEND_ONLY:
            case INSERT:
                return RowKind.INSERT;
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case DELETE:
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException("Unsupported change type: " + changeType);
        }
    }

    public static ChangeType toChangeType(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
                return ChangeType.INSERT;
            case UPDATE_BEFORE:
                return ChangeType.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return ChangeType.UPDATE_AFTER;
            case DELETE:
                return ChangeType.DELETE;
            default:
                throw new IllegalArgumentException("Unsupported rowKind: " + rowKind);
        }
    }

    public static Identifier toPaimon(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public static Object toPaimonLiteral(DataType dataType, Object flussLiteral) {
        RowType rowType = RowType.of(dataType);
        InternalRow flussRow = GenericRow.of(flussLiteral);
        FlussRowAsPaimonRow flussRowAsPaimonRow = new FlussRowAsPaimonRow(flussRow, rowType);
        return org.apache.paimon.data.InternalRow.createFieldGetter(dataType, 0)
                .getFieldOrNull(flussRowAsPaimonRow);
    }

    public static org.apache.fluss.types.RowType toFlussRowType(RowType paimonRowType) {
        org.apache.fluss.types.RowType.Builder builder = org.apache.fluss.types.RowType.builder();
        for (DataField field : paimonRowType.getFields()) {
            builder.field(
                    field.name(), field.type().accept(PaimonDataTypeToFlussDataType.INSTANCE));
        }
        return builder.build();
    }

    /**
     * Encodes a Paimon row as a Fluss KV value.
     *
     * @throws RuntimeException if encoding or closing the encoder fails
     */
    public static byte[] toFlussValue(
            org.apache.paimon.data.InternalRow paimonRow,
            short schemaId,
            org.apache.fluss.types.RowType valueRowType,
            KvFormat kvFormat) {
        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);
        InternalRow.FieldGetter[] fieldGetters = InternalRow.createFieldGetters(valueRowType);
        try (RowEncoder rowEncoder = RowEncoder.create(kvFormat, valueRowType)) {
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldGetters.length; i++) {
                rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(flussRow));
            }
            return ValueEncoder.encodeValue(schemaId, rowEncoder.finishRow());
        } catch (Exception e) {
            throw new RuntimeException("Failed to encode Paimon lookup row as Fluss value.", e);
        }
    }

    /**
     * Renders a Paimon partition row into Fluss partition value strings, in partition-key order.
     */
    public static List<String> toFlussPartitionValues(
            BinaryRow partition, org.apache.fluss.types.RowType flussPartitionType) {
        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow().replaceRow(partition);
        List<String> values = new ArrayList<>(partition.getFieldCount());
        for (int i = 0; i < partition.getFieldCount(); i++) {
            org.apache.fluss.types.DataType flussType = flussPartitionType.getTypeAt(i);
            Object value = InternalRow.createFieldGetter(flussType, i).getFieldOrNull(flussRow);
            values.add(PartitionUtils.convertValueOfType(value, flussType.getTypeRoot()));
        }
        return values;
    }

    /** Converts a Fluss resolved partition spec to a Paimon partition row. */
    public static BinaryRow toPaimonPartition(
            ResolvedPartitionSpec partitionSpec,
            org.apache.fluss.types.RowType flussRowType,
            RowType paimonRowType,
            Function<org.apache.paimon.data.InternalRow, BinaryRow> partitionExtractor) {
        List<String> partitionKeys = partitionSpec.getPartitionKeys();
        List<String> partitionValues = partitionSpec.getPartitionValues();

        // The synthetic row must match the Paimon table row layout, including system columns.
        GenericRow partitionRow = new GenericRow(paimonRowType.getFieldCount());
        for (int i = 0; i < partitionKeys.size(); i++) {
            String partitionKey = partitionKeys.get(i);
            int fieldIndex = flussRowType.getFieldIndex(partitionKey);
            checkState(
                    fieldIndex >= 0,
                    "Partition key '%s' not found in Fluss row type.",
                    partitionKey);
            DataTypeRoot typeRoot = flussRowType.getTypeAt(fieldIndex).getTypeRoot();
            partitionRow.setField(
                    fieldIndex, PartitionUtils.parseValueOfType(partitionValues.get(i), typeRoot));
        }

        return partitionExtractor.apply(new FlussRowAsPaimonRow(partitionRow, paimonRowType));
    }

    public static List<SchemaChange> toPaimonSchemaChanges(
            Table paimonTable, List<TableChange> tableChanges) {
        // A legacy table (created before FIP-27) still carries the three trailing system columns,
        // recognisable by the presence of the __timestamp column. A clean table has none of them.
        boolean paimonIncludingSystemColumns = PaimonUtils.isLegacyTable(paimonTable.rowType());
        List<SchemaChange> schemaChanges = new ArrayList<>(tableChanges.size());

        for (TableChange tableChange : tableChanges) {
            if (tableChange instanceof TableChange.SetOption) {
                TableChange.SetOption setOption = (TableChange.SetOption) tableChange;
                String key = convertFlussPropertyKeyToPaimon(setOption.getKey());
                validateAlterPaimonOptions(key);
                schemaChanges.add(SchemaChange.setOption(key, setOption.getValue()));
                if (TABLE_DATALAKE_ENABLED.key().equals(setOption.getKey())) {
                    // #4102: keep lakestream.enabled in sync with datalake acceleration state.
                    appendLakeStreamOptionChange(
                            Boolean.parseBoolean(setOption.getValue()),
                            paimonIncludingSystemColumns,
                            schemaChanges);
                }
            } else if (tableChange instanceof TableChange.ResetOption) {
                TableChange.ResetOption resetOption = (TableChange.ResetOption) tableChange;
                String key = convertFlussPropertyKeyToPaimon(resetOption.getKey());
                validateAlterPaimonOptions(key);
                schemaChanges.add(SchemaChange.removeOption(key));
                if (TABLE_DATALAKE_ENABLED.key().equals(resetOption.getKey())) {
                    // #4102: resetting datalake.enabled is equivalent to disabling acceleration.
                    appendLakeStreamOptionChange(
                            false, paimonIncludingSystemColumns, schemaChanges);
                }
            } else if (tableChange instanceof TableChange.AddColumn) {
                TableChange.AddColumn addColumn = (TableChange.AddColumn) tableChange;

                if (LEGACY_SYSTEM_COLUMNS.containsKey(addColumn.getName())) {
                    throw new InvalidTableException(
                            "Column "
                                    + addColumn.getName()
                                    + " conflicts with a system column name of paimon table, please rename the column.");
                }

                if (!(addColumn.getPosition() instanceof TableChange.Last)) {
                    throw new UnsupportedOperationException(
                            "Only support to add column at last for paimon table.");
                }

                org.apache.fluss.types.DataType flussDataType = addColumn.getDataType();
                if (!flussDataType.isNullable()) {
                    throw new UnsupportedOperationException(
                            "Only support to add nullable column for paimon table.");
                }

                org.apache.paimon.types.DataType paimonDataType =
                        flussDataType.accept(FlussDataTypeToPaimonDataType.INSTANCE);

                if (paimonIncludingSystemColumns) {
                    // Legacy tables keep the three system columns as the last physical columns, so
                    // a new business column must be inserted right before the first system column.
                    String firstSystemColumnName = LEGACY_SYSTEM_COLUMNS.keySet().iterator().next();
                    schemaChanges.add(
                            SchemaChange.addColumn(
                                    addColumn.getName(),
                                    paimonDataType,
                                    addColumn.getComment(),
                                    SchemaChange.Move.before(
                                            addColumn.getName(), firstSystemColumnName)));
                } else {
                    // Clean tables have no trailing system columns, so a new business column is
                    // simply appended at the end.
                    schemaChanges.add(
                            SchemaChange.addColumn(
                                    addColumn.getName(),
                                    paimonDataType,
                                    addColumn.getComment(),
                                    SchemaChange.Move.last(addColumn.getName())));
                }
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported table change: " + tableChange.getClass());
            }
        }

        return schemaChanges;
    }

    // ------------------------------------------------------------------------
    // Table metadata conversions
    // ------------------------------------------------------------------------

    /** Converts an existing Paimon append-only table to a Fluss table descriptor. */
    public static TableDescriptor toFlussTableDescriptor(
            TablePath tablePath, FileStoreTable table) {
        if (!table.primaryKeys().isEmpty()) {
            throw new InvalidTableException(
                    String.format(
                            "Converting the Paimon primary-key table %s to a Fluss table is not supported yet.",
                            tablePath));
        }
        validateAppendOnlyTable(tablePath, table);

        List<DataField> dataFields = table.schema().fields();
        org.apache.fluss.metadata.Schema.Builder schemaBuilder =
                org.apache.fluss.metadata.Schema.newBuilder();
        for (DataField dataField : dataFields) {
            try {
                schemaBuilder
                        .column(
                                dataField.name(),
                                dataField.type().accept(PaimonDataTypeToFlussDataType.INSTANCE))
                        .withComment(dataField.description());
            } catch (UnsupportedOperationException e) {
                throw new InvalidTableException(
                        String.format(
                                "Paimon table %s column '%s' uses unsupported type %s.",
                                tablePath, dataField.name(), dataField.type()),
                        e);
            }
        }

        TableDescriptor.Builder descriptorBuilder =
                TableDescriptor.builder()
                        .schema(schemaBuilder.build())
                        .partitionedBy(table.partitionKeys())
                        .properties(deriveAutoPartitionProperties(table))
                        .comment(table.comment().orElse(null));
        applyDistribution(tablePath, table, descriptorBuilder);
        return descriptorBuilder.build();
    }

    public static Schema toPaimonSchema(TableDescriptor tableDescriptor) {
        // validate paimon options first
        validatePaimonOptions(tableDescriptor.getProperties());
        validatePaimonOptions(tableDescriptor.getCustomProperties());

        Schema.Builder schemaBuilder = Schema.newBuilder();
        Options options = new Options();

        // set default properties
        setPaimonDefaultProperties(options);

        // When bucket key is undefined, it should use dynamic bucket (bucket = -1) mode.
        List<String> bucketKeys = tableDescriptor.getBucketKeys();
        if (!bucketKeys.isEmpty()) {
            int numBuckets =
                    tableDescriptor
                            .getTableDistribution()
                            .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Bucket count should be set."));
            options.set(CoreOptions.BUCKET, numBuckets);
            options.set(CoreOptions.BUCKET_KEY, String.join(",", bucketKeys));
        } else {
            options.set(CoreOptions.BUCKET, CoreOptions.BUCKET.defaultValue());
        }

        // set schema
        for (org.apache.fluss.metadata.Schema.Column column :
                tableDescriptor.getSchema().getColumns()) {
            String columnName = column.getName();
            if (LEGACY_SYSTEM_COLUMNS.containsKey(columnName)) {
                throw new InvalidTableException(
                        "Column "
                                + columnName
                                + " conflicts with a system column name of paimon table, please rename the column.");
            }
            schemaBuilder.column(
                    columnName,
                    column.getDataType().accept(FlussDataTypeToPaimonDataType.INSTANCE),
                    column.getComment().orElse(null));
        }

        // set pk
        if (tableDescriptor.hasPrimaryKey()) {
            schemaBuilder.primaryKey(
                    tableDescriptor.getSchema().getPrimaryKey().get().getColumnNames());
            options.set(
                    CoreOptions.CHANGELOG_PRODUCER.key(),
                    CoreOptions.ChangelogProducer.INPUT.toString());
        }

        // set partition keys
        schemaBuilder.partitionKeys(tableDescriptor.getPartitionKeys());

        // set properties to paimon schema
        tableDescriptor.getProperties().forEach((k, v) -> setFlussPropertyToPaimon(k, v, options));
        tableDescriptor
                .getCustomProperties()
                .forEach((k, v) -> setFlussPropertyToPaimon(k, v, options));

        // #4102: newly created lake tables are always clean (system columns are rejected above), so
        // a lake-enabled table must advertise its LakeStream state to Paimon.
        if (isDataLakeEnabled(tableDescriptor)) {
            options.set(LAKESTREAM_ENABLED_OPTION_KEY, Boolean.TRUE.toString());
        }

        schemaBuilder.options(options.toMap());

        // currently we only support string type, todo
        // consider to support other types
        if (options.get(CoreOptions.DELETION_VECTORS_ENABLED)) {
            org.apache.fluss.types.RowType rowType = tableDescriptor.getSchema().getRowType();
            Optional<String> invalidKey =
                    tableDescriptor.getPartitionKeys().stream()
                            .filter(
                                    key ->
                                            rowType.getField(key).getType().getTypeRoot()
                                                    != DataTypeRoot.STRING)
                            .findFirst();
            if (invalidKey.isPresent()) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Only support String type as partitioned key when 'deletion-vectors.enabled' is set to true for paimon, found '%s' is not String type.",
                                invalidKey.get()));
            }
        }

        // set comment
        tableDescriptor.getComment().ifPresent(schemaBuilder::comment);

        return schemaBuilder.build();
    }

    private static void validateAppendOnlyTable(TablePath tablePath, FileStoreTable table) {
        CoreOptions coreOptions = new CoreOptions(table.options());
        // TODO: Support rowkind.field for Paimon append tables. Preserve Paimon's
        // +I/+U append and -D/-U reject/filter rules (including ignore-delete and
        // ignore-update-before) before records enter the Fluss log. Adapt the
        // record and Arrow tiering paths, which can bypass TableWriteImpl's
        // row-kind checks, and test both paths before removing this rejection.
        if (coreOptions.rowkindField().isPresent()) {
            throw unsupportedAppendTableFeature(tablePath, CoreOptions.ROWKIND_FIELD.key());
        }
        if (coreOptions.dataEvolutionEnabled()) {
            throw unsupportedAppendTableFeature(
                    tablePath, CoreOptions.DATA_EVOLUTION_ENABLED.key());
        }
        if (coreOptions.rowTrackingEnabled()) {
            throw unsupportedAppendTableFeature(tablePath, CoreOptions.ROW_TRACKING_ENABLED.key());
        }
        if (coreOptions.deletionVectorsEnabled()) {
            throw unsupportedAppendTableFeature(
                    tablePath, CoreOptions.DELETION_VECTORS_ENABLED.key());
        }

        for (DataField field : table.schema().fields()) {
            if (field.defaultValue() != null) {
                throw new InvalidTableException(
                        String.format(
                                "Paimon table %s column '%s' has a default value, which cannot be "
                                        + "represented in a Fluss table.",
                                tablePath, field.name()));
            }
        }
    }

    private static InvalidTableException unsupportedAppendTableFeature(
            TablePath tablePath, String option) {
        return new InvalidTableException(
                String.format(
                        "Paimon table %s enables unsupported append-table feature '%s'.",
                        tablePath, option));
    }

    private static Map<String, String> deriveAutoPartitionProperties(FileStoreTable table) {
        if (table.partitionKeys().isEmpty()) {
            return Collections.emptyMap();
        }

        CoreOptions coreOptions = new CoreOptions(table.options());
        String paimonFormatter = coreOptions.partitionTimestampFormatter();
        String timeKey = inferTimePartitionKey(table.partitionKeys(), coreOptions);
        if (paimonFormatter == null || timeKey == null) {
            return Collections.emptyMap();
        }

        AutoPartitionTimeUnit timeUnit = inferTimeUnit(paimonFormatter, coreOptions);
        if (timeUnit == null) {
            return Collections.emptyMap();
        }

        DataType timeKeyType = table.rowType().getTypeAt(table.rowType().getFieldIndex(timeKey));
        // Fluss reads DATE partition values as yyyy-MM-dd (for example, 2025-10-13).
        // Auto partitioning with yyyyMMdd would generate 20251013, which cannot be read as DATE.
        // Keep other DATE formats as regular partitions; inferTimeUnit maps yyyy-MM-dd to DAY.
        // Other key types may use formats such as yyyyMMdd.
        if (timeKeyType.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.DATE
                && !"yyyy-MM-dd".equals(paimonFormatter)) {
            return Collections.emptyMap();
        }

        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), Boolean.TRUE.toString());
        properties.put(ConfigOptions.TABLE_AUTO_PARTITION_KEY.key(), timeKey);
        properties.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), paimonFormatter);
        properties.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), timeUnit.toString());
        properties.put(
                ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE.key(),
                coreOptions.sinkProcessTimeZone().getId());
        if (table.partitionKeys().size() > 1) {
            properties.put(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key(), "0");
        }
        return properties;
    }

    /**
     * Returns the partition key referenced by the Paimon timestamp pattern, or the first partition
     * key when no pattern is configured. Returns {@code null} when the configured pattern does not
     * reference exactly one partition key.
     */
    @Nullable
    private static String inferTimePartitionKey(
            List<String> partitionKeys, CoreOptions coreOptions) {
        String pattern = coreOptions.partitionTimestampPattern();
        if (pattern == null) {
            return partitionKeys.get(0);
        }
        for (String partitionKey : partitionKeys) {
            if (("$" + partitionKey).equals(pattern)) {
                return partitionKey;
            }
        }
        return null;
    }

    /**
     * Returns the single Fluss time unit compatible with the Paimon timestamp formatter, or {@code
     * null} when the formatter cannot be mapped unambiguously.
     */
    @Nullable
    private static AutoPartitionTimeUnit inferTimeUnit(
            String paimonFormatter, CoreOptions coreOptions) {
        List<AutoPartitionTimeUnit> compatibleUnits = new ArrayList<>();
        // Find every Fluss time unit that accepts the Paimon formatter. A unique match is required.
        for (AutoPartitionTimeUnit candidate : AutoPartitionTimeUnit.values()) {
            Map<String, String> options = new HashMap<>();
            options.put(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), Boolean.TRUE.toString());
            options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), paimonFormatter);
            options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), candidate.toString());
            options.put(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE.key(),
                    coreOptions.sinkProcessTimeZone().getId());
            try {
                PartitionUtils.validateTimeFormat(candidate, AutoPartitionStrategy.from(options));
                compatibleUnits.add(candidate);
            } catch (IllegalArgumentException ignored) {
                // The formatter is not compatible with this Fluss time unit.
            }
        }
        return compatibleUnits.size() == 1 ? compatibleUnits.get(0) : null;
    }

    private static void applyDistribution(
            TablePath tablePath, FileStoreTable table, TableDescriptor.Builder descriptorBuilder) {
        BucketMode bucketMode = table.bucketMode();
        BucketSpec bucketSpec = table.bucketSpec();
        if (bucketMode == BucketMode.HASH_FIXED) {
            CoreOptions coreOptions = new CoreOptions(table.options());
            if (coreOptions.bucketFunctionType() != CoreOptions.BucketFunctionType.DEFAULT) {
                throw new InvalidTableException(
                        String.format(
                                "Paimon table %s uses unsupported bucket function '%s'.",
                                tablePath, coreOptions.bucketFunctionType()));
            }
            descriptorBuilder.distributedBy(bucketSpec.getNumBuckets(), bucketSpec.getBucketKeys());
        } else if (bucketMode == BucketMode.BUCKET_UNAWARE) {
            descriptorBuilder.distributedBy(null, Collections.<String>emptyList());
        } else {
            throw new InvalidTableException(
                    String.format(
                            "Paimon table %s uses unsupported bucket mode '%s'.",
                            tablePath, bucketMode));
        }
    }

    private static boolean isDataLakeEnabled(TableDescriptor tableDescriptor) {
        return Boolean.parseBoolean(
                tableDescriptor.getProperties().get(TABLE_DATALAKE_ENABLED.key()));
    }

    /**
     * Maintains the {@code lakestream.enabled} Paimon option together with the {@code
     * table.datalake.enabled} lifecycle. Only new-layout (clean) tables are managed; legacy tables
     * that still carry the Fluss system columns are left untouched. Disabling removes the option
     * instead of persisting {@code false}.
     *
     * @param lakeStreamEnabled whether datalake acceleration is enabled after this change
     * @param legacyTable whether the Paimon table uses the legacy system-column layout
     * @param out the schema-change list to append to
     */
    private static void appendLakeStreamOptionChange(
            boolean lakeStreamEnabled, boolean legacyTable, List<SchemaChange> out) {
        // Old-layout tables are outside the scope of this option.
        if (legacyTable) {
            return;
        }
        if (lakeStreamEnabled) {
            out.add(SchemaChange.setOption(LAKESTREAM_ENABLED_OPTION_KEY, Boolean.TRUE.toString()));
        } else {
            // Disabling (SetOption "false") or resetting removes the option entirely.
            out.add(SchemaChange.removeOption(LAKESTREAM_ENABLED_OPTION_KEY));
        }
    }

    private static void validatePaimonOptions(Map<String, String> properties) {
        properties.forEach(
                (k, v) -> {
                    String paimonKey = k;
                    if (k.startsWith(PAIMON_CONF_PREFIX)) {
                        paimonKey = k.substring(PAIMON_CONF_PREFIX.length());
                    }
                    if (PAIMON_UNSETTABLE_OPTIONS.contains(paimonKey)) {
                        throw new InvalidConfigException(
                                String.format(
                                        "The Paimon option %s will be set automatically by Fluss "
                                                + "and should not be set manually.",
                                        k));
                    }
                });
    }

    private static void validateAlterPaimonOptions(String key) {
        if (PAIMON_UNSETTABLE_OPTIONS.contains(key)
                || CoreOptions.IMMUTABLE_OPTIONS.contains(key)) {
            throw new InvalidConfigException(
                    String.format("The Paimon option %s cannot be changed.", key));
        }
    }

    private static void setPaimonDefaultProperties(Options options) {
        // set partition.legacy-name to false, otherwise paimon will use toString for all types,
        // which will cause inconsistent partition value for the same binary value
        options.set(PARTITION_GENERATE_LEGACY_NAME_OPTION_KEY, Boolean.FALSE.toString());
    }

    private static void setFlussPropertyToPaimon(String key, String value, Options options) {
        if (key.startsWith(PAIMON_CONF_PREFIX)) {
            options.set(key.substring(PAIMON_CONF_PREFIX.length()), value);
        } else if (!key.startsWith(TABLE_DATALAKE_PAIMON_PREFIX)) {
            // This persisted prefix is an integration contract: the Flink lake table factory uses
            // the custom lake database and table-name options to resolve the physical identifier.
            options.set(FLUSS_CONF_PREFIX + key, value);
        }
    }

    private static String convertFlussPropertyKeyToPaimon(String key) {
        if (key.startsWith(PAIMON_CONF_PREFIX)) {
            return key.substring(PAIMON_CONF_PREFIX.length());
        } else {
            return FLUSS_CONF_PREFIX + key;
        }
    }
}
