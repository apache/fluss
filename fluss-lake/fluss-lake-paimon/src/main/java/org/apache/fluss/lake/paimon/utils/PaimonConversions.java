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

import org.apache.fluss.config.FlussConfigUtils;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.paimon.FlussDataTypeToPaimonDataType;
import org.apache.fluss.lake.paimon.PaimonDataTypeToFlussDataType;
import org.apache.fluss.lake.paimon.source.FlussRowAsPaimonRow;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.lake.paimon.PaimonLakeCatalog.SYSTEM_COLUMNS;

/** Utils for conversion between Paimon and Fluss. */
public class PaimonConversions {

    // for fluss config
    private static final String FLUSS_CONF_PREFIX = "fluss.";
    // for paimon config
    private static final String PAIMON_CONF_PREFIX = "paimon.";

    // Paimon options added by Paimon catalog, should ignore when convert to TableDescriptor
    private static final Set<String> IGNORE_PAIMON_OPTIONS;

    static {
        IGNORE_PAIMON_OPTIONS = new HashSet<>();
        IGNORE_PAIMON_OPTIONS.add(CoreOptions.BUCKET.key());
        IGNORE_PAIMON_OPTIONS.add(CoreOptions.BUCKET_KEY.key());
        IGNORE_PAIMON_OPTIONS.add(CoreOptions.PATH.key());
        // for pk tables
        IGNORE_PAIMON_OPTIONS.add(CoreOptions.CHANGELOG_PRODUCER.key());
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

    public static BinaryRow toPaimonPartitionBinaryRow(
            List<String> partitionKeys, @Nullable String partitionName) {
        if (partitionName == null || partitionKeys.isEmpty()) {
            return BinaryRow.EMPTY_ROW;
        }

        //  Fluss's existing utility
        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);

        BinaryRow partitionBinaryRow = new BinaryRow(partitionKeys.size());
        BinaryRowWriter writer = new BinaryRowWriter(partitionBinaryRow);

        List<String> partitionValues = resolvedPartitionSpec.getPartitionValues();
        for (int i = 0; i < partitionKeys.size(); i++) {
            // Todo Currently, partition column must be String datatype, so we can always use
            // `BinaryString.fromString` to convert to Paimon's data structure. Revisit here when
            // #489 is finished.
            writer.writeString(i, BinaryString.fromString(partitionValues.get(i)));
        }

        writer.complete();
        return partitionBinaryRow;
    }

    public static Object toPaimonLiteral(DataType dataType, Object flussLiteral) {
        RowType rowType = RowType.of(dataType);
        InternalRow flussRow = GenericRow.of(flussLiteral);
        FlussRowAsPaimonRow flussRowAsPaimonRow = new FlussRowAsPaimonRow(flussRow, rowType);
        return org.apache.paimon.data.InternalRow.createFieldGetter(dataType, 0)
                .getFieldOrNull(flussRowAsPaimonRow);
    }

    public static Schema toPaimonSchema(TableDescriptor tableDescriptor) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        Options options = new Options();

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
            if (SYSTEM_COLUMNS.containsKey(columnName)) {
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

        // add system metadata columns to schema
        for (Map.Entry<String, DataType> systemColumn : SYSTEM_COLUMNS.entrySet()) {
            schemaBuilder.column(systemColumn.getKey(), systemColumn.getValue());
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
        schemaBuilder.options(options.toMap());
        return schemaBuilder.build();
    }

    public static List<SchemaChange> toPaimonSchemaChanges(List<TableChange> tableChanges) {
        List<SchemaChange> schemaChanges = new ArrayList<>(tableChanges.size());

        for (TableChange tableChange : tableChanges) {
            if (tableChange instanceof TableChange.SetOption) {
                TableChange.SetOption setOption = (TableChange.SetOption) tableChange;
                schemaChanges.add(
                        SchemaChange.setOption(
                                getFlussPropertyKeyToPaimon(setOption.getKey()),
                                setOption.getValue()));
            } else if (tableChange instanceof TableChange.ResetOption) {
                TableChange.ResetOption resetOption = (TableChange.ResetOption) tableChange;
                schemaChanges.add(
                        SchemaChange.removeOption(
                                getFlussPropertyKeyToPaimon(resetOption.getKey())));
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported table change: " + tableChange.getClass());
            }
        }

        return schemaChanges;
    }

    public static TableDescriptor toFlussTableDescriptor(Table table) {
        FileStoreTable paimonTable = (FileStoreTable) table;
        Map<String, String> options = table.options();
        TableSchema paimonSchema = paimonTable.schema();

        TableDescriptor.Builder builder = TableDescriptor.builder();

        // extract bucket num and bucket keys
        int paimonBuckets = paimonSchema.numBuckets();
        if (paimonBuckets != -1) {
            builder.distributedBy(paimonBuckets, paimonSchema.bucketKeys());
        }
        // if paimonBuckets == -1, we keep TableDistribution as null here.
        // Because when create tables by Java api, we don't know the bucket num

        // build schema
        org.apache.fluss.metadata.Schema.Builder schemaBuilder =
                org.apache.fluss.metadata.Schema.newBuilder();
        for (DataField field : paimonSchema.fields()) {
            if (SYSTEM_COLUMNS.containsKey(field.name())) {
                continue;
            }
            schemaBuilder
                    .column(
                            field.name(),
                            field.type().accept(PaimonDataTypeToFlussDataType.INSTANCE))
                    .withComment(field.description());
        }

        // set pk
        if (!paimonSchema.primaryKeys().isEmpty()) {
            schemaBuilder.primaryKey(paimonSchema.primaryKeys());
        }
        builder.schema(schemaBuilder.build());

        // set partition keys
        builder.partitionedBy(paimonSchema.partitionKeys());

        // set properties to fluss table descriptor
        Map<String, String> properties = new HashMap<>();
        Map<String, String> customProperties = new HashMap<>();
        options.forEach(
                (k, v) -> {
                    if (!IGNORE_PAIMON_OPTIONS.contains(k)) {
                        String flussKey = getPaimonPropertyKeyToFluss(k);
                        if (FlussConfigUtils.isTableStorageConfig(flussKey)) {
                            properties.put(flussKey, v);
                        }
                        customProperties.put(flussKey, v);
                    }
                });
        builder.properties(properties);
        builder.customProperties(customProperties);

        return builder.build();
    }

    private static void setFlussPropertyToPaimon(String key, String value, Options options) {
        if (key.startsWith(PAIMON_CONF_PREFIX)) {
            options.set(key.substring(PAIMON_CONF_PREFIX.length()), value);
        } else {
            options.set(FLUSS_CONF_PREFIX + key, value);
        }
    }

    private static String getFlussPropertyKeyToPaimon(String key) {
        if (key.startsWith(PAIMON_CONF_PREFIX)) {
            return key.substring(PAIMON_CONF_PREFIX.length());
        } else {
            return FLUSS_CONF_PREFIX + key;
        }
    }

    private static String getPaimonPropertyKeyToFluss(String key) {
        if (key.startsWith(FLUSS_CONF_PREFIX)) {
            return key.substring(FLUSS_CONF_PREFIX.length());
        } else {
            return PAIMON_CONF_PREFIX + key;
        }
    }
}
