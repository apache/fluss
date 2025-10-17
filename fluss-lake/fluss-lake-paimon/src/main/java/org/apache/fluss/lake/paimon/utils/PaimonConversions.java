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
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.paimon.FlussDataTypeToPaimonDataType;
import org.apache.fluss.lake.paimon.source.FlussRowAsPaimonRow;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
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

    /** Paimon config options set by Fluss should not be set by users. */
    @VisibleForTesting public static final Set<String> PAIMON_UNSETTABLE_OPTIONS = new HashSet<>();

    static {
        PAIMON_UNSETTABLE_OPTIONS.add(CoreOptions.BUCKET.key());
        PAIMON_UNSETTABLE_OPTIONS.add(CoreOptions.BUCKET_KEY.key());
        PAIMON_UNSETTABLE_OPTIONS.add(CoreOptions.PARTITION_GENERATE_LEGCY_NAME.key());
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

    public static List<SchemaChange> toPaimonSchemaChanges(List<TableChange> tableChanges) {
        List<SchemaChange> schemaChanges = new ArrayList<>(tableChanges.size());

        for (TableChange tableChange : tableChanges) {
            if (tableChange instanceof TableChange.SetOption) {
                TableChange.SetOption setOption = (TableChange.SetOption) tableChange;
                schemaChanges.add(
                        SchemaChange.setOption(
                                convertFlussPropertyKeyToPaimon(setOption.getKey()),
                                setOption.getValue()));
            } else if (tableChange instanceof TableChange.ResetOption) {
                TableChange.ResetOption resetOption = (TableChange.ResetOption) tableChange;
                schemaChanges.add(
                        SchemaChange.removeOption(
                                convertFlussPropertyKeyToPaimon(resetOption.getKey())));
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported table change: " + tableChange.getClass());
            }
        }

        return schemaChanges;
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

    private static void setPaimonDefaultProperties(Options options) {
        // set partition.legacy-name to false, otherwise paimon will use toString for all types,
        // which will cause inconsistent partition value for the same binary value
        options.set(CoreOptions.PARTITION_GENERATE_LEGCY_NAME, false);
    }

    private static void setFlussPropertyToPaimon(String key, String value, Options options) {
        if (key.startsWith(PAIMON_CONF_PREFIX)) {
            options.set(key.substring(PAIMON_CONF_PREFIX.length()), value);
        } else {
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
