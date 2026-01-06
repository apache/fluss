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

package org.apache.fluss.flink.utils;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.Password;
import org.apache.fluss.flink.adapter.CatalogTableAdapter;
import org.apache.fluss.flink.catalog.FlinkCatalogFactory;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.StringUtils;
import org.apache.fluss.utils.TimeUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshHandler;
import org.apache.flink.table.catalog.TableChange.ModifyRefreshStatus;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.utils.EncodingUtils.decodeBase64ToBytes;
import static org.apache.flink.table.utils.EncodingUtils.encodeBytesToBase64;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.fluss.config.FlussConfigUtils.isTableStorageConfig;
import static org.apache.fluss.flink.FlinkConnectorOptions.AUTO_INCREMENT_FIELDS;
import static org.apache.fluss.flink.FlinkConnectorOptions.BUCKET_KEY;
import static org.apache.fluss.flink.FlinkConnectorOptions.BUCKET_NUMBER;
import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_DEFINITION_QUERY;
import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_INTERVAL_FRESHNESS;
import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT;
import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE;
import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_PREFIX;
import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES;
import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION;
import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_REFRESH_MODE;
import static org.apache.fluss.flink.FlinkConnectorOptions.MATERIALIZED_TABLE_REFRESH_STATUS;
import static org.apache.fluss.flink.adapter.CatalogTableAdapter.toCatalogTable;
import static org.apache.fluss.utils.PropertiesUtils.excludeByPrefix;

/** Utils for conversion between Flink and Fluss. */
public class FlinkConversions {

    private FlinkConversions() {}

    /** Convert Fluss's type to Flink's type. */
    @VisibleForTesting
    public static org.apache.flink.table.types.DataType toFlinkType(DataType flussDataType) {
        return flussDataType.accept(FlussTypeToFlinkType.INSTANCE);
    }

    /** Convert Fluss's RowType to Flink's RowType. */
    public static org.apache.flink.table.types.logical.RowType toFlinkRowType(
            RowType flussRowType) {
        return (org.apache.flink.table.types.logical.RowType)
                flussRowType.accept(FlussTypeToFlinkType.INSTANCE).getLogicalType();
    }

    /** Convert Flink's physical type to Fluss' type. */
    @VisibleForTesting
    public static DataType toFlussType(org.apache.flink.table.types.DataType flinkDataType) {
        return flinkDataType.getLogicalType().accept(FlinkTypeToFlussType.INSTANCE);
    }

    /** Convert Flink's RowType to Fluss' RowType. */
    public static RowType toFlussRowType(
            org.apache.flink.table.types.logical.RowType flinkRowType) {
        return (RowType) flinkRowType.accept(FlinkTypeToFlussType.INSTANCE);
    }

    /** Convert Fluss's table to Flink's table. */
    public static CatalogBaseTable toFlinkTable(TableInfo tableInfo) {
        Map<String, String> newOptions = new HashMap<>(tableInfo.getCustomProperties().toMap());

        // put fluss table properties into flink options, to make the properties visible to users
        convertFlussTablePropertiesToFlinkOptions(tableInfo.getProperties().toMap(), newOptions);

        org.apache.flink.table.api.Schema.Builder schemaBuilder =
                org.apache.flink.table.api.Schema.newBuilder();
        if (tableInfo.hasPrimaryKey()) {
            schemaBuilder.primaryKey(tableInfo.getPrimaryKeys());
        }

        Schema schema = tableInfo.getSchema();
        List<String> physicalColumns = schema.getColumnNames();
        int columnCount =
                physicalColumns.size()
                        + CatalogPropertiesUtils.nonPhysicalColumnsCount(
                                newOptions, physicalColumns);

        int physicalColumnIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            String optionalName = newOptions.get(CatalogPropertiesUtils.columnKey(i));
            if (optionalName == null) {
                // build physical column from table row field
                Schema.Column column = schema.getColumns().get(physicalColumnIndex++);
                schemaBuilder.column(
                        column.getName(), FlinkConversions.toFlinkType(column.getDataType()));
                if (column.getComment().isPresent()) {
                    schemaBuilder.withComment(column.getComment().get());
                }
            } else {
                // build non-physical column from options
                CatalogPropertiesUtils.deserializeComputedColumn(newOptions, i, schemaBuilder);
            }
        }

        // now, put distribution information to options
        newOptions.put(BUCKET_NUMBER.key(), String.valueOf(tableInfo.getNumBuckets()));
        if (!tableInfo.getBucketKeys().isEmpty()) {
            newOptions.put(BUCKET_KEY.key(), String.join(",", tableInfo.getBucketKeys()));
        }

        // deserialize watermark
        CatalogPropertiesUtils.deserializeWatermark(newOptions, schemaBuilder);

        // Check if this is a materialized table based on specific options
        if (isMaterializedTable(newOptions)) {
            return toFlinkMaterializedTable(
                    schemaBuilder.build(),
                    tableInfo.getComment().orElse(null),
                    tableInfo.getPartitionKeys(),
                    newOptions);
        }

        return toCatalogTable(
                schemaBuilder.build(),
                tableInfo.getComment().orElse(null),
                tableInfo.getPartitionKeys(),
                CatalogPropertiesUtils.deserializeOptions(newOptions));
    }

    /** Convert Flink's table to Fluss's table. */
    public static TableDescriptor toFlussTable(ResolvedCatalogBaseTable<?> catalogBaseTable) {
        Configuration flinkTableConf = Configuration.fromMap(catalogBaseTable.getOptions());
        String connector = flinkTableConf.get(CONNECTOR);
        if (!StringUtils.isNullOrWhitespaceOnly(connector)
                && !FlinkCatalogFactory.IDENTIFIER.equals(connector)) {
            throw new CatalogException(
                    "Fluss Catalog only supports fluss tables,"
                            + " but you specify  'connector'= '"
                            + connector
                            + "' when using Fluss Catalog\n"
                            + " You can create TEMPORARY table instead if you want to create the table of other connector.");
        }

        ResolvedSchema resolvedSchema = catalogBaseTable.getResolvedSchema();

        // now, build Fluss's table
        Schema.Builder schemBuilder = Schema.newBuilder();
        if (resolvedSchema.getPrimaryKey().isPresent()) {
            schemBuilder.primaryKey(resolvedSchema.getPrimaryKey().get().getColumns());
        }

        // first build schema with physical columns
        resolvedSchema.getColumns().stream()
                .filter(Column::isPhysical)
                .forEachOrdered(
                        column -> {
                            schemBuilder
                                    .column(
                                            column.getName(),
                                            FlinkConversions.toFlussType(column.getDataType()))
                                    .withComment(column.getComment().orElse(null));
                        });

        // Configure auto-increment columns based on the 'auto-increment.fields' option.
        if (flinkTableConf.containsKey(AUTO_INCREMENT_FIELDS.key())) {
            for (String autoIncrementColumn :
                    flinkTableConf.get(AUTO_INCREMENT_FIELDS).split(",")) {
                schemBuilder.enableAutoIncrement(autoIncrementColumn.trim());
            }
        }

        // convert some flink options to fluss table configs.
        Map<String, String> storageProperties =
                convertFlinkOptionsToFlussTableProperties(flinkTableConf);

        // serialize computed column and watermark spec to custom properties
        Map<String, String> customProperties =
                extractCustomProperties(flinkTableConf, storageProperties);
        CatalogPropertiesUtils.serializeComputedColumns(
                customProperties, resolvedSchema.getColumns());
        CatalogPropertiesUtils.serializeWatermarkSpecs(
                customProperties, catalogBaseTable.getResolvedSchema().getWatermarkSpecs());

        Schema schema = schemBuilder.build();

        resolvedSchema.getColumns().stream()
                .filter(col -> col instanceof Column.MetadataColumn)
                .findAny()
                .ifPresent(
                        (col) -> {
                            throw new CatalogException(
                                    "Metadata column " + col + " is not supported.");
                        });

        CatalogBaseTable.TableKind tableKind = catalogBaseTable.getTableKind();
        List<String> partitionKeys =
                CatalogBaseTable.TableKind.TABLE == tableKind
                        ? ((ResolvedCatalogTable) catalogBaseTable).getPartitionKeys()
                        : ((ResolvedCatalogMaterializedTable) catalogBaseTable).getPartitionKeys();

        // Set materialized table flags to fluss table custom properties
        if (CatalogTableAdapter.isMaterializedTable(tableKind)) {
            CatalogMaterializedTable.RefreshMode refreshMode =
                    ((ResolvedCatalogMaterializedTable) catalogBaseTable).getRefreshMode();
            if (refreshMode == CatalogMaterializedTable.RefreshMode.FULL) {
                throw new UnsupportedOperationException(
                        "Fluss currently supports only continuous refresh mode for materialized tables.");
            }
            serializeMaterializedTableToCustomProperties(
                    (CatalogMaterializedTable) catalogBaseTable, customProperties);
        }

        String comment = catalogBaseTable.getComment();

        // then set distributed by information
        List<String> bucketKey;
        if (flinkTableConf.containsKey(BUCKET_KEY.key())) {
            bucketKey =
                    Arrays.stream(flinkTableConf.get(BUCKET_KEY).split(","))
                            .map(String::trim)
                            .collect(Collectors.toList());
        } else {
            // use primary keys - partition keys
            bucketKey =
                    schema.getPrimaryKey()
                            .map(
                                    pk -> {
                                        List<String> bucketKeys =
                                                new ArrayList<>(pk.getColumnNames());
                                        bucketKeys.removeAll(partitionKeys);
                                        return bucketKeys;
                                    })
                            .orElse(Collections.emptyList());
        }
        Integer bucketNum = flinkTableConf.getOptional(BUCKET_NUMBER).orElse(null);

        return TableDescriptor.builder()
                .schema(schema)
                .partitionedBy(partitionKeys)
                .distributedBy(bucketNum, bucketKey)
                .comment(comment)
                .properties(storageProperties)
                .customProperties(customProperties)
                .build();
    }

    /** Convert Flink's table to Fluss's database. */
    public static DatabaseDescriptor toFlussDatabase(CatalogDatabase catalogDatabase) {
        return DatabaseDescriptor.builder()
                .comment(catalogDatabase.getComment())
                .customProperties(catalogDatabase.getProperties())
                .build();
    }

    /** Convert Fluss's ConfigOptions to Flink's ConfigOptions. */
    public static List<org.apache.flink.configuration.ConfigOption<?>> toFlinkOptions(
            Collection<ConfigOption<?>> flussOption) {
        return flussOption.stream()
                .map(FlinkConversions::toFlinkOption)
                .collect(Collectors.toList());
    }

    /** Convert Fluss's ConfigOption to Flink's ConfigOption. */
    @SuppressWarnings("unchecked")
    public static <T> org.apache.flink.configuration.ConfigOption<T> toFlinkOption(
            ConfigOption<T> flussOption) {
        org.apache.flink.configuration.ConfigOptions.OptionBuilder builder =
                org.apache.flink.configuration.ConfigOptions.key(flussOption.key());
        org.apache.flink.configuration.ConfigOption<?> option;
        Class<?> clazz = flussOption.getClazz();
        boolean isList = flussOption.isList();
        if (clazz.equals(String.class)) {
            if (!isList) {
                option = builder.stringType().defaultValue((String) flussOption.defaultValue());
            } else {
                // currently, we only support string type for list
                //noinspection unchecked
                String[] defaultValues =
                        ((List<String>) flussOption.defaultValue()).toArray(new String[0]);
                option = builder.stringType().asList().defaultValues(defaultValues);
            }
        } else if (clazz.equals(Integer.class)) {
            option = builder.intType().defaultValue((Integer) flussOption.defaultValue());
        } else if (clazz.equals(Long.class)) {
            option = builder.longType().defaultValue((Long) flussOption.defaultValue());
        } else if (clazz.equals(Boolean.class)) {
            option = builder.booleanType().defaultValue((Boolean) flussOption.defaultValue());
        } else if (clazz.equals(Float.class)) {
            option = builder.floatType().defaultValue((Float) flussOption.defaultValue());
        } else if (clazz.equals(Double.class)) {
            option = builder.doubleType().defaultValue((Double) flussOption.defaultValue());
        } else if (clazz.equals(Duration.class)) {
            // use string type in Flink option instead to make convert back easier
            option =
                    builder.stringType()
                            .defaultValue(
                                    TimeUtils.formatWithHighestUnit(
                                            (Duration) flussOption.defaultValue()));
        } else if (clazz.equals(Password.class)) {
            String defaultValue = ((Password) flussOption.defaultValue()).value();
            option = builder.stringType().defaultValue(defaultValue);
        } else if (clazz.equals(MemorySize.class)) {
            // use string type in Flink option instead to make convert back easier
            option = builder.stringType().defaultValue(flussOption.defaultValue().toString());
        } else if (clazz.isEnum()) {
            //noinspection unchecked
            option =
                    builder.enumType((Class<Enum>) clazz)
                            .defaultValue((Enum) flussOption.defaultValue());
        } else {
            throw new IllegalArgumentException("Unsupported type: " + clazz);
        }
        option.withDescription(flussOption.description());
        // TODO: support fallback keys in the future.
        return (org.apache.flink.configuration.ConfigOption<T>) option;
    }

    public static RowKind toFlinkRowKind(ChangeType changeType) {
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

    private static Map<String, String> convertFlinkOptionsToFlussTableProperties(
            Configuration options) {
        Map<String, String> properties = new HashMap<>();
        options.toMap()
                .forEach(
                        (k, v) -> {
                            if (isTableStorageConfig(k)) {
                                properties.put(k, v);
                            }
                        });
        return properties;
    }

    private static void convertFlussTablePropertiesToFlinkOptions(
            Map<String, String> flussProperties, Map<String, String> flinkOptions) {
        flussProperties.forEach(
                (k, v) -> {
                    if (isTableStorageConfig(k)) {
                        flinkOptions.put(k, v);
                    }
                });
    }

    public static List<TableChange> toFlussTableChanges(
            org.apache.flink.table.catalog.TableChange tableChange) {
        if (tableChange instanceof org.apache.flink.table.catalog.TableChange.SetOption) {
            return Collections.singletonList(
                    convertSetOption(
                            (org.apache.flink.table.catalog.TableChange.SetOption) tableChange));
        } else if (tableChange instanceof org.apache.flink.table.catalog.TableChange.AddColumn) {
            org.apache.flink.table.catalog.TableChange.AddColumn addColumn =
                    (org.apache.flink.table.catalog.TableChange.AddColumn) tableChange;
            Column column = addColumn.getColumn();
            return Collections.singletonList(
                    TableChange.addColumn(
                            column.getName(),
                            toFlussType(column.getDataType()),
                            column.getComment().orElse(null),
                            toFlussColumnPosition(addColumn.getPosition())));
        } else if (tableChange instanceof org.apache.flink.table.catalog.TableChange.DropColumn) {
            return Collections.singletonList(
                    TableChange.dropColumn(
                            ((org.apache.flink.table.catalog.TableChange.DropColumn) tableChange)
                                    .getColumnName()));
        } else if (tableChange
                instanceof org.apache.flink.table.catalog.TableChange.ModifyColumnName) {
            org.apache.flink.table.catalog.TableChange.ModifyColumnName renameColumn =
                    (org.apache.flink.table.catalog.TableChange.ModifyColumnName) tableChange;
            return Collections.singletonList(
                    TableChange.renameColumn(
                            renameColumn.getOldColumn().getName(),
                            renameColumn.getNewColumnName()));
        } else if (tableChange instanceof org.apache.flink.table.catalog.TableChange.ModifyColumn) {
            org.apache.flink.table.catalog.TableChange.ModifyColumn modifyColumn =
                    (org.apache.flink.table.catalog.TableChange.ModifyColumn) tableChange;
            checkState(
                    Objects.equals(
                            modifyColumn.getNewColumn().getName(),
                            modifyColumn.getOldColumn().getName()),
                    "Can only modify columns with the same name.");
            Column newColumn = modifyColumn.getNewColumn();
            return Collections.singletonList(
                    TableChange.modifyColumn(
                            newColumn.getName(),
                            toFlussType(newColumn.getDataType()),
                            newColumn.getComment().orElse(null),
                            toFlussColumnPosition(modifyColumn.getNewPosition())));
        } else if (tableChange instanceof org.apache.flink.table.catalog.TableChange.ResetOption) {
            return Collections.singletonList(
                    convertResetOption(
                            (org.apache.flink.table.catalog.TableChange.ResetOption) tableChange));
        } else if (tableChange instanceof ModifyRefreshStatus
                || tableChange instanceof ModifyRefreshHandler) {
            // MaterializedTableChange may produce multiple fluss TableChange.
            return convertMaterializedTableChange(tableChange);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported flink table change: %s.", tableChange));
        }
    }

    private static TableChange.ColumnPosition toFlussColumnPosition(
            org.apache.flink.table.catalog.TableChange.ColumnPosition columnPosition) {
        if (columnPosition == null) {
            return TableChange.ColumnPosition.last();
        } else if (columnPosition instanceof org.apache.flink.table.catalog.TableChange.First) {
            return TableChange.ColumnPosition.first();
        } else if (columnPosition instanceof org.apache.flink.table.catalog.TableChange.After) {
            return TableChange.ColumnPosition.after(
                    ((org.apache.flink.table.catalog.TableChange.After) columnPosition).column());
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported column position: %s.", columnPosition));
        }
    }

    private static TableChange.SetOption convertSetOption(
            org.apache.flink.table.catalog.TableChange.SetOption flinkSetOption) {
        return TableChange.set(flinkSetOption.getKey(), flinkSetOption.getValue());
    }

    private static TableChange.ResetOption convertResetOption(
            org.apache.flink.table.catalog.TableChange.ResetOption flinkResetOption) {
        return TableChange.reset(flinkResetOption.getKey());
    }

    /** Converts a {@code MaterializedTableChange} to a list of Fluss TableChanges. */
    private static List<TableChange> convertMaterializedTableChange(
            org.apache.flink.table.catalog.TableChange materializedTableChange) {
        List<TableChange> flussTableChanges = new ArrayList<>();
        // Handle different types of MaterializedTableChange
        if (materializedTableChange instanceof ModifyRefreshStatus) {
            convertModifyRefreshStatus(
                    (ModifyRefreshStatus) materializedTableChange, flussTableChanges);
        } else if (materializedTableChange instanceof ModifyRefreshHandler) {
            convertModifyRefreshHandler(
                    (ModifyRefreshHandler) materializedTableChange, flussTableChanges);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported flink materialized table change: %s",
                            materializedTableChange));
        }

        return flussTableChanges;
    }

    /** Handles {@code ModifyRefreshStatus} change. */
    private static void convertModifyRefreshStatus(
            ModifyRefreshStatus modifyRefreshStatus, List<TableChange> flussTableChanges) {
        CatalogMaterializedTable.RefreshStatus newRefreshStatus =
                modifyRefreshStatus.getRefreshStatus();
        flussTableChanges.add(
                TableChange.set(MATERIALIZED_TABLE_REFRESH_STATUS.key(), newRefreshStatus.name()));
    }

    /**
     * Handles {@code ModifyRefreshHandler} change. This change produces two TableChanges: one for
     * description and one for bytes.
     */
    private static void convertModifyRefreshHandler(
            ModifyRefreshHandler modifyRefreshHandler, List<TableChange> flussTableChanges) {
        String newHandlerDesc = modifyRefreshHandler.getRefreshHandlerDesc();
        byte[] newHandlerBytes = modifyRefreshHandler.getRefreshHandlerBytes();
        flussTableChanges.add(
                TableChange.set(
                        MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION.key(), newHandlerDesc));
        flussTableChanges.add(
                TableChange.set(
                        MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES.key(),
                        encodeBytesToBase64(newHandlerBytes)));
    }

    /**
     * Determines if the given options represent a materialized table.
     *
     * @param options the table options to check
     * @return true if this is a materialized table, false otherwise
     */
    private static boolean isMaterializedTable(Map<String, String> options) {
        return options.keySet().stream().anyMatch(key -> key.startsWith(MATERIALIZED_TABLE_PREFIX));
    }

    private static void serializeMaterializedTableToCustomProperties(
            CatalogMaterializedTable mt, Map<String, String> customProperties) {
        // Serialize core materialized table properties
        customProperties.put(MATERIALIZED_TABLE_DEFINITION_QUERY.key(), mt.getDefinitionQuery());
        // Serialize freshness configuration
        IntervalFreshness freshness = mt.getDefinitionFreshness();
        customProperties.put(MATERIALIZED_TABLE_INTERVAL_FRESHNESS.key(), freshness.getInterval());
        customProperties.put(
                MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT.key(),
                freshness.getTimeUnit().name());
        // Serialize refresh configuration
        customProperties.put(
                MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE.key(), mt.getLogicalRefreshMode().name());
        customProperties.put(MATERIALIZED_TABLE_REFRESH_MODE.key(), mt.getRefreshMode().name());
        customProperties.put(MATERIALIZED_TABLE_REFRESH_STATUS.key(), mt.getRefreshStatus().name());
        // Serialize optional refresh handler information
        serializeRefreshHandler(mt, customProperties);
    }

    /**
     * Serializes the refresh handler information if present.
     *
     * @param mt the materialized table
     * @param customProperties the properties map to update
     */
    private static void serializeRefreshHandler(
            CatalogMaterializedTable mt, Map<String, String> customProperties) {
        // Serialize refresh handler description if present
        mt.getRefreshHandlerDescription()
                .ifPresent(
                        desc ->
                                customProperties.put(
                                        MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION.key(),
                                        desc));
        // Serialize refresh handler bytes if present
        byte[] serializedRefreshHandler = mt.getSerializedRefreshHandler();
        if (serializedRefreshHandler != null) {
            customProperties.put(
                    MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES.key(),
                    encodeBytesToBase64(serializedRefreshHandler));
        }
    }

    private static CatalogMaterializedTable toFlinkMaterializedTable(
            org.apache.flink.table.api.Schema schema,
            String comment,
            List<String> partitionKeys,
            Map<String, String> options) {
        // Validate required materialized table options first
        String definitionQuery = options.get(MATERIALIZED_TABLE_DEFINITION_QUERY.key());
        String intervalFreshness = options.get(MATERIALIZED_TABLE_INTERVAL_FRESHNESS.key());
        String timeUnitStr = options.get(MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT.key());
        String logicalRefreshModeStr = options.get(MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE.key());
        String refreshModeStr = options.get(MATERIALIZED_TABLE_REFRESH_MODE.key());
        String refreshStatusStr = options.get(MATERIALIZED_TABLE_REFRESH_STATUS.key());

        // Validate required materialized table options first
        checkNotNull(
                definitionQuery, "Materialized table definition query is required but missing");
        checkNotNull(
                intervalFreshness, "Materialized table interval freshness is required but missing");
        checkNotNull(
                timeUnitStr,
                "Materialized table interval freshness time unit is required but missing");
        checkNotNull(
                logicalRefreshModeStr,
                "Materialized table logical refresh mode is required but missing");
        checkNotNull(refreshModeStr, "Materialized table refresh mode is required but missing");
        checkNotNull(refreshStatusStr, "Materialized table refresh status is required but missing");

        // Parse validated values
        IntervalFreshness.TimeUnit timeUnit = IntervalFreshness.TimeUnit.valueOf(timeUnitStr);
        IntervalFreshness freshness = IntervalFreshness.of(intervalFreshness, timeUnit);
        CatalogMaterializedTable.LogicalRefreshMode logicalRefreshMode =
                CatalogMaterializedTable.LogicalRefreshMode.valueOf(logicalRefreshModeStr);
        CatalogMaterializedTable.RefreshMode refreshMode =
                CatalogMaterializedTable.RefreshMode.valueOf(refreshModeStr);
        CatalogMaterializedTable.RefreshStatus refreshStatus =
                CatalogMaterializedTable.RefreshStatus.valueOf(refreshStatusStr);

        @Nullable
        String refreshHandlerDesc =
                options.get(MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION.key());
        @Nullable
        String refreshHandlerStringBytes =
                options.get(MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES.key());
        @Nullable
        byte[] refreshHandlerBytes =
                StringUtils.isNullOrWhitespaceOnly(refreshHandlerStringBytes)
                        ? null
                        : decodeBase64ToBytes(refreshHandlerStringBytes);

        CatalogMaterializedTable.Builder builder = CatalogMaterializedTable.newBuilder();
        builder.schema(schema)
                .comment(comment)
                .partitionKeys(partitionKeys)
                .options(excludeByPrefix(options, MATERIALIZED_TABLE_PREFIX))
                .definitionQuery(definitionQuery)
                .freshness(freshness)
                .logicalRefreshMode(logicalRefreshMode)
                .refreshMode(refreshMode)
                .refreshStatus(refreshStatus)
                .refreshHandlerDescription(refreshHandlerDesc)
                .serializedRefreshHandler(refreshHandlerBytes);
        return builder.build();
    }

    private static Map<String, String> extractCustomProperties(
            Configuration allProperties, Map<String, String> flussTableProperties) {
        Map<String, String> customProperties = new HashMap<>(allProperties.toMap());
        customProperties.keySet().removeAll(flussTableProperties.keySet());
        return customProperties;
    }
}
