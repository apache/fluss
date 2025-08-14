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

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.FlussConfigUtils;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.config.Password;
import com.alibaba.fluss.flink.FlinkConnectorOptions;
import com.alibaba.fluss.flink.catalog.FlinkCatalogFactory;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.StringUtils;
import com.alibaba.fluss.utils.TimeUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.types.RowKind;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.fluss.flink.FlinkConnectorOptions.BUCKET_KEY;
import static com.alibaba.fluss.flink.FlinkConnectorOptions.BUCKET_NUMBER;
import static com.alibaba.fluss.flink.adapter.CatalogTableAdapter.toCatalogTable;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

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
    public static CatalogTable toFlinkTable(TableInfo tableInfo) {
        Map<String, String> newOptions = new HashMap<>(tableInfo.getCustomProperties().toMap());

        // put fluss table properties into flink options, to make the properties visible to users
        convertFlussTablePropertiesToFlinkOptions(tableInfo.getProperties().toMap(), newOptions);

        // put lake related options to table options
        Optional<DataLakeFormat> optDataLakeFormat = tableInfo.getTableConfig().getDataLakeFormat();
        if (optDataLakeFormat.isPresent()) {
            DataLakeFormat dataLakeFormat = optDataLakeFormat.get();
            String dataLakePrefix = "table.datalake." + dataLakeFormat + ".";

            for (Map.Entry<String, String> tableProperty :
                    tableInfo.getProperties().toMap().entrySet()) {
                if (tableProperty.getKey().startsWith(dataLakePrefix)) {
                    newOptions.put(tableProperty.getKey(), tableProperty.getValue());
                }
            }
        }

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
        return toCatalogTable(
                schemaBuilder.build(),
                tableInfo.getComment().orElse(null),
                tableInfo.getPartitionKeys(),
                CatalogPropertiesUtils.deserializeOptions(newOptions));
    }

    /** Convert Flink's table to Fluss's table. */
    public static TableDescriptor toFlussTable(ResolvedCatalogTable catalogTable) {
        Configuration flinkTableConf = Configuration.fromMap(catalogTable.getOptions());
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

        ResolvedSchema resolvedSchema = catalogTable.getResolvedSchema();

        // now, build Fluss's table
        Schema.Builder schemBuilder = Schema.newBuilder();
        if (resolvedSchema.getPrimaryKey().isPresent()) {
            schemBuilder.primaryKey(resolvedSchema.getPrimaryKey().get().getColumns());
        }

        // first build schema with physical columns
        Schema schema =
                schemBuilder
                        .fromColumns(
                                resolvedSchema.getColumns().stream()
                                        .filter(Column::isPhysical)
                                        .map(
                                                column ->
                                                        new Schema.Column(
                                                                column.getName(),
                                                                FlinkConversions.toFlussType(
                                                                        column.getDataType()),
                                                                column.getComment().orElse(null)))
                                        .collect(Collectors.toList()))
                        .build();
        resolvedSchema.getColumns().stream()
                .filter(col -> col instanceof Column.MetadataColumn)
                .findAny()
                .ifPresent(
                        (col) -> {
                            throw new CatalogException(
                                    "Metadata column " + col + " is not supported.");
                        });

        Map<String, String> customProperties = flinkTableConf.toMap();
        CatalogPropertiesUtils.serializeComputedColumns(
                customProperties, resolvedSchema.getColumns());
        CatalogPropertiesUtils.serializeWatermarkSpecs(
                customProperties, catalogTable.getResolvedSchema().getWatermarkSpecs());

        String comment = catalogTable.getComment();

        // convert some flink options to fluss table configs.
        Map<String, String> properties = convertFlinkOptionsToFlussTableProperties(flinkTableConf);

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
                                        bucketKeys.removeAll(catalogTable.getPartitionKeys());
                                        return bucketKeys;
                                    })
                            .orElse(Collections.emptyList());
        }
        Integer bucketNum = flinkTableConf.getOptional(BUCKET_NUMBER).orElse(null);

        return TableDescriptor.builder()
                .schema(schema)
                .partitionedBy(catalogTable.getPartitionKeys())
                .distributedBy(bucketNum, bucketKey)
                .comment(comment)
                .properties(properties)
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
        for (org.apache.flink.configuration.ConfigOption<?> option :
                FlinkConnectorOptions.TABLE_OPTIONS) {
            if (options.containsKey(option.key())) {
                properties.put(option.key(), options.getValue(option));
            }
        }
        return properties;
    }

    private static void convertFlussTablePropertiesToFlinkOptions(
            Map<String, String> flussProperties, Map<String, String> flinkOptions) {
        for (ConfigOption<?> option : FlussConfigUtils.TABLE_OPTIONS.values()) {
            if (flussProperties.containsKey(option.key())) {
                flinkOptions.put(option.key(), flussProperties.get(option.key()));
            }
        }
    }
}
