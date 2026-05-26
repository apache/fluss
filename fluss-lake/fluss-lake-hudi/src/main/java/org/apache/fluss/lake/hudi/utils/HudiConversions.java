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

package org.apache.fluss.lake.hudi.utils;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.hudi.FlussDataTypeToHudiDataType;
import org.apache.fluss.lake.hudi.utils.catalog.HudiCatalogUtils;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.lake.hudi.HudiLakeCatalog.SYSTEM_COLUMNS;
import static org.apache.fluss.lake.hudi.utils.catalog.HudiCatalogUtils.HIVE_META_STORE_TYPE;

/** Utils for conversion between Hudi and Fluss. */
public class HudiConversions {

    private static final Logger LOG = LoggerFactory.getLogger(HudiConversions.class);

    // for fluss config
    private static final String FLUSS_CONF_PREFIX = "fluss.";
    // for hudi config
    private static final String HUDI_CONF_PREFIX = "hudi.";

    private static final String DELIMITER = ",";

    /** Hudi config options set by Fluss should not be set by users. */
    @VisibleForTesting public static final Set<String> HUDI_UNSETTABLE_OPTIONS = new HashSet<>();

    /**
     * Converts a Fluss TablePath to a Hudi ObjectPath.
     *
     * @param tablePath the Fluss table path
     * @return the corresponding Hudi ObjectPath
     */
    public static ObjectPath toHudiObjectPath(TablePath tablePath) {
        return new ObjectPath(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public static ResolvedSchema convertToFlinkResolvedSchema(
            TableDescriptor tableDescriptor, boolean isPkTable) {
        // validate hudi options first
        validateHudiOptions(tableDescriptor.getProperties());
        validateHudiOptions(tableDescriptor.getCustomProperties());

        List<Column> columns = new ArrayList<>();

        // Add regular columns
        for (org.apache.fluss.metadata.Schema.Column column :
                tableDescriptor.getSchema().getColumns()) {
            String columnName = column.getName();
            if (SYSTEM_COLUMNS.containsKey(columnName)) {
                throw new InvalidTableException(
                        "Column "
                                + columnName
                                + " conflicts with a system column name of hudi table, please rename the column.");
            }
            columns.add(
                    Column.physical(
                            columnName,
                            column.getDataType().accept(FlussDataTypeToHudiDataType.DFS_INSTANCE)));
        }

        // add system metadata columns to schema
        for (Map.Entry<String, DataType> systemColumn : SYSTEM_COLUMNS.entrySet()) {
            columns.add(Column.physical(systemColumn.getKey(), systemColumn.getValue()));
        }

        UniqueConstraint constraint = null;
        // Set primary key if this is a PK table
        if (isPkTable && tableDescriptor.hasPrimaryKey()) {
            List<String> primaryKeys = new ArrayList<>();
            for (int pkIndex : tableDescriptor.getSchema().getPrimaryKeyIndexes()) {
                primaryKeys.add(tableDescriptor.getSchema().getColumns().get(pkIndex).getName());
            }
            constraint = UniqueConstraint.primaryKey("primaryKey", primaryKeys);
        }

        return new ResolvedSchema(columns, Collections.emptyList(), constraint);
    }

    /**
     * Builds Hudi table properties from Fluss TableDescriptor.
     *
     * @param tableDescriptor the Fluss table descriptor
     * @param isPkTable whether this is a primary key table
     * @return map of Hudi table properties
     */
    public static Map<String, String> buildHudiTableProperties(
            TableDescriptor tableDescriptor, boolean isPkTable) {
        Map<String, String> hudiProperties = new HashMap<>();
        // Set connector type
        hudiProperties.put(FactoryUtil.CONNECTOR.key(), "hudi");
        hudiProperties.put("storageType", "hudi");

        // Set table type based on whether it's a PK table
        if (isPkTable) {
            hudiProperties.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
            List<String> primaryKeys = new ArrayList<>();
            for (int pkIndex : tableDescriptor.getSchema().getPrimaryKeyIndexes()) {
                primaryKeys.add(tableDescriptor.getSchema().getColumns().get(pkIndex).getName());
            }
            hudiProperties.put(FlinkOptions.RECORD_KEY_FIELD.key(), String.join(",", primaryKeys));
        } else {
            hudiProperties.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
            // set primary key for Fluss Log Table.
            String recordKeyField =
                    tableDescriptor
                            .getCustomProperties()
                            .get(HUDI_CONF_PREFIX + FlinkOptions.RECORD_KEY_FIELD.key());
            if (recordKeyField == null || recordKeyField.isEmpty()) {
                throw new IllegalArgumentException("Record key field should be set.");
            }
            hudiProperties.put(FlinkOptions.RECORD_KEY_FIELD.key(), recordKeyField);
            hudiProperties.put(
                    FlinkOptions.INDEX_KEY_FIELD.key(),
                    recordKeyField); // use primary key as index key
        }

        // buket keys column
        hudiProperties.put(FlinkOptions.INDEX_TYPE.key(), HoodieIndex.IndexType.BUCKET.name());
        List<String> bucketKeys = tableDescriptor.getBucketKeys();
        int numBuckets =
                tableDescriptor
                        .getTableDistribution()
                        .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                        .orElseThrow(
                                () -> new IllegalArgumentException("Bucket count should be set."));

        if (!bucketKeys.isEmpty()) {
            hudiProperties.put(
                    FlinkOptions.INDEX_KEY_FIELD.key(), String.join(DELIMITER, bucketKeys));
        }
        hudiProperties.put(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), String.valueOf(numBuckets));

        // partition keys column
        List<String> partitionKeys = tableDescriptor.getPartitionKeys();
        hudiProperties.put(
                FlinkOptions.PARTITION_PATH_FIELD.key(), String.join(DELIMITER, partitionKeys));

        // Convert Fluss properties to Hudi properties
        tableDescriptor
                .getProperties()
                .forEach((k, v) -> setFlussPropertyToHudi(k, v, hudiProperties));
        tableDescriptor
                .getCustomProperties()
                .forEach((k, v) -> setFlussPropertyToHudi(k, v, hudiProperties));

        return hudiProperties;
    }

    /**
     * Creates a CatalogTable for Hudi from Fluss TableDescriptor.
     *
     * @param tableDescriptor the Fluss table descriptor
     * @param isPkTable whether this is a primary key table
     * @return the created CatalogTable
     */
    public static CatalogTable createHudiCatalogTable(
            TableDescriptor tableDescriptor, boolean isPkTable, String catalogMode) {
        ResolvedSchema resolvedSchema = convertToFlinkResolvedSchema(tableDescriptor, isPkTable);
        Schema schema = Schema.newBuilder().fromResolvedSchema(resolvedSchema).build();
        List<String> partitionKeys = tableDescriptor.getPartitionKeys();
        Map<String, String> options = buildHudiTableProperties(tableDescriptor, isPkTable);
        LOG.info("Hudi table properties: {}", options);

        String comment = tableDescriptor.getComment().orElse("Hudi table created from Fluss");
        return HIVE_META_STORE_TYPE.equals(catalogMode)
                ? HudiCatalogUtils.createCatalogTable(schema, partitionKeys, options, comment)
                : HudiCatalogUtils.createResolvedCatalogTable(
                        schema, partitionKeys, options, comment, resolvedSchema);
    }

    private static void setFlussPropertyToHudi(
            String key, String value, Map<String, String> hudiProperties) {
        if (key.startsWith(HUDI_CONF_PREFIX)) {
            hudiProperties.put(key.substring(HUDI_CONF_PREFIX.length()), value);
        } else {
            hudiProperties.put(FLUSS_CONF_PREFIX + key, value);
        }
    }

    private static void validateHudiOptions(Map<String, String> properties) {
        properties.forEach(
                (k, v) -> {
                    String hudiKey = k;
                    if (k.startsWith(HUDI_CONF_PREFIX)) {
                        hudiKey = k.substring(HUDI_CONF_PREFIX.length());
                    }
                    if (HUDI_UNSETTABLE_OPTIONS.contains(hudiKey)) {
                        throw new InvalidConfigException(
                                String.format(
                                        "The Hudi option %s will be set automatically by Fluss "
                                                + "and should not be set manually.",
                                        k));
                    }
                });
    }
}
