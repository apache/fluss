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

package com.alibaba.fluss.lake.iceberg;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.lake.lakestorage.LakeCatalog;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.utils.IOUtils;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.iceberg.CatalogUtil.loadCatalog;

/** A Iceberg implementation of {@link LakeCatalog}. */
public class IcebergLakeCatalog implements LakeCatalog {

    private static final LinkedHashMap<String, Type> SYSTEM_COLUMNS = new LinkedHashMap<>();

    static {
        // We need __bucket system column to filter out the given bucket
        // for iceberg bucket append only table & primary key table.
        SYSTEM_COLUMNS.put(BUCKET_COLUMN_NAME, Types.IntegerType.get());
        SYSTEM_COLUMNS.put(OFFSET_COLUMN_NAME, Types.LongType.get());
        SYSTEM_COLUMNS.put(TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone());
    }

    private final Catalog icebergCatalog;

    // for fluss config
    private static final String FLUSS_CONF_PREFIX = "fluss.";
    // for iceberg config
    private static final String ICEBERG_CONF_PREFIX = "iceberg.";

    public IcebergLakeCatalog(Configuration configuration) {
        this.icebergCatalog = createIcebergCatalog(configuration);
    }

    private Catalog createIcebergCatalog(Configuration configuration) {
        Map<String, String> icebergProps = configuration.toMap();

        String catalogType = icebergProps.get("type");
        if (catalogType == null) {
            throw new IllegalArgumentException(
                    "Missing required Iceberg catalog type. Set 'iceberg.catalog.type' in your configuration (e.g., 'hive', 'hadoop', or 'rest').");
        }

        String catalogName = icebergProps.getOrDefault("name", "fluss-iceberg-catalog");

        return loadCatalog(
                catalogType,
                catalogName,
                icebergProps,
                null // Optional: pass Hadoop configuration if available
                );
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws TableAlreadyExistException {
        try {

            if (!tableDescriptor.hasPrimaryKey()) {
                throw new UnsupportedOperationException(
                        "Iceberg integration currently supports only primary key tables.");
            }

            TableIdentifier icebergId = toIcebergTableIdentifier(tablePath);

            createDatabaseIfAbsent(tablePath.getDatabaseName());

            Schema icebergSchema = convertToIcebergSchema(tableDescriptor);
            PartitionSpec partitionSpec = createPartitionSpec(tableDescriptor, icebergSchema);
            SortOrder sortOrder = createSortOrder(icebergSchema);

            // table builder for complete configuration
            Catalog.TableBuilder tableBuilder = icebergCatalog.buildTable(icebergId, icebergSchema);
            tableBuilder.withProperties(buildTableProperties(tableDescriptor));
            tableBuilder.withPartitionSpec(partitionSpec);
            tableBuilder.withSortOrder(sortOrder);
            tableBuilder.create();

        } catch (AlreadyExistsException e) {
            throw new TableAlreadyExistException(tablePath.getTableName(), e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Iceberg table: " + tablePath, e);
        }
    }

    private TableIdentifier toIcebergTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private Schema convertToIcebergSchema(TableDescriptor tableDescriptor) {
        List<Types.NestedField> fields = new ArrayList<>();
        int fieldId = 1;

        for (com.alibaba.fluss.metadata.Schema.Column column :
                tableDescriptor.getSchema().getColumns()) {
            String colName = column.getName();
            if (SYSTEM_COLUMNS.containsKey(colName)) {
                throw new IllegalArgumentException(
                        "Column '" + colName + "' conflicts with a reserved system column name.");
            }
            fields.add(
                    Types.NestedField.optional(
                            fieldId++, colName, convertFlussToIcebergType(column.getDataType())));
        }

        for (Map.Entry<String, Type> systemColumn : SYSTEM_COLUMNS.entrySet()) {
            fields.add(
                    Types.NestedField.optional(
                            fieldId++, systemColumn.getKey(), systemColumn.getValue()));
        }

        return new Schema(fields);
    }

    private Type convertFlussToIcebergType(DataType dataType) {
        DataTypeRoot typeRoot = dataType.getTypeRoot();

        switch (typeRoot) {
            case INTEGER:
                return Types.IntegerType.get();
            case BIGINT:
                return Types.LongType.get();
            case STRING:
                return Types.StringType.get();
            case BOOLEAN:
                return Types.BooleanType.get();
            case DOUBLE:
                return Types.DoubleType.get();
            case FLOAT:
                return Types.FloatType.get();
            case DATE:
                return Types.DateType.get();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Types.TimestampType.withZone();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Types.TimestampType.withoutZone();
            default:
                throw new UnsupportedOperationException("Unsupported Fluss data type: " + typeRoot);
        }
    }

    private PartitionSpec createPartitionSpec(
            TableDescriptor tableDescriptor, Schema icebergSchema) {

        // Only PK tables supported for now
        List<String> bucketKeys = tableDescriptor.getBucketKeys();
        int bucketCount =
                tableDescriptor
                        .getTableDistribution()
                        .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Bucket count (bucket.num) must be set"));

        if (bucketKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    "Bucket key must be set for primary key Iceberg tables");
        }
        if (bucketKeys.size() > 1) {
            throw new UnsupportedOperationException(
                    "Only one bucket key is supported for Iceberg at the moment");
        }

        PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
        List<String> partitionKeys = tableDescriptor.getPartitionKeys();
        for (String partitionKey : partitionKeys) {
            builder.identity(partitionKey);
        }
        builder.bucket(bucketKeys.get(0), bucketCount);

        return builder.build();
    }

    private void setFlussPropertyToIceberg(
            String key, String value, Map<String, String> icebergProperties) {
        if (key.startsWith(ICEBERG_CONF_PREFIX)) {
            icebergProperties.put(key.substring(ICEBERG_CONF_PREFIX.length()), value);
        } else {
            icebergProperties.put(FLUSS_CONF_PREFIX + key, value);
        }
    }

    private void createDatabaseIfAbsent(String databaseName) {
        Namespace namespace = Namespace.of(databaseName);
        if (icebergCatalog instanceof SupportsNamespaces) {
            SupportsNamespaces supportsNamespaces = (SupportsNamespaces) icebergCatalog;
            if (!supportsNamespaces.namespaceExists(namespace)) {
                supportsNamespaces.createNamespace(namespace);
            }
        } else {
            throw new UnsupportedOperationException(
                    "The underlying Iceberg catalog does not support namespace operations.");
        }
    }

    private SortOrder createSortOrder(Schema icebergSchema) {
        // Sort by __offset system column for deterministic ordering
        SortOrder.Builder builder = SortOrder.builderFor(icebergSchema);
        builder.asc(OFFSET_COLUMN_NAME);
        return builder.build();
    }

    private Map<String, String> buildTableProperties(TableDescriptor tableDescriptor) {
        Map<String, String> icebergProperties = new HashMap<>();

        // MOR table properties for streaming workloads
        icebergProperties.put("write.delete.mode", "merge-on-read");
        icebergProperties.put("write.update.mode", "merge-on-read");
        icebergProperties.put("write.merge.mode", "merge-on-read");

        // Add Fluss-specific properties with prefix
        tableDescriptor
                .getProperties()
                .forEach((k, v) -> setFlussPropertyToIceberg(k, v, icebergProperties));
        tableDescriptor
                .getCustomProperties()
                .forEach((k, v) -> setFlussPropertyToIceberg(k, v, icebergProperties));

        return icebergProperties;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly((AutoCloseable) icebergCatalog, "fluss-iceberg-catalog");
    }
}
