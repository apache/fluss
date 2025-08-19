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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.lake.iceberg.conf.IcebergConfiguration;
import com.alibaba.fluss.lake.lakestorage.LakeCatalog;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.IOUtils;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;

/** An Iceberg implementation of {@link LakeCatalog}. */
public class IcebergLakeCatalog implements LakeCatalog {

    public static final String ICEBERG_CATALOG_DEFAULT_NAME = "fluss-iceberg-catalog";

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

    @VisibleForTesting
    protected Catalog getIcebergCatalog() {
        return icebergCatalog;
    }

    private Catalog createIcebergCatalog(Configuration configuration) {
        Map<String, String> icebergProps = configuration.toMap();
        String catalogName = icebergProps.getOrDefault("name", ICEBERG_CATALOG_DEFAULT_NAME);
        return buildIcebergCatalog(
                catalogName, icebergProps, IcebergConfiguration.from(configuration).get());
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws TableAlreadyExistException {
        // convert Fluss table path to iceberg table
        boolean isPkTable = tableDescriptor.hasPrimaryKey();
        TableIdentifier icebergId = toIcebergTableIdentifier(tablePath);
        Schema icebergSchema = convertToIcebergSchema(tableDescriptor, isPkTable);
        Catalog.TableBuilder tableBuilder = icebergCatalog.buildTable(icebergId, icebergSchema);

        PartitionSpec partitionSpec =
                createPartitionSpec(tableDescriptor, icebergSchema, isPkTable);
        SortOrder sortOrder = createSortOrder(icebergSchema);
        tableBuilder.withProperties(buildTableProperties(tableDescriptor, isPkTable));
        tableBuilder.withPartitionSpec(partitionSpec);
        tableBuilder.withSortOrder(sortOrder);
        try {
            createTable(tablePath, tableBuilder);
        } catch (NoSuchNamespaceException e) {
            createDatabase(tablePath.getDatabaseName());
            try {
                createTable(tablePath, tableBuilder);
            } catch (NoSuchNamespaceException t) {
                // shouldn't happen in normal cases
                throw new RuntimeException(
                        String.format(
                                "Fail to create table %s in Iceberg, because "
                                        + "Namespace %s still doesn't exist although create namespace "
                                        + "successfully, please try again.",
                                tablePath, tablePath.getDatabaseName()));
            }
        }
    }

    private TableIdentifier toIcebergTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private void createTable(TablePath tablePath, Catalog.TableBuilder tableBuilder) {
        try {
            tableBuilder.create();
        } catch (AlreadyExistsException e) {
            throw new TableAlreadyExistException("Table " + tablePath + " already exists.");
        }
    }

    public Schema convertToIcebergSchema(TableDescriptor tableDescriptor, boolean isPkTable) {
        List<Types.NestedField> fields = new ArrayList<>();
        int fieldId = 0;

        // general columns
        for (com.alibaba.fluss.metadata.Schema.Column column :
                tableDescriptor.getSchema().getColumns()) {
            String colName = column.getName();
            if (SYSTEM_COLUMNS.containsKey(colName)) {
                throw new IllegalArgumentException(
                        "Column '" + colName + "' conflicts with a reserved system column name.");
            }
            Types.NestedField field;
            if (column.getDataType().isNullable()) {
                field =
                        Types.NestedField.optional(
                                fieldId++,
                                colName,
                                column.getDataType()
                                        .accept(FlussDataTypeToIcebergDataType.INSTANCE),
                                column.getComment().orElse(null));
            } else {
                field =
                        Types.NestedField.required(
                                fieldId++,
                                colName,
                                column.getDataType()
                                        .accept(FlussDataTypeToIcebergDataType.INSTANCE),
                                column.getComment().orElse(null));
            }
            fields.add(field);
        }

        // system columns
        for (Map.Entry<String, Type> systemColumn : SYSTEM_COLUMNS.entrySet()) {
            fields.add(
                    Types.NestedField.required(
                            fieldId++, systemColumn.getKey(), systemColumn.getValue()));
        }

        if (isPkTable) {
            // set identifier fields
            int[] primaryKeyIndexes = tableDescriptor.getSchema().getPrimaryKeyIndexes();
            Set<Integer> identifierFieldIds = new HashSet<>();
            for (int pkIdx : primaryKeyIndexes) {
                identifierFieldIds.add(fields.get(pkIdx).fieldId());
            }
            return new Schema(fields, identifierFieldIds);
        } else {
            return new Schema(fields);
        }
    }

    private PartitionSpec createPartitionSpec(
            TableDescriptor tableDescriptor, Schema icebergSchema, boolean isPkTable) {
        List<String> bucketKeys = tableDescriptor.getBucketKeys();
        int bucketCount =
                tableDescriptor
                        .getTableDistribution()
                        .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Bucket count (bucket.num) must be set"));

        // Only support one bucket key for now
        if (bucketKeys.size() > 1) {
            throw new UnsupportedOperationException(
                    "Only one bucket key is supported for Iceberg at the moment");
        }

        // pk table must have bucket key
        if (bucketKeys.isEmpty() && isPkTable) {
            throw new IllegalArgumentException(
                    "Bucket key must be set for primary key Iceberg tables");
        }

        PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
        List<String> partitionKeys = tableDescriptor.getPartitionKeys();
        // always set identity partition with partition key
        for (String partitionKey : partitionKeys) {
            builder.identity(partitionKey);
        }

        if (isPkTable) {
            builder.bucket(bucketKeys.get(0), bucketCount);
        } else {
            // if there is no bucket keys, use identity(__bucket)
            if (bucketKeys.isEmpty()) {
                builder.identity(BUCKET_COLUMN_NAME);
            } else {
                builder.bucket(bucketKeys.get(0), bucketCount);
            }
        }

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

    private void createDatabase(String databaseName) {
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

    private Map<String, String> buildTableProperties(
            TableDescriptor tableDescriptor, boolean isPkTable) {
        Map<String, String> icebergProperties = new HashMap<>();

        if (isPkTable) {
            // MOR table properties for streaming workloads
            icebergProperties.put("write.delete.mode", "merge-on-read");
            icebergProperties.put("write.update.mode", "merge-on-read");
            icebergProperties.put("write.merge.mode", "merge-on-read");
        }

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
