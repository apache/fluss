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
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.IOUtils;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

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
    private static final String ICEBERG_CATALOG_PREFIX = "iceberg.catalog.";

    public IcebergLakeCatalog(Configuration configuration) {
        // Extract Iceberg catalog properties from Fluss configuration
        Map<String, String> icebergProps =
                configuration.toMap().entrySet().stream()
                        .filter(e -> e.getKey().startsWith(ICEBERG_CATALOG_PREFIX))
                        .collect(
                                Collectors.toMap(
                                        e -> e.getKey().substring(ICEBERG_CATALOG_PREFIX.length()),
                                        Map.Entry::getValue));

        String catalogType = icebergProps.get("type");
        if (catalogType == null) {
            throw new IllegalArgumentException(
                    "Missing required Iceberg catalog type. Set 'iceberg.catalog.type' in your configuration (e.g., 'hive', 'hadoop', or 'rest').");
        }
        String catalogName = icebergProps.getOrDefault("name", "fluss-iceberg-catalog");

        this.icebergCatalog =
                org.apache.iceberg.CatalogUtil.loadCatalog(
                        catalogType,
                        catalogName,
                        icebergProps,
                        null // Optional: pass Hadoop configuration if available
                        );
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws TableAlreadyExistException {
        TableIdentifier icebergId = toIcebergIdentifier(tablePath);
        Schema icebergSchema = toIcebergSchema(tableDescriptor);
        PartitionSpec partitionSpec = createPartitionSpec(tableDescriptor, icebergSchema);

        Map<String, String> icebergProperties = new HashMap<>();
        tableDescriptor
                .getProperties()
                .forEach((k, v) -> setFlussPropertyToIceberg(k, v, icebergProperties));
        tableDescriptor
                .getCustomProperties()
                .forEach((k, v) -> setFlussPropertyToIceberg(k, v, icebergProperties));

        // Create namespace if it does not exist
        createDatabase(tablePath.getDatabaseName());

        try {
            createTable(icebergId, icebergSchema, partitionSpec, icebergProperties);
        } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
            throw new TableAlreadyExistException("Table " + tablePath + " already exists.");
        }
    }

    private void createTable(
            TableIdentifier icebergId,
            Schema icebergSchema,
            PartitionSpec partitionSpec,
            Map<String, String> icebergTableProperties)
            throws org.apache.iceberg.exceptions.NoSuchNamespaceException,
                    org.apache.iceberg.exceptions.AlreadyExistsException {

        icebergCatalog.createTable(icebergId, icebergSchema, partitionSpec, icebergTableProperties);
    }

    private TableIdentifier toIcebergIdentifier(TablePath tablePath) {
        return TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private Schema toIcebergSchema(TableDescriptor tableDescriptor) {
        throw new UnsupportedOperationException("Not implemented");
    }

    private Types convertFlussToIcebergType(DataTypes dataType) {
        throw new UnsupportedOperationException("Not implemented");
    }

    private PartitionSpec createPartitionSpec(
            TableDescriptor tableDescriptor, Schema icebergSchema) {
        throw new UnsupportedOperationException("Not implemented");
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
        if (icebergCatalog instanceof SupportsNamespaces) {
            SupportsNamespaces supportsNamespaces = (SupportsNamespaces) icebergCatalog;
            try {
                supportsNamespaces.createNamespace(Namespace.of(databaseName));
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
                // Namespace/database already exists, nothing to do
            }
        } else {
            throw new UnsupportedOperationException(
                    "The underlying Iceberg catalog does not support namespace operations.");
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly((AutoCloseable) icebergCatalog, "fluss-iceberg-catalog");
    }
}
