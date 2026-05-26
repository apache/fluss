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

package org.apache.fluss.lake.hudi;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.hudi.utils.HudiConversions;
import org.apache.fluss.lake.hudi.utils.catalog.CatalogDatabaseImpl;
import org.apache.fluss.lake.hudi.utils.catalog.HudiCatalogUtils;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.fluss.lake.hudi.utils.catalog.HudiCatalogUtils.HIVE_META_STORE_TYPE;
import static org.apache.fluss.lake.hudi.utils.catalog.HudiCatalogUtils.HUDI_CATALOG_DEFAULT_NAME;
import static org.apache.fluss.lake.hudi.utils.catalog.HudiCatalogUtils.MODE_CONFIG;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** Implementation of {@link LakeCatalog} for Hudi. */
public class HudiLakeCatalog implements LakeCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(HudiLakeCatalog.class);

    public static final LinkedHashMap<String, DataType> SYSTEM_COLUMNS = new LinkedHashMap<>();

    static {
        SYSTEM_COLUMNS.put(BUCKET_COLUMN_NAME, DataTypes.INT());
        SYSTEM_COLUMNS.put(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
        SYSTEM_COLUMNS.put(TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP(6));
    }

    private final Catalog hudiCatalog;
    private final String catalogMode;

    public HudiLakeCatalog(Configuration configuration) {
        this.catalogMode = configuration.toMap().getOrDefault(MODE_CONFIG, HIVE_META_STORE_TYPE);
        this.hudiCatalog = HudiCatalogUtils.createHudiCatalog(configuration);
        this.hudiCatalog.open();
    }

    @VisibleForTesting
    protected Catalog getHudiCatalog() {
        return hudiCatalog;
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor, Context context)
            throws org.apache.fluss.exception.TableAlreadyExistException {
        LOG.info("create the lake table for : {} with props: {}", tablePath, tableDescriptor);

        ObjectPath objectPath = HudiConversions.toHudiObjectPath(tablePath);

        boolean isPkTable = tableDescriptor.getSchema().getPrimaryKeyIndexes().length > 0;

        // Create Hudi catalog table
        CatalogTable catalogTable =
                HudiConversions.createHudiCatalogTable(tableDescriptor, isPkTable, catalogMode);

        // Create table in Hudi catalog
        try {
            createTable(objectPath, catalogTable);
        } catch (DatabaseNotExistException e) {
            createDatabase(tablePath.getDatabaseName());
            try {
                createTable(objectPath, catalogTable);
            } catch (DatabaseNotExistException t) {
                // shouldn't happen in normal cases
                throw new RuntimeException(
                        String.format(
                                "Fail to create table %s in Hudi, because "
                                        + "Database %s still doesn't exist although create database "
                                        + "successfully, please try again.",
                                tablePath, tablePath.getDatabaseName()));
            }
        }
    }

    @Override
    public void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
            throws org.apache.fluss.exception.TableNotExistException {
        throw new UnsupportedOperationException(
                "Alter table is not supported for Hudi at the moment");
    }

    private void createTable(ObjectPath tablePath, CatalogBaseTable catalogTable)
            throws DatabaseNotExistException {
        try {
            hudiCatalog.createTable(tablePath, catalogTable, true);
            LOG.info("Table {} created successfully.", tablePath);
        } catch (TableAlreadyExistException e) {
            throw new org.apache.fluss.exception.TableAlreadyExistException(
                    "Table " + tablePath + " already exists.");
        }
    }

    @Override
    public void close() {
        if (hudiCatalog != null && hudiCatalog instanceof AutoCloseable) {
            IOUtils.closeQuietly((AutoCloseable) hudiCatalog, HUDI_CATALOG_DEFAULT_NAME);
        }
    }

    public void createDatabase(String databaseName) {
        try {
            CatalogDatabase database = new CatalogDatabaseImpl(new HashMap<>(), "Hudi database");
            // ignore if exists
            hudiCatalog.createDatabase(databaseName, database, true);
        } catch (DatabaseAlreadyExistException e) {
            // do nothing, shouldn't throw since ignoreIfExists
        }
    }
}
