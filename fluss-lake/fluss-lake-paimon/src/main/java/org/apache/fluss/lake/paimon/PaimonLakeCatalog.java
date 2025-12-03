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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimonSchema;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimonSchemaChanges;
import static org.apache.fluss.lake.paimon.utils.PaimonTableValidation.checkTableIsEmpty;
import static org.apache.fluss.lake.paimon.utils.PaimonTableValidation.validatePaimonSchemaCompatible;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** A Paimon implementation of {@link LakeCatalog}. */
public class PaimonLakeCatalog implements LakeCatalog {

    public static final LinkedHashMap<String, DataType> SYSTEM_COLUMNS = new LinkedHashMap<>();

    static {
        // We need __bucket system column to filter out the given bucket
        // for paimon bucket-unaware append only table.
        // It's not required for paimon bucket-aware table like primary key table
        // and bucket-aware append only table, but we always add the system column
        // for consistent behavior
        SYSTEM_COLUMNS.put(BUCKET_COLUMN_NAME, DataTypes.INT());
        SYSTEM_COLUMNS.put(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
        SYSTEM_COLUMNS.put(TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }

    private final Catalog paimonCatalog;

    public PaimonLakeCatalog(Configuration configuration) {
        this.paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(configuration.toMap())));
    }

    @VisibleForTesting
    protected Catalog getPaimonCatalog() {
        return paimonCatalog;
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor, Context context)
            throws TableAlreadyExistException {
        // then, create the table
        Identifier paimonPath = toPaimon(tablePath);
        Schema paimonSchema = toPaimonSchema(tableDescriptor);
        try {
            createTable(paimonPath, paimonSchema, context.isCreatingFlussTable());
        } catch (Catalog.DatabaseNotExistException e) {
            // create database
            createDatabase(tablePath.getDatabaseName());
            try {
                createTable(paimonPath, paimonSchema, context.isCreatingFlussTable());
            } catch (Catalog.DatabaseNotExistException t) {
                // shouldn't happen in normal cases
                throw new RuntimeException(
                        String.format(
                                "Fail to create table %s in Paimon, because "
                                        + "Database %s still doesn't exist although create database "
                                        + "successfully, please try again.",
                                tablePath, tablePath.getDatabaseName()));
            }
        }
    }

    @Override
    public void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
            throws TableNotExistException {
        try {
            Identifier paimonPath = toPaimon(tablePath);
            List<SchemaChange> paimonSchemaChanges = toPaimonSchemaChanges(tableChanges);
            alterTable(paimonPath, paimonSchemaChanges);
        } catch (Catalog.ColumnAlreadyExistException | Catalog.ColumnNotExistException e) {
            // shouldn't happen before we support schema change
            throw new RuntimeException(e);
        }
    }

    private void createTable(Identifier tablePath, Schema schema, boolean isCreatingFlussTable)
            throws Catalog.DatabaseNotExistException {
        try {
            // not ignore if table exists
            paimonCatalog.createTable(tablePath, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            try {
                Table table = paimonCatalog.getTable(tablePath);
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                validatePaimonSchemaCompatible(
                        tablePath, fileStoreTable.schema().toSchema(), schema);
                // if creating a new fluss table, we should ensure the lake table is empty
                if (isCreatingFlussTable) {
                    checkTableIsEmpty(tablePath, fileStoreTable);
                }
            } catch (Catalog.TableNotExistException tableNotExistException) {
                // shouldn't happen in normal cases
                throw new RuntimeException(
                        String.format(
                                "Failed to create table %s in Paimon. The table already existed "
                                        + "during the initial creation attempt, but subsequently "
                                        + "could not be found when trying to get it. "
                                        + "Please check whether the Paimon table was manually deleted, and try again.",
                                tablePath));
            }
        }
    }

    private void createDatabase(String databaseName) {
        try {
            // ignore if exists
            paimonCatalog.createDatabase(databaseName, true);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            // do nothing, shouldn't throw since ignoreIfExists
        }
    }

    private void alterTable(Identifier tablePath, List<SchemaChange> tableChanges)
            throws Catalog.ColumnAlreadyExistException, Catalog.ColumnNotExistException {
        try {
            paimonCatalog.alterTable(tablePath, tableChanges, false);
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotExistException("Table " + tablePath + " not exists.");
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(paimonCatalog, "paimon catalog");
    }
}
