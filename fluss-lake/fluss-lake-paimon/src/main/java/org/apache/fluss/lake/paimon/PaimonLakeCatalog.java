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
import org.apache.fluss.exception.InvalidAlterTableException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(PaimonLakeCatalog.class);
    public static final LinkedHashMap<String, DataType> SYSTEM_COLUMNS = new LinkedHashMap<>();

    static {
        // We need __bucket system column to filter out the given bucket
        // for paimon bucket-unaware append only table.
        // It's not required for paimon bucket-aware table like primary key table
        // and bucket-aware append only table, but we always add the system column
        // for consistent behavior
        SYSTEM_COLUMNS.put(BUCKET_COLUMN_NAME, DataTypes.INT());
        SYSTEM_COLUMNS.put(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
        SYSTEM_COLUMNS.put(TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP_LTZ_MILLIS());
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
        Schema paimonSchema = toPaimonSchema(tableDescriptor);
        try {
            createTable(tablePath, paimonSchema, context.isCreatingFlussTable());
        } catch (Catalog.DatabaseNotExistException e) {
            // create database
            createDatabase(tablePath.getDatabaseName());
            try {
                createTable(tablePath, paimonSchema, context.isCreatingFlussTable());
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
            List<SchemaChange> paimonSchemaChanges = toPaimonSchemaChanges(tableChanges);

            // Compare current Paimon table schema with expected target schema before altering
            if (shouldAlterTable(tablePath, tableChanges)) {
                alterTable(tablePath, paimonSchemaChanges);
            } else {
                // If schemas already match, treat as idempotent success
                LOG.info(
                        "Skipping schema evolution for Paimon table {} because the column(s) to add {} already exist.",
                        tablePath,
                        tableChanges);
            }
        } catch (Catalog.ColumnAlreadyExistException e) {
            // This shouldn't happen if shouldAlterTable works correctly, but keep as safeguard
            throw new InvalidAlterTableException(e.getMessage());
        } catch (Catalog.ColumnNotExistException e) {
            // This shouldn't happen for AddColumn operations
            throw new InvalidAlterTableException(e.getMessage());
        }
    }

    private boolean shouldAlterTable(TablePath tablePath, List<TableChange> tableChanges)
            throws TableNotExistException {
        try {
            Table table = paimonCatalog.getTable(toPaimon(tablePath));
            FileStoreTable fileStoreTable = (FileStoreTable) table;
            Schema currentSchema = fileStoreTable.schema().toSchema();

            for (TableChange change : tableChanges) {
                if (change instanceof TableChange.AddColumn) {
                    TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
                    if (!isColumnAlreadyExists(currentSchema, addColumn)) {
                        return true;
                    }
                } else {
                    return true;
                }
            }

            return false;
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotExistException("Table " + tablePath + " does not exist.");
        }
    }

    private boolean isColumnAlreadyExists(Schema currentSchema, TableChange.AddColumn addColumn) {
        String columnName = addColumn.getName();

        for (org.apache.paimon.types.DataField field : currentSchema.fields()) {
            if (field.name().equals(columnName)) {
                org.apache.paimon.types.DataType expectedType =
                        addColumn
                                .getDataType()
                                .accept(
                                        org.apache.fluss.lake.paimon.utils
                                                .FlussDataTypeToPaimonDataType.INSTANCE);

                if (!field.type().equals(expectedType)) {
                    throw new InvalidAlterTableException(
                            String.format(
                                    "Column '%s' already exists but with different type. "
                                            + "Existing: %s, Expected: %s",
                                    columnName, field.type(), expectedType));
                }
                String existingComment = field.description();
                String expectedComment = addColumn.getComment();

                boolean commentsMatch =
                        (existingComment == null && expectedComment == null)
                                || (existingComment != null
                                        && existingComment.equals(expectedComment));

                if (!commentsMatch) {
                    throw new InvalidAlterTableException(
                            String.format(
                                    "Column %s already exists but with different comment. "
                                            + "Existing: %s, Expected: %s",
                                    columnName, existingComment, expectedComment));
                }

                return true;
            }
        }

        return false;
    }

    private void createTable(TablePath tablePath, Schema schema, boolean isCreatingFlussTable)
            throws Catalog.DatabaseNotExistException {
        Identifier paimonPath = toPaimon(tablePath);
        try {
            // not ignore if table exists
            paimonCatalog.createTable(paimonPath, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            try {
                Table table = paimonCatalog.getTable(paimonPath);
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                validatePaimonSchemaCompatible(
                        paimonPath, fileStoreTable.schema().toSchema(), schema);
                // if creating a new fluss table, we should ensure the lake table is empty
                if (isCreatingFlussTable) {
                    checkTableIsEmpty(tablePath, fileStoreTable);
                }
            } catch (Catalog.TableNotExistException tableNotExistException) {
                // shouldn't happen in normal cases
                throw new InvalidAlterTableException(
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

    private void alterTable(TablePath tablePath, List<SchemaChange> tableChanges)
            throws Catalog.ColumnAlreadyExistException, Catalog.ColumnNotExistException {
        try {
            paimonCatalog.alterTable(toPaimon(tablePath), tableChanges, false);
        } catch (Catalog.TableNotExistException e) {
            throw new TableNotExistException("Table " + tablePath + " does not exist.");
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(paimonCatalog, "paimon catalog");
    }
}
