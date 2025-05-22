/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.lakehouse.lakestorage.LakeCatalog;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.IOUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** A Paimon implementation of {@link LakeCatalog}. */
public class PaimonLakeCatalog implements LakeCatalog {

    private final Catalog paimonCatalog;

    public PaimonLakeCatalog(Configuration configuration) {
        this.paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(configuration.toMap())));
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws TableAlreadyExistException {
        // then, create the table
        Identifier paimonPath = toPaimonIdentifier(tablePath);
        Schema paimonSchema = toPaimonSchema(tableDescriptor);
        try {
            createTable(paimonPath, paimonSchema);
        } catch (Catalog.DatabaseNotExistException e) {
            // create database
            createDatabase(tablePath.getDatabaseName());
            try {
                createTable(paimonPath, paimonSchema);
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

    private void createTable(Identifier tablePath, Schema schema)
            throws Catalog.DatabaseNotExistException {
        try {
            // not ignore if table exists
            paimonCatalog.createTable(tablePath, schema, false);
        } catch (Catalog.TableAlreadyExistException e) {
            throw new TableAlreadyExistException("Table " + tablePath + " already exists.");
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

    private Identifier toPaimonIdentifier(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private Schema toPaimonSchema(TableDescriptor tableDescriptor) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        Options options = new Options();

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

        Map<String, DataType> systemColumns = new LinkedHashMap<>();
        if (!tableDescriptor.hasPrimaryKey()) {
            // for log table, need to set bucket, offset and timestamp as system metadata columns
            systemColumns.put(BUCKET_COLUMN_NAME, DataTypes.INT());
            systemColumns.put(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
            // we use timestamp_ltz type
            systemColumns.put(TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        }

        // set schema
        for (com.alibaba.fluss.metadata.Schema.Column column :
                tableDescriptor.getSchema().getColumns()) {
            String columnName = column.getName();
            if (systemColumns.containsKey(columnName)) {
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
        for (Map.Entry<String, DataType> systemColumn : systemColumns.entrySet()) {
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
        tableDescriptor.getProperties().forEach((k, v) -> setFlussProperty(k, v, options));
        tableDescriptor.getCustomProperties().forEach((k, v) -> setFlussProperty(k, v, options));
        schemaBuilder.options(options.toMap());
        return schemaBuilder.build();
    }

    private void setFlussProperty(String key, String value, Options options) {
        options.set("fluss." + key, value);
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(paimonCatalog, "paimon catalog");
    }
}
