/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.sink;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.paimon.source.NewTablesAddedListener;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkFileIOLoader;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/** A paimon Listener for discovering new tables added to be synced. */
public class NewTablesAddedPaimonListener implements NewTablesAddedListener {

    private static final long serialVersionUID = 1L;

    public static final String OFFSET_COLUMN_NAME = "__offset";
    public static final String TIMESTAMP_COLUMN_NAME = "__timestamp";

    private final Configuration configuration;

    private final Set<Long> addedTables;

    private transient Catalog paimonCatalog;

    public NewTablesAddedPaimonListener(Configuration configuration) {
        this.configuration = configuration;
        this.addedTables = new HashSet<>();
    }

    @Override
    public void onNewTablesAdded(Collection<TableInfo> newTables) {
        if (newTables.isEmpty()) {
            return;
        }
        if (paimonCatalog == null) {
            paimonCatalog =
                    CatalogFactory.createCatalog(
                            CatalogContext.create(
                                    Options.fromMap(configuration.toMap()),
                                    new FlinkFileIOLoader()));
        }
        for (TableInfo tableInfo : newTables) {
            if (addedTables.contains(tableInfo.getTableId())) {
                continue;
            }
            try {
                createTable(tableInfo);
                addedTables.add(tableInfo.getTableId());
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to create table in paimon." + tableInfo.getTablePath(), e);
            }
        }
    }

    protected void createTable(TableInfo tableInfo) throws Exception {
        Identifier identifier = toPaimonIdentifier(tableInfo.getTablePath());

        // if database not exists, create it
        try {
            paimonCatalog.getDatabase(identifier.getDatabaseName());
        } catch (DatabaseNotExistException ignored) {
            paimonCatalog.createDatabase(identifier.getDatabaseName(), true);
        }

        // then, create the table
        paimonCatalog.createTable(identifier, toPaimonSchema(tableInfo), true);
    }

    private Identifier toPaimonIdentifier(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private Schema toPaimonSchema(TableInfo flussTable) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        Options options = new Options();

        // When bucket key is undefined, it should use dynamic bucket (bucket = -1) mode.
        if (flussTable.hasBucketKey()) {
            options.set(CoreOptions.BUCKET, flussTable.getNumBuckets());
            options.set(CoreOptions.BUCKET_KEY, String.join(",", flussTable.getBucketKeys()));
        } else {
            options.set(CoreOptions.BUCKET, CoreOptions.BUCKET.defaultValue());
        }

        // set schema
        for (com.alibaba.fluss.metadata.Schema.Column column :
                flussTable.getSchema().getColumns()) {
            schemaBuilder.column(
                    column.getName(),
                    column.getDataType().accept(FlussDataTypeToPaimonDataType.INSTANCE),
                    column.getComment().orElse(null));
        }

        // set pk
        if (flussTable.getSchema().getPrimaryKey().isPresent()) {
            schemaBuilder.primaryKey(flussTable.getSchema().getPrimaryKey().get().getColumnNames());
            options.set(
                    CoreOptions.CHANGELOG_PRODUCER.key(),
                    CoreOptions.ChangelogProducer.INPUT.toString());
        } else {
            // for log table, need to set offset and timestamp
            schemaBuilder.column(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
            // we use timestamp_ltz type
            schemaBuilder.column(TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        }
        // set partition keys
        schemaBuilder.partitionKeys(flussTable.getPartitionKeys());

        // set custom properties to paimon schema
        flussTable.getCustomProperties().toMap().forEach(options::set);
        schemaBuilder.options(options.toMap());
        return schemaBuilder.build();
    }
}
