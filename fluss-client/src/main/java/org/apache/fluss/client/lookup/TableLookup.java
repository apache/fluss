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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;

import javax.annotation.Nullable;

import java.util.List;

/** API for configuring and creating {@link Lookuper}. */
public class TableLookup implements Lookup {

    private final TableInfo tableInfo;
    private final SchemaGetter schemaGetter;
    private final MetadataUpdater metadataUpdater;
    private final LookupClient lookupClient;

    @Nullable private final List<String> lookupColumnNames;

    private final boolean insertIfNotExists;

    public TableLookup(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient) {
        this(tableInfo, schemaGetter, metadataUpdater, lookupClient, null, false);
    }

    private TableLookup(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            @Nullable List<String> lookupColumnNames) {
        this(tableInfo, schemaGetter, metadataUpdater, lookupClient, lookupColumnNames, false);
    }

    private TableLookup(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            boolean insertIfNotExists) {
        this(tableInfo, schemaGetter, metadataUpdater, lookupClient, null, insertIfNotExists);
    }

    private TableLookup(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            @Nullable List<String> lookupColumnNames,
            boolean insertIfNotExists) {
        this.tableInfo = tableInfo;
        this.schemaGetter = schemaGetter;
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;
        this.lookupColumnNames = lookupColumnNames;
        this.insertIfNotExists = insertIfNotExists;
    }

    @Override
    public Lookup enableInsertIfNotExists() {
        if (lookupColumnNames != null) {
            throw new IllegalArgumentException(
                    "insertIfNotExists can not be used with prefix lookup");
        }

        if (tableInfo.getSchema().getColumns().stream()
                .filter(column -> column.getDataType().isNullable())
                .filter(column -> !tableInfo.getPrimaryKeys().contains(column.getName()))
                .anyMatch(
                        column ->
                                !tableInfo
                                        .getSchema()
                                        .getAutoIncrementColumnNames()
                                        .contains(column.getName()))) {
            throw new IllegalArgumentException(
                    "insertIfNotExists cannot be enabled for tables with nullable columns besides primary key and auto increment columns.");
        }

        return new TableLookup(tableInfo, schemaGetter, metadataUpdater, lookupClient, true);
    }

    @Override
    public Lookup lookupBy(List<String> lookupColumnNames) {
        if (insertIfNotExists) {
            throw new IllegalArgumentException(
                    "insertIfNotExists can not be used with prefix lookup");
        }
        return new TableLookup(
                tableInfo, schemaGetter, metadataUpdater, lookupClient, lookupColumnNames);
    }

    @Override
    public Lookuper createLookuper() {
        if (lookupColumnNames == null) {
            return new PrimaryKeyLookuper(
                    tableInfo, schemaGetter, metadataUpdater, lookupClient, insertIfNotExists);
        } else {
            return new PrefixKeyLookuper(
                    tableInfo, schemaGetter, metadataUpdater, lookupClient, lookupColumnNames);
        }
    }

    @Override
    public <T> TypedLookuper<T> createTypedLookuper(Class<T> pojoClass) {
        return new TypedLookuperImpl<>(createLookuper(), tableInfo, lookupColumnNames, pojoClass);
    }
}
