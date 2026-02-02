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
import java.util.stream.Collectors;

/** API for configuring and creating {@link Lookuper}. */
public class TableLookup implements Lookup {

    private final TableInfo tableInfo;
    private final SchemaGetter schemaGetter;
    private final MetadataUpdater metadataUpdater;
    private final LookupClient lookupClient;

    @Nullable private final List<String> lookupColumnNames;

    public TableLookup(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient) {
        this(tableInfo, schemaGetter, metadataUpdater, lookupClient, null);
    }

    private TableLookup(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            @Nullable List<String> lookupColumnNames) {
        this.tableInfo = tableInfo;
        this.schemaGetter = schemaGetter;
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;
        this.lookupColumnNames = lookupColumnNames;
    }

    @Override
    public Lookup lookupBy(List<String> lookupColumnNames) {
        return new TableLookup(
                tableInfo, schemaGetter, metadataUpdater, lookupClient, lookupColumnNames);
    }

    @Override
    public Lookuper createLookuper() {
        if (lookupColumnNames == null || isPrimaryKey(lookupColumnNames)) {
            return new PrimaryKeyLookuper(tableInfo, schemaGetter, metadataUpdater, lookupClient);
        }  

        if (isPrefixKey(lookupColumnNames)) {
            return new PrefixKeyLookuper(
                tableInfo, schemaGetter, metadataUpdater, lookupClient, lookupColumnNames);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid lookup columns %s for table '%s'. "
                                    + "Lookup columns must be either the complete primary key %s "
                                    + "or a valid prefix key (bucket key %s as prefix of physical primary key %s).",
                            lookupColumnNames,
                            tableInfo.getTablePath(),
                            tableInfo.getPrimaryKeys(),
                            tableInfo.getBucketKeys(),
                            tableInfo.getPhysicalPrimaryKeys()));
        }
    }

    private boolean isPrimaryKey(List<String> lookupColumns) {
        return lookupColumns.equals(tableInfo.getPrimaryKeys());
    }

    private boolean isPrefixKey(List<String> lookupColumns) {
        if (!tableInfo.hasPrimaryKey()) {
            return false;
        }
        
        List<String> physicalLookupColumns =
                lookupColumns.stream()
                        .filter(col -> !tableInfo.getPartitionKeys().contains(col))
                        .collect(Collectors.toList());
        
        List<String> physicalPrimaryKeys = tableInfo.getPhysicalPrimaryKeys();
        
        if (physicalLookupColumns.isEmpty() || physicalLookupColumns.size() >= physicalPrimaryKeys.size()) {
            return false;
        }
        
        for (int i = 0; i < physicalLookupColumns.size(); i++) {
            if (!physicalLookupColumns.get(i).equals(physicalPrimaryKeys.get(i))) {
                return false;
            }
        }
        
        return true;
    }

    @Override
    public <T> TypedLookuper<T> createTypedLookuper(Class<T> pojoClass) {
        return new TypedLookuperImpl<>(createLookuper(), tableInfo, lookupColumnNames, pojoClass);
    }
}
