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
import org.apache.fluss.metadata.TableInfo;

import javax.annotation.Nullable;

import java.util.List;

/** API for configuring and creating {@link Lookuper}. */
public class TableLookup implements Lookup {

    private final TableInfo tableInfo;
    private final MetadataUpdater metadataUpdater;
    private final LookupClient lookupClient;

    @Nullable private final List<String> lookupColumnNames;

    public TableLookup(
            TableInfo tableInfo, MetadataUpdater metadataUpdater, LookupClient lookupClient) {
        this(tableInfo, metadataUpdater, lookupClient, null);
    }

    private TableLookup(
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            @Nullable List<String> lookupColumnNames) {
        this.tableInfo = tableInfo;
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;
        this.lookupColumnNames = lookupColumnNames;
    }

    @Override
    public Lookup lookupBy(List<String> lookupColumnNames) {
        return new TableLookup(tableInfo, metadataUpdater, lookupClient, lookupColumnNames);
    }

    @Override
    public Lookuper createLookuper() {
        if (lookupColumnNames == null) {
            return new PrimaryKeyLookuper(tableInfo, metadataUpdater, lookupClient);
        } else {
            return new PrefixKeyLookuper(
                    tableInfo, metadataUpdater, lookupClient, lookupColumnNames);
        }
    }
}
