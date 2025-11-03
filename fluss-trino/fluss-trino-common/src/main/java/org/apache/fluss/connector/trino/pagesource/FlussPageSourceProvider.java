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

package org.apache.fluss.connector.trino.pagesource;

import org.apache.fluss.connector.trino.connection.FlussClientManager;
import org.apache.fluss.connector.trino.handle.FlussColumnHandle;
import org.apache.fluss.connector.trino.handle.FlussTableHandle;
import org.apache.fluss.connector.trino.split.FlussSplit;

import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Provider for Fluss page sources.
 * 
 * <p>This class creates page sources for reading data from Fluss tables.
 */
public class FlussPageSourceProvider implements ConnectorPageSourceProvider {

    private static final Logger log = Logger.get(FlussPageSourceProvider.class);

    private final FlussClientManager clientManager;

    @Inject
    public FlussPageSourceProvider(FlussClientManager clientManager) {
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        
        try {
            FlussSplit flussSplit = (FlussSplit) split;
            FlussTableHandle flussTable = (FlussTableHandle) table;
            
            List<FlussColumnHandle> flussColumns = columns.stream()
                    .map(col -> {
                        if (!(col instanceof FlussColumnHandle)) {
                            throw new IllegalArgumentException("Expected FlussColumnHandle, got: " + col.getClass());
                        }
                        return (FlussColumnHandle) col;
                    })
                    .collect(Collectors.toList());
            
            log.debug("Creating page source for table: %s, bucket: %s, columns: %d",
                    flussSplit.getTablePath(),
                    flussSplit.getTableBucket(),
                    flussColumns.size());
            
            // Apply dynamic filters if available
            if (!dynamicFilter.getCurrentPredicate().isAll()) {
                log.debug("Applying dynamic filter for split: %s", flussSplit.getTableBucket());
                // In a full implementation, we would apply dynamic filters to the scanner
            }
            
            return new FlussPageSource(
                    clientManager,
                    flussTable,
                    flussSplit,
                    flussColumns);
        } catch (Exception e) {
            log.error(e, "Error creating page source for split: %s", split);
            throw new RuntimeException("Failed to create page source", e);
        }
    }
}
