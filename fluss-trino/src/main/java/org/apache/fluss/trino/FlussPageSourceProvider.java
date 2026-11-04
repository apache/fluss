/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;

import java.util.List;
import java.util.Optional;

import static org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Creates an independent bounded page source for each scheduled Fluss split. */
public final class FlussPageSourceProvider implements ConnectorPageSourceProvider {

    private final FlussClientManager clients;

    @Inject
    FlussPageSourceProvider(FlussClientManager clients) {
        this.clients = checkNotNull(clients, "clients is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            MemoryContext memoryContext) {
        FlussTableHandle handle = (FlussTableHandle) table;
        FlussSplit flussSplit = (FlussSplit) split;

        ImmutableList<FlussColumnHandle> projectedColumns =
                columns.stream().map(FlussColumnHandle.class::cast).collect(toImmutableList());

        return new FlussPageSource(
                clients,
                handle,
                flussSplit,
                projectedColumns,
                checkNotNull(memoryContext, "memoryContext is null"));
    }
}
