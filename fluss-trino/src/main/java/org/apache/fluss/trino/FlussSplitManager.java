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

import org.apache.fluss.metadata.TableInfo;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;

import java.util.Set;

import static org.apache.fluss.trino.FlussTableScanValidator.validateTable;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Plans bounded physical-bucket scans for native Fluss tables. */
public final class FlussSplitManager implements ConnectorSplitManager {

    private final FlussMetadataAccess metadataAccess;
    private final FlussSplitPlanner splitPlanner;

    @Inject
    FlussSplitManager(FlussMetadataAccess metadataAccess, FlussSplitPlanner splitPlanner) {
        this.metadataAccess = checkNotNull(metadataAccess, "metadataAccess is null");
        this.splitPlanner = checkNotNull(splitPlanner, "splitPlanner is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint) {
        FlussTableHandle handle = (FlussTableHandle) table;
        TableInfo tableInfo = metadataAccess.getTableInfo(handle);
        validateTable(handle, tableInfo);
        return new FixedSplitSource(splitPlanner.plan(handle, tableInfo));
    }
}
