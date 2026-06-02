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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.utils.Projection;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Per-table scan specification consumed by {@link LogFetcher#registerTable}.
 *
 * <p>Captures the {@link TableInfo} together with optional projection and filter pushdown to be
 * applied while reading log records for the table.
 *
 * @since 0.7
 */
@Internal
public final class TableScanSpec {

    private final TableInfo tableInfo;
    @Nullable private final Projection projection;
    @Nullable private final Predicate filter;

    public TableScanSpec(
            TableInfo tableInfo, @Nullable Projection projection, @Nullable Predicate filter) {
        this.tableInfo = checkNotNull(tableInfo, "tableInfo");
        this.projection = projection;
        this.filter = filter;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    @Nullable
    public Projection getProjection() {
        return projection;
    }

    @Nullable
    public Predicate getFilter() {
        return filter;
    }
}
