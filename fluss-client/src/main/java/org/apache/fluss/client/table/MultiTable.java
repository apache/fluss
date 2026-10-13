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

package org.apache.fluss.client.table;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.scanner.MultiTableScan;
import org.apache.fluss.client.table.writer.MultiTableWrite;
import org.apache.fluss.metadata.TablePath;

/**
 * Used to communicate with multiple Fluss tables through a single client. Obtain an instance from
 * {@link Connection#getMultiTable()}.
 *
 * <p>{@code MultiTable} is the cross-table counterpart of {@link Table}. Useful for CDC ingestion,
 * cross-table streaming pipelines, and multi-table catalog sinks.
 *
 * <p>Unlike {@link Connection#getTable(TablePath)} which is bound to one table, {@code MultiTable}
 * is table-agnostic: tables are identified per scan subscription and per write record.
 *
 * <p>{@code MultiTable} instances are light-weight and NOT thread-safe; obtain per-thread.
 *
 * @since 0.7
 */
@PublicEvolving
public interface MultiTable {

    /** Build a scanner that reads data from multiple tables simultaneously. */
    MultiTableScan newMultiTableScan();

    /** Build a writer that writes data (with change types) to multiple tables. */
    MultiTableWrite newMultiTableWrite();
}
