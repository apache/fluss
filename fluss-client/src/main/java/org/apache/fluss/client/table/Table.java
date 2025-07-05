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
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableInfo;

/**
 * Used to communicate with a single Fluss table. Obtain an instance from a {@link Connection}.
 *
 * <p>Table can be used to get, put, delete or scan data from a fluss table.
 *
 * @since 0.1
 */
@PublicEvolving
public interface Table extends AutoCloseable {

    /**
     * Get the {@link TableInfo} for this table.
     *
     * <p>Note: the table info of this {@link Table} is set during the creation of this {@link
     * Table} and will not be updated after that, even if the table info of the table has been
     * changed. Therefore, if there are any changes to the table info, it may be necessary to
     * reconstruct the {@link Table}.
     */
    TableInfo getTableInfo();

    /**
     * Creates a new {@link Scan} for this table to configure and create a scanner to scan data for
     * this table. The scanner can be a log scanner to continuously read streaming log data or a
     * batch scanner to read batch data.
     */
    Scan newScan();

    /**
     * Creates a new {@link Lookup} for this table to configure and create a {@link Lookuper} to
     * lookup data for this table by primary key or a prefix of primary key.
     */
    Lookup newLookup();

    /**
     * Creates a new {@link Append} to build a {@link AppendWriter} to append data to this table
     * (requires to be a Log Table).
     */
    Append newAppend();

    /**
     * Creates a new {@link Upsert} to build a {@link UpsertWriter} to upsert and delete data to
     * this table (requires to be a Primary Key Table).
     */
    Upsert newUpsert();
}
