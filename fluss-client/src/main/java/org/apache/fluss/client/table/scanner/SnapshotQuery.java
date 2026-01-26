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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * Used to configure and execute a snapshot query to read all kv data of a primary key table.
 *
 * @since 0.9
 */
@PublicEvolving
public interface SnapshotQuery {
    /**
     * Executes the snapshot query to read all current data in the given table bucket.
     *
     * @param tableBucket the table bucket to read
     * @return a closeable iterator of the rows in the table bucket
     */
    CloseableIterator<InternalRow> execute(TableBucket tableBucket);

    /**
     * Executes the snapshot query to read all current data in the table. Everything around
     * partitions and buckets will be taken care of from the client.
     *
     * @return a closeable iterator of the rows in the table
     */
    CloseableIterator<InternalRow> execute();
}
