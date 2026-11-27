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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.InternalRow;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;

/**
 * Builds and commits a complete KV snapshot for one bucket from the supplied rows. Each row must
 * use the full table schema, including partition columns. The caller routes each row to its target
 * bucket before writing it. Later rows with the same primary key replace earlier rows.
 *
 * <p>A bucket with no input still needs to be committed. Across all processes, callers must
 * coordinate a single writer at a time for a bucket of a given BulkLoad transaction.
 */
@PublicEvolving
@NotThreadSafe
public interface BulkLoadBucketWriter extends Closeable {

    /** Writes a row to this bucket, replacing any previous row with the same primary key. */
    void write(InternalRow row) throws IOException;

    /**
     * Uploads and commits this bucket's snapshot on the calling thread. The writer stops accepting
     * new rows when commit begins.
     *
     * <p>If snapshot creation or upload fails, close the writer. If the upload completes and the
     * coordinator commit fails, call this method on the same writer to retry the coordinator
     * commit.
     */
    void commit() throws IOException;

    /** Releases the writer's local resources and leaves remote snapshot files untouched. */
    @Override
    void close() throws IOException;
}
