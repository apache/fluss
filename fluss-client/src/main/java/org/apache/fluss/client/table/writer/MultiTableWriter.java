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
import org.apache.fluss.exception.TableNotExistException;

import java.util.concurrent.CompletableFuture;

/**
 * A writer that sends records (with any {@link ChangeType}) to multiple tables through one
 * underlying writer client. The target table, change type, and row are carried by the {@link
 * MultiTableWriteRecord} itself.
 *
 * <p>NOT thread-safe.
 *
 * @since 0.7
 */
@PublicEvolving
public interface MultiTableWriter extends AutoCloseable {

    /**
     * Write a record to the table identified by {@link MultiTableWriteRecord#getTablePath()}.
     *
     * <p>Asynchronous; the returned future completes when the server has acknowledged the write (or
     * has failed to write it).
     *
     * @throws TableNotExistException wrapped in the future, if the table is unknown
     */
    CompletableFuture<WriteResult> write(MultiTableWriteRecord record);

    /**
     * Flush all buffered records and block until acknowledged by the servers (or failed).
     * Consistent with {@link TableWriter#flush()}.
     */
    void flush();

    /** Close the writer; pending records are flushed first. */
    @Override
    void close() throws Exception;
}
