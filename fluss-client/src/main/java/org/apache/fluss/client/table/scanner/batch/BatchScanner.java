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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

/**
 * The scanner that reads records form a table in a batch fashion. Compared to {@link LogScanner},
 * this scanner is designed to read bounded data and will stop when reading end of a bucket, but
 * {@link LogScanner} is designed to read unbounded data and continuously read data from buckets.
 *
 * @since 0.6
 */
@PublicEvolving
public interface BatchScanner extends Closeable {

    /**
     * Poll one batch records. The method should return null when reaching the end of the input.
     *
     * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE}
     *     milliseconds)
     */
    @Nullable
    CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException;

    /** Closes the scanner and should release all resources. */
    @Override
    void close() throws IOException;
}
