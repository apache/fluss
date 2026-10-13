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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.metrics.MultiTableWriterMetricGroup;
import org.apache.fluss.client.write.WriterClient;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Default implementation of {@link MultiTableWrite}.
 *
 * <p>Thin pass-through builder that constructs a {@link MultiTableWriterImpl} owning a dedicated
 * {@link WriterClient}, so that flushes and schema-scoped batching of one writer never interfere
 * with other writers on the same connection. Tables are registered dynamically on the writer on
 * first {@code write(...)} call &mdash; no upfront declaration is required.
 *
 * @since 0.7
 */
@Internal
public class MultiTableWriteImpl implements MultiTableWrite {

    private final FlussConnection connection;

    public MultiTableWriteImpl(FlussConnection connection) {
        this.connection = checkNotNull(connection, "connection");
    }

    @Override
    public MultiTableWriter createWriter() {
        // Each writer owns a dedicated WriterClient (buffer pool + sender thread) instead of the
        // connection-level shared one, so MultiTableWriter#flush() only affects this writer and
        // schema-id batching of different writers cannot interfere with each other.
        return new MultiTableWriterImpl(
                connection.getMetadataUpdater(),
                connection.getOrCreateAdmin(),
                new WriterClient(
                        connection.getConfiguration(),
                        connection.getMetadataUpdater(),
                        new MultiTableWriterMetricGroup(connection.getClientMetricGroup()),
                        connection.getOrCreateAdmin()));
    }
}
