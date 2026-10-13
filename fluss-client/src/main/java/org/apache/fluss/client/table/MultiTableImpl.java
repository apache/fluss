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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.table.scanner.MultiTableScan;
import org.apache.fluss.client.table.scanner.MultiTableScanImpl;
import org.apache.fluss.client.table.writer.MultiTableWrite;
import org.apache.fluss.client.table.writer.MultiTableWriteImpl;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Default implementation of {@link MultiTable}.
 *
 * <p>{@code MultiTableImpl} is light-weight and NOT thread-safe; the underlying {@link
 * FlussConnection} is shared and thread-safe, but each {@code MultiTableImpl} instance should be
 * obtained per-thread.
 *
 * @since 0.7
 */
@Internal
public class MultiTableImpl implements MultiTable {

    private final FlussConnection connection;

    public MultiTableImpl(FlussConnection connection) {
        this.connection = checkNotNull(connection, "connection");
    }

    @Override
    public MultiTableScan newMultiTableScan() {
        return new MultiTableScanImpl(connection);
    }

    @Override
    public MultiTableWrite newMultiTableWrite() {
        return new MultiTableWriteImpl(connection);
    }
}
