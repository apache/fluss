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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.table.scanner.log.MultiTableLogScanner;
import org.apache.fluss.client.table.scanner.log.MultiTableLogScannerImpl;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Default implementation of {@link MultiTableScan}.
 *
 * <p>Phase 1: a thin pass-through that creates a {@link MultiTableLogScanner} bound to the {@link
 * FlussConnection}. Tables are registered dynamically on the scanner via {@code subscribe(...)} —
 * no upfront declaration is required.
 *
 * @since 0.7
 */
@Internal
public class MultiTableScanImpl implements MultiTableScan {

    private final FlussConnection connection;

    public MultiTableScanImpl(FlussConnection connection) {
        this.connection = checkNotNull(connection, "connection");
    }

    @Override
    public MultiTableLogScanner createLogScanner() {
        return new MultiTableLogScannerImpl(
                "multi-table-log-scanner",
                connection.getMetadataUpdater(),
                connection.getConfiguration(),
                connection.getClientMetricGroup(),
                connection.getOrCreateRemoteFileDownloader(),
                connection.getOrCreateAdmin());
    }
}
