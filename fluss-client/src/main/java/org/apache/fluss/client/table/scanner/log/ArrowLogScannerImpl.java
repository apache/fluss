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

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ConcurrentModificationException;

/**
 * The default impl of {@link ArrowLogScanner}.
 *
 * <p>The {@link ArrowLogScannerImpl} is NOT thread-safe. It is the responsibility of the user to
 * ensure that multithreaded access is properly synchronized. Un-synchronized access will result in
 * {@link ConcurrentModificationException}.
 */
public class ArrowLogScannerImpl extends AbstractLogScanner<ArrowScanRecords>
        implements ArrowLogScanner {
    private static final Logger LOG = LoggerFactory.getLogger(ArrowLogScannerImpl.class);

    public ArrowLogScannerImpl(
            Configuration conf,
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            ClientMetricGroup clientMetricGroup,
            RemoteFileDownloader remoteFileDownloader,
            @Nullable int[] projectedFields,
            SchemaGetter schemaGetter) {
        super(
                conf,
                tableInfo,
                metadataUpdater,
                clientMetricGroup,
                remoteFileDownloader,
                projectedFields,
                schemaGetter,
                LOG,
                "arrow log scanner");
    }

    @Override
    public ArrowScanRecords poll(Duration timeout) {
        return doPoll(timeout);
    }

    @Override
    protected ArrowScanRecords pollForFetches() {
        ArrowScanRecords scanRecords = logFetcher.collectArrowFetch();
        if (!scanRecords.isEmpty()) {
            return scanRecords;
        }

        // send any new fetches (won't resend pending fetches).
        logFetcher.sendFetches();
        return logFetcher.collectArrowFetch();
    }

    @Override
    protected ArrowScanRecords emptyResult() {
        return ArrowScanRecords.EMPTY;
    }

    @Override
    protected boolean isEmpty(ArrowScanRecords result) {
        return result.isEmpty();
    }
}
