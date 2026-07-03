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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * {@link LogFetchCollector} operates at the {@link LogRecordBatch} level, as that is what is stored
 * in the {@link LogFetchBuffer}. Each {@link LogRecord} in the {@link LogRecordBatch} is converted
 * to a {@link ScanRecord} and added to the returned {@link LogFetcher}.
 */
@ThreadSafe
@Internal
public class LogFetchCollector extends AbstractLogFetchCollector<ScanRecord, ScanRecords> {
    private static final Logger LOG = LoggerFactory.getLogger(LogFetchCollector.class);

    public LogFetchCollector(
            TablePath tablePath,
            LogScannerStatus logScannerStatus,
            Configuration conf,
            MetadataUpdater metadataUpdater) {
        super(LOG, tablePath, logScannerStatus, conf, metadataUpdater);
    }

    @Override
    protected List<ScanRecord> doFetchRecords(CompletedFetch nextInLineFetch, int maxRecords) {
        return nextInLineFetch.fetchRecords(maxRecords);
    }

    @Override
    protected int recordCount(List<ScanRecord> fetchedRecords) {
        return fetchedRecords.size();
    }

    @Override
    protected ScanRecords toResult(
            Map<TableBucket, List<ScanRecord>> fetchedRecords,
            Map<TableBucket, Long> consumedUpToOffsets) {
        return new ScanRecords(fetchedRecords, consumedUpToOffsets);
    }
}
