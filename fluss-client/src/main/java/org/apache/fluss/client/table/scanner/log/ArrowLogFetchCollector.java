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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ArrowBatchData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;

/** Collects Arrow batches from completed fetches. */
@ThreadSafe
@Internal
public class ArrowLogFetchCollector
        extends AbstractLogFetchCollector<ArrowBatchData, ArrowScanRecords> {
    private static final Logger LOG = LoggerFactory.getLogger(ArrowLogFetchCollector.class);

    public ArrowLogFetchCollector(
            TablePath tablePath,
            LogScannerStatus logScannerStatus,
            Configuration conf,
            MetadataUpdater metadataUpdater) {
        super(LOG, tablePath, logScannerStatus, conf, metadataUpdater);
    }

    @Override
    protected List<ArrowBatchData> doFetchRecords(CompletedFetch nextInLineFetch, int maxRecords) {
        return nextInLineFetch.fetchArrowBatches(maxRecords);
    }

    @Override
    protected int recordCount(List<ArrowBatchData> fetchedRecords) {
        int count = 0;
        for (ArrowBatchData fetchedRecord : fetchedRecords) {
            count += fetchedRecord.getRecordCount();
        }
        return count;
    }

    @Override
    protected ArrowScanRecords toResult(
            Map<TableBucket, List<ArrowBatchData>> fetchedRecords,
            Map<TableBucket, Long> consumedUpToOffsets) {
        // Arrow scan paths don't need consumedUpToOffsets (issue #2371 is specific to
        // row-based tiering), so it's discarded here rather than carried in ArrowScanRecords.
        return new ArrowScanRecords(fetchedRecords);
    }

    @Override
    protected void closeFetchedRecords(Map<TableBucket, List<ArrowBatchData>> fetched) {
        for (List<ArrowBatchData> batches : fetched.values()) {
            for (ArrowBatchData batch : batches) {
                try {
                    batch.close();
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to close Arrow batch during cleanup for table {}",
                            tablePath,
                            e);
                }
            }
        }
    }
}
