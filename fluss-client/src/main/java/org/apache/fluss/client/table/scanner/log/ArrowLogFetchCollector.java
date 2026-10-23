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
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Collects Arrow batches from completed fetches. */
@ThreadSafe
@Internal
public class ArrowLogFetchCollector
        extends AbstractLogFetchCollector<ArrowBatchData, ArrowScanRecords> {
    private static final Logger LOG = LoggerFactory.getLogger(ArrowLogFetchCollector.class);

    public ArrowLogFetchCollector(
            LogScannerStatus logScannerStatus,
            Configuration conf,
            MetadataUpdater metadataUpdater) {
        super(LOG, logScannerStatus, conf, metadataUpdater);
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
            Map<TableBucket, Long> consumedUpToOffsets,
            Set<TableBucket> finishedBuckets) {
        return new ArrowScanRecords(fetchedRecords, consumedUpToOffsets, finishedBuckets);
    }

    @Override
    protected List<ArrowBatchData> trimFetchedRecords(
            List<ArrowBatchData> fetchedRecords, long stoppingOffset) {
        for (int i = 0; i < fetchedRecords.size(); i++) {
            ArrowBatchData batch = fetchedRecords.get(i);
            long batchBaseOffset = batch.getBaseLogOffset();
            int batchRecordCount = batch.getRecordCount();

            if (batchBaseOffset >= stoppingOffset) {
                for (int j = i; j < fetchedRecords.size(); j++) {
                    IOUtils.closeQuietly(fetchedRecords.get(j));
                }

                if (i == 0) {
                    return Collections.emptyList();
                }
                return new ArrayList<>(fetchedRecords.subList(0, i));
            }

            long writableRowCount = stoppingOffset - batchBaseOffset;
            if (writableRowCount < batchRecordCount) {
                ArrowBatchData truncatedBatch =
                        batch.truncateAndTransferOwnership((int) writableRowCount);

                List<ArrowBatchData> trimmedRecords = new ArrayList<>(i + 1);
                trimmedRecords.addAll(fetchedRecords.subList(0, i));
                trimmedRecords.add(truncatedBatch);

                for (int j = i + 1; j < fetchedRecords.size(); j++) {
                    IOUtils.closeQuietly(fetchedRecords.get(j));
                }

                return trimmedRecords;
            }
        }

        return fetchedRecords;
    }

    @Override
    protected void closeFetchedRecords(Map<TableBucket, List<ArrowBatchData>> fetched) {
        for (Map.Entry<TableBucket, List<ArrowBatchData>> entry : fetched.entrySet()) {

            for (ArrowBatchData batch : entry.getValue()) {
                try {
                    batch.close();
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to close Arrow batch during cleanup for bucket {}",
                            entry.getKey(),
                            e);
                }
            }
        }
    }
}
