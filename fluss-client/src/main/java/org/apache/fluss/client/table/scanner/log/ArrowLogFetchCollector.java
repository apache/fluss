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

import java.util.Collections;
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
    protected List<ArrowBatchData> fetchRecords(CompletedFetch nextInLineFetch, int maxRecords) {
        TableBucket tb = nextInLineFetch.tableBucket;
        Long offset = logScannerStatus.getBucketOffset(tb);
        if (offset == null) {
            LOG.debug(
                    "Ignoring fetched records for {} at offset {} since the current offset is null which means the bucket has been unsubscribe.",
                    tb,
                    nextInLineFetch.nextFetchOffset());
        } else {
            if (nextInLineFetch.nextFetchOffset() == offset) {
                List<ArrowBatchData> batches = nextInLineFetch.fetchArrowBatches(maxRecords);
                LOG.trace(
                        "Returning {} fetched arrow batches at offset {} for assigned bucket {}.",
                        batches.size(),
                        offset,
                        tb);

                if (nextInLineFetch.nextFetchOffset() > offset) {
                    LOG.trace(
                            "Updating fetch offset from {} to {} for bucket {} and returning {} arrow batches from poll()",
                            offset,
                            nextInLineFetch.nextFetchOffset(),
                            tb,
                            batches.size());
                    logScannerStatus.updateOffset(tb, nextInLineFetch.nextFetchOffset());
                }
                return batches;
            } else {
                // these records aren't next in line based on the last consumed offset, ignore them
                // they must be from an obsolete request
                LOG.warn(
                        "Ignoring fetched records for {} at offset {} since the current offset is {}",
                        nextInLineFetch.tableBucket,
                        nextInLineFetch.nextFetchOffset(),
                        offset);
            }
        }

        LOG.trace("Draining fetched records for bucket {}", nextInLineFetch.tableBucket);
        nextInLineFetch.drain();
        return Collections.emptyList();
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
    protected ArrowScanRecords toResult(Map<TableBucket, List<ArrowBatchData>> fetchedRecords) {
        return new ArrowScanRecords(fetchedRecords);
    }
}
