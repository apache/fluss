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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A container that holds the list {@link ScanRecord} per bucket for a particular table. There is
 * one {@link ScanRecord} list for every bucket returned by a {@link
 * LogScanner#poll(java.time.Duration)} operation.
 *
 * @since 0.1
 */
@PublicEvolving
public class ScanRecords implements Iterable<ScanRecord>, AutoCloseable {
    public static final ScanRecords EMPTY = new ScanRecords(Collections.emptyMap());

    private final Map<TableBucket, CloseableIterator<ScanRecord>> records;

    public ScanRecords(Map<TableBucket, CloseableIterator<ScanRecord>> records) {
        this.records = records;
    }

    /**
     * Get just the records for the given bucketId.
     *
     * @param scanBucket The bucket to get records for
     */
    public Iterator<ScanRecord> records(TableBucket scanBucket) {
        if (records.containsKey(scanBucket)) {
            return records.get(scanBucket);
        }
        return CloseableIterator.emptyIterator();
    }

    /**
     * Get the bucket ids which have records contained in this record set.
     *
     * @return the set of partitions with data in this record set (maybe empty if no data was
     *     returned)
     */
    public Set<TableBucket> buckets() {
        return Collections.unmodifiableSet(records.keySet());
    }

    @Override
    public Iterator<ScanRecord> iterator() {
        return CloseableIterator.concatenate(records.values());
    }

    @Override
    public void close() throws Exception {
        Exception thrownException = null;
        for (CloseableIterator<ScanRecord> scanRecord: records.values()) {
            try {
                scanRecord.close();
            } catch (Exception e) {
                thrownException = ExceptionUtils.firstOrSuppressed(e, thrownException);
            }
        }

        if (thrownException != null) {
            throw thrownException;
        }
    }
}
