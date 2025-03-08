/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.flink.source.event;

import com.alibaba.fluss.metadata.TableBucket;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Collection;
import java.util.Objects;

/**
 * An event send from reader to enumerator to indicate the splits of the partition buckets have been
 * removed by the reader. It contains the table buckets that have been removed from the reader.
 */
public class PartitionBucketsUnsubscribedEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final Collection<TableBucket> removedTableBuckets;

    public PartitionBucketsUnsubscribedEvent(Collection<TableBucket> removedTableBuckets) {
        this.removedTableBuckets = removedTableBuckets;
    }

    public Collection<TableBucket> getRemovedTableBuckets() {
        return removedTableBuckets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionBucketsUnsubscribedEvent that = (PartitionBucketsUnsubscribedEvent) o;
        return Objects.equals(removedTableBuckets, that.removedTableBuckets);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(removedTableBuckets);
    }

    @Override
    public String toString() {
        return "PartitionBucketsRemovedEvent{" + "removedTableBuckets=" + removedTableBuckets + '}';
    }
}
