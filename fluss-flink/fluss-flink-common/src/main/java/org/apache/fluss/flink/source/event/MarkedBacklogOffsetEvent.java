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

package org.apache.fluss.flink.source.event;

import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Event to signal the marked backlog offsets for specific table buckets. This event is sent from
 * the source enumerator to the source reader to indicate the marked backlog offsets for each
 * bucket.
 */
public class MarkedBacklogOffsetEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    /** Mapping from TableBucket to its marked backlog offset. */
    private final Map<TableBucket, Long> markedBacklogOffsets;

    public MarkedBacklogOffsetEvent(Map<TableBucket, Long> markedBacklogOffsets) {
        this.markedBacklogOffsets =
                markedBacklogOffsets != null
                        ? new HashMap<>(markedBacklogOffsets)
                        : Collections.emptyMap();
    }

    public Long getMarkedBacklogOffset(TableBucket tableBucket) {
        return markedBacklogOffsets.get(tableBucket);
    }

    public Map<TableBucket, Long> getMarkedBacklogOffsets() {
        return Collections.unmodifiableMap(markedBacklogOffsets);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MarkedBacklogOffsetEvent that = (MarkedBacklogOffsetEvent) o;
        return Objects.equals(markedBacklogOffsets, that.markedBacklogOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(markedBacklogOffsets);
    }

    @Override
    public String toString() {
        return "MarkedBacklogOffsetEvent{" + "markedBacklogOffsets=" + markedBacklogOffsets + '}';
    }
}
