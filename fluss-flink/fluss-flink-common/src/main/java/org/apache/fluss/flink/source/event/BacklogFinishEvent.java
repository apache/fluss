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

import java.util.Objects;

/**
 * Event to signal that the backlog processing for a specific table bucket has been completed. This
 * event is sent from the source reader to the source enumerator to indicate that all historical
 * data (backlog) in the specified bucket has been processed.
 */
public class BacklogFinishEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    private final TableBucket tableBucket;

    public BacklogFinishEvent(TableBucket tableBucket) {
        this.tableBucket = Objects.requireNonNull(tableBucket, "tableBucket must not be null");
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BacklogFinishEvent that = (BacklogFinishEvent) o;
        return Objects.equals(tableBucket, that.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket);
    }

    @Override
    public String toString() {
        return "BacklogFinishEvent{" + "tableBucket=" + tableBucket + '}';
    }
}
