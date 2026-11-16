/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.tiering.event;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Objects;

/**
 * SourceEvent used to tell a TieringSourceReader the backlog only the enumerator knows about: the
 * log records of splits not yet assigned to any reader.
 *
 * <p>Autoscalers sum {@code pendingRecords} over the source's subtasks and don't read enumerator
 * metrics, so the enumerator sends this backlog to a single reader, which adds it to its own.
 */
public class TieringBacklogEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final long backlog;

    public TieringBacklogEvent(long backlog) {
        this.backlog = backlog;
    }

    public long getBacklog() {
        return backlog;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TieringBacklogEvent)) {
            return false;
        }
        TieringBacklogEvent that = (TieringBacklogEvent) o;
        return backlog == that.backlog;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(backlog);
    }

    @Override
    public String toString() {
        return "TieringBacklogEvent{" + "backlog=" + backlog + '}';
    }
}
