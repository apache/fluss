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

package org.apache.fluss.server.coordinator.event;

import javax.annotation.Nullable;

/**
 * Marker event emitted by {@code TableLifecycleThrottler} to drive coordinator-startup
 * reconciliation of stale tables/partitions whose {@code TableInfo} is already gone (i.e. a regular
 * {@code DropTableEvent} / {@code DropPartitionEvent} would NPE on the missing {@code TableInfo}).
 * It carries only identity (tableId + optional partitionId), never a {@link Runnable}, so the
 * dispatch target is determined by the event-thread handler and cannot be influenced by the
 * (potentially non-event-thread) submitter.
 *
 * <p>A {@code null} {@link #getPartitionId()} indicates a whole-table resume; otherwise the resume
 * targets the specific partition.
 *
 * <p>The event handler in {@code CoordinatorEventProcessor} routes this to {@code
 * TableManager#onDeleteTable} / {@code TableManager#onDeletePartition}, ensuring the
 * {@code @NotThreadSafe} {@code CoordinatorContext} is only ever mutated from the coordinator event
 * thread.
 */
public class ResumeDropEvent implements CoordinatorEvent {

    private final long tableId;
    @Nullable private final Long partitionId;

    private ResumeDropEvent(long tableId, @Nullable Long partitionId) {
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    public static ResumeDropEvent forTable(long tableId) {
        return new ResumeDropEvent(tableId, null);
    }

    public static ResumeDropEvent forPartition(long tableId, long partitionId) {
        return new ResumeDropEvent(tableId, partitionId);
    }

    public long getTableId() {
        return tableId;
    }

    /** Returns {@code null} when this resume targets the whole table. */
    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    @Override
    public String toString() {
        if (partitionId == null) {
            return "ResumeDropEvent{tableId=" + tableId + "}";
        }
        return "ResumeDropEvent{tableId=" + tableId + ", partitionId=" + partitionId + "}";
    }
}
