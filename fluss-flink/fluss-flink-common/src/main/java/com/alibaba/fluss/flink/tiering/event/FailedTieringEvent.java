/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.event;

import org.apache.flink.api.connector.source.SourceEvent;

/** SourceEvent used to represent a Fluss table is failed during tiering. */
public class FailedTieringEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final long tableId;

    private final String failReason;

    public FailedTieringEvent(long tableId, String failReason) {
        this.tableId = tableId;
        this.failReason = failReason;
    }

    public long getTableId() {
        return tableId;
    }

    public String failReason() {
        return failReason;
    }
}
