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

package org.apache.fluss.flink.tiering.event;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Objects;

/**
 * SourceEvent used to notify TieringSourceReader that a table has reached the maximum tiering
 * duration and should be force completed.
 */
public class TieringReachMaxDurationEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final long tableId;

    public TieringReachMaxDurationEvent(long tableId) {
        this.tableId = tableId;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TieringReachMaxDurationEvent)) {
            return false;
        }
        TieringReachMaxDurationEvent that = (TieringReachMaxDurationEvent) o;
        return tableId == that.tableId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableId);
    }
}
