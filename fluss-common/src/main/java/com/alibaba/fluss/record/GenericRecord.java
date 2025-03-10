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

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.row.InternalRow;

/** A generic implementation of {@link LogRecord} which is backed by generic Java objects. */
@PublicEvolving
public class GenericRecord implements LogRecord {
    private final long logOffset;
    private final long timestamp;
    private final ChangeType changeType;
    private final InternalRow row;

    public GenericRecord(long logOffset, long timestamp, ChangeType changeType, InternalRow row) {
        this.logOffset = logOffset;
        this.timestamp = timestamp;
        this.changeType = changeType;
        this.row = row;
    }

    @Override
    public long logOffset() {
        return logOffset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public ChangeType getChangeType() {
        return changeType;
    }

    @Override
    public InternalRow getRow() {
        return row;
    }
}
