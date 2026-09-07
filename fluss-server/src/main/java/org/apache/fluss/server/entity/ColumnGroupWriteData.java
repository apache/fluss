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

package org.apache.fluss.server.entity;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.record.MemoryLogRecords;

/** The column-group rows to append for one bucket (FIP-45 produce log columns). */
@Internal
public final class ColumnGroupWriteData {
    private final long firstSourceOffset;
    private final MemoryLogRecords records;

    public ColumnGroupWriteData(long firstSourceOffset, MemoryLogRecords records) {
        this.firstSourceOffset = firstSourceOffset;
        this.records = records;
    }

    /** The base-log offset filled by the first row of {@link #getRecords()}. */
    public long getFirstSourceOffset() {
        return firstSourceOffset;
    }

    public MemoryLogRecords getRecords() {
        return records;
    }
}
