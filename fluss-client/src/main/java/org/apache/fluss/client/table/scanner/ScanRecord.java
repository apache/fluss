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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.record.ChangeType;

import java.util.Objects;

/** one scan record. */
@Internal
public class ScanRecord<T> {
    private static final long INVALID = -1L;

    private final long offset;
    private final long timestamp;
    private final ChangeType changeType;
    private final T value;

    public ScanRecord(T value) {
        this(INVALID, INVALID, ChangeType.INSERT, value);
    }

    public ScanRecord(long offset, long timestamp, ChangeType changeType, T value) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.changeType = changeType;
        this.value = value;
    }

    /** The position of this record in the corresponding fluss table bucket. */
    public long logOffset() {
        return offset;
    }

    public long timestamp() {
        return timestamp;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    /** Returns the carried record as InternalRow for backward compatibility. */
    public org.apache.fluss.row.InternalRow getRow() {
        return (org.apache.fluss.row.InternalRow) value;
    }

    /** Returns the carried record value. */
    public T getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScanRecord<?> that = (ScanRecord<?>) o;
        return offset == that.offset
                && changeType == that.changeType
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, changeType, value);
    }

    @Override
    public String toString() {
        return changeType.shortString() + value + "@" + offset;
    }
}