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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;

import java.util.Objects;

/**
 * A record produced by a table scanner which contains a typed value.
 *
 * @param <T> The type of the value.
 */
@PublicEvolving
public class TypedScanRecord<T> {

    private final ScanRecord scanRecord;
    private final T value;

    public TypedScanRecord(ScanRecord scanRecord, T value) {
        this.scanRecord = scanRecord;
        this.value = value;
    }

    /** The position of this record in the corresponding fluss table bucket. */
    public long logOffset() {
        return scanRecord.logOffset();
    }

    /** The timestamp of this record. */
    public long timestamp() {
        return scanRecord.timestamp();
    }

    /** The change type of this record. */
    public ChangeType getChangeType() {
        return scanRecord.getChangeType();
    }

    /** Returns the record value. */
    public T getValue() {
        return value;
    }

    /** Returns the internal row of this record. */
    public InternalRow getRow() {
        return scanRecord.getRow();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypedScanRecord<?> that = (TypedScanRecord<?>) o;
        return Objects.equals(scanRecord, that.scanRecord) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scanRecord, value);
    }

    @Override
    public String toString() {
        return scanRecord.getChangeType().shortString() + value + "@" + scanRecord.logOffset();
    }
}
