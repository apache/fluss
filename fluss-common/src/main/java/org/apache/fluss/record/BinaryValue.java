/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.record;

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.ValueEncoder;

import java.util.Objects;

/** A value of key-value pair that contains schema id and binary row. */
public class BinaryValue {

    public final short schemaId;
    public final BinaryRow row;
    private final long valueTimestampMs;
    private final boolean hasValueTimestamp;

    public BinaryValue(short schemaId, BinaryRow row) {
        this.schemaId = schemaId;
        this.row = row;
        this.valueTimestampMs = 0L;
        this.hasValueTimestamp = false;
    }

    public BinaryValue(short schemaId, long valueTimestampMs, BinaryRow row) {
        this.schemaId = schemaId;
        this.row = row;
        this.valueTimestampMs = valueTimestampMs;
        this.hasValueTimestamp = true;
    }

    /** Returns whether this value carries an internal value timestamp prefix. */
    public boolean hasValueTimestamp() {
        return hasValueTimestamp;
    }

    /** Returns the internal value timestamp in milliseconds. */
    public long getValueTimestampMs() {
        return valueTimestampMs;
    }

    /** Returns a value with a different row while preserving the value-layout metadata. */
    public BinaryValue withRow(short schemaId, BinaryRow row) {
        return hasValueTimestamp
                ? new BinaryValue(schemaId, valueTimestampMs, row)
                : new BinaryValue(schemaId, row);
    }

    /**
     * Encode the value (consisted of {@code row} with a {@code schemaId}) to a byte array value to
     * be expected persisted to kv store.
     */
    public byte[] encodeValue() {
        return hasValueTimestamp
                ? ValueEncoder.encodeValue(schemaId, valueTimestampMs, row)
                : ValueEncoder.encodeValue(schemaId, row);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BinaryValue that = (BinaryValue) o;
        return schemaId == that.schemaId
                && valueTimestampMs == that.valueTimestampMs
                && hasValueTimestamp == that.hasValueTimestamp
                && Objects.equals(row, that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId, row, valueTimestampMs, hasValueTimestamp);
    }

    @Override
    public String toString() {
        return "BinaryValue{"
                + "schemaId="
                + schemaId
                + ", row="
                + row
                + ", valueTimestampMs="
                + valueTimestampMs
                + ", hasValueTimestamp="
                + hasValueTimestamp
                + '}';
    }
}
