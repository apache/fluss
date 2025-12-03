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

    public BinaryValue(short schemaId, BinaryRow row) {
        this.schemaId = schemaId;
        this.row = row;
    }

    /**
     * Encode the value (consisted of {@code row} with a {@code schemaId}) to a byte array value to
     * be expected persisted to kv store.
     */
    public byte[] encodeValue() {
        return ValueEncoder.encodeValue(schemaId, row);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BinaryValue that = (BinaryValue) o;
        return schemaId == that.schemaId && Objects.equals(row, that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId, row);
    }

    @Override
    public String toString() {
        return "BinaryValue{" + "schemaId=" + schemaId + ", row=" + row + '}';
    }
}
