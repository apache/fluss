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

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An {@link AutoIncrementUpdater} implementation that assigns auto-increment values to a specific
 * column based on a fixed schema. It is bound to a particular schema version and assumes the
 * auto-increment column position remains constant within that schema.
 *
 * <p>This class is not thread-safe and is intended to be used within a single-threaded execution
 * context.
 */
@NotThreadSafe
public class PerSchemaAutoIncrementUpdater
        implements AutoIncrementUpdater, ValueRowEncoder.FieldOverrideProvider {
    private final ValueRowEncoder rowEncoder;
    private final int targetColumnIdx;
    private final SequenceGenerator sequenceGenerator;
    private final short schemaId;
    private final boolean requireInteger;

    // Cached auto-increment value for current encoding operation
    private long currentAutoIncrementValue;

    public PerSchemaAutoIncrementUpdater(
            KvFormat kvFormat,
            short schemaId,
            Schema schema,
            int autoIncrementColumnId,
            SequenceGenerator sequenceGenerator) {
        DataType[] fieldDataTypes = schema.getRowType().getChildren().toArray(new DataType[0]);

        this.sequenceGenerator = sequenceGenerator;
        this.schemaId = schemaId;
        this.targetColumnIdx = schema.getColumnIds().indexOf(autoIncrementColumnId);
        if (targetColumnIdx == -1) {
            throw new IllegalStateException(
                    String.format(
                            "Auto-increment column ID %d not found in schema columns: %s",
                            autoIncrementColumnId, schema.getColumnIds()));
        }
        this.requireInteger = fieldDataTypes[targetColumnIdx].is(DataTypeRoot.INTEGER);
        this.rowEncoder = new ValueRowEncoder(kvFormat, fieldDataTypes);
    }

    @Override
    public BinaryValue applyAutoIncrement(BinaryValue rowValue) {
        return new BinaryValue(schemaId, encodeRow(rowValue.row));
    }

    @Override
    public BinaryRow encodeRow(InternalRow row) {
        // Generate new auto-increment value for this encoding
        currentAutoIncrementValue = sequenceGenerator.nextVal();
        return rowEncoder.encode(row, this);
    }

    @Override
    public boolean hasAutoIncrement() {
        return true;
    }

    // FieldOverrideProvider implementation
    @Override
    public boolean hasOverride(int fieldIndex) {
        return fieldIndex == targetColumnIdx;
    }

    @Override
    public Object getOverrideValue(int fieldIndex) {
        if (fieldIndex == targetColumnIdx) {
            return requireInteger ? (int) currentAutoIncrementValue : currentAutoIncrementValue;
        }
        throw new IllegalArgumentException("No override value for field index: " + fieldIndex);
    }
}
