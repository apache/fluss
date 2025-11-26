/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.row.encode;

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryWriter;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.types.DataType;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.ALIGNED;

/**
 * A {@link RowEncoder} for {@link AlignedRow}.
 *
 * @since 0.9
 */
public class AlignedRowEncoder implements RowEncoder {
    private final AlignedRow reuseRow;
    private final AlignedRowWriter reuseWriter;
    private final BinaryWriter.ValueWriter[] valueWriters;

    public AlignedRowEncoder(DataType[] fieldTypes) {
        this.reuseRow = new AlignedRow(fieldTypes.length);
        this.reuseWriter = new AlignedRowWriter(reuseRow);
        this.valueWriters = new BinaryWriter.ValueWriter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            valueWriters[i] = BinaryWriter.createValueWriter(fieldTypes[i], ALIGNED);
        }
    }

    @Override
    public void startNewRow() {
        reuseWriter.reset();
    }

    @Override
    public void encodeField(int pos, Object value) {
        valueWriters[pos].writeValue(reuseWriter, pos, value);
    }

    @Override
    public BinaryRow finishRow() {
        reuseWriter.complete();
        return reuseRow;
    }

    @Override
    public void close() throws Exception {
        // nothing to close
    }
}
