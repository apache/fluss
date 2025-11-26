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

package org.apache.fluss.row.encode;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.BinaryWriter;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;

/**
 * A {@link RowEncoder} for {@link IndexedRow}.
 *
 * @since 0.2
 */
@PublicEvolving
public class IndexedRowEncoder implements RowEncoder {

    private final DataType[] fieldDataTypes;
    private final IndexedRowWriter rowWriter;
    private final BinaryWriter.ValueWriter[] fieldWriters;

    public IndexedRowEncoder(RowType rowType) {
        this(rowType.getChildren().toArray(new DataType[0]));
    }

    public IndexedRowEncoder(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
        // create writer.
        this.fieldWriters = new BinaryWriter.ValueWriter[fieldDataTypes.length];
        this.rowWriter = new IndexedRowWriter(fieldDataTypes);
        for (int i = 0; i < fieldDataTypes.length; i++) {
            fieldWriters[i] = BinaryWriter.createValueWriter(fieldDataTypes[i], INDEXED);
        }
    }

    @Override
    public void startNewRow() {
        rowWriter.reset();
    }

    @Override
    public void encodeField(int pos, Object value) {
        fieldWriters[pos].writeValue(rowWriter, pos, value);
    }

    @Override
    public IndexedRow finishRow() {
        IndexedRow row = new IndexedRow(fieldDataTypes);
        row.pointTo(rowWriter.segment(), 0, rowWriter.position());
        return row;
    }

    @Override
    public void close() throws Exception {
        rowWriter.close();
    }
}
