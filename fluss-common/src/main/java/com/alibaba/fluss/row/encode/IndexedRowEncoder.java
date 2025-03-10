/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.row.encode;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

/**
 * A {@link RowEncoder} for {@link IndexedRow}.
 *
 * @since 0.2
 */
@PublicEvolving
public class IndexedRowEncoder implements RowEncoder {

    private final DataType[] fieldDataTypes;
    private final IndexedRowWriter rowWriter;
    private final IndexedRowWriter.FieldWriter[] fieldWriters;

    public IndexedRowEncoder(RowType rowType) {
        this(rowType.getChildren().toArray(new DataType[0]));
    }

    public IndexedRowEncoder(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
        // create writer.
        this.fieldWriters = new IndexedRowWriter.FieldWriter[fieldDataTypes.length];
        this.rowWriter = new IndexedRowWriter(fieldDataTypes);
        for (int i = 0; i < fieldDataTypes.length; i++) {
            fieldWriters[i] = IndexedRowWriter.createFieldWriter(fieldDataTypes[i]);
        }
    }

    @Override
    public void startNewRow() {
        rowWriter.reset();
    }

    @Override
    public void encodeField(int pos, Object value) {
        fieldWriters[pos].writeField(rowWriter, pos, value);
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
