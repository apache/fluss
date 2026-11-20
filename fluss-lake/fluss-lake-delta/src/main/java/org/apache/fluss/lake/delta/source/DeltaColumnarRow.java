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

package org.apache.fluss.lake.delta.source;

import org.apache.fluss.row.InternalRow;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;

/**
 * A Fluss {@link InternalRow} view of one row of Delta Kernel column vectors, either the columns of
 * a batch or the fields of a struct. The row id can be moved to reuse the view across rows.
 */
class DeltaColumnarRow extends DeltaVectorGetters implements InternalRow {

    private final ColumnVector[] fields;
    private int rowId;

    DeltaColumnarRow(ColumnVector[] fields) {
        this.fields = fields;
    }

    /** Creates a view over the columns of the batch, positioned before the first row. */
    static DeltaColumnarRow of(ColumnarBatch batch) {
        ColumnVector[] fields = new ColumnVector[batch.getSchema().length()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = batch.getColumnVector(i);
        }
        return new DeltaColumnarRow(fields);
    }

    void setRowId(int rowId) {
        this.rowId = rowId;
    }

    @Override
    public int getFieldCount() {
        return fields.length;
    }

    @Override
    ColumnVector vectorAt(int pos) {
        return fields[pos];
    }

    @Override
    int rowIdAt(int pos) {
        return rowId;
    }
}
