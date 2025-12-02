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

package org.apache.fluss.row.arrow;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.columnar.ColumnVector;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.row.columnar.VectorizedColumnBatch;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** {@link ArrowReader} which read the underlying Arrow format data as {@link InternalRow}. */
@Internal
public class ArrowReader {

    /**
     * An array of vectors which are responsible for the deserialization of each column of the rows.
     */
    private final ColumnVector[] columnVectors;

    private final int rowCount;

    public ArrowReader(ColumnVector[] columnVectors, int rowCount) {
        this.columnVectors = checkNotNull(columnVectors);
        this.rowCount = rowCount;
    }

    public int getRowCount() {
        return rowCount;
    }

    /** Read the {@link InternalRow} from underlying Arrow format data. */
    public ColumnarRow read(int rowId) {
        return new ColumnarRow(new VectorizedColumnBatch(columnVectors), rowId);
    }
}
