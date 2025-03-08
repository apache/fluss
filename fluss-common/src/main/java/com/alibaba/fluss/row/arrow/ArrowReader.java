/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.row.arrow;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.columnar.ColumnVector;
import com.alibaba.fluss.row.columnar.ColumnarRow;
import com.alibaba.fluss.row.columnar.VectorizedColumnBatch;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** {@link ArrowReader} which read the underlying Arrow format data as {@link InternalRow}. */
@Internal
public class ArrowReader {

    /**
     * The arrow root which holds vector resources and should be released when the reader is closed.
     */
    private final VectorSchemaRoot root;

    /**
     * An array of vectors which are responsible for the deserialization of each column of the rows.
     */
    private final ColumnVector[] columnVectors;

    private final int rowCount;

    public ArrowReader(VectorSchemaRoot root, ColumnVector[] columnVectors) {
        this.root = root;
        this.columnVectors = checkNotNull(columnVectors);
        this.rowCount = root.getRowCount();
    }

    public int getRowCount() {
        return rowCount;
    }

    /** Read the {@link InternalRow} from underlying Arrow format data. */
    public ColumnarRow read(int rowId) {
        return new ColumnarRow(new VectorizedColumnBatch(columnVectors), rowId);
    }

    public void close() {
        root.close();
    }
}
