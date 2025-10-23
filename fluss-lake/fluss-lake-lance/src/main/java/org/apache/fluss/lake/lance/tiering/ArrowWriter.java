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

package org.apache.fluss.lake.lance.tiering;

import org.apache.fluss.lake.lance.utils.LanceArrowUtils;
import org.apache.fluss.lake.lance.writers.ArrowFieldWriter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/** An Arrow writer for {@link InternalRow}. */
public class ArrowWriter {
    private final VectorSchemaRoot root;

    private final ArrowFieldWriter<InternalRow>[] fieldWriters;

    private int recordsCount;

    /**
     * Writer which serializes the Fluss rows to Arrow record batches.
     *
     * @param fieldWriters An array of writers which are responsible for the serialization of each
     *     column of the rows
     * @param root Container that holds a set of vectors for the rows
     */
    public ArrowWriter(ArrowFieldWriter<InternalRow>[] fieldWriters, VectorSchemaRoot root) {
        this.fieldWriters = fieldWriters;
        this.root = root;
    }

    public static ArrowWriter create(VectorSchemaRoot root, RowType rowType) {
        ArrowFieldWriter<InternalRow>[] fieldWriters =
                new ArrowFieldWriter[root.getFieldVectors().size()];
        for (int i = 0; i < fieldWriters.length; i++) {
            FieldVector fieldVector = root.getVector(i);

            fieldWriters[i] =
                    LanceArrowUtils.createArrowFieldWriter(fieldVector, rowType.getTypeAt(i));
        }
        return new ArrowWriter(fieldWriters, root);
    }

    /** Writes the specified row which is serialized into Arrow format. */
    public void writeRow(InternalRow row) {
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(row, i, true);
        }
        recordsCount++;
    }

    public void finish() {
        root.setRowCount(recordsCount);
        for (ArrowFieldWriter<InternalRow> fieldWriter : fieldWriters) {
            fieldWriter.finish();
        }
    }
}
