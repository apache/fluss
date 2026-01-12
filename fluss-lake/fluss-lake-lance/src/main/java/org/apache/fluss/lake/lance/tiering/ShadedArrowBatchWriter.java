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

import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.writers.ArrowFieldWriter;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

/**
 * Batch writer using shaded Arrow and ArrowFieldWriter from fluss-common.
 *
 * <p>This class uses shaded Arrow vectors and ArrowFieldWriters to write Fluss InternalRows. It can
 * later be converted to non-shaded Arrow format for Lance compatibility.
 */
public class ShadedArrowBatchWriter implements AutoCloseable {
    private final VectorSchemaRoot shadedRoot;
    private final ArrowFieldWriter[] fieldWriters;
    private int recordsCount;

    public ShadedArrowBatchWriter(BufferAllocator shadedAllocator, RowType rowType) {
        this.shadedRoot =
                VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), shadedAllocator);
        this.shadedRoot.allocateNew();
        this.fieldWriters = new ArrowFieldWriter[rowType.getFieldCount()];

        for (int i = 0; i < fieldWriters.length; i++) {
            FieldVector fieldVector = shadedRoot.getVector(i);
            fieldWriters[i] = ArrowUtils.createArrowFieldWriter(fieldVector, rowType.getTypeAt(i));
        }
        this.recordsCount = 0;
    }

    public void writeRow(InternalRow row) {
        boolean handleSafe = recordsCount >= 1024;
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(recordsCount, row, i, handleSafe);
        }
        recordsCount++;
    }

    public void finish() {
        shadedRoot.setRowCount(recordsCount);
    }

    public void reset() {
        recordsCount = 0;
        for (ArrowFieldWriter fieldWriter : fieldWriters) {
            fieldWriter.reset();
        }
        shadedRoot.allocateNew();
    }

    public int getRecordsCount() {
        return recordsCount;
    }

    public VectorSchemaRoot getShadedRoot() {
        return shadedRoot;
    }

    @Override
    public void close() {
        if (shadedRoot != null) {
            shadedRoot.close();
        }
    }
}
