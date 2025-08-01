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

package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.lake.lance.utils.LanceArrowUtils;
import com.alibaba.fluss.lake.lance.writers.ArrowFieldWriter;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.types.RowType;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/** An Arrow writer for InternalRow. */
public class ArrowWriter {
    /**
     * An array of writers which are responsible for the serialization of each column of the rows.
     */
    private final ArrowFieldWriter<InternalRow>[] fieldWriters;

    private static final int LAKE_LANCE_SYSTEM_COLUMNS = 3;
    private int recordsCount;
    private VectorSchemaRoot root;

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
    public void writeRow(InternalRow row, int bucket, long offset, long timestamp) {
        int i;
        for (i = 0; i < fieldWriters.length - LAKE_LANCE_SYSTEM_COLUMNS; i++) {
            fieldWriters[i].write(row, i, true);
        }
        fieldWriters[i].write(GenericRow.of(bucket), 0, true);
        i++;
        fieldWriters[i].write(GenericRow.of(offset), 0, true);
        i++;
        fieldWriters[i].write(GenericRow.of(TimestampLtz.fromEpochMillis(timestamp)), 0, true);
        recordsCount++;
    }

    public void finish() {
        root.setRowCount(recordsCount);
        for (ArrowFieldWriter<InternalRow> fieldWriter : fieldWriters) {
            fieldWriter.finish();
        }
    }
}
