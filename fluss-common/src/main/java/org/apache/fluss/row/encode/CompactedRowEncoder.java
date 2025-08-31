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
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.types.DataType;

/**
 * A {@link RowEncoder} for {@link CompactedRow}.
 *
 * @since 0.2
 */
@PublicEvolving
public class CompactedRowEncoder implements RowEncoder {

    private final DataType[] fieldDataTypes;
    private final CompactedRowWriter writer;
    private final CompactedRowWriter.FieldWriter[] fieldWriters;
    private final CompactedRowDeserializer compactedRowDeserializer;

    public CompactedRowEncoder(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
        // writer for row's fields
        writer = new CompactedRowWriter(fieldDataTypes.length);
        fieldWriters = new CompactedRowWriter.FieldWriter[fieldDataTypes.length];
        for (int i = 0; i < fieldDataTypes.length; i++) {
            fieldWriters[i] = CompactedRowWriter.createFieldWriter(fieldDataTypes[i]);
        }
        this.compactedRowDeserializer = new CompactedRowDeserializer(fieldDataTypes);
    }

    @Override
    public void startNewRow() {
        writer.reset();
    }

    @Override
    public void encodeField(int pos, Object value) {
        fieldWriters[pos].writeField(writer, pos, value);
    }

    @Override
    public CompactedRow finishRow() {
        CompactedRow row = new CompactedRow(fieldDataTypes.length, compactedRowDeserializer);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }
}
