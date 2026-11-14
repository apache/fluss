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

package org.apache.fluss.row.encode.paimon;

import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KeyEncodingRecycler;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

/**
 * An implementation of {@link KeyEncoder} to follow Paimon's encoding strategy.
 *
 * <p>One instance may be shared by concurrent callers: the mutable writer is borrowed per {@link
 * #encodeKey(InternalRow)} call from a {@link KeyEncodingRecycler} instead of being held as state.
 */
@ThreadSafe
public class PaimonKeyEncoder implements KeyEncoder {

    private final InternalRow.FieldGetter[] fieldGetters;

    private final PaimonBinaryRowWriter.FieldWriter[] fieldEncoders;

    private final KeyEncodingRecycler<PaimonBinaryRowWriter> keyWriterRecycler;

    public PaimonKeyEncoder(RowType rowType, List<String> keys) {
        final int keyCount = keys.size();
        // for get fields from fluss internal row
        fieldGetters = new InternalRow.FieldGetter[keyCount];
        // for encode fields into paimon
        fieldEncoders = new PaimonBinaryRowWriter.FieldWriter[keyCount];
        for (int i = 0; i < keyCount; i++) {
            int keyIndex = rowType.getFieldIndex(keys.get(i));
            DataType keyDataType = rowType.getTypeAt(keyIndex);
            fieldGetters[i] = InternalRow.createFieldGetter(keyDataType, keyIndex);
            fieldEncoders[i] = PaimonBinaryRowWriter.createFieldWriter(keyDataType);
        }

        keyWriterRecycler =
                new KeyEncodingRecycler<>(
                        () -> new PaimonBinaryRowWriter(keyCount),
                        PaimonBinaryRowWriter::reset,
                        PaimonBinaryRowWriter::capacity);
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        PaimonBinaryRowWriter paimonBinaryRowWriter = keyWriterRecycler.borrow();
        paimonBinaryRowWriter.reset();
        try {
            // always be RowKind.INSERT for bucketed row
            paimonBinaryRowWriter.writeChangeType(ChangeType.INSERT);
            // iterate all the fields of the row, and encode each field
            for (int i = 0; i < fieldGetters.length; i++) {
                fieldEncoders[i].writeField(
                        paimonBinaryRowWriter, i, fieldGetters[i].getFieldOrNull(row));
            }
            return paimonBinaryRowWriter.toBytes();
        } finally {
            keyWriterRecycler.recycle(paimonBinaryRowWriter);
        }
    }
}
