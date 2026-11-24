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

package org.apache.fluss.row.encode.iceberg;

import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KeyEncodingRecycler;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * An implementation of {@link KeyEncoder} to follow Iceberg's encoding strategy.
 *
 * <p>One instance may be shared by concurrent callers: the mutable writer is borrowed per {@link
 * #encodeKey(InternalRow)} call from a {@link KeyEncodingRecycler} instead of being held as state.
 */
@ThreadSafe
public class IcebergKeyEncoder implements KeyEncoder {

    private final InternalRow.FieldGetter fieldGetter;

    private final IcebergBinaryRowWriter.FieldWriter fieldEncoder;

    private final KeyEncodingRecycler<IcebergBinaryRowWriter> keyWriterRecycler;

    public IcebergKeyEncoder(RowType rowType, List<String> keys) {
        final int keyCount = keys.size();

        // Validate single key field requirement as per FIP
        checkArgument(
                keyCount == 1,
                "Key fields must have exactly one field for iceberg format, but got: %s",
                keys);

        int keyIndex = rowType.getFieldIndex(keys.get(0));
        DataType type = rowType.getTypeAt(keyIndex);
        fieldGetter = InternalRow.createFieldGetter(type, keyIndex);
        fieldEncoder = IcebergBinaryRowWriter.createFieldWriter(type);
        keyWriterRecycler =
                new KeyEncodingRecycler<>(
                        () -> new IcebergBinaryRowWriter(keyCount),
                        IcebergBinaryRowWriter::reset,
                        IcebergBinaryRowWriter::capacity);
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        IcebergBinaryRowWriter icebergBinaryRowWriter = keyWriterRecycler.borrow();
        try {
            fieldEncoder.writeField(icebergBinaryRowWriter, fieldGetter.getFieldOrNull(row));
            return icebergBinaryRowWriter.toBytes();
        } finally {
            keyWriterRecycler.recycle(icebergBinaryRowWriter);
        }
    }
}
