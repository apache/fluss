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

package org.apache.fluss.record;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KvValueLayout;

import java.io.IOException;

/**
 * A value record is a tuple consisting of a value row and a schema id for the row.
 *
 * <p>The schema is as follows:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>RawValue => schema id and {@link BinaryRow}
 * </ul>
 *
 * @since 0.3
 */
public class DefaultValueRecord implements ValueRecord {

    static final int LENGTH_OFFSET = 0;
    static final int LENGTH_LENGTH = 4;

    private final short schemaId;
    private final BinaryRow row;
    private final int sizeInBytes;

    public DefaultValueRecord(short schemaId, BinaryRow row) {
        this(
                schemaId,
                row,
                LENGTH_LENGTH + KvValueLayout.PLAIN.rowPayloadOffset() + row.getSizeInBytes());
    }

    private DefaultValueRecord(short schemaId, BinaryRow row, int sizeInBytes) {
        this.schemaId = schemaId;
        this.row = row;
        this.sizeInBytes = sizeInBytes;
    }

    @Override
    public short schemaId() {
        return schemaId;
    }

    @Override
    public BinaryRow getRow() {
        return row;
    }

    @Override
    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public int writeTo(MemorySegmentOutputView outputView) throws IOException {
        KvValueLayout layout = KvValueLayout.PLAIN;
        int valueLength = layout.schemaIdLength() + row.getSizeInBytes();
        int sizeInBytes = LENGTH_LENGTH + valueLength;
        outputView.writeInt(valueLength);
        int valueOffset = outputView.getPosition();
        outputView.setPosition(valueOffset + layout.schemaIdLength());
        layout.writeSchemaId(outputView.getSharedBuffer(), valueOffset, schemaId);
        outputView.write(row.getSegments()[0], row.getOffset(), row.getSizeInBytes());
        return sizeInBytes;
    }

    public static DefaultValueRecord readFrom(
            MemorySegment segment, int position, ValueRecordBatch.ReadContext readContext) {
        int valueLength = segment.getInt(position + LENGTH_OFFSET);
        int valueOffset = position + LENGTH_LENGTH;
        KvValueLayout layout = KvValueLayout.PLAIN;
        short schemaId = layout.readSchemaId(segment, valueOffset);
        RowDecoder decoder = readContext.getRowDecoder(schemaId);
        BinaryRow value =
                decoder.decode(
                        segment,
                        valueOffset + layout.rowPayloadOffset(),
                        layout.rowPayloadLength(valueLength));
        return new DefaultValueRecord(schemaId, value, LENGTH_LENGTH + valueLength);
    }
}
