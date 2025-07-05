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

import java.io.IOException;

/**
 * A value record is a tuple consisting of a value row and a schema id for the row.
 *
 * <p>The schema is as follows:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>SchemaId => int16
 *   <li>Value => {@link BinaryRow}
 * </ul>
 *
 * @since 0.3
 */
public class DefaultValueRecord implements ValueRecord {

    static final int LENGTH_OFFSET = 0;
    static final int LENGTH_LENGTH = 4;
    static final int SCHEMA_ID_OFFSET = LENGTH_LENGTH;
    static final int SCHEMA_ID_LENGTH = 2;
    static final int VALUE_OFFSET = SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;

    private final short schemaId;
    private final BinaryRow row;

    public DefaultValueRecord(short schemaId, BinaryRow row) {
        this.schemaId = schemaId;
        this.row = row;
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
        return row.getSizeInBytes() + LENGTH_LENGTH + SCHEMA_ID_LENGTH;
    }

    public int writeTo(MemorySegmentOutputView outputView) throws IOException {
        int sizeInBytes = getSizeInBytes();
        outputView.writeInt(sizeInBytes - LENGTH_LENGTH);
        outputView.writeShort(schemaId);
        outputView.write(row.getSegments()[0], row.getOffset(), row.getSizeInBytes());
        return sizeInBytes;
    }

    public static DefaultValueRecord readFrom(
            MemorySegment segment, int position, ValueRecordBatch.ReadContext readContext) {
        int sizeInBytesWithoutLength = segment.getInt(position + LENGTH_OFFSET);
        short schemaId = segment.getShort(position + SCHEMA_ID_OFFSET);
        RowDecoder decoder = readContext.getRowDecoder(schemaId);
        BinaryRow value =
                decoder.decode(
                        segment,
                        position + VALUE_OFFSET,
                        sizeInBytesWithoutLength - SCHEMA_ID_LENGTH);
        return new DefaultValueRecord(schemaId, value);
    }
}
