/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.serializer.InternalArraySerializer;

import java.io.Closeable;

/**
 * Writer to write a composite data format, like row, array. 1. Invoke {@link #reset()}. 2. Write
 * each field by writeXX or setNullAt. (Same field can not be written repeatedly.) 3. Invoke {@link
 * #complete()}.
 */
public interface BinaryWriter extends Closeable {

    /** Reset writer to prepare next write. */
    void reset();

    /** Set null to this field. */
    void setNullAt(int pos);

    void writeBoolean(boolean value);

    void writeByte(byte value);

    void writeBytes(byte[] value);

    void writeChar(BinaryString value, int length);

    void writeString(BinaryString value);

    void writeShort(short value);

    void writeInt(int value);

    void writeLong(long value);

    void writeFloat(float value);

    void writeDouble(double value);

    void writeBinary(byte[] bytes, int length);

    void writeDecimal(Decimal value, int precision);

    void writeTimestampNtz(TimestampNtz value, int precision);

    void writeTimestampLtz(TimestampLtz value, int precision);

    void writeArray(InternalArray value, InternalArraySerializer serializer);

    // TODO: Map and Row write methods will be added in Issue #1973 and #1974
    // void writeMap(InternalMap value, InternalMapSerializer serializer);
    // void writeRow(InternalRow value, InternalRowSerializer serializer);

    /** Finally, complete write to set real size to binary. */
    void complete();

    MemorySegment segment();

    int position();

    // --------------------------------------------------------------------------------------------

}
