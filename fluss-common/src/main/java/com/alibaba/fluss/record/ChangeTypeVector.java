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

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.row.columnar.ByteColumnVector;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** An on-memory change type vector. */
public class ChangeTypeVector implements ByteColumnVector {

    private final MemorySegment segment;
    private final int position;
    private final int recordCount;

    public ChangeTypeVector(MemorySegment segment, int position, int recordCount) {
        checkArgument(position >= 0, "position must be >= 0");
        checkArgument(recordCount >= 0, "recordCount must be >= 0");
        this.segment = segment;
        this.position = position;
        this.recordCount = recordCount;
    }

    @Override
    public byte getByte(int i) {
        checkArgument(i >= 0 && i < recordCount, "i must be in [0, %s), but is %s", recordCount, i);
        return segment.get(position + i);
    }

    /** Get the change type at i-th position. */
    public ChangeType getChangeType(int i) {
        return ChangeType.fromByteValue(getByte(i));
    }

    public int sizeInBytes() {
        return recordCount;
    }

    @Override
    public boolean isNullAt(int i) {
        // change type is never null
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (int i = 0; i < recordCount; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(getChangeType(i));
        }
        builder.append("]");
        return builder.toString();
    }
}
