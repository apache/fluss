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

import org.apache.fluss.memory.AbstractPagedOutputView;

import java.io.IOException;
import java.util.Arrays;

/**
 * A writer for {@link ChangeTypeVector}.
 *
 * <p>The change types are buffered on heap, one byte per record, and copied into the batch when it
 * is built. The vector therefore is not bounded by the size of a memory segment and may span
 * several segments of the output view.
 */
public class ChangeTypeVectorWriter {

    private static final int INITIAL_CAPACITY = 256;

    private byte[] buffer = new byte[INITIAL_CAPACITY];
    private int recordsCount = 0;

    public void writeChangeType(ChangeType changeType) {
        if (recordsCount == buffer.length) {
            buffer = Arrays.copyOf(buffer, Math.multiplyExact(buffer.length, 2));
        }
        buffer[recordsCount++] = changeType.toByteValue();
    }

    /** Writes the vector at the current position of the given output view. */
    public void writeTo(AbstractPagedOutputView outputView) throws IOException {
        outputView.write(buffer, 0, recordsCount);
    }

    public int sizeInBytes() {
        return recordsCount;
    }
}
