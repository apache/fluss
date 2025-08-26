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

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** A writer for {@link ChangeTypeVector}. */
public class ChangeTypeVectorWriter {

    private final MemorySegment segment;
    private final int capacity;
    private final int startPosition;
    private int recordsCount = 0;

    public ChangeTypeVectorWriter(MemorySegment segment, int startPosition) {
        checkArgument(segment.size() >= startPosition, "The start position is out of bound.");
        this.segment = segment;
        this.capacity = segment.size() - startPosition;
        this.startPosition = startPosition;
    }

    public void writeChangeType(ChangeType changeType) {
        if (recordsCount > capacity) {
            // TODO: support AbstractPagedOutputView to have extendable capacity
            throw new IllegalStateException("The change type vector is full.");
        }
        segment.put(startPosition + recordsCount, changeType.toByteValue());
        recordsCount++;
    }

    public int sizeInBytes() {
        return recordsCount;
    }
}
