/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.memory;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * A special implementation of {@link AbstractPagedOutputView} that input is a pre-allocated {@link
 * MemorySegment} list and one {@link MemorySegmentPool}. It will not allocate new memory segment
 * from {@link MemorySegmentPool} until the pre-allocated list is exhausted.
 *
 * <p>Note: If you use this outputView, you should make sure that the pre-allocated memory segments
 * deallocated by yourself if the pre-allocated buffer is not being fully used. Like
 * ArrowLogWriteBatch#memorySegments().
 */
public class PreAllocatedManagedPagedOutputView extends AbstractPagedOutputView {
    private final List<MemorySegment> memorySegments;
    private final MemorySegmentPool segmentPool;
    private int currentSegmentIndex;

    public PreAllocatedManagedPagedOutputView(
            List<MemorySegment> memorySegments, MemorySegmentPool segmentPool) {
        super(memorySegments.get(0), segmentPool.pageSize());
        this.memorySegments = memorySegments;
        this.segmentPool = segmentPool;
        currentSegmentIndex = 1;
    }

    @Override
    protected MemorySegment nextSegment() throws IOException {
        if (currentSegmentIndex < memorySegments.size()) {
            return memorySegments.get(currentSegmentIndex++);
        } else {
            MemorySegment segment = segmentPool.nextSegment(waitingSegment);
            if (segment == null) {
                throw new EOFException("No more memory segments available.");
            } else {
                return segment;
            }
        }
    }

    @Override
    protected void deallocate(List<MemorySegment> segments) {
        segmentPool.returnAll(segments);
    }
}
