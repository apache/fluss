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

import java.io.IOException;
import java.util.List;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/**
 * A special implementation of {@link AbstractPagedOutputView} that pre-allocates a list of {@link
 * MemorySegment} from {@link MemorySegmentPool}. The {@link #nextSegment()} will apply memory from
 * the pre-allocated memory list instead of {@link MemorySegmentPool}, and if the pre-allocated list
 * is exhausted, it will apply unmanaged memory from heap. This makes the {@link
 * AbstractPagedOutputView} not blocked on memory allocation during writing.
 *
 * <p>Note: In order to reduce GC overhead, users should pre-allocate enough and appropriate memory
 * segments to avoid allocate from heap too much.
 */
public class PreAllocatedPagedOutputView extends AbstractPagedOutputView {
    private final List<MemorySegment> allocatedSegments;
    private final int preAllocatedSize;
    private int nextSegmentIndex;

    public PreAllocatedPagedOutputView(List<MemorySegment> allocatedSegments) {
        super(allocatedSegments.get(0), allocatedSegments.get(0).size());
        checkArgument(
                !allocatedSegments.isEmpty(),
                "The pre-allocated memory segments should not be empty.");
        this.allocatedSegments = allocatedSegments;
        this.preAllocatedSize = getPageSize() * allocatedSegments.size();
        this.nextSegmentIndex = 1;
    }

    @Override
    protected MemorySegment nextSegment() throws IOException {
        if (nextSegmentIndex < allocatedSegments.size()) {
            return allocatedSegments.get(nextSegmentIndex++);
        } else {
            // allocate from heap if the pre-allocated list is exhausted
            return MemorySegment.allocateHeapMemory(pageSize);
        }
    }

    @Override
    public List<MemorySegment> allocatedPooledSegments() {
        return allocatedSegments;
    }

    /** Get the pre-allocated size in bytes. */
    public int getPreAllocatedSize() {
        return preAllocatedSize;
    }
}
