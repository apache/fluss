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

import java.util.ArrayList;
import java.util.List;

/** Testing pooled memory segment source. */
public class TestingMemorySegmentPool implements MemorySegmentPool {

    private final int pageSize;

    public TestingMemorySegmentPool(int pageSize) {
        this.pageSize = pageSize;
    }

    @Override
    public MemorySegment nextSegment() {
        return MemorySegment.wrap(new byte[pageSize]);
    }

    @Override
    public List<MemorySegment> allocatePages(int required) {
        List<MemorySegment> segments = new ArrayList<>(required);
        for (int i = 0; i < required; i++) {
            segments.add(nextSegment());
        }
        return segments;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public int totalSize() {
        return pageSize;
    }

    @Override
    public void returnPage(MemorySegment segment) {
        // do noting.
    }

    @Override
    public void returnAll(List<MemorySegment> memory) {
        // do noting.
    }

    @Override
    public int freePages() {
        return Integer.MAX_VALUE;
    }

    @Override
    public long availableMemory() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void close() {
        // do nothing.
    }
}
