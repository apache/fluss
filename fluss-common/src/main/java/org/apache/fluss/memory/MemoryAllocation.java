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

package org.apache.fluss.memory;

import org.apache.fluss.annotation.Internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Pages owned by one operation and returned together on close. Use with try-with-resources so
 * failed or cancelled operations release their pages. An allocation has a single allocating thread;
 * closing it must not race with that thread or with users of its pages.
 */
@Internal
public class MemoryAllocation implements MemorySegmentPool, AutoCloseable {

    private final MemorySegmentPool pool;
    protected final List<MemorySegment> pages = new ArrayList<>();
    protected boolean closed;

    MemoryAllocation(MemorySegmentPool pool) {
        this.pool = pool;
    }

    @Override
    public MemorySegment nextSegment() throws IOException {
        return allocatePages(1).get(0);
    }

    @Override
    public List<MemorySegment> allocatePages(int required) throws IOException {
        checkState(!closed, "Memory allocation is closed.");
        List<MemorySegment> allocated = pool.allocatePages(required);
        pages.addAll(allocated);
        return allocated;
    }

    @Override
    public void returnPage(MemorySegment segment) {
        returnAll(Collections.singletonList(segment));
    }

    @Override
    public void returnAll(List<MemorySegment> memory) {
        checkState(!closed, "Memory allocation is closed.");
        if (memory.size() == pages.size()) {
            pool.returnAll(memory);
            pages.clear();
        } else {
            // The usual batch cleanup returns all pages. Individual returns are uncommon.
            for (MemorySegment page : memory) {
                checkState(pages.remove(page), "Page does not belong to this allocation.");
                pool.returnPage(page);
            }
        }
    }

    @Override
    public int pageSize() {
        return pool.pageSize();
    }

    @Override
    public long totalSize() {
        return pool.totalSize();
    }

    @Override
    public int freePages() {
        return pool.freePages();
    }

    @Override
    public long availableMemory() {
        return pool.availableMemory();
    }

    @Override
    public void close() {
        if (!closed) {
            pool.returnAll(pages);
            pages.clear();
            closed = true;
        }
    }
}
