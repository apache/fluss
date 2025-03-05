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

import java.util.Collections;
import java.util.List;

/** A managed {@link AbstractPagedOutputView}, whose {@link MemorySegment} is un-managed. */
public class UnmanagedPagedOutputView extends AbstractPagedOutputView {

    public UnmanagedPagedOutputView(int pageSize) {
        super(MemorySegment.allocateHeapMemory(pageSize), pageSize);
    }

    @Override
    protected MemorySegment nextSegment() {
        return MemorySegment.allocateHeapMemory(pageSize);
    }

    @Override
    public List<MemorySegment> allocatedPooledSegments() {
        return Collections.emptyList();
    }
}
