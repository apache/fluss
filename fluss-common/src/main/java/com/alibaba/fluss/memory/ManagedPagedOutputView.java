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
import java.util.ArrayList;
import java.util.List;

/**
 * A managed {@link AbstractPagedOutputView}, whose {@link MemorySegment} is managed by the {@link
 * MemorySegmentPool}.
 */
public class ManagedPagedOutputView extends AbstractPagedOutputView {
    private final MemorySegmentPool segmentPool;
    private final List<MemorySegment> pooledSegments;

    public ManagedPagedOutputView(MemorySegmentPool segmentPool) throws IOException {
        super(segmentPool.nextSegment(), segmentPool.pageSize());
        this.segmentPool = segmentPool;
        this.pooledSegments = new ArrayList<>();
        this.pooledSegments.add(getCurrentSegment());
    }

    @Override
    protected MemorySegment nextSegment() throws IOException {
        MemorySegment segment = segmentPool.nextSegment();
        pooledSegments.add(segment);
        return segment;
    }

    @Override
    public List<MemorySegment> allocatedPooledSegments() {
        return pooledSegments;
    }
}
