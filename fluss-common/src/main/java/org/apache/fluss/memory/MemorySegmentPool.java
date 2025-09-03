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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** MemorySegment pool to hold pages in memory. */
@Internal
public interface MemorySegmentPool {

    /**
     * Get the page size of each page this pool holds.
     *
     * @return the page size
     */
    int pageSize();

    /**
     * Get the total size of this pool.
     *
     * @return the total size
     */
    long totalSize();

    /**
     * Gets the next memory segment. If no more segments are available, it returns null.
     *
     * @return The next memory segment, or null, if none is available.
     */
    @Nullable
    MemorySegment nextSegment() throws IOException;

    List<MemorySegment> allocatePages(int required) throws IOException;

    /**
     * Return one page back into this pool.
     *
     * @param segment the page which want to be returned.
     */
    void returnPage(MemorySegment segment);

    /**
     * Return all pages back into this pool.
     *
     * @param memory the pages which want to be returned.
     */
    void returnAll(List<MemorySegment> memory);

    /**
     * @return Free page number.
     */
    int freePages();

    /**
     * @return the available memory size in bytes.
     */
    long availableMemory();

    void close();
}
