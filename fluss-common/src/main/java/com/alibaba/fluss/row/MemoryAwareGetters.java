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

package com.alibaba.fluss.row;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.memory.MemorySegment;

/** Provides memory ({@link MemorySegment}) related getters. */
@Internal
public interface MemoryAwareGetters {

    /** Gets the underlying {@link MemorySegment}s this binary format spans. */
    // TODO: maybe we only need a single MemorySegment.
    MemorySegment[] getSegments();

    /** Gets the start offset of this binary data in the {@link MemorySegment}s. */
    int getOffset();

    /** Gets the size in bytes of this binary data. */
    int getSizeInBytes();
}
