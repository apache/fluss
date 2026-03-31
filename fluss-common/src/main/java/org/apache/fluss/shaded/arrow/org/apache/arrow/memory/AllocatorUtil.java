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

package org.apache.fluss.shaded.arrow.org.apache.arrow.memory;

import org.apache.fluss.record.FlussRoundingPolicy;

import static org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BaseAllocator.configBuilder;

/** Util to create UNSAFE_ALLOCATOR. */
public class AllocatorUtil {

    private AllocatorUtil() {}

    public static BufferAllocator createBufferAllocator(
            AllocationManager.Factory allocationManagerFactory) {
        return new RootAllocator(
                configBuilder()
                        .listener(AllocationListener.NOOP)
                        .maxAllocation(Long.MAX_VALUE)
                        .roundingPolicy(FlussRoundingPolicy.DEFAULT_ROUNDING_POLICY)
                        .allocationManagerFactory(allocationManagerFactory)
                        .build());
    }
}
