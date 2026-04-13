/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.row.arrow.memory;

import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.AllocationListener;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;

import static org.apache.fluss.row.arrow.memory.ArrowRoundingPolicy.ARROW_ROUNDING_POLICY;

/** Utility class for creating Arrow BufferAllocators with the custom ArrowRoundingPolicy. */
public class BufferAllocatorUtil {

    /** Creates a {@link BufferAllocator} configured with the {@link ArrowRoundingPolicy}. */
    public static BufferAllocator createBufferAllocator() {
        return new RootAllocator(AllocationListener.NOOP, Long.MAX_VALUE, ARROW_ROUNDING_POLICY);
    }
}
