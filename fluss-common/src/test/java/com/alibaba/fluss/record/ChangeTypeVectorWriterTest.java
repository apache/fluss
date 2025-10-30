/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.MemorySegment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ChangeTypeVectorWriter}. */
public class ChangeTypeVectorWriterTest {

    @Test
    public void testReproduceOriginalIndexOutOfBoundsExceptionScenario() {
        int segmentSize = 131072; // 128KB
        MemorySegment segment = MemorySegment.allocateHeapMemory(segmentSize);

        // Create a ChangeTypeVectorWriter that uses the entire segment
        ChangeTypeVectorWriter writer = new ChangeTypeVectorWriter(segment, 0);

        // Write records up to capacity
        for (int i = 0; i < segmentSize; i++) {
            writer.writeChangeType(ChangeType.INSERT);
        }

        // This should now throw IllegalStateException instead of IndexOutOfBoundsException
        assertThatThrownBy(() -> writer.writeChangeType(ChangeType.INSERT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The change type vector is full.");
    }
}
