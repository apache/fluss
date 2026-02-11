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

package org.apache.fluss.record;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link LogRecordReadContext}. */
class LogRecordReadContextTest {

    private static final RowType ROW_TYPE = TestData.DATA1_ROW_TYPE;

    /**
     * Simulates the scenario where an OOM occurs during Arrow decompression, leaving temporary
     * buffers allocated in the {@link BufferAllocator} that are not tracked by VectorSchemaRoot.
     *
     * <p>Without the fix (force-releasing remaining memory before close), {@code
     * bufferAllocator.close()} would throw an {@link IllegalStateException} with "Memory was leaked
     * by query" because the allocator still has outstanding allocations.
     *
     * @see <a href="https://github.com/apache/fluss/issues/2646">FLUSS-2646</a>
     */
    @Test
    void testCloseWithLeakedBufferFromDecompressionOom() {
        Schema schema = Schema.newBuilder().fromRowType(ROW_TYPE).build();
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(schema, DEFAULT_SCHEMA_ID));

        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        ROW_TYPE, DEFAULT_SCHEMA_ID, schemaGetter);
        BufferAllocator allocator = readContext.getBufferAllocator();

        // Simulate decompression allocating a temporary buffer that is never released
        // (e.g. OOM occurs during VectorLoader.load() after allocator.buffer() succeeds).
        allocator.buffer(1024);
        assertThatCode(readContext::close).doesNotThrowAnyException();
    }
}
