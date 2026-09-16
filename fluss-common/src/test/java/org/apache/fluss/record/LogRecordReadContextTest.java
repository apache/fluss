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

import org.apache.fluss.record.ArrowRecordBatchContext.UnshadedArrowBatchAccess;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LogRecordReadContext}. */
class LogRecordReadContextTest {

    @Test
    void testCloseReleasesUnshadedAllocatorWhenShadedCloseFails() throws Exception {
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA);
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, schemaGetter);
        UnshadedArrowBatchAccess unshadedAccess =
                readContext.createUnshadedArrowBatchAccess(DEFAULT_SCHEMA_ID);
        // Release unshaded vectors first so the unshaded allocator can actually close.
        unshadedAccess.close();
        ArrowBuf outstanding = readContext.getBufferAllocator().buffer(64);

        Field unshadedField =
                LogRecordReadContext.class.getDeclaredField("unshadedBufferAllocator");
        unshadedField.setAccessible(true);
        BufferAllocator unshadedAllocator = (BufferAllocator) unshadedField.get(readContext);
        assertThat(unshadedAllocator).isNotNull();

        try {
            assertThatThrownBy(readContext::close).isInstanceOf(IllegalStateException.class);
            assertThat(unshadedField.get(readContext)).isNull();
            assertThatThrownBy(() -> unshadedAllocator.buffer(8))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            try {
                outstanding.close();
            } catch (IllegalStateException ignored) {
                // The shaded allocator is already closed together with the read context.
            }
        }
    }

    @Test
    void testCloseKeepsUnshadedAllocatorWhenUnshadedCloseFails() throws Exception {
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA);
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, schemaGetter);
        UnshadedArrowBatchAccess unshadedAccess =
                readContext.createUnshadedArrowBatchAccess(DEFAULT_SCHEMA_ID);
        unshadedAccess.close();

        Field unshadedField =
                LogRecordReadContext.class.getDeclaredField("unshadedBufferAllocator");
        unshadedField.setAccessible(true);
        BufferAllocator unshadedAllocator = (BufferAllocator) unshadedField.get(readContext);
        org.apache.arrow.memory.ArrowBuf outstanding = unshadedAllocator.buffer(64);

        try {
            assertThatThrownBy(readContext::close)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("pollRecordBatch()");
            assertThat(unshadedField.get(readContext)).isSameAs(unshadedAllocator);
        } finally {
            try {
                outstanding.close();
            } catch (IllegalStateException ignored) {
                // Arrow may mark the allocator closed even when close() throws.
            }
        }

        readContext.close();
        assertThat(unshadedField.get(readContext)).isNull();
    }
}
