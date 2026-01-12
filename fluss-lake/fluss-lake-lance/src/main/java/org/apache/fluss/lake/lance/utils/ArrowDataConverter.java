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

package org.apache.fluss.lake.lance.utils;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Utility class to convert between shaded and non-shaded Arrow VectorSchemaRoot by sharing off-heap
 * memory directly.
 *
 * <p>Since both shaded and non-shaded Arrow use the same off-heap memory layout, we can share the
 * underlying ByteBuffer/memory address directly without serialization overhead.
 */
public class ArrowDataConverter {

    /**
     * Converts a shaded Arrow VectorSchemaRoot to a non-shaded Arrow VectorSchemaRoot by sharing
     * the underlying off-heap memory.
     *
     * @param shadedRoot The shaded Arrow VectorSchemaRoot from fluss-common
     * @param nonShadedAllocator The non-shaded BufferAllocator for Lance
     * @param nonShadedSchema The non-shaded Arrow Schema for Lance
     * @return A non-shaded VectorSchemaRoot compatible with Lance
     */
    public static VectorSchemaRoot convertToNonShaded(
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot shadedRoot,
            BufferAllocator nonShadedAllocator,
            Schema nonShadedSchema) {

        VectorSchemaRoot nonShadedRoot =
                VectorSchemaRoot.create(nonShadedSchema, nonShadedAllocator);
        nonShadedRoot.allocateNew();

        List<org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector> shadedVectors =
                shadedRoot.getFieldVectors();
        List<FieldVector> nonShadedVectors = nonShadedRoot.getFieldVectors();

        for (int i = 0; i < shadedVectors.size(); i++) {
            copyVectorData(shadedVectors.get(i), nonShadedVectors.get(i));
        }

        nonShadedRoot.setRowCount(shadedRoot.getRowCount());
        return nonShadedRoot;
    }

    private static void copyVectorData(
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector shadedVector,
            FieldVector nonShadedVector) {
        try {
            List<org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf> shadedBuffers =
                    getFieldBuffers(shadedVector);

            int valueCount = getValueCount(shadedVector);
            nonShadedVector.setValueCount(valueCount);

            List<ArrowBuf> nonShadedBuffers = nonShadedVector.getFieldBuffers();

            for (int i = 0; i < Math.min(shadedBuffers.size(), nonShadedBuffers.size()); i++) {
                org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf shadedBuf =
                        shadedBuffers.get(i);
                ArrowBuf nonShadedBuf = nonShadedBuffers.get(i);

                long size = Math.min(shadedBuf.capacity(), nonShadedBuf.capacity());
                if (size > 0) {
                    ByteBuffer srcBuffer = getByteBuffer(shadedBuf);
                    if (srcBuffer != null) {
                        srcBuffer.position(0);
                        srcBuffer.limit((int) Math.min(size, Integer.MAX_VALUE));
                        nonShadedBuf.setBytes(0, srcBuffer);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to copy vector data via off-heap memory", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf>
            getFieldBuffers(
                    org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector vector) {
        try {
            Method method = vector.getClass().getMethod("getFieldBuffers");
            return (List<org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf>)
                    method.invoke(vector);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get field buffers from shaded vector", e);
        }
    }

    private static int getValueCount(
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector vector) {
        try {
            Method method = vector.getClass().getMethod("getValueCount");
            return (int) method.invoke(vector);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get value count from shaded vector", e);
        }
    }

    private static ByteBuffer getByteBuffer(
            org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf buf) {
        try {
            Method method = buf.getClass().getMethod("nioBuffer", long.class, int.class);
            return (ByteBuffer) method.invoke(buf, 0L, (int) buf.capacity());
        } catch (Exception e) {
            try {
                Field field = buf.getClass().getDeclaredField("memoryAddress");
                field.setAccessible(true);
                long address = (long) field.get(buf);
                return null;
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get ByteBuffer from ArrowBuf", ex);
            }
        }
    }
}
