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
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Schema;

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

        try {
            List<org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector> shadedVectors =
                    shadedRoot.getFieldVectors();
            List<FieldVector> nonShadedVectors = nonShadedRoot.getFieldVectors();

            for (int i = 0; i < shadedVectors.size(); i++) {
                copyVectorData(shadedVectors.get(i), nonShadedVectors.get(i));
            }

            nonShadedRoot.setRowCount(shadedRoot.getRowCount());
            return nonShadedRoot;
        } catch (Exception e) {
            nonShadedRoot.close();
            throw e;
        }
    }

    private static void copyVectorData(
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector shadedVector,
            FieldVector nonShadedVector) {

        if (shadedVector
                instanceof
                org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector) {
            if (nonShadedVector instanceof FixedSizeListVector) {
                copyListToFixedSizeListVectorData(
                        (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector)
                                shadedVector,
                        (FixedSizeListVector) nonShadedVector);
                return;
            } else if (nonShadedVector instanceof ListVector) {
                copyListVectorData(
                        (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector)
                                shadedVector,
                        (ListVector) nonShadedVector);
                return;
            }
        }

        List<org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf> shadedBuffers =
                shadedVector.getFieldBuffers();

        int valueCount = shadedVector.getValueCount();
        nonShadedVector.setValueCount(valueCount);

        List<ArrowBuf> nonShadedBuffers = nonShadedVector.getFieldBuffers();

        for (int i = 0; i < Math.min(shadedBuffers.size(), nonShadedBuffers.size()); i++) {
            org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf shadedBuf =
                    shadedBuffers.get(i);
            ArrowBuf nonShadedBuf = nonShadedBuffers.get(i);

            long size = Math.min(shadedBuf.capacity(), nonShadedBuf.capacity());
            if (size > 0) {
                ByteBuffer srcBuffer = shadedBuf.nioBuffer(0, (int) size);
                srcBuffer.position(0);
                srcBuffer.limit((int) Math.min(size, Integer.MAX_VALUE));
                nonShadedBuf.setBytes(0, srcBuffer);
            }
        }
    }

    private static void copyListVectorData(
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector
                    shadedListVector,
            ListVector nonShadedListVector) {

        // First, recursively copy the child data vector
        org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector shadedDataVector =
                shadedListVector.getDataVector();
        FieldVector nonShadedDataVector = nonShadedListVector.getDataVector();

        if (shadedDataVector != null && nonShadedDataVector != null) {
            copyVectorData(shadedDataVector, nonShadedDataVector);
        }

        // Copy the ListVector's own buffers (validity and offset buffers)
        List<org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf> shadedBuffers =
                shadedListVector.getFieldBuffers();
        List<ArrowBuf> nonShadedBuffers = nonShadedListVector.getFieldBuffers();

        for (int i = 0; i < Math.min(shadedBuffers.size(), nonShadedBuffers.size()); i++) {
            org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf shadedBuf =
                    shadedBuffers.get(i);
            ArrowBuf nonShadedBuf = nonShadedBuffers.get(i);

            long size = Math.min(shadedBuf.capacity(), nonShadedBuf.capacity());
            if (size > 0) {
                ByteBuffer srcBuffer = shadedBuf.nioBuffer(0, (int) size);
                srcBuffer.position(0);
                srcBuffer.limit((int) Math.min(size, Integer.MAX_VALUE));
                nonShadedBuf.setBytes(0, srcBuffer);
            }
        }

        // Set value count WITHOUT calling setValueCount() which would overwrite offset buffer
        // Instead, directly set the internal value count field
        int valueCount = shadedListVector.getValueCount();
        // For ListVector, we need to manually set lastSet to avoid offset buffer recalculation
        nonShadedListVector.setLastSet(valueCount - 1);
    }

    private static void copyListToFixedSizeListVectorData(
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector
                    shadedListVector,
            FixedSizeListVector nonShadedFixedSizeListVector) {

        int valueCount = shadedListVector.getValueCount();
        int expectedListSize = nonShadedFixedSizeListVector.getListSize();
        int nonNullCount = valueCount - shadedListVector.getNullCount();
        int expectedTotalChildCount = nonNullCount * expectedListSize;

        // Validate that backing data vector element count matches expected fixed-size layout.
        int totalChildCount = shadedListVector.getDataVector().getValueCount();
        if (totalChildCount != expectedTotalChildCount) {
            throw new IllegalArgumentException(
                    String.format(
                            "Total child elements (%d) does not match expected %d for FixedSizeList conversion.",
                            totalChildCount, expectedTotalChildCount));
        }

        // Copy child data from the source ListVector to the target FixedSizeListVector.
        //
        // In a ListVector, child elements for non-null rows are packed contiguously
        // (null rows contribute 0 children). In a FixedSizeListVector, child data is
        // stride-based: row i's data starts at index i * listSize, so null rows still
        // occupy child slots. When null rows exist, a simple bulk buffer copy won't
        // work because the layouts differ — we must remap per-row using the source
        // offset buffer.
        org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector shadedDataVector =
                shadedListVector.getDataVector();
        FieldVector nonShadedDataVector = nonShadedFixedSizeListVector.getDataVector();

        if (shadedDataVector != null && nonShadedDataVector != null) {
            if (nonNullCount == valueCount) {
                // No null rows — child layouts are identical, use fast bulk copy.
                copyVectorData(shadedDataVector, nonShadedDataVector);
            } else if (totalChildCount > 0) {
                // Null rows present — copy child data row-by-row with offset remapping.
                copyChildDataWithOffsetRemapping(
                        shadedListVector, nonShadedDataVector, valueCount, expectedListSize);
                nonShadedDataVector.setValueCount(valueCount * expectedListSize);
            }
        }

        // FixedSizeListVector only has a validity buffer (no offset buffer).
        // Copy the validity buffer from the shaded ListVector.
        List<org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf> shadedBuffers =
                shadedListVector.getFieldBuffers();
        List<ArrowBuf> nonShadedBuffers = nonShadedFixedSizeListVector.getFieldBuffers();

        // Both ListVector and FixedSizeListVector have validity as their first buffer
        if (!shadedBuffers.isEmpty() && !nonShadedBuffers.isEmpty()) {
            org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf shadedValidityBuf =
                    shadedBuffers.get(0);
            ArrowBuf nonShadedValidityBuf = nonShadedBuffers.get(0);

            long size = Math.min(shadedValidityBuf.capacity(), nonShadedValidityBuf.capacity());
            if (size > 0) {
                ByteBuffer srcBuffer = shadedValidityBuf.nioBuffer(0, (int) size);
                srcBuffer.position(0);
                srcBuffer.limit((int) Math.min(size, Integer.MAX_VALUE));
                nonShadedValidityBuf.setBytes(0, srcBuffer);
            }
        }

        nonShadedFixedSizeListVector.setValueCount(valueCount);
    }

    /**
     * Copies child element data from a shaded ListVector to a non-shaded child vector, remapping
     * offsets so that row i's data lands at index {@code i * listSize} in the target (the layout
     * required by FixedSizeListVector). Null rows in the source are skipped.
     */
    private static void copyChildDataWithOffsetRemapping(
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector
                    shadedListVector,
            FieldVector nonShadedChildVector,
            int valueCount,
            int listSize) {

        // Source child data buffer (index 1 for fixed-width vectors: [validity, data])
        org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf srcDataBuf =
                shadedListVector.getDataVector().getFieldBuffers().get(1);
        // Source offset buffer: offset[i] is the start element index for row i
        org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf srcOffsetBuf =
                shadedListVector.getOffsetBuffer();

        // Target child buffers: [0] = validity, [1] = data
        ArrowBuf tgtValidityBuf = nonShadedChildVector.getFieldBuffers().get(0);
        ArrowBuf tgtDataBuf = nonShadedChildVector.getFieldBuffers().get(1);

        // Get element byte width from the non-shaded child vector type.
        // Buffer capacity cannot be used because Arrow pads/aligns buffers.
        if (!(nonShadedChildVector instanceof org.apache.arrow.vector.BaseFixedWidthVector)) {
            throw new UnsupportedOperationException(
                    "FixedSizeList conversion with null rows only supports fixed-width child vectors, got "
                            + nonShadedChildVector.getClass().getSimpleName());
        }
        int elementByteWidth =
                ((org.apache.arrow.vector.BaseFixedWidthVector) nonShadedChildVector)
                        .getTypeWidth();
        int rowByteWidth = listSize * elementByteWidth;

        for (int i = 0; i < valueCount; i++) {
            if (!shadedListVector.isNull(i)) {
                int srcElementStart = srcOffsetBuf.getInt((long) i * Integer.BYTES);
                int srcByteOffset = srcElementStart * elementByteWidth;
                int tgtElementStart = i * listSize;
                int tgtByteOffset = tgtElementStart * elementByteWidth;

                // Copy the data bytes for this row's child elements
                ByteBuffer srcSlice = srcDataBuf.nioBuffer(srcByteOffset, rowByteWidth);
                tgtDataBuf.setBytes(tgtByteOffset, srcSlice);

                // Set validity bits for each child element in this row
                for (int j = 0; j < listSize; j++) {
                    int bitIndex = tgtElementStart + j;
                    int byteIndex = bitIndex / 8;
                    int bitOffset = bitIndex % 8;
                    byte currentByte = tgtValidityBuf.getByte(byteIndex);
                    tgtValidityBuf.setByte(byteIndex, currentByte | (1 << bitOffset));
                }
            }
        }
    }
}
