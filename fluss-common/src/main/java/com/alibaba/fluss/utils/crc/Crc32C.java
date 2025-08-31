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

package com.alibaba.fluss.utils.crc;

import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.Checksum;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A class that can be used to compute the CRC32C (Castagnoli) of a ByteBuffer or array of bytes.
 *
 * <p>We use java.util.zip.CRC32C (introduced in Java 9) if it is available and fallback to
 * PureJavaCrc32C, otherwise. java.util.zip.CRC32C is significantly faster on reasonably modern CPUs
 * as it uses the CRC32 instruction introduced in SSE4.2.
 *
 * <p>NOTE: This class is intended for INTERNAL usage only within Fluss.
 */
public final class Crc32C {

    private static final ChecksumFactory CHECKSUM_FACTORY;

    static {
        if (Java.IS_JAVA9_COMPATIBLE) {
            CHECKSUM_FACTORY = new Java9ChecksumFactory();
        } else {
            CHECKSUM_FACTORY = new PureJavaChecksumFactory();
        }
    }

    private Crc32C() {}

    /**
     * Compute the CRC32C (Castagnoli) of the segment of the byte array given by the specified size
     * and offset.
     *
     * @param bytes The bytes to checksum
     * @param offset the offset at which to begin the checksum computation
     * @param size the number of bytes to checksum
     * @return The CRC32C
     */
    public static long compute(byte[] bytes, int offset, int size) {
        Checksum crc = create();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    /**
     * Compute the CRC32C (Castagnoli) of a byte buffer from a given offset (relative to the
     * buffer's current position).
     *
     * @param buffer The buffer with the underlying data
     * @param offset The offset relative to the current position
     * @param size The number of bytes beginning from the offset to include
     * @return The CRC32C
     */
    public static long compute(ByteBuffer buffer, int offset, int size) {
        Checksum crc = create();
        Checksums.update(crc, buffer, offset, size);
        return crc.getValue();
    }

    /**
     * Compute the CRC32C (Castagnoli) of an array of byte buffers from a given offset (relative to
     * the buffer's current position).
     *
     * @param buffers The buffers with the underlying data
     * @param offsets The offsets relative to the current position
     * @param size The number of bytes beginning from the offset to include
     * @return The CRC32C
     */
    public static long compute(ByteBuffer[] buffers, int[] offsets, int[] size) {
        Checksum crc = create();
        for (int i = 0; i < buffers.length; i++) {
            Checksums.update(crc, buffers[i], offsets[i], size[i]);
        }
        return crc.getValue();
    }

    /**
     * Compute the CRC32C (Castagnoli) of a list of {@link MemorySegmentBytesView} from a given
     * start offset (relative to the first buffer's current position).
     *
     * @param buffers the list of buffers with the underlying data
     * @param startOffset the offset relative to the first buffer's current position
     * @return the CRC32C
     */
    public static long compute(List<MemorySegmentBytesView> buffers, int startOffset) {
        Checksum crc = create();
        boolean first = true;
        for (MemorySegmentBytesView buffer : buffers) {
            int offset = first ? startOffset : 0;
            int size = buffer.getBytesLength() - offset;
            Checksums.update(crc, buffer.getByteBuffer(), offset, size);
            first = false;
        }
        return crc.getValue();
    }

    public static Checksum create() {
        return CHECKSUM_FACTORY.create();
    }

    private interface ChecksumFactory {
        Checksum create();
    }

    /** A ChecksumFactory for Java 9+. */
    static class Java9ChecksumFactory implements ChecksumFactory {
        private static final MethodHandle CONSTRUCTOR;

        static {
            try {
                Class<?> cls = Class.forName("java.util.zip.CRC32C");
                CONSTRUCTOR =
                        MethodHandles.publicLookup()
                                .findConstructor(cls, MethodType.methodType(void.class));
            } catch (ReflectiveOperationException e) {
                // Should never happen
                throw new RuntimeException(e);
            }
        }

        @Override
        public Checksum create() {
            try {
                return (Checksum) CONSTRUCTOR.invoke();
            } catch (Throwable throwable) {
                // Should never happen
                throw new RuntimeException(throwable);
            }
        }
    }

    private static class PureJavaChecksumFactory implements ChecksumFactory {
        @Override
        public Checksum create() {
            return new PureJavaCrc32C();
        }
    }
}
