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

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.zip.Checksum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link Crc32C}. */
class Crc32CTest {
    @Test
    public void testUpdate() {
        final byte[] bytes = "Any String you want".getBytes();
        final int len = bytes.length;

        Checksum crc1 = Crc32C.create();
        Checksum crc2 = Crc32C.create();
        Checksum crc3 = Crc32C.create();

        crc1.update(bytes, 0, len);
        for (byte aByte : bytes) {
            crc2.update(aByte);
        }
        crc3.update(bytes, 0, len / 2);
        crc3.update(bytes, len / 2, len - len / 2);

        assertThat(crc1.getValue()).isEqualTo(crc2.getValue());
        assertThat(crc1.getValue()).isEqualTo(crc3.getValue());
    }

    @Test
    public void testValue() {
        final byte[] bytes = "Some String".getBytes();
        assertThat(Crc32C.compute(bytes, 0, bytes.length)).isEqualTo(608512271);
    }

    @Test
    public void testComputeByteArray() {
        byte[] data = "hello".getBytes();
        long crc = Crc32C.compute(data, 0, data.length);
        assertThat(crc).isEqualTo(2591144780L);
    }

    @Test
    public void testComputeByteArrayWithOffsetAndSize() {
        byte[] data = " hello".getBytes();
        long crc = Crc32C.compute(data, 1, 5);
        assertThat(crc).isEqualTo(2591144780L);
    }

    @Test
    public void testComputeByteBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap("fluss".getBytes());
        long crc = Crc32C.compute(buffer, 0, buffer.remaining());
        assertThat(crc).isEqualTo(2355313305L);
    }

    @Test
    public void testComputeByteBufferWithOffsetAndSize() {
        ByteBuffer buffer = ByteBuffer.wrap("hello fluss".getBytes());
        long crc = Crc32C.compute(buffer, 6, 5);
        assertThat(crc).isEqualTo(2355313305L);
    }

    @Test
    public void testComputeMultipleByteBuffers() {
        ByteBuffer b1 = ByteBuffer.wrap("hello".getBytes());
        ByteBuffer b2 = ByteBuffer.wrap("world".getBytes());
        ByteBuffer[] buffers = new ByteBuffer[] {b1, b2};
        int[] offsets = {0, 0};
        int[] sizes = {b1.remaining(), b2.remaining()};
        long crc = Crc32C.compute(buffers, offsets, sizes);
        assertThat(crc).isEqualTo(1456190592L);
    }

    @Test
    public void testComputeListOfMemorySegmentBytesViews() {
        byte[] data = "hello".getBytes();
        List<MemorySegmentBytesView> views =
                Collections.singletonList(
                        new MemorySegmentBytesView(MemorySegment.wrap(data), 0, data.length));
        long crc = Crc32C.compute(views, 0);
        assertThat(crc).isEqualTo(2591144780L);
    }

    @Test
    public void testComputeListWithStartOffset() {
        byte[] data = "1234 hello".getBytes();
        List<MemorySegmentBytesView> views =
                Collections.singletonList(
                        new MemorySegmentBytesView(MemorySegment.wrap(data), 0, data.length));
        long crc = Crc32C.compute(views, 5);
        assertThat(crc).isEqualTo(2591144780L);
    }

    @Test
    public void testCreateChecksumInstance() {
        Checksum checksum = Crc32C.create();
        checksum.update("checksum".getBytes(), 0, "checksum".length());
        long value = checksum.getValue();
        assertThat(value).isEqualTo(2474969539L);
    }

    @Test
    @EnabledOnJre({JRE.JAVA_8})
    public void testJava9ChecksumFactoryOnJava8() {
        assertThatThrownBy(Crc32C.Java9ChecksumFactory::new)
                .rootCause()
                .isInstanceOf(ClassNotFoundException.class)
                .hasMessageContaining("java.util.zip.CRC32C");
    }
}
