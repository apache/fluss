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

package com.alibaba.fluss.record.send;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.record.bytesview.FileRegionBytesView;
import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static com.alibaba.fluss.testutils.ByteBufChannel.toByteBuf;
import static com.alibaba.fluss.utils.ProtoCodecUtils.readFixedInt32;
import static com.alibaba.fluss.utils.ProtoCodecUtils.readFixedInt64;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SendWritableOutput}. */
class SendWritableOutputTest {

    @Test
    void testZeroCopyMemorySegment() {
        byte[] data = BinaryString.encodeUTF8("foo");
        MemorySegment zeroCopySegment = MemorySegment.wrap(data);
        int bufSize = 12; // int32 + int64
        ByteBuf buf = Unpooled.buffer(bufSize, bufSize);
        SendWritableOutput out = new SendWritableOutput(buf);

        out.writeFixedInt32(5);
        out.writeBytes(new MemorySegmentBytesView(zeroCopySegment, 0, data.length));
        out.writeFixedInt64(15);

        Send send = out.buildSend();
        assertThat(buf.refCnt()).isEqualTo(2); // 2 retained slices

        // overwrite the original buffer in order to prove the data was not copied
        byte[] overwrittenData = BinaryString.encodeUTF8("bar");
        assertThat(overwrittenData.length).isEqualTo(data.length);
        zeroCopySegment.put(0, overwrittenData, 0, overwrittenData.length);

        ByteBuf outBuf = toByteBuf(send);
        assertThat(outBuf.readableBytes()).isEqualTo(bufSize + data.length);
        assertThat(readFixedInt32(outBuf)).isEqualTo(5);
        // should be the overwritten data, because the bytes are lazily copied
        assertThat(readString(outBuf, data.length)).isEqualTo("bar");
        assertThat(readFixedInt64(outBuf)).isEqualTo(15);
        assertThat(buf.refCnt()).isEqualTo(0);
    }

    @Test
    void testZeroCopyFileRegion(@TempDir Path tempDir) throws IOException {
        Path tempFile = tempDir.resolve("send_zero_copy");
        byte[] data = BinaryString.encodeUTF8("foo");
        Files.write(tempFile, data);
        FileChannel fileChannel = FileChannel.open(tempFile);
        FileRegionBytesView fr = new FileRegionBytesView(fileChannel, 0, data.length);
        assertThat(fr.getByteBuf().array()).isEqualTo(data);

        int bufSize = 12; // int32 + int64
        ByteBuf buf = Unpooled.buffer(bufSize, bufSize);
        SendWritableOutput out = new SendWritableOutput(buf);

        out.writeFixedInt32(5);
        out.writeBytes(fr);
        out.writeFixedInt64(15);

        Send send = out.buildSend();
        assertThat(buf.refCnt()).isEqualTo(2); // 2 retained slices

        // overwrite the original buffer in order to prove the data was not copied
        byte[] overwrittenData = BinaryString.encodeUTF8("bar");
        assertThat(overwrittenData.length).isEqualTo(data.length);
        Files.write(tempFile, overwrittenData);

        ByteBuf outBuf = toByteBuf(send);
        assertThat(outBuf.readableBytes()).isEqualTo(bufSize + data.length);
        assertThat(readFixedInt32(outBuf)).isEqualTo(5);
        // should be the overwritten data, because the bytes are lazily copied
        assertThat(readString(outBuf, data.length)).isEqualTo("bar");
        assertThat(readFixedInt64(outBuf)).isEqualTo(15);
        assertThat(buf.refCnt()).isEqualTo(0);
    }

    @Test
    void testZeroCopyPartialFileRegion(@TempDir Path tempDir) throws IOException {
        Path tempFile = tempDir.resolve("send_zero_copy");
        byte[] foo = BinaryString.encodeUTF8("foo");
        Files.write(tempFile, foo);
        byte[] bar = BinaryString.encodeUTF8("barbar");
        Files.write(tempFile, bar, StandardOpenOption.APPEND);
        FileChannel fileChannel = FileChannel.open(tempFile);
        // region of "bar"
        FileRegionBytesView fr = new FileRegionBytesView(fileChannel, foo.length, bar.length);

        int bufSize = 12; // int32 + int64
        ByteBuf buf = Unpooled.buffer(bufSize, bufSize);
        SendWritableOutput out = new SendWritableOutput(buf);

        out.writeFixedInt32(5);
        out.writeBytes(fr);
        out.writeFixedInt64(15);

        Send send = out.buildSend();
        assertThat(buf.refCnt()).isEqualTo(2); // 2 retained slices

        // append more data after the original buffer, shouldn't affect the result
        byte[] appendedData = BinaryString.encodeUTF8("baz");
        Files.write(tempFile, appendedData, StandardOpenOption.APPEND);

        ByteBuf outBuf = toByteBuf(send);
        assertThat(outBuf.readableBytes()).isEqualTo(bufSize + bar.length);
        assertThat(readFixedInt32(outBuf)).isEqualTo(5);
        // shouldn't be "baz"
        assertThat(readString(outBuf, bar.length)).isEqualTo("barbar");
        assertThat(readFixedInt64(outBuf)).isEqualTo(15);
        assertThat(buf.refCnt()).isEqualTo(0);
    }

    @Test
    void testZeroCopyMultiBytesView(@TempDir Path tempDir) throws IOException {
        Path tempFile = tempDir.resolve("send_zero_copy");
        byte[] foo = BinaryString.encodeUTF8("foo");
        Files.write(tempFile, foo);
        byte[] bar = BinaryString.encodeUTF8("bar");
        Files.write(tempFile, bar, StandardOpenOption.APPEND);
        FileChannel fileChannel = FileChannel.open(tempFile);
        MemorySegment mem = MemorySegment.wrap(" world!".getBytes());

        MultiBytesView bv =
                MultiBytesView.builder()
                        // zero-copy file region of "foo" = 3 bytes
                        .addBytes(fileChannel, 0, foo.length)
                        // no-zero-copy part = 7 bytes
                        .addBytes(" HELLO ".getBytes())
                        // region of "bar" = 3 bytes
                        .addBytes(fileChannel, foo.length, bar.length)
                        // zero-copy memory part = 7 bytes
                        .addBytes(mem, 0, mem.size())
                        .build();

        assertThat(bv.getBytesLength()).isEqualTo(20);
        assertThat(bv.getZeroCopyLength()).isEqualTo(13);

        int bufSize = 12 + (bv.getBytesLength() - bv.getZeroCopyLength()); // int32 + int64 + HELLO
        ByteBuf buf = Unpooled.buffer(bufSize, bufSize);
        SendWritableOutput out = new SendWritableOutput(buf);

        out.writeFixedInt32(5);
        out.writeBytes(bv);
        out.writeFixedInt64(15);

        Send send = out.buildSend();
        assertThat(buf.refCnt()).isEqualTo(3); // 3 retained slices

        // overwrite the original buffer in order to prove the data was not copied
        byte[] overwrittenData = BinaryString.encodeUTF8("FOOBAR");
        assertThat(overwrittenData.length).isEqualTo(foo.length + bar.length);
        Files.write(tempFile, overwrittenData);
        // throw IndexOutOfBounds if the data overflows
        mem.put(0, " WORLD!".getBytes());

        ByteBuf outBuf = toByteBuf(send);
        assertThat(outBuf.readableBytes()).isEqualTo(bufSize + bv.getZeroCopyLength());
        assertThat(readFixedInt32(outBuf)).isEqualTo(5);
        assertThat(readString(outBuf, bv.getBytesLength())).isEqualTo("FOO HELLO BAR WORLD!");
        assertThat(readFixedInt64(outBuf)).isEqualTo(15);
        assertThat(buf.refCnt()).isEqualTo(0);
    }

    private static String readString(ByteBuf buf, int length) {
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return BinaryString.decodeUTF8(bytes, 0, length);
    }
}
