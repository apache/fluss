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

package com.alibaba.fluss.protogen.tests;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.record.send.Send;
import com.alibaba.fluss.record.send.SendWritableOutput;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static com.alibaba.fluss.testutils.ByteBufChannel.toByteArray;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Bytes}. */
public class BytesTest {

    private final byte[] b1 = new byte[4096];
    private final ByteBuf bb1 = Unpooled.wrappedBuffer(b1);
    private final byte[] b2 = new byte[4096];

    @BeforeEach
    public void setup() {
        bb1.clear();
        Arrays.fill(b1, (byte) 0);
        Arrays.fill(b2, (byte) 0);
    }

    @Test
    public void testBytes() throws Exception {
        B lpb = new B().setPayload(new byte[] {1, 2, 3, 4, 5});

        assertThat(lpb.getPayloadSize()).isEqualTo(5);
        assertThat(lpb.getPayload()).isEqualTo(new byte[] {1, 2, 3, 4, 5});

        // test binary equals to protobuf
        Bytes.B pbb =
                Bytes.B
                        .newBuilder()
                        .setPayload(ByteString.copyFrom(new byte[] {1, 2, 3, 4, 5}))
                        .build();

        assertThat(lpb.totalSize()).isEqualTo(pbb.getSerializedSize());
        lpb.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpb.totalSize());

        pbb.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        // test parseFrom
        B parsed = new B();
        parsed.parseFrom(bb1, bb1.readableBytes());

        assertThat(parsed.hasPayload()).isTrue();
        assertThat(parsed.getPayloadSize()).isEqualTo(5);
        assertThat(parsed.getPayload()).isEqualTo(new byte[] {1, 2, 3, 4, 5});

        // test parsedBuf writeTo
        bb1.clear();
        parsed.writeTo(bb1);

        assertThat(bb1.readableBytes()).isEqualTo(lpb.totalSize());
        assertThat(b2).isEqualTo(b1);

        // test writeTo(WritableOutput)
        int bufSize = lpb.sizeExcludingZeroCopy();
        SendWritableOutput out = new SendWritableOutput(Unpooled.buffer(bufSize, bufSize));
        lpb.writeTo(out);
        Send send = out.buildSend();
        byte[] b3 = toByteArray(send);
        assertThat(b3.length).isEqualTo(lpb.totalSize());
        byte[] expected = new byte[lpb.totalSize()];
        System.arraycopy(b1, 0, expected, 0, lpb.totalSize());
        assertThat(b3).isEqualTo(expected);
    }

    @Test
    public void testRecordsLazilyParsed() {
        RD rd = new RD();
        assertThat(rd.isLazilyParsed()).isTrue();
        ORD ord = new ORD();
        // ORD contains optional RD
        assertThat(ord.isLazilyParsed()).isTrue();
        RRD rrd = new RRD();
        // RRD contains repeated RD
        assertThat(rrd.isLazilyParsed()).isTrue();

        B b = new B();
        // B doesn't contain RD
        assertThat(b.isLazilyParsed()).isFalse();
    }

    @Test
    public void testRecordsBytes() throws Exception {
        RD lpb = new RD().setRecords(new byte[] {1, 2, 3, 4, 5});

        verifyBytesField(lpb);
    }

    @Test
    public void testRecordsBytesBuf() throws Exception {
        RD lpb = new RD();
        ByteBuf b = Unpooled.directBuffer(5);
        b.writeBytes(new byte[] {1, 2, 3, 4, 5});
        lpb.setRecords(b);

        verifyBytesField(lpb);
    }

    @Test
    public void testRecordsMemorySegment() throws Exception {
        RD lpb = new RD();
        MemorySegment segment = MemorySegment.wrap(new byte[] {1, 2, 3, 4, 5});
        lpb.setRecords(segment, 0, segment.size());

        verifyBytesField(lpb);
    }

    @Test
    public void testRecordsOffHeapMemorySegment() throws Exception {
        RD lpb = new RD();
        MemorySegment segment = MemorySegment.allocateOffHeapMemory(5);
        segment.put(0, new byte[] {1, 2, 3, 4, 5}, 0, 5);
        lpb.setRecords(segment, 0, segment.size());

        verifyBytesField(lpb);
    }

    @Test
    public void testRecordsFileChannel(@TempDir Path tempDir) throws Exception {
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        Path tempFile = tempDir.resolve("bytes_test");
        Files.write(tempFile, data);
        FileChannel fileChannel = FileChannel.open(tempFile);

        RD lpb = new RD();
        lpb.setRecords(fileChannel, 0, data.length);

        verifyBytesField(lpb);
    }

    @Test
    public void testRecordsMultiBytes(@TempDir Path tempDir) throws Exception {
        byte[] data = new byte[] {1, 5};
        Path tempFile = tempDir.resolve("bytes_test");
        Files.write(tempFile, data);
        FileChannel fileChannel = FileChannel.open(tempFile);
        MemorySegment segment = MemorySegment.allocateOffHeapMemory(2);
        segment.put(0, new byte[] {3, 4}, 0, 2);

        MultiBytesView mbv =
                MultiBytesView.builder()
                        .addBytes(fileChannel, 0, 1)
                        .addBytes(new byte[] {2})
                        .addBytes(segment, 0, segment.size())
                        .addBytes(fileChannel, 1, 1)
                        .build();

        RD lpb = new RD();
        lpb.setRecordsBytesView(mbv);

        verifyBytesField(lpb);
    }

    private void verifyBytesField(RD lpb) throws IOException {
        // test basic getters
        assertThat(lpb.hasRecords()).isTrue();
        assertThat(lpb.getRecordsSize()).isEqualTo(5);
        assertThat(lpb.getRecords()).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(lpb.getRecordsSlice())
                .isEqualTo(Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4, 5}));

        // test binary equals to protobuf
        Bytes.RD pbb =
                Bytes.RD
                        .newBuilder()
                        .setRecords(ByteString.copyFrom(new byte[] {1, 2, 3, 4, 5}))
                        .build();

        assertThat(lpb.totalSize()).isEqualTo(pbb.getSerializedSize());
        lpb.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpb.totalSize());

        pbb.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        // test parseFrom
        RD parsed = new RD();
        parsed.parseFrom(bb1, bb1.readableBytes());

        assertThat(parsed.hasRecords()).isTrue();
        assertThat(parsed.getRecordsSize()).isEqualTo(5);
        assertThat(parsed.getRecordsSlice())
                .isEqualTo(Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4, 5}));
        assertThat(parsed.getRecords()).isEqualTo(new byte[] {1, 2, 3, 4, 5});

        // test parsedBuf writeTo
        bb1.clear();
        parsed.writeTo(bb1);

        assertThat(bb1.readableBytes()).isEqualTo(lpb.totalSize());
        assertThat(b2).isEqualTo(b1);

        // test writeTo(WritableOutput)
        int bufSize = lpb.sizeExcludingZeroCopy();
        SendWritableOutput out = new SendWritableOutput(Unpooled.buffer(bufSize, bufSize));
        lpb.writeTo(out);
        Send send = out.buildSend();
        byte[] b3 = toByteArray(send);
        assertThat(b3.length).isEqualTo(lpb.totalSize());
        byte[] expected = new byte[lpb.totalSize()];
        System.arraycopy(b1, 0, expected, 0, lpb.totalSize());
        assertThat(b3).isEqualTo(expected);
    }

    @Test
    public void testRepeatedBytes() throws Exception {
        B lpb = new B();
        lpb.addExtraItem(new byte[] {1, 2, 3});
        lpb.addExtraItem(new byte[] {4, 5, 6, 7});

        assertThat(lpb.getExtraItemsCount()).isEqualTo(2);
        assertThat(lpb.getExtraItemSizeAt(0)).isEqualTo(3);
        assertThat(lpb.getExtraItemSizeAt(1)).isEqualTo(4);
        assertThat(lpb.getExtraItemAt(0)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(lpb.getExtraItemAt(1)).isEqualTo(new byte[] {4, 5, 6, 7});

        Bytes.B pbb =
                Bytes.B
                        .newBuilder()
                        .addExtraItems(ByteString.copyFrom(new byte[] {1, 2, 3}))
                        .addExtraItems(ByteString.copyFrom(new byte[] {4, 5, 6, 7}))
                        .build();

        assertThat(lpb.totalSize()).isEqualTo(pbb.getSerializedSize());
        lpb.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpb.totalSize());

        pbb.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        B parsed = new B();
        parsed.parseFrom(bb1, bb1.readableBytes());

        assertThat(parsed.getExtraItemsCount()).isEqualTo(2);
        assertThat(parsed.getExtraItemSizeAt(0)).isEqualTo(3);
        assertThat(parsed.getExtraItemSizeAt(1)).isEqualTo(4);
        assertThat(parsed.getExtraItemAt(0)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(parsed.getExtraItemAt(1)).isEqualTo(new byte[] {4, 5, 6, 7});
    }
}
