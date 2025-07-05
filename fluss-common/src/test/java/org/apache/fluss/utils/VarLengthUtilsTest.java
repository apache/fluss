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

package org.apache.fluss.utils;

import org.apache.fluss.memory.MemorySegmentInputView;
import org.apache.fluss.memory.MemorySegmentOutputView;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.fluss.utils.VarLengthUtils.readVarInt;
import static org.apache.fluss.utils.VarLengthUtils.readVarLong;
import static org.apache.fluss.utils.VarLengthUtils.sizeOfVarInt;
import static org.apache.fluss.utils.VarLengthUtils.sizeOfVarLong;
import static org.apache.fluss.utils.VarLengthUtils.writeVarInt;
import static org.apache.fluss.utils.VarLengthUtils.writeVarLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.utils.VarLengthUtils}. */
public class VarLengthUtilsTest {

    MemorySegmentOutputView out;

    @BeforeEach
    public void before() {
        out = new MemorySegmentOutputView(4096);
    }

    @Test
    void testVarInt() throws IOException {
        writeVarInt(out, -1);
        writeVarInt(out, 10);
        writeVarInt(out, 20);
        MemorySegmentInputView in = new MemorySegmentInputView(out.getMemorySegment());
        assertThat(readVarInt(in)).isEqualTo(-1);
        assertThat(readVarInt(in)).isEqualTo(10);
        assertThat(readVarInt(in)).isEqualTo(20);
    }

    @Test
    void testReadVarInt() throws IOException {
        int size = 0;
        writeVarInt(out, -1);
        int len1 = sizeOfVarInt(-1);
        size += len1;
        writeVarInt(out, 10);
        int len2 = sizeOfVarInt(10);
        size += len2;
        writeVarInt(out, 1000);
        size += sizeOfVarInt(1000);

        byte[] buffer = new byte[size];
        new MemorySegmentInputView(out.getMemorySegment()).readFully(buffer, 0, size);

        assertThat(readVarInt(buffer, 0)).isEqualTo(-1);
        assertThat(readVarInt(buffer, len1)).isEqualTo(10);
        assertThat(readVarInt(buffer, len1 + len2)).isEqualTo(1000);
    }

    @Test
    void testVarLong() throws IOException {
        writeVarLong(out, -1L);
        writeVarLong(out, 10L);
        writeVarLong(out, 20L);
        MemorySegmentInputView in = new MemorySegmentInputView(out.getMemorySegment());
        assertThat(readVarLong(in)).isEqualTo(-1L);
        assertThat(readVarLong(in)).isEqualTo(10L);
        assertThat(readVarLong(in)).isEqualTo(20L);
    }

    @Test
    void testReadVarLong() throws IOException {
        int size = 0;
        writeVarLong(out, -1L);
        int len1 = sizeOfVarLong(-1L);
        size += len1;
        writeVarLong(out, 10L);
        int len2 = sizeOfVarLong(10L);
        size += len2;
        writeVarLong(out, 1000L);
        size += sizeOfVarLong(1000L);

        byte[] buffer = new byte[size];
        new MemorySegmentInputView(out.getMemorySegment()).readFully(buffer, 0, size);
        assertThat(readVarLong(buffer, 0)).isEqualTo(-1L);
        assertThat(readVarLong(buffer, len1)).isEqualTo(10L);
        assertThat(readVarLong(buffer, len1 + len2)).isEqualTo(1000L);

        final byte[] buffer2 =
                new byte[] {
                    (byte) 0x80,
                    (byte) 0x80,
                    (byte) 0x80,
                    (byte) 0x80,
                    (byte) 0x80,
                    (byte) 0x80,
                    (byte) 0x80,
                    (byte) 0x80,
                    (byte) 0x80,
                    (byte) 0x80
                };
        assertThatThrownBy(() -> readVarLong(buffer2, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "VarLong is too long, most significant bit in the 10th byte is set");
    }
}
