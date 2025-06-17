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

package com.alibaba.fluss.protogen.tests;

import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link Enums}. */
public class EnumsTest {

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
    public void testUnknownRequiredEnum() throws Exception {
        EnumTest2 lpet2 = new EnumTest2().setE(E2.D2);

        assertThat(lpet2.getE()).isEqualTo(E2.D2);

        Enums.EnumTest2 pbet2 = Enums.EnumTest2.newBuilder().setE(Enums.E2.D2).build();

        assertThat(lpet2.totalSize()).isEqualTo(pbet2.getSerializedSize());

        lpet2.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpet2.totalSize());

        pbet2.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        EnumTest1 parsed = new EnumTest1();
        try {
            parsed.parseFrom(bb1, bb1.readableBytes());
            fail("Should have failed");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage().contains("Some required fields are missing")).isTrue();
        }
    }

    @Test
    public void testUnknownOptionalEnum() throws Exception {
        EnumTest2 lpet2 = new EnumTest2().setE(E2.D2);

        assertThat(lpet2.getE()).isEqualTo(E2.D2);

        Enums.EnumTest2 pbet2 = Enums.EnumTest2.newBuilder().setE(Enums.E2.D2).build();

        assertThat(lpet2.totalSize()).isEqualTo(pbet2.getSerializedSize());

        lpet2.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpet2.totalSize());

        pbet2.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        EnumTest1Optional parsed = new EnumTest1Optional();
        parsed.parseFrom(bb1, bb1.readableBytes());
        assertThat(parsed.hasE()).isFalse();
    }

    @Test
    public void testUnknownRepeatedEnum() throws Exception {
        EnumTest2Repeated lpet2 = new EnumTest2Repeated();
        lpet2.addE(E2.A2);
        lpet2.addE(E2.B2);
        lpet2.addE(E2.C2);
        lpet2.addE(E2.D2);

        assertThat(lpet2.getEsCount()).isEqualTo(4);

        Enums.EnumTest2Repeated pbet2 =
                Enums.EnumTest2Repeated.newBuilder()
                        .addE(Enums.E2.A2)
                        .addE(Enums.E2.B2)
                        .addE(Enums.E2.C2)
                        .addE(Enums.E2.D2)
                        .build();

        assertThat(lpet2.totalSize()).isEqualTo(pbet2.getSerializedSize());

        lpet2.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpet2.totalSize());

        pbet2.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        EnumTest1Repeated parsed = new EnumTest1Repeated();
        parsed.parseFrom(bb1, bb1.readableBytes());
        assertThat(parsed.getEsCount()).isEqualTo(3);
        assertThat(parsed.getEAt(0)).isEqualTo(E1.A1);
        assertThat(parsed.getEAt(1)).isEqualTo(E1.B1);
        assertThat(parsed.getEAt(2)).isEqualTo(E1.C1);
    }

    @Test
    public void testUnknownRepeatedPackedEnum() throws Exception {
        EnumTest2Packed lpet2 = new EnumTest2Packed();
        lpet2.addE(E2.A2);
        lpet2.addE(E2.B2);
        lpet2.addE(E2.C2);
        lpet2.addE(E2.D2);

        assertThat(lpet2.getEsCount()).isEqualTo(4);

        Enums.EnumTest2Packed pbet2 =
                Enums.EnumTest2Packed.newBuilder()
                        .addE(Enums.E2.A2)
                        .addE(Enums.E2.B2)
                        .addE(Enums.E2.C2)
                        .addE(Enums.E2.D2)
                        .build();

        assertThat(lpet2.totalSize()).isEqualTo(pbet2.getSerializedSize());

        lpet2.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpet2.totalSize());

        pbet2.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        EnumTest1Packed parsed = new EnumTest1Packed();
        parsed.parseFrom(bb1, bb1.readableBytes());
        assertThat(parsed.getEsCount()).isEqualTo(3);
        assertThat(parsed.getEAt(0)).isEqualTo(E1.A1);
        assertThat(parsed.getEAt(1)).isEqualTo(E1.B1);
        assertThat(parsed.getEAt(2)).isEqualTo(E1.C1);
    }
}
