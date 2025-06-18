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

/** Tests for {@link Numbers}. */
public class NumbersTest {

    private final byte[] b1 = new byte[4096];
    private final ByteBuf bb1 = Unpooled.wrappedBuffer(b1);
    private final byte[] b2 = new byte[4096];
    private final ByteBuf bb2 = Unpooled.wrappedBuffer(b2);

    private static void assertException(Runnable r) {
        try {
            r.run();
            fail("Should raise exception");
        } catch (IllegalStateException e) {
            // Expected
        }
    }

    @BeforeEach
    public void setup() {
        bb1.clear();
        Arrays.fill(b1, (byte) 0);

        bb2.clear();
        Arrays.fill(b2, (byte) 0);
    }

    @Test
    public void testEnumValue() throws Exception {
        assertThat(Enum1.X1_0.getValue()).isEqualTo(Enum1.X1_0_VALUE);
        assertThat(Enum1.X1_1.getValue()).isEqualTo(Enum1.X1_1_VALUE);
        assertThat(Enum1.X1_2.getValue()).isEqualTo(Enum1.X1_2_VALUE);
    }

    @Test
    public void testEmpty() throws Exception {
        Numbers lpn = new Numbers();
        NumbersOuterClass.Numbers pbn = NumbersOuterClass.Numbers.newBuilder().build();
        verify(lpn, pbn);
    }

    private void verify(Numbers lpn, NumbersOuterClass.Numbers pbn) throws Exception {
        assertThat(lpn.totalSize()).isEqualTo(pbn.getSerializedSize());

        lpn.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpn.totalSize());

        pbn.writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        Numbers parsed = new Numbers();
        parsed.parseFrom(bb1, bb1.readableBytes());

        assertThat(parsed.hasEnum1()).isEqualTo(pbn.hasEnum1());
        assertThat(parsed.hasEnum2()).isEqualTo(pbn.hasEnum2());
        assertThat(parsed.hasXBool()).isEqualTo(pbn.hasXBool());
        assertThat(parsed.hasXDouble()).isEqualTo(pbn.hasXDouble());
        assertThat(parsed.hasXFixed32()).isEqualTo(pbn.hasXFixed32());
        assertThat(parsed.hasXFixed64()).isEqualTo(pbn.hasXFixed64());
        assertThat(parsed.hasXSfixed32()).isEqualTo(pbn.hasXFixed32());
        assertThat(parsed.hasXSfixed64()).isEqualTo(pbn.hasXSfixed64());
        assertThat(parsed.hasXFloat()).isEqualTo(pbn.hasXFloat());
        assertThat(parsed.hasXInt32()).isEqualTo(pbn.hasXInt32());
        assertThat(parsed.hasXInt64()).isEqualTo(pbn.hasXInt64());
        assertThat(parsed.hasXSint32()).isEqualTo(pbn.hasXSint32());
        assertThat(parsed.hasXSint64()).isEqualTo(pbn.hasXSint64());

        if (parsed.hasEnum1()) {
            assertThat(parsed.getEnum1().getValue()).isEqualTo(pbn.getEnum1().getNumber());
        }
        if (parsed.hasEnum2()) {
            assertThat(parsed.getEnum2().getValue()).isEqualTo(pbn.getEnum2().getNumber());
        }
        if (parsed.hasXBool()) {
            assertThat(parsed.isXBool()).isEqualTo(pbn.getXBool());
        }
        if (parsed.hasXDouble()) {
            assertThat(parsed.getXDouble()).isEqualTo(pbn.getXDouble());
        }
        if (parsed.hasXFixed32()) {
            assertThat(parsed.getXFixed32()).isEqualTo(pbn.getXFixed32());
        }
        if (parsed.hasXFixed64()) {
            assertThat(parsed.getXFixed64()).isEqualTo(pbn.getXFixed64());
        }
        if (parsed.hasXSfixed32()) {
            assertThat(parsed.getXSfixed32()).isEqualTo(pbn.getXSfixed32());
        }
        if (parsed.hasXSfixed64()) {
            assertThat(parsed.getXSfixed64()).isEqualTo(pbn.getXSfixed64());
        }
        if (parsed.hasXFloat()) {
            assertThat(parsed.getXFloat()).isEqualTo(pbn.getXFloat());
        }
        if (parsed.hasXInt32()) {
            assertThat(parsed.getXInt32()).isEqualTo(pbn.getXInt32());
        }
        if (parsed.hasXInt64()) {
            assertThat(parsed.getXInt64()).isEqualTo(pbn.getXInt64());
        }
        if (parsed.hasXSint32()) {
            assertThat(parsed.getXSint32()).isEqualTo(pbn.getXSint32());
        }
        if (parsed.hasXSint64()) {
            assertThat(parsed.getXSint64()).isEqualTo(pbn.getXSint64());
        }
    }

    @Test
    public void testNumberFields() throws Exception {
        Numbers lpn = new Numbers();
        NumbersOuterClass.Numbers.Builder pbn = NumbersOuterClass.Numbers.newBuilder();

        assertThat(lpn.hasEnum1()).isFalse();
        assertThat(lpn.hasEnum2()).isFalse();
        assertThat(lpn.hasXBool()).isFalse();
        assertThat(lpn.hasXDouble()).isFalse();
        assertThat(lpn.hasXFixed32()).isFalse();
        assertThat(lpn.hasXFixed64()).isFalse();
        assertThat(lpn.hasXSfixed32()).isFalse();
        assertThat(lpn.hasXSfixed64()).isFalse();
        assertThat(lpn.hasXFloat()).isFalse();
        assertThat(lpn.hasXInt32()).isFalse();
        assertThat(lpn.hasXInt64()).isFalse();
        assertThat(lpn.hasXInt32()).isFalse();
        assertThat(lpn.hasXUint64()).isFalse();
        assertThat(lpn.hasXUint32()).isFalse();
        assertThat(lpn.hasXSint32()).isFalse();
        assertThat(lpn.hasXSint64()).isFalse();

        assertException(lpn::getEnum1);
        assertException(lpn::getEnum2);
        assertException(lpn::isXBool);
        assertException(lpn::getXDouble);
        assertException(lpn::getXFixed32);
        assertException(lpn::getXFixed64);
        assertException(lpn::getXSfixed32);
        assertException(lpn::getXSfixed64);
        assertException(lpn::getXFloat);
        assertException(lpn::getXInt32);
        assertException(lpn::getXInt64);
        assertException(lpn::getXInt32);
        assertException(lpn::getXUint64);
        assertException(lpn::getXUint32);
        assertException(lpn::getXSint32);
        assertException(lpn::getXSint64);

        lpn.setEnum1(Enum1.X1_1);
        lpn.setEnum2(Numbers.Enum2.X2_1);
        lpn.setXBool(true);
        lpn.setXDouble(1.0);
        lpn.setXFixed32(2);
        lpn.setXFixed64(12345L);
        lpn.setXSfixed32(-2);
        lpn.setXSfixed64(-12345L);
        lpn.setXFloat(1.2f);
        lpn.setXInt32(4);
        lpn.setXInt64(126L);
        lpn.setXUint32(40);
        lpn.setXUint64(1260L);
        lpn.setXSint32(-11);
        lpn.setXSint64(-12L);

        pbn.setEnum1(NumbersOuterClass.Enum1.X1_1);
        pbn.setEnum2(NumbersOuterClass.Numbers.Enum2.X2_1);
        pbn.setXBool(true);
        pbn.setXDouble(1.0);
        pbn.setXFixed32(2);
        pbn.setXFixed64(12345L);
        pbn.setXSfixed32(-2);
        pbn.setXSfixed64(-12345L);
        pbn.setXFloat(1.2f);
        pbn.setXInt32(4);
        pbn.setXInt64(126L);
        pbn.setXUint32(40);
        pbn.setXUint64(1260L);
        pbn.setXSint32(-11);
        pbn.setXSint64(-12L);

        assertThat(lpn.hasEnum1()).isTrue();
        assertThat(lpn.hasEnum2()).isTrue();
        assertThat(lpn.hasXBool()).isTrue();
        assertThat(lpn.hasXDouble()).isTrue();
        assertThat(lpn.hasXFixed32()).isTrue();
        assertThat(lpn.hasXFixed64()).isTrue();
        assertThat(lpn.hasXSfixed32()).isTrue();
        assertThat(lpn.hasXSfixed64()).isTrue();
        assertThat(lpn.hasXFloat()).isTrue();
        assertThat(lpn.hasXInt32()).isTrue();
        assertThat(lpn.hasXInt64()).isTrue();
        assertThat(lpn.hasXSint32()).isTrue();
        assertThat(lpn.hasXSint64()).isTrue();

        assertThat(lpn.getEnum1()).isEqualTo(Enum1.X1_1);
        assertThat(lpn.getEnum2()).isEqualTo(Numbers.Enum2.X2_1);
        assertThat(lpn.isXBool()).isEqualTo(true);
        assertThat(lpn.getXDouble()).isEqualTo(1.0);
        assertThat(lpn.getXFixed32()).isEqualTo(2);
        assertThat(lpn.getXFixed64()).isEqualTo(12345L);
        assertThat(lpn.getXSfixed32()).isEqualTo(-2);
        assertThat(lpn.getXSfixed64()).isEqualTo(-12345L);
        assertThat(lpn.getXFloat()).isEqualTo(1.2f);
        assertThat(lpn.getXInt32()).isEqualTo(4);
        assertThat(lpn.getXInt64()).isEqualTo(126L);
        assertThat(lpn.getXUint32()).isEqualTo(40);
        assertThat(lpn.getXUint64()).isEqualTo(1260L);
        assertThat(lpn.getXSint32()).isEqualTo(-11);
        assertThat(lpn.getXSint64()).isEqualTo(-12L);

        verify(lpn, pbn.build());
    }
}
