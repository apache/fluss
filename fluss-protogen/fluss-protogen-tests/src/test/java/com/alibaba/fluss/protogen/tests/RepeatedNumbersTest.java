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

import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link RepeatedNumbers}. */
public class RepeatedNumbersTest {

    private final byte[] b1 = new byte[4096];
    private final ByteBuf bb1 = Unpooled.wrappedBuffer(b1);
    private final byte[] b2 = new byte[4096];
    private final ByteBuf bb2 = Unpooled.wrappedBuffer(b2);

    private static void assertException(Callable<?> r) {
        try {
            r.call();
            fail("Should raise exception");
        } catch (IndexOutOfBoundsException e) {
            // Expected
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    public void setup() {
        bb1.clear();
        Arrays.fill(b1, (byte) 0);

        bb2.clear();
        Arrays.fill(b2, (byte) 0);
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
        Repeated lpn = new Repeated();
        RepeatedNumbers.Repeated.Builder pbn = RepeatedNumbers.Repeated.newBuilder();

        assertThat(lpn.getEnum1sCount()).isEqualTo(0);
        assertThat(lpn.getXBoolsCount()).isEqualTo(0);
        assertThat(lpn.getXDoublesCount()).isEqualTo(0);
        assertThat(lpn.getXFixed32sCount()).isEqualTo(0);
        assertThat(lpn.getXFixed64sCount()).isEqualTo(0);
        assertThat(lpn.getXSfixed32sCount()).isEqualTo(0);
        assertThat(lpn.getXSfixed64sCount()).isEqualTo(0);
        assertThat(lpn.getXFloatsCount()).isEqualTo(0);
        assertThat(lpn.getXInt32sCount()).isEqualTo(0);
        assertThat(lpn.getXInt64sCount()).isEqualTo(0);
        assertThat(lpn.getXInt32sCount()).isEqualTo(0);
        assertThat(lpn.getXUint64sCount()).isEqualTo(0);
        assertThat(lpn.getXUint32sCount()).isEqualTo(0);
        assertThat(lpn.getXSint32sCount()).isEqualTo(0);
        assertThat(lpn.getXSint64sCount()).isEqualTo(0);

        assertException(() -> lpn.getEnum1At(0));
        assertException(() -> lpn.getXBoolAt(0));
        assertException(() -> lpn.getXDoubleAt(0));
        assertException(() -> lpn.getXFixed32At(0));
        assertException(() -> lpn.getXFixed64At(0));
        assertException(() -> lpn.getXSfixed32At(0));
        assertException(() -> lpn.getXSfixed64At(0));
        assertException(() -> lpn.getXFloatAt(0));
        assertException(() -> lpn.getXInt32At(0));
        assertException(() -> lpn.getXInt64At(0));
        assertException(() -> lpn.getXInt32At(0));
        assertException(() -> lpn.getXUint64At(0));
        assertException(() -> lpn.getXUint32At(0));
        assertException(() -> lpn.getXSint32At(0));
        assertException(() -> lpn.getXSint64At(0));

        lpn.addEnum1(Repeated.Enum.X2_1);
        lpn.addEnum1(Repeated.Enum.X2_2);
        lpn.addXBool(true);
        lpn.addXBool(false);
        lpn.addXDouble(1.0);
        lpn.addXDouble(2.0);
        lpn.addXFixed32(2);
        lpn.addXFixed32(3);
        lpn.addXFixed64(12345L);
        lpn.addXFixed64(12346L);
        lpn.addXSfixed32(-2);
        lpn.addXSfixed32(-3);
        lpn.addXSfixed64(-12345L);
        lpn.addXSfixed64(-12346L);
        lpn.addXFloat(1.2f);
        lpn.addXFloat(1.3f);
        lpn.addXInt32(4);
        lpn.addXInt32(5);
        lpn.addXInt64(126L);
        lpn.addXInt64(127L);
        lpn.addXUint32(40);
        lpn.addXUint32(41);
        lpn.addXUint64(1260L);
        lpn.addXUint64(1261L);
        lpn.addXSint32(-11);
        lpn.addXSint32(-12);
        lpn.addXSint64(-12L);
        lpn.addXSint64(-13L);

        pbn.addEnum1(RepeatedNumbers.Repeated.Enum.X2_1);
        pbn.addEnum1(RepeatedNumbers.Repeated.Enum.X2_2);
        pbn.addXBool(true);
        pbn.addXBool(false);
        pbn.addXDouble(1.0);
        pbn.addXDouble(2.0);
        pbn.addXFixed32(2);
        pbn.addXFixed32(3);
        pbn.addXFixed64(12345L);
        pbn.addXFixed64(12346L);
        pbn.addXSfixed32(-2);
        pbn.addXSfixed32(-3);
        pbn.addXSfixed64(-12345L);
        pbn.addXSfixed64(-12346L);
        pbn.addXFloat(1.2f);
        pbn.addXFloat(1.3f);
        pbn.addXInt32(4);
        pbn.addXInt32(5);
        pbn.addXInt64(126L);
        pbn.addXInt64(127L);
        pbn.addXUint32(40);
        pbn.addXUint32(41);
        pbn.addXUint64(1260L);
        pbn.addXUint64(1261L);
        pbn.addXSint32(-11);
        pbn.addXSint32(-12);
        pbn.addXSint64(-12L);
        pbn.addXSint64(-13L);

        assertThat(lpn.getEnum1sCount()).isEqualTo(2);
        assertThat(lpn.getXBoolsCount()).isEqualTo(2);
        assertThat(lpn.getXDoublesCount()).isEqualTo(2);
        assertThat(lpn.getXFixed32sCount()).isEqualTo(2);
        assertThat(lpn.getXFixed64sCount()).isEqualTo(2);
        assertThat(lpn.getXSfixed32sCount()).isEqualTo(2);
        assertThat(lpn.getXSfixed64sCount()).isEqualTo(2);
        assertThat(lpn.getXFloatsCount()).isEqualTo(2);
        assertThat(lpn.getXInt32sCount()).isEqualTo(2);
        assertThat(lpn.getXInt64sCount()).isEqualTo(2);
        assertThat(lpn.getXInt32sCount()).isEqualTo(2);
        assertThat(lpn.getXUint64sCount()).isEqualTo(2);
        assertThat(lpn.getXUint32sCount()).isEqualTo(2);
        assertThat(lpn.getXSint32sCount()).isEqualTo(2);
        assertThat(lpn.getXSint64sCount()).isEqualTo(2);

        assertThat(lpn.getEnum1At(0)).isEqualTo(Repeated.Enum.X2_1);
        assertThat(lpn.getEnum1At(1)).isEqualTo(Repeated.Enum.X2_2);
        assertThat(lpn.getXBoolAt(0)).isEqualTo(true);
        assertThat(lpn.getXBoolAt(1)).isEqualTo(false);
        assertThat(lpn.getXDoubleAt(0)).isEqualTo(1.0);
        assertThat(lpn.getXDoubleAt(1)).isEqualTo(2.0);
        assertThat(lpn.getXFixed32At(0)).isEqualTo(2);
        assertThat(lpn.getXFixed32At(1)).isEqualTo(3);
        assertThat(lpn.getXFixed64At(0)).isEqualTo(12345L);
        assertThat(lpn.getXFixed64At(1)).isEqualTo(12346L);
        assertThat(lpn.getXSfixed32At(0)).isEqualTo(-2);
        assertThat(lpn.getXSfixed32At(1)).isEqualTo(-3);
        assertThat(lpn.getXSfixed64At(0)).isEqualTo(-12345L);
        assertThat(lpn.getXSfixed64At(1)).isEqualTo(-12346L);
        assertThat(lpn.getXFloatAt(0)).isEqualTo(1.2f);
        assertThat(lpn.getXFloatAt(1)).isEqualTo(1.3f);
        assertThat(lpn.getXInt32At(0)).isEqualTo(4);
        assertThat(lpn.getXInt32At(1)).isEqualTo(5);
        assertThat(lpn.getXInt64At(0)).isEqualTo(126L);
        assertThat(lpn.getXInt64At(1)).isEqualTo(127L);
        assertThat(lpn.getXUint32At(0)).isEqualTo(40);
        assertThat(lpn.getXUint32At(1)).isEqualTo(41);
        assertThat(lpn.getXUint64At(0)).isEqualTo(1260L);
        assertThat(lpn.getXUint64At(1)).isEqualTo(1261L);
        assertThat(lpn.getXSint32At(0)).isEqualTo(-11);
        assertThat(lpn.getXSint32At(1)).isEqualTo(-12);
        assertThat(lpn.getXSint64At(0)).isEqualTo(-12L);
        assertThat(lpn.getXSint64At(1)).isEqualTo(-13L);

        assertThat(lpn.totalSize()).isEqualTo(pbn.build().getSerializedSize());

        lpn.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpn.totalSize());

        pbn.build().writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        Repeated parsed = new Repeated();
        parsed.parseFrom(bb1, bb1.readableBytes());

        assertThat(parsed.getEnum1At(0)).isEqualTo(Repeated.Enum.X2_1);
        assertThat(parsed.getEnum1At(1)).isEqualTo(Repeated.Enum.X2_2);
        assertThat(parsed.getXBoolAt(0)).isEqualTo(true);
        assertThat(parsed.getXBoolAt(1)).isEqualTo(false);
        assertThat(parsed.getXDoubleAt(0)).isEqualTo(1.0);
        assertThat(parsed.getXDoubleAt(1)).isEqualTo(2.0);
        assertThat(parsed.getXFixed32At(0)).isEqualTo(2);
        assertThat(parsed.getXFixed32At(1)).isEqualTo(3);
        assertThat(parsed.getXFixed64At(0)).isEqualTo(12345L);
        assertThat(parsed.getXFixed64At(1)).isEqualTo(12346L);
        assertThat(parsed.getXSfixed32At(0)).isEqualTo(-2);
        assertThat(parsed.getXSfixed32At(1)).isEqualTo(-3);
        assertThat(parsed.getXSfixed64At(0)).isEqualTo(-12345L);
        assertThat(parsed.getXSfixed64At(1)).isEqualTo(-12346L);
        assertThat(parsed.getXFloatAt(0)).isEqualTo(1.2f);
        assertThat(parsed.getXFloatAt(1)).isEqualTo(1.3f);
        assertThat(parsed.getXInt32At(0)).isEqualTo(4);
        assertThat(parsed.getXInt32At(1)).isEqualTo(5);
        assertThat(parsed.getXInt64At(0)).isEqualTo(126L);
        assertThat(parsed.getXInt64At(1)).isEqualTo(127L);
        assertThat(parsed.getXUint32At(0)).isEqualTo(40);
        assertThat(parsed.getXUint32At(1)).isEqualTo(41);
        assertThat(parsed.getXUint64At(0)).isEqualTo(1260L);
        assertThat(parsed.getXUint64At(1)).isEqualTo(1261L);
        assertThat(parsed.getXSint32At(0)).isEqualTo(-11);
        assertThat(parsed.getXSint32At(1)).isEqualTo(-12);
        assertThat(parsed.getXSint64At(0)).isEqualTo(-12L);
        assertThat(parsed.getXSint64At(1)).isEqualTo(-13L);
    }

    @Test
    public void testNumberFieldsPacked() throws Exception {
        RepeatedPacked lpn = new RepeatedPacked();
        RepeatedNumbers.RepeatedPacked.Builder pbn = RepeatedNumbers.RepeatedPacked.newBuilder();

        assertThat(lpn.getEnum1sCount()).isEqualTo(0);
        assertThat(lpn.getXBoolsCount()).isEqualTo(0);
        assertThat(lpn.getXDoublesCount()).isEqualTo(0);
        assertThat(lpn.getXFixed32sCount()).isEqualTo(0);
        assertThat(lpn.getXFixed64sCount()).isEqualTo(0);
        assertThat(lpn.getXSfixed32sCount()).isEqualTo(0);
        assertThat(lpn.getXSfixed64sCount()).isEqualTo(0);
        assertThat(lpn.getXFloatsCount()).isEqualTo(0);
        assertThat(lpn.getXInt32sCount()).isEqualTo(0);
        assertThat(lpn.getXInt64sCount()).isEqualTo(0);
        assertThat(lpn.getXInt32sCount()).isEqualTo(0);
        assertThat(lpn.getXUint64sCount()).isEqualTo(0);
        assertThat(lpn.getXUint32sCount()).isEqualTo(0);
        assertThat(lpn.getXSint32sCount()).isEqualTo(0);
        assertThat(lpn.getXSint64sCount()).isEqualTo(0);

        assertException(() -> lpn.getEnum1At(0));
        assertException(() -> lpn.getXBoolAt(0));
        assertException(() -> lpn.getXDoubleAt(0));
        assertException(() -> lpn.getXFixed32At(0));
        assertException(() -> lpn.getXFixed64At(0));
        assertException(() -> lpn.getXSfixed32At(0));
        assertException(() -> lpn.getXSfixed64At(0));
        assertException(() -> lpn.getXFloatAt(0));
        assertException(() -> lpn.getXInt32At(0));
        assertException(() -> lpn.getXInt64At(0));
        assertException(() -> lpn.getXInt32At(0));
        assertException(() -> lpn.getXUint64At(0));
        assertException(() -> lpn.getXUint32At(0));
        assertException(() -> lpn.getXSint32At(0));
        assertException(() -> lpn.getXSint64At(0));

        lpn.addEnum1(RepeatedPacked.Enum.X2_1);
        lpn.addEnum1(RepeatedPacked.Enum.X2_2);
        lpn.addXBool(true);
        lpn.addXBool(false);
        lpn.addXDouble(1.0);
        lpn.addXDouble(2.0);
        lpn.addXFixed32(2);
        lpn.addXFixed32(3);
        lpn.addXFixed64(12345L);
        lpn.addXFixed64(12346L);
        lpn.addXSfixed32(-2);
        lpn.addXSfixed32(-3);
        lpn.addXSfixed64(-12345L);
        lpn.addXSfixed64(-12346L);
        lpn.addXFloat(1.2f);
        lpn.addXFloat(1.3f);
        lpn.addXInt32(4);
        lpn.addXInt32(5);
        lpn.addXInt64(126L);
        lpn.addXInt64(127L);
        lpn.addXUint32(40);
        lpn.addXUint32(41);
        lpn.addXUint64(1260L);
        lpn.addXUint64(1261L);
        lpn.addXSint32(-11);
        lpn.addXSint32(-12);
        lpn.addXSint64(-12L);
        lpn.addXSint64(-13L);

        pbn.addEnum1(RepeatedNumbers.RepeatedPacked.Enum.X2_1);
        pbn.addEnum1(RepeatedNumbers.RepeatedPacked.Enum.X2_2);
        pbn.addXBool(true);
        pbn.addXBool(false);
        pbn.addXDouble(1.0);
        pbn.addXDouble(2.0);
        pbn.addXFixed32(2);
        pbn.addXFixed32(3);
        pbn.addXFixed64(12345L);
        pbn.addXFixed64(12346L);
        pbn.addXSfixed32(-2);
        pbn.addXSfixed32(-3);
        pbn.addXSfixed64(-12345L);
        pbn.addXSfixed64(-12346L);
        pbn.addXFloat(1.2f);
        pbn.addXFloat(1.3f);
        pbn.addXInt32(4);
        pbn.addXInt32(5);
        pbn.addXInt64(126L);
        pbn.addXInt64(127L);
        pbn.addXUint32(40);
        pbn.addXUint32(41);
        pbn.addXUint64(1260L);
        pbn.addXUint64(1261L);
        pbn.addXSint32(-11);
        pbn.addXSint32(-12);
        pbn.addXSint64(-12L);
        pbn.addXSint64(-13L);

        assertThat(lpn.getEnum1sCount()).isEqualTo(2);
        assertThat(lpn.getXBoolsCount()).isEqualTo(2);
        assertThat(lpn.getXDoublesCount()).isEqualTo(2);
        assertThat(lpn.getXFixed32sCount()).isEqualTo(2);
        assertThat(lpn.getXFixed64sCount()).isEqualTo(2);
        assertThat(lpn.getXSfixed32sCount()).isEqualTo(2);
        assertThat(lpn.getXSfixed64sCount()).isEqualTo(2);
        assertThat(lpn.getXFloatsCount()).isEqualTo(2);
        assertThat(lpn.getXInt32sCount()).isEqualTo(2);
        assertThat(lpn.getXInt64sCount()).isEqualTo(2);
        assertThat(lpn.getXInt32sCount()).isEqualTo(2);
        assertThat(lpn.getXUint64sCount()).isEqualTo(2);
        assertThat(lpn.getXUint32sCount()).isEqualTo(2);
        assertThat(lpn.getXSint32sCount()).isEqualTo(2);
        assertThat(lpn.getXSint64sCount()).isEqualTo(2);

        assertThat(lpn.getEnum1At(0)).isEqualTo(RepeatedPacked.Enum.X2_1);
        assertThat(lpn.getEnum1At(1)).isEqualTo(RepeatedPacked.Enum.X2_2);
        assertThat(lpn.getXBoolAt(0)).isEqualTo(true);
        assertThat(lpn.getXBoolAt(1)).isEqualTo(false);
        assertThat(lpn.getXDoubleAt(0)).isEqualTo(1.0);
        assertThat(lpn.getXDoubleAt(1)).isEqualTo(2.0);
        assertThat(lpn.getXFixed32At(0)).isEqualTo(2);
        assertThat(lpn.getXFixed32At(1)).isEqualTo(3);
        assertThat(lpn.getXFixed64At(0)).isEqualTo(12345L);
        assertThat(lpn.getXFixed64At(1)).isEqualTo(12346L);
        assertThat(lpn.getXSfixed32At(0)).isEqualTo(-2);
        assertThat(lpn.getXSfixed32At(1)).isEqualTo(-3);
        assertThat(lpn.getXSfixed64At(0)).isEqualTo(-12345L);
        assertThat(lpn.getXSfixed64At(1)).isEqualTo(-12346L);
        assertThat(lpn.getXFloatAt(0)).isEqualTo(1.2f);
        assertThat(lpn.getXFloatAt(1)).isEqualTo(1.3f);
        assertThat(lpn.getXInt32At(0)).isEqualTo(4);
        assertThat(lpn.getXInt32At(1)).isEqualTo(5);
        assertThat(lpn.getXInt64At(0)).isEqualTo(126L);
        assertThat(lpn.getXInt64At(1)).isEqualTo(127L);
        assertThat(lpn.getXUint32At(0)).isEqualTo(40);
        assertThat(lpn.getXUint32At(1)).isEqualTo(41);
        assertThat(lpn.getXUint64At(0)).isEqualTo(1260L);
        assertThat(lpn.getXUint64At(1)).isEqualTo(1261L);
        assertThat(lpn.getXSint32At(0)).isEqualTo(-11);
        assertThat(lpn.getXSint32At(1)).isEqualTo(-12);
        assertThat(lpn.getXSint64At(0)).isEqualTo(-12L);
        assertThat(lpn.getXSint64At(1)).isEqualTo(-13L);

        assertThat(lpn.totalSize()).isEqualTo(pbn.build().getSerializedSize());

        lpn.writeTo(bb1);
        assertThat(bb1.readableBytes()).isEqualTo(lpn.totalSize());

        pbn.build().writeTo(CodedOutputStream.newInstance(b2));

        assertThat(b2).isEqualTo(b1);

        RepeatedPacked parsed = new RepeatedPacked();
        parsed.parseFrom(bb1, bb1.readableBytes());

        assertThat(parsed.getEnum1At(0)).isEqualTo(RepeatedPacked.Enum.X2_1);
        assertThat(parsed.getEnum1At(1)).isEqualTo(RepeatedPacked.Enum.X2_2);
        assertThat(parsed.getXBoolAt(0)).isEqualTo(true);
        assertThat(parsed.getXBoolAt(1)).isEqualTo(false);
        assertThat(parsed.getXDoubleAt(0)).isEqualTo(1.0);
        assertThat(parsed.getXDoubleAt(1)).isEqualTo(2.0);
        assertThat(parsed.getXFixed32At(0)).isEqualTo(2);
        assertThat(parsed.getXFixed32At(1)).isEqualTo(3);
        assertThat(parsed.getXFixed64At(0)).isEqualTo(12345L);
        assertThat(parsed.getXFixed64At(1)).isEqualTo(12346L);
        assertThat(parsed.getXSfixed32At(0)).isEqualTo(-2);
        assertThat(parsed.getXSfixed32At(1)).isEqualTo(-3);
        assertThat(parsed.getXSfixed64At(0)).isEqualTo(-12345L);
        assertThat(parsed.getXSfixed64At(1)).isEqualTo(-12346L);
        assertThat(parsed.getXFloatAt(0)).isEqualTo(1.2f);
        assertThat(parsed.getXFloatAt(1)).isEqualTo(1.3f);
        assertThat(parsed.getXInt32At(0)).isEqualTo(4);
        assertThat(parsed.getXInt32At(1)).isEqualTo(5);
        assertThat(parsed.getXInt64At(0)).isEqualTo(126L);
        assertThat(parsed.getXInt64At(1)).isEqualTo(127L);
        assertThat(parsed.getXUint32At(0)).isEqualTo(40);
        assertThat(parsed.getXUint32At(1)).isEqualTo(41);
        assertThat(parsed.getXUint64At(0)).isEqualTo(1260L);
        assertThat(parsed.getXUint64At(1)).isEqualTo(1261L);
        assertThat(parsed.getXSint32At(0)).isEqualTo(-11);
        assertThat(parsed.getXSint32At(1)).isEqualTo(-12);
        assertThat(parsed.getXSint64At(0)).isEqualTo(-12L);
        assertThat(parsed.getXSint64At(1)).isEqualTo(-13L);
    }
}
