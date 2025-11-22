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

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.fluss.utils.ProtoCodecUtils}. */
public class ProtoCodecUtilsTest {

    private final byte[] b = new byte[4096];
    private final ByteBuf bb = Unpooled.wrappedBuffer(b);

    @BeforeEach
    public void setup() {
        bb.clear();
        Arrays.fill(b, (byte) 0);
    }

    @ParameterizedTest
    @ValueSource(
            ints = {
                Integer.MIN_VALUE,
                -1000,
                -100,
                -2,
                -1,
                0,
                1,
                10,
                100,
                1000,
                (int) 1e4,
                (int) 1e5,
                (int) 1e7,
                Integer.MAX_VALUE
            })
    public void testVarInt(int i) throws Exception {
        ProtoCodecUtils.writeVarInt(bb, i);

        CodedInputStream is = CodedInputStream.newInstance(b);
        int res = is.readRawVarint32();
        assertThat(res).isEqualTo(i);

        res = ProtoCodecUtils.readVarInt(bb);
        assertThat(res).isEqualTo(i);

        assertThat(ProtoCodecUtils.computeVarIntSize(i))
                .isEqualTo(CodedOutputStream.computeInt32SizeNoTag(i));
    }

    @ParameterizedTest
    @ValueSource(
            longs = {
                Long.MIN_VALUE,
                -10000000,
                -100,
                -2,
                -1,
                0,
                1,
                10,
                100,
                10000000,
                (long) 2e18,
                (long) 2e32,
                (long) 2e43,
                (long) 2e57,
                Long.MAX_VALUE
            })
    public void testVarInt64(long i) throws Exception {
        ProtoCodecUtils.writeVarInt64(bb, i);

        CodedInputStream is = CodedInputStream.newInstance(b);
        long res = is.readRawVarint64();
        assertThat(res).isEqualTo(i);

        res = ProtoCodecUtils.readVarInt64(bb);
        assertThat(res).isEqualTo(i);

        assertThat(ProtoCodecUtils.computeVarInt64Size(i))
                .isEqualTo(CodedOutputStream.computeInt64SizeNoTag(i));
    }

    @ParameterizedTest
    @ValueSource(
            ints = {Integer.MIN_VALUE, -1000, -100, -2, -1, 0, 1, 10, 100, 1000, Integer.MAX_VALUE})
    public void testSignedVarInt(int i) throws Exception {
        ProtoCodecUtils.writeSignedVarInt(bb, i);

        CodedInputStream is = CodedInputStream.newInstance(b);
        int res = is.readSInt32();
        assertThat(res).isEqualTo(i);

        res = ProtoCodecUtils.readSignedVarInt(bb);
        assertThat(res).isEqualTo(i);

        assertThat(ProtoCodecUtils.computeSignedVarIntSize(i))
                .isEqualTo(CodedOutputStream.computeSInt32SizeNoTag(i));
    }

    @ParameterizedTest
    @ValueSource(
            longs = {
                Long.MIN_VALUE,
                -10000000,
                -100,
                -2,
                -1,
                0,
                1,
                10,
                100,
                10000000,
                Long.MAX_VALUE
            })
    public void testSignedVarInt64(long i) throws Exception {
        ProtoCodecUtils.writeSignedVarInt64(bb, i);

        CodedInputStream is = CodedInputStream.newInstance(b);
        long res = is.readSInt64();
        assertThat(res).isEqualTo(i);

        res = ProtoCodecUtils.readSignedVarInt64(bb);
        assertThat(res).isEqualTo(i);

        assertThat(ProtoCodecUtils.computeSignedVarInt64Size(i))
                .isEqualTo(CodedOutputStream.computeSInt64SizeNoTag(i));
    }

    @ParameterizedTest
    @ValueSource(
            ints = {Integer.MIN_VALUE, -1000, -100, -2, -1, 0, 1, 10, 100, 1000, Integer.MAX_VALUE})
    public void testFixedInt32(int i) throws Exception {
        ProtoCodecUtils.writeFixedInt32(bb, i);

        CodedInputStream is = CodedInputStream.newInstance(b);
        int res = is.readFixed32();
        assertThat(res).isEqualTo(i);

        res = ProtoCodecUtils.readFixedInt32(bb);
        assertThat(res).isEqualTo(i);
    }

    @ParameterizedTest
    @ValueSource(
            longs = {
                Long.MIN_VALUE,
                -10000000,
                -100,
                -2,
                -1,
                0,
                1,
                10,
                100,
                10000000,
                Long.MAX_VALUE
            })
    public void testFixedInt64(long i) throws Exception {
        ProtoCodecUtils.writeFixedInt64(bb, i);

        CodedInputStream is = CodedInputStream.newInstance(b);
        long res = is.readFixed64();
        assertThat(res).isEqualTo(i);

        res = ProtoCodecUtils.readFixedInt64(bb);
        assertThat(res).isEqualTo(i);
    }

    @ParameterizedTest
    @ValueSource(
            floats = {
                Float.MIN_VALUE,
                -1000.0f,
                -100.0f,
                -2.f,
                -1.f,
                0f,
                1f,
                10f,
                100f,
                1000f,
                Float.MAX_VALUE
            })
    public void testFloat(float i) throws Exception {
        ProtoCodecUtils.writeFloat(bb, i);

        CodedInputStream is = CodedInputStream.newInstance(b);
        float res = is.readFloat();
        assertThat(res).isEqualTo(i);

        res = ProtoCodecUtils.readFloat(bb);
        assertThat(res).isEqualTo(i);
    }

    @ParameterizedTest
    @ValueSource(
            doubles = {
                Double.MIN_VALUE,
                -10000000.0,
                -100.0,
                -2.0,
                -1.0,
                0.0,
                1.0,
                10.0,
                100.0,
                10000000.0,
                Double.MAX_VALUE
            })
    public void testDouble(double i) throws Exception {
        ProtoCodecUtils.writeDouble(bb, i);

        CodedInputStream is = CodedInputStream.newInstance(b);
        double res = is.readDouble();
        assertThat(res).isEqualTo(i);

        res = ProtoCodecUtils.readDouble(bb);
        assertThat(res).isEqualTo(i);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "hello",
                "UTF16 Ελληνικά Русский 日本語",
                "Neque porro quisquam est qui dolorem ipsum"
            })
    public void testString(String s) throws Exception {
        byte[] sb = s.getBytes(StandardCharsets.UTF_8);
        assertThat(ProtoCodecUtils.computeStringUTF8Size(s)).isEqualTo(sb.length);

        ProtoCodecUtils.writeVarInt(bb, sb.length);
        int idx = bb.writerIndex();
        ProtoCodecUtils.writeString(bb, s, sb.length);

        CodedInputStream is = CodedInputStream.newInstance(b);
        assertThat(is.readString()).isEqualTo(s);

        assertThat(ProtoCodecUtils.readVarInt(bb)).isEqualTo(sb.length);
        assertThat(ProtoCodecUtils.readString(bb, idx, sb.length)).isEqualTo(s);

        assertThat(
                        ProtoCodecUtils.computeVarIntSize(sb.length)
                                + ProtoCodecUtils.computeStringUTF8Size(s))
                .isEqualTo(CodedOutputStream.computeStringSizeNoTag(s));
    }

    // ----------------------- IEEE 754 Special Values Binary Serialization Tests
    // -----------------------

    /**
     * Tests that Float.NaN survives binary serialization round-trip with exact bit pattern
     * preservation. This validates FR-006: System MUST preserve Float.NaN through binary
     * serialization.
     *
     * <p>Uses {@code Float.floatToRawIntBits} to verify exact bit-level equality, ensuring no NaN
     * canonicalization occurs during serialization/deserialization.
     */
    @org.junit.jupiter.api.Test
    public void testFloatNaNRoundTrip() throws Exception {
        bb.clear();
        float original = Float.NaN;
        ProtoCodecUtils.writeFloat(bb, original);

        bb.resetReaderIndex();
        float deserialized = ProtoCodecUtils.readFloat(bb);

        assertThat(Float.floatToRawIntBits(original))
                .as("Float.NaN bit pattern should be preserved through serialization round-trip")
                .isEqualTo(Float.floatToRawIntBits(deserialized));
    }

    /**
     * Tests that Double.NaN survives binary serialization round-trip with exact bit pattern
     * preservation. This validates FR-007: System MUST preserve Double.NaN through binary
     * serialization.
     */
    @org.junit.jupiter.api.Test
    public void testDoubleNaNRoundTrip() throws Exception {
        bb.clear();
        double original = Double.NaN;
        ProtoCodecUtils.writeDouble(bb, original);

        bb.resetReaderIndex();
        double deserialized = ProtoCodecUtils.readDouble(bb);

        assertThat(Double.doubleToRawLongBits(original))
                .as("Double.NaN bit pattern should be preserved through serialization round-trip")
                .isEqualTo(Double.doubleToRawLongBits(deserialized));
    }

    /**
     * Tests that Float.POSITIVE_INFINITY survives binary serialization round-trip. This validates
     * FR-008: System MUST preserve Float.POSITIVE_INFINITY through binary serialization.
     */
    @org.junit.jupiter.api.Test
    public void testFloatPositiveInfinityRoundTrip() throws Exception {
        bb.clear();
        float original = Float.POSITIVE_INFINITY;
        ProtoCodecUtils.writeFloat(bb, original);

        bb.resetReaderIndex();
        float deserialized = ProtoCodecUtils.readFloat(bb);

        assertThat(deserialized)
                .as("Float.POSITIVE_INFINITY should be preserved through serialization")
                .isEqualTo(Float.POSITIVE_INFINITY);
    }

    /**
     * Tests that Float.NEGATIVE_INFINITY survives binary serialization round-trip. This validates
     * FR-008: System MUST preserve Float.NEGATIVE_INFINITY through binary serialization.
     */
    @org.junit.jupiter.api.Test
    public void testFloatNegativeInfinityRoundTrip() throws Exception {
        bb.clear();
        float original = Float.NEGATIVE_INFINITY;
        ProtoCodecUtils.writeFloat(bb, original);

        bb.resetReaderIndex();
        float deserialized = ProtoCodecUtils.readFloat(bb);

        assertThat(deserialized)
                .as("Float.NEGATIVE_INFINITY should be preserved through serialization")
                .isEqualTo(Float.NEGATIVE_INFINITY);
    }

    /**
     * Tests that Double.POSITIVE_INFINITY survives binary serialization round-trip. This validates
     * FR-009: System MUST preserve Double.POSITIVE_INFINITY through binary serialization.
     */
    @org.junit.jupiter.api.Test
    public void testDoublePositiveInfinityRoundTrip() throws Exception {
        bb.clear();
        double original = Double.POSITIVE_INFINITY;
        ProtoCodecUtils.writeDouble(bb, original);

        bb.resetReaderIndex();
        double deserialized = ProtoCodecUtils.readDouble(bb);

        assertThat(deserialized)
                .as("Double.POSITIVE_INFINITY should be preserved through serialization")
                .isEqualTo(Double.POSITIVE_INFINITY);
    }

    /**
     * Tests that Double.NEGATIVE_INFINITY survives binary serialization round-trip. This validates
     * FR-009: System MUST preserve Double.NEGATIVE_INFINITY through binary serialization.
     */
    @org.junit.jupiter.api.Test
    public void testDoubleNegativeInfinityRoundTrip() throws Exception {
        bb.clear();
        double original = Double.NEGATIVE_INFINITY;
        ProtoCodecUtils.writeDouble(bb, original);

        bb.resetReaderIndex();
        double deserialized = ProtoCodecUtils.readDouble(bb);

        assertThat(deserialized)
                .as("Double.NEGATIVE_INFINITY should be preserved through serialization")
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    /**
     * Tests that custom NaN bit patterns are preserved without canonicalization. This is an
     * advanced test verifying bit-level precision for signaling vs quiet NaN variants.
     *
     * <p>IEEE 754 allows 2^23-2 distinct NaN bit patterns for Float. This test ensures Fluss
     * preserves the exact payload bits, not just the "NaN" semantic.
     */
    @org.junit.jupiter.api.Test
    public void testNaNBitPatternPreservation() throws Exception {
        bb.clear();
        // Create a custom NaN with specific payload bits (signaling NaN pattern)
        float customNaN = Float.intBitsToFloat(0x7fc12345);
        ProtoCodecUtils.writeFloat(bb, customNaN);

        bb.resetReaderIndex();
        float deserialized = ProtoCodecUtils.readFloat(bb);

        int originalBits = Float.floatToRawIntBits(customNaN);
        int deserializedBits = Float.floatToRawIntBits(deserialized);

        assertThat(deserializedBits)
                .as("Custom NaN bit pattern (including payload) should be preserved exactly")
                .isEqualTo(originalBits);

        // Also verify it's still recognized as NaN
        assertThat(Float.isNaN(deserialized))
                .as("Deserialized value should still be recognized as NaN")
                .isTrue();
    }
}
