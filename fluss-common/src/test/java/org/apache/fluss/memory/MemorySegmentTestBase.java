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

package org.apache.fluss.memory;

import org.apache.fluss.testutils.junit.parameterized.Parameters;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the access and transfer methods of the {@link MemorySegment}. */
public abstract class MemorySegmentTestBase {

    private final Random random = new Random();

    private final int pageSize;

    MemorySegmentTestBase(int pageSize) {
        this.pageSize = pageSize;
    }

    // ------------------------------------------------------------------------
    //  Access to primitives
    // ------------------------------------------------------------------------

    abstract MemorySegment createSegment(int size);

    // ------------------------------------------------------------------------
    //  Access to primitives
    // ------------------------------------------------------------------------

    protected MemorySegment segment = null;

    @BeforeEach
    public void before() {
        segment = createSegment(pageSize);
    }

    @TestTemplate
    public void testByteAccess() {
        // test exceptions.
        assertThatThrownBy(() -> segment.put(-1, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(pageSize, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(Integer.MIN_VALUE, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(pageSize))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // test expected correct behavior, random access.
        long seed = random.nextLong();
        random.setSeed(seed);
        for (int i = 0; i < pageSize; i++) {
            segment.put(i, (byte) random.nextInt());
        }

        random.setSeed(seed);
        for (int i = 0; i < pageSize; i++) {
            assertThat((byte) random.nextInt()).isEqualTo(segment.get(i));
        }

        // test expected correct behavior, random access.
        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            segment.put(pos, (byte) random.nextInt());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            assertThat((byte) random.nextInt()).isEqualTo(segment.get(pos));
        }
    }

    @TestTemplate
    public void testBooleanAccess() {
        // test exceptions.
        assertThatThrownBy(() -> segment.putBoolean(-1, false))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putBoolean(pageSize, false))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putBoolean(Integer.MAX_VALUE, false))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putBoolean(Integer.MIN_VALUE, false))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getBoolean(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getBoolean(pageSize))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getBoolean(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getBoolean(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // test expected correct behavior, sequential access.
        long seed = random.nextLong();
        random.setSeed(seed);
        for (int i = 0; i < pageSize; i++) {
            segment.putBoolean(i, random.nextBoolean());
        }

        random.setSeed(seed);
        for (int i = 0; i < pageSize; i++) {
            assertThat(random.nextBoolean()).isEqualTo(segment.getBoolean(i));
        }

        // test expected correct behavior, random access.
        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            segment.putBoolean(pos, random.nextBoolean());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            assertThat(random.nextBoolean()).isEqualTo(segment.getBoolean(pos));
        }
    }

    @TestTemplate
    public void testCharAccess() {
        // test exceptions.
        assertThatThrownBy(() -> segment.putShortNativeEndian(-1, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putShortNativeEndian(pageSize, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putShortNativeEndian(Integer.MAX_VALUE, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putShortNativeEndian(Integer.MIN_VALUE, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getShortNativeEndian(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getShortNativeEndian(pageSize))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getShortNativeEndian(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getShortNativeEndian(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // test expected correct behavior, sequential access.
        long seed = random.nextLong();
        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 2; i += 2) {
            segment.putShortNativeEndian(i, (short) random.nextInt());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 2; i += 2) {
            assertThat((short) random.nextInt()).isEqualTo(segment.getShortNativeEndian(i));
        }

        // test expected correct behavior, random access.
        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];
        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 1);

            if (occupied[pos] || occupied[pos + 1]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
            }

            segment.putShortNativeEndian(pos, (short) random.nextInt());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 1);

            if (occupied[pos] || occupied[pos + 1]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
            }

            assertThat((short) random.nextInt()).isEqualTo(segment.getShortNativeEndian(pos));
        }
    }

    @TestTemplate
    public void testIntAccess() {
        // test exceptions.
        assertThatThrownBy(() -> segment.putIntNativeEndian(-1, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putIntNativeEndian(pageSize, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putIntNativeEndian(Integer.MAX_VALUE - 3, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putIntNativeEndian(Integer.MIN_VALUE, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getIntNativeEndian(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getIntNativeEndian(pageSize))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getIntNativeEndian(Integer.MAX_VALUE - 3))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getIntNativeEndian(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // test expected correct behavior, sequential access.
        long seed = random.nextLong();
        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            segment.putIntNativeEndian(i, random.nextInt());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            assertThat(random.nextInt()).isEqualTo(segment.getIntNativeEndian(i));
        }

        // test expected correct behavior, random access.
        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            segment.putIntNativeEndian(pos, random.nextInt());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            assertThat(random.nextInt()).isEqualTo(segment.getIntNativeEndian(pos));
        }
    }

    @TestTemplate
    public void testLongAccess() {
        // test exceptions.
        assertThatThrownBy(() -> segment.putLongNativeEndian(-1, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putLongNativeEndian(pageSize, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putLongNativeEndian(Integer.MAX_VALUE - 7, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putLongNativeEndian(Integer.MIN_VALUE, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getLongNativeEndian(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getLongNativeEndian(pageSize))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getLongNativeEndian(Integer.MAX_VALUE - 7))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getLongNativeEndian(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // test expected correct behavior, sequential access.
        long seed = random.nextLong();
        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            segment.putLongNativeEndian(i, random.nextLong());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            assertThat(random.nextLong()).isEqualTo(segment.getLongNativeEndian(i));
        }

        // test expected correct behavior, random access.
        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            segment.putLongNativeEndian(pos, random.nextLong());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            assertThat(random.nextLong()).isEqualTo(segment.getLongNativeEndian(pos));
        }
    }

    @TestTemplate
    public void testFloatAccess() {
        // test exceptions.
        assertThatThrownBy(() -> segment.putFloatNativeEndian(-1, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putFloatNativeEndian(pageSize, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putFloatNativeEndian(Integer.MAX_VALUE - 3, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putFloatNativeEndian(Integer.MIN_VALUE, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getFloatNativeEndian(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getFloatNativeEndian(pageSize))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getFloatNativeEndian(Integer.MAX_VALUE - 3))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getFloatNativeEndian(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // test expected correct behavior, sequential access
        long seed = random.nextLong();
        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            segment.putFloatNativeEndian(i, random.nextFloat());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            assertThat(random.nextFloat())
                    .isCloseTo(
                            segment.getFloatNativeEndian(i),
                            Offset.offset(Float.parseFloat("0.0")));
        }

        // test expected correct behavior, random access
        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            segment.putFloatNativeEndian(pos, random.nextFloat());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            assertThat(random.nextFloat())
                    .isCloseTo(
                            segment.getFloatNativeEndian(pos),
                            Offset.offset(Float.parseFloat("0.0")));
        }
    }

    @TestTemplate
    public void testDoubleAccess() {
        // test exceptions.
        assertThatThrownBy(() -> segment.putDoubleNativeEndian(-1, 0.0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putDoubleNativeEndian(pageSize, 0.0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putDoubleNativeEndian(Integer.MAX_VALUE - 7, 0.0))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putDoubleNativeEndian(Integer.MIN_VALUE, 0.0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getDoubleNativeEndian(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getDoubleNativeEndian(pageSize))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getDoubleNativeEndian(Integer.MAX_VALUE - 7))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getDoubleNativeEndian(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // test expected correct behavior, sequential access.
        long seed = random.nextLong();
        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            segment.putDoubleNativeEndian(i, random.nextDouble());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            assertThat(random.nextDouble())
                    .isCloseTo(segment.getDoubleNativeEndian(i), Offset.offset(0.0));
        }

        // test expected correct behavior, random access.
        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            segment.putDoubleNativeEndian(pos, random.nextDouble());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            assertThat(random.nextDouble())
                    .isCloseTo(segment.getDoubleNativeEndian(pos), Offset.offset(0.0));
        }
    }

    @TestTemplate
    public void testEqualTo() {
        MemorySegment seg1 = createSegment(pageSize);
        MemorySegment seg2 = createSegment(pageSize);

        byte[] referenceArray = new byte[pageSize];
        seg1.put(0, referenceArray);
        seg2.put(0, referenceArray);

        int i = new Random().nextInt(pageSize - 8);

        seg1.put(i, (byte) 10);
        assertThat(seg1.equalTo(seg2, i, i, 9)).isFalse();

        seg1.put(i, (byte) 0);
        assertThat(seg1.equalTo(seg2, i, i, 9)).isTrue();

        seg1.put(i + 8, (byte) 10);
        assertThat(seg1.equalTo(seg2, i, i, 9)).isFalse();
    }

    @TestTemplate
    public void testCopyUnsafeIndexOutOfBounds() {
        byte[] bytes = new byte[pageSize];

        assertThatThrownBy(() -> segment.copyToUnsafe(1, bytes, 0, pageSize))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.copyFromUnsafe(1, bytes, 0, pageSize))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    // ------------------------------------------------------------------------
    //  Bulk Byte Movements
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testBulkBytePutExceptions() {
        byte[] bytes = new byte[pageSize / 4 + (pageSize % 4)];
        random.nextBytes(bytes);

        // wrong positions into memory segment.
        assertThatThrownBy(() -> segment.put(-1, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(-1, bytes, 4, 5))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(Integer.MIN_VALUE, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(pageSize, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(pageSize, bytes, 6, 44))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(pageSize, bytes, 6, 44))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(pageSize - bytes.length + 1, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(pageSize - 5, bytes, 3, 6))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE, bytes, 10, 20))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE - bytes.length + 1, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE - 11, bytes, 11, 11))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(3 * (pageSize / 4) + 1, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(3 * (pageSize / 4) + 2, bytes, 0, bytes.length - 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(7 * (pageSize / 8) + 1, bytes, 0, bytes.length / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(0, bytes, -1, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(0, bytes, -1, bytes.length + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(0, bytes, Integer.MIN_VALUE, bytes.length))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(-2, bytes, -1, bytes.length / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @TestTemplate
    public void testBulkByteGetExceptions() {
        byte[] bytes = new byte[pageSize / 4];

        assertThatThrownBy(() -> segment.get(-1, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(-1, bytes, 4, 5))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(Integer.MIN_VALUE, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(Integer.MIN_VALUE, bytes, 4, 5))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(pageSize, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(pageSize, bytes, 6, 44))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(pageSize - bytes.length + 1, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(pageSize - 5, bytes, 3, 6))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(Integer.MAX_VALUE, bytes, 10, 20))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(Integer.MAX_VALUE - bytes.length + 1, bytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(Integer.MAX_VALUE - 11, bytes, 11, 11))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(-2, bytes, -1, bytes.length / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @TestTemplate
    public void testBulkByteAccess() {
        // test expected correct behavior with default offset / length.
        long seed = random.nextLong();
        random.setSeed(seed);
        byte[] src = new byte[pageSize / 8];
        for (int i = 0; i < 8; i++) {
            random.nextBytes(src);
            segment.put(i * (pageSize / 8), src);
        }

        random.setSeed(seed);
        byte[] expected = new byte[pageSize / 8];
        byte[] actual = new byte[pageSize / 8];
        for (int i = 0; i < 8; i++) {
            random.nextBytes(expected);
            segment.get(i * (pageSize / 8), actual);

            assertThat(actual).isEqualTo(expected);
        }

        // test expected correct behavior with specific offset / length.
        expected = new byte[pageSize];
        random.nextBytes(expected);

        for (int i = 0; i < 16; i++) {
            segment.put(i * (pageSize / 16), expected, i * (pageSize / 16), pageSize / 16);
        }

        actual = new byte[pageSize];
        for (int i = 0; i < 16; i++) {
            segment.get(i * (pageSize / 16), actual, i * (pageSize / 16), pageSize / 16);
        }
        assertThat(actual).isEqualTo(expected);

        // put segments of various lengths to various positions.
        expected = new byte[pageSize];
        segment.put(0, expected, 0, pageSize);

        for (int i = 0; i < 200; i++) {
            int numBytes = random.nextInt(pageSize - 10) + 1;
            int pos = random.nextInt(pageSize - numBytes + 1);

            byte[] data = new byte[(random.nextInt(3) + 1) * numBytes];
            int dataStartPos = random.nextInt(data.length - numBytes + 1);

            random.nextBytes(data);

            // copy to the expected
            System.arraycopy(data, dataStartPos, expected, pos, numBytes);

            // put to the memory segment
            segment.put(pos, data, dataStartPos, numBytes);
        }

        byte[] validation = new byte[pageSize];
        segment.get(0, validation);
        assertThat(validation).isEqualTo(expected);

        // get segments with various contents.
        byte[] contents = new byte[pageSize];
        random.nextBytes(contents);
        segment.put(0, contents);

        for (int i = 0; i < 200; i++) {
            int numBytes = random.nextInt(pageSize / 8) + 1;
            int pos = random.nextInt(pageSize - numBytes + 1);

            byte[] data = new byte[(random.nextInt(3) + 1) * numBytes];
            int dataStartPos = random.nextInt(data.length - numBytes + 1);

            segment.get(pos, data, dataStartPos, numBytes);

            expected = Arrays.copyOfRange(contents, pos, pos + numBytes);
            validation = Arrays.copyOfRange(data, dataStartPos, dataStartPos + numBytes);
            assertThat(validation).isEqualTo(expected);
        }
    }

    // ------------------------------------------------------------------------
    //  Writing / Reading to/from DataInput / DataOutput
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testDataInputOutput() throws IOException {
        byte[] contents = new byte[pageSize];
        random.nextBytes(contents);
        segment.put(0, contents);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream(pageSize);
        DataOutputStream out = new DataOutputStream(buffer);

        // write the segment in chunks into the stream.
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(200);
            len = Math.min(len, pageSize - pos);
            segment.get(out, pos, len);
            pos += len;
        }

        // verify that we wrote the same bytes
        byte[] result = buffer.toByteArray();
        assertThat(result).isEqualTo(contents);

        // re-read the bytes into a new memory segment.
        MemorySegment reader = createSegment(pageSize);
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(result));

        pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(200);
            len = Math.min(len, pageSize - pos);
            reader.put(in, pos, len);
            pos += len;
        }

        byte[] targetBuffer = new byte[pageSize];
        reader.get(0, targetBuffer);

        assertThat(targetBuffer).isEqualTo(contents);
    }

    @TestTemplate
    public void testDataInputOutputOutOfBounds() {
        final int segmentSize = 52;
        // segment with random contents
        MemorySegment seg = createSegment(segmentSize);
        byte[] bytes = new byte[segmentSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        // out of bounds when writing.
        DataOutputStream out = new DataOutputStream(new ByteArrayOutputStream());
        assertThatThrownBy(() -> seg.get(out, -1, segmentSize / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> seg.get(out, segmentSize, segmentSize / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> seg.get(out, -segmentSize, segmentSize / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> seg.get(out, Integer.MIN_VALUE, segmentSize / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> seg.get(out, Integer.MAX_VALUE, segmentSize / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // out of bounds when reading.
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(new byte[segmentSize]));
        assertThatThrownBy(() -> seg.put(in, -1, segmentSize / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> seg.put(in, segmentSize, segmentSize / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> seg.put(in, -segmentSize, segmentSize / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> seg.put(in, Integer.MIN_VALUE, segmentSize / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> seg.put(in, Integer.MAX_VALUE, segmentSize / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @TestTemplate
    public void testDataInputOutputStreamUnderflowOverflow() {
        final int segmentSize = 1337;
        // segment with random contents.
        MemorySegment seg = createSegment(segmentSize);
        byte[] bytes = new byte[segmentSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        // a stream that we cannot fully write to.
        DataOutputStream out =
                new DataOutputStream(
                        new OutputStream() {

                            int bytesSoFar = 0;

                            @Override
                            public void write(int b) throws IOException {
                                bytesSoFar++;
                                if (bytesSoFar > segmentSize / 2) {
                                    throw new IOException("overflow");
                                }
                            }
                        });

        assertThatThrownBy(
                        () -> {
                            int pos = 0;
                            while (pos < pageSize) {
                                int len = random.nextInt(segmentSize / 10);
                                len = Math.min(len, pageSize - pos);
                                seg.get(out, pos, len);
                                pos += len;
                            }
                        })
                .isInstanceOf(IOException.class);

        DataInputStream in =
                new DataInputStream(new ByteArrayInputStream(new byte[segmentSize / 2]));

        assertThatThrownBy(
                        () -> {
                            int pos = 0;
                            while (pos < pageSize) {
                                int len = random.nextInt(segmentSize / 10);
                                len = Math.min(len, pageSize - pos);
                                seg.put(in, pos, len);
                                pos += len;
                            }
                        })
                .isInstanceOf(IOException.class);
    }

    // ------------------------------------------------------------------------
    //  ByteBuffer Ops
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testGetByteBuffer() {
        testByteBufferGet(false);
        testByteBufferGet(true);
    }

    @TestTemplate
    public void testByteBufferGetReadOnly() {
        assertThatThrownBy(() -> testByteBufferGetReadOnly(false))
                .isInstanceOf(ReadOnlyBufferException.class);
        assertThatThrownBy(() -> testByteBufferGetReadOnly(true))
                .isInstanceOf(ReadOnlyBufferException.class);
    }

    @TestTemplate
    public void testByteBufferPut() {
        testByteBufferPut(false);
        testByteBufferPut(true);
    }

    // ------------------------------------------------------------------------
    //  ByteBuffer Ops on sliced byte buffers
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testSlicedByteBufferGet() {
        testSlicedByteBufferGet(false);
        testSlicedByteBufferGet(true);
    }

    @TestTemplate
    public void testSlicedByteBufferPut() {
        testSlicedByteBufferPut(false);
        testSlicedByteBufferPut(true);
    }

    // ------------------------------------------------------------------------
    //  ByteBuffer overflow / underflow and out of bounds
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testByteBufferOutOfBounds() {
        final int bbCapacity = pageSize / 10;
        final int[] validOffsets = {0, 1, pageSize / 10 * 9};
        final int[] invalidOffsets = {
            -1, pageSize + 1, -pageSize, Integer.MAX_VALUE, Integer.MIN_VALUE
        };

        final int[] validLengths = {0, 1, bbCapacity, pageSize};
        final int[] invalidLengths = {-1, -pageSize, Integer.MAX_VALUE, Integer.MIN_VALUE};

        for (ByteBuffer bb :
                new ByteBuffer[] {
                    ByteBuffer.allocate(bbCapacity), ByteBuffer.allocateDirect(bbCapacity)
                }) {
            for (int off : validOffsets) {
                for (int len : invalidLengths) {
                    assertThatThrownBy(() -> segment.put(off, bb, len))
                            .isInstanceOfAny(
                                    IndexOutOfBoundsException.class,
                                    BufferUnderflowException.class);
                    assertThatThrownBy(() -> segment.get(off, bb, len))
                            .isInstanceOfAny(
                                    IndexOutOfBoundsException.class, BufferOverflowException.class);

                    // position/limit may not have changed.
                    assertThat(bb.position()).isEqualTo(0);
                    assertThat(bb.limit()).isEqualTo(bb.capacity());
                }
            }

            for (int off : invalidOffsets) {
                for (int len : validLengths) {
                    assertThatThrownBy(() -> segment.put(off, bb, len))
                            .isInstanceOfAny(
                                    IndexOutOfBoundsException.class,
                                    BufferUnderflowException.class);
                    assertThatThrownBy(() -> segment.get(off, bb, len))
                            .isInstanceOfAny(
                                    IndexOutOfBoundsException.class, BufferOverflowException.class);

                    // position/limit may not have changed.
                    assertThat(bb.position()).isEqualTo(0);
                    assertThat(bb.limit()).isEqualTo(bb.capacity());
                }
            }

            for (int off : validOffsets) {
                for (int len : validLengths) {
                    if (off + len > pageSize) {
                        assertThatThrownBy(() -> segment.put(off, bb, len))
                                .isInstanceOfAny(
                                        IndexOutOfBoundsException.class,
                                        BufferUnderflowException.class);
                        assertThatThrownBy(() -> segment.get(off, bb, len))
                                .isInstanceOfAny(
                                        IndexOutOfBoundsException.class,
                                        BufferOverflowException.class);

                        // position/limit may not have changed.
                        assertThat(bb.position()).isEqualTo(0);
                        assertThat(bb.limit()).isEqualTo(bb.capacity());
                    }
                }
            }
        }
    }

    @TestTemplate
    public void testByteBufferOverflowUnderflow() {
        final int bbCapacity = pageSize / 10;
        ByteBuffer bb = ByteBuffer.allocate(bbCapacity);

        assertThatThrownBy(() -> segment.get(pageSize / 5, bb, pageSize / 10 + 2))
                .isInstanceOf(BufferOverflowException.class);

        // position / limit should not have been modified.
        assertThat(bb.position()).isEqualTo(0);
        assertThat(bb.limit()).isEqualTo(bb.capacity());

        assertThatThrownBy(() -> segment.put(pageSize / 5, bb, pageSize / 10 + 2))
                .isInstanceOf(BufferUnderflowException.class);

        // position / limit should not have been modified.
        assertThat(bb.position()).isEqualTo(0);
        assertThat(bb.limit()).isEqualTo(bb.capacity());

        int pos = bb.capacity() / 3;
        int limit = 2 * bb.capacity() / 3;
        bb.limit(limit);
        bb.position(pos);

        assertThatThrownBy(() -> segment.get(20, bb, bb.capacity() / 3 + 3))
                .isInstanceOf(BufferOverflowException.class);

        // position / limit should not have been modified.
        assertThat(bb.position()).isEqualTo(pos);
        assertThat(bb.limit()).isEqualTo(limit);

        assertThatThrownBy(() -> segment.put(20, bb, bb.capacity() / 3 + 3))
                .isInstanceOf(BufferUnderflowException.class);

        // position / limit should not have been modified.
        assertThat(bb.position()).isEqualTo(pos);
        assertThat(bb.limit()).isEqualTo(limit);
    }

    // ------------------------------------------------------------------------
    //  Comparing and swapping
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testCompareBytes() {
        final byte[] bytes1 = new byte[pageSize];
        final byte[] bytes2 = new byte[pageSize];

        final int stride = pageSize / 255;
        final int shift = 16666;

        for (int i = 0; i < pageSize; i++) {
            byte val = (byte) ((i / stride) & 0xff);
            bytes1[i] = val;

            if (i + shift < bytes2.length) {
                bytes2[i + shift] = val;
            }
        }

        MemorySegment seg1 = createSegment(pageSize);
        MemorySegment seg2 = createSegment(pageSize);
        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        for (int i = 0; i < 1000; i++) {
            int pos1 = random.nextInt(bytes1.length);
            int pos2 = random.nextInt(bytes2.length);

            int len =
                    Math.min(
                            Math.min(bytes1.length - pos1, bytes2.length - pos2),
                            random.nextInt(pageSize / 50));

            int cmp = seg1.compare(seg2, pos1, pos2, len);

            if (pos1 < pos2 - shift) {
                assertThat(cmp <= 0).isTrue();
            } else {
                assertThat(cmp >= 0).isTrue();
            }
        }
    }

    @TestTemplate
    public void testCompareBytesWithDifferentLength() {
        final byte[] bytes1 = new byte[] {'a', 'b', 'c'};
        final byte[] bytes2 = new byte[] {'a', 'b', 'c', 'd'};

        MemorySegment seg1 = createSegment(4);
        MemorySegment seg2 = createSegment(4);
        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        assertThat(seg1.compare(seg2, 0, 0, 3, 4)).isLessThan(0);
        assertThat(seg1.compare(seg2, 0, 0, 3, 3)).isEqualTo(0);
        assertThat(seg1.compare(seg2, 0, 0, 3, 2)).isGreaterThan(0);
        // test non-zero offset
        assertThat(seg1.compare(seg2, 1, 1, 2, 3)).isLessThan(0);
        assertThat(seg1.compare(seg2, 1, 1, 2, 2)).isEqualTo(0);
        assertThat(seg1.compare(seg2, 1, 1, 2, 1)).isGreaterThan(0);
    }

    @TestTemplate
    public void testSwapBytes() {
        final int halfPageSize = pageSize / 2;

        final byte[] bytes1 = new byte[pageSize];
        final byte[] bytes2 = new byte[halfPageSize];

        Arrays.fill(bytes2, (byte) 1);

        MemorySegment seg1 = createSegment(pageSize);
        MemorySegment seg2 = createSegment(halfPageSize);
        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        // wap the second half of the first segment with the second segment

        int pos = 0;
        while (pos < halfPageSize) {
            int len = random.nextInt(pageSize / 40);
            len = Math.min(len, halfPageSize - pos);
            seg1.swapBytes(new byte[len], seg2, pos + halfPageSize, pos, len);
            pos += len;
        }

        // the second segment should now be all zeros, the first segment should have one in its
        // second half

        for (int i = 0; i < halfPageSize; i++) {
            assertThat(seg1.get(i)).isEqualTo((byte) 0);
            assertThat(seg2.get(i)).isEqualTo((byte) 0);
            assertThat(seg1.get(i + halfPageSize)).isEqualTo((byte) 1);
        }
    }

    // ------------------------------------------------------------------------
    //  Miscellaneous
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testByteBufferWrapping() {
        ByteBuffer buf1 = segment.wrap(13, 47);
        assertThat(buf1.position()).isEqualTo(13);
        assertThat(buf1.limit()).isEqualTo(60);
        assertThat(buf1.remaining()).isEqualTo(47);

        ByteBuffer buf2 = segment.wrap(500, 267);
        assertThat(buf2.position()).isEqualTo(500);
        assertThat(buf2.limit()).isEqualTo(767);
        assertThat(buf2.remaining()).isEqualTo(267);

        ByteBuffer buf3 = segment.wrap(0, 1024);
        assertThat(buf3.position()).isEqualTo(0);
        assertThat(buf3.limit()).isEqualTo(1024);
        assertThat(buf3.remaining()).isEqualTo(1024);

        // verify that operations on the byte buffer are correctly reflected
        // in the memory segment.
        buf3.order(ByteOrder.LITTLE_ENDIAN);
        buf3.putInt(112, 651797651);
        assertThat(segment.getInt(112)).isEqualTo(651797651);

        buf3.order(ByteOrder.BIG_ENDIAN);
        buf3.putInt(187, 992288337);
        assertThat(segment.getIntBigEndian(187)).isEqualTo(992288337);

        assertThatThrownBy(() -> segment.wrap(-1, 20))
                .isInstanceOfAny(IndexOutOfBoundsException.class, IllegalArgumentException.class);
        assertThatThrownBy(() -> segment.wrap(10, -20))
                .isInstanceOfAny(IndexOutOfBoundsException.class, IllegalArgumentException.class);

        // after freeing, no wrapping should be possible anymore.
        segment.free();

        assertThatThrownBy(() -> segment.wrap(13, 47)).isInstanceOf(IllegalStateException.class);

        // existing wraps should stay valid after freeing.
        buf3.order(ByteOrder.LITTLE_ENDIAN);
        buf3.putInt(112, 651797651);
        assertThat(buf3.getInt(112)).isEqualTo(651797651);
        buf3.order(ByteOrder.BIG_ENDIAN);
        buf3.putInt(187, 992288337);
        assertThat(buf3.getInt(187)).isEqualTo(992288337);
    }

    @TestTemplate
    public void testSizeAndFreeing() {
        // a segment without an owner has a null owner.
        final int segmentSize = 651;
        MemorySegment seg = createSegment(segmentSize);

        assertThat(seg.size()).isEqualTo(segmentSize);
        assertThat(seg.isFreed()).isFalse();

        seg.free();
        assertThat(seg.isFreed()).isTrue();
        assertThat(seg.size()).isEqualTo(segmentSize);
    }

    @AfterEach
    public void after() {
        if (!segment.isFreed()) {
            segment.free();
        }
    }

    // ------------------------------------------------------------------------
    //  Methods
    // ------------------------------------------------------------------------

    private void testByteBufferGet(boolean directBuffer) {
        MemorySegment seg = createSegment(pageSize);
        byte[] bytes = new byte[pageSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        ByteBuffer target =
                directBuffer
                        ? ByteBuffer.allocateDirect(3 * pageSize)
                        : ByteBuffer.allocate(3 * pageSize);
        target.position(2 * pageSize);

        // transfer the segment in chunks into the byte buffer.
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.get(pos, target, len);
            pos += len;
        }

        // verify that we wrote the same bytes.
        byte[] result = new byte[pageSize];
        target.position(2 * pageSize);
        target.get(result);
        assertThat(result).isEqualTo(bytes);
    }

    private void testByteBufferGetReadOnly(boolean directBuffer) throws ReadOnlyBufferException {
        MemorySegment seg = createSegment(pageSize);
        ByteBuffer target =
                (directBuffer ? ByteBuffer.allocateDirect(pageSize) : ByteBuffer.allocate(pageSize))
                        .asReadOnlyBuffer();
        seg.get(0, target, pageSize);
    }

    private void testByteBufferPut(boolean directBuffer) {
        byte[] bytes = new byte[pageSize];
        random.nextBytes(bytes);

        ByteBuffer source =
                directBuffer ? ByteBuffer.allocateDirect(pageSize) : ByteBuffer.allocate(pageSize);

        source.put(bytes);
        source.clear();

        MemorySegment seg = createSegment(3 * pageSize);

        int offset = 2 * pageSize;

        // transfer the segment in chunks into the byte buffer.
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.put(offset + pos, source, len);
            pos += len;
        }

        // verify that we read the same bytes.
        byte[] result = new byte[pageSize];
        seg.get(offset, result);
        assertThat(result).isEqualTo(bytes);
    }

    private void testSlicedByteBufferGet(boolean directBuffer) {
        MemorySegment seg = createSegment(pageSize);
        byte[] bytes = new byte[pageSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        ByteBuffer target =
                directBuffer
                        ? ByteBuffer.allocateDirect(pageSize + 49)
                        : ByteBuffer.allocate(pageSize + 49);

        target.position(19).limit(19 + pageSize);

        ByteBuffer slicedTarget = target.slice();

        // transfer the segment in chunks into the byte buffer.
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.get(pos, slicedTarget, len);
            pos += len;
        }

        // verify that we wrote the same bytes.
        byte[] result = new byte[pageSize];
        target.position(19);
        target.get(result);
        assertThat(result).isEqualTo(bytes);
    }

    private void testSlicedByteBufferPut(boolean directBuffer) {
        byte[] bytes = new byte[pageSize + 49];
        random.nextBytes(bytes);

        ByteBuffer source =
                directBuffer
                        ? ByteBuffer.allocateDirect(pageSize + 49)
                        : ByteBuffer.allocate(pageSize + 49);

        source.put(bytes);
        source.position(19).limit(19 + pageSize);
        ByteBuffer slicedSource = source.slice();

        MemorySegment seg = createSegment(3 * pageSize);

        final int offset = 2 * pageSize;

        // transfer the segment in chunks into the byte buffer.
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.put(offset + pos, slicedSource, len);
            pos += len;
        }

        // verify that we read the same bytes.
        byte[] result = new byte[pageSize];
        seg.get(offset, result);

        byte[] expected = Arrays.copyOfRange(bytes, 19, 19 + pageSize);
        assertThat(result).isEqualTo(expected);
    }

    // ------------------------------------------------------------------------
    //  Parametrization to run with different segment sizes
    // ------------------------------------------------------------------------

    @Parameters(name = "segment-size = {0}")
    public static Collection<Object[]> executionModes() {
        return Arrays.asList(
                new Object[] {32 * 1024}, new Object[] {4 * 1024}, new Object[] {512 * 1024});
    }
}
