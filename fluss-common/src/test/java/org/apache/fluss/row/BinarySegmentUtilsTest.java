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

package org.apache.fluss.row;

import org.apache.fluss.memory.MemorySegment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.row.BinarySegmentUtils}. */
public class BinarySegmentUtilsTest {

    @Test
    public void testCopy() {
        // test copy the content of the latter Seg
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = MemorySegment.wrap(new byte[] {0, 2, 5});
        segments[1] = MemorySegment.wrap(new byte[] {6, 12, 15});

        byte[] bytes = BinarySegmentUtils.copyToBytes(segments, 4, 2);
        assertThat(bytes).isEqualTo(new byte[] {12, 15});
    }

    @Test
    public void testEquals() {
        // test copy the content of the latter Seg
        MemorySegment[] segments1 = new MemorySegment[3];
        segments1[0] = MemorySegment.wrap(new byte[] {0, 2, 5});
        segments1[1] = MemorySegment.wrap(new byte[] {6, 12, 15});
        segments1[2] = MemorySegment.wrap(new byte[] {1, 1, 1});

        MemorySegment[] segments2 = new MemorySegment[2];
        segments2[0] = MemorySegment.wrap(new byte[] {6, 0, 2, 5});
        segments2[1] = MemorySegment.wrap(new byte[] {6, 12, 15, 18});

        assertThat(BinarySegmentUtils.equalsMultiSegments(segments1, 0, segments2, 0, 0)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 3)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 6)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 7)).isFalse();
    }

    @Test
    public void testBoundaryEquals() {
        // test var segs
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegment.wrap(new byte[32]);
        segments1[1] = MemorySegment.wrap(new byte[32]);
        MemorySegment[] segments2 = new MemorySegment[3];
        segments2[0] = MemorySegment.wrap(new byte[16]);
        segments2[1] = MemorySegment.wrap(new byte[16]);
        segments2[2] = MemorySegment.wrap(new byte[16]);

        segments1[0].put(9, (byte) 1);
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 14, 14)).isFalse();
        segments2[1].put(7, (byte) 1);
        assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 14, 14)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 2, segments2, 16, 14)).isTrue();
        assertThat(BinarySegmentUtils.equals(segments1, 2, segments2, 16, 16)).isTrue();

        segments2[2].put(7, (byte) 1);
        assertThat(BinarySegmentUtils.equals(segments1, 2, segments2, 32, 14)).isTrue();
    }

    @Test
    public void testBoundaryCopy() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegment.wrap(new byte[32]);
        segments1[1] = MemorySegment.wrap(new byte[32]);
        segments1[0].put(15, (byte) 5);
        segments1[1].put(15, (byte) 6);

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegment.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 0, bytes, 0, 64);
            assertThat(BinarySegmentUtils.equals(segments1, 0, segments2, 0, 64)).isTrue();
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegment.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 32, bytes, 0, 14);
            assertThat(BinarySegmentUtils.equals(segments1, 32, segments2, 0, 14)).isTrue();
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegment.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 34, bytes, 0, 14);
            assertThat(BinarySegmentUtils.equals(segments1, 34, segments2, 0, 14)).isTrue();
        }
    }

    @Test
    public void testFind() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegment.wrap(new byte[32]);
        segments1[1] = MemorySegment.wrap(new byte[32]);
        MemorySegment[] segments2 = new MemorySegment[3];
        segments2[0] = MemorySegment.wrap(new byte[16]);
        segments2[1] = MemorySegment.wrap(new byte[16]);
        segments2[2] = MemorySegment.wrap(new byte[16]);

        assertThat(BinarySegmentUtils.find(segments1, 34, 0, segments2, 0, 0)).isEqualTo(34);
        assertThat(BinarySegmentUtils.find(segments1, 34, 0, segments2, 0, 15)).isEqualTo(-1);
    }
}
