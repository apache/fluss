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

package org.apache.fluss.row.columnar;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ColumnVector} interface and its default methods. */
public class ColumnVectorTest {

    @Test
    public void testDefaultGetCapacity() {
        TestColumnVector columnVector = new TestColumnVector(10);
        assertThat(columnVector.getCapacity()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    public void testDefaultGetChildren() {
        TestColumnVector columnVector = new TestColumnVector(10);
        assertThat(columnVector.getChildren()).isNull();
    }

    @Test
    public void testIsNullAt() {
        boolean[] nulls = {false, true, false, true, false};
        TestColumnVector columnVector = new TestColumnVector(5, nulls);

        assertThat(columnVector.isNullAt(0)).isFalse();
        assertThat(columnVector.isNullAt(1)).isTrue();
        assertThat(columnVector.isNullAt(2)).isFalse();
        assertThat(columnVector.isNullAt(3)).isTrue();
        assertThat(columnVector.isNullAt(4)).isFalse();
    }

    @Test
    public void testCustomCapacity() {
        TestColumnVectorWithCapacity columnVector = new TestColumnVectorWithCapacity(100, 100);
        assertThat(columnVector.getCapacity()).isEqualTo(100);
    }

    @Test
    public void testCustomChildren() {
        ColumnVector[] children = new ColumnVector[3];
        children[0] = new TestColumnVector(10);
        children[1] = new TestColumnVector(10);
        children[2] = new TestColumnVector(10);

        TestColumnVectorWithChildren columnVector = new TestColumnVectorWithChildren(10, children);
        assertThat(columnVector.getChildren()).isEqualTo(children);
        assertThat(columnVector.getChildren()).hasSize(3);
    }

    @Test
    public void testBytesColumnVectorBytes() {
        byte[] data = {1, 2, 3, 4, 5};
        BytesColumnVector.Bytes bytes = new BytesColumnVector.Bytes(data, 0, 5);

        assertThat(bytes.data).isEqualTo(data);
        assertThat(bytes.offset).isEqualTo(0);
        assertThat(bytes.len).isEqualTo(5);
        assertThat(bytes.getBytes()).isEqualTo(data);
    }

    @Test
    public void testBytesColumnVectorBytesWithOffset() {
        byte[] data = {1, 2, 3, 4, 5, 6, 7, 8};
        BytesColumnVector.Bytes bytes = new BytesColumnVector.Bytes(data, 2, 4);

        assertThat(bytes.data).isEqualTo(data);
        assertThat(bytes.offset).isEqualTo(2);
        assertThat(bytes.len).isEqualTo(4);

        byte[] extracted = bytes.getBytes();
        assertThat(extracted).hasSize(4);
        assertThat(extracted).isEqualTo(new byte[] {3, 4, 5, 6});
    }

    @Test
    public void testBytesColumnVectorBytesNoCopy() {
        byte[] data = {1, 2, 3, 4, 5};
        BytesColumnVector.Bytes bytes = new BytesColumnVector.Bytes(data, 0, 5);

        // When offset is 0 and length equals data.length, should return same reference
        byte[] result = bytes.getBytes();
        assertThat(result).isSameAs(data);
    }

    // Helper test implementations

    private static class TestColumnVector implements ColumnVector {
        private final int size;
        private final boolean[] nulls;

        TestColumnVector(int size) {
            this(size, new boolean[size]);
        }

        TestColumnVector(int size, boolean[] nulls) {
            this.size = size;
            this.nulls = nulls;
        }

        @Override
        public boolean isNullAt(int i) {
            return i < nulls.length && nulls[i];
        }
    }

    private static class TestColumnVectorWithCapacity implements ColumnVector {
        private final int size;
        private final int capacity;

        TestColumnVectorWithCapacity(int size, int capacity) {
            this.size = size;
            this.capacity = capacity;
        }

        @Override
        public boolean isNullAt(int i) {
            return false;
        }

        @Override
        public int getCapacity() {
            return capacity;
        }
    }

    private static class TestColumnVectorWithChildren implements ColumnVector {
        private final int size;
        private final ColumnVector[] children;

        TestColumnVectorWithChildren(int size, ColumnVector[] children) {
            this.size = size;
            this.children = children;
        }

        @Override
        public boolean isNullAt(int i) {
            return false;
        }

        @Override
        public ColumnVector[] getChildren() {
            return children;
        }
    }
}
