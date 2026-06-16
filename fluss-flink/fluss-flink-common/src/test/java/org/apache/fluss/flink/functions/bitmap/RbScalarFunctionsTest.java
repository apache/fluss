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

package org.apache.fluss.flink.functions.bitmap;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the six Phase 1 scalar bitmap functions. */
class RbScalarFunctionsTest {

    // -------------------------------------------------------------------------
    // rb_cardinality
    // -------------------------------------------------------------------------

    @Test
    void testCardinalityBasic() throws IOException {
        RbCardinalityFunction fn = new RbCardinalityFunction();
        byte[] bytes = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3, 4, 5));
        assertThat(fn.eval(bytes)).isEqualTo(5L);
    }

    @Test
    void testCardinalityNullInput() throws IOException {
        assertThat(new RbCardinalityFunction().eval(null)).isNull();
    }

    @Test
    void testCardinalityEmptyInput() throws IOException {
        assertThat(new RbCardinalityFunction().eval(new byte[0])).isNull();
    }

    @Test
    void testCardinalityEmptyBitmap() throws IOException {
        RbCardinalityFunction fn = new RbCardinalityFunction();
        byte[] bytes = BitmapUtils.toBytes(new RoaringBitmap());
        assertThat(fn.eval(bytes)).isEqualTo(0L);
    }

    // -------------------------------------------------------------------------
    // rb_build
    // -------------------------------------------------------------------------

    @Test
    void testBuildBasic() throws IOException {
        RbBuildFunction fn = new RbBuildFunction();
        byte[] result = fn.eval(1, 2, 3, 2); // duplicate 2 ignored
        assertThat(result).isNotNull();
        RoaringBitmap bitmap = BitmapUtils.fromBytes(result);
        assertThat(bitmap.getLongCardinality()).isEqualTo(3L);
        assertThat(bitmap.contains(1)).isTrue();
        assertThat(bitmap.contains(3)).isTrue();
    }

    @Test
    void testBuildNullInputs() throws IOException {
        RbBuildFunction fn = new RbBuildFunction();
        assertThat(fn.eval((Integer[]) null)).isNull();
        assertThat(fn.eval(new Integer[0])).isNull();
        assertThat(fn.eval(null, null)).isNull(); // all null values
    }

    @Test
    void testBuildNullValuesIgnored() throws IOException {
        RbBuildFunction fn = new RbBuildFunction();
        byte[] result = fn.eval(1, null, 3);
        RoaringBitmap bitmap = BitmapUtils.fromBytes(result);
        assertThat(bitmap.getLongCardinality()).isEqualTo(2L);
        assertThat(bitmap.contains(2)).isFalse();
    }

    // -------------------------------------------------------------------------
    // rb_contains
    // -------------------------------------------------------------------------

    @Test
    void testContainsTrue() throws IOException {
        RbContainsFunction fn = new RbContainsFunction();
        byte[] bytes = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(10, 20, 30));
        assertThat(fn.eval(bytes, 20)).isTrue();
    }

    @Test
    void testContainsFalse() throws IOException {
        RbContainsFunction fn = new RbContainsFunction();
        byte[] bytes = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(10, 20, 30));
        assertThat(fn.eval(bytes, 99)).isFalse();
    }

    @Test
    void testContainsNullBitmap() throws IOException {
        assertThat(new RbContainsFunction().eval(null, 1)).isNull();
    }

    @Test
    void testContainsNullValue() throws IOException {
        byte[] bytes = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2));
        assertThat(new RbContainsFunction().eval(bytes, null)).isNull();
    }

    // -------------------------------------------------------------------------
    // rb_to_array
    // -------------------------------------------------------------------------

    @Test
    void testToArrayBasic() throws IOException {
        RbToArrayFunction fn = new RbToArrayFunction();
        byte[] bytes = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 1, 2));
        Integer[] result = fn.eval(bytes);
        assertThat(result).isNotNull();
        // RoaringBitmap returns values in ascending order
        assertThat(Arrays.asList(result)).containsExactly(1, 2, 3);
    }

    @Test
    void testToArrayNullInput() throws IOException {
        assertThat(new RbToArrayFunction().eval(null)).isNull();
    }

    @Test
    void testToArrayEmptyBitmap() throws IOException {
        RbToArrayFunction fn = new RbToArrayFunction();
        byte[] bytes = BitmapUtils.toBytes(new RoaringBitmap());
        Integer[] result = fn.eval(bytes);
        assertThat(result).isNotNull().isEmpty();
    }

    @Test
    void testToArrayReturnTypeIsIntegerArray() throws IOException {
        // Verifies the return type is Integer[] (ARRAY<INT>), not int[] (BYTES)
        RbToArrayFunction fn = new RbToArrayFunction();
        byte[] bytes = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(5));
        Object result = fn.eval(bytes);
        assertThat(result).isInstanceOf(Integer[].class);
    }

    // -------------------------------------------------------------------------
    // rb_or (scalar)
    // -------------------------------------------------------------------------

    @Test
    void testOrBasic() throws IOException {
        RbOrFunction fn = new RbOrFunction();
        byte[] left = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));
        byte[] right = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 4, 5));
        byte[] result = fn.eval(left, right);
        RoaringBitmap union = BitmapUtils.fromBytes(result);
        assertThat(union.getLongCardinality()).isEqualTo(5L);
    }

    @Test
    void testOrLeftNull() throws IOException {
        RbOrFunction fn = new RbOrFunction();
        byte[] right = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2));
        assertThat(fn.eval(null, right)).isEqualTo(right);
    }

    @Test
    void testOrRightNull() throws IOException {
        RbOrFunction fn = new RbOrFunction();
        byte[] left = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2));
        assertThat(fn.eval(left, null)).isEqualTo(left);
    }

    @Test
    void testOrBothNull() throws IOException {
        assertThat(new RbOrFunction().eval(null, null)).isNull();
    }

    // -------------------------------------------------------------------------
    // rb_and (scalar)
    // -------------------------------------------------------------------------

    @Test
    void testAndBasic() throws IOException {
        RbAndFunction fn = new RbAndFunction();
        byte[] left = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));
        byte[] right = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3, 4));
        byte[] result = fn.eval(left, right);
        RoaringBitmap intersection = BitmapUtils.fromBytes(result);
        assertThat(intersection.getLongCardinality()).isEqualTo(2L);
        assertThat(intersection.contains(2)).isTrue();
        assertThat(intersection.contains(3)).isTrue();
    }

    @Test
    void testAndLeftNull() throws IOException {
        byte[] right = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2));
        assertThat(new RbAndFunction().eval(null, right)).isNull();
    }

    @Test
    void testAndRightNull() throws IOException {
        byte[] left = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2));
        assertThat(new RbAndFunction().eval(left, null)).isNull();
    }

    @Test
    void testAndEmptyIntersectionReturnsBytes() throws IOException {
        // Unlike rb_and_agg, scalar rb_and returns bytes even for empty intersection
        // because both inputs were explicitly provided
        RbAndFunction fn = new RbAndFunction();
        byte[] left = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2));
        byte[] right = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 4));
        byte[] result = fn.eval(left, right);
        assertThat(result).isNotNull();
        RoaringBitmap intersection = BitmapUtils.fromBytes(result);
        assertThat(intersection.isEmpty()).isTrue();
    }
}
