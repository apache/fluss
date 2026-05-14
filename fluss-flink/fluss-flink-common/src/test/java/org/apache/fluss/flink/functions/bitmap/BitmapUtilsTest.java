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

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link BitmapUtils}. */
class BitmapUtilsTest {

    @Test
    void testNullInputToBytes() throws IOException {
        assertThat(BitmapUtils.toBytes(null)).isNull();
    }

    @Test
    void testNullInputFromBytes() throws IOException {
        assertThat(BitmapUtils.fromBytes(null)).isNull();
    }

    @Test
    void testEmptyBitmapRoundTrip() throws IOException {
        RoaringBitmap bitmap = new RoaringBitmap();
        byte[] bytes = BitmapUtils.toBytes(bitmap);
        assertThat(bytes).isNotNull();
        RoaringBitmap result = BitmapUtils.fromBytes(bytes);
        assertThat(result).isNotNull();
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void testKnownValuesRoundTrip() throws IOException {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(1);
        bitmap.add(100);
        bitmap.add(1000);
        bitmap.add(Integer.MAX_VALUE);

        byte[] bytes = BitmapUtils.toBytes(bitmap);
        assertThat(bytes).isNotNull();

        RoaringBitmap result = BitmapUtils.fromBytes(bytes);
        assertThat(result).isNotNull();
        assertThat(result.getLongCardinality()).isEqualTo(4L);
        assertThat(result.contains(1)).isTrue();
        assertThat(result.contains(100)).isTrue();
        assertThat(result.contains(1000)).isTrue();
        assertThat(result.contains(Integer.MAX_VALUE)).isTrue();
        assertThat(result.contains(2)).isFalse();
    }

    @Test
    void testLargeCardinality() throws IOException {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < 100_000; i++) {
            bitmap.add(i);
        }
        byte[] bytes = BitmapUtils.toBytes(bitmap);
        RoaringBitmap result = BitmapUtils.fromBytes(bytes);
        assertThat(result.getLongCardinality()).isEqualTo(100_000L);
    }

    @Test
    void testFormatCompatibleWithServerSerialization() throws IOException {
        // This test verifies that our ByteBuffer-based serialization produces bytes
        // that can be deserialized back correctly — same guarantee the server relies on.
        RoaringBitmap original = RoaringBitmap.bitmapOf(42, 100, 200, 300);
        byte[] bytes = BitmapUtils.toBytes(original);
        RoaringBitmap restored = BitmapUtils.fromBytes(bytes);
        assertThat(restored).isEqualTo(original);
    }
}
