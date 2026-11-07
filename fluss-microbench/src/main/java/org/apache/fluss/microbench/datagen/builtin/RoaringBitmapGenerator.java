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

package org.apache.fluss.microbench.datagen.builtin;

import org.apache.fluss.microbench.datagen.FieldGenerator;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Generates serialized 32-bit or 64-bit RoaringBitmap values. */
public class RoaringBitmapGenerator implements FieldGenerator {

    private final boolean wide;
    private final int size;
    private final long rangeMin;
    private final long range;
    private final String distribution;
    private final double overlap;
    private final Random random;
    private final ZipfDistribution zipf;

    @SuppressWarnings("unchecked")
    public RoaringBitmapGenerator(boolean wide, Map<String, Object> params, long seed) {
        this.wide = wide;
        this.size = ((Number) params.getOrDefault("size", 100)).intValue();
        if (size < 0) {
            throw new IllegalArgumentException("bitmap size must be non-negative");
        }
        Object configuredRange = params.get("range");
        List<Number> bounds =
                configuredRange instanceof List ? (List<Number>) configuredRange : null;
        this.rangeMin = bounds == null ? 0 : bounds.get(0).longValue();
        long rangeMax =
                bounds == null
                        ? (wide ? Long.MAX_VALUE : Integer.MAX_VALUE)
                        : bounds.get(1).longValue();
        this.range = rangeMax - rangeMin;
        if (range <= 0) {
            throw new IllegalArgumentException("bitmap range max must be greater than min");
        }
        this.distribution = (String) params.getOrDefault("distribution", "uniform");
        this.overlap = ((Number) params.getOrDefault("overlap", 0.0)).doubleValue();
        this.random = new Random(seed);
        this.zipf =
                "zipf".equals(distribution)
                        ? new ZipfDistribution((int) Math.min(range, Integer.MAX_VALUE - 1), 1.1)
                        : null;
    }

    @Override
    public Object generate(long index) {
        RoaringBitmap bitmap32 = wide ? null : new RoaringBitmap();
        Roaring64Bitmap bitmap64 = wide ? new Roaring64Bitmap() : null;
        for (int i = 0; i < size; i++) {
            long offset;
            switch (distribution) {
                case "sequential":
                    offset = (long) ((index * size + i) * (1.0 - overlap)) % range;
                    break;
                case "zipf":
                    offset = zipf.sample() - 1;
                    break;
                default:
                    long value = random.nextLong();
                    offset = (wide ? value : value >>> 1) % range;
            }
            long value = rangeMin + (offset < 0 ? offset + range : offset);
            if (wide) {
                bitmap64.add(value);
            } else {
                bitmap32.add((int) value);
            }
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            if (wide) {
                bitmap64.serialize(output);
            } else {
                bitmap32.serialize(output);
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize RoaringBitmap", e);
        }
    }
}
