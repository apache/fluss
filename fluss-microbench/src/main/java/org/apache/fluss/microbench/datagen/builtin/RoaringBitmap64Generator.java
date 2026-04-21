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
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Generates serialized 64-bit RoaringBitmap byte arrays. */
public class RoaringBitmap64Generator implements FieldGenerator {

    private int size;
    private long rangeMin;
    private long rangeMax;
    private String distribution;
    private double overlap;
    private Random rng;
    private ZipfDistribution zipf;

    @Override
    public String type() {
        return "roaring-bitmap-64";
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, Object> params, long seed) {
        this.size = ((Number) params.getOrDefault("size", 100)).intValue();
        if (size < 0) {
            throw new IllegalArgumentException(
                    "roaring-bitmap-64: 'size' must be non-negative, got " + size);
        }
        Object rangeObj = params.get("range");
        if (rangeObj instanceof List) {
            List<Number> range = (List<Number>) rangeObj;
            this.rangeMin = range.get(0).longValue();
            this.rangeMax = range.get(1).longValue();
        } else {
            this.rangeMin = 0L;
            this.rangeMax = Long.MAX_VALUE;
        }
        if (rangeMax <= rangeMin) {
            throw new IllegalArgumentException(
                    "roaring-bitmap-64: range max ("
                            + rangeMax
                            + ") must be greater than min ("
                            + rangeMin
                            + ")");
        }
        this.distribution = (String) params.getOrDefault("distribution", "uniform");
        this.overlap = ((Number) params.getOrDefault("overlap", 0.0)).doubleValue();
        this.rng = new Random(seed);
        if ("zipf".equals(this.distribution)) {
            long range = rangeMax - rangeMin;
            if (range < 0) {
                throw new IllegalArgumentException(
                        "roaring-bitmap-64: range too large for zipf distribution"
                                + " (rangeMax - rangeMin overflows long)");
            }
            int zipfRange = (int) Math.min(range, Integer.MAX_VALUE - 1);
            this.zipf = new ZipfDistribution(zipfRange, 1.1);
        }
    }

    @Override
    public Object generate(long index) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        long range = rangeMax - rangeMin;

        switch (distribution) {
            case "sequential":
                for (int i = 0; i < size; i++) {
                    long offset = (long) (((index * size + i) * (1.0 - overlap)) % range);
                    if (offset < 0) {
                        offset += range;
                    }
                    bitmap.add(rangeMin + offset);
                }
                break;
            case "zipf":
                for (int i = 0; i < size; i++) {
                    bitmap.add(rangeMin + zipf.sample() - 1);
                }
                break;
            default: // uniform
                for (int i = 0; i < size; i++) {
                    long raw = rng.nextLong() % range;
                    if (raw < 0) {
                        raw += range;
                    }
                    bitmap.add(rangeMin + raw);
                }
                break;
        }

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            bitmap.serialize(new DataOutputStream(baos));
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Roaring64Bitmap", e);
        }
    }
}
