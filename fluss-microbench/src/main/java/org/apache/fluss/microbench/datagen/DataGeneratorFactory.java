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

package org.apache.fluss.microbench.datagen;

import org.apache.fluss.microbench.datagen.builtin.RoaringBitmapGenerator;

import java.util.List;
import java.util.Map;
import java.util.Random;

/** Creates row-field generators from scenario configuration. */
public final class DataGeneratorFactory {

    private static final String ALPHANUMERIC =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private DataGeneratorFactory() {}

    public static FieldGenerator create(String type, Map<String, Object> params, long seed) {
        Random rng = new Random(seed);
        switch (type) {
            case "sequential":
                {
                    int start = number(params, "start", 0).intValue();
                    int end = number(params, "end", 0).intValue();
                    int step = number(params, "step", 1).intValue();
                    long range = (long) end - start;
                    if (range <= 0) {
                        throw new IllegalArgumentException("sequential requires end > start");
                    }
                    return index -> start + (int) ((index * step) % range);
                }
            case "random-int":
                {
                    int min = number(params, "min", 0).intValue();
                    int max = number(params, "max", Integer.MAX_VALUE).intValue();
                    if (max <= min) {
                        throw new IllegalArgumentException("random-int requires max > min");
                    }
                    return index -> min + rng.nextInt(max - min);
                }
            case "random-long":
                {
                    long min = number(params, "min", 0L).longValue();
                    long max = number(params, "max", Long.MAX_VALUE).longValue();
                    if (max <= min) {
                        throw new IllegalArgumentException("random-long requires max > min");
                    }
                    return index -> min + (rng.nextLong() >>> 1) % (max - min);
                }
            case "random-float":
                {
                    float min = number(params, "min", 0f).floatValue();
                    float max = number(params, "max", 1f).floatValue();
                    return index -> (float) (min + rng.nextDouble() * (max - min));
                }
            case "random-double":
                {
                    double min = number(params, "min", 0d).doubleValue();
                    double max = number(params, "max", 1d).doubleValue();
                    return index -> min + rng.nextDouble() * (max - min);
                }
            case "random-boolean":
                {
                    double trueRatio = number(params, "true-ratio", 0.5d).doubleValue();
                    return index -> rng.nextDouble() < trueRatio;
                }
            case "random-bytes":
                {
                    int length = number(params, "length", 16).intValue();
                    return index -> {
                        byte[] bytes = new byte[length];
                        rng.nextBytes(bytes);
                        return bytes;
                    };
                }
            case "random-string":
                return randomString(params, rng);
            case "timestamp-now":
                {
                    long offsetMs = number(params, "offset-ms", 0L).longValue();
                    return index -> System.currentTimeMillis() + offsetMs;
                }
            case "enum":
                {
                    @SuppressWarnings("unchecked")
                    List<String> values = (List<String>) params.get("values");
                    if (values == null || values.isEmpty()) {
                        throw new IllegalArgumentException("enum requires non-empty values");
                    }
                    return index -> values.get(rng.nextInt(values.size()));
                }
            case "roaring-bitmap-32":
                return new RoaringBitmapGenerator(false, params, seed);
            case "roaring-bitmap-64":
                return new RoaringBitmapGenerator(true, params, seed);
            default:
                throw new IllegalArgumentException("Unknown generator type: " + type);
        }
    }

    private static Number number(Map<String, Object> params, String key, Number fallback) {
        return (Number) params.getOrDefault(key, fallback);
    }

    @SuppressWarnings("unchecked")
    private static FieldGenerator randomString(Map<String, Object> params, Random rng) {
        Object length = params.getOrDefault("length", 16);
        int minLength;
        int maxLength;
        if (length instanceof Map) {
            Map<String, Object> range = (Map<String, Object>) length;
            minLength = ((Number) range.get("min")).intValue();
            maxLength = ((Number) range.get("max")).intValue();
        } else {
            minLength = ((Number) length).intValue();
            maxLength = minLength;
        }
        char[] charset = ((String) params.getOrDefault("charset", ALPHANUMERIC)).toCharArray();
        return index -> {
            int size =
                    minLength == maxLength
                            ? minLength
                            : minLength + rng.nextInt(maxLength - minLength + 1);
            char[] chars = new char[size];
            for (int i = 0; i < size; i++) {
                chars[i] = charset[rng.nextInt(charset.length)];
            }
            return new String(chars);
        };
    }
}
