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

import org.apache.fluss.microbench.datagen.builtin.EnumGenerator;
import org.apache.fluss.microbench.datagen.builtin.RandomBooleanGenerator;
import org.apache.fluss.microbench.datagen.builtin.RandomBytesGenerator;
import org.apache.fluss.microbench.datagen.builtin.RandomDoubleGenerator;
import org.apache.fluss.microbench.datagen.builtin.RandomFloatGenerator;
import org.apache.fluss.microbench.datagen.builtin.RandomIntGenerator;
import org.apache.fluss.microbench.datagen.builtin.RandomLongGenerator;
import org.apache.fluss.microbench.datagen.builtin.RandomStringGenerator;
import org.apache.fluss.microbench.datagen.builtin.RoaringBitmap32Generator;
import org.apache.fluss.microbench.datagen.builtin.RoaringBitmap64Generator;
import org.apache.fluss.microbench.datagen.builtin.SequentialIntGenerator;
import org.apache.fluss.microbench.datagen.builtin.TimestampNowGenerator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Factory that creates {@link FieldGenerator} instances by type name. */
public final class DataGeneratorFactory {

    private static final Map<String, Supplier<FieldGenerator>> REGISTRY = new LinkedHashMap<>();

    static {
        REGISTRY.put("sequential", SequentialIntGenerator::new);
        REGISTRY.put("random-int", RandomIntGenerator::new);
        REGISTRY.put("random-long", RandomLongGenerator::new);
        REGISTRY.put("random-float", RandomFloatGenerator::new);
        REGISTRY.put("random-double", RandomDoubleGenerator::new);
        REGISTRY.put("random-boolean", RandomBooleanGenerator::new);
        REGISTRY.put("random-string", RandomStringGenerator::new);
        REGISTRY.put("random-bytes", RandomBytesGenerator::new);
        REGISTRY.put("timestamp-now", TimestampNowGenerator::new);
        REGISTRY.put("enum", EnumGenerator::new);
        REGISTRY.put("roaring-bitmap-32", RoaringBitmap32Generator::new);
        REGISTRY.put("roaring-bitmap-64", RoaringBitmap64Generator::new);
    }

    private DataGeneratorFactory() {}

    /**
     * Creates and configures a {@link FieldGenerator} of the given type.
     *
     * @param type the generator type name
     * @param params configuration parameters
     * @param seed random seed
     * @return a configured generator
     * @throws IllegalArgumentException if the type is unknown
     */
    public static FieldGenerator create(String type, Map<String, Object> params, long seed) {
        Supplier<FieldGenerator> supplier = REGISTRY.get(type);
        if (supplier == null) {
            throw new IllegalArgumentException(
                    "Unknown generator type: " + type + ". Available: " + REGISTRY.keySet());
        }
        FieldGenerator generator = supplier.get();
        generator.configure(params, seed);
        return generator;
    }
}
