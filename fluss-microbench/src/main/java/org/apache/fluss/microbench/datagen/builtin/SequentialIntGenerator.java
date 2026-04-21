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

import java.util.Map;

/** Generates sequential integers with wrap-around. */
public class SequentialIntGenerator implements FieldGenerator {

    private int start;
    private int end;
    private int step;
    private long range;

    @Override
    public String type() {
        return "sequential";
    }

    @Override
    public void configure(Map<String, Object> params, long seed) {
        this.start = ((Number) params.getOrDefault("start", 0)).intValue();
        Object endParam = params.get("end");
        if (endParam == null) {
            throw new IllegalArgumentException("SequentialIntGenerator requires 'end' parameter");
        }
        this.end = ((Number) endParam).intValue();
        this.step = ((Number) params.getOrDefault("step", 1)).intValue();

        // Use long to avoid integer overflow when computing range
        this.range = (long) this.end - this.start;
        if (this.range <= 0) {
            throw new IllegalArgumentException(
                    "'end' (" + this.end + ") must be greater than 'start' (" + this.start + ")");
        }
    }

    @Override
    public Object generate(long index) {
        // Use pre-computed long range to avoid overflow in modulo operation
        return start + (int) ((index * step) % range);
    }
}
