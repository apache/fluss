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

import java.util.Map;

/** Generates random integers within [min, max). */
public class RandomIntGenerator extends AbstractRandomGenerator {

    private int min;
    private int max;

    @Override
    public String type() {
        return "random-int";
    }

    @Override
    protected void doConfigure(Map<String, Object> params) {
        this.min = ((Number) params.getOrDefault("min", 0)).intValue();
        this.max = ((Number) params.getOrDefault("max", Integer.MAX_VALUE)).intValue();
        if (max <= min) {
            throw new IllegalArgumentException(
                    "random-int: 'max' (" + max + ") must be greater than 'min' (" + min + ")");
        }
    }

    @Override
    public Object generate(long index) {
        return min + rng.nextInt(max - min);
    }
}
