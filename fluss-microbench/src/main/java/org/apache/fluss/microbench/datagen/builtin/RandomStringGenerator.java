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

/** Generates random strings of configurable length and charset. */
public class RandomStringGenerator extends AbstractRandomGenerator {

    private static final String ALPHANUMERIC =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private int minLength;
    private int maxLength;
    private char[] charset;

    @Override
    public String type() {
        return "random-string";
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doConfigure(Map<String, Object> params) {
        Object lengthParam = params.getOrDefault("length", 16);
        if (lengthParam instanceof Map) {
            Map<String, Object> range = (Map<String, Object>) lengthParam;
            this.minLength = ((Number) range.get("min")).intValue();
            this.maxLength = ((Number) range.get("max")).intValue();
        } else {
            int fixed = ((Number) lengthParam).intValue();
            this.minLength = fixed;
            this.maxLength = fixed;
        }
        String charsetStr = (String) params.getOrDefault("charset", ALPHANUMERIC);
        this.charset = charsetStr.toCharArray();
    }

    @Override
    public Object generate(long index) {
        int len =
                (minLength == maxLength)
                        ? minLength
                        : minLength + rng.nextInt(maxLength - minLength + 1);
        char[] buf = new char[len];
        for (int i = 0; i < len; i++) {
            buf[i] = charset[rng.nextInt(charset.length)];
        }
        return new String(buf);
    }
}
