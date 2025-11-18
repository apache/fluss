/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.utils;

import java.io.Serializable;

/**
 * A simple wrapper for sensitive strings that masks their content when converted to string (e.g.,
 * in logs). It reveals a small prefix and hides the rest with fixed stars.
 */
public final class MaskedValue implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final char MASK_CHAR = '*';

    private final String value;
    private final int prefixVisible;

    private MaskedValue(String value, int prefixVisible) {
        this.value = value;
        this.prefixVisible = Math.max(0, prefixVisible);
    }

    public static MaskedValue of(String value) {
        // default: reveal first 3 characters
        return new MaskedValue(value, 3);
    }

    public static MaskedValue of(String value, int prefixVisible) {
        return new MaskedValue(value, prefixVisible);
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        if (value == null || value.isEmpty()) {
            return "";
        }
        int visible = Math.min(prefixVisible, value.length());
        String prefix = value.substring(0, visible);
        int hidden = Math.max(0, value.length() - visible);
        return prefix + repeat(hidden);
    }

    private static String repeat(int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, MaskedValue.MASK_CHAR);
        return new String(chars);
    }

    @Override
    public int hashCode() {
        return value == null ? 0 : value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MaskedValue)) {
            return false;
        }
        MaskedValue other = (MaskedValue) obj;
        if (this.value == null) {
            return other.value == null;
        }
        return this.value.equals(other.value);
    }
}
