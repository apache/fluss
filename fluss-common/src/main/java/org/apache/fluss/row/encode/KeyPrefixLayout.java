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

package org.apache.fluss.row.encode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable byte boundaries of consecutive encoded key fields, independent of a storage engine. */
public final class KeyPrefixLayout {
    /** Byte encodings that can be traversed without decoding field values. */
    public enum Component {
        FIXED1,
        FIXED2,
        FIXED4,
        FIXED8,
        /** Unsigned base-128 varint, terminated by a clear high bit, occupying at most 5 bytes. */
        VARINT32,
        /** Unsigned base-128 varint, terminated by a clear high bit, occupying at most 10 bytes. */
        VARINT64,
        /** A VARINT32 byte length followed by exactly that many payload bytes. */
        BYTES_VARINT32
    }

    private final List<Component> components;

    KeyPrefixLayout(List<Component> components) {
        this.components = Collections.unmodifiableList(new ArrayList<>(components));
    }

    /** Returns the byte-boundary operations in encoding order. */
    public List<Component> getComponents() {
        return components;
    }
}
