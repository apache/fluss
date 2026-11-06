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

package org.apache.fluss.compression;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Batch-level compression codec of row-based log record batches (e.g. COMPACTED). The id is stored
 * in the attribute byte of the batch header.
 *
 * <p>This is separate from {@link ArrowCompressionType}: Arrow compresses each buffer inside the
 * Arrow IPC body and records the codec in the Arrow metadata, while this codec compresses the whole
 * records section of a batch and its id is part of the on-disk batch format. Keeping a separate
 * enum keeps these ids stable regardless of changes to {@link ArrowCompressionType}.
 */
@PublicEvolving
public enum LogCompressionType {
    NONE((byte) 0),
    LZ4_FRAME((byte) 1),
    ZSTD((byte) 2);

    private final byte id;

    LogCompressionType(byte id) {
        this.id = id;
    }

    /** Returns the id of this codec that is stored in the batch attributes. */
    public byte id() {
        return id;
    }

    /**
     * Returns the codec of the given id.
     *
     * @throws IllegalArgumentException if the id is unknown
     */
    public static LogCompressionType fromId(int id) {
        for (LogCompressionType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown log compression type id: " + id);
    }
}
