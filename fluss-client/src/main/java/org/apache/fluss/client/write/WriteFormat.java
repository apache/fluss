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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.KvFormat;

/** The format of the write record. */
@Internal
public enum WriteFormat {
    ARROW_LOG(true),
    INDEXED_LOG(true),
    COMPACTED_LOG(true),
    INDEXED_KV(false),
    COMPACTED_KV(false);

    private final boolean isLog;

    WriteFormat(boolean isLog) {
        this.isLog = isLog;
    }

    public boolean isLog() {
        return isLog;
    }

    public boolean isKv() {
        return !isLog;
    }

    /** Converts this {@link WriteFormat} to a {@link KvFormat}. */
    public KvFormat toKvFormat() {
        switch (this) {
            case INDEXED_KV:
                return KvFormat.INDEXED;
            case COMPACTED_KV:
                return KvFormat.COMPACTED;
            default:
                throw new IllegalArgumentException("WriteFormat " + this + " is not a KvFormat");
        }
    }

    /** Converts a {@link KvFormat} to a {@link WriteFormat}. */
    public static WriteFormat fromKvFormat(KvFormat kvFormat) {
        switch (kvFormat) {
            case INDEXED:
                return INDEXED_KV;
            case COMPACTED:
                return COMPACTED_KV;
            default:
                throw new IllegalArgumentException("Unknown KvFormat: " + kvFormat);
        }
    }
}
