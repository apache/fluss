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

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.indexed.IndexedRow;

/**
 * The format of the kv records in kv store. The supported formats are 'compacted' and 'indexed'.
 */
public enum KvFormat {

    /**
     * The kv record batches are stored in {@link CompactedRow} format. It's more efficient in disk
     * and memory usage.
     */
    COMPACTED,

    /**
     * The kv record batches are stored in {@link IndexedRow} format. It is more efficient for read
     * partial columns from snapshot as it won't need to deserialize the whole row.
     */
    INDEXED;

    /**
     * Creates a {@link LogFormat} from the given string. The string must be either 'arrow' or
     * 'indexed'.
     */
    public static KvFormat fromString(String format) {
        switch (format.toUpperCase()) {
            case "COMPACTED":
                return COMPACTED;
            case "INDEXED":
                return INDEXED;
            default:
                throw new IllegalArgumentException("Unsupported kv format: " + format);
        }
    }
}
