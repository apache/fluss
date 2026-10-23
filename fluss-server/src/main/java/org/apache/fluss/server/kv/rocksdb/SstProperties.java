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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.annotation.Internal;

import io.github.fluss_contrib.rocksdb.TableProperties;

import java.io.IOException;
import java.util.Map;

/** Immutable index layout summary of the live SST files in a column family. */
@Internal
public final class SstProperties {
    private static final String INDEX_TYPE = "rocksdb.block.based.table.index.type";

    private final int fileCount;
    private final int partitionedFileCount;

    private SstProperties(int fileCount, int partitionedFileCount) {
        this.fileCount = fileCount;
        this.partitionedFileCount = partitionedFileCount;
    }

    static SstProperties from(Map<String, TableProperties> tables) throws IOException {
        int partitioned = 0;
        for (Map.Entry<String, TableProperties> table : tables.entrySet()) {
            String indexType = table.getValue().getUserCollectedProperties().get(INDEX_TYPE);
            // RocksJava marshals this fixed-width binary property as a C string. Binary-search
            // index type zero is therefore present as an empty string, not a missing property.
            if ("".equals(indexType)) {
                continue;
            }
            if ("\u0002".equals(indexType)) {
                partitioned++;
            } else {
                throw new IOException("Unknown SST index layout in " + table.getKey());
            }
        }
        return new SstProperties(tables.size(), partitioned);
    }

    /** Returns the number of inspected live SST files. */
    public int fileCount() {
        return fileCount;
    }

    /** Returns whether every file has the requested layout; an empty database always matches. */
    public boolean matches(boolean partitionedIndex) {
        return partitionedIndex ? partitionedFileCount == fileCount : partitionedFileCount == 0;
    }
}
