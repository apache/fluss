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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Objects;

/** A key that identifies lookup batches by route bucket and original partition name. */
@Internal
class LookupBatchKey {

    private final TableBucket tableBucket;

    /** Null for normal lookup batches; non-null for historical batches. */
    private final @Nullable String originalPartitionName;

    LookupBatchKey(TableBucket tableBucket, @Nullable String originalPartitionName) {
        this.tableBucket = tableBucket;
        this.originalPartitionName = originalPartitionName;
    }

    TableBucket tableBucket() {
        return tableBucket;
    }

    @Nullable
    String originalPartitionName() {
        return originalPartitionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LookupBatchKey that = (LookupBatchKey) o;
        return Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(originalPartitionName, that.originalPartitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket, originalPartitionName);
    }

    @Override
    public String toString() {
        return "LookupBatchKey{"
                + "tableBucket="
                + tableBucket
                + ", originalPartitionName='"
                + originalPartitionName
                + '\''
                + '}';
    }
}
