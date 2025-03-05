/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Collections;
import java.util.Map;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Represents a partition spec in fluss. Partition columns and values are NOT of strict order, and
 * they need to be re-arranged to the correct order by comparing with a list of strictly ordered
 * partition keys.
 *
 * @since 0.6
 */
@PublicEvolving
public class PartitionSpec {

    // An unmodifiable map as <partition key, value>
    private final Map<String, String> partitionSpec;

    public PartitionSpec(Map<String, String> partitionSpec) {
        checkNotNull(partitionSpec, "partitionSpec cannot be null");
        this.partitionSpec = Collections.unmodifiableMap(partitionSpec);
    }

    public Map<String, String> getPartitionSpec() {
        return partitionSpec;
    }

    @Override
    public String toString() {
        return "PartitionSpec{" + partitionSpec + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionSpec that = (PartitionSpec) o;
        return partitionSpec.equals(that.partitionSpec);
    }

    @Override
    public int hashCode() {
        return partitionSpec.hashCode();
    }
}
