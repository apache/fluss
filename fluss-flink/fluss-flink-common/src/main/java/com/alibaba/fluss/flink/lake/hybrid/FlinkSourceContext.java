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

package com.alibaba.fluss.flink.lake.hybrid;

import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.List;
import java.util.Set;

/** The context for flink source. */
public class FlinkSourceContext {

    private final long snapshotId;
    private final TablePath tablePath;

    private final int[][] projectedFields;
    private final List<ResolvedExpression> filters;

    private final Set<String> partitions;

    private final LakeSplitAssigner lakeSplitAssigner;

    public FlinkSourceContext(
            long snapshotId,
            TablePath tablePath,
            int[][] projectedFields,
            List<ResolvedExpression> filters,
            Set<String> partitions,
            LakeSplitAssigner lakeSplitAssigner) {
        this.snapshotId = snapshotId;
        this.tablePath = tablePath;
        this.projectedFields = projectedFields;
        this.filters = filters;
        this.partitions = partitions;
        this.lakeSplitAssigner = lakeSplitAssigner;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public int[][] getProjectedFields() {
        return projectedFields;
    }

    public List<ResolvedExpression> getFilters() {
        return filters;
    }

    public Set<String> getPartitions() {
        return partitions;
    }

    public LakeSplitAssigner getLakeSplitAssigner() {
        return lakeSplitAssigner;
    }
}
