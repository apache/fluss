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

package org.apache.fluss.client.table.getter;

import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.RowPartitionGetter;
import org.apache.fluss.types.RowType;

import java.util.List;

/** A getter to get partition name from a row. */
public class PartitionGetter {

    private final RowPartitionGetter delegate;

    /** Creates a partition getter for the given row type and partition keys. */
    public PartitionGetter(RowType rowType, List<String> partitionKeys) {
        this.delegate = new RowPartitionGetter(rowType, partitionKeys);
    }

    /** Returns the partition name extracted from the given row. */
    public String getPartition(InternalRow row) {
        return delegate.getPartition(row);
    }

    /** Returns the resolved partition spec extracted from the given row. */
    public ResolvedPartitionSpec getResolvedPartitionSpec(InternalRow row) {
        return delegate.getResolvedPartitionSpec(row);
    }
}
