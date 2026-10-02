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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.source.LakeSplit;

import org.apache.paimon.table.source.DataSplit;

import java.util.List;

/** Split for paimon table. */
public class PaimonSplit implements LakeSplit {

    private static final long serialVersionUID = 1L;

    private final DataSplit dataSplit;

    private final boolean isBucketUnAware;

    // Partition values in Fluss partition-name format
    private final List<String> partition;

    public PaimonSplit(DataSplit dataSplit, boolean isBucketUnAware, List<String> partition) {
        this.dataSplit = dataSplit;
        this.isBucketUnAware = isBucketUnAware;
        this.partition = partition;
    }

    @Override
    public int bucket() {
        if (isBucketUnAware) {
            // bucket-unaware table returns -1
            return -1;
        }
        return dataSplit.bucket();
    }

    @Override
    public List<String> partition() {
        return partition;
    }

    public DataSplit dataSplit() {
        return dataSplit;
    }

    public boolean isBucketUnAware() {
        return isBucketUnAware;
    }

    @Override
    public String toString() {
        return "PaimonSplit{"
                + "dataSplit="
                + dataSplit
                + ", isBucketUnAware="
                + isBucketUnAware
                + '}';
    }
}
