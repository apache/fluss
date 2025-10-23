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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.lake.source.LakeSplit;

import org.apache.iceberg.FileScanTask;

import java.io.Serializable;
import java.util.List;

/** Split for Iceberg table. */
public class IcebergSplit implements LakeSplit, Serializable {
    private static final long serialVersionUID = 1L;

    private final FileScanTask fileScanTask;
    private final int bucket;
    private final List<String> partition;

    public IcebergSplit(FileScanTask fileScanTask, int bucket, List<String> partition) {
        this.fileScanTask = fileScanTask;
        this.bucket = bucket;
        this.partition = partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public List<String> partition() {
        return partition;
    }

    public FileScanTask fileScanTask() {
        return fileScanTask;
    }

    @Override
    public String toString() {
        return "IcebergSplit{"
                + "task="
                + fileScanTask
                + ", bucket="
                + bucket
                + ", partition="
                + partition
                + '}';
    }
}
