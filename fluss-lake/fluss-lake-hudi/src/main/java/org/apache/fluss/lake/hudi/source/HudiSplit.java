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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.lake.source.LakeSplit;

import org.apache.hudi.common.model.FileSlice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** A readable split of a Hudi table. */
public class HudiSplit implements LakeSplit {

    private static final long serialVersionUID = 1L;

    private final FileSlice fileSlice;
    private final int bucket;
    private final List<String> partition;

    public HudiSplit(FileSlice fileSlice, int bucket, List<String> partition) {
        this.fileSlice = Objects.requireNonNull(fileSlice, "fileSlice cannot be null");
        this.bucket = bucket;
        this.partition =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Objects.requireNonNull(partition, "partition cannot be null")));
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public List<String> partition() {
        return partition;
    }

    public FileSlice getFileSlice() {
        return fileSlice;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HudiSplit)) {
            return false;
        }
        HudiSplit hudiSplit = (HudiSplit) o;
        return bucket == hudiSplit.bucket
                && Objects.equals(fileSlice, hudiSplit.fileSlice)
                && Objects.equals(partition, hudiSplit.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileSlice, bucket, partition);
    }

    @Override
    public String toString() {
        return "HudiSplit{"
                + "fileSlice="
                + fileSlice
                + ", bucket="
                + bucket
                + ", partition="
                + partition
                + '}';
    }
}
