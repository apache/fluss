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

package com.alibaba.fluss.client.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.fs.FsPathAndFileName;

import java.util.List;

/**
 * A class to represent the snapshot info of a kv tablet (bucket). It contains:
 *
 * <ul>
 *   <li>The snapshot files of the bucket.
 *   <li>The log offset corresponding to the snapshot.
 * </ul>
 *
 * <p>To read the full data of a bucket, it requires to read the snapshot and the log from the
 * corresponding log offset.
 *
 * @since 0.6
 */
@PublicEvolving
public class KvSnapshotMetadata {

    private final List<FsPathAndFileName> snapshotFiles;
    private final long logOffset;

    public KvSnapshotMetadata(List<FsPathAndFileName> snapshotFiles, long logOffset) {
        this.snapshotFiles = snapshotFiles;
        this.logOffset = logOffset;
    }

    public List<FsPathAndFileName> getSnapshotFiles() {
        return snapshotFiles;
    }

    public long getLogOffset() {
        return logOffset;
    }

    @Override
    public String toString() {
        return "BucketSnapshotInfo{"
                + "snapshotFiles="
                + snapshotFiles
                + ", logOffset="
                + logOffset
                + '}';
    }
}
