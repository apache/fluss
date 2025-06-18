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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.fs.FsPath;

import java.io.Serializable;

/**
 * A class represents the result of a snapshot for a table bucket. It contains the snapshot handle
 * for the kv tablet and the path to the snapshot directory which usually is a remote directory to
 * store the snapshot.
 */
public class SnapshotResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final KvSnapshotHandle kvSnapshotHandle;

    private final FsPath snapshotPath;
    private final long logOffset;

    public SnapshotResult(KvSnapshotHandle kvSnapshotHandle, FsPath snapshotPath, long logOffset) {
        this.kvSnapshotHandle = kvSnapshotHandle;
        this.snapshotPath = snapshotPath;
        this.logOffset = logOffset;
    }

    public KvSnapshotHandle getKvSnapshotHandle() {
        return kvSnapshotHandle;
    }

    public FsPath getSnapshotPath() {
        return snapshotPath;
    }

    public long getLogOffset() {
        return logOffset;
    }

    public long getSnapshotSize() {
        return kvSnapshotHandle.getSnapshotSize();
    }
}
