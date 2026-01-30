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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.fs.FsPath;

import java.nio.file.Path;

/**
 * This class represents a download specification for the content of one {@link KvSnapshotHandle} to
 * a target {@link Path}.
 */
public class KvSnapshotDownloadSpec {

    /** The directory for exclusive snapshot data. */
    private final FsPath snapshotDirectory;

    /** The directory for shared snapshot data. */
    private final FsPath sharedSnapshotDirectory;

    /** The handle to download . */
    private final KvSnapshotHandle kvSnapshotHandle;

    /** The path to which the content of the snapshot handle shall be downloaded. */
    private final Path downloadDestination;

    public KvSnapshotDownloadSpec(
            FsPath snapshotDirectory,
            FsPath sharedSnapshotDirectory,
            KvSnapshotHandle kvSnapshotHandle,
            Path downloadDestination) {
        this.snapshotDirectory = snapshotDirectory;
        this.sharedSnapshotDirectory = sharedSnapshotDirectory;
        this.kvSnapshotHandle = kvSnapshotHandle;
        this.downloadDestination = downloadDestination;
    }

    public FsPath getSnapshotDirectory() {
        return snapshotDirectory;
    }

    public FsPath getSharedSnapshotDirectory() {
        return sharedSnapshotDirectory;
    }

    public KvSnapshotHandle getKvSnapshotHandle() {
        return kvSnapshotHandle;
    }

    public Path getDownloadDestination() {
        return downloadDestination;
    }
}
