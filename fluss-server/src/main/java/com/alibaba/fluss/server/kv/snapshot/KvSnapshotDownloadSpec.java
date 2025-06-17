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

import java.nio.file.Path;

/**
 * This class represents a download specification for the content of one {@link KvSnapshotHandle} to
 * a target {@link Path}.
 */
public class KvSnapshotDownloadSpec {

    /** The handle to download . */
    private final KvSnapshotHandle kvSnapshotHandle;

    /** The path to which the content of the snapshot handle shall be downloaded. */
    private final Path downloadDestination;

    public KvSnapshotDownloadSpec(KvSnapshotHandle kvSnapshotHandle, Path downloadDestination) {
        this.kvSnapshotHandle = kvSnapshotHandle;
        this.downloadDestination = downloadDestination;
    }

    public KvSnapshotHandle getKvSnapshotHandle() {
        return kvSnapshotHandle;
    }

    public Path getDownloadDestination() {
        return downloadDestination;
    }
}
