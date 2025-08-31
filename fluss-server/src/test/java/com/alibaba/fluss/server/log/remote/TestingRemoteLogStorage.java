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

package com.alibaba.fluss.server.log.remote;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.RemoteStorageException;
import com.alibaba.fluss.fs.FsPath;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A testing implementation of {@link com.alibaba.fluss.server.log.remote.RemoteLogStorage} which
 * can be used to simulate failures.
 */
public class TestingRemoteLogStorage extends DefaultRemoteLogStorage {

    public final AtomicBoolean writeManifestFail = new AtomicBoolean(false);

    public TestingRemoteLogStorage(Configuration conf) throws IOException {
        super(conf);
    }

    @Override
    public FsPath writeRemoteLogManifestSnapshot(RemoteLogManifest manifest)
            throws RemoteStorageException {
        if (writeManifestFail.get()) {
            throw new RuntimeException("failed to upload remote log manifest snapshot");
        }
        return super.writeRemoteLogManifestSnapshot(manifest);
    }
}
