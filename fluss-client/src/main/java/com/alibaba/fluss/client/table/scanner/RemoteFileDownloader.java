/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner;

import com.alibaba.fluss.fs.FsPathAndFileName;
import com.alibaba.fluss.fs.utils.FileDownloadSpec;
import com.alibaba.fluss.fs.utils.FileDownloadUtils;
import com.alibaba.fluss.utils.CloseableRegistry;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The downloader to download the remote files (like kv snapshots files, log segment files) to be
 * read.
 */
public class RemoteFileDownloader implements Closeable {

    protected final ExecutorService snapshotDownLoadThreadPool;

    public RemoteFileDownloader(int threadNum) {
        snapshotDownLoadThreadPool =
                Executors.newFixedThreadPool(
                        threadNum,
                        new ExecutorThreadFactory(
                                "fluss-client-remote-file-downloader",
                                // use the current classloader of the current thread as the given
                                // classloader of the thread created by the ExecutorThreadFactory
                                // to avoid use weird classloader provided by
                                // CompletableFuture.runAsync of method #initReaderAsynchronously
                                Thread.currentThread().getContextClassLoader()));
    }

    @Override
    public void close() throws IOException {
        snapshotDownLoadThreadPool.shutdownNow();
    }

    public void transferAllToDirectory(
            List<FsPathAndFileName> fsPathAndFileNames,
            Path targetDirectory,
            CloseableRegistry closeableRegistry)
            throws IOException {
        FileDownloadSpec fileDownloadSpec =
                new FileDownloadSpec(fsPathAndFileNames, targetDirectory);
        FileDownloadUtils.transferAllDataToDirectory(
                Collections.singleton(fileDownloadSpec),
                closeableRegistry,
                snapshotDownLoadThreadPool);
    }
}
