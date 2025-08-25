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

package com.alibaba.fluss.client.table.scanner;

import com.alibaba.fluss.fs.FSDataInputStream;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.FsPathAndFileName;
import com.alibaba.fluss.fs.utils.FileDownloadSpec;
import com.alibaba.fluss.fs.utils.FileDownloadUtils;
import com.alibaba.fluss.utils.CloseableRegistry;
import com.alibaba.fluss.utils.IOUtils;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The downloader that has a IO thread pool to download the remote files (like kv snapshots files,
 * log segment files).
 */
public class RemoteFileDownloader implements Closeable {

    protected final ExecutorService downloadThreadPool;

    public RemoteFileDownloader(int threadNum) {
        downloadThreadPool =
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

    /**
     * Downloads the file from the given remote file path to the target directory asynchronously,
     * returns a Future object of the number of downloaded bytes. The Future will fail if the
     * download fails after retrying for RETRY_COUNT times.
     */
    public CompletableFuture<Long> downloadFileAsync(
            FsPathAndFileName fsPathAndFileName, Path targetDirectory) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        downloadThreadPool.submit(
                () -> {
                    try {
                        Path targetFilePath =
                                targetDirectory.resolve(fsPathAndFileName.getFileName());
                        FsPath remoteFilePath = fsPathAndFileName.getPath();
                        long downloadBytes = downloadFile(targetFilePath, remoteFilePath);
                        future.complete(downloadBytes);
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
        return future;
    }

    /**
     * Copies the file from a remote file path to the given target file path, returns the number of
     * downloaded bytes.
     */
    protected long downloadFile(Path targetFilePath, FsPath remoteFilePath) throws IOException {
        List<Closeable> closeableRegistry = new ArrayList<>(2);
        try {
            FileSystem fileSystem = remoteFilePath.getFileSystem();
            FSDataInputStream inputStream = fileSystem.open(remoteFilePath);
            closeableRegistry.add(inputStream);

            Files.createDirectories(targetFilePath.getParent());
            OutputStream outputStream = Files.newOutputStream(targetFilePath);
            closeableRegistry.add(outputStream);

            return IOUtils.copyBytes(inputStream, outputStream, false);
        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            closeableRegistry.forEach(IOUtils::closeQuietly);
        }
    }

    @Override
    public void close() throws IOException {
        downloadThreadPool.shutdownNow();
    }

    public void transferAllToDirectory(
            List<FsPathAndFileName> fsPathAndFileNames,
            Path targetDirectory,
            CloseableRegistry closeableRegistry)
            throws IOException {
        FileDownloadSpec fileDownloadSpec =
                new FileDownloadSpec(fsPathAndFileNames, targetDirectory);
        FileDownloadUtils.transferAllDataToDirectory(
                Collections.singleton(fileDownloadSpec), closeableRegistry, downloadThreadPool);
    }
}
