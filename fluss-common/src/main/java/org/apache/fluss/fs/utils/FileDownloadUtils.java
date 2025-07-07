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

package org.apache.fluss.fs.utils;

import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.function.CheckedSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utils for file download. */
public class FileDownloadUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FileDownloadUtils.class);

    /**
     * Transfer all data to the target directory, as specified in the download requests.
     *
     * @param fileDownloadSpecs the list of downloads.
     * @throws IOException If anything about the download goes wrong.
     */
    public static void transferAllDataToDirectory(
            Collection<FileDownloadSpec> fileDownloadSpecs,
            CloseableRegistry closeableRegistry,
            ExecutorService executorService)
            throws IOException {
        // We use this closer for fine-grained shutdown of all parallel downloading.
        CloseableRegistry internalCloser = new CloseableRegistry();
        // Make sure we also react to external close signals.
        closeableRegistry.registerCloseable(internalCloser);
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        try {
            futures =
                    transferDataToDirectoryAsync(fileDownloadSpecs, internalCloser, executorService)
                            .collect(Collectors.toList());
            // Wait until either all futures completed successfully or one failed exceptionally.
            FutureUtils.waitForAll(futures, new DownloadProgressAction(fileDownloadSpecs.size()))
                    .get();
        } catch (Exception e) {
            futures.forEach(future -> future.cancel(true));
            fileDownloadSpecs.stream()
                    .map(FileDownloadSpec::getTargetDirectory)
                    .map(Path::toFile)
                    .forEach(FileUtils::deleteDirectoryQuietly);
            // Error reporting
            Throwable throwable = ExceptionUtils.stripExecutionException(e);
            throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
            if (throwable instanceof IOException) {
                throw (IOException) throwable;
            } else {
                throw new FlussRuntimeException("Failed to download data.", e);
            }
        } finally {
            // Unregister and close the internal closer.
            if (closeableRegistry.unregisterCloseable(internalCloser)) {
                IOUtils.closeQuietly(internalCloser);
            }
        }
    }

    /** Asynchronously runs the specified download requests on executorService. */
    private static Stream<CompletableFuture<Long>> transferDataToDirectoryAsync(
            Collection<FileDownloadSpec> fileDownloadSpecs,
            CloseableRegistry closeableRegistry,
            ExecutorService executorService) {
        return fileDownloadSpecs.stream()
                .flatMap(
                        downloadSpec -> {
                            Path targetDirectory = downloadSpec.getTargetDirectory();
                            return downloadSpec.getFileHandles().stream()
                                    .map(
                                            fileHandle -> {
                                                Path downloadDest =
                                                        targetDirectory.resolve(
                                                                fileHandle.getFileName());
                                                // Create one runnable for each file handle
                                                return CheckedSupplier.unchecked(
                                                        () ->
                                                                downloadFile(
                                                                        downloadDest,
                                                                        fileHandle.getPath(),
                                                                        closeableRegistry));
                                            });
                        })
                .map(runnable -> CompletableFuture.supplyAsync(runnable, executorService));
    }

    /**
     * Copies the file from a remote file path to the given target file path, returns the number of
     * downloaded bytes.
     */
    private static Long downloadFile(
            Path targetFilePath, FsPath remoteFilePath, CloseableRegistry closeableRegistry)
            throws IOException {

        if (closeableRegistry.isClosed()) {
            return null;
        }

        try {
            FileSystem fileSystem = remoteFilePath.getFileSystem();
            FSDataInputStream inputStream = fileSystem.open(remoteFilePath);
            closeableRegistry.registerCloseable(inputStream);

            Files.createDirectories(targetFilePath.getParent());
            OutputStream outputStream = Files.newOutputStream(targetFilePath);
            closeableRegistry.registerCloseable(outputStream);

            long readBytes = IOUtils.copyBytes(inputStream, outputStream, false);

            closeableRegistry.unregisterAndCloseAll(outputStream, inputStream);

            return readBytes;
        } catch (Exception ex) {
            // Quickly close all open streams. This also stops all concurrent downloads because they
            // are registered with the same registry.
            IOUtils.closeQuietly(closeableRegistry);
            throw new IOException(ex);
        }
    }

    /** Logging the files download progress. It's not thread safe, should be used in locks. */
    @NotThreadSafe
    private static class DownloadProgressAction implements BiConsumer<Long, Throwable> {

        private final int totalFiles;
        private int numFilesDownloaded = 0;
        private long totalBytesDownloaded = 0;
        private double nextProgress = 0.25;

        private DownloadProgressAction(int totalFiles) {
            this.totalFiles = totalFiles;
        }

        @Override
        public void accept(Long readBytes, Throwable throwable) {
            if (throwable == null && readBytes != null) {
                totalBytesDownloaded += readBytes;
                numFilesDownloaded++;
                double progress = (double) numFilesDownloaded / totalFiles;
                if (progress >= nextProgress) {
                    // only logging for 25%, 50%, 100% to reduce logs
                    LOG.debug(
                            "Remote file has been downloaded {}, {}/{} files.",
                            new MemorySize(totalBytesDownloaded),
                            numFilesDownloaded,
                            totalFiles);
                    nextProgress += 0.25;
                }
            }
        }
    }
}
