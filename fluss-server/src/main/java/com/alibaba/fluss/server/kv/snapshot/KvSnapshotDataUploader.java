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

import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.server.utils.SnapshotUtil;
import com.alibaba.fluss.utils.CloseableRegistry;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.IOUtils;
import com.alibaba.fluss.utils.concurrent.FutureUtils;
import com.alibaba.fluss.utils.function.CheckedSupplier;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/** Help class for uploading Kv snapshot files. */
public class KvSnapshotDataUploader extends KvSnapshotDataTransfer {

    private static final int READ_BUFFER_SIZE = 16 * 1024;

    public KvSnapshotDataUploader(ExecutorService dataTransferThreadPool) {
        super(dataTransferThreadPool);
    }

    /**
     * Upload all the files to the target snapshot location using specified number of threads.
     *
     * @param files The files will be uploaded to the snapshot location.
     * @throws Exception Thrown if can not upload all the files.
     */
    public List<KvFileHandleAndLocalPath> uploadFilesToSnapshotLocation(
            @Nonnull List<Path> files,
            SnapshotLocation snapshotLocation,
            SnapshotFileScope snapshotFileScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws Exception {

        List<CompletableFuture<KvFileHandleAndLocalPath>> futures =
                createUploadFutures(
                        files,
                        snapshotLocation,
                        snapshotFileScope,
                        closeableRegistry,
                        tmpResourcesRegistry);

        List<KvFileHandleAndLocalPath> handles = new ArrayList<>(files.size());

        try {
            FutureUtils.waitForAll(futures).get();

            for (CompletableFuture<KvFileHandleAndLocalPath> future : futures) {
                handles.add(future.get());
            }
        } catch (ExecutionException e) {
            Throwable throwable = ExceptionUtils.stripExecutionException(e);
            throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
            if (throwable instanceof IOException) {
                throw (IOException) throwable;
            } else {
                throw new FlussRuntimeException("Failed to upload data for kv file handles.", e);
            }
        }

        return handles;
    }

    private List<CompletableFuture<KvFileHandleAndLocalPath>> createUploadFutures(
            List<Path> files,
            SnapshotLocation snapshotLocation,
            SnapshotFileScope snapshotFileScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry) {
        return files.stream()
                .map(
                        e ->
                                CompletableFuture.supplyAsync(
                                        CheckedSupplier.unchecked(
                                                () ->
                                                        uploadLocalFileToSnapshotLocation(
                                                                e,
                                                                snapshotLocation,
                                                                snapshotFileScope,
                                                                closeableRegistry,
                                                                tmpResourcesRegistry)),
                                        dataTransferThreadPool))
                .collect(Collectors.toList());
    }

    private KvFileHandleAndLocalPath uploadLocalFileToSnapshotLocation(
            Path filePath,
            SnapshotLocation snapshotLocation,
            SnapshotFileScope snapshotFileScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws IOException {

        InputStream inputStream = null;
        SnapshotLocation.FsSnapshotOutputStream outputStream = null;

        try {
            final byte[] buffer = new byte[READ_BUFFER_SIZE];

            inputStream = Files.newInputStream(filePath);
            closeableRegistry.registerCloseable(inputStream);

            outputStream = snapshotLocation.createSnapshotOutputStream(snapshotFileScope);
            closeableRegistry.registerCloseable(outputStream);

            while (true) {
                int numBytes = inputStream.read(buffer);

                if (numBytes == -1) {
                    break;
                }
                outputStream.write(buffer, 0, numBytes);
            }

            final KvFileHandle result;
            if (closeableRegistry.unregisterCloseable(outputStream)) {
                result = outputStream.closeAndGetHandle();
            } else {
                result = null;
            }
            // tmp resource registry will be closed when the snapshot is not completed,
            // which will then discard the uploaded files
            tmpResourcesRegistry.registerCloseable(() -> SnapshotUtil.discardKvFileQuietly(result));
            return KvFileHandleAndLocalPath.of(result, filePath.getFileName().toString());
        } finally {
            if (closeableRegistry.unregisterCloseable(inputStream)) {
                IOUtils.closeQuietly(inputStream);
            }
            if (closeableRegistry.unregisterCloseable(outputStream)) {
                IOUtils.closeQuietly(outputStream);
            }
        }
    }
}
