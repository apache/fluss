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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static org.apache.fluss.utils.FlussPaths.INDEX_FILE_SUFFIX;
import static org.apache.fluss.utils.FlussPaths.TIME_INDEX_FILE_SUFFIX;
import static org.apache.fluss.utils.FlussPaths.WRITER_SNAPSHOT_FILE_SUFFIX;
import static org.apache.fluss.utils.FlussPaths.remoteLogIndexFile;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentFile;

/** This class is the default implementation of {@link RemoteLogStorage}. */
public class DefaultRemoteLogStorage implements RemoteLogStorage {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRemoteLogStorage.class);

    private static final int READ_BUFFER_SIZE = 16 * 1024;

    private final FsPath remoteLogDir;
    private final FileSystem fileSystem;
    private final ExecutorService ioExecutor;
    private final int writeBufferSize;

    public DefaultRemoteLogStorage(Configuration conf, ExecutorService ioExecutor)
            throws IOException {
        this.remoteLogDir = FlussPaths.remoteLogDir(conf);
        this.fileSystem = remoteLogDir.getFileSystem();
        this.writeBufferSize = (int) conf.get(ConfigOptions.REMOTE_FS_WRITE_BUFFER_SIZE).getBytes();
        this.ioExecutor = ioExecutor;
    }

    @Override
    public FsPath getRemoteLogDir() {
        return remoteLogDir;
    }

    /**
     * Copy log segments to remote path.
     *
     * <pre>
     * {$remote.data.dir}/log/{db}/{tableName}_{tableId}/{bucketId}/{segment_uuid}/{remote_log_start_offset}.log
     * {$remote.data.dir}/log/{db}/{tableName}_{tableId}/{bucketId}/{segment_uuid}/{remote_log_start_offset}.index
     * {$remote.data.dir}/log/{db}/{tableName}_{tableId}/{bucketId}/{segment_uuid}/{remote_log_start_offset}.timeindex
     * {$remote.data.dir}/log/{db}/{tableName}_{tableId}/{bucketId}/{segment_uuid}/{remote_log_end_offset}.writer_snapshot
     * </pre>
     */
    @Override
    public void copyLogSegmentFiles(
            RemoteLogSegment remoteLogSegment, LogSegmentFiles logSegmentFiles)
            throws RemoteStorageException {
        LOG.debug("copying log segment and indexes for remoteLogSegment: {}", remoteLogSegment);
        try {
            List<CompletableFuture<Void>> futures =
                    createUploadFutures(remoteLogSegment, logSegmentFiles);
            FutureUtils.waitForAll(futures).get();
        } catch (ExecutionException e) {
            Throwable throwable = ExceptionUtils.stripExecutionException(e);
            throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
            throw new RemoteStorageException(
                    "Failed to copy log segment and indexes to remote dir for path: "
                            + remoteLogSegment,
                    throwable);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteStorageException(
                    "Interrupted while copying log segment and indexes to remote for path: "
                            + remoteLogSegment,
                    e);
        } catch (Exception e) {
            throw new RemoteStorageException(
                    "Failed to copy log segment and indexes to remote for path: "
                            + remoteLogSegment,
                    e);
        }
    }

    /**
     * Delete log segments from remote path. Currently, these files need to be deleted:
     *
     * <pre>
     * {$remote.data.dir}/log/{db}/{tableName}_{tableId}/{bucketId}/{segment_uuid}/{remote_log_start_offset}.log
     * {$remote.data.dir}/log/{db}/{tableName}_{tableId}/{bucketId}/{segment_uuid}/{remote_log_start_offset}.index
     * {$remote.data.dir}/log/{db}/{tableName}_{tableId}/{bucketId}/{segment_uuid}/{remote_log_start_offset}.timeindex
     * {$remote.data.dir}/log/{db}/{tableName}_{tableId}/{bucketId}/{segment_uuid}/{remote_log_end_offset}.writer_snapshot
     * </pre>
     *
     * <p>Note: We need to delete specify remote file instead of delete the whole directory
     * recursive because the list files operation in object storage is a heavy operation.
     */
    @Override
    public void deleteLogSegmentFiles(RemoteLogSegment remoteLogSegment)
            throws RemoteStorageException {
        LOG.debug("Deleting log segment and indexes for : {}", remoteLogSegment);
        try {
            FsPath segmentDir = remoteLogSegmentDir(remoteLogDir, remoteLogSegment);
            long baseOffset = remoteLogSegment.remoteLogStartOffset();
            FsPath logFile = remoteLogSegmentFile(segmentDir, baseOffset);
            FsPath offsetIndex = remoteLogIndexFile(segmentDir, baseOffset, INDEX_FILE_SUFFIX);
            FsPath timeIndex = remoteLogIndexFile(segmentDir, baseOffset, TIME_INDEX_FILE_SUFFIX);
            FsPath writerSnapshot =
                    remoteLogIndexFile(
                            segmentDir,
                            remoteLogSegment.remoteLogEndOffset(),
                            WRITER_SNAPSHOT_FILE_SUFFIX);
            // delete dir at last
            for (FsPath path :
                    Arrays.asList(logFile, offsetIndex, timeIndex, writerSnapshot, segmentDir)) {
                fileSystem.delete(path, false);
            }
            LOG.debug("Successful delete log segment and indexes for : {}", remoteLogSegment);
        } catch (IOException e) {
            throw new RemoteStorageException(
                    "Failed to delete log segment and indexes for path: " + remoteLogSegment, e);
        }
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegment remoteLogSegment, IndexType indexType)
            throws RemoteStorageException {
        FsPath remoteLogSegmentIndexFile;
        if (indexType == IndexType.WRITER_ID_SNAPSHOT) {
            remoteLogSegmentIndexFile =
                    FlussPaths.remoteWriterSnapshotFile(
                            remoteLogDir, remoteLogSegment, IndexType.getFileSuffix(indexType));
        } else {
            remoteLogSegmentIndexFile =
                    FlussPaths.remoteOffsetIndexFile(
                            remoteLogDir, remoteLogSegment, IndexType.getFileSuffix(indexType));
        }

        try {
            return fileSystem.open(remoteLogSegmentIndexFile);
        } catch (IOException e) {
            throw new RemoteStorageException(
                    "Failed to fetch index file type: "
                            + indexType
                            + " from path: "
                            + remoteLogSegmentIndexFile,
                    e);
        }
    }

    @Override
    public RemoteLogManifest readRemoteLogManifestSnapshot(FsPath remoteLogManifestPath)
            throws RemoteStorageException {
        FSDataInputStream inputStream = null;
        ByteArrayOutputStream outputStream = null;
        try {
            inputStream = fileSystem.open(remoteLogManifestPath);
            outputStream = new ByteArrayOutputStream();
            IOUtils.copyBytes(inputStream, outputStream, false);
            return RemoteLogManifest.fromJsonBytes(outputStream.toByteArray());
        } catch (Exception e) {
            throw new RemoteStorageException(
                    String.format(
                            "Failed to read remote log manifest from %s", remoteLogManifestPath),
                    e);
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
        }
    }

    @Override
    public void deleteRemoteLogManifestSnapshot(FsPath remoteLogManifestPath)
            throws RemoteStorageException {
        LOG.debug("Deleting remote log segment manifest: {}", remoteLogManifestPath);
        try {
            fileSystem.delete(remoteLogManifestPath, false);
            LOG.debug("Successful delete log segment manifest: {}", remoteLogManifestPath);
        } catch (IOException e) {
            throw new RemoteStorageException(
                    "Failed to delete log segment manifest: " + remoteLogManifestPath, e);
        }
    }

    /** Write log manifest snapshot to remote file system. */
    @Override
    public FsPath writeRemoteLogManifestSnapshot(RemoteLogManifest manifest)
            throws RemoteStorageException {
        FsPath manifestFile =
                FlussPaths.remoteLogManifestFile(
                        FlussPaths.remoteLogTabletDir(
                                remoteLogDir,
                                manifest.getPhysicalTablePath(),
                                manifest.getTableBucket()),
                        UUID.randomUUID());
        try {
            return writeToRemote(
                    new ByteArrayInputStream(manifest.toJsonBytes()),
                    manifestFile.getParent(),
                    manifestFile.getName());
        } catch (Exception e) {
            throw new RemoteStorageException(
                    String.format(
                            "Failed to upload the remote log manifest to remote path: %s",
                            manifestFile),
                    e);
        }
    }

    @Override
    public void deleteTableBucket(PhysicalTablePath physicalTablePath, TableBucket tableBucket)
            throws RemoteStorageException {
        FsPath remoteLogTabletDir =
                FlussPaths.remoteLogTabletDir(remoteLogDir, physicalTablePath, tableBucket);
        try {
            if (fileSystem.exists(remoteLogTabletDir)) {
                fileSystem.delete(remoteLogTabletDir, true);
            }
        } catch (Exception e) {
            throw new RemoteStorageException(
                    "Failed to delete remote log tablet path: " + remoteLogTabletDir, e);
        }
    }

    private List<CompletableFuture<Void>> createUploadFutures(
            RemoteLogSegment remoteLogSegment, LogSegmentFiles logSegmentFiles) throws IOException {
        FsPath rlsPath = createRemoteLogSegmentDir(remoteLogSegment);
        List<Path> localFiles = logSegmentFiles.getAllPaths();
        List<CompletableFuture<Void>> list = new ArrayList<>();
        for (Path localFile : localFiles) {
            CompletableFuture<Void> voidCompletableFuture =
                    CompletableFuture.runAsync(
                            ThrowingRunnable.unchecked(
                                    () ->
                                            writeToRemote(
                                                    Files.newInputStream(localFile),
                                                    rlsPath,
                                                    localFile.getFileName().toString())),
                            ioExecutor);
            list.add(voidCompletableFuture);
        }
        return list;
    }

    /**
     * Write input stream to remote.
     *
     * @param inputStream input stream of the data to write
     * @param remoteFileName remote file name
     * @param remoteDir remote dir
     * @return remote file path including file name
     */
    private @Nullable FsPath writeToRemote(
            InputStream inputStream, FsPath remoteDir, String remoteFileName) throws IOException {
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            final byte[] buffer = new byte[READ_BUFFER_SIZE];
            closeableRegistry.registerCloseable(inputStream);

            FsRemoteLogOutputStream outputStream =
                    new FsRemoteLogOutputStream(remoteDir, writeBufferSize, remoteFileName);
            closeableRegistry.registerCloseable(outputStream);

            while (true) {
                int numBytes = inputStream.read(buffer);

                if (numBytes == -1) {
                    break;
                }
                outputStream.write(buffer, 0, numBytes);
            }

            final FsPath result;
            if (closeableRegistry.unregisterCloseable(outputStream)) {
                LOG.debug("Successful upload file {} to remote path {}", remoteFileName, remoteDir);
                result = outputStream.closeAndGetFsPath();
            } else {
                result = null;
            }
            return result;
        }
    }

    private FsPath createRemoteLogSegmentDir(RemoteLogSegment remoteLogSegment) throws IOException {
        FsPath remoteLogSegmentDir = remoteLogSegmentDir(remoteLogDir, remoteLogSegment);
        fileSystem.mkdirs(remoteLogSegmentDir);
        return remoteLogSegmentDir;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
