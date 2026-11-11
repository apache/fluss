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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.RebalanceTaskHandle;
import org.apache.fluss.server.zk.data.RebalanceTaskJsonSerde;
import org.apache.fluss.server.zk.data.RebalanceTaskZNodeData;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.RetryUtils;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkState;

/** Persists rebalance tasks inline in ZooKeeper or as remote files referenced by ZooKeeper. */
public final class RebalanceTaskStore {

    private static final Logger LOG = LoggerFactory.getLogger(RebalanceTaskStore.class);

    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 100L;
    private static final long MAX_BACKOFF_MS = 1000L;
    private static final long ORPHAN_FILE_RETENTION_MS = 60 * 60 * 1000L;

    private final ZooKeeperClient zkClient;
    private final String remoteDataDir;

    public RebalanceTaskStore(ZooKeeperClient zkClient, String remoteDataDir) {
        this.zkClient = zkClient;
        this.remoteDataDir = remoteDataDir;
    }

    /** Loads the currently committed rebalance task, resolving a remote handle when necessary. */
    public Optional<RebalanceTask> load() throws Exception {
        Optional<VersionedData<RebalanceTaskZNodeData>> stored =
                zkClient.getRebalanceTaskDataWithVersion();
        if (!stored.isPresent()) {
            return Optional.empty();
        }

        RebalanceTaskZNodeData data = stored.get().data();
        if (data.isInline()) {
            return Optional.of(data.getInlineTask());
        }

        RebalanceTaskHandle handle = data.getRemoteTaskHandle();
        RebalanceTask task = readTask(handle.getTaskPath());
        checkState(
                handle.getRebalanceId().equals(task.getRebalanceId()),
                "Rebalance id %s in %s does not match handle id %s.",
                task.getRebalanceId(),
                handle.getTaskPath(),
                handle.getRebalanceId());
        return Optional.of(task);
    }

    /**
     * Persists a task and atomically changes the rebalance znode to the new representation. Remote
     * files become immutable once their handle is committed.
     */
    public void persist(
            RebalanceTask task, int expectedCoordinatorEpochZkVersion, boolean storeRemotely)
            throws Exception {
        Optional<VersionedData<RebalanceTaskZNodeData>> previous =
                zkClient.getRebalanceTaskDataWithVersion();
        int expectedRebalanceZkVersion =
                previous.map(VersionedData::zkVersion)
                        .orElse(ZkVersion.UNKNOWN_VERSION.getVersion());

        FsPath createdFile = null;
        RebalanceTaskZNodeData newData;
        if (storeRemotely) {
            byte[] serializedTask =
                    JsonSerdeUtils.writeValueAsBytes(task, RebalanceTaskJsonSerde.INSTANCE);
            createdFile =
                    FlussPaths.remoteRebalanceTaskPath(
                            remoteDataDir, task.getRebalanceId(), UUID.randomUUID());
            writeTask(createdFile, serializedTask, task.getRebalanceId());
            newData =
                    RebalanceTaskZNodeData.remote(
                            new RebalanceTaskHandle(task.getRebalanceId(), createdFile));
        } else {
            newData = RebalanceTaskZNodeData.inline(task);
        }

        boolean committed = false;
        try {
            zkClient.upsertRebalanceTaskData(
                    newData, expectedRebalanceZkVersion, expectedCoordinatorEpochZkVersion);
            committed = true;
        } catch (Exception writeFailure) {
            try {
                committed = isCommitted(newData);
            } catch (Exception verificationFailure) {
                writeFailure.addSuppressed(verificationFailure);
                // The pointer may have committed. Keep the file until recovery or orphan cleanup
                // can determine whether it is referenced.
                throw writeFailure;
            }
            if (!committed) {
                deleteFile(createdFile);
                throw writeFailure;
            }
            LOG.warn(
                    "Rebalance task {} was persisted despite the ZooKeeper write error.",
                    task.getRebalanceId(),
                    writeFailure);
        }

        if (committed && previous.isPresent()) {
            deletePreviousRemoteFile(previous.get().data(), createdFile);
        }
    }

    private boolean isCommitted(RebalanceTaskZNodeData expected) throws Exception {
        Optional<VersionedData<RebalanceTaskZNodeData>> current =
                zkClient.getRebalanceTaskDataWithVersion();
        if (!current.isPresent()) {
            return false;
        }
        RebalanceTaskZNodeData actual = current.get().data();
        if (expected.isInline() != actual.isInline()) {
            return false;
        }
        return expected.isInline()
                ? expected.getInlineTask().equals(actual.getInlineTask())
                : expected.getRemoteTaskHandle().equals(actual.getRemoteTaskHandle());
    }

    private void writeTask(FsPath path, byte[] bytes, String rebalanceId) throws IOException {
        RetryUtils.executeIOWithRetry(
                () -> {
                    FileSystem fileSystem = path.getFileSystem();
                    FsPath parent = path.getParent();
                    if (parent != null && !fileSystem.exists(parent)) {
                        fileSystem.mkdirs(parent);
                    }
                    try (FSDataOutputStream output =
                            fileSystem.create(path, FileSystem.WriteMode.OVERWRITE)) {
                        output.write(bytes);
                    }
                    return null;
                },
                "write rebalance task " + rebalanceId,
                MAX_RETRIES,
                INITIAL_BACKOFF_MS,
                MAX_BACKOFF_MS);
    }

    private RebalanceTask readTask(FsPath path) throws IOException {
        return RetryUtils.executeIOWithRetry(
                () -> {
                    try (FSDataInputStream input = path.getFileSystem().open(path);
                            ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                        IOUtils.copyBytes(input, output, false);
                        return JsonSerdeUtils.readValue(
                                output.toByteArray(), RebalanceTaskJsonSerde.INSTANCE);
                    }
                },
                "read rebalance task " + path,
                MAX_RETRIES,
                INITIAL_BACKOFF_MS,
                MAX_BACKOFF_MS);
    }

    private void deletePreviousRemoteFile(RebalanceTaskZNodeData previous, FsPath currentFile) {
        if (previous.isInline()) {
            return;
        }
        FsPath previousFile = previous.getRemoteTaskHandle().getTaskPath();
        if (!previousFile.equals(currentFile)) {
            deleteFile(previousFile);
        }
    }

    private void deleteFile(FsPath path) {
        if (path == null) {
            return;
        }
        try {
            FileSystem fileSystem = path.getFileSystem();
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, false);
            }
        } catch (Exception e) {
            LOG.warn("Failed to delete rebalance task file {}.", path, e);
        }
    }

    void cleanupOrphanFiles() {
        FsPath rebalanceDir = FlussPaths.remoteRebalanceDir(remoteDataDir);
        try {
            FileSystem fileSystem = rebalanceDir.getFileSystem();
            if (!fileSystem.exists(rebalanceDir)) {
                return;
            }

            // Re-read the pointer so cleanup cannot remove a concurrently committed task. The age
            // guard also protects a new leader's file before its pointer is visible in ZooKeeper.
            Set<FsPath> retainedFiles = new HashSet<>();
            Optional<VersionedData<RebalanceTaskZNodeData>> current =
                    zkClient.getRebalanceTaskDataWithVersion();
            if (current.isPresent() && !current.get().data().isInline()) {
                retainedFiles.add(current.get().data().getRemoteTaskHandle().getTaskPath());
            }

            long cutoffMillis = System.currentTimeMillis() - ORPHAN_FILE_RETENTION_MS;
            FileStatus[] rebalanceDirectories = fileSystem.listStatus(rebalanceDir);
            if (rebalanceDirectories == null) {
                return;
            }
            for (FileStatus rebalanceStatus : rebalanceDirectories) {
                if (!rebalanceStatus.isDir()) {
                    continue;
                }

                FsPath rebalancePath = rebalanceStatus.getPath();
                FileStatus[] taskFiles = fileSystem.listStatus(rebalancePath);
                if (taskFiles == null) {
                    continue;
                }
                for (FileStatus taskStatus : taskFiles) {
                    if (!taskStatus.isDir()
                            && !retainedFiles.contains(taskStatus.getPath())
                            && taskStatus.getModificationTime() < cutoffMillis) {
                        fileSystem.delete(taskStatus.getPath(), false);
                    }
                }

                FileStatus[] remainingFiles = fileSystem.listStatus(rebalancePath);
                if (remainingFiles != null && remainingFiles.length == 0) {
                    fileSystem.delete(rebalancePath, false);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to clean orphan rebalance task files under {}.", rebalanceDir, e);
        }
    }
}
