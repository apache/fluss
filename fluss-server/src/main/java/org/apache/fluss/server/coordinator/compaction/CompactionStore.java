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

package org.apache.fluss.server.coordinator.compaction;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;
import org.apache.fluss.utils.FlussPaths;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Blocking storage operations, called on the coordinator's existing I/O executor.
 *
 * <p>The sole writer, {@link CompactionManager}, owns logical task transitions and invariants. This
 * store enforces coordinator epoch and version fencing, handle ownership and payload size limits.
 */
public final class CompactionStore {
    private static final int CLEANUP_LIMIT = 32;
    private final ZooKeeperClient zk;
    private final CuratorFramework curator;
    private final String remoteDataDir;
    private final FileSystem fs;
    private final int nodeLimit;
    private final int blockLimit;
    private final long planLimit;

    @Nullable private String cleanupCursor;

    /** Uses the production metadata size limits. */
    public CompactionStore(ZooKeeperClient zk, String remoteDataDir) throws Exception {
        this(
                zk,
                remoteDataDir,
                new FsPath(remoteDataDir).getFileSystem(),
                64 * 1024,
                256 * 1024,
                64L * 1024 * 1024);
    }

    CompactionStore(
            ZooKeeperClient zk,
            String remoteDataDir,
            FileSystem fs,
            int nodeLimit,
            int blockLimit,
            long planLimit)
            throws Exception {
        checkArgument(
                nodeLimit > 0 && blockLimit > 0 && planLimit > 0, "Invalid metadata size limits");
        this.zk = zk;
        this.curator = zk.getCuratorClient();
        this.remoteDataDir = remoteDataDir;
        this.fs = fs;
        this.nodeLimit = nodeLimit;
        this.blockLimit = blockLimit;
        this.planLimit = planLimit;
        try {
            curator.create()
                    .creatingParentsIfNeeded()
                    .forPath(ZkData.CompactionActiveZNode.path(), new byte[0]);
        } catch (KeeperException.NodeExistsException ignored) {
            // Never delete or recreate the active pointer between operations.
        }
        try {
            curator.create()
                    .creatingParentsIfNeeded()
                    .forPath(ZkData.CompactionTasksZNode.path(), new byte[0]);
        } catch (KeeperException.NodeExistsException ignored) {
            // Shared persistent parent for retained task summaries.
        }
    }

    /** Atomically creates PENDING metadata and claims the active pointer. */
    public int create(CompactionTask task, int activeVersion, int coordinatorEpochVersion)
            throws Exception {
        checkVersions(activeVersion, coordinatorEpochVersion);
        byte[] data = taskBytes(task);
        byte[] active = task.info().operationId().getBytes(StandardCharsets.UTF_8);
        List<CuratorOp> ops =
                Arrays.asList(
                        curator.transactionOp()
                                .create()
                                .forPath(taskPath(task.info().operationId()), data),
                        curator.transactionOp()
                                .setData()
                                .withVersion(activeVersion)
                                .forPath(ZkData.CompactionActiveZNode.path(), active));
        curator.transaction()
                .forOperations(zk.wrapRequestsWithEpochCheck(ops, coordinatorEpochVersion));
        return 0;
    }

    /** Publishes handles/counts, atomically clearing active on a terminal transition. */
    public int publish(
            CompactionTask task, int taskVersion, int activeVersion, int coordinatorEpochVersion)
            throws Exception {
        checkVersions(activeVersion, coordinatorEpochVersion);
        checkArgument(taskVersion >= 0, "Task version must be non-negative");
        byte[] data = taskBytes(task);
        byte[] active =
                task.isTerminal()
                        ? new byte[0]
                        : task.info().operationId().getBytes(StandardCharsets.UTF_8);
        List<CuratorOp> ops =
                Arrays.asList(
                        curator.transactionOp()
                                .setData()
                                .withVersion(taskVersion)
                                .forPath(taskPath(task.info().operationId()), data),
                        curator.transactionOp()
                                .setData()
                                .withVersion(activeVersion)
                                .forPath(ZkData.CompactionActiveZNode.path(), active));
        curator.transaction()
                .forOperations(zk.wrapRequestsWithEpochCheck(ops, coordinatorEpochVersion));
        return taskVersion + 1;
    }

    /** Loads only the small ZooKeeper summary. */
    public CompactionTask load(String operationId) throws Exception {
        Stat stat = new Stat();
        byte[] data = curator.getData().storingStatIn(stat).forPath(taskPath(operationId));
        return CompactionTaskJsonSerde.deserialize(data, stat.getVersion());
    }

    /** Lists summaries without reading remote plans or progress. */
    public List<CompactionTask> list() throws Exception {
        List<CompactionTask> tasks = new ArrayList<>();
        for (String id : curator.getChildren().forPath(ZkData.CompactionTasksZNode.path())) {
            try {
                tasks.add(load(id));
            } catch (KeeperException.NoNodeException ignored) {
                // A retained terminal record can be deleted during listing.
            }
        }
        return tasks;
    }

    /** Writes and closes all plan chunks before returning the manifest handle. */
    public synchronized FsPath writePlan(String operationId, CompactionTask.Plan plan)
            throws Exception {
        Map<FsPath, byte[]> files = new LinkedHashMap<>();
        ObjectNode manifest = CompactionTaskJsonSerde.object();
        ObjectNode configuration = manifest.putObject("configuration");
        plan.configuration().forEach(configuration::put);
        manifest.putArray("chunks");
        ObjectNode chunk = planChunk();
        int size = CompactionTaskJsonSerde.encode(chunk).length;
        for (TableBucket bucket : plan.buckets()) {
            ObjectNode entry = CompactionTaskJsonSerde.bucket(bucket);
            int entrySize = CompactionTaskJsonSerde.encode(entry).length;
            if (size + entrySize + 1 > blockLimit && chunk.withArray("buckets").size() > 0) {
                addChunk(operationId, files, manifest, chunk);
                chunk = planChunk();
                size = CompactionTaskJsonSerde.encode(chunk).length;
            }
            chunk.withArray("buckets").add(entry);
            size += entrySize + 1;
        }
        if (chunk.withArray("buckets").size() > 0) {
            addChunk(operationId, files, manifest, chunk);
        }
        FsPath handle = newPath(operationId, "plan");
        files.put(handle, CompactionTaskJsonSerde.encode(manifest));
        long bytes = fileBytes(files);
        if (bytes > planLimit) {
            throw new IOException("Compaction plan budget exceeded");
        }
        writeFiles(operationId, files);
        return handle;
    }

    /** Writes one immutable delta; an unpublished delta is never part of recovery. */
    public synchronized FsPath writeProgress(
            String operationId, @Nullable FsPath previous, Map<TableBucket, Long> snapshots)
            throws Exception {
        if (previous != null) {
            checkHandle(operationId, previous);
        }
        checkArgument(!snapshots.isEmpty(), "Progress update is empty");
        FsPath handle = newPath(operationId, "progress");
        writeFiles(
                operationId,
                Collections.singletonMap(
                        handle,
                        CompactionTaskJsonSerde.encode(
                                CompactionTaskJsonSerde.progress(previous, snapshots))));
        return handle;
    }

    /** Reads the fixed plan from its published manifest. */
    public CompactionTask.Plan readPlan(FsPath handle) throws IOException {
        JsonNode manifest = read(handle);
        List<TableBucket> buckets = new ArrayList<>();
        long bytes = fs.getFileStatus(handle).getLen();
        for (JsonNode path : manifest.required("chunks")) {
            FsPath chunk = new FsPath(path.asText());
            checkSibling(handle, chunk);
            bytes += fs.getFileStatus(chunk).getLen();
            if (bytes > planLimit) {
                throw new IOException("Compaction plan budget exceeded");
            }
            for (JsonNode bucket : read(chunk).required("buckets")) {
                buckets.add(CompactionTaskJsonSerde.bucket(bucket));
            }
        }
        Map<String, String> configuration = new LinkedHashMap<>();
        manifest.required("configuration")
                .fields()
                .forEachRemaining(e -> configuration.put(e.getKey(), e.getValue().asText()));
        return new CompactionTask.Plan(buckets, configuration);
    }

    /** Reads published successes, bounded by the authoritative summary count. */
    public Map<TableBucket, Long> readProgress(@Nullable FsPath handle, long expectedSuccesses)
            throws IOException {
        checkArgument(expectedSuccesses >= 0, "Negative compaction success count");
        Map<TableBucket, Long> snapshots = new LinkedHashMap<>();
        Set<FsPath> visited = new HashSet<>();
        FsPath cursor = handle;
        while (cursor != null) {
            checkSibling(handle, cursor);
            if (!visited.add(cursor)) {
                throw new IOException("Cyclic compaction progress chain");
            }
            if (visited.size() > expectedSuccesses) {
                throw new IOException("Compaction progress exceeds published success count");
            }
            JsonNode node = read(cursor);
            Map<TableBucket, Long> block = CompactionTaskJsonSerde.progress(node);
            if (block.isEmpty()) {
                throw new IOException("Empty compaction progress block");
            }
            for (Map.Entry<TableBucket, Long> entry : block.entrySet()) {
                if (snapshots.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IOException("Duplicate compaction success bucket");
                }
                if (snapshots.size() > expectedSuccesses) {
                    throw new IOException("Compaction progress exceeds published success count");
                }
            }
            cursor = CompactionTaskJsonSerde.handle(node, "previous");
        }
        if (snapshots.size() != expectedSuccesses) {
            throw new IOException("Invalid compaction completion journal");
        }
        return snapshots;
    }

    /** Removes at most 32 expired task or orphan directories, remote contents first. */
    public synchronized int cleanup(long cutoff, int coordinatorEpochVersion) throws Exception {
        TreeMap<String, CompactionTask> candidates = new TreeMap<>();
        Set<String> retained = new HashSet<>();
        for (CompactionTask task : list()) {
            String id = task.info().operationId();
            retained.add(id);
            if (task.isTerminal() && task.finishTime() < cutoff) {
                candidates.put(id, task);
            }
        }
        FsPath root = directory("orphan").getParent();
        if (fs.exists(root)) {
            for (FileStatus child : fs.listStatus(root)) {
                String id = child.getPath().getName();
                if (child.isDir()
                        && !retained.contains(id)
                        && child.getModificationTime() < cutoff) {
                    candidates.put(id, null);
                }
            }
        }
        List<String> ids = new ArrayList<>(candidates.keySet());
        int start = cleanupCursor == null ? 0 : Collections.binarySearch(ids, cleanupCursor);
        if (cleanupCursor != null) {
            start = start < 0 ? -start - 1 : start + 1;
        }
        int attempts = Math.min(CLEANUP_LIMIT, ids.size());
        IOException failure = null;
        for (int i = 0; i < attempts; i++) {
            String id = ids.get((start + i) % ids.size());
            cleanupCursor = id;
            checkCoordinatorEpoch(coordinatorEpochVersion);
            CompactionTask task = candidates.get(id);
            if (task == null && curator.checkExists().forPath(taskPath(id)) != null) {
                continue;
            }
            try {
                FsPath directory = directory(id);
                if (fs.exists(directory) && !fs.delete(directory, true)) {
                    throw new IOException("Could not delete compaction directory " + directory);
                }
                if (task != null) {
                    curator.transaction()
                            .forOperations(
                                    zk.wrapRequestsWithEpochCheck(
                                            Collections.singletonList(
                                                    curator.transactionOp()
                                                            .delete()
                                                            .withVersion(task.version())
                                                            .forPath(taskPath(id))),
                                            coordinatorEpochVersion));
                }
            } catch (IOException error) {
                failure = error;
            }
        }
        if (failure != null) {
            throw failure;
        }
        return attempts;
    }

    /** Checks writable coordinator ownership before recovery or remote cleanup. */
    public void checkCoordinatorEpoch(int coordinatorEpochVersion) throws Exception {
        checkArgument(coordinatorEpochVersion >= 0, "Compaction cleanup requires an active epoch");
        curator.transaction()
                .forOperations(
                        zk.wrapRequestsWithEpochCheck(
                                Collections.emptyList(), coordinatorEpochVersion));
    }

    private void checkVersions(int active, int epoch) {
        checkArgument(
                active >= 0 && epoch >= 0,
                "Compaction writes require explicit active and coordinator epoch versions");
    }

    private byte[] taskBytes(CompactionTask task) throws IOException {
        directory(task.info().operationId());
        if (task.planHandle() != null) {
            checkHandle(task.info().operationId(), task.planHandle());
        }
        if (task.progressHandle() != null) {
            checkHandle(task.info().operationId(), task.progressHandle());
        }
        byte[] bytes = CompactionTaskJsonSerde.serialize(task);
        if (bytes.length > nodeLimit) {
            throw new IOException("Compaction task record budget exceeded");
        }
        return bytes;
    }

    private FsPath directory(String operationId) {
        return FlussPaths.remoteCompactionDir(remoteDataDir, operationId);
    }

    private String taskPath(String operationId) {
        directory(operationId);
        return ZkData.CompactionTasksZNode.path(operationId);
    }

    private FsPath newPath(String operationId, String kind) {
        return new FsPath(directory(operationId), UUID.randomUUID() + "." + kind);
    }

    private void checkHandle(String operationId, FsPath handle) {
        checkArgument(
                directory(operationId).equals(handle.getParent()),
                "Compaction handle belongs to another operation");
    }

    private void checkSibling(FsPath handle, FsPath other) throws IOException {
        if (!Objects.equals(handle.getParent(), other.getParent())) {
            throw new IOException("Compaction block belongs to another operation");
        }
    }

    private ObjectNode planChunk() {
        ObjectNode n = CompactionTaskJsonSerde.object();
        n.putArray("buckets");
        return n;
    }

    private void addChunk(
            String id, Map<FsPath, byte[]> files, ObjectNode manifest, ObjectNode chunk)
            throws IOException {
        FsPath path = newPath(id, "buckets");
        files.put(path, CompactionTaskJsonSerde.encode(chunk));
        manifest.withArray("chunks").add(path.toString());
    }

    private long fileBytes(Map<FsPath, byte[]> files) throws IOException {
        long total = 0;
        for (Map.Entry<FsPath, byte[]> file : files.entrySet()) {
            if (file.getValue().length > blockLimit) {
                throw new IOException("Compaction block budget exceeded");
            }
            total += file.getValue().length;
        }
        return total;
    }

    private void writeFiles(String id, Map<FsPath, byte[]> files) throws IOException {
        fileBytes(files);
        fs.mkdirs(directory(id));
        for (Map.Entry<FsPath, byte[]> file : files.entrySet()) {
            try (FSDataOutputStream out =
                    fs.create(file.getKey(), FileSystem.WriteMode.NO_OVERWRITE)) {
                out.write(file.getValue());
            }
        }
    }

    private JsonNode read(FsPath handle) throws IOException {
        if (fs.getFileStatus(handle).getLen() > blockLimit) {
            throw new IOException("Compaction block budget exceeded");
        }
        try (FSDataInputStream input = fs.open(handle);
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int n;
            while ((n = input.read(buffer)) != -1) {
                if (output.size() + n > blockLimit) {
                    throw new IOException("Compaction block budget exceeded");
                }
                output.write(buffer, 0, n);
            }
            return CompactionTaskJsonSerde.decode(output.toByteArray());
        }
    }
}
