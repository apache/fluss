/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.KvStorageException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.memory.LazyMemorySegmentPool;
import com.alibaba.fluss.memory.MemorySegmentPool;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.TabletManagerBase;
import com.alibaba.fluss.server.log.LogManager;
import com.alibaba.fluss.server.log.LogTablet;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/**
 * The entry point to the fluss kv management subsystem. The kv manager is responsible for kv tablet
 * creation, retrieval, and cleaning. All read and write operations to kv tablet are delegated to
 * the individual instances.
 */
@ThreadSafe
public final class KvManager extends TabletManagerBase {

    private static final Logger LOG = LoggerFactory.getLogger(KvManager.class);
    private final LogManager logManager;

    private final ZooKeeperClient zkClient;

    private final Map<TableBucket, KvTablet> currentKvs = new ConcurrentHashMap<>();

    /**
     * For arrow log format. The buffer allocator to allocate memory for arrow write batch of
     * changelog records.
     */
    private final BufferAllocator arrowBufferAllocator;

    /** The memory segment pool to allocate memorySegment. */
    private final MemorySegmentPool memorySegmentPool;

    private final FsPath remoteKvDir;

    private final FileSystem remoteFileSystem;

    private KvManager(
            File dataDir,
            Configuration conf,
            ZooKeeperClient zkClient,
            int recoveryThreadsPerDataDir,
            LogManager logManager)
            throws IOException {
        super(TabletType.KV, dataDir, conf, recoveryThreadsPerDataDir);
        this.logManager = logManager;
        this.arrowBufferAllocator = new RootAllocator(Long.MAX_VALUE);
        this.memorySegmentPool = LazyMemorySegmentPool.create(conf);
        this.zkClient = zkClient;
        this.remoteKvDir = FlussPaths.remoteKvDir(conf);
        this.remoteFileSystem = remoteKvDir.getFileSystem();
    }

    public static KvManager create(
            Configuration conf, ZooKeeperClient zkClient, LogManager logManager)
            throws IOException {
        String dataDirString = conf.getString(ConfigOptions.DATA_DIR);
        File dataDir = new File(dataDirString).getAbsoluteFile();
        return new KvManager(
                dataDir,
                conf,
                zkClient,
                conf.getInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS),
                logManager);
    }

    public void startup() {
        // should do nothing now
    }

    public void shutdown() {
        LOG.info("Shutting down KvManager");
        List<KvTablet> kvs = new ArrayList<>(currentKvs.values());
        for (KvTablet kvTablet : kvs) {
            try {
                kvTablet.close();
            } catch (Exception e) {
                LOG.warn("Exception while closing kv tablet {}.", kvTablet.getTableBucket(), e);
            }
        }
        arrowBufferAllocator.close();
        memorySegmentPool.close();
        LOG.info("Shut down KvManager complete.");
    }

    /**
     * If the kv already exists, just return a copy of the existing kv. Otherwise, create a kv for
     * the given table and the given bucket.
     *
     * <p>Note: if the parameter {@code partitionName} is null, the log dir path is:
     * /{database}/{table-name}-{table_id}/kv-{bucket-id}. Otherwise, the log dir path is:
     * /{database}/{table-name}-{partitionName}-{table_id}-p{partition_id}/kv-{bucket-id}
     *
     * @param tablePath the table path of the bucket belongs to
     * @param tableBucket the table bucket
     * @param logTablet the cdc log tablet of the kv tablet
     * @param kvFormat the kv format
     */
    public KvTablet getOrCreateKv(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            KvFormat kvFormat)
            throws Exception {
        return inLock(
                tabletCreationOrDeletionLock,
                () -> {
                    if (currentKvs.containsKey(tableBucket)) {
                        return currentKvs.get(tableBucket);
                    }

                    File tabletDir = getOrCreateTabletDir(tablePath, tableBucket);

                    KvTablet tablet =
                            KvTablet.create(
                                    logTablet,
                                    tabletDir,
                                    conf,
                                    arrowBufferAllocator,
                                    memorySegmentPool,
                                    kvFormat);
                    currentKvs.put(tableBucket, tablet);

                    LOG.info(
                            "Created kv tablet for bucket {} in dir {}.",
                            tableBucket,
                            tabletDir.getAbsolutePath());

                    return tablet;
                });
    }

    /**
     * Create the tablet directory for the given table path and table bucket.
     *
     * <p>When the tablet directory exists, it will first delete it and create a new directory.
     *
     * @param tablePath the table path of the bucket
     * @param tableBucket the table bucket
     * @return the tablet directory
     */
    public File createTabletDir(PhysicalTablePath tablePath, TableBucket tableBucket) {
        File tabletDir = getTabletDir(tablePath, tableBucket);

        // delete the tablet dir if exists
        FileUtils.deleteDirectoryQuietly(tabletDir);
        createTabletDirectory(tabletDir);
        return tabletDir;
    }

    public Optional<KvTablet> getKv(TableBucket tableBucket) {
        return Optional.ofNullable(currentKvs.get(tableBucket));
    }

    public void dropKv(TableBucket tableBucket) {
        KvTablet dropKvTablet =
                inLock(tabletCreationOrDeletionLock, () -> currentKvs.remove(tableBucket));

        if (dropKvTablet != null) {
            TablePath tablePath = dropKvTablet.getTablePath();
            try {
                dropKvTablet.drop();
                if (dropKvTablet.getPartitionName() == null) {
                    LOG.info(
                            "Deleted kv bucket {} for table {} in file path {}.",
                            tableBucket.getBucket(),
                            tablePath,
                            dropKvTablet.getKvTabletDir().getAbsolutePath());
                } else {
                    LOG.info(
                            "Deleted kv bucket {} for the partition {} of table {} in file path {}.",
                            tableBucket.getBucket(),
                            dropKvTablet.getPartitionName(),
                            tablePath,
                            dropKvTablet.getKvTabletDir().getAbsolutePath());
                }
            } catch (Exception e) {
                throw new KvStorageException(
                        String.format(
                                "Exception while deleting kv for table %s, bucket %s in dir %s.",
                                tablePath,
                                tableBucket.getBucket(),
                                dropKvTablet.getKvTabletDir().getAbsolutePath()),
                        e);
            }
        } else {
            LOG.warn("Fail to delete kv bucket {}.", tableBucket.getBucket());
        }
    }

    public KvTablet loadKv(File tabletDir) throws Exception {
        Tuple2<PhysicalTablePath, TableBucket> pathAndBucket = FlussPaths.parseTabletDir(tabletDir);
        PhysicalTablePath physicalTablePath = pathAndBucket.f0;
        TableBucket tableBucket = pathAndBucket.f1;
        // get the log tablet for the kv tablet
        LogTablet logTablet =
                logManager
                        .getLog(tableBucket)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                String.format(
                                                        "Find a kv tablet for %s in dir %s to load, but can't find the log tablet for the bucket."
                                                                + " It is recommended to delete the dir %s to make the loading other kv tablets can success.",
                                                        tableBucket,
                                                        tabletDir.getAbsolutePath(),
                                                        tabletDir.getAbsolutePath())));

        // TODO: we should support recover schema from disk to decouple put and schema.
        TablePath tablePath = physicalTablePath.getTablePath();
        TableDescriptor tableDescriptor =
                getTableDescriptor(zkClient, tablePath, tableBucket, tabletDir);
        KvTablet kvTablet =
                KvTablet.create(
                        physicalTablePath,
                        tableBucket,
                        logTablet,
                        tabletDir,
                        conf,
                        arrowBufferAllocator,
                        memorySegmentPool,
                        tableDescriptor.getKvFormat());
        if (this.currentKvs.containsKey(tableBucket)) {
            throw new IllegalStateException(
                    String.format(
                            "Duplicate kv tablet directories for bucket %s are found in both %s and %s. "
                                    + "Recover server from this "
                                    + "failure by manually deleting one of the two kv directories for this bucket. "
                                    + "It is recommended to delete the bucket in the kv tablet directory that is "
                                    + "known to have failed recently.",
                            tableBucket,
                            tabletDir.getAbsolutePath(),
                            currentKvs.get(tableBucket).getKvTabletDir().getAbsolutePath()));
        }
        this.currentKvs.put(tableBucket, kvTablet);
        return kvTablet;
    }

    public void deleteRemoteKvSnapshot(
            PhysicalTablePath physicalTablePath, TableBucket tableBucket) {
        FsPath remoteKvTabletDir =
                FlussPaths.remoteKvTabletDir(remoteKvDir, physicalTablePath, tableBucket);
        try {
            if (remoteFileSystem.exists(remoteKvTabletDir)) {
                remoteFileSystem.delete(remoteKvTabletDir, true);
                LOG.info("Delete table's remote bucket snapshot dir of {} success.", tableBucket);
            }
        } catch (Exception e) {
            LOG.error(
                    "Delete table's remote bucket snapshot dir of {} failed.",
                    remoteKvTabletDir,
                    e);
        }
    }
}
