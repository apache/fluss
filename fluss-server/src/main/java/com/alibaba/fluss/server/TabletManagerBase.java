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

package com.alibaba.fluss.server;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.KvStorageException;
import com.alibaba.fluss.exception.LogStorageException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.kv.KvManager;
import com.alibaba.fluss.server.log.LogManager;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.FlussPaths.KV_TABLET_DIR_PREFIX;
import static com.alibaba.fluss.utils.FlussPaths.LOG_TABLET_DIR_PREFIX;
import static com.alibaba.fluss.utils.FlussPaths.isPartitionDir;

/**
 * A base class for {@link LogManager} {@link KvManager} which provide a common logic for both of
 * them.
 */
public abstract class TabletManagerBase {

    private static final Logger LOG = LoggerFactory.getLogger(TabletManagerBase.class);

    /** The enum for the tablet type. */
    public enum TabletType {
        LOG,
        KV
    }

    protected final File dataDir;

    protected final Configuration conf;

    protected final Lock tabletCreationOrDeletionLock = new ReentrantLock();

    // TODO make this parameter configurable.
    private final int recoveryThreads;
    private final TabletType tabletType;
    private final String tabletDirPrefix;

    public TabletManagerBase(
            TabletType tabletType, File dataDir, Configuration conf, int recoveryThreads) {
        this.tabletType = tabletType;
        this.tabletDirPrefix = getTabletDirPrefix(tabletType);
        this.dataDir = dataDir;
        this.conf = conf;
        this.recoveryThreads = recoveryThreads;
    }

    /**
     * Return the directories of the tablets to be loaded.
     *
     * <p>See more about the local directory contracts: {@link FlussPaths#logTabletDir(File,
     * PhysicalTablePath, TableBucket)} and {@link FlussPaths#kvTabletDir(File, PhysicalTablePath,
     * TableBucket)}.
     */
    protected List<File> listTabletsToLoad() {
        List<File> tabletsToLoad = new ArrayList<>();
        // Get all database directory.
        File[] dbDirs = FileUtils.listDirectories(dataDir);
        for (File dbDir : dbDirs) {
            // Get all table path directory.
            File[] tableDirs = FileUtils.listDirectories(dbDir);
            for (File tableDir : tableDirs) {
                // maybe tablet directories or partition directories
                File[] tabletOrPartitionDirs = FileUtils.listDirectories(tableDir);

                List<File> tabletDirs = new ArrayList<>();
                for (File tabletOrPartitionDir : tabletOrPartitionDirs) {
                    // if not partition dir, consider it as a tablet dir
                    if (!isPartitionDir(tabletOrPartitionDir.getName())) {
                        tabletDirs.add(tabletOrPartitionDir);
                    } else {
                        // consider all dirs in partition as tablet dirs
                        tabletDirs.addAll(
                                Arrays.asList(FileUtils.listDirectories(tabletOrPartitionDir)));
                    }
                }

                // it may contain the directory for kv tablet and log tablet
                // filter out the directory for specific type tablet
                // actually it identified by the prefix of the directory
                tabletsToLoad.addAll(
                        tabletDirs.stream()
                                .filter(
                                        tabletDir ->
                                                tabletDir.getName().startsWith(tabletDirPrefix))
                                .collect(Collectors.toList()));
            }
        }

        return tabletsToLoad;
    }

    protected ExecutorService createThreadPool(String poolName) {
        return Executors.newFixedThreadPool(recoveryThreads, new ExecutorThreadFactory(poolName));
    }

    /** Running a series of jobs in a thread pool, and return the count of the successful job. */
    protected int runInThreadPool(Runnable[] runnableJobs, String poolName) throws Throwable {
        List<Future<?>> jobsForTabletDir = new ArrayList<>();
        ExecutorService pool = createThreadPool(poolName);
        for (Runnable runnable : runnableJobs) {
            jobsForTabletDir.add(pool.submit(runnable));
        }
        int successCount = 0;
        try {
            for (Future<?> future : jobsForTabletDir) {
                try {
                    future.get();
                    successCount++;
                } catch (InterruptedException | ExecutionException e) {
                    throw e.getCause();
                }
            }
        } finally {
            pool.shutdown();
        }
        return successCount;
    }

    /**
     * Get the tablet directory with given directory name for the given table path and table bucket.
     *
     * <p>When the parent directory of the tablet directory is missing, it will create the
     * directory.
     *
     * @param tablePath the table path of the bucket
     * @param tableBucket the table bucket
     * @return the tablet directory
     */
    protected File getOrCreateTabletDir(PhysicalTablePath tablePath, TableBucket tableBucket) {
        File tabletDir = getTabletDir(tablePath, tableBucket);
        if (tabletDir.exists()) {
            return tabletDir;
        }
        createTabletDirectory(tabletDir);
        return tabletDir;
    }

    public Path getTabletParentDir(PhysicalTablePath tablePath, TableBucket tableBucket) {
        return getTabletDir(tablePath, tableBucket).toPath().getParent();
    }

    protected File getTabletDir(PhysicalTablePath tablePath, TableBucket tableBucket) {
        switch (tabletType) {
            case LOG:
                return FlussPaths.logTabletDir(dataDir, tablePath, tableBucket);
            case KV:
                return FlussPaths.kvTabletDir(dataDir, tablePath, tableBucket);
            default:
                throw new IllegalArgumentException("Unknown tablet type: " + tabletType);
        }
    }

    // TODO: we should support get table info from local properties file instead of from zk
    public static TableInfo getTableInfo(ZooKeeperClient zkClient, TablePath tablePath)
            throws Exception {
        int schemaId = zkClient.getCurrentSchemaId(tablePath);
        Optional<SchemaInfo> schemaInfoOpt = zkClient.getSchemaById(tablePath, schemaId);
        SchemaInfo schemaInfo;
        if (!schemaInfoOpt.isPresent()) {
            throw new LogStorageException(
                    String.format(
                            "Failed to load table '%s': Table schema not found in zookeeper metadata.",
                            tablePath));
        } else {
            schemaInfo = schemaInfoOpt.get();
        }

        TableRegistration tableRegistration =
                zkClient.getTable(tablePath)
                        .orElseThrow(
                                () ->
                                        new LogStorageException(
                                                String.format(
                                                        "Failed to load table '%s': table info not found in zookeeper metadata.",
                                                        tablePath)));

        return tableRegistration.toTableInfo(tablePath, schemaInfo);
    }

    /** Create a tablet directory in the given dir. */
    protected void createTabletDirectory(File tabletDir) {
        try {
            Files.createDirectories(tabletDir.toPath());
        } catch (IOException e) {
            String errorMsg =
                    String.format(
                            "Failed to create directory %s for %s tablet.",
                            tabletDir.toPath(), tabletType);
            LOG.error(errorMsg, e);
            if (tabletType == TabletType.KV) {
                throw new KvStorageException(errorMsg, e);
            } else {
                throw new LogStorageException(errorMsg, e);
            }
        }
    }

    private static String getTabletDirPrefix(TabletType tabletType) {
        switch (tabletType) {
            case LOG:
                return LOG_TABLET_DIR_PREFIX;
            case KV:
                return KV_TABLET_DIR_PREFIX;
            default:
                throw new IllegalArgumentException("Unknown tablet type: " + tabletType);
        }
    }
}
