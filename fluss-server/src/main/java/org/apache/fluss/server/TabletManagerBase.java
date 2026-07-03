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

package org.apache.fluss.server;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.FlussPaths.KV_TABLET_DIR_PREFIX;
import static org.apache.fluss.utils.FlussPaths.LOG_TABLET_DIR_PREFIX;
import static org.apache.fluss.utils.FlussPaths.isPartitionDir;

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

    protected final List<File> dataDirs;

    protected final Configuration conf;

    protected final Lock tabletCreationOrDeletionLock = new ReentrantLock();

    // TODO make this parameter configurable.
    private final int recoveryThreads;
    private final TabletType tabletType;
    private final String tabletDirPrefix;

    public TabletManagerBase(
            TabletType tabletType, List<File> dataDirs, Configuration conf, int recoveryThreads) {
        this.tabletType = tabletType;
        this.tabletDirPrefix = getTabletDirPrefix(tabletType);
        this.dataDirs = new ArrayList<>(dataDirs);
        this.conf = conf;
        this.recoveryThreads = recoveryThreads;
    }

    /**
     * Return the directories of the tablets to be loaded, grouped by configured data directory.
     *
     * <p>See more about the local directory contracts: {@link FlussPaths#logTabletDir(File,
     * PhysicalTablePath, TableBucket)} and {@link FlussPaths#kvTabletDir(File, PhysicalTablePath,
     * TableBucket)}.
     */
    protected Map<File, List<File>> listTabletsToLoad() {
        Map<File, List<File>> tabletsToLoadByDataDir = new LinkedHashMap<>();
        for (File dataDir : dataDirs) {
            tabletsToLoadByDataDir.put(dataDir, listTabletsToLoad(dataDir));
        }
        return tabletsToLoadByDataDir;
    }

    /** Returns the tablet directories to be loaded from a single configured data directory. */
    protected List<File> listTabletsToLoad(File dataDir) {
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

    /**
     * Get the tablet directory with given directory name for the given data directory, table path
     * and table bucket.
     *
     * <p>When the parent directory of the tablet directory is missing, it will create the
     * directory.
     *
     * @param dataDir the local data directory chosen for this tablet
     * @param tablePath the table path of the bucket
     * @param tableBucket the table bucket
     * @return the tablet directory
     */
    protected File getOrCreateTabletDir(
            File dataDir, PhysicalTablePath tablePath, TableBucket tableBucket) {
        File tabletDir = getTabletDir(dataDir, tablePath, tableBucket);
        if (tabletDir.exists()) {
            return tabletDir;
        }
        createTabletDirectory(tabletDir);
        return tabletDir;
    }

    public Path getTabletParentDir(
            File dataDir, PhysicalTablePath tablePath, TableBucket tableBucket) {
        return getTabletDir(dataDir, tablePath, tableBucket).toPath().getParent();
    }

    protected File getTabletDir(
            File dataDir, PhysicalTablePath tablePath, TableBucket tableBucket) {
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
            throw new SchemaNotExistException(
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
