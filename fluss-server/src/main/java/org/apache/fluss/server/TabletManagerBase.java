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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.FlussPaths.HISTORICAL_LOOKUP_CACHE_DIR_NAME;
import static org.apache.fluss.utils.FlussPaths.KV_TABLET_DIR_PREFIX;
import static org.apache.fluss.utils.FlussPaths.LOG_TABLET_DIR_PREFIX;
import static org.apache.fluss.utils.FlussPaths.REMOTE_LOG_INDEX_LOCAL_CACHE;
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
        return listTabletsToLoad(
                dataDir,
                (directory, nameFilter) ->
                        Arrays.stream(FileUtils.listDirectories(directory))
                                .filter(file -> nameFilter.test(file.getName()))
                                .collect(Collectors.toList()));
    }

    /**
     * Lists tablet directories using the common layout and cache exclusions, with a caller-supplied
     * directory lister to control symbolic-link handling and listing failures.
     */
    protected <E extends Exception> List<File> listTabletsToLoad(
            File dataDir, DirectoryLister<E> directoryLister) throws E {
        List<File> tabletsToLoad = new ArrayList<>();
        for (File dbDir :
                directoryLister.listDirectories(
                        dataDir,
                        name ->
                                !name.equals(HISTORICAL_LOOKUP_CACHE_DIR_NAME)
                                        && !name.equals(REMOTE_LOG_INDEX_LOCAL_CACHE))) {
            for (File tableDir : directoryLister.listDirectories(dbDir, name -> true)) {
                for (File tabletOrPartitionDir :
                        directoryLister.listDirectories(
                                tableDir,
                                name -> isPartitionDir(name) || name.startsWith(tabletDirPrefix))) {
                    if (isPartitionDir(tabletOrPartitionDir.getName())) {
                        tabletsToLoad.addAll(
                                directoryLister.listDirectories(
                                        tabletOrPartitionDir,
                                        name -> name.startsWith(tabletDirPrefix)));
                    } else {
                        tabletsToLoad.add(tabletOrPartitionDir);
                    }
                }
            }
        }
        return tabletsToLoad;
    }

    /**
     * Lists child directories whose names match a filter, optionally reporting listing failures.
     */
    @FunctionalInterface
    protected interface DirectoryLister<E extends Exception> {

        /** Returns the matching child directories. */
        List<File> listDirectories(File parent, Predicate<String> nameFilter) throws E;
    }

    protected ExecutorService createThreadPool(String poolName) {
        return Executors.newFixedThreadPool(recoveryThreads, new ExecutorThreadFactory(poolName));
    }

    /**
     * Closes the given tablets concurrently and returns a future that completes after all close
     * operations have finished.
     *
     * <p>The closing thread pool is shut down automatically after all close operations complete.
     * The caller is responsible for handling exceptions inside the close action if one failed
     * tablet should not prevent the remaining shutdown steps from running.
     *
     * @param tablets tablets to close
     * @param poolName name of the closing thread pool
     * @param closeAction action that closes a tablet
     * @param <T> type of tablet
     * @return a future that completes after all tablets have been closed
     */
    protected <T> CompletableFuture<Void> closeTabletsConcurrently(
            Collection<T> tablets, String poolName, Consumer<T> closeAction) {
        LOG.info(
                "Closing {} tablets with up to {} threads in pool {}.",
                tablets.size(),
                recoveryThreads,
                poolName);
        ExecutorService closingPool = createThreadPool(poolName);
        List<CompletableFuture<Void>> closingFutures = new ArrayList<>(tablets.size());
        for (T tablet : tablets) {
            closingFutures.add(
                    CompletableFuture.runAsync(() -> closeAction.accept(tablet), closingPool));
        }

        CompletableFuture<Void> closingFuture =
                CompletableFuture.allOf(
                        closingFutures.toArray(new CompletableFuture<?>[closingFutures.size()]));
        return closingFuture.whenComplete((ignored, throwable) -> closingPool.shutdown());
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
